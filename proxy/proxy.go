// Package proxy 实现核心反代逻辑：从请求路径提取目标 URL、ACL 校验、
// 域名规则配置合并、url-redirect 轮询回源（含成功判据与直连回退）、
// 缓存读写与响应（状态码/响应头/Body）透传。
package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// Version 服务版本号，默认 dev（可在构建时通过 -ldflags 注入）。
var Version = "dev"

// hopByHopHeaders 逐跳头，不应在上游与客户端之间透传。
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Trailers", "Transfer-Encoding", "Upgrade",
}

// forwardRequestHeaders 回源请求（含 url-redirect 候选请求）透传的客户端请求头白名单。
// 注意：条件请求头（If-Match / If-Modified-Since / If-None-Match / If-Range / If-Unmodified-Since）
// 不透传——否则客户端带 If-None-Match 访问时，上游会对候选与直连回源都回 304（空 body），
// 导致成功判据不过、缓存不写、日志全程报失败；回源始终为无条件 GET/HEAD，
// 客户端的 304 语义由代理自身缓存 ETag 逻辑（serveEntry，HIT 场景）负责。其余白名单头（含 Range）保持透传。
var forwardRequestHeaders = []string{
	"Accept", "Accept-Encoding", "Accept-Language", "Authorization",
	"Content-Type", "Range", "User-Agent",
}

// upstreamClientKey 标识一个可复用的上游 http.Client 组合：上游代理地址 + 建连超时 +
// 忽略证书校验。connect-timeout 进入 Transport 的拨号/TLS 握手超时，insecure 进入
// Transport 的 TLSClientConfig，不同值必须各一个 Transport；
// 相同组合共享同一 Client/Transport，保证连接复用不被破坏。
type upstreamClientKey struct {
	// proxy 上游代理地址（空串 = 直连）。
	proxy string
	// connectTimeout 建连超时（0 = 不限制）。
	connectTimeout time.Duration
	// insecure 跳过上游 TLS 证书校验（insecure-skip-verify 的生效值）：
	// true 时 TLSClientConfig.InsecureSkipVerify=true——该客户端内的所有上游连接
	//（含 30x 重定向目标如 1ms.run→cloudfront、token dance 的第二次请求）都跳过校验，
	// 这正是「信任该候选/规则/全局配置所指向的上游」的语义。
	insecure bool
}

// Server 是代理服务 HTTP 处理器，实现 http.Handler。
type Server struct {
	// cfg 已校验的运行期只读配置。
	cfg *config.Config
	// caches 按目录复用缓存实例的管理器（nil 表示禁用缓存，简化外部注入）。
	caches *cache.Manager
	// log 结构化日志器。
	log *slog.Logger
	// rr url-redirect 轮询起始下标计数器：第 1 个使用该列表的请求从 0 开始，
	// 之后每个请求 +1，实现候选列表的起始位置轮换（round-robin 负载轮询）。
	rr atomic.Uint64
	// clientsMu 保护 clients 的惰性补建（同一组合并发首用时只建一次）。
	clientsMu sync.Mutex
	// clients 可复用的上游 http.Client：key = (上游代理地址, connect-timeout)。
	// New 预构建配置里出现的全部组合，运行期对遗漏组合（防御）惰性补建。
	clients map[upstreamClientKey]*http.Client
	// flight 同一缓存键的单飞组，防止缓存击穿。
	flight flightGroup
	// dockerTokenMu 保护 dockerTokens 的并发读写。
	dockerTokenMu sync.Mutex
	// dockerTokens Docker Registry 匿名 token 进程内缓存：key = realm|service|scope，
	// value = token 与过期时间点。镜像 blob 分层很多，进程内缓存避免每层都打一次
	// auth 端点（auth.docker.io 对国内很慢且有频控）。
	dockerTokens map[string]dockerTokenEntry
	// prefetchMu 保护 docker 预取在途集合与信号量的并发访问/惰性补建（见 prefetch.go）。
	prefetchMu sync.Mutex
	// prefetchInflight docker 预取在途集合：blob 缓存键 -> struct{}，
	// 同一 blob 的预取只排队一次（防重复排队，见 prefetch.go）。
	prefetchInflight map[string]struct{}
	// prefetchSlots docker 预取的 Server 级并发信号量（容量 = docker-prefetch-concurrency
	// 生效值，默认 4；New 预构建，防御性惰性补建见 prefetch.go）。
	prefetchSlots chan struct{}
	// prefetchWG docker 预取任务 WaitGroup：预取 goroutine 在此登记，仅供测试/诊断观察
	//（waitPrefetchIdle）；进程退出不等待——预取任务不使用请求 context 且自带超时兜底，
	// 随进程消亡即可，绝不悬挂退出（见 prefetch.go 生命周期说明）。
	prefetchWG sync.WaitGroup
	// prefetchHub docker 预取 blob 在途流共享注册表（搭车）：blob 缓存键 -> 在途下载
	// 条目，客户端请求撞上在途预取时拼接已落盘前缀与实时增量回放，上游仅预取一路下载
	//（见 prefetch_hub.go；New 预构建，防御性惰性补建见 prefetchHubOrInit）。
	prefetchHub *prefetchStreamHub
	// partialMu 保护 partialWriters 的并发访问/惰性补建。
	partialMu sync.Mutex
	// partialWriters blob 半截文件的按 key 写锁注册表：串行化同一 blob 半截文件的全部
	// 写侧操作（续传执行方逐块追加 / 失败前缀整体保存 / 预取半截改名），杜绝并发写
	// 交错产生「非前缀」内容（半截必须是 blob 原文前缀，续传拼接正确性的前提）。
	// 见 partial_resume.go（New 预构建，防御性惰性补建见 partWriter）。
	partialWriters map[string]*sync.Mutex
	// progressMu 保护 progressReg 的并发访问/惰性补建（活跃下载周期进度汇总，见 progress_summary.go）。
	progressMu sync.Mutex
	// progressReg 活跃下载注册表：全部下载流（client / ridealong / resume / prefetch）开始写时
	// 登记、结束注销，供周期汇总输出全局活跃下载进度（New 预构建，防御性惰性补建见 progressOrInit）。
	progressReg *progressRegistry
	// progressOnce 保证周期汇总 goroutine 只启动一次（StartProgressSummary 幂等）。
	progressOnce sync.Once
	// blobSizeMu 保护 blobSizes 的并发访问/惰性补建（manifest layer size 索引，见
	// blob_size_index.go）。
	blobSizeMu sync.Mutex
	// blobSizes docker layer blob 的 manifest size 进程内索引（digest → size，有界 FIFO）：
	// blob 流式回源响应没有 Content-Length 时用它兜底 knownTotal（日志 total_source=
	// manifest），让判速/时间驱动的多源并发补充在「上游 200 无 Content-Length」场景仍能
	// 正常工作（线上 1.32GiB 单层故障的根因之一）。New 预构建，防御性惰性补建见
	// blobSizeIndex。
	blobSizes *manifestBlobSizeIndex
	// blobDigestVerify blob 写缓存前的内容寻址校验钩子（防毒缓存）：入参 (body 原文,
	// digest "sha256:<hex>" 原文)，返回 (实际摘要 hex, 是否一致)。nil 时使用默认实现
	//（sha256 全量比对）——生产恒走默认强校验；仅测试注入 stub（既有夹具的 digest
	// 常量与内容无关，见各 *_test.go 构造器；防毒缓存回归测试还原 nil 走真校验）。
	blobDigestVerify func(body []byte, digest string) (string, bool)
}

// New 创建 Server：预编译全局/各域名规则及其 url-redirect 候选对象用到的上游代理客户端
// （候选级 (proxy, connect-timeout) 组合按候选显式 ?? 规则 ?? 全局 解析后一并纳入）。
// caches 传 nil 表示禁用缓存；多目录场景由 Manager 按生效目录复用实例。
func New(cfg *config.Config, caches *cache.Manager, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, caches: caches, log: logger, dockerTokens: make(map[string]dockerTokenEntry)}
	// docker 预取的 Server 级并发额度与在途集合：启动期按配置固化容量（默认 4），
	// 运行期防御性惰性补建见 prefetch.go
	s.prefetchSlots = make(chan struct{}, cfg.GlobalDockerPrefetchConcurrency())
	s.prefetchInflight = make(map[string]struct{})
	// docker 预取 blob 在途流共享注册表（搭车，见 prefetch_hub.go）
	s.prefetchHub = newPrefetchStreamHub()
	// blob 半截文件的按 key 写锁注册表（半截保留与无感续传，见 partial_resume.go）
	s.partialWriters = make(map[string]*sync.Mutex)
	// 活跃下载注册表（周期进度汇总，见 progress_summary.go）：New 只预构建注册表，
	// 定时汇总 goroutine 由 main.go 调 StartProgressSummary(ctx) 启动（生命周期归调用方）
	s.progressReg = newProgressRegistry()
	// docker layer blob 的 manifest size 索引（blob 无 Content-Length 时的 knownTotal
	// 兜底，见 blob_size_index.go）：容量有界，进程内即可（manifest 与 layer 通常在同一次
	// pull 内先后回源；size 与 digest 内容一样不可变）
	s.blobSizes = newManifestBlobSizeIndex()

	// 预构建全局与各域名规则可能用到的 (上游代理, connect-timeout) 组合客户端：
	// 启动期完成代理地址解析校验（配置错误启动快速失败），运行期 client() 只做防御性补建。
	// insecure-skip-verify 维度：insecure=false 变体始终构建；仅当配置里存在任一 true
	//（全局 flag / 规则 flag / 候选 item flag）时，才为每个组合额外预构建 insecure=true
	// 变体（无任何 insecure 配置时不建多余的 Transport）。
	baseCombos := map[upstreamClientKey]struct{}{
		{proxy: "", connectTimeout: cfg.GlobalConnectTimeout()}: {},
	}
	if p := strings.TrimSpace(cfg.HTTPProxy); p != "" {
		baseCombos[upstreamClientKey{proxy: p, connectTimeout: cfg.GlobalConnectTimeout()}] = struct{}{}
	}
	// 全局 url-redirect 候选列表：候选对象可显式配置 http-proxy / connect-timeout
	//（三级优先级最高），按「item 显式 ?? 全局 eff」解析并纳入预构建；
	// 未配置候选级字段时解析结果与全局组合相同（map 去重，不产生多余客户端）。
	globalEff := config.Options{
		HTTPProxy:      strings.TrimSpace(cfg.HTTPProxy),
		ConnectTimeout: cfg.GlobalConnectTimeout(),
	}
	for _, item := range cfg.URLRedirect {
		baseCombos[upstreamClientKey{
			proxy:          globalEff.CandidateHTTPProxy(item),
			connectTimeout: globalEff.CandidateConnectTimeout(item),
		}] = struct{}{}
	}
	for i := range cfg.DomainRules {
		r := &cfg.DomainRules[i]
		proxy := strings.TrimSpace(cfg.HTTPProxy) // 规则未配 http-proxy 时沿用全局
		if r.HTTPProxy != nil {
			proxy = strings.TrimSpace(*r.HTTPProxy) // 规则显式空串 = 直连
		}
		ct := cfg.GlobalConnectTimeout() // 规则未配 connect-timeout 时沿用全局
		if r.ConnectTimeout != nil {
			ct = r.ConnectTimeout.D()
		}
		baseCombos[upstreamClientKey{proxy: proxy, connectTimeout: ct}] = struct{}{}
		// 规则级候选列表：按「item 显式 ?? 该规则 eff」逐候选解析（与运行期
		// ExpandCandidates 的解析同源），候选显式组合与规则组合不同时补建对应客户端；
		// 未配置候选级字段时解析结果即上面的规则组合，map 去重不产生多余客户端
		ruleEff := config.Options{HTTPProxy: proxy, ConnectTimeout: ct}
		if r.URLRedirect != nil {
			for _, item := range *r.URLRedirect {
				baseCombos[upstreamClientKey{
					proxy:          ruleEff.CandidateHTTPProxy(item),
					connectTimeout: ruleEff.CandidateConnectTimeout(item),
				}] = struct{}{}
			}
		}
	}
	comboSet := make(map[upstreamClientKey]struct{}, 2*len(baseCombos))
	anyInsecure := configHasInsecureSkipVerify(cfg)
	for c := range baseCombos {
		comboSet[c] = struct{}{} // insecure=false 恒预构建
		if anyInsecure {
			inc := c
			inc.insecure = true
			comboSet[inc] = struct{}{}
		}
	}
	s.clients = make(map[upstreamClientKey]*http.Client, len(comboSet))
	for c := range comboSet {
		cl, err := buildHTTPClient(c.proxy, c.connectTimeout, c.insecure)
		if err != nil {
			return nil, fmt.Errorf("预构建上游客户端失败（proxy=%q connect-timeout=%s insecure=%v）: %w",
				c.proxy, c.connectTimeout, c.insecure, err)
		}
		s.clients[c] = cl
	}
	return s, nil
}

// configHasInsecureSkipVerify 扫描配置里是否存在任一「忽略 TLS 证书校验」的开启：
// 全局 insecure-skip-verify、任一域名规则的 insecure-skip-verify 或任一候选对象的
// insecure-skip-verify 显式 true（候选/规则的 nil = 继承下一级，不算显式开启）。
// 供 New 决定是否需要预构建 insecure=true 客户端变体。
func configHasInsecureSkipVerify(cfg *config.Config) bool {
	if cfg.InsecureSkipVerify {
		return true
	}
	for i := range cfg.DomainRules {
		r := &cfg.DomainRules[i]
		if r.InsecureSkipVerify != nil && *r.InsecureSkipVerify {
			return true
		}
		if r.URLRedirect == nil {
			continue
		}
		for _, item := range *r.URLRedirect {
			if item.InsecureSkipVerify != nil && *item.InsecureSkipVerify {
				return true
			}
		}
	}
	for _, item := range cfg.URLRedirect {
		if item.InsecureSkipVerify != nil && *item.InsecureSkipVerify {
			return true
		}
	}
	return false
}

// newUpstreamDialer 构建上游拨号器：connect-timeout 用于 TCP 拨号超时（0 = 不限制）。
func newUpstreamDialer(connectTimeout time.Duration) *net.Dialer {
	return &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
}

// buildHTTPClient 构建指定上游代理（空串=直连）、建连超时与忽略证书校验的 http.Client：
// connect-timeout 同时用于 TCP 拨号与 TLS 握手，0 = 不设超时（不限制，
// 旧版硬编码的 10s 建连/TLS 超时已被该模型取代）。相同 (代理, connect-timeout, insecure)
// 组合应共享同一 Client 实例（见 Server.client），保证连接复用不被破坏。
func buildHTTPClient(proxyAddr string, connectTimeout time.Duration, insecureSkipVerify bool) (*http.Client, error) {
	tr, err := newUpstreamTransport(proxyAddr, connectTimeout, insecureSkipVerify)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: tr}, nil
}

// newUpstreamTransport 构建上游 Transport：独立成函数便于测试直接检视
// connect-timeout 是否正确落到拨号器与 TLS 握手超时上、insecure 是否落到 TLSClientConfig。
// insecureSkipVerify=true 时设置 TLSClientConfig.InsecureSkipVerify=true：该 Transport 上的
// 所有上游连接（含 30x 重定向目标与 token dance 的第二次请求）都跳过 TLS 证书校验，
// 即「信任该上游路径」的显式语义（insecure-skip-verify 配置生效的落地点）。
func newUpstreamTransport(proxyAddr string, connectTimeout time.Duration, insecureSkipVerify bool) (*http.Transport, error) {
	tr := &http.Transport{
		// TCP 拨号与 TLS 握手共用 connect-timeout；0 值在 net.Dialer 与
		// TLSHandshakeTimeout 的语义即为无超时（不限制）
		DialContext:           newUpstreamDialer(connectTimeout).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   connectTimeout,
		ExpectContinueTimeout: time.Second,
	}
	if insecureSkipVerify {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if proxyAddr != "" {
		u, err := url.Parse(proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("http-proxy %q 解析失败: %w", proxyAddr, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" {
			return nil, fmt.Errorf("http-proxy %q 协议仅支持 http/https/socks5", proxyAddr)
		}
		tr.Proxy = http.ProxyURL(u)
	}
	return tr, nil
}

// client 返回（上游代理, connect-timeout, insecure）组合对应的 Client：New 已预构建配置内
// 全部组合，这里对防御性遗漏的组合惰性补建并缓存（同一组合只建一次，连接复用不受影响）。
func (s *Server) client(proxyAddr string, connectTimeout time.Duration, insecureSkipVerify bool) *http.Client {
	key := upstreamClientKey{proxy: proxyAddr, connectTimeout: connectTimeout, insecure: insecureSkipVerify}
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	if cl, ok := s.clients[key]; ok && cl != nil {
		return cl
	}
	// 防御分支：代理地址已在配置校验/启动预构建阶段验证过，正常不会走到这里；
	// 补建失败兜底直连客户端，绝不让请求因客户端构建问题而 panic
	cl, err := buildHTTPClient(proxyAddr, connectTimeout, insecureSkipVerify)
	if err != nil {
		s.log.Error("惰性构建上游客户端失败，兜底直连客户端",
			"proxy", proxyAddr, "connect_timeout", connectTimeout.String(),
			"insecure", insecureSkipVerify, "err", err)
		if direct, ok := s.clients[upstreamClientKey{}]; ok && direct != nil {
			cl = direct
		} else {
			cl, _ = buildHTTPClient("", 0, false)
		}
	}
	s.clients[key] = cl
	return cl
}

// ServeHTTP 是 http.Handler 入口：分流首页 / 健康检查 /healthz / 代理请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "", "/":
		s.handleIndex(w, r)
		return
	case "/healthz":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, `{"status":"ok"}`+"\n"); err != nil {
			s.log.Warn("写 healthz 响应失败", "err", err)
		}
		return
	}
	s.handleProxy(w, r)
}

// handleIndex 输出服务用法说明（首页）。
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	msg := "proxy-cache " + Version + "（GitHub 反向代理 + 缓存）\n\n" +
		"用法:\n  http://<监听地址>/<目标URL>\n\n" +
		"示例:\n  curl -i http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml\n\n" +
		"Docker Registry 加速（需 domain-rules 命中入站域名并配置 url-redirect，详见 README）:\n" +
		"  docker pull <入站域名>:<端口>/<镜像名>，如 registry-proxy-cache.linkease.net:5480/adockero/proxy-cache\n\n" +
		"健康检查: /healthz\n"
	if _, err := io.WriteString(w, msg); err != nil {
		s.log.Warn("写首页响应失败", "err", err)
	}
}

// handleProxy 处理一条代理请求：提取目标（完整 URL / 路径目标）-> 日志请求开始 ->
// ACL -> 合并生效配置 -> 路径目标分发限制 -> 按方法分发。
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// //////////////////  提取并区分目标类型  start  ////////////////////////////////////////////////
	// fullURL=true：target 为完整 http/https URL（原有语义不变）；
	// fullURL=false：路径目标（如 docker registry 的 /v2/xxx/manifests/latest），
	// 没有完整原始 URL，只能由命中的域名规则 url-redirect 模板拼出回源地址
	target, fullURL, err := ExtractTarget(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// docker 日志统一标签：客户端请求入口按需解析 image/digest/ref（/v2/ 目标才有值，
	// web 完整 URL 目标不加新键，日志零变化，见 log_attrs.go）
	labels := parseDockerLogInfo(target)
	s.log.Debug("代理请求开始", labels.attrs(dockerLogSourceClient, "method", r.Method, "remote", r.RemoteAddr,
		"target", target, "target_kind", targetKind(fullURL))...)
	// //////////////////  提取并区分目标类型  end  ////////////////////////////////////////////////

	// //////////////////  allow/deny 访问控制  start  ////////////////////////////////////////////////
	// 完整 URL 请求对 target 匹配（行为不变）；路径目标请求对「http://<Host>/<路径目标>」
	// 整串匹配，保持对路径目标请求的拦截能力。
	// matched 为命中的列表项：deny 命中时非空且 err!=nil；allow 白名单模式未命中时为空串且 err!=nil
	aclInput := target
	if !fullURL {
		aclInput = config.BuildMatchStr(r.Host, target)
	}
	if matched, aclErr := s.cfg.CheckACL(aclInput); aclErr != nil {
		if matched != "" {
			s.log.Warn("请求被拒绝列表拦截", "target", aclInput, "matched", matched)
		} else {
			s.log.Warn("请求未命中白名单", "target", aclInput, "matched", matched)
		}
		s.writeError(w, http.StatusForbidden, aclErr.Error())
		return
	}
	// //////////////////  allow/deny 访问控制  end  ////////////////////////////////////////////////

	// //////////////////  合并全局配置与域名规则  start  ////////////////////////////////////////////////
	// 域名规则对「http://<入站Host>/<目标>」整串匹配，入站 Host 参与匹配
	//（路径目标请求的匹配串形如 http://registry-proxy-cache.xxx:5480/v2/xxx/manifests/latest）；
	// fullURL 传真实请求类型，供规则 type 请求类型过滤
	//（type: docker 仅承接路径目标 / type: web 仅承接完整 URL / 留空不过滤）
	eff := s.cfg.ResolveOptions(r.Host, target, fullURL)
	// //////////////////  合并全局配置与域名规则  end  ////////////////////////////////////////////////

	// //////////////////  路径目标请求的分发限制  start  ////////////////////////////////////////////////
	// 路径目标没有完整原始 URL，必须依赖命中的域名规则 url-redirect 模板拼出回源地址：
	//   - 仅支持 GET / HEAD（docker registry 拉取只用这两个方法），其他方法 400；
	//   - 必须命中某条域名规则（eff.Rule != ""）且生效 url-redirect 非空，否则 400
	//    （全局默认配置不承接路径目标）。
	if !fullURL {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			s.writeError(w, http.StatusBadRequest,
				fmt.Sprintf("路径目标仅支持 GET/HEAD 请求（docker registry 拉取只用这两个方法），收到 %s", r.Method))
			return
		}
		if eff.Rule == "" || len(eff.URLRedirect) == 0 {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf(
				"目标 URL 必须是完整的 http/https 地址，收到 %q：路径目标请求必须命中某条 domain-rules 规则且该规则配置了 url-redirect 才能回源（全局默认配置不承接路径目标）",
				truncate(target, 128)))
			return
		}
	}
	// //////////////////  路径目标请求的分发限制  end  ////////////////////////////////////////////////

	// GET 走完整链路（缓存读写 + url-redirect 轮询）；HEAD 统一走候选轮询管线
	//（读缓存 + 轮询回源，不写缓存）；其他方法不缓存、不轮询，直接透传
	//（路径目标已在上方拦截非 GET/HEAD，这里只有完整 URL 请求会落到直连透传）
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, target, fullURL, eff, start)
	case http.MethodHead:
		s.handleHead(w, r, target, fullURL, eff, start)
	default:
		e, err := s.fetchDirect(r, target, eff)
		if err != nil {
			s.writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		s.serveEntry(w, r, e, "BYPASS", eff, start)
	}
}

// targetKind 返回目标类型标识（日志用）：full-url（完整 URL 目标）/ path（路径目标）。
func targetKind(fullURL bool) string {
	if fullURL {
		return "full-url"
	}
	return "path"
}

// handleGet 处理 GET 请求：读缓存（HIT）或 单飞回源 + 写缓存（MISS）；不可缓存时直接回源（BYPASS）。
// fullURL 区分两类目标的缓存 key 输入（见 cacheKeyInput）：完整 URL = target 原文
// （兼容已有缓存文件）；路径目标 = 「http://<入站Host>/<路径目标>」（入站 Host 参与，
// 避免不同入站域名的同路径缓存互相污染）。
//
// 路径目标请求（fullURL=false，docker registry 回源）的回源为流式透传：
// 候选竞速选定后立即 commit（WriteHeader + 透传响应头）再边读上游边写客户端，
// 保证首字节时间只有「选定候选」的耗时，不被上游完整下载拖到超过 docker 客户端
// 约 30s 的响应头超时（详见 stream.go）。完整 URL 目标（web）保持整包缓冲回放不变。
//
// 客户端 Range 分流（docker blob，详见 client_range.go）：起点 0 等价完整 GET（剥离 Range
// 后走缓存/半截续传/预取搭车，响应侧改写成 206）；起点 >0 时缓存 HIT 直接本地切片回 206、
// 有半截前缀时按半截长度对齐走续传拼接（都是 206），无半截 / 有缺口才维持旧版 206 透传。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, target string, fullURL bool, eff config.Options, start time.Time) {
	key := cache.Key(cacheKeyInput(r, target, fullURL))
	// docker 日志统一标签：客户端主路径（HIT / BYPASS / 单飞回源）统一 source=client，
	// /v2/ 目标按需解析 image/digest/ref（见 log_attrs.go）
	labels := parseDockerLogInfo(target)
	// 路径目标 GET：流式透传（选定候选立即 commit）；完整 URL GET：缓冲回放（行为不变）
	streaming := !fullURL

	// //////////////////  客户端 Range 分流：docker blob（起点 0 / 任意起点）  start  ////////////////
	// 现场故障：docker/containerd 拉 blob 无论从哪个偏移都带 Range（httpReadSeeker）——首次
	// `bytes=0-`，断点续传重试 `bytes=<已有偏移>-`（现场起点 ≈ 82.8MB）。旧版把所有 Range
	// 一律判 BYPASS，于是每次重试都绕过缓存/半截续传/预取搭车、纯透传（现场上游 13KB/s，
	// 慢而不死、每轮重试还要再烧一遍候选探测）：链路中途断一次就只能从 0 重来，302MB 层
	// 必然断，最终 502。现在 docker blob 目标的单区间 bytes Range 全部接管（见 client_range.go）：
	//   - 起点 0 = 完整 GET：立刻剥离 Range，内部一切机制（缓存 HIT / 半截续传 / 预取搭车 /
	//     单飞合并）照常生效，且 Range 绝不再透传给上游（否则上游回 206，判据/写缓存/续传
	//     全部失配）；响应侧包一层 zeroRangeWriter 把内部 200 改写成 206 + Content-Range；
	//   - 起点 >0 = 真正的断点续传：缓存 HIT 直接从条目 body 本地切片回 206（不发上游）；
	//     缓存 MISS 时按「起点与半截长度的关系」走续传管线（起点 <= 半截长度）或落到下方
	//     既有 request-range 206 透传（无半截 / 有缺口，缺口另打 Warn）。
	// 非 blob 目标与后缀区间/多区间保持原样（下方 request-range）。
	rangeSpec, blobRange := parseBlobRange(r, target)
	zeroRange := blobRange && rangeSpec.start == 0
	if zeroRange {
		r.Header.Del("Range")
		w = newZeroRangeWriter(w, rangeSpec)
		s.log.Info("docker blob 请求带 Range 起点 0：按完整 GET 处理（对客户端回 206 + Content-Range，缓存/半截续传/预取搭车全部生效）",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "target", truncate(target, 300),
				"range_end", rangeSpec.end, "range_has_end", rangeSpec.hasEnd)...)
	}
	// 起点 >0 的 blob Range 仍保留 Range 头（本轮不写正式缓存条目、也不按完整 GET 回放），
	// 由下方 blobRangeLocal 分支决定「本地缓存切片 / 半截对齐续传 / 旧版 206 透传」；
	// 客户端 no-cache（显式要求强制回源）与缓存关闭/负 TTL 时不拿本地内容满足它
	blobRangeLocal := blobRange && rangeSpec.start > 0 &&
		eff.CacheEnabled && eff.CacheTTL >= 0 && !clientNoCache(r)
	// //////////////////  客户端 Range 分流：docker blob（起点 0 / 任意起点）  end  ////////////////

	// serveEntry 的可选区间参数：仅 zeroRange（blob 起点 0）的 HIT 回放需要——用来抑制
	// ETag 304 降级（对 Range 请求回 304 会让 docker 拿不到任何内容），body/状态码的 206
	// 改写由 zeroRangeWriter 负责。起点 >0 的区间请求由 serveEntryBlobRange 直接切片回放
	// （天然不回 304）；非 blobRange 请求传空，行为与旧版完全一致。
	var zeroArgs []blobRangeSpec
	if zeroRange {
		zeroArgs = []blobRangeSpec{rangeSpec}
	}

	// 先判定本请求不走缓存的具体原因（空串表示可缓存），再把原因暴露到日志，
	// 避免出现「没写缓存文件但日志看不出为什么」的排查黑洞。
	// 可缓存条件：cache.enabled 且 ttl>=0（TTL=0 表示永不过期，同样可缓存；负数理论上
	// 已被 config.Validate 拦截，此处 >=0 兜底防御）且无 Range / no-cache 语义冲突
	//（起点 0 的 blob Range 已在上方按完整 GET 接管并剥离，走不到下面的 request-range；
	//  起点 >0 的 blob Range 只在「读缓存 / 半截续传」上放开（blob-range），仍不写正式
	//  缓存条目与半截——它的完整内容由续传执行方按整 blob 写缓存）
	bypassReason := ""
	switch {
	case !eff.CacheEnabled:
		bypassReason = "cache-disabled" // 全局或命中规则显式关闭缓存
	case eff.CacheTTL < 0:
		bypassReason = "negative-ttl" // 防御分支：Validate 已拦截负 TTL
	case dockerMutableTarget(target):
		// docker 可变目标（tag manifest / referrers / tags list）：名字寻址、内容随
		// push / 签名追加而变化，读写缓存都跳过——读旧条目会回放过期
		// Docker-Content-Digest 让 docker 判「Image is up to date」永远拉不到新推送
		//（2026-11-07 现场：latest 卡旧 digest）。不可变的 blob / digest manifest 不受影响。
		bypassReason = dockerMutableBypassReason
	case r.Header.Get("Range") != "":
		if blobRangeLocal {
			bypassReason = "blob-range" // blob 起点 >0：不写缓存，但允许本地切片 / 半截续传
		} else {
			bypassReason = "request-range" // Range 请求绕过缓存读取（避免语义冲突）
		}
	case clientNoCache(r):
		bypassReason = "request-no-cache" // 客户端要求强制回源刷新
	}
	cacheable := bypassReason == ""

	// 解析本请求生效的缓存实例：规则可配独立缓存目录，按生效目录向管理器获取（复用已预热实例）；
	// 获取失败时本次请求降级为直接回源（BYPASS），不中断服务。
	// blobRangeLocal 同样要拿到实例：缓存 HIT 才能本地切片回 206，半截文件也在该目录下
	var disk *cache.Cache
	if cacheable || blobRangeLocal {
		c, err := s.cacheFor(eff)
		if err != nil {
			s.log.Warn("获取缓存实例失败，本次请求降级为直接回源", "dir", eff.CachePath, "err", err)
			cacheable = false
			bypassReason = "cache-init-failed"
		} else {
			disk = c
		}
	}

	// finalizeFetchEntry 回源完成后的缓存写入与留痕（仅 200 且过判据写缓存，
	// 304 / 未过判据分别留痕说明原因；条件与日志字段与旧版一致）：本闭包只承接客户端
	// 主路径的回源，写缓存日志统一标注 source=client（续传执行方 / 预取写缓存各自
	// 直调 finalizeFetchEntry 并标注 resume / prefetch）
	finalize := func(ent *cache.Entry) {
		s.finalizeFetchEntry(r, key, target, ent, eff, disk, dockerLogSourceClient)
	}
	// streamingError 流式回源错误的一致出口：commit 前失败 502（候选全败且最后失败为
	// 上游明确 4xx 时透传上游状态码与响应，见 upstream_status.go）；commit 后失败响应已
	// 固化（头已写出），只能终止连接，绝不能再 writeError 污染响应
	streamingError := func(err error, committed bool) {
		if err != nil && !committed {
			if use, ok := asUpstreamStatusError(err); ok {
				// 候选全败且最后失败为上游明确 4xx（如镜像站对 referrers 端点回 404）：
				// 透传上游状态码 + 响应头 + body（探测量级），docker 客户端对这类 registry
				// 语义错误自带容错，透传后 pull 可正常完成；网络/超时类失败仍走 502
				s.writeUpstreamStatus(w, r, use)
				return
			}
			s.writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if err != nil {
			if errors.Is(err, errClientDisconnected) {
				// 客户端断开终止（断开即停，正常运维事件而非失败）：连接已断，绝不再向
				// 客户端写任何字节（不 502 不 writeError——在已关闭的连接上继续写只会
				// 报错刷日志）；上游读取已终止、已接收字节数与半截保留状态已由流内部
				// Warn 留痕（clientGoneAbort），此处 Debug 简记即可。客户端将以断点续传
				// 或缓存重试接续。
				s.log.Debug("客户端已断开，本次响应终止（流内部已留痕，客户端将以断点续传或缓存重试）",
					labels.attrs(dockerLogSourceClient, "target", truncate(r.URL.Path, 300))...)
				return
			}
			if errors.Is(err, errResumeGraceAborted) {
				// 续传宽限终止（客户端断开且宽限到期仍无搭车订阅者，正常运维事件而非
				// 失败）：与 errClientDisconnected 同款低调处理——响应已 commit 且客户端
				// 已断，绝不再向客户端写任何字节（不 502 不 writeError）；宽限终止事件
				// 已由续传执行方 Info 留痕（半截保留、条目移除），此处 Debug 简记即可。
				// 客户端将以断点续传重试，从半截断点接着拉。
				s.log.Debug("blob 半截续传宽限终止，本次响应终止（流内部已留痕，客户端将以断点续传重试）",
					labels.attrs(dockerLogSourceResume, "target", truncate(r.URL.Path, 300))...)
				return
			}
			s.log.Warn("流式回源失败（响应头已写出，终止本次响应）",
				labels.attrs(dockerLogSourceClient, "target", truncate(r.URL.Path, 300), "err", err)...)
		}
	}

	// //////////////////  起点 >0 的 blob Range：本地切片 / 半截对齐  start  //////////////////
	// 本地已有完整内容就绝不再打上游：任意起点直接从缓存条目 body 切片回 206（「一次落盘、
	// 后续任意起点全秒回」）。只有半截前缀时按「起点与半截长度的关系」分流（见 client_range.go）：
	//   - 起点 <= 半截长度：复用既有续传管线（本地半截从起点回放 + 上游剩余区间续传拼接），
	//     对客户端回 206；执行方照常把完整 blob 写缓存，下次任意起点直接走上面的切片；
	//   - 起点 > 半截长度（中间缺一段，不能凭空补）：tryServeBlobResume 内部打 Warn 后返回
	//     false，落到下方旧版 206 透传；
	//   - 无半截 / 半截为空：同样返回 false，维持旧版 206 透传（语义与旧版完全一致）。
	// 本块必须在 !cacheable 的 BYPASS 分支之前：blobRangeLocal 的 bypassReason 是 blob-range
	// （不写缓存），但「读缓存」与「半截续传」仍然可用。
	if blobRangeLocal && disk != nil {
		// A) 正式缓存 HIT：条目 body 即完整 blob，任意起点本地切片回 206（不发上游）
		if e, ok := disk.Get(key); ok && e.Status == http.StatusOK && len(e.Body) > 0 {
			if mismatch, actual := s.entryDigestMismatch(target, e); mismatch {
				// 毒条目：删除后继续走下方未命中链路（同请求内回源自愈），绝不回放毒字节
				s.purgePoisonEntry(disk, key, target, eff.Rule, actual)
			} else {
				s.log.Info("缓存命中（HIT）并按客户端区间本地切片回 206（不发上游）",
					labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
						"file", disk.FilePath(key), "range_start", rangeSpec.start,
						"range_has_end", rangeSpec.hasEnd, "bytes", len(e.Body))...)
				s.serveEntryBlobRange(w, r, e, rangeSpec, "HIT", eff, start)
				return
			}
		}
		// B) 本地半截对齐：起点 <= 半截长度时走续传管线（前缀从起点切片回放 + 上游续传拼接）
		if streaming && s.tryServeBlobResume(w, r, key, target, eff, disk, start, finalize, streamingError, rangeSpec) {
			return
		}
	}
	// //////////////////  起点 >0 的 blob Range：本地切片 / 半截对齐  end  ////////////////////

	if !cacheable {
		// 不走缓存的决策留痕：Info 级输出原因与生效规则（含未命中任何规则的 global-default），
		// 让「为什么不写缓存文件」在日志里直接可见
		s.log.Info("GET 请求绕过缓存（BYPASS）", labels.attrs(dockerLogSourceClient,
			"rule", eff.Rule, "reason", bypassReason,
			"target", truncate(target, 300))...)
		// //////////////////  不走缓存：直接按生效配置回源  start  ////////////////////////////////////////////////
		if streaming {
			// docker 可变目标多源并发回源（tags 合并 / referrers 合并 / manifest 摘要共识，
			// 见 docker_multisource.go）：开关开启且条件满足时接管本次响应（含客户端断开
			// 的低调错误出口）；返回 false（条件不满足或全部源失败）退回下方既有顺序候选
			// 管线（继承其 4xx 透传 / 502 语义不变）
			if s.serveDockerMultiSource(w, r, target, eff, bypassReason == dockerMutableBypassReason, start) {
				return
			}
			// 路径目标 BYPASS（Range / no-cache / 关缓存等）同样流式透传：
			// docker 断点续传走 Range，上游回 206 视为成功并流式透传、不写缓存
			_, committed, err := s.fetchViaCandidatesStreaming(w, r, target, eff, "BYPASS", nil, start)
			streamingError(err, committed)
			return
		}
		e, err := s.fetchViaCandidates(r, target, eff)
		if err != nil {
			s.writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		s.serveEntry(w, r, e, "BYPASS", eff, start)
		return
		// //////////////////  不走缓存：直接按生效配置回源  end  ////////////////////////////////////////////////
	}

	// //////////////////  查缓存：命中直接回放  start  ////////////////////////////////////////////////
	if disk != nil {
		if e, ok := disk.Get(key); ok {
			if mismatch, actual := s.entryDigestMismatch(target, e); mismatch {
				// 毒条目：删除后按未命中继续（下方链路同请求内回源自愈），绝不回放毒字节
				s.purgePoisonEntry(disk, key, target, eff.Rule, actual)
			} else {
				// 缓存命中是用户最关心的决策结果之一，Info 级输出实际命中的磁盘文件路径
				s.log.Info("缓存命中（HIT）", labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
					"file", disk.FilePath(key), "bytes", len(e.Body), "ttl", ttlDisplay(e.TTL))...)
				s.serveEntry(w, r, e, "HIT", eff, start, zeroArgs...)
				return
			}
		}
	}
	s.log.Debug("缓存查询结果", labels.attrs(dockerLogSourceClient, "result", "MISS", "key", key, "dir", eff.CachePath)...)
	// //////////////////  查缓存：命中直接回放  end  ////////////////////////////////////////////////

	// //////////////////  在途预取层搭车检查  start  //////////////////////////////////////////////
	// 该 blob 正被预取流式下载时直接搭车（拼接已落盘前缀 + 实时增量回放，X-Cache: PREFETCH），
	// 不进下方 flight 等待——flightWaitLimit（10s）撑不住大 layer 的完整下载窗口，超限即
	// 自行回源 = 第二路完整下载且从 0 开始（见 prefetch_hub.go）；已 commit 后的订阅超时/
	// 失败在内部终止本次响应（docker 对该层以 Range 断点续传重试，206 透传不写缓存兜底
	// 「不从 0 重新下载」）。未命中 hub（无预取 / 条目不存在）则行为与旧版完全一致。
	// 进入本检查的请求必然是缓存可读（非 BYPASS）的无 Range 完整 GET——HEAD / 206 续传 /
	// no-cache 已被上方分支分流，天然不搭车。
	if streaming && s.tryServeFromPrefetchHub(w, r, key, target, eff, disk, start) {
		return
	}
	// //////////////////  在途预取层搭车检查  end  ////////////////////////////////////////////////

	// //////////////////  blob 半截续传检查  start  ////////////////////////////////////////////////
	// 正式缓存 MISS 且半截文件存在时走「续传执行方」：按候选轮询从断点 Range 续传，
	// 对客户端回一次完整 200（本地半截前缀回放 + 上游剩余实时拼接，docker 无需支持
	// Range、完全无感知；见 partial_resume.go）。并发合并：其余同 key 请求经既有
	// flightGroup / 搭车注册表共享同一路续传流，上游只一路下载。进入本检查的请求与
	// 搭车检查同约束：缓存可读、无 Range / no-cache 的完整 GET。开关关闭 / 非 blob /
	// 无半截文件时行为与旧版完全一致。注：带 Range 的请求维持既有 206 透传不写缓存
	//（不做本地拼接——docker 重试时既有 206 机制已兜底）。
	if streaming && s.tryServeBlobResume(w, r, key, target, eff, disk, start, finalize, streamingError) {
		return
	}
	// //////////////////  blob 半截续传检查  end  ////////////////////////////////////////////////

	// //////////////////  未命中：单飞回源并写缓存  start  ////////////////////////////////////////////////
	// 单飞 key 仍为缓存 key 输入串的 sha256：同一入站域名下同一目标必然命中同一规则/目录，
	// 必然落同一缓存实例（路径目标的 key 输入含入站 Host，跨入站域名天然不共享单飞）。
	// 流式回源（路径目标）的等待方有常量上限（flightWaitLimit=10s）：大 blob 回源可持续
	// 几十秒到几分钟，等待方超限后自行走候选回源（cache.Set 临时文件+rename 原子替换
	// 保证并发写缓存不互相污染），避免被首个执行方吊死到超过 docker 响应头超时；
	// 完整 URL 目标（web）保持无限期等待（行为不变）。
	waitLimit := time.Duration(0)
	if streaming {
		waitLimit = flightWaitLimit
	}
	e, executed, err := s.flight.DoLimit(key, waitLimit, func() (*cache.Entry, error) {
		if streaming {
			// 流式执行方：响应已在本次调用内 commit 并边读边写给客户端，
			// EOF 后 finalize 写缓存，完整 Entry 返回给等待方；
			// commit 后失败用 errStreamingCommitted 标记（响应头已固化，不得再 502）；
			// partial sink：blob 流式下载中断/失败时保留半截前缀（partial_resume.go）
			ent, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
			if ferr != nil && committed {
				return nil, fmt.Errorf("%w: %v", errStreamingCommitted, ferr)
			}
			return ent, ferr
		}
		ent, ferr := s.fetchViaCandidates(r, target, eff)
		if ferr != nil {
			return nil, ferr
		}
		finalize(ent)
		return ent, nil
	})
	switch {
	case errors.Is(err, errFlightWaitTimeout):
		// 等待同 key 单飞回源超限：自行走候选回源（不注册单飞，流式透传 + EOF 写缓存；
		// partial sink：blob 半截保留同执行方路径）
		s.log.Warn("等待同 key 单飞回源超限，自行走候选回源",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key, "wait", waitLimit.String(), "target", truncate(target, 300))...)
		_, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
		streamingError(ferr, committed)
	case errors.Is(err, errStreamingCommitted):
		// 本调用是流式执行方且 commit 后失败：底层原因已留痕（读上游失败=Error、客户端
		// 断开=Warn 运维事件），响应头已写出，终止本次响应即可，绝不能再 writeError
		//（断开场景客户端连接已断，同样无从写起）
	case err != nil:
		// 流式执行方/等待方共享到候选全败错误：最后失败为上游明确 4xx 时透传（仅流式
		// 路径目标，docker registry 语义；完整 URL（web）目标保持既有 502 行为不变）
		if streaming {
			if use, ok := asUpstreamStatusError(err); ok {
				s.writeUpstreamStatus(w, r, use)
				return
			}
		}
		s.writeError(w, http.StatusBadGateway, err.Error())
	case streaming && executed:
		// 本调用是流式执行方：响应头已 commit、body 已边读边写、完成日志已输出，
		// 无需再走 serveEntry 回放
	default:
		s.serveEntry(w, r, e, "MISS", eff, start)
	}
	// //////////////////  未命中：单飞回源并写缓存  end  ////////////////////////////////////////////////
}

// blobDigestMatches blob 内容寻址校验的默认实现：sha256(body) 与 digest（"sha256:<hex>"
// 形态）的 hex 全量比对。digest 不带 sha256: 前缀时无法本地校验，按一致放行（不拦截）。
func blobDigestMatches(body []byte, digest string) (string, bool) {
	hexdig := strings.TrimPrefix(digest, "sha256:")
	if hexdig == digest {
		return "", true // 非 sha256 digest：跳过校验（仅 sha256 算法可本地验证）
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	return actual, actual == hexdig
}

// entryDigestMismatch 缓存条目的 blob 摘要复核（HIT 侧兜底，与 finalizeFetchEntry 的
// 写入侧强校验同一校验器）：blob 目标条目在回放给客户端前必须再次通过 sha256 比对——
// 写入守卫部署之前落盘的历史条目无法追溯验证（2026-11-07 现场：镜像 18 个 blob 中
// 7 个为毒，连 1-5KB 的 config 小文件都中招，证明普通全量 GET 路径同样投毒过），
// 只有回放前复核才能拦住。返回 (是否不符, 实际摘要 hex)；非 blob 目标恒 false。
func (s *Server) entryDigestMismatch(target string, e *cache.Entry) (bool, string) {
	dgst, isBlob := parseDockerBlobTarget(target)
	if !isBlob {
		return false, ""
	}
	verify := s.blobDigestVerify
	if verify == nil {
		verify = blobDigestMatches
	}
	actual, ok := verify(e.Body, dgst)
	return !ok, actual
}

// purgePoisonEntry 毒条目处置：删除磁盘文件并留痕（Error）。删除失败仅 Warn——后台
// 清理不感知摘要，残留毒文件会在下次命中时再次被发现并重试删除；调用方随后按未命中
// 回源（同请求内自愈，绝不把毒字节回放给客户端）。
func (s *Server) purgePoisonEntry(disk *cache.Cache, key, target, rule, actual string) {
	disk.Remove(key)
	labels := parseDockerLogInfo(target)
	dgst, _ := parseDockerBlobTarget(target)
	s.log.Error("缓存条目内容摘要不符，已删除毒条目并按未命中回源（防毒缓存：HIT 侧复核）",
		labels.attrs(dockerLogSourceClient, "rule", rule, "key", key,
			"digest", dgst, "actual_digest", "sha256:"+actual,
			"file", disk.FilePath(key))...)
}

// finalizeFetchEntry 回源完成后的缓存写入与留痕（仅 GET，条件与日志字段与旧版一致）：
// 仅缓存满足成功判据的 200 响应（TTL 在写入时固化）；304 与「回源成功但未过判据」
// 分别留痕说明为什么这次没写缓存文件。logSource 为本路写缓存的日志来源（source 键，
// 四值枚举见 log_attrs.go）：客户端主路径传 client、预取传 prefetch、续传执行方传
// resume——同一函数被三条链路复用，按写者区分「异步下载」与「客户端请求下载」。
// 写盘成功后按条件尝试 docker 镜像 layer 异步预取（manifest 驱动，触发条件与失败降级
// 语义见 prefetch.go；条件不满足静默返回）。
//
// 返回值 cached：true = 已写入正式缓存（各项守卫全过且 disk.Set 成功）。续传执行方
// （partial_resume.go）以它决定「完成」广播与失败收场——拒绝入库时绝不能向搭车订阅者
// 广播 done（并发 MISS 会按 done 直接 HIT 到不存在/毒性的条目）。
//
// blob 摘要强校验（写缓存侧兜底，防毒缓存）：blob 按 digest 寻址、内容不可变，入库前
// 必须 sha256 全量比对——2026-11-07 现场：2f1083db 的毒半截（前缀混入异常镜像源的
// 错误字节）被拼接固化成正式缓存后，客户端每次 pull 都以「unexpected commit digest」
// 确定性失败（重试 10 次同一错误哈希）。流式中途不做逐块校验（GB 级成本不可接受，
// 见 partial_resume.go 边界说明），写缓存是最后的兜底闸口：三条写链路（client /
// prefetch / resume）全部经过这里。校验失败 = 毒内容：拒绝入库，且续传写入者
// （logSource=resume）的半截文件正是本次拼接的数据源，必须一并丢弃——否则每次续传
// 都撞同一毒前缀；client / prefetch 写入者的字节全部来自上游响应本身，与既有半截
// 无关，不动。
func (s *Server) finalizeFetchEntry(r *http.Request, key, target string, ent *cache.Entry, eff config.Options, disk *cache.Cache, logSource string) bool {
	if ent == nil {
		return false
	}
	labels := parseDockerLogInfo(target) // 日志统一标签（/v2/ 目标才有 image/digest/ref）
	if ent.Status == http.StatusOK && checkSuccess(ent, eff.SuccessCheck, http.MethodGet) {
		// HTML 页面防毒化（写缓存侧兜底，现场动机同 probePassed：2026-11-07
		// deepseek-harness:my——dockerpull.cn 对 registry 路径目标回 200 text/html 反爬页，
		// 旧版写入缓存后 TTL 666h 内所有重试全部命中毒缓存）。探测判据已在竞速侧拒绝
		// HTML 候选；这里是「绕过探针判据的直调写缓存路径」（预取 / 半截续传执行方）的
		// 兜底：registry 路径目标下合法 200 载荷只会是 manifest JSON / blob 二进制，
		// 绝不可能是 HTML；完整 URL 目标（web / github raw）HTML 是合法载荷，不受影响。
		poisonSkip := false
		if isPathTarget(target) && htmlPageContentType(ent.Header.Get("Content-Type")) {
			s.log.Warn("回源 200 但 Content-Type 为 HTML 页面，拒绝写缓存（防镜像站反爬页毒化缓存）",
				labels.attrs(logSource, "rule", eff.Rule, "target", truncate(target, 300),
					"content_type", ent.Header.Get("Content-Type"), "body_bytes", len(ent.Body))...)
			poisonSkip = true
		}
		if !poisonSkip {
			if dgst, isBlob := parseDockerBlobTarget(target); isBlob {
				verify := s.blobDigestVerify
				if verify == nil {
					verify = blobDigestMatches
				}
				if actual, ok := verify(ent.Body, dgst); !ok {
					s.log.Error("blob 回源内容摘要不符，拒绝写缓存（防毒缓存：上游返回了错误内容）",
						labels.attrs(logSource, "rule", eff.Rule, "digest", dgst,
							"actual_digest", "sha256:"+actual, "body_bytes", len(ent.Body),
							"target", truncate(target, 300))...)
					poisonSkip = true
				}
			}
		}
		if poisonSkip {
			// 毒内容绝不入库；续传写入者的毒半截一并丢弃（理由见上方函数注释）
			if logSource == dockerLogSourceResume && disk != nil {
				disk.RemovePart(key)
			}
			return false
		}
		ent.TTL = eff.CacheTTL // TTL 在写入时固化到条目（0 = 永不过期）
		if disk != nil {
			if serr := disk.Set(key, ent); serr != nil {
				s.log.Warn("写入缓存失败", labels.attrs(logSource, "key", key, "target", target, "err", serr)...)
				return false
			}
			// 写盘成功必须可见：规则名、缓存 key、实际落盘文件路径、body 大小与生效 TTL
			s.log.Info("写入缓存成功",
				labels.attrs(logSource, "rule", eff.Rule, "target", truncate(target, 300), "key", key,
					"file", disk.FilePath(key), "bytes", len(ent.Body), "ttl", ttlDisplay(ent.TTL))...)
			// blob 正式缓存条目已落盘：立即删除对应半截文件（其内容已完整入库，
			// 使命完成；非 blob 目标静默跳过，见 partial_resume.go）
			if _, isBlob := parseDockerBlobTarget(target); isBlob {
				disk.RemovePart(key)
			}
			// docker 镜像 layer 异步预取：manifest GET 回源成功且已写入缓存时触发
			//（触发条件在内部逐项判定，不满足静默返回；预取只写缓存，异步执行，
			// 绝不影响本次客户端响应）
			s.maybePrefetchImageLayers(r, target, ent, eff, disk)
			return true
		}
		return false
	}
	if ent.Status == http.StatusNotModified {
		// 回源 304（内容未变更）：条目无实体不可缓存，304 原样透传给客户端，
		// 客户端用本地缓存副本渲染，属成功语义而非失败（条件头已不透传，
		// 上游对无条件 GET 仍回 304 属异常/缓存副本有效兜底场景）
		s.log.Info("回源 304 未变更，不写缓存（客户端缓存副本有效）",
			labels.attrs(logSource, "rule", eff.Rule, "status", ent.Status, "target", truncate(target, 300))...)
		return false
	}
	// 回源成功但不满足缓存判据：同样留痕，说明为什么这次没写缓存文件
	s.log.Info("回源成功但不写缓存（未过成功判据或非 200）",
		labels.attrs(logSource, "rule", eff.Rule, "status", ent.Status, "check", eff.SuccessCheck,
			"body_bytes", len(ent.Body), "target", truncate(target, 300))...)
	return false
}

// handleHead 处理 HEAD 请求（docker registry 用 HEAD 探测 manifest/blob 元信息）：
// 与 GET 一致统一走候选轮询管线（完整 URL 与路径目标行为相同）——
// 先查缓存（复用 GET 写下的条目回放响应头，无 body）；未命中按候选轮询回源
// （请求方法保持 HEAD，让 HEAD 探测同样打到镜像站）；HEAD 响应无 body，一律不写缓存。
// 未配置 url-redirect（候选为空）时由 fetchViaCandidates 回退 fetchDirect 直连，与旧版一致。
func (s *Server) handleHead(w http.ResponseWriter, r *http.Request, target string, fullURL bool, eff config.Options, start time.Time) {
	key := cache.Key(cacheKeyInput(r, target, fullURL))
	// docker 日志统一标签：HEAD 探测同属客户端主路径（source=client，见 log_attrs.go）
	labels := parseDockerLogInfo(target)
	// 缓存不可读（未启用 / no-cache / 实例获取失败）时回源结果按 BYPASS 标注，与 GET 的语义对齐
	cacheStatus := "MISS"
	// headMutableBypass 本次 HEAD 是否为 docker 可变目标 BYPASS：仅该类请求启用多源并发
	// 回源（开关与边界见 docker_multisource.go），其余 BYPASS / MISS 维持既有顺序管线
	headMutableBypass := false

	// //////////////////  查缓存：命中直接回放响应头  start  ////////////////////////////////////////////////
	// docker 可变目标（tag manifest / referrers / tags list，判定见 docker_mutable.go）：
	// HEAD 不读缓存——HEAD manifests/latest 复用 GET 写下的旧条目会回放过期
	// Docker-Content-Digest，docker 直接判「Image is up to date」永不更新（2026-11-07 现场）
	if dockerMutableTarget(target) {
		s.log.Info("HEAD 请求绕过缓存（BYPASS，docker 可变目标不读缓存）", labels.attrs(dockerLogSourceClient,
			"rule", eff.Rule, "reason", dockerMutableBypassReason, "target", truncate(target, 300))...)
		cacheStatus = "BYPASS"
		headMutableBypass = true
	} else if disk := s.readableCache(r, eff); disk != nil {
		if e, ok := disk.Get(key); ok {
			if mismatch, actual := s.entryDigestMismatch(target, e); mismatch {
				// 毒条目：删除后按未命中回源（HEAD 探测同样绝不放行毒条目——错误的
				// Content-Length / Docker-Content-Digest 会把 docker 引向错误决策）
				s.purgePoisonEntry(disk, key, target, eff.Rule, actual)
			} else {
				// 缓存命中是用户最关心的决策结果之一，Info 级输出实际命中的磁盘文件路径；
				// serveEntry 的 HEAD 分支只回放状态码与响应头（含 Content-Length），不写 body
				s.log.Info("缓存命中（HIT）", labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
					"file", disk.FilePath(key), "bytes", len(e.Body), "ttl", ttlDisplay(e.TTL))...)
				s.serveEntry(w, r, e, "HIT", eff, start)
				return
			}
		}
		s.log.Debug("缓存查询结果", labels.attrs(dockerLogSourceClient, "result", "MISS", "key", key, "dir", eff.CachePath)...)
	} else {
		cacheStatus = "BYPASS"
	}
	// //////////////////  查缓存：命中直接回放响应头  end  ////////////////////////////////////////////////

	// //////////////////  docker 可变目标多源并发回源  start  ////////////////////////////////////////////////
	// 开关开启时并发请求全部候选源并按端点类型合并/共识（tags 并集 / referrers 去重 /
	// manifest 摘要共识，HEAD 与 GET 同策略保证 pull 流程内判定一致，见 docker_multisource.go）；
	// 返回 false（条件不满足或全部源失败）退回下方既有顺序候选管线（4xx 透传 / 502 语义不变）
	if s.serveDockerMultiSource(w, r, target, eff, headMutableBypass, start) {
		return
	}
	// //////////////////  docker 可变目标多源并发回源  end  //////////////////////////////////////////////////

	// //////////////////  未命中：候选轮询回源（HEAD 不写缓存）  start  ////////////////////////////////////////////////
	e, err := s.fetchViaCandidates(r, target, eff)
	if err != nil {
		// 候选全败且最后失败为上游明确 4xx：透传上游状态码与响应（docker 对 HEAD 探测的
		// registry 语义错误同样能理解，如 blob 不存在 404）；网络/超时类失败仍 502
		if use, ok := asUpstreamStatusError(err); ok {
			s.writeUpstreamStatus(w, r, use)
			return
		}
		s.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.serveEntry(w, r, e, cacheStatus, eff, start)
	// //////////////////  未命中：候选轮询回源（HEAD 不写缓存）  end  ////////////////////////////////////////////////
}

// readableCache 返回本请求可用于「读缓存」的缓存实例（HEAD 复用 GET 写下的条目）：
// 缓存未启用、客户端 Cache-Control: no-cache 要求强制回源、实例获取失败时返回 nil（跳过缓存读取）。
func (s *Server) readableCache(r *http.Request, eff config.Options) *cache.Cache {
	if !eff.CacheEnabled || clientNoCache(r) {
		return nil
	}
	c, err := s.cacheFor(eff)
	if err != nil {
		s.log.Warn("获取缓存实例失败，本次请求跳过缓存读取", "dir", eff.CachePath, "err", err)
		return nil
	}
	return c
}

// cacheKeyInput 返回缓存 key 的输入串（cache.Key 对其取 sha256）：
//   - 完整 URL 目标：target 原文（与旧版一致，兼容已有缓存文件）；
//   - 路径目标：「http://<入站Host>/<路径目标>」——入站 Host 必须参与 key 输入，
//     避免不同入站域名的同路径缓存互相污染（与域名规则匹配串同构）。
func cacheKeyInput(r *http.Request, target string, fullURL bool) string {
	if fullURL {
		return target
	}
	return config.BuildMatchStr(r.Host, target)
}

// cacheFor 返回该请求生效的缓存实例（按生效目录向管理器获取，同目录复用）；
// caches 为 nil（禁用缓存）时返回 (nil, nil)。目录创建失败时返回错误，由调用方降级处理。
func (s *Server) cacheFor(eff config.Options) (*cache.Cache, error) {
	if s.caches == nil {
		return nil, nil
	}
	return s.caches.Acquire(eff.CachePath, eff.CacheCleanInterval)
}

// fetchViaCandidates 回源获取目标内容（缓冲模式：整包读进内存构造 Entry，供 HEAD /
// 完整 URL 目标（web）回放；GET 路径目标的流式透传见 fetchViaCandidatesStreaming）：
//  1. 展开生效的 url-redirect 模板为候选 URL 列表（占位符见 RequestVars：$1 与请求级变量）；
//     无模板且目标为完整 URL 时直接请求原始 URL，路径目标无模板直接报错
//     （路径目标必须由规则模板拼出回源地址，没有可直连的原始 URL）；
//  2. 按 round-robin 轮换的起始下标在候选上执行「探测提速管线」（见 candidates.go）：
//     probe-timeout 探测预算 + 硬失败快切（403 / token dance 后仍 401，跳过 item 重试）
//     + 阶梯并发竞速（hedge，第一个通过 success-check 的候选胜出，其余取消）；
//  3. 一轮 = 所有候选（含各自 item 重试）都失败；整轮全败后可按
//     url-redirect-all-error-retry 整体重来最多 N 轮（总轮数 = 1+N），
//     同一请求内的重试轮沿用同一候选顺序（同一 start 下标）；
//  4. 所有轮次都失败时，完整 URL 目标按 fallback-direct 决定是否回退直连原始 URL；
//     路径目标没有完整原始 URL 可直连，fallback-direct 不适用，一律直接报错（无直连回退）。
func (s *Server) fetchViaCandidates(r *http.Request, target string, eff config.Options) (*cache.Entry, error) {
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	if len(candidates) == 0 {
		if !isFullURLTarget(target) {
			// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，此处兜底绝不直连
			return nil, errors.New("路径目标请求未配置 url-redirect，无法回源（路径目标必须由命中的域名规则提供 url-redirect）")
		}
		// 未配置 url-redirect：直接回源原始 URL（结果原样透传，不套用成功判据）
		s.log.Debug("无 url-redirect 候选，直连回源", "target", target)
		return s.fetchDirect(r, target, eff)
	}

	// //////////////////  探测提速管线竞速选定候选  start  ///////////////////////////////////////
	res, err := s.selectCandidate(r, candidates, eff, target)
	if err != nil {
		return s.candidatesExhaustedFallback(r, target, eff, err)
	}
	// //////////////////  探测提速管线竞速选定候选  end  /////////////////////////////////////////

	// 胜出候选排干为完整 Entry（缓存条目/回放语义与旧版一致）
	e, derr := s.drainProbe(res)
	if derr != nil {
		return nil, derr
	}
	e.Source = res.url
	return e, nil
}

// candidatesExhaustedFallback 候选全部失败后的既有出口语义（行为不变）：
//   - 路径目标没有完整原始 URL 可直连：fallback-direct 不适用，直接报错；
//   - 完整 URL 目标按 fallback-direct 决定是否回退直连原始 URL（Source=direct）。
func (s *Server) candidatesExhaustedFallback(r *http.Request, target string, eff config.Options, lastErr error) (*cache.Entry, error) {
	// //////////////////  全部轮次失败：按配置回退直连  start  /////////////////////////////////////////
	if !isFullURLTarget(target) {
		// 路径目标没有完整原始 URL 可直连：fallback-direct 不适用，候选全败直接报错
		//（错误信息注明路径目标无直连回退）。本兜底承接 HEAD 路径目标（docker HEAD
		// 探测）等客户端主路径：source=client
		s.log.Warn("url-redirect 全部候选失败，路径目标无直连回退（fallback-direct 不适用）",
			parseDockerLogInfo(target).attrs(dockerLogSourceClient,
				"target", truncate(target, 300), "last_error", lastErr)...)
		return nil, fmt.Errorf("全部 url-redirect 候选失败，路径目标无直连回退（无完整原始 URL 可直连）: %w", lastErr)
	}
	if eff.FallbackDirect {
		s.log.Warn("url-redirect 全部候选失败，回退直连原始 URL", "target", target, "last_error", lastErr)
		e, err := s.fetchDirect(r, target, eff)
		if err != nil {
			return nil, fmt.Errorf("全部候选失败且直连失败: %w（最后候选错误: %v）", err, lastErr)
		}
		return e, nil
	}
	return nil, fmt.Errorf("全部 url-redirect 候选失败（fallback-direct 已关闭）: %w", lastErr)
	// //////////////////  全部轮次失败：按配置回退直连  end  ///////////////////////////////////////////
}

// max0 返回 n 与 0 的较大值（重试次数防御：配置校验已拒绝负数，此处兜底避免负数
// 导致循环异常）。
func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

// fetchDirect 直接请求原始 URL：保留客户端方法，非 GET/HEAD 时透传请求体。
func (s *Server) fetchDirect(r *http.Request, target string, eff config.Options) (*cache.Entry, error) {
	var body io.Reader
	switch r.Method {
	case http.MethodGet, http.MethodHead:
	default:
		body = r.Body // POST/PUT/PATCH 等透传原始请求体
	}
	e, err := s.doRequest(r, r.Method, target, body, eff)
	if err != nil {
		return nil, err
	}
	e.Source = "direct"
	return e, nil
}

// applyForwardRequestHeaders 把入站请求头按白名单拷贝到回源请求（候选与直连共用）：
// 条件请求头（If-Match 等）不在白名单（见 forwardRequestHeaders 注释），Range 保持透传；
// User-Agent 缺省补 proxy-cache/<Version>。
func applyForwardRequestHeaders(req *http.Request, r *http.Request) {
	for _, h := range forwardRequestHeaders {
		if vv := r.Header.Values(h); len(vv) > 0 {
			req.Header[h] = vv
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "proxy-cache/"+Version)
	}
}

// doRequest 向 urlStr 发起一次上游尝试（整包缓冲模式，供直连回退 / 完整 URL 目标 /
// 非 GET/HEAD 方法使用；候选管线的探测式尝试见 probe.go 的 probeCandidateAttempt）。
// 所有上游请求（直连与 url-redirect 候选）统一走这里，并应用生效配置的 http-proxy
// 与三个独立超时：
//   - http-timeout：本次尝试的总超时（建连 + 响应头 + 完整读 body + token dance 全程），
//     每次尝试（含候选重试）独立计时；未配置（0）= 不限制；
//   - read-timeout：无新增数据看门狗（见 progressWatchdog），响应头到达、每读到一段
//     body 字节都重置计时器，窗口内无任何新数据才取消本次尝试（错误含 "read-timeout"）；
//   - connect-timeout：建连超时，按 (代理, connect-timeout) 组合选择 Transport。
//
// GET/HEAD 回源遇上游 401 + WWW-Authenticate: Bearer 挑战时，自动完成 Docker Registry
// 匿名 token dance（401 → 取 token → 带 Bearer 重试一次，见 dockerAuthDance）；
// 其余方法（fetchDirect 透传 r.Body 的那类）不做 dance。
func (s *Server) doRequest(r *http.Request, method, urlStr string, body io.Reader, eff config.Options) (*cache.Entry, error) {
	// //////////////////  构造尝试级 ctx（http-timeout 总超时）  start  ///////////////////////
	// 与客户端连接解耦（WithoutCancel）：首个客户端断开也不中断回源，
	// 保证缓存填充与单飞等待方仍能拿到完整结果。
	// http-timeout 按「本次尝试」独立计时：候选重试/整轮重试时每次都会重新进入本函数，
	// 因此每次尝试都拿到全新的总超时窗口。0 = 未配置，不限制。
	attemptParent := context.WithoutCancel(r.Context())
	if eff.HTTPTimeout > 0 {
		var cancelTimeout context.CancelFunc
		attemptParent, cancelTimeout = context.WithTimeout(attemptParent, eff.HTTPTimeout)
		defer cancelTimeout()
	}
	// //////////////////  构造尝试级 ctx（http-timeout 总超时）  end  /////////////////////////

	// //////////////////  挂载 read-timeout 看门狗  start  /////////////////////////////////////
	// 看门狗与总超时并存、先到先取消：慢速但持续有数据时只有总超时能砍（读不进总窗口），
	// 窗口内无任何新数据时看门狗先砍（cancel 本次尝试的 ctx）
	wd := newProgressWatchdog(attemptParent, eff.ReadTimeout)
	defer wd.Stop()
	// //////////////////  挂载 read-timeout 看门狗  end  ///////////////////////////////////////

	// //////////////////  构造上游请求  start  ////////////////////////////////////////////////
	req, err := http.NewRequestWithContext(wd.ctx, method, urlStr, body)
	if err != nil {
		return nil, fmt.Errorf("构造上游请求失败（%s）: %w", urlStr, err)
	}
	applyForwardRequestHeaders(req, r)
	// //////////////////  构造上游请求  end  //////////////////////////////////////////////////

	// //////////////////  发起请求并读取响应  start  ////////////////////////////////////////////
	// 直连回退/直连透传路径的 insecure 按「规则 ?? 全局」生效值（eff.InsecureSkipVerify）
	// 执行：30x 重定向目标与下方 token dance 共用同一 Client，一并跳过证书校验
	resp, err := s.client(eff.HTTPProxy, eff.ConnectTimeout, eff.InsecureSkipVerify).Do(req)
	if err != nil {
		// 看门狗已触发时把错误标注为 read-timeout（与 http-timeout 到期的 deadline exceeded 区分）
		return nil, fmt.Errorf("请求上游失败（%s）: %w", urlStr, wd.wrapErr(err))
	}
	wd.reset() // 响应头到达 = 一次进度（read-timeout 计时器重置）
	defer resp.Body.Close()
	// HEAD 响应天然无 body（不读 body 语义）：跳过读取，Body 保持为空，
	// 状态码与响应头（含 Content-Length / Docker-Content-Digest 等）照常透传
	var buf []byte
	if method != http.MethodHead {
		// 经 progressReader 读取：每收到 n>0 字节都算进度并重置看门狗，
		// 慢速但持续传输的大 blob 不会被 read-timeout 误砍
		buf, err = io.ReadAll(&progressReader{r: resp.Body, wd: wd})
		if err != nil {
			return nil, fmt.Errorf("读取上游响应失败（%s）: %w", urlStr, wd.wrapErr(err))
		}
	}
	// 深拷贝响应头，避免与连接复用的上游对象共享底层数据
	hdr := make(http.Header, len(resp.Header))
	for k, vv := range resp.Header {
		hdr[k] = append([]string(nil), vv...)
	}
	// //////////////////  发起请求并读取响应  end  ////////////////////////////////////////////

	entry := &cache.Entry{
		Status:   resp.StatusCode,
		Header:   hdr,
		Body:     buf,
		StoredAt: time.Now(),
	}

	// //////////////////  Docker Registry 401 匿名 token dance  start  ///////////////////////////
	// 镜像站（docker.m.daocloud.io / docker.1ms.run / hub.rat.dev 等）对匿名 GET/HEAD 的
	// blob/manifest 常回 401 + WWW-Authenticate: Bearer 挑战：docker 客户端直连时会自动
	// 「按 challenge 去 realm 匿名取 token → 带 Authorization: Bearer 重试」，这里等价补齐，
	// 否则候选裸请求全被 401 拒绝、整轮轮询全败最终 502。触发条件（GET/HEAD 回源 + 上游
	// 401 + Bearer 挑战）与失败兜底（沿用原始 401 响应）见 dockerAuthDance；
	// 每个 doRequest 只 dance 一次，不循环（重试响应即使仍 401 也原样返回）。
	// dance 全程共用本次尝试的看门狗（wd.ctx 受 http-timeout 与 read-timeout 共同约束）。
	if resp.StatusCode == http.StatusUnauthorized && (method == http.MethodGet || method == http.MethodHead) {
		if e2, ok := s.dockerAuthDance(wd, s.client(eff.HTTPProxy, eff.ConnectTimeout, eff.InsecureSkipVerify), req, resp, urlStr, nil); ok { // 无候选上下文（直连回退），无凭据可用，维持匿名 dance
			entry = e2 // 重试响应作为最终响应，继续既有的返回 Entry 语义
		}
	}
	// //////////////////  Docker Registry 401 匿名 token dance  end  /////////////////////////////

	return entry, nil
}

// //////////////////  Docker Registry 匿名 token dance（401 → 取 token → 带 Bearer 重试）  //////

// dockerTokenDefaultTTL 挑战响应未提供有效 expires_in 时的匿名 token 缺省有效期。
const dockerTokenDefaultTTL = 60 * time.Second

// dockerTokenBodyLimit 匿名 token 响应 body 的读取上限（token JSON 只有几百字节，
// 限制仅为防异常端点返回超大响应拖爆内存）。
const dockerTokenBodyLimit = 1 << 20

// accountAttr 返回 dance 凭据的日志展示值（auth 为 nil 时输出「匿名」）：凭据 dance
// 与匿名 dance 共用日志键，account 字段区分两类 token 来源（密码绝不进日志）。
func accountAttr(auth *config.CandidateAuth) string {
	if auth == nil {
		return "匿名"
	}
	return auth.Username
}

// dockerTokenEntry 是一条进程内缓存的匿名 registry token。
type dockerTokenEntry struct {
	// token 匿名 Bearer token 原文（仅内存持有，不落盘、不打完整日志）。
	token string
	// expiresAt 过期时间点：写入时按 expires_in（缺省 60s）固化。
	expiresAt time.Time
}

// bearerChallenge 是 WWW-Authenticate: Bearer 挑战中本服务用到的参数。
type bearerChallenge struct {
	// realm token 签发端点（必填，缺失则不 dance）。
	realm string
	// service 目标 service（如 registry.docker.io），挑战缺省时为空串。
	service string
	// scope 权限范围（如 repository:<name>:pull），挑战缺省时为空串。
	scope string
}

// dockerAuthDance 尝试 Docker Registry 匿名 token dance（401 → 取 token → 带 Bearer 重试一次）。
// 调用方（doRequest）已保证 method 为 GET/HEAD 且首个响应状态码为 401；此处再要求
// WWW-Authenticate 含可解析的 Bearer 挑战（scheme 大小写不敏感且带 realm），否则放弃。
// 整包缓冲模式：dance 请求/响应阶段见 dockerAuthDanceRetry（探测式尝试复用同一核心），
// 这里在拿到重试响应后按既有语义读完整 body 并构建 Entry 返回：
//   - 重试响应按既有语义（深拷贝响应头 + 读 body）构建为 Entry 返回（ok=true）；
//     重试即使仍 401 也按该响应原样返回，不再 dance；
//   - 挑战不可解析 / 取 token / 构造重试 / 重试失败时返回 ok=false，调用方沿用原始
//     401 entry（行为与未实现 dance 的版本一致，绝不因 dance 引入新的失败路径）；
//     看门狗触发（read-timeout）时失败日志经 wd.wrapErr 标注，便于日志辨认。
func (s *Server) dockerAuthDance(wd *progressWatchdog, cl *http.Client, orig *http.Request, resp *http.Response, urlStr string, auth *config.CandidateAuth) (*cache.Entry, bool) {
	resp2, _, ok := s.dockerAuthDanceRetry(wd, cl, orig, resp, urlStr, auth)
	if !ok {
		return nil, false
	}
	defer resp2.Body.Close()

	// 重试响应按既有语义读取：HEAD 天然无 body 跳过，其余读全文（经 progressReader
	// 每读到数据都重置看门狗）；读取失败按重试失败兜底（看门狗触发时标注 read-timeout）
	var buf []byte
	var err error
	if orig.Method != http.MethodHead {
		buf, err = io.ReadAll(&progressReader{r: resp2.Body, wd: wd})
		if err != nil {
			s.log.Warn("读取匿名 token 重试响应失败，按原始 401 响应返回",
				"url", truncate(urlStr, 300), "err", wd.wrapErr(err))
			return nil, false
		}
	}
	hdr := make(http.Header, len(resp2.Header))
	for k, vv := range resp2.Header {
		hdr[k] = append([]string(nil), vv...)
	}
	return &cache.Entry{
		Status:   resp2.StatusCode,
		Header:   hdr,
		Body:     buf,
		StoredAt: time.Now(),
	}, true
}

// dockerAuthDanceRetry 是 token dance 的请求/响应阶段核心（缓冲与探测式尝试共用）：
// 解析挑战 → 排干并关闭首个 401 响应 → 去 realm 匿名取 token（进程内缓存优先）→
// 复制原上游请求删除 Authorization 后改为 Bearer 重试一次，重试响应头可用即返回
// （body 未读，由调用方决定读完整（缓冲）还是只读首块（探测））。
// 返回值：
//   - resp2：带 token 重试的响应（ok=true 时非 nil，由调用方负责关闭）；
//   - consumed：首个 401 响应是否已被排干并关闭（挑战可解析即 true——此后原响应
//     不再可用，调用方失败兜底时须把其 body 标记为无剩余数据）；
//   - ok：是否拿到了带 token 重试的响应。
//
// 各阶段失败均按既有语义返回 ok=false（调用方沿用原始 401 响应，绝不因 dance 引入
// 新的失败路径）；请求 ctx 使用本次尝试的看门狗 ctx（wd.ctx），取 token 与带 token
// 重试同样受 http-timeout / 探测预算约束，且各阶段响应头/数据到达都会重置 read-timeout。
func (s *Server) dockerAuthDanceRetry(wd *progressWatchdog, cl *http.Client, orig *http.Request, resp *http.Response, urlStr string, auth *config.CandidateAuth) (resp2 *http.Response, consumed, ok bool) {
	// //////////////////  解析 Bearer 挑战  start  //////////////////////////////////////////////
	var ch *bearerChallenge
	for _, v := range resp.Header.Values("WWW-Authenticate") {
		if c := parseBearerChallenge(v); c != nil {
			ch = c
			break
		}
	}
	if ch == nil {
		s.log.Debug("上游 401 无可解析的 Bearer 挑战，跳过匿名 token dance",
			"url", truncate(urlStr, 300),
			"www_authenticate", truncate(strings.Join(resp.Header.Values("WWW-Authenticate"), ", "), 300))
		return nil, false, false
	}
	// //////////////////  解析 Bearer 挑战  end  ////////////////////////////////////////////////

	// 排干首个 401 响应 body 并关闭：dance 触发后原响应不再复用，不排干会占住连接造成泄漏
	//（排干同样经 progressReader：读到数据即向看门狗报告进度）
	_, _ = io.Copy(io.Discard, &progressReader{r: resp.Body, wd: wd}) // body 已废弃，排干错误无补救动作
	_ = resp.Body.Close()

	// dance 触发留痕：带凭据时输出账号（密码绝不进日志），匿名时 account 留空
	if auth != nil {
		s.log.Info("触发 Docker Registry token dance（带凭据）",
			"url", truncate(urlStr, 300), "realm", ch.realm, "service", ch.service,
			"scope", ch.scope, "account", auth.Username)
	} else {
		s.log.Info("触发 Docker Registry 匿名 token dance",
			"url", truncate(urlStr, 300), "realm", ch.realm, "service", ch.service, "scope", ch.scope)
	}

	// //////////////////  取匿名 token（进程内缓存优先）  start  ///////////////////////////////
	token, err := s.dockerToken(wd, cl, ch, auth)
	if err != nil {
		s.log.Warn("获取 Docker Registry 匿名 token 失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "realm", ch.realm, "err", wd.wrapErr(err))
		return nil, true, false
	}
	// //////////////////  取匿名 token（进程内缓存优先）  end  ///////////////////////////////////

	// //////////////////  带 token 重试一次  start  /////////////////////////////////////////////
	// 复制原请求（同 method/URL），头沿用原上游请求头：删除客户端 Authorization 后改为
	// Bearer <token>（GET/HEAD 回源 body 恒为 nil，无需透传请求体）
	req, err := http.NewRequestWithContext(wd.ctx, orig.Method, urlStr, nil)
	if err != nil {
		s.log.Warn("构造匿名 token 重试请求失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "err", err)
		return nil, true, false
	}
	req.Header = orig.Header.Clone()
	req.Header.Del("Authorization")
	req.Header.Set("Authorization", "Bearer "+token)

	resp2, err = cl.Do(req)
	if err != nil {
		s.log.Warn("带匿名 token 重试上游请求失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "token", maskToken(token), "err", wd.wrapErr(err))
		return nil, true, false
	}
	wd.reset() // 重试响应头到达 = 一次进度（read-timeout 计时器重置）

	// 重试结果留痕（Info）：状态码 + 脱敏 token（绝不输出完整 token 值）
	s.log.Info("带匿名 token 重试完成", "url", truncate(urlStr, 300),
		"status", resp2.StatusCode, "token", maskToken(token))
	return resp2, true, true
	// //////////////////  带 token 重试一次  end  /////////////////////////////////////////////
}

// dockerToken 返回挑战对应的匿名 token：优先命中进程内缓存（key = realm|service|scope，
// 未过期直接复用，避免每个 blob 层都打一次 auth 端点），未命中或已过期才请求 realm
// 换取新 token 并写回缓存（TTL = expires_in，>0 才用，否则缺省 60s）。
// 请求 ctx 使用本次尝试的看门狗 ctx（wd.ctx）。
func (s *Server) dockerToken(wd *progressWatchdog, cl *http.Client, ch *bearerChallenge, auth *config.CandidateAuth) (string, error) {
	// 缓存 key 必须区分凭据身份：同一 realm/service/scope 下匿名与不同账号的 token
	// 权限不同，绝不能互相复用；密码不进 key 原文（内存键表不落盘、不打日志），
	// 只取 sha256 前 16 hex（密码轮换后旧 token 键自然失配，不会误用）
	key := ch.realm + "|" + ch.service + "|" + ch.scope
	if auth != nil {
		sum := sha256.Sum256([]byte(auth.Password))
		key += "|u:" + auth.Username + "|p:" + hex.EncodeToString(sum[:])[:16]
	}
	now := time.Now()

	// 缓存命中（未过期）：Debug 留痕即可
	s.dockerTokenMu.Lock()
	if ent, ok := s.dockerTokens[key]; ok && now.Before(ent.expiresAt) {
		s.dockerTokenMu.Unlock()
		s.log.Debug("Docker Registry token 缓存命中（匿名/带凭据按 account 区分）",
			"realm", ch.realm, "service", ch.service, "scope", ch.scope,
			"account", accountAttr(auth),
			"token", maskToken(ent.token),
			"expires_in", ent.expiresAt.Sub(now).Round(time.Second).String())
		return ent.token, nil
	}
	s.dockerTokenMu.Unlock()

	token, ttl, err := fetchDockerToken(wd, cl, ch, auth)
	if err != nil {
		return "", err
	}
	s.dockerTokenMu.Lock()
	if s.dockerTokens == nil { // 防御：绕过 New 构造的 Server 也能安全写入
		s.dockerTokens = make(map[string]dockerTokenEntry)
	}
	s.dockerTokens[key] = dockerTokenEntry{token: token, expiresAt: now.Add(ttl)}
	s.dockerTokenMu.Unlock()
	return token, nil
}

// fetchDockerToken 请求 realm 匿名换取 token（不带进程内缓存，由 dockerToken 负责缓存）：
//   - token URL = realm + service/scope 查询参数：仅附带挑战里存在的参数；与 realm 自带
//     query 合并，已有键不覆盖；
//   - 请求头仅 Accept: application/json 与固定 User-Agent（proxy-cache/<Version>），
//     绝不携带客户端的 Authorization；
//   - 请求 ctx 使用本次尝试的看门狗 ctx（wd.ctx）：token 响应头到达与 body 数据
//     同样向看门狗报告进度（read-timeout 触发时错误标注 read-timeout）；
//   - 响应 JSON 优先取 "token" 字段、为空再取 "access_token"，两者皆空视为失败；
//     "expires_in"（秒，整数，>0 才生效）作为缓存 TTL，缺省 60s。
func fetchDockerToken(wd *progressWatchdog, cl *http.Client, ch *bearerChallenge, auth *config.CandidateAuth) (string, time.Duration, error) {
	ctx := wd.ctx // 受本次尝试的 http-timeout 与 read-timeout 共同约束
	u, err := url.Parse(ch.realm)
	if err != nil {
		return "", 0, fmt.Errorf("解析 token realm %q 失败: %w", ch.realm, err)
	}
	q := u.Query()
	if ch.service != "" && q.Get("service") == "" {
		q.Set("service", ch.service)
	}
	if ch.scope != "" && q.Get("scope") == "" {
		q.Set("scope", ch.scope)
	}
	// 带凭据 dance：追加 account 参数（Docker Hub token 端点约定，realm 自带同键不覆盖）
	if auth != nil && q.Get("account") == "" {
		q.Set("account", auth.Username)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, fmt.Errorf("构造 token 请求失败（%s）: %w", u.String(), err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "proxy-cache/"+Version)
	// 带凭据 dance：Basic base64(user:password) 换登录态 token（匿名 dance 不带此头，
	// 与既有「token 请求绝不携带客户端 Authorization」语义不冲突——这是代理自身配置
	// 的候选凭据，不是客户端凭据）
	if auth != nil {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString(
			[]byte(auth.Username+":"+auth.Password)))
	}

	resp, err := cl.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("请求 token 端点失败（%s）: %w", u.String(), wd.wrapErr(err))
	}
	defer resp.Body.Close()
	wd.reset() // token 响应头到达 = 一次进度（read-timeout 计时器重置）
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token 端点返回异常状态码 %d（%s）", resp.StatusCode, u.String())
	}
	body, err := io.ReadAll(io.LimitReader(&progressReader{r: resp.Body, wd: wd}, dockerTokenBodyLimit))
	if err != nil {
		return "", 0, fmt.Errorf("读取 token 响应失败（%s）: %w", u.String(), wd.wrapErr(err))
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", 0, fmt.Errorf("解析 token 响应 JSON 失败（%s）: %w", u.String(), err)
	}
	token := tok.Token
	if token == "" {
		token = tok.AccessToken
	}
	if token == "" {
		return "", 0, fmt.Errorf("token 响应缺少 token/access_token 字段（%s）", u.String())
	}
	ttl := dockerTokenDefaultTTL
	if tok.ExpiresIn > 0 {
		ttl = time.Duration(tok.ExpiresIn) * time.Second
	}
	return token, ttl, nil
}

// parseBearerChallenge 解析一条 WWW-Authenticate 挑战头为 Bearer 挑战参数：
//   - scheme 与参数以首个空白分隔，scheme 大小写不敏感且必须为 Bearer；
//   - 参数形如 key=value 或 key="value"（双引号包裹时剥离），参数名大小写不敏感；
//     取 realm / service / scope 三个参数（realm 必填，缺失或非 Bearer scheme 返回 nil，不 dance）；
//   - 逗号分隔参数，双引号内的逗号不参与分隔（scope 等值内可能含逗号）。
func parseBearerChallenge(header string) *bearerChallenge {
	scheme, params, _ := strings.Cut(strings.TrimSpace(header), " ")
	if !strings.EqualFold(strings.TrimSpace(scheme), "Bearer") {
		return nil
	}
	ch := &bearerChallenge{}
	for _, part := range splitChallengeParams(params) {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		v = strings.Trim(strings.TrimSpace(v), `"`)
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "realm":
			ch.realm = v
		case "service":
			ch.service = v
		case "scope":
			ch.scope = v
		}
	}
	if ch.realm == "" {
		return nil
	}
	return ch
}

// splitChallengeParams 按逗号拆分挑战参数串：双引号内的逗号不参与拆分，
// 保留引号原文（引号剥离由 parseBearerChallenge 统一处理）。
func splitChallengeParams(s string) []string {
	var (
		parts []string
		b     strings.Builder
		inQuo bool
	)
	for _, c := range s {
		switch {
		case c == '"':
			inQuo = !inQuo
			b.WriteRune(c)
		case c == ',' && !inQuo:
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteRune(c)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

// maskToken 脱敏展示匿名 token：仅输出前 8 字符与总长度，避免日志泄露完整凭证。
func maskToken(tok string) string {
	if tok == "" {
		return ""
	}
	if len(tok) <= 8 {
		return fmt.Sprintf("***（len=%d）", len(tok))
	}
	return fmt.Sprintf("%s***（len=%d）", tok[:8], len(tok))
}

// serveEntry 把上游响应/缓存条目回放给客户端：
// 透传状态码与响应头（剔除逐跳头）、补 X-Cache / X-Proxy-Upstream 观测头、
// 缓存命中时支持 If-None-Match/ETag 条件请求（304）。
//
// 可选参数 zr（variadic，既有调用方与既有测试不受影响）：zeroRange（docker blob 起点 0 的
// Range，见 client_range.go）的客户端请求语义——仅在 HIT 回放时用于**抑制 ETag 304 降级**：
// Range 请求必须拿到内容（206），回 304 会让 docker 的 httpReadSeeker 直接报
// 「unexpected status」。body/状态码到 206 的改写由 zeroRangeWriter 负责，这里不重复做。
// （起点 >0 的 blob Range 不走这里：handleGet 在缓存 HIT 时已用 serveEntryBlobRange 直接切片回 206。）
func (s *Server) serveEntry(w http.ResponseWriter, r *http.Request, e *cache.Entry, cacheStatus string, eff config.Options, start time.Time, zr ...blobRangeSpec) {
	// docker 日志统一标签（source 按回放链路派生）：X-Cache: RESUME 的回放属续传链路
	//（source=resume），其余 HIT / MISS / BYPASS 回放属客户端请求主路径（source=client）；
	// /v2/ 目标按需解析 image/digest/ref（见 log_attrs.go）
	logSource := dockerLogSourceClient
	if cacheStatus == resumeCacheStatus {
		logSource = dockerLogSourceResume
	}
	labels := parseDockerLogInfo(r.URL.Path)
	// //////////////////  透传上游响应头  start  ////////////////////////////////////////////////
	h := w.Header()
	for k, vv := range e.Header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	h.Set("X-Cache", cacheStatus)
	if e.Source != "" {
		h.Set("X-Proxy-Upstream", e.Source) // 观测本次内容来源（direct / 轮询命中的候选 URL）
	}
	// //////////////////  透传上游响应头  end  ////////////////////////////////////////////////

	// //////////////////  写响应  start  ////////////////////////////////////////////////
	// 先算最终给客户端的状态码：HIT 且客户端 If-None-Match 命中条目 ETag 时降为 304；
	// len(zr) != 0（blob 起点 0 的 Range 请求）时绝不降 304——客户端要的是内容（206）
	status := e.Status
	if cacheStatus == "HIT" && len(zr) == 0 && etagNotModified(r, e.Header) {
		status = http.StatusNotModified
	}
	switch {
	case status == http.StatusNotModified:
		// 304（HIT ETag 命中，或上游对无条件 GET 仍回 304 的兜底透传）：
		// 无实体，不应携带 Content-Length 等实体头，只写状态码
		w.WriteHeader(status)
	case r.Method == http.MethodHead:
		// HEAD：保留上游声明的 Content-Length，不写 body
		if cl := e.Header.Get("Content-Length"); cl != "" {
			h.Set("Content-Length", cl)
		} else {
			h.Set("Content-Length", strconv.Itoa(len(e.Body)))
		}
		w.WriteHeader(status)
	default:
		h.Set("Content-Length", strconv.Itoa(len(e.Body)))
		w.WriteHeader(status)
		if len(e.Body) > 0 {
			// 活跃下载进度登记（周期汇总，见 progress_summary.go）：缓存命中 / 单飞等待
			// 回放 / BYPASS 回放同样是把内容传给客户端，按 source=client（续传链路回放为
			// resume）登记，分块写逐块 addBytes——大 blob 一次 Write 会让 3 分钟汇总在
			// 传输期间只能看到 0 字节。写失败/成功返回都 defer 注销，不留幽灵条目
			prog := s.progressRegister(logSource, labels.image, labels.digest, r.Method, 0, int64(len(e.Body)))
			defer prog.unregister()
			if err := writeBodyWithProgress(w, e.Body, prog); err != nil {
				s.log.Warn("写响应体失败", labels.attrs(logSource, "target", truncate(r.URL.Path, 200), "err", err)...)
			}
		}
	}
	// //////////////////  写响应  end  ////////////////////////////////////////////////

	// //////////////////  成功返回日志  start  ////////////////////////////////////////////////
	// 响应写给客户端后补一条轻量的成功语义日志，避免「客户端实际拿到了内容、
	// 日志却整条像失败」的误判；字段刻意精简，不与下方「代理请求完成」重复堆叠
	switch {
	case status == http.StatusNotModified:
		// 最终 304（HIT ETag 命中或上游 304 透传）也是成功：客户端用本地缓存副本正常渲染
		s.log.Info("内容未变更（304），客户端缓存副本有效",
			labels.attrs(logSource, "rule", eff.Rule, "upstream", e.Source, "cache", cacheStatus)...)
	case status == http.StatusOK && len(e.Body) > 0 && r.Method != http.MethodHead:
		s.log.Info("成功返回内容",
			labels.attrs(logSource, "rule", eff.Rule, "upstream", e.Source, "status", status,
				"bytes", len(e.Body), "cache", cacheStatus)...)
	}
	// //////////////////  成功返回日志  end  ////////////////////////////////////////////////

	s.log.Info("代理请求完成",
		labels.attrs(logSource,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", status,
			"cache", cacheStatus,
			"upstream", e.Source,
			"rule", eff.Rule,
			"bytes", len(e.Body),
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
}

// writeUpstreamStatus 把「候选全败且最后失败为上游明确 4xx」的上游响应透传给客户端
// （见 upstream_status.go）：透传状态码与响应头（沿用 skipResponseHeader 过滤逐跳头与
// 自管头）、X-Proxy-Upstream 观测头标明产生该响应的候选、body 为探测量级（探测首块，
// ≤probeFirstChunkLimit）。HEAD 请求只回状态码与响应头（无 body），Content-Length 沿用
// 上游声明值（描述 GET 语义的完整长度）。Warn 级留痕（透传是刻意的语义而非故障，
// 与 502 的 Error 级区分）。
func (s *Server) writeUpstreamStatus(w http.ResponseWriter, r *http.Request, use *upstreamStatusError) {
	h := w.Header()
	for k, vv := range use.header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	if use.url != "" {
		h.Set("X-Proxy-Upstream", use.url) // 观测产生该 4xx 的候选 URL
	}
	if r.Method == http.MethodHead {
		// HEAD：无 body，Content-Length 用上游声明值（缺失则不设，Go 按 HEAD 语义回放）
		if cl := use.header.Get("Content-Length"); cl != "" {
			h.Set("Content-Length", cl)
		}
		w.WriteHeader(use.status)
	} else {
		h.Set("Content-Length", strconv.Itoa(len(use.body)))
		w.WriteHeader(use.status)
		if len(use.body) > 0 {
			if _, err := w.Write(use.body); err != nil {
				s.log.Warn("写上游 4xx 透传响应体失败", "status", use.status, "err", err)
			}
		}
	}
	s.log.Warn("候选全败且最后失败为上游明确 4xx，已透传上游状态码与响应",
		parseDockerLogInfo(r.URL.Path).attrs(dockerLogSourceClient,
			"status", use.status, "upstream", truncate(use.url, 300),
			"body_bytes", len(use.body), "target", truncate(r.URL.Path, 300))...)
}

// writeError 输出 JSON 格式错误响应并记录日志（502 回源彻底失败升级为 Error，其余保持 Warn）。
func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg}); err != nil {
		s.log.Warn("写错误响应失败", "status", code, "err", err)
	}
	if code == http.StatusBadGateway {
		s.log.Error("请求处理失败", "status", code, "error", msg)
		return
	}
	s.log.Warn("请求处理失败", "status", code, "error", msg)
}

// ExtractTarget 从请求路径中提取目标，并区分「完整 URL 目标」与「路径目标」两类：
//   - 完整 URL 目标（fullURL=true）：路径形如 /https://raw.githubusercontent.com/xxx/yyy，
//     去掉首个 / 后的整段（含查询串）是完整 http/https URL，即目标 URL（原有语义不变）；
//   - 路径目标（fullURL=false）：该段不是完整 URL（url.Parse 后 scheme 非 http/https 或
//     host 为空），如 docker registry 的 /v2/xxx/manifests/latest——target 返回该段原文
//     （含查询串、不带 scheme），由命中的域名规则 url-redirect 模板拼出回源地址；
//   - 空路径仍报错（文案不变）。
//
// 百分号编码回退逻辑保持不变：优先用 EscapedPath，未含 :// 时回退解码后的 r.URL.Path。
func ExtractTarget(r *http.Request) (target string, fullURL bool, err error) {
	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if !strings.Contains(p, "://") {
		// 客户端可能对整段目标 URL 做了百分号编码，回退用解码后的路径
		p = strings.TrimPrefix(r.URL.Path, "/")
	}
	if r.URL.RawQuery != "" {
		p += "?" + r.URL.RawQuery
	}
	if p == "" {
		return "", false, errors.New("缺少目标 URL，用法: /<目标URL>，例如 /https://raw.githubusercontent.com/user/repo/main/file.yaml")
	}
	if isFullURLTarget(p) {
		return p, true, nil
	}
	// 非完整 URL：不再报错，作为路径目标返回（能否承接由 handleProxy 的路径目标分发限制把关）
	return p, false, nil
}

// isFullURLTarget 判断 target 是否为完整 http/https URL（url.Parse 后 scheme 合法且 host 非空），
// 与 ExtractTarget 的完整 URL 判定保持同一语义，供回源链路区分两类目标
// （直连回退 / 无模板直连仅对完整 URL 目标适用）。
func isFullURLTarget(target string) bool {
	u, err := url.Parse(target)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// checkSuccess 按生效的 success-check 判据判断候选响应是否成功：
// status：HTTP 200 即成功；content：HTTP 200 且 body 非空（默认）。
// HEAD 响应天然无 body：content 判据对 HEAD 自动按 status 语义（200 即成功），
// 保证 docker registry 的 HEAD 探测不会被「body 为空」误拒。
// HTTP 304 在两种模式下均视为成功（内容未变更）：条件请求头已不透传，正常回源不会出现 304；
// 若上游对无条件 GET/HEAD 仍回 304，属异常/缓存副本有效语义，应按成功候选处理（304 原样透传给客户端，
// 客户端沿用本地缓存副本），而不是把整条链路判成失败。
func checkSuccess(e *cache.Entry, check, method string) bool {
	switch e.Status {
	case http.StatusNotModified:
		// 304 无实体：无条件视为成功，不再检查 body
		return true
	case http.StatusOK:
		if check == config.SuccessCheckStatus || method == http.MethodHead {
			return true
		}
		return len(e.Body) > 0
	default:
		return false
	}
}

// skipResponseHeader 判断某响应头是否不应透传给客户端（逐跳头 + 本服务自管头）。
func skipResponseHeader(k string) bool {
	for _, h := range hopByHopHeaders {
		if strings.EqualFold(k, h) {
			return true
		}
	}
	switch {
	case strings.EqualFold(k, "Content-Length"),
		strings.EqualFold(k, "X-Cache"),
		strings.EqualFold(k, "X-Proxy-Upstream"):
		return true
	}
	return false
}

// etagNotModified 判断缓存命中场景下客户端 If-None-Match 是否命中条目 ETag（用于回 304）。
func etagNotModified(r *http.Request, h http.Header) bool {
	if r.Method != http.MethodGet {
		return false
	}
	// RFC 9110：If-None-Match 按弱比较语义匹配，两侧统一剥掉 W/ 前缀后再比对。
	etag := strings.TrimPrefix(h.Get("ETag"), "W/")
	inm := r.Header.Get("If-None-Match")
	if etag == "" || inm == "" {
		return false
	}
	for _, part := range strings.Split(inm, ",") {
		part = strings.TrimPrefix(strings.TrimSpace(part), "W/")
		if part == etag || part == "*" {
			return true
		}
	}
	return false
}

// clientNoCache 判断客户端是否通过 Cache-Control: no-cache 要求强制回源。
func clientNoCache(r *http.Request) bool {
	for _, v := range r.Header.Values("Cache-Control") {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "no-cache") {
				return true
			}
		}
	}
	return false
}

// ttlDisplay 把缓存 TTL 格式化为日志展示值：<=0（永不过期）输出 never，其余输出 duration 字符串。
func ttlDisplay(d time.Duration) string {
	if d <= 0 {
		return "never"
	}
	return d.String()
}

// truncate 截断过长字符串，用于日志与错误信息。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
