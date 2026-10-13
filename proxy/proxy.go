// Package proxy 实现核心反代逻辑：从请求路径提取目标 URL、ACL 校验、
// 域名规则配置合并、url-redirect 轮询回源（含成功判据与直连回退）、
// 缓存读写与响应（状态码/响应头/Body）透传。
package proxy

import (
	"context"
	"crypto/tls"
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
}

// New 创建 Server：预编译全局/各域名规则及其 url-redirect 候选对象用到的上游代理客户端
// （候选级 (proxy, connect-timeout) 组合按候选显式 ?? 规则 ?? 全局 解析后一并纳入）。
// caches 传 nil 表示禁用缓存；多目录场景由 Manager 按生效目录复用实例。
func New(cfg *config.Config, caches *cache.Manager, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, caches: caches, log: logger, dockerTokens: make(map[string]dockerTokenEntry)}

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
	s.log.Debug("代理请求开始", "method", r.Method, "remote", r.RemoteAddr,
		"target", target, "target_kind", targetKind(fullURL))
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
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, target string, fullURL bool, eff config.Options, start time.Time) {
	key := cache.Key(cacheKeyInput(r, target, fullURL))
	// 路径目标 GET：流式透传（选定候选立即 commit）；完整 URL GET：缓冲回放（行为不变）
	streaming := !fullURL
	// 先判定本请求不走缓存的具体原因（空串表示可缓存），再把原因暴露到日志，
	// 避免出现「没写缓存文件但日志看不出为什么」的排查黑洞。
	// 可缓存条件：cache.enabled 且 ttl>=0（TTL=0 表示永不过期，同样可缓存；负数理论上
	// 已被 config.Validate 拦截，此处 >=0 兜底防御）且无 Range / no-cache 语义冲突
	bypassReason := ""
	switch {
	case !eff.CacheEnabled:
		bypassReason = "cache-disabled" // 全局或命中规则显式关闭缓存
	case eff.CacheTTL < 0:
		bypassReason = "negative-ttl" // 防御分支：Validate 已拦截负 TTL
	case r.Header.Get("Range") != "":
		bypassReason = "request-range" // Range 请求绕过缓存读取（避免语义冲突）
	case clientNoCache(r):
		bypassReason = "request-no-cache" // 客户端要求强制回源刷新
	}
	cacheable := bypassReason == ""

	// 解析本请求生效的缓存实例：规则可配独立缓存目录，按生效目录向管理器获取（复用已预热实例）；
	// 获取失败时本次请求降级为直接回源（BYPASS），不中断服务
	var disk *cache.Cache
	if cacheable {
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
	// 304 / 未过判据分别留痕说明原因；条件与日志字段与旧版一致）
	finalize := func(ent *cache.Entry) {
		s.finalizeFetchEntry(key, target, ent, eff, disk)
	}
	// streamingError 流式回源错误的一致出口：commit 前失败 502；commit 后失败响应已
	// 固化（头已写出），只能终止连接，绝不能再 writeError 污染响应
	streamingError := func(err error, committed bool) {
		if err != nil && !committed {
			s.writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if err != nil {
			s.log.Warn("流式回源失败（响应头已写出，终止本次响应）",
				"target", truncate(r.URL.Path, 300), "err", err)
		}
	}

	if !cacheable {
		// 不走缓存的决策留痕：Info 级输出原因与生效规则（含未命中任何规则的 global-default），
		// 让「为什么不写缓存文件」在日志里直接可见
		s.log.Info("GET 请求绕过缓存（BYPASS）", "rule", eff.Rule, "reason", bypassReason,
			"target", truncate(target, 300))
		// //////////////////  不走缓存：直接按生效配置回源  start  ////////////////////////////////////////////////
		if streaming {
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
			// 缓存命中是用户最关心的决策结果之一，Info 级输出实际命中的磁盘文件路径
			s.log.Info("缓存命中（HIT）", "rule", eff.Rule, "key", key,
				"file", disk.FilePath(key), "bytes", len(e.Body), "ttl", ttlDisplay(e.TTL))
			s.serveEntry(w, r, e, "HIT", eff, start)
			return
		}
	}
	s.log.Debug("缓存查询结果", "result", "MISS", "key", key, "dir", eff.CachePath)
	// //////////////////  查缓存：命中直接回放  end  ////////////////////////////////////////////////

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
			// commit 后失败用 errStreamingCommitted 标记（响应头已固化，不得再 502）
			ent, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start)
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
		// 等待同 key 单飞回源超限：自行走候选回源（不注册单飞，流式透传 + EOF 写缓存）
		s.log.Warn("等待同 key 单飞回源超限，自行走候选回源",
			"rule", eff.Rule, "key", key, "wait", waitLimit.String(), "target", truncate(target, 300))
		_, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start)
		streamingError(ferr, committed)
	case errors.Is(err, errStreamingCommitted):
		// 本调用是流式执行方且 commit 后失败：底层原因已留痕（Error），响应头已写出，
		// 终止本次响应即可，绝不能再 writeError
	case err != nil:
		s.writeError(w, http.StatusBadGateway, err.Error())
	case streaming && executed:
		// 本调用是流式执行方：响应头已 commit、body 已边读边写、完成日志已输出，
		// 无需再走 serveEntry 回放
	default:
		s.serveEntry(w, r, e, "MISS", eff, start)
	}
	// //////////////////  未命中：单飞回源并写缓存  end  ////////////////////////////////////////////////
}

// finalizeFetchEntry 回源完成后的缓存写入与留痕（仅 GET，条件与日志字段与旧版一致）：
// 仅缓存满足成功判据的 200 响应（TTL 在写入时固化）；304 与「回源成功但未过判据」
// 分别留痕说明为什么这次没写缓存文件。
func (s *Server) finalizeFetchEntry(key, target string, ent *cache.Entry, eff config.Options, disk *cache.Cache) {
	if ent == nil {
		return
	}
	if ent.Status == http.StatusOK && checkSuccess(ent, eff.SuccessCheck, http.MethodGet) {
		ent.TTL = eff.CacheTTL // TTL 在写入时固化到条目（0 = 永不过期）
		if disk != nil {
			if serr := disk.Set(key, ent); serr != nil {
				s.log.Warn("写入缓存失败", "key", key, "target", target, "err", serr)
			} else {
				// 写盘成功必须可见：规则名、缓存 key、实际落盘文件路径、body 大小与生效 TTL
				s.log.Info("写入缓存成功",
					"rule", eff.Rule, "target", truncate(target, 300), "key", key,
					"file", disk.FilePath(key), "bytes", len(ent.Body), "ttl", ttlDisplay(ent.TTL))
			}
		}
		return
	}
	if ent.Status == http.StatusNotModified {
		// 回源 304（内容未变更）：条目无实体不可缓存，304 原样透传给客户端，
		// 客户端用本地缓存副本渲染，属成功语义而非失败（条件头已不透传，
		// 上游对无条件 GET 仍回 304 属异常/缓存副本有效兜底场景）
		s.log.Info("回源 304 未变更，不写缓存（客户端缓存副本有效）",
			"rule", eff.Rule, "status", ent.Status, "target", truncate(target, 300))
		return
	}
	// 回源成功但不满足缓存判据：同样留痕，说明为什么这次没写缓存文件
	s.log.Info("回源成功但不写缓存（未过成功判据或非 200）",
		"rule", eff.Rule, "status", ent.Status, "check", eff.SuccessCheck,
		"body_bytes", len(ent.Body), "target", truncate(target, 300))
}

// handleHead 处理 HEAD 请求（docker registry 用 HEAD 探测 manifest/blob 元信息）：
// 与 GET 一致统一走候选轮询管线（完整 URL 与路径目标行为相同）——
// 先查缓存（复用 GET 写下的条目回放响应头，无 body）；未命中按候选轮询回源
// （请求方法保持 HEAD，让 HEAD 探测同样打到镜像站）；HEAD 响应无 body，一律不写缓存。
// 未配置 url-redirect（候选为空）时由 fetchViaCandidates 回退 fetchDirect 直连，与旧版一致。
func (s *Server) handleHead(w http.ResponseWriter, r *http.Request, target string, fullURL bool, eff config.Options, start time.Time) {
	key := cache.Key(cacheKeyInput(r, target, fullURL))
	// 缓存不可读（未启用 / no-cache / 实例获取失败）时回源结果按 BYPASS 标注，与 GET 的语义对齐
	cacheStatus := "MISS"

	// //////////////////  查缓存：命中直接回放响应头  start  ////////////////////////////////////////////////
	if disk := s.readableCache(r, eff); disk != nil {
		if e, ok := disk.Get(key); ok {
			// 缓存命中是用户最关心的决策结果之一，Info 级输出实际命中的磁盘文件路径；
			// serveEntry 的 HEAD 分支只回放状态码与响应头（含 Content-Length），不写 body
			s.log.Info("缓存命中（HIT）", "rule", eff.Rule, "key", key,
				"file", disk.FilePath(key), "bytes", len(e.Body), "ttl", ttlDisplay(e.TTL))
			s.serveEntry(w, r, e, "HIT", eff, start)
			return
		}
		s.log.Debug("缓存查询结果", "result", "MISS", "key", key, "dir", eff.CachePath)
	} else {
		cacheStatus = "BYPASS"
	}
	// //////////////////  查缓存：命中直接回放响应头  end  ////////////////////////////////////////////////

	// //////////////////  未命中：候选轮询回源（HEAD 不写缓存）  start  ////////////////////////////////////////////////
	e, err := s.fetchViaCandidates(r, target, eff)
	if err != nil {
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
	res, err := s.selectCandidate(r, candidates, eff)
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
		//（错误信息注明路径目标无直连回退）
		s.log.Warn("url-redirect 全部候选失败，路径目标无直连回退（fallback-direct 不适用）",
			"target", truncate(target, 300), "last_error", lastErr)
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
		if e2, ok := s.dockerAuthDance(wd, s.client(eff.HTTPProxy, eff.ConnectTimeout, eff.InsecureSkipVerify), req, resp, urlStr); ok {
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
func (s *Server) dockerAuthDance(wd *progressWatchdog, cl *http.Client, orig *http.Request, resp *http.Response, urlStr string) (*cache.Entry, bool) {
	resp2, _, ok := s.dockerAuthDanceRetry(wd, cl, orig, resp, urlStr)
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
func (s *Server) dockerAuthDanceRetry(wd *progressWatchdog, cl *http.Client, orig *http.Request, resp *http.Response, urlStr string) (resp2 *http.Response, consumed, ok bool) {
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

	s.log.Info("触发 Docker Registry 匿名 token dance",
		"url", truncate(urlStr, 300), "realm", ch.realm, "service", ch.service, "scope", ch.scope)

	// //////////////////  取匿名 token（进程内缓存优先）  start  ///////////////////////////////
	token, err := s.dockerToken(wd, cl, ch)
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
func (s *Server) dockerToken(wd *progressWatchdog, cl *http.Client, ch *bearerChallenge) (string, error) {
	key := ch.realm + "|" + ch.service + "|" + ch.scope
	now := time.Now()

	// 缓存命中（未过期）：Debug 留痕即可
	s.dockerTokenMu.Lock()
	if ent, ok := s.dockerTokens[key]; ok && now.Before(ent.expiresAt) {
		s.dockerTokenMu.Unlock()
		s.log.Debug("Docker Registry 匿名 token 缓存命中",
			"realm", ch.realm, "service", ch.service, "scope", ch.scope,
			"token", maskToken(ent.token),
			"expires_in", ent.expiresAt.Sub(now).Round(time.Second).String())
		return ent.token, nil
	}
	s.dockerTokenMu.Unlock()

	token, ttl, err := fetchDockerToken(wd, cl, ch)
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
func fetchDockerToken(wd *progressWatchdog, cl *http.Client, ch *bearerChallenge) (string, time.Duration, error) {
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
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, fmt.Errorf("构造 token 请求失败（%s）: %w", u.String(), err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "proxy-cache/"+Version)

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
func (s *Server) serveEntry(w http.ResponseWriter, r *http.Request, e *cache.Entry, cacheStatus string, eff config.Options, start time.Time) {
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
	// 先算最终给客户端的状态码：HIT 且客户端 If-None-Match 命中条目 ETag 时降为 304
	status := e.Status
	if cacheStatus == "HIT" && etagNotModified(r, e.Header) {
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
			if _, err := w.Write(e.Body); err != nil {
				s.log.Warn("写响应体失败", "target", truncate(r.URL.Path, 200), "err", err)
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
			"rule", eff.Rule, "upstream", e.Source, "cache", cacheStatus)
	case status == http.StatusOK && len(e.Body) > 0 && r.Method != http.MethodHead:
		s.log.Info("成功返回内容",
			"rule", eff.Rule, "upstream", e.Source, "status", status,
			"bytes", len(e.Body), "cache", cacheStatus)
	}
	// //////////////////  成功返回日志  end  ////////////////////////////////////////////////

	s.log.Info("代理请求完成",
		"method", r.Method,
		"target", truncate(r.URL.Path, 300),
		"status", status,
		"cache", cacheStatus,
		"upstream", e.Source,
		"rule", eff.Rule,
		"bytes", len(e.Body),
		"cost", time.Since(start).Round(time.Millisecond).String(),
	)
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
