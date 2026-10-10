// Package proxy 实现核心反代逻辑：从请求路径提取目标 URL、ACL 校验、
// 域名规则配置合并、url-redirect 轮询回源（含成功判据与直连回退）、
// 缓存读写与响应（状态码/响应头/Body）透传。
package proxy

import (
	"context"
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
	// clients 上游代理地址 -> 复用的 http.Client（初始化后只读，并发安全）。
	clients map[string]*http.Client
	// flight 同一缓存键的单飞组，防止缓存击穿。
	flight flightGroup
	// dockerTokenMu 保护 dockerTokens 的并发读写。
	dockerTokenMu sync.Mutex
	// dockerTokens Docker Registry 匿名 token 进程内缓存：key = realm|service|scope，
	// value = token 与过期时间点。镜像 blob 分层很多，进程内缓存避免每层都打一次
	// auth 端点（auth.docker.io 对国内很慢且有频控）。
	dockerTokens map[string]dockerTokenEntry
}

// New 创建 Server：预编译全局与各域名规则用到的上游代理客户端。
// caches 传 nil 表示禁用缓存；多目录场景由 Manager 按生效目录复用实例。
func New(cfg *config.Config, caches *cache.Manager, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, caches: caches, log: logger, dockerTokens: make(map[string]dockerTokenEntry)}

	// 收集全部可能用到的上游代理地址（空串代表直连），各建一个可复用 Client
	proxySet := map[string]struct{}{"": {}}
	if cfg.HTTPProxy != "" {
		proxySet[cfg.HTTPProxy] = struct{}{}
	}
	for i := range cfg.DomainRules {
		if p := cfg.DomainRules[i].HTTPProxy; p != nil && *p != "" {
			proxySet[*p] = struct{}{}
		}
	}
	s.clients = make(map[string]*http.Client, len(proxySet))
	for p := range proxySet {
		cl, err := buildHTTPClient(p)
		if err != nil {
			return nil, err
		}
		s.clients[p] = cl
	}
	return s, nil
}

// buildHTTPClient 构建指定上游代理（空串=直连）的 http.Client。
func buildHTTPClient(proxyAddr string) (*http.Client, error) {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
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
	return &http.Client{Transport: tr}, nil
}

// client 返回生效代理对应的 Client（未知代理地址兜底直连客户端）。
func (s *Server) client(proxyAddr string) *http.Client {
	if cl, ok := s.clients[proxyAddr]; ok && cl != nil {
		return cl
	}
	return s.clients[""]
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
	//（路径目标请求的匹配串形如 http://registry-proxy-cache.xxx:5480/v2/xxx/manifests/latest）
	eff := s.cfg.ResolveOptions(r.Host, target)
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
//（兼容已有缓存文件）；路径目标 = 「http://<入站Host>/<路径目标>」（入站 Host 参与，
// 避免不同入站域名的同路径缓存互相污染）。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, target string, fullURL bool, eff config.Options, start time.Time) {
	key := cache.Key(cacheKeyInput(r, target, fullURL))
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

	if !cacheable {
		// 不走缓存的决策留痕：Info 级输出原因与生效规则（含未命中任何规则的 global-default），
		// 让「为什么不写缓存文件」在日志里直接可见
		s.log.Info("GET 请求绕过缓存（BYPASS）", "rule", eff.Rule, "reason", bypassReason,
			"target", truncate(target, 300))
		// //////////////////  不走缓存：直接按生效配置回源  start  ////////////////////////////////////////////////
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
	// 必然落同一缓存实例（路径目标的 key 输入含入站 Host，跨入站域名天然不共享单飞）
	e, err := s.flight.Do(key, func() (*cache.Entry, error) {
		ent, ferr := s.fetchViaCandidates(r, target, eff)
		if ferr != nil {
			return nil, ferr
		}
		// 仅缓存满足成功判据的 200 响应（handleGet 只处理 GET，判据按 GET 语义）
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
		} else if ent.Status == http.StatusNotModified {
			// 回源 304（内容未变更）：条目无实体不可缓存，304 原样透传给客户端，
			// 客户端用本地缓存副本渲染，属成功语义而非失败（条件头已不透传，
			// 上游对无条件 GET 仍回 304 属异常/缓存副本有效兜底场景）
			s.log.Info("回源 304 未变更，不写缓存（客户端缓存副本有效）",
				"rule", eff.Rule, "status", ent.Status, "target", truncate(target, 300))
		} else {
			// 回源成功但不满足缓存判据：同样留痕，说明为什么这次没写缓存文件
			s.log.Info("回源成功但不写缓存（未过成功判据或非 200）",
				"rule", eff.Rule, "status", ent.Status, "check", eff.SuccessCheck,
				"body_bytes", len(ent.Body), "target", truncate(target, 300))
		}
		return ent, nil
	})
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.serveEntry(w, r, e, "MISS", eff, start)
	// //////////////////  未命中：单飞回源并写缓存  end  ////////////////////////////////////////////////
}

// handleHead 处理 HEAD 请求（docker registry 用 HEAD 探测 manifest/blob 元信息）：
// 与 GET 一致统一走候选轮询管线（完整 URL 与路径目标行为相同）——
// 先查缓存（复用 GET 写下的条目回放响应头，无 body）；未命中按候选轮询回源
//（请求方法保持 HEAD，让 HEAD 探测同样打到镜像站）；HEAD 响应无 body，一律不写缓存。
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

// fetchViaCandidates 回源获取目标内容：
//  1. 展开生效的 url-redirect 模板为候选 URL 列表（占位符见 RequestVars：$1 与请求级变量）；
//     无模板且目标为完整 URL 时直接请求原始 URL，路径目标无模板直接报错
//    （路径目标必须由规则模板拼出回源地址，没有可直连的原始 URL）；
//  2. 按 round-robin 轮换的起始下标逐个尝试候选（请求方法沿用入站方法 GET/HEAD，
//     docker registry 的 HEAD 探测因此同样打到镜像站），通过 success-check 判据即返回；
//  3. 候选全部失败时，完整 URL 目标按 fallback-direct 决定是否回退直连原始 URL；
//     路径目标没有完整原始 URL 可直连，fallback-direct 不适用，一律直接报错（无直连回退）。
func (s *Server) fetchViaCandidates(r *http.Request, target string, eff config.Options) (*cache.Entry, error) {
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target))
	if len(candidates) == 0 {
		if !isFullURLTarget(target) {
			// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，此处兜底绝不直连
			return nil, errors.New("路径目标请求未配置 url-redirect，无法回源（路径目标必须由命中的域名规则提供 url-redirect）")
		}
		// 未配置 url-redirect：直接回源原始 URL（结果原样透传，不套用成功判据）
		s.log.Debug("无 url-redirect 候选，直连回源", "target", target)
		return s.fetchDirect(r, target, eff)
	}

	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  start  ////////
	start := int(s.rr.Add(1)-1) % len(candidates)
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  end  ////////

	// //////////////////  轮询展开明细日志  start  ////////////////////////////////////////////////
	// 按「本轮实际尝试顺序」输出每个候选的原始模板与占位符替换后的最终 URL，并给出本轮
	// round-robin 起始下标（0 基，指向模板列表下标）；candidates 与 eff.URLRedirect 同序等长，
	// details[i] 即第 (start+i)%n 个模板展开结果。Info 级保证默认日志级别下可见。
	details := make([]string, 0, len(candidates))
	for i := range candidates {
		idx := (start + i) % len(candidates) // 该候选在模板列表中的原始下标
		details = append(details, fmt.Sprintf("模板[%d] %s -> %s", idx, eff.URLRedirect[idx], candidates[idx]))
	}
	s.log.Info("url-redirect 轮询展开", "start_index", start,
		"count", len(candidates), "candidates", details)
	// //////////////////  轮询展开明细日志  end  ////////////////////////////////////////////////

	// //////////////////  逐个尝试候选  start  ////////////////////////////////////////////////
	var lastErr error
	for i := 0; i < len(candidates); i++ {
		idx := (start + i) % len(candidates) // 该候选在模板列表中的原始下标
		cand := candidates[idx]
		tpl := eff.URLRedirect[idx]
		s.log.Debug("尝试 url-redirect 候选", "attempt", i, "index", idx,
			"template", tpl, "candidate", cand)
		candStart := time.Now() // 该候选耗时（含请求与响应读取）
		// 请求方法沿用入站方法（GET/HEAD）：HEAD 探测（docker registry）同样经候选轮询打到镜像站
		e, err := s.doRequest(r, r.Method, cand, nil, eff)
		if err != nil {
			lastErr = err
			s.log.Warn("url-redirect 候选请求失败", "attempt", i, "index", idx,
				"template", tpl, "candidate", cand,
				"cost", time.Since(candStart).Round(time.Millisecond).String(), "err", err)
			continue
		}
		if checkSuccess(e, eff.SuccessCheck, r.Method) {
			e.Source = cand
			// 命中即最终返回：输出命中的是第几个候选（模板下标 + 轮内尝试序）、
			// 模板与替换后 URL、HTTP 状态码与耗时，作为「最终采用哪个候选」的留痕
			s.log.Info("url-redirect 候选命中并作为最终返回", "attempt", i, "index", idx,
				"template", tpl, "candidate", cand, "status", e.Status,
				"bytes", len(e.Body), "cost", time.Since(candStart).Round(time.Millisecond).String())
			return e, nil
		}
		lastErr = fmt.Errorf("候选 %s 未通过 success-check=%s（状态码 %d，body %d 字节）",
			cand, eff.SuccessCheck, e.Status, len(e.Body))
		s.log.Warn("url-redirect 候选未通过成功判据", "attempt", i, "index", idx,
			"template", tpl, "candidate", cand,
			"check", eff.SuccessCheck, "status", e.Status, "body_bytes", len(e.Body))
	}
	// //////////////////  逐个尝试候选  end  ////////////////////////////////////////////////

	// //////////////////  全部失败：按配置回退直连  start  ////////////////////////////////////////////////
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
	// //////////////////  全部失败：按配置回退直连  end  ////////////////////////////////////////////////
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

// doRequest 向 urlStr 发起请求。所有上游请求（直连与 url-redirect 候选）统一走这里，
// 并应用生效配置的 http-proxy（上游代理）与 timeout（总超时）。
// GET/HEAD 回源遇上游 401 + WWW-Authenticate: Bearer 挑战时，自动完成 Docker Registry
// 匿名 token dance（401 → 取 token → 带 Bearer 重试一次，见 dockerAuthDance）；
// 其余方法（fetchDirect 透传 r.Body 的那类）不做 dance。
func (s *Server) doRequest(r *http.Request, method, urlStr string, body io.Reader, eff config.Options) (*cache.Entry, error) {
	// //////////////////  校验参数  start  ////////////////////////////////////////////////
	timeout := eff.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	// //////////////////  校验参数  end  ////////////////////////////////////////////////

	// //////////////////  构造上游请求  start  ////////////////////////////////////////////////
	// 与客户端连接解耦（WithoutCancel）：首个客户端断开也不中断回源，
	// 保证缓存填充与单飞等待方仍能拿到完整结果
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, fmt.Errorf("构造上游请求失败（%s）: %w", urlStr, err)
	}
	for _, h := range forwardRequestHeaders {
		if vv := r.Header.Values(h); len(vv) > 0 {
			req.Header[h] = vv
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "proxy-cache/"+Version)
	}
	// //////////////////  构造上游请求  end  ////////////////////////////////////////////////

	// //////////////////  发起请求并读取响应  start  ////////////////////////////////////////////////
	resp, err := s.client(eff.HTTPProxy).Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求上游失败（%s）: %w", urlStr, err)
	}
	defer resp.Body.Close()
	// HEAD 响应天然无 body（不读 body 语义）：跳过读取，Body 保持为空，
	// 状态码与响应头（含 Content-Length / Docker-Content-Digest 等）照常透传
	var buf []byte
	if method != http.MethodHead {
		buf, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("读取上游响应失败（%s）: %w", urlStr, err)
		}
	}
	// 深拷贝响应头，避免与连接复用的上游对象共享底层数据
	hdr := make(http.Header, len(resp.Header))
	for k, vv := range resp.Header {
		hdr[k] = append([]string(nil), vv...)
	}
	// //////////////////  发起请求并读取响应  end  ////////////////////////////////////////////////

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
	if resp.StatusCode == http.StatusUnauthorized && (method == http.MethodGet || method == http.MethodHead) {
		if e2, ok := s.dockerAuthDance(ctx, s.client(eff.HTTPProxy), req, resp, urlStr); ok {
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
//   - 取 token 走进程内缓存（见 dockerToken），客户端与 ctx 均沿用原请求（受同一 timeout 约束）；
//   - 取到 token 后复制原上游请求（同 method/URL，沿用原上游请求头）删除 Authorization
//     后改为 Bearer 重试一次：重试响应按既有语义（深拷贝响应头 + 读 body）构建为 Entry
//     返回（ok=true）；重试即使仍 401 也按该响应原样返回，不再 dance；
//   - 挑战不可解析 / 取 token / 构造重试 / 重试失败时返回 ok=false，调用方沿用原始
//     401 entry（行为与未实现 dance 的版本一致，绝不因 dance 引入新的失败路径）；
//   - 确认 dance 触发后先把首个 401 响应 body 用 io.Copy(io.Discard, ...) 排干再 Close
//    （GET 此前已读到 EOF、HEAD 天然无 body，这里是显式兜底），避免连接泄漏；
//     调用方 defer 的重复 Close 对 http 响应体是幂等的，安全。
func (s *Server) dockerAuthDance(ctx context.Context, cl *http.Client, orig *http.Request, resp *http.Response, urlStr string) (*cache.Entry, bool) {
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
		return nil, false
	}
	// //////////////////  解析 Bearer 挑战  end  ////////////////////////////////////////////////

	// 排干首个 401 响应 body 并关闭：dance 触发后原响应不再复用，不排干会占住连接造成泄漏
	_, _ = io.Copy(io.Discard, resp.Body) // body 已废弃，排干错误无补救动作
	_ = resp.Body.Close()

	s.log.Info("触发 Docker Registry 匿名 token dance",
		"url", truncate(urlStr, 300), "realm", ch.realm, "service", ch.service, "scope", ch.scope)

	// //////////////////  取匿名 token（进程内缓存优先）  start  ///////////////////////////////
	token, err := s.dockerToken(ctx, cl, ch)
	if err != nil {
		s.log.Warn("获取 Docker Registry 匿名 token 失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "realm", ch.realm, "err", err)
		return nil, false
	}
	// //////////////////  取匿名 token（进程内缓存优先）  end  ///////////////////////////////////

	// //////////////////  带 token 重试一次  start  /////////////////////////////////////////////
	// 复制原请求（同 method/URL），头沿用原上游请求头：删除客户端 Authorization 后改为
	// Bearer <token>（GET/HEAD 回源 body 恒为 nil，无需透传请求体）
	req, err := http.NewRequestWithContext(ctx, orig.Method, urlStr, nil)
	if err != nil {
		s.log.Warn("构造匿名 token 重试请求失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "err", err)
		return nil, false
	}
	req.Header = orig.Header.Clone()
	req.Header.Del("Authorization")
	req.Header.Set("Authorization", "Bearer "+token)

	resp2, err := cl.Do(req)
	if err != nil {
		s.log.Warn("带匿名 token 重试上游请求失败，按原始 401 响应返回",
			"url", truncate(urlStr, 300), "token", maskToken(token), "err", err)
		return nil, false
	}
	defer resp2.Body.Close()

	// 重试响应按既有语义读取：HEAD 天然无 body 跳过，其余读全文；读取失败按重试失败兜底
	var buf []byte
	if orig.Method != http.MethodHead {
		buf, err = io.ReadAll(resp2.Body)
		if err != nil {
			s.log.Warn("读取匿名 token 重试响应失败，按原始 401 响应返回",
				"url", truncate(urlStr, 300), "err", err)
			return nil, false
		}
	}
	hdr := make(http.Header, len(resp2.Header))
	for k, vv := range resp2.Header {
		hdr[k] = append([]string(nil), vv...)
	}
	// 重试结果留痕（Info）：状态码 + 脱敏 token（绝不输出完整 token 值）
	s.log.Info("带匿名 token 重试完成", "url", truncate(urlStr, 300),
		"status", resp2.StatusCode, "token", maskToken(token))
	return &cache.Entry{
		Status:   resp2.StatusCode,
		Header:   hdr,
		Body:     buf,
		StoredAt: time.Now(),
	}, true
	// //////////////////  带 token 重试一次  end  /////////////////////////////////////////////
}

// dockerToken 返回挑战对应的匿名 token：优先命中进程内缓存（key = realm|service|scope，
// 未过期直接复用，避免每个 blob 层都打一次 auth 端点），未命中或已过期才请求 realm
// 换取新 token 并写回缓存（TTL = expires_in，>0 才用，否则缺省 60s）。
func (s *Server) dockerToken(ctx context.Context, cl *http.Client, ch *bearerChallenge) (string, error) {
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

	token, ttl, err := fetchDockerToken(ctx, cl, ch)
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
//   - 响应 JSON 优先取 "token" 字段、为空再取 "access_token"，两者皆空视为失败；
//     "expires_in"（秒，整数，>0 才生效）作为缓存 TTL，缺省 60s。
func fetchDockerToken(ctx context.Context, cl *http.Client, ch *bearerChallenge) (string, time.Duration, error) {
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
		return "", 0, fmt.Errorf("请求 token 端点失败（%s）: %w", u.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token 端点返回异常状态码 %d（%s）", resp.StatusCode, u.String())
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, dockerTokenBodyLimit))
	if err != nil {
		return "", 0, fmt.Errorf("读取 token 响应失败（%s）: %w", u.String(), err)
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
//（直连回退 / 无模板直连仅对完整 URL 目标适用）。
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
