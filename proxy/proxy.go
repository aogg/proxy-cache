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
var forwardRequestHeaders = []string{
	"Accept", "Accept-Encoding", "Accept-Language", "Authorization",
	"Content-Type", "If-Match", "If-Modified-Since", "If-None-Match",
	"If-Range", "If-Unmodified-Since", "Range", "User-Agent",
}

// Server 是代理服务 HTTP 处理器，实现 http.Handler。
type Server struct {
	// cfg 已校验的运行期只读配置。
	cfg *config.Config
	// cache 文件缓存（可为 nil 表示禁用缓存，简化外部注入）。
	cache *cache.Cache
	// log 结构化日志器。
	log *slog.Logger
	// rr url-redirect 轮询起始下标计数器：第 1 个使用该列表的请求从 0 开始，
	// 之后每个请求 +1，实现候选列表的起始位置轮换（round-robin 负载轮询）。
	rr atomic.Uint64
	// clients 上游代理地址 -> 复用的 http.Client（初始化后只读，并发安全）。
	clients map[string]*http.Client
	// flight 同一缓存键的单飞组，防止缓存击穿。
	flight flightGroup
}

// New 创建 Server：预编译全局与各域名规则用到的上游代理客户端。
func New(cfg *config.Config, diskCache *cache.Cache, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, cache: diskCache, log: logger}

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
		"健康检查: /healthz\n"
	if _, err := io.WriteString(w, msg); err != nil {
		s.log.Warn("写首页响应失败", "err", err)
	}
}

// handleProxy 处理一条代理请求：提取目标 URL -> ACL -> 合并生效配置 -> 按方法分发。
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// //////////////////  提取并校验目标 URL  start  ////////////////////////////////////////////////
	target, err := ExtractTarget(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// //////////////////  提取并校验目标 URL  end  ////////////////////////////////////////////////

	// //////////////////  allow/deny 访问控制  start  ////////////////////////////////////////////////
	if err := s.cfg.CheckACL(target); err != nil {
		s.writeError(w, http.StatusForbidden, err.Error())
		return
	}
	// //////////////////  allow/deny 访问控制  end  ////////////////////////////////////////////////

	// //////////////////  合并全局配置与域名规则  start  ////////////////////////////////////////////////
	eff := s.cfg.ResolveOptions(target)
	// //////////////////  合并全局配置与域名规则  end  ////////////////////////////////////////////////

	// GET 走完整链路（缓存 + url-redirect 轮询）；其他方法不缓存、不轮询，直接透传
	if r.Method == http.MethodGet {
		s.handleGet(w, r, target, eff, start)
		return
	}
	e, err := s.fetchDirect(r, target, eff)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.serveEntry(w, r, e, "BYPASS", eff, start)
}

// handleGet 处理 GET 请求：读缓存（HIT）或 单飞回源 + 写缓存（MISS）；不可缓存时直接回源（BYPASS）。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, target string, eff config.Options, start time.Time) {
	key := cache.Key(target)
	// Range / no-cache 请求绕过缓存读取（避免语义冲突与强制刷新），其余启用缓存条件：cache.enabled 且 ttl>0
	cacheable := eff.CacheEnabled && eff.CacheTTL > 0 &&
		r.Header.Get("Range") == "" && !clientNoCache(r)

	if !cacheable {
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
	if s.cache != nil {
		if e, ok := s.cache.Get(key); ok {
			s.serveEntry(w, r, e, "HIT", eff, start)
			return
		}
	}
	// //////////////////  查缓存：命中直接回放  end  ////////////////////////////////////////////////

	// //////////////////  未命中：单飞回源并写缓存  start  ////////////////////////////////////////////////
	e, err := s.flight.Do(key, func() (*cache.Entry, error) {
		ent, ferr := s.fetchViaCandidates(r, target, eff)
		if ferr != nil {
			return nil, ferr
		}
		// 仅缓存满足成功判据的 200 响应
		if ent.Status == http.StatusOK && checkSuccess(ent, eff.SuccessCheck) {
			ent.TTL = eff.CacheTTL // TTL 在写入时固化到条目
			if s.cache != nil {
				if serr := s.cache.Set(key, ent); serr != nil {
					s.log.Warn("写入缓存失败", "key", key, "target", target, "err", serr)
				}
			}
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

// fetchViaCandidates 回源获取目标内容：
//  1. 展开生效的 url-redirect 模板为候选 URL 列表（无模板则直接请求原始 URL）；
//  2. 按 round-robin 轮换的起始下标逐个尝试候选，通过 success-check 判据即返回；
//  3. 候选全部失败时，按 fallback-direct 决定是否回退直连原始 URL。
func (s *Server) fetchViaCandidates(r *http.Request, target string, eff config.Options) (*cache.Entry, error) {
	candidates := ExpandCandidates(eff.URLRedirect, target)
	if len(candidates) == 0 {
		// 未配置 url-redirect：直接回源原始 URL（结果原样透传，不套用成功判据）
		return s.fetchDirect(r, target, eff)
	}

	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  start  ////////
	start := int(s.rr.Add(1)-1) % len(candidates)
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  end  ////////

	// //////////////////  逐个尝试候选  start  ////////////////////////////////////////////////
	var lastErr error
	for i := 0; i < len(candidates); i++ {
		cand := candidates[(start+i)%len(candidates)]
		e, err := s.doRequest(r, http.MethodGet, cand, nil, eff)
		if err != nil {
			lastErr = err
			s.log.Warn("url-redirect 候选请求失败", "candidate", cand, "err", err)
			continue
		}
		if checkSuccess(e, eff.SuccessCheck) {
			e.Source = cand
			return e, nil
		}
		lastErr = fmt.Errorf("候选 %s 未通过 success-check=%s（状态码 %d，body %d 字节）",
			cand, eff.SuccessCheck, e.Status, len(e.Body))
		s.log.Warn("url-redirect 候选未通过成功判据", "candidate", cand,
			"check", eff.SuccessCheck, "status", e.Status, "body_bytes", len(e.Body))
	}
	// //////////////////  逐个尝试候选  end  ////////////////////////////////////////////////

	// //////////////////  全部失败：按配置回退直连  start  ////////////////////////////////////////////////
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
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取上游响应失败（%s）: %w", urlStr, err)
	}
	// 深拷贝响应头，避免与连接复用的上游对象共享底层数据
	hdr := make(http.Header, len(resp.Header))
	for k, vv := range resp.Header {
		hdr[k] = append([]string(nil), vv...)
	}
	// //////////////////  发起请求并读取响应  end  ////////////////////////////////////////////////

	return &cache.Entry{
		Status:   resp.StatusCode,
		Header:   hdr,
		Body:     buf,
		StoredAt: time.Now(),
	}, nil
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
	status := e.Status
	switch {
	case cacheStatus == "HIT" && etagNotModified(r, e.Header):
		// 缓存命中且客户端 ETag 匹配：直接回 304，不再回传 body
		status = http.StatusNotModified
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

// writeError 输出 JSON 格式错误响应并记录日志。
func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg}); err != nil {
		s.log.Warn("写错误响应失败", "status", code, "err", err)
	}
	s.log.Warn("请求处理失败", "status", code, "error", msg)
}

// ExtractTarget 从请求路径中提取完整目标 URL：
// 路径形如 /https://raw.githubusercontent.com/xxx/yyy，去掉首个 / 后的整段即目标 URL；
// 同时携带查询串（RawQuery）。支持目标被百分号编码的情况（回退到解码后的路径）。
func ExtractTarget(r *http.Request) (string, error) {
	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if !strings.Contains(p, "://") {
		// 客户端可能对整段目标 URL 做了百分号编码，回退用解码后的路径
		p = strings.TrimPrefix(r.URL.Path, "/")
	}
	if r.URL.RawQuery != "" {
		p += "?" + r.URL.RawQuery
	}
	if p == "" {
		return "", errors.New("缺少目标 URL，用法: /<目标URL>，例如 /https://raw.githubusercontent.com/user/repo/main/file.yaml")
	}
	u, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("目标 URL 无效: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("目标 URL 必须是完整的 http/https 地址，收到 %q", truncate(p, 128))
	}
	return p, nil
}

// ExpandCandidates 把 url-redirect 模板列表展开为候选 URL 列表（$1 / ${1} 均替换为原始目标 URL）。
func ExpandCandidates(templates []string, target string) []string {
	if len(templates) == 0 {
		return nil
	}
	out := make([]string, 0, len(templates))
	for _, t := range templates {
		t = strings.ReplaceAll(t, "${1}", target)
		t = strings.ReplaceAll(t, "$1", target)
		out = append(out, t)
	}
	return out
}

// checkSuccess 按生效的 success-check 判据判断候选响应是否成功：
// status：HTTP 200 即成功；content：HTTP 200 且 body 非空（默认）。
func checkSuccess(e *cache.Entry, check string) bool {
	if e.Status != http.StatusOK {
		return false
	}
	if check == config.SuccessCheckStatus {
		return true
	}
	return len(e.Body) > 0
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
	etag := h.Get("ETag")
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

// truncate 截断过长字符串，用于日志与错误信息。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
