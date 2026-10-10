package proxy

// registry_test.go —— docker registry 路径目标请求的 ServeHTTP 级单测：
// 全部用 httptest 假上游（不启动真实服务、不发真实外网请求），经 Server.ServeHTTP
// 走完整链路（ExtractTarget -> ACL -> 域名规则合并 -> 路径目标分发限制 -> GET/HEAD 管线）：
//   - GET 路径目标：命中 domain-rules（match 命中入站 Host）+ url-redirect 用
//     $http.server.header.full_url_no_server 指向假上游，断言上游收到的路径/方法与内容透传；
//   - HEAD 路径目标：上游收到 HEAD、响应头透传、客户端无 body；
//   - 未命中规则 -> 400；非 GET/HEAD -> 400；
//   - 候选全败且 fallback-direct=true 也不直连（502）；
//   - 缓存 key 含入站 Host：同 Host 第二次 GET X-Cache: HIT；换 Host 同路径不共享缓存。

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// upstreamRecord 记录假上游收到的请求（并发安全，请求逐个串行到达）。
type upstreamRecord struct {
	mu     sync.Mutex
	method []string // 逐次收到的请求方法
	path   []string // 逐次收到的请求路径（含查询串）
}

// add 记录一次收到的请求。
func (rec *upstreamRecord) add(method, path string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.method = append(rec.method, method)
	rec.path = append(rec.path, path)
}

// calls 返回收到的请求次数。
func (rec *upstreamRecord) calls() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return len(rec.method)
}

// last 返回最近一次收到的 (method, path)。
func (rec *upstreamRecord) last() (string, string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.method) == 0 {
		return "", ""
	}
	return rec.method[len(rec.method)-1], rec.path[len(rec.path)-1]
}

// newRegistryServer 构建带一条 docker registry 域名规则的 Server（参考 serve_test.go 构造方式）：
// match 为 hostRegex（对「http://<入站Host>/<目标>」整串匹配），规则级 url-redirect 为 templates。
// cacheDir 非空时启用缓存（目录用 cacheDir，实例来自 caches 管理器）；
// cacheDir 为空时显式关闭缓存，X-Cache 恒为 BYPASS、不落盘。
func newRegistryServer(t *testing.T, hostRegex, cacheDir string, templates []string, caches *cache.Manager) *Server {
	t.Helper()
	cfg := config.Default()
	if cacheDir != "" {
		cfg.Cache.Path = cacheDir
	} else {
		cfg.Cache.Enabled = false
	}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "docker-registry",
		Match:       hostRegex,
		URLRedirect: &templates,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, caches, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// TestServeHTTPRegistryGetPathTarget 验证 GET 路径目标全链路：
// 假上游收到 GET /v2/...（full_url_no_server 展开且斜杠接缝去重），
// 客户端拿到透传的 200 body 与上游响应头。
func TestServeHTTPRegistryGetPathTarget(t *testing.T) {
	var rec upstreamRecord
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.URL.RawQuery != "" {
			p += "?" + r.URL.RawQuery
		}
		rec.add(r.Method, p)
		w.Header().Set("Docker-Content-Digest", "sha256:abc123")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "manifest-json")
	}))
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		nil)

	r := httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+pathDockerManifest, nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Body.String(); got != "manifest-json" {
		t.Errorf("Body = %q, want 透传 %q", got, "manifest-json")
	}
	if got := w.Header().Get("Docker-Content-Digest"); got != "sha256:abc123" {
		t.Errorf("Docker-Content-Digest = %q, want 响应头透传 %q", got, "sha256:abc123")
	}
	if got := w.Header().Get("X-Cache"); got != "BYPASS" {
		t.Errorf("X-Cache = %q, want %q（本用例显式关闭缓存）", got, "BYPASS")
	}
	if want := upstream.URL + pathDockerManifest; w.Header().Get("X-Proxy-Upstream") != want {
		t.Errorf("X-Proxy-Upstream = %q, want 命中候选 %q", w.Header().Get("X-Proxy-Upstream"), want)
	}
	if n := rec.calls(); n != 1 {
		t.Fatalf("假上游收到 %d 次请求, want 1", n)
	}
	if m, p := rec.last(); m != http.MethodGet || p != pathDockerManifest {
		t.Errorf("上游收到 %s %q, want GET %q（不得出现双斜杠）", m, p, pathDockerManifest)
	}
}

// TestServeHTTPRegistryHeadPathTarget 验证 HEAD 路径目标全链路：
// 假上游收到 HEAD（统一走候选轮询回源），响应头（含上游声明的 Content-Length 与
// Docker-Content-Digest）透传给客户端，且客户端无 body。
func TestServeHTTPRegistryHeadPathTarget(t *testing.T) {
	var rec upstreamRecord
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.URL.RawQuery != "" {
			p += "?" + r.URL.RawQuery
		}
		rec.add(r.Method, p)
		w.Header().Set("Docker-Content-Digest", "sha256:def456")
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Header().Set("Content-Length", "15") // HEAD 响应无 body，长度由上游声明
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		nil)

	r := httptest.NewRequest(http.MethodHead,
		"http://"+hostDockerRegistry+pathDockerManifest, nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
	}
	if m, p := rec.last(); m != http.MethodHead || p != pathDockerManifest {
		t.Errorf("上游收到 %s %q, want HEAD %q", m, p, pathDockerManifest)
	}
	if got := w.Header().Get("Docker-Content-Digest"); got != "sha256:def456" {
		t.Errorf("Docker-Content-Digest = %q, want 响应头透传 %q", got, "sha256:def456")
	}
	if got := w.Header().Get("Content-Type"); got != "application/vnd.docker.distribution.manifest.v2+json" {
		t.Errorf("Content-Type = %q, want 透传上游声明值", got)
	}
	if got := w.Header().Get("Content-Length"); got != "15" {
		t.Errorf("Content-Length = %q, want 上游声明的 %q", got, "15")
	}
	if w.Body.Len() != 0 {
		t.Errorf("HEAD 响应不应有 body，实际 %d 字节: %q", w.Body.Len(), w.Body.String())
	}
}

// TestServeHTTPRegistryPathTargetNoRule 验证路径目标未命中任何 domain-rules 规则
// （全局默认配置不承接路径目标）时返回 400，且不发起任何回源请求。
func TestServeHTTPRegistryPathTargetNoRule(t *testing.T) {
	var rec upstreamRecord
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r.Method, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`, // 只命中 docker 入站 Host
		"",
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		nil)

	// 入站 Host 不命中规则的路径目标请求
	r := httptest.NewRequest(http.MethodGet,
		"http://other.example.com/v2/adockero/proxy-cache/manifests/latest", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusBadRequest, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "路径目标请求必须命中某条 domain-rules") {
		t.Errorf("错误响应 %q 应说明路径目标必须命中 domain-rules 规则", w.Body.String())
	}
	if n := rec.calls(); n != 0 {
		t.Errorf("未命中规则不应回源，假上游却收到 %d 次请求", n)
	}
}

// TestServeHTTPRegistryPathTargetPostRejected 验证路径目标仅支持 GET/HEAD：
// POST 路径目标请求直接 400，不发起回源。
func TestServeHTTPRegistryPathTargetPostRejected(t *testing.T) {
	var rec upstreamRecord
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r.Method, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		nil)

	r := httptest.NewRequest(http.MethodPost,
		"http://"+hostDockerRegistry+pathDockerManifest, strings.NewReader("{}"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusBadRequest, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "路径目标仅支持 GET/HEAD") {
		t.Errorf("错误响应 %q 应说明路径目标仅支持 GET/HEAD", w.Body.String())
	}
	if n := rec.calls(); n != 0 {
		t.Errorf("非 GET/HEAD 路径目标不应回源，假上游却收到 %d 次请求", n)
	}
}

// TestServeHTTPRegistryPathTargetNoDirectFallback 验证路径目标候选全败时无直连回退：
// 即使 fallback-direct=true（Default 默认），路径目标没有完整原始 URL 可直连，
// 应返回 502 且错误信息注明无直连回退。
func TestServeHTTPRegistryPathTargetNoDirectFallback(t *testing.T) {
	upstream := brokenUpstream(t) // 恒 500：任何 success-check 判据都不过
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		nil) // Default 的 fallback-direct=true

	r := httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+pathDockerManifest, nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusBadGateway, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "无直连回退") {
		t.Errorf("错误响应 %q 应注明路径目标无直连回退（fallback-direct 不适用）", w.Body.String())
	}
}

// TestServeHTTPRegistryCacheKeyIncludesHost 验证缓存 key 含入站 Host（t.TempDir 目录）：
// 同一 Host 的同一路径目标第二次 GET X-Cache: HIT 且不再回源；
// 换入站 Host 同路径不共享缓存（再次 MISS 回源），证明 Host 参与缓存 key；
// GET 写下的条目可被同 Host 的 HEAD 复用（读缓存回放响应头，无 body）。
func TestServeHTTPRegistryCacheKeyIncludesHost(t *testing.T) {
	const path = "/v2/adockero/proxy-cache/manifests/latest"
	var rec upstreamRecord
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.URL.RawQuery != "" {
			p += "?" + r.URL.RawQuery
		}
		rec.add(r.Method, p)
		w.Header().Set("Docker-Content-Digest", "sha256:abc123")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "manifest-json") // 13 字节，content 判据可过、可缓存
	}))
	defer upstream.Close()

	mgr := cache.NewManager(context.Background())
	s := newRegistryServer(t,
		`http://registry-[ab]\.example\.com/`, // 同时覆盖 registry-a / registry-b 两个入站 Host
		t.TempDir(),
		[]string{upstream.URL + "/$http.server.header.full_url_no_server"},
		mgr)

	do := func(method, host string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://"+host+path, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}

	// 第一次 GET：MISS 回源
	w1 := do(http.MethodGet, "registry-a.example.com")
	if w1.Code != http.StatusOK || w1.Body.String() != "manifest-json" {
		t.Fatalf("第一次 GET: Code=%d Body=%q, want 200/manifest-json", w1.Code, w1.Body.String())
	}
	if got := w1.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("第一次 GET X-Cache = %q, want %q", got, "MISS")
	}

	// 第二次 GET（同 Host 同路径）：HIT，内容一致，不再回源
	w2 := do(http.MethodGet, "registry-a.example.com")
	if got := w2.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("第二次 GET X-Cache = %q, want %q（缓存 key 含 Host，同 Host 同路径应命中）", got, "HIT")
	}
	if w2.Body.String() != "manifest-json" {
		t.Errorf("HIT Body = %q, want %q", w2.Body.String(), "manifest-json")
	}
	if got := w2.Header().Get("Docker-Content-Digest"); got != "sha256:abc123" {
		t.Errorf("HIT 响应头 Docker-Content-Digest = %q, want 缓存回放 %q", got, "sha256:abc123")
	}
	if n := rec.calls(); n != 1 {
		t.Errorf("第二次 GET 后假上游收到 %d 次请求, want 1（HIT 不应回源）", n)
	}

	// 换入站 Host 同路径：缓存 key 含 Host，不共享条目，应再次 MISS 回源
	w3 := do(http.MethodGet, "registry-b.example.com")
	if got := w3.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("registry-b 首次 GET X-Cache = %q, want %q（Host 参与缓存 key，不共享）", got, "MISS")
	}
	if n := rec.calls(); n != 2 {
		t.Errorf("registry-b 首次 GET 后假上游收到 %d 次请求, want 2", n)
	}

	// 同 Host 的 HEAD 复用 GET 写下的条目：HIT 回放响应头，无 body，不再回源
	w4 := do(http.MethodHead, "registry-a.example.com")
	if got := w4.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("HEAD X-Cache = %q, want %q（复用 GET 条目回放响应头）", got, "HIT")
	}
	if w4.Code != http.StatusOK {
		t.Errorf("HEAD Code = %d, want %d", w4.Code, http.StatusOK)
	}
	if got := w4.Header().Get("Content-Length"); got != "13" {
		t.Errorf("HEAD Content-Length = %q, want 条目 body 长度 %q", got, "13")
	}
	if w4.Body.Len() != 0 {
		t.Errorf("HEAD 回放不应有 body，实际 %d 字节", w4.Body.Len())
	}
	if n := rec.calls(); n != 2 {
		t.Errorf("HEAD 命中缓存后假上游收到 %d 次请求, want 2（不应新增回源）", n)
	}
}
