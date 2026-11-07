package proxy

// html_page_guard_test.go —— registry 路径目标的「HTML 反爬页防误判」单测（probe.go 的
// htmlPageContentType / probePassed / probeHardFailReason 与 proxy.go finalizeFetchEntry
// 写缓存兜底）：
// 现场动机（2026-11-07 deepseek-harness:my 拉取失败）：dockerpull.cn 对 registry API 路径
// 一律回 200 + text/html 中文反爬页，旧 content 判据只看状态码与非空首块被判成功并写入
// 缓存 → docker 把 text/html 当 manifest →「unexpected media type text/html」整次拉取
// 失败，且毒缓存 TTL（666h）内所有重试全部命中。
// 全部 httptest 假上游，经 Server.ServeHTTP 走完整链路（与 registry_test.go 同风格），
// 不含任何 e2e。覆盖点：
//   1. HTML 候选 + 合法 manifest 候选竞速：HTML 候选硬失败（恰 1 次请求，无原地重试），
//      合法候选胜出（Content-Type / body / X-Proxy-Upstream 三重断言）；
//   2. 仅 HTML 候选（缓存关闭）：客户端 502 JSON 错误体，绝不透传 HTML（无 4xx 载荷，
//      不带 X-Proxy-Upstream）；
//   3. 缓存防毒化（缓存开启）：两次请求都回源 502、上游被请求 2 次（若毒化则第二次为
//      200 HIT、上游仅 1 次）；
//   4. web 完整 URL 目标的 HTML 是合法载荷：照常 200 透传（pathTarget=false 不受判据影响）；
//   5. HEAD manifest 的 text/html 同样被拒（HEAD 无 body，纯靠 Content-Type 判定）。
//   6. 白盒：htmlPageContentType 对 text/html;charset=utf-8 / 前导空白 / 大小写 /
//      application/xhtml+xml 命中，对 blob / manifest / 空 / 纯 JSON 不命中。

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"proxy-cache/config"
)

// typedUpstream 带请求计数与可配 Content-Type 的假上游（HTML 反爬页 / 合法 manifest 通用）。
type typedUpstream struct {
	mu          sync.Mutex
	hits        int
	status      int
	contentType string
	body        string
}

func (u *typedUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.hits++
	status, ct, body := u.status, u.contentType, u.body
	u.mu.Unlock()
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func (u *typedUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

// newHTMLUpstream 启动一个恒返回 200 + text/html 反爬页的假上游（模拟 dockerpull.cn 现场）。
func newHTMLUpstream(t *testing.T) (*typedUpstream, *httptest.Server) {
	t.Helper()
	u := &typedUpstream{status: http.StatusOK, contentType: "text/html; charset=utf-8",
		body: "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"></head><body>加速页</body></html>"}
	srv := httptest.NewServer(u)
	t.Cleanup(srv.Close)
	return u, srv
}

// newManifestUpstreamTyped 启动一个恒返回 200 + 合法 manifest 的假上游。
func newManifestUpstreamTyped(t *testing.T) (*typedUpstream, *httptest.Server) {
	t.Helper()
	u := &typedUpstream{status: http.StatusOK,
		contentType: "application/vnd.docker.distribution.manifest.v2+json",
		body:        `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{},"layers":[]}`}
	srv := httptest.NewServer(u)
	t.Cleanup(srv.Close)
	return u, srv
}

// TestHTMLPageCandidateRejected 覆盖点 1：HTML 候选硬失败、合法候选胜出。
func TestHTMLPageCandidateRejected(t *testing.T) {
	htmlU, htmlSrv := newHTMLUpstream(t)
	goodU, goodSrv := newManifestUpstreamTyped(t)

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{
			{Template: htmlSrv.URL + "/$http.server.header.full_url_no_server"},
			{Template: goodSrv.URL + "/$http.server.header.full_url_no_server"},
		},
		nil)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, referrersRequest()) // 路径目标（GET /v2/.../referrers/<digest>）

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 合法候选胜出 200, body: %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != goodU.body {
		t.Errorf("Body = %q, want 合法候选的 manifest body %q（绝不能是 HTML 反爬页）", got, goodU.body)
	}
	if got := w.Header().Get("Content-Type"); got != goodU.contentType {
		t.Errorf("Content-Type = %q, want %q（透传合法候选响应头）", got, goodU.contentType)
	}
	wantUpstream := goodSrv.URL + "/" + referrersTarget()
	if got := w.Header().Get("X-Proxy-Upstream"); got != wantUpstream {
		t.Errorf("X-Proxy-Upstream = %q, want 胜出候选 %q", got, wantUpstream)
	}
	if n := htmlU.count(); n != 1 {
		t.Errorf("HTML 候选收到 %d 次请求, want 1（hard-fail: html-error-page 跳过原地重试）", n)
	}
	if n := goodU.count(); n != 1 {
		t.Errorf("合法候选收到 %d 次请求, want 1", n)
	}
}

// TestHTMLPageAllCandidatesFailStill502 覆盖点 2：仅 HTML 候选 → 502 JSON 错误体，不透传 HTML。
func TestHTMLPageAllCandidatesFailStill502(t *testing.T) {
	_, htmlSrv := newHTMLUpstream(t)

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{{Template: htmlSrv.URL + "/$http.server.header.full_url_no_server"}},
		nil)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, referrersRequest())

	if w.Code != http.StatusBadGateway {
		t.Fatalf("Code = %d, want %d（全候选失败维持 502，绝不透传 text/html 200）, body: %s",
			w.Code, http.StatusBadGateway, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "<html") || w.Header().Get("Content-Type") == "text/html; charset=utf-8" {
		t.Errorf("响应不应是 HTML 反爬页透传: Content-Type=%q body=%q",
			w.Header().Get("Content-Type"), w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"error"`) {
		t.Errorf("body = %q, want JSON 502 错误体", w.Body.String())
	}
	if w.Header().Get("X-Proxy-Upstream") != "" {
		t.Errorf("X-Proxy-Upstream = %q, want 不设置（200 HTML 非 4xx，无透传载荷）",
			w.Header().Get("X-Proxy-Upstream"))
	}
}

// TestHTMLPageNotCached 覆盖点 3：缓存开启时 HTML 候选全败 → 拒绝写缓存，第二次请求仍回源。
func TestHTMLPageNotCached(t *testing.T) {
	htmlU, htmlSrv := newHTMLUpstream(t)
	cacheDir := t.TempDir()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		cacheDir,
		[]config.URLRedirectItem{{Template: htmlSrv.URL + "/$http.server.header.full_url_no_server"}},
		nil)

	for i := 1; i <= 2; i++ {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, referrersRequest())
		if w.Code != http.StatusBadGateway {
			t.Fatalf("第 %d 次请求 Code = %d, want 502（HTML 未过判据不写缓存，不得出现 200 HIT）",
				i, w.Code)
		}
	}
	if n := htmlU.count(); n != 2 {
		t.Errorf("上游收到 %d 次请求, want 2（每次都回源；若被毒化缓存则第二次 HIT 仍为 1）", n)
	}
	// 缓存目录不应有任何落盘条目（含子目录 cache-docker）
	_ = filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Base(path) != filepath.Base(cacheDir) {
			t.Errorf("缓存目录出现意外文件 %s（HTML 反爬页被写入了缓存）", path)
		}
		return nil
	})
}

// TestHTMLPageWebTargetStillOK 覆盖点 4：web 完整 URL 目标的 HTML 是合法载荷，照常透传。
func TestHTMLPageWebTargetStillOK(t *testing.T) {
	htmlU, htmlSrv := newHTMLUpstream(t)

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{{Template: htmlSrv.URL + "/$http.server.header.full_url_no_server"}},
		nil)

	target := "https://example.com/some/page.html"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://registry-proxy-cache.linkease.net:5480/"+target, nil)
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200（完整 URL 目标 HTML 是合法载荷，不受判据影响）, body: %s",
			w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != htmlU.body {
		t.Errorf("Body = %q, want 原样透传 HTML 载荷 %q", got, htmlU.body)
	}
	if got := w.Header().Get("Content-Type"); got != htmlU.contentType {
		t.Errorf("Content-Type = %q, want %q", got, htmlU.contentType)
	}
}

// TestHTMLPageHeadRejected 覆盖点 5：HEAD manifest 的 text/html 同样被拒（无 body 纯靠头判定）。
func TestHTMLPageHeadRejected(t *testing.T) {
	htmlU, htmlSrv := newHTMLUpstream(t)
	goodU, goodSrv := newManifestUpstreamTyped(t)

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{
			{Template: htmlSrv.URL + "/$http.server.header.full_url_no_server"},
			{Template: goodSrv.URL + "/$http.server.header.full_url_no_server"},
		},
		nil)

	req := httptest.NewRequest(http.MethodHead, "http://"+hostDockerRegistry+"/"+referrersTarget(), nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 合法候选胜出 200", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != goodU.contentType {
		t.Errorf("Content-Type = %q, want %q（HEAD 同样不得回放 text/html）", got, goodU.contentType)
	}
	if n := htmlU.count(); n != 1 {
		t.Errorf("HTML 候选收到 %d 次请求, want 1（HEAD 探测同样硬失败切候选）", n)
	}
}

// TestHTMLPageContentTypeHelper 覆盖点 6：htmlPageContentType 边界。
func TestHTMLPageContentTypeHelper(t *testing.T) {
	tests := []struct {
		ct   string
		want bool
	}{
		{"text/html", true},
		{"text/html; charset=utf-8", true},
		{"  Text/HTML  ", true},
		{"application/xhtml+xml", true},
		{"application/xhtml+xml; charset=utf-8", true},
		{"application/vnd.docker.distribution.manifest.v2+json", false},
		{"application/vnd.oci.image.index.v1+json", false},
		{"application/octet-stream", false},
		{"application/json", false},
		{"", false},
		{"text/plain", false},
		{"text/htmlfoo", true}, // 前缀匹配：text/html 前缀即判 HTML（含 text/html-custom 场景）
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("ct=%q", tt.ct), func(t *testing.T) {
			if got := htmlPageContentType(tt.ct); got != tt.want {
				t.Errorf("htmlPageContentType(%q) = %v, want %v", tt.ct, got, tt.want)
			}
		})
	}
}
