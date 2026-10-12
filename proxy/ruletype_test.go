package proxy

// ruletype_test.go —— domain-rules 规则 type 请求类型分流的全链路单测：
// 全部用 httptest 假上游（不起真实服务、不发真实外网请求），经 Server.ServeHTTP
// 走 handleProxy 完整链路（ExtractTarget -> ACL -> 域名规则合并（含 type 过滤）-> 回源）：
//   - 路径目标请求（GET /v2/...，带 Host）命中 type: docker 规则：经规则 url-redirect
//     回源本地假上游 200；规则归属由 X-Proxy-Upstream（规则候选模板展开出的上游地址）、
//     上游收到的路径模板特征与服务日志 rule=<规则名> 三重断言；同 Host 第二次 GET
//     命中规则独立缓存目录（HIT 不再回源）；
//   - 同一 Host 的完整 URL 请求在同配置下不命中 type: docker 规则：另配 type: web 规则时
//     命中它（docker 上游零请求）；仅 docker 规则时落全局默认（X-Cache: BYPASS，
//     不写 docker 规则的独立缓存目录，经 fallback-direct 直连本地假上游）；
//   - 回归：旧配置（规则无 type 字段）两类请求行为不变（留空 = 不过滤请求类型）。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// ruletype 测试专用常量：入站 Host 与两类目标串。
// match 正则只写「http://<Host>/」前缀，对路径目标与完整 URL 两类请求串恒命中，
// 从而让「命中哪条规则」只由 type 过滤决定。
const (
	hostRuleType    = "registry-proxy-cache.example.com:5480"    // 入站 Host（docker registry 场景）
	pathManifestRT  = "v2/adockero/proxy-cache/manifests/latest" // 路径目标（无前导斜杠，ExtractTarget 语义）
	fullTargetRT    = "https://example.com/some/file.yaml"       // 完整 URL 请求的目标
	ruleTypeRegexRT = `http://registry-proxy-cache\.example\.com:5480/`
)

// rtStrPtr / rtBoolPtr 生成指针，便于规则内写 cache 覆盖项（proxy 包测试尚无同名 helper）。
func rtStrPtr(s string) *string { return &s }
func rtBoolPtr(b bool) *bool    { return &b }

// newManifestUpstream 启动一个返回固定 body 的假上游（带 Docker-Content-Digest 响应头），
// 并把收到的 (method, path) 记入 rec；生命周期挂到 t.Cleanup。
func newManifestUpstream(t *testing.T, rec *upstreamRecord, body string) *httptest.Server {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.URL.RawQuery != "" {
			p += "?" + r.URL.RawQuery
		}
		rec.add(r.Method, p)
		w.Header().Set("Docker-Content-Digest", "sha256:abc123")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(upstream.Close)
	return upstream
}

// dockerTypeRule 构造一条 type: docker 的规则：独立缓存目录（显式开启缓存），
// url-redirect 以 full_url_no_server 指向 docker 假上游（展开为路径目标本身）。
func dockerTypeRule(upstreamURL, cacheDir string) config.DomainRule {
	tpl := upstreamURL + "/$http.server.header.full_url_no_server"
	return config.DomainRule{
		Name:        "docker-registry",
		Type:        config.RuleTypeDocker,
		Match:       ruleTypeRegexRT,
		URLRedirect: &[]config.URLRedirectItem{{Template: tpl}},
		Cache:       config.RuleCache{Enabled: rtBoolPtr(true), Path: rtStrPtr(cacheDir)},
	}
}

// newRuleTypeProxyServer 构建带若干条规则（可带 type）的代理 Server：
// globalCacheDir 非空时启用全局缓存（目录 = globalCacheDir），为空时关闭全局缓存；
// 服务日志写入 logBuf，便于断言命中规则的归属（成功日志含 rule=<规则名>）。
func newRuleTypeProxyServer(t *testing.T, logBuf *bytes.Buffer, rules []config.DomainRule, globalCacheDir string, caches *cache.Manager) *Server {
	t.Helper()
	cfg := config.Default()
	if globalCacheDir != "" {
		cfg.Cache.Path = globalCacheDir
	} else {
		cfg.Cache.Enabled = false
	}
	cfg.DomainRules = rules
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, caches, captureLogger(logBuf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// TestServeHTTPRuleTypeDockerPathTarget 验证路径目标请求命中 type: docker 规则的全链路：
// GET /v2/...（带 Host）经规则 url-redirect 回源假上游 200；第二次 GET 命中规则
// 独立缓存目录（HIT），假上游只收到第一次的回源请求。
func TestServeHTTPRuleTypeDockerPathTarget(t *testing.T) {
	var rec upstreamRecord
	dockerUp := newManifestUpstream(t, &rec, "manifest-json")

	ruleCacheDir := t.TempDir()
	var logBuf bytes.Buffer
	s := newRuleTypeProxyServer(t, &logBuf,
		[]config.DomainRule{dockerTypeRule(dockerUp.URL, ruleCacheDir)},
		t.TempDir(), // 全局缓存目录（本用例只有 docker 类请求命中规则，不会写到全局目录）
		cache.NewManager(context.Background()))

	do := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet,
			"http://"+hostRuleType+"/"+pathManifestRT, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}

	// 第一次 GET：MISS 回源，命中 docker 规则
	w1 := do()
	if w1.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（body: %s）", w1.Code, http.StatusOK, w1.Body.String())
	}
	if got := w1.Body.String(); got != "manifest-json" {
		t.Errorf("Body = %q, want 透传 %q", got, "manifest-json")
	}
	if want := dockerUp.URL + "/" + pathManifestRT; w1.Header().Get("X-Proxy-Upstream") != want {
		t.Errorf("X-Proxy-Upstream = %q, want docker 规则候选 %q（规则归属可观测）", w1.Header().Get("X-Proxy-Upstream"), want)
	}
	if got := w1.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want %q（应写入 docker 规则的独立缓存目录）", got, "MISS")
	}
	if !strings.Contains(logBuf.String(), "rule=docker-registry") {
		t.Errorf("服务日志应包含 rule=docker-registry（命中 docker 规则），实际: %s", logBuf.String())
	}
	if n := rec.calls(); n != 1 {
		t.Fatalf("假上游收到 %d 次请求, want 1", n)
	}
	if m, p := rec.last(); m != http.MethodGet || p != "/"+pathManifestRT {
		t.Errorf("上游收到 %s %q, want GET %q（url-redirect 路径模板展开且无双斜杠）", m, p, "/"+pathManifestRT)
	}

	// 第二次 GET（同 Host 同路径）：HIT，读 docker 规则独立目录的条目，不再回源
	w2 := do()
	if got := w2.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("第二次 GET X-Cache = %q, want %q（docker 规则独立缓存目录应生效）", got, "HIT")
	}
	if w2.Body.String() != "manifest-json" {
		t.Errorf("HIT Body = %q, want %q", w2.Body.String(), "manifest-json")
	}
	if n := rec.calls(); n != 1 {
		t.Errorf("第二次 GET 后假上游收到 %d 次请求, want 1（HIT 不应回源）", n)
	}
}

// TestServeHTTPRuleTypeDockerFullURLNotMatch 验证同一 Host 的完整 URL 请求
// 在同配置下不命中 type: docker 规则：
//   - 另配 type: web 规则时命中 web 规则（经其候选回源），docker 上游零请求；
//   - 仅 docker 规则时落全局默认：全局关缓存 → X-Cache: BYPASS，docker 规则的
//     独立缓存目录不被写入，无候选时按 fallback-direct 直连本地假上游。
func TestServeHTTPRuleTypeDockerFullURLNotMatch(t *testing.T) {
	t.Run("另配web规则_完整URL请求命中web规则", func(t *testing.T) {
		var dockerRec, webRec upstreamRecord
		dockerUp := newManifestUpstream(t, &dockerRec, "manifest-json")
		webUp := newManifestUpstream(t, &webRec, "web-content")

		webTemplates := []config.URLRedirectItem{{Template: webUp.URL + "/$1"}}
		webRule := config.DomainRule{
			Name:        "web-rule",
			Type:        config.RuleTypeWeb,
			Match:       ruleTypeRegexRT,
			URLRedirect: &webTemplates,
		}
		var logBuf bytes.Buffer
		s := newRuleTypeProxyServer(t, &logBuf,
			[]config.DomainRule{dockerTypeRule(dockerUp.URL, t.TempDir()), webRule},
			"", cache.NewManager(context.Background())) // 全局关缓存：web 规则未配 cache 沿用全局

		r := httptest.NewRequest(http.MethodGet,
			"http://"+hostRuleType+"/"+fullTargetRT, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Body.String(); got != "web-content" {
			t.Errorf("Body = %q, want %q", got, "web-content")
		}
		if want := webUp.URL + "/" + fullTargetRT; w.Header().Get("X-Proxy-Upstream") != want {
			t.Errorf("X-Proxy-Upstream = %q, want web 规则候选 %q", w.Header().Get("X-Proxy-Upstream"), want)
		}
		if !strings.Contains(logBuf.String(), "rule=web-rule") {
			t.Errorf("服务日志应包含 rule=web-rule（命中 web 规则而非 docker 规则），实际: %s", logBuf.String())
		}
		if n := dockerRec.calls(); n != 0 {
			t.Errorf("docker 假上游收到 %d 次请求, want 0（完整 URL 请求不应命中 type: docker 规则）", n)
		}
		if n := webRec.calls(); n != 1 {
			t.Errorf("web 假上游收到 %d 次请求, want 1", n)
		}
	})

	t.Run("仅docker规则_完整URL请求落全局默认_不写docker缓存目录", func(t *testing.T) {
		var dockerRec, directRec upstreamRecord
		dockerUp := newManifestUpstream(t, &dockerRec, "manifest-json")
		// 直连目标必须是本地 httptest（禁止真实外网）：完整 URL 请求落全局默认后
		// 无候选，按 fallback-direct=true 直连该地址
		directUp := newManifestUpstream(t, &directRec, "direct-content")
		directTarget := directUp.URL + "/x"

		ruleCacheDir := t.TempDir()
		var logBuf bytes.Buffer
		s := newRuleTypeProxyServer(t, &logBuf,
			[]config.DomainRule{dockerTypeRule(dockerUp.URL, ruleCacheDir)},
			"", cache.NewManager(context.Background())) // 全局关缓存：docker 规则的 cache.enabled 只对 docker 类请求生效

		r := httptest.NewRequest(http.MethodGet,
			"http://"+hostRuleType+"/"+directTarget, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Body.String(); got != "direct-content" {
			t.Errorf("Body = %q, want 直连回源 %q", got, "direct-content")
		}
		if got := w.Header().Get("X-Cache"); got != "BYPASS" {
			t.Errorf("X-Cache = %q, want %q（落全局默认：全局关缓存，docker 规则覆盖不应生效）", got, "BYPASS")
		}
		if n := dockerRec.calls(); n != 0 {
			t.Errorf("docker 假上游收到 %d 次请求, want 0（完整 URL 请求不应命中 type: docker 规则）", n)
		}
		if entries, err := os.ReadDir(ruleCacheDir); err == nil && len(entries) > 0 {
			t.Errorf("docker 规则独立缓存目录 %s 不应被完整 URL 请求写入，实际有 %d 个条目", ruleCacheDir, len(entries))
		}
		if n := directRec.calls(); n != 1 {
			t.Fatalf("直连假上游收到 %d 次请求, want 1（fallback-direct 直连）", n)
		}
		if m, p := directRec.last(); m != http.MethodGet || p != "/x" {
			t.Errorf("直连假上游收到 %s %q, want GET %q", m, p, "/x")
		}
		if strings.Contains(logBuf.String(), "rule=docker-registry") {
			t.Errorf("服务日志不应出现 rule=docker-registry（完整 URL 请求不命中该规则），实际: %s", logBuf.String())
		}
	})
}

// TestServeHTTPRuleTypeEmptyLegacyBehavior 回归旧配置（规则无 type 字段）：
// 留空 = 不过滤请求类型，同一规则对路径目标与完整 URL 请求都能命中，行为不变。
func TestServeHTTPRuleTypeEmptyLegacyBehavior(t *testing.T) {
	var rec upstreamRecord
	upstream := newManifestUpstream(t, &rec, "legacy-content")

	templates := []config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}}
	legacyRule := config.DomainRule{
		Name:        "legacy-rule", // 旧配置写法：不配置 type 字段
		Match:       ruleTypeRegexRT,
		URLRedirect: &templates,
	}
	var logBuf bytes.Buffer
	s := newRuleTypeProxyServer(t, &logBuf,
		[]config.DomainRule{legacyRule}, "", nil) // 全局关缓存，聚焦规则命中语义

	t.Run("路径目标请求_命中规则回源成功", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://"+hostRuleType+"/"+pathManifestRT, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Body.String(); got != "legacy-content" {
			t.Errorf("Body = %q, want %q", got, "legacy-content")
		}
		if want := upstream.URL + "/" + pathManifestRT; w.Header().Get("X-Proxy-Upstream") != want {
			t.Errorf("X-Proxy-Upstream = %q, want 规则候选 %q", w.Header().Get("X-Proxy-Upstream"), want)
		}
		if !strings.Contains(logBuf.String(), "rule=legacy-rule") {
			t.Errorf("服务日志应包含 rule=legacy-rule（无 type 规则照常命中路径目标），实际: %s", logBuf.String())
		}
	})

	t.Run("完整URL请求_同一规则同样命中", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://"+hostRuleType+"/"+fullTargetRT, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
		}
		if want := upstream.URL + "/" + fullTargetRT; w.Header().Get("X-Proxy-Upstream") != want {
			t.Errorf("X-Proxy-Upstream = %q, want 规则候选 %q（留空 = 不过滤请求类型）", w.Header().Get("X-Proxy-Upstream"), want)
		}
		if n := rec.calls(); n != 2 {
			t.Errorf("假上游累计收到 %d 次请求, want 2（两类请求都经同一规则回源）", n)
		}
	})
}
