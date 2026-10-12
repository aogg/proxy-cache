package proxy

// insecure_tls_test.go —— 三级 insecure-skip-verify 的 proxy 侧生效单测（config 侧的
// UnmarshalYAML 与三级优先级解析见 config/redirect_item_test.go）：
//   - ExpandCandidates：占位符展开与斜杠接缝去重语义不变，且每候选 insecureSkipVerify
//     按「item 显式 ?? eff.InsecureSkipVerify」解析（候选显式值覆盖上层）；
//   - newUpstreamTransport：insecure=true → TLSClientConfig.InsecureSkipVerify=true；
//     false → 不设（TLSClientConfig 保持 nil）；
//   - New 预构建：配置含任一 insecure true（全局 flag / 候选 item flag）→ clients map
//     存在 insecure 变体（且 Transport 真正跳过校验）；全 false → 不预建 insecure 变体；
//   - httptest.NewTLSServer 自签证书集成：候选级 insecure=true → 回源成功取到 body；
//     全关 → 回源失败（错误含 tls/x509/certificate 字样即可）。

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestExpandCandidatesInsecureResolution 表驱动验证候选展开的 insecure 解析与展开语义不变。
func TestExpandCandidatesInsecureResolution(t *testing.T) {
	tests := []struct {
		name        string
		effInsecure bool
		item        *bool
		want        bool
	}{
		{name: "eff关_item未配置_继承eff为关", effInsecure: false, item: nil, want: false},
		{name: "eff开_item未配置_继承eff为开", effInsecure: true, item: nil, want: true},
		{name: "eff关_item显式true_候选覆盖上层", effInsecure: false, item: boolPtr(true), want: true},
		{name: "eff关_item显式false_仍为关", effInsecure: false, item: boolPtr(false), want: false},
		{name: "eff开_item显式false_候选覆盖上层", effInsecure: true, item: boolPtr(false), want: false},
		{name: "eff开_item显式true_仍为开", effInsecure: true, item: boolPtr(true), want: true},
	}
	const (
		target  = "v2/x/manifests/latest"
		tpl     = "https://docker.1panel.live/$http.server.header.full_url_no_server"
		wantURL = "https://docker.1panel.live/v2/x/manifests/latest"
	)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet,
				"http://"+slowHost+"/"+target, nil)
			items := []config.URLRedirectItem{{
				Template:           tpl, // 斜杠接缝场景：占位符前紧邻 /
				InsecureSkipVerify: tt.item,
			}}
			eff := config.Options{InsecureSkipVerify: tt.effInsecure}
			got := ExpandCandidates(items, NewRequestVars(r, target), eff)
			if len(got) != 1 {
				t.Fatalf("应展开出 1 个候选，得到 %d", len(got))
			}
			if got[0].url != wantURL {
				t.Errorf("占位符展开与斜杠接缝去重语义应不变：got %q, want %q", got[0].url, wantURL)
			}
			if got[0].insecureSkipVerify != tt.want {
				t.Errorf("候选 insecureSkipVerify = %v, want %v（item 显式 ?? eff.InsecureSkipVerify=%v）",
					got[0].insecureSkipVerify, tt.want, tt.effInsecure)
			}
		})
	}
}

// TestExpandCandidatesInsecurePerItem 多候选各自独立解析：item 显式值只影响本候选。
func TestExpandCandidatesInsecurePerItem(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/v2/x", nil)
	items := []config.URLRedirectItem{
		{Template: "https://a.example.com/$1"},
		{Template: "https://b.example.com/$1", InsecureSkipVerify: boolPtr(true)},
		{Template: "https://c.example.com/$1", InsecureSkipVerify: boolPtr(false)},
	}
	got := ExpandCandidates(items, NewRequestVars(r, "v2/x"), config.Options{InsecureSkipVerify: false})
	want := []bool{false, true, false}
	for i, w := range want {
		if got[i].insecureSkipVerify != w {
			t.Errorf("候选[%d] insecureSkipVerify = %v, want %v（逐候选独立解析）", i, got[i].insecureSkipVerify, w)
		}
	}
	if got[0].url != "https://a.example.com/v2/x" || got[1].url != "https://b.example.com/v2/x" {
		t.Errorf("候选 URL 展开语义应不变，得到 %q / %q", got[0].url, got[1].url)
	}
}

// TestNewUpstreamTransportInsecure insecure 落地点：true 时设置 TLSClientConfig.InsecureSkipVerify，
// false 时不设（TLSClientConfig 保持 nil，不引入多余 TLS 配置）。
func TestNewUpstreamTransportInsecure(t *testing.T) {
	tr, err := newUpstreamTransport("", time.Second, true)
	if err != nil {
		t.Fatalf("构建 insecure Transport 失败: %v", err)
	}
	if tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Errorf("insecure=true 应设置 TLSClientConfig.InsecureSkipVerify=true，得到 %+v", tr.TLSClientConfig)
	}

	tr2, err := newUpstreamTransport("", time.Second, false)
	if err != nil {
		t.Fatalf("构建常规 Transport 失败: %v", err)
	}
	if tr2.TLSClientConfig != nil {
		t.Errorf("insecure=false 不应设置 TLSClientConfig，得到 %+v", tr2.TLSClientConfig)
	}
}

// insecureVariants 返回 Server 预构建 clients map 里的 insecure=true 组合键。
func insecureVariants(s *Server) []upstreamClientKey {
	var keys []upstreamClientKey
	for k := range s.clients {
		if k.insecure {
			keys = append(keys, k)
		}
	}
	return keys
}

// TestNewPrebuildsInsecureVariantGlobalFlag 全局 insecure-skip-verify=true：
// New 预构建 insecure=true 变体（同一 (proxy, connect-timeout) 组合两种各一），
// 且其 Transport 真正跳过证书校验；Server.client 对该组合返回预构建实例。
func TestNewPrebuildsInsecureVariantGlobalFlag(t *testing.T) {
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.InsecureSkipVerify = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	if n := len(insecureVariants(s)); n == 0 {
		t.Fatal("配置含 insecure true 时，New 应预构建 insecure=true 客户端变体")
	}
	for _, k := range insecureVariants(s) {
		cl := s.clients[k]
		tr, ok := cl.Transport.(*http.Transport)
		if !ok || tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
			t.Errorf("insecure 变体 %v 的 Transport 应设置 InsecureSkipVerify=true", k)
		}
	}
	// 同组合经 Server.client 获取应命中预构建实例（不惰性补建）
	ct := cfg.GlobalConnectTimeout()
	if got := s.client("", ct, true); got != s.clients[upstreamClientKey{proxy: "", connectTimeout: ct, insecure: true}] {
		t.Error("Server.client(…, insecure=true) 应返回 New 预构建的 insecure 变体")
	}
	if got := s.client("", ct, false); got == nil {
		t.Error("insecure=false 变体同样应预构建")
	}
}

// TestNewPrebuildsInsecureVariantCandidateItem 仅候选对象显式 insecure-skip-verify=true
// （全局/规则均 false）：同样触发 New 预构建 insecure 变体（候选级显式开启算「配置里存在
// 任一 true」）。
func TestNewPrebuildsInsecureVariantCandidateItem(t *testing.T) {
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.DomainRules = []config.DomainRule{{
		Name:  "tls-rule",
		Match: `http://reg\.example\.com/`,
		URLRedirect: &[]config.URLRedirectItem{{
			Template:           "https://mirror.example.com/$1",
			InsecureSkipVerify: boolPtr(true),
		}},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	if n := len(insecureVariants(s)); n == 0 {
		t.Fatal("候选 item 显式 insecure=true 时，New 应预构建 insecure=true 变体")
	}
	for _, k := range insecureVariants(s) {
		cl := s.clients[k]
		tr := cl.Transport.(*http.Transport)
		if tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
			t.Errorf("insecure 变体 %v 的 Transport 应设置 InsecureSkipVerify=true", k)
		}
	}
}

// TestNewNoInsecureVariantWhenAllOff 配置里没有任何 insecure true：
// clients map 不应出现 insecure=true 变体（不建多余 Transport）。
func TestNewNoInsecureVariantWhenAllOff(t *testing.T) {
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.InsecureSkipVerify = false
	cfg.DomainRules = []config.DomainRule{{
		Name:  "plain-rule",
		Match: `http://reg\.example\.com/`,
		URLRedirect: &[]config.URLRedirectItem{{
			Template:           "https://mirror.example.com/$1",
			InsecureSkipVerify: boolPtr(false), // 显式 false 也不算开启
		}},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	if keys := insecureVariants(s); len(keys) != 0 {
		t.Errorf("全关配置不应预构建 insecure 变体，实际存在 %v", keys)
	}
	if len(s.clients) == 0 {
		t.Error("insecure=false 基础变体仍应预构建")
	}
}

// TestInsecureCandidateTLSIntegration httptest.NewTLSServer 自签证书集成：
//   - 候选级 insecure=true：该候选走 insecure 客户端，回源成功取到 body；
//   - 全关：TLS 校验生效，回源失败（错误含 tls/x509/certificate 字样即可）。
func TestInsecureCandidateTLSIntegration(t *testing.T) {
	tlsUp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "15")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tls-secret-body"))
	}))
	defer tlsUp.Close()

	const target = "https://private.example.com/x.yaml"

	t.Run("候选级insecure开启_自签回源成功", func(t *testing.T) {
		s := newTimeoutTestServer(t)
		eff := effFor([]config.URLRedirectItem{{
			Template:           tlsUp.URL + "/$1",
			InsecureSkipVerify: boolPtr(true),
		}}, func(o *config.Options) { o.FallbackDirect = false })
		r := httptest.NewRequest(http.MethodGet, "/"+target, nil)
		e, err := s.fetchViaCandidates(r, target, eff)
		if err != nil {
			t.Fatalf("候选级 insecure=true 应信任自签证书回源成功: %v", err)
		}
		if string(e.Body) != "tls-secret-body" {
			t.Errorf("Body = %q, want %q", string(e.Body), "tls-secret-body")
		}
		if want := tlsUp.URL + "/" + target; e.Source != want {
			t.Errorf("Source = %q, want 候选 %q", e.Source, want)
		}
	})

	t.Run("全关_自签证书回源失败", func(t *testing.T) {
		s := newTimeoutTestServer(t)
		eff := effFor([]config.URLRedirectItem{{
			Template: tlsUp.URL + "/$1", // item 未配置 insecure → 继承上层 false
		}}, func(o *config.Options) { o.FallbackDirect = false })
		r := httptest.NewRequest(http.MethodGet, "/"+target, nil)
		_, err := s.fetchViaCandidates(r, target, eff)
		if err == nil {
			t.Fatal("自签证书在全关配置下应回源失败")
		}
		low := strings.ToLower(err.Error())
		if !strings.Contains(low, "tls") && !strings.Contains(low, "x509") && !strings.Contains(low, "certificate") {
			t.Errorf("错误信息应含 tls/x509/certificate 字样，得到: %v", err)
		}
	})
}
