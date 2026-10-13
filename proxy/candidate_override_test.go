package proxy

// candidate_override_test.go —— 候选级 http-proxy 与超时覆盖的 proxy 侧生效单测
//（config 侧的解析/三级优先级/Validate 见 config/redirect_item_test.go）：
//   - ExpandCandidates：候选 httpProxy / connectTimeout / httpTimeout / readTimeout /
//     probeTimeout 按「item 显式 ?? eff（规则 ?? 全局）」逐候选解析——候选显式空串代理 =
//     强制直连、显式 0 超时 = 不限制，多候选互相独立；
//   - New 预构建：候选显式 http-proxy / connect-timeout 解析出的 (proxy, connect-timeout)
//     组合客户端在启动期构建（全局候选列表按全局 eff、规则候选列表按该规则 eff 解析），
//     且 Server.client 对这些组合命中预构建实例（不惰性补建）。

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestExpandCandidatesProxyTimeoutResolution 表驱动验证候选展开的 http-proxy 与超时解析：
// item 显式指针非 nil 时取 item 值（空串代理 = 强制直连、0 超时 = 不限制），
// nil 时逐候选回落 eff（规则 ?? 全局的合并结果）。
func TestExpandCandidatesProxyTimeoutResolution(t *testing.T) {
	eff := config.Options{
		HTTPProxy:               "http://eff.proxy:7890",
		ConnectTimeout:          3 * time.Second,
		HTTPTimeout:             time.Minute,
		ReadTimeout:             30 * time.Second,
		URLRedirectProbeTimeout: 5 * time.Second,
	}
	tests := []struct {
		name string
		item config.URLRedirectItem
		want candidate // 期望解析出的候选级生效值（url 字段不参与断言）
	}{
		{
			name: "item全未配置_逐级继承eff",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1"},
			want: candidate{httpProxy: eff.HTTPProxy, connectTimeout: eff.ConnectTimeout,
				httpTimeout: eff.HTTPTimeout, readTimeout: eff.ReadTimeout, probeTimeout: eff.URLRedirectProbeTimeout},
		},
		{
			name: "item显式代理_覆盖eff",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1", HTTPProxy: strPtr("socks5://item.proxy:1080")},
			want: candidate{httpProxy: "socks5://item.proxy:1080", connectTimeout: eff.ConnectTimeout,
				httpTimeout: eff.HTTPTimeout, readTimeout: eff.ReadTimeout, probeTimeout: eff.URLRedirectProbeTimeout},
		},
		{
			name: "item显式空串_强制直连覆盖eff代理",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1", HTTPProxy: strPtr("")},
			want: candidate{httpProxy: "", connectTimeout: eff.ConnectTimeout,
				httpTimeout: eff.HTTPTimeout, readTimeout: eff.ReadTimeout, probeTimeout: eff.URLRedirectProbeTimeout},
		},
		{
			name: "item显式空白串_trim后强制直连",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1", HTTPProxy: strPtr("   ")},
			want: candidate{httpProxy: "", connectTimeout: eff.ConnectTimeout,
				httpTimeout: eff.HTTPTimeout, readTimeout: eff.ReadTimeout, probeTimeout: eff.URLRedirectProbeTimeout},
		},
		{
			name: "item显式connect与http超时_覆盖对应维度其余继承",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1",
				ConnectTimeout: durPtr(15 * time.Second), HTTPTimeout: durPtr(10 * time.Minute)},
			want: candidate{httpProxy: eff.HTTPProxy, connectTimeout: 15 * time.Second,
				httpTimeout: 10 * time.Minute, readTimeout: eff.ReadTimeout, probeTimeout: eff.URLRedirectProbeTimeout},
		},
		{
			name: "item显式0超时_不限制覆盖上层预算",
			item: config.URLRedirectItem{Template: "https://m.example.com/$1",
				ConnectTimeout: durPtr(0), ReadTimeout: durPtr(0), ProbeTimeout: durPtr(0)},
			want: candidate{httpProxy: eff.HTTPProxy, connectTimeout: 0,
				httpTimeout: eff.HTTPTimeout, readTimeout: 0, probeTimeout: 0},
		},
	}
	const target = "v2/x/manifests/latest"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/"+target, nil)
			got := ExpandCandidates([]config.URLRedirectItem{tt.item}, NewRequestVars(r, target), eff)
			if len(got) != 1 {
				t.Fatalf("应展开出 1 个候选，得到 %d", len(got))
			}
			c := got[0]
			if c.httpProxy != tt.want.httpProxy {
				t.Errorf("候选 httpProxy = %q, want %q（item 显式 ?? eff=%q）", c.httpProxy, tt.want.httpProxy, eff.HTTPProxy)
			}
			if c.connectTimeout != tt.want.connectTimeout {
				t.Errorf("候选 connectTimeout = %s, want %s", c.connectTimeout, tt.want.connectTimeout)
			}
			if c.httpTimeout != tt.want.httpTimeout {
				t.Errorf("候选 httpTimeout = %s, want %s", c.httpTimeout, tt.want.httpTimeout)
			}
			if c.readTimeout != tt.want.readTimeout {
				t.Errorf("候选 readTimeout = %s, want %s", c.readTimeout, tt.want.readTimeout)
			}
			if c.probeTimeout != tt.want.probeTimeout {
				t.Errorf("候选 probeTimeout = %s, want %s", c.probeTimeout, tt.want.probeTimeout)
			}
		})
	}
}

// TestExpandCandidatesProxyTimeoutPerItem 多候选各自独立解析：item 显式值只影响本候选，
// 其余候选继承 eff（慢代理候选走指定代理放宽超时、直连候选互不干扰）。
func TestExpandCandidatesProxyTimeoutPerItem(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/v2/x", nil)
	eff := config.Options{
		HTTPProxy:               "http://eff.proxy:7890",
		ConnectTimeout:          3 * time.Second,
		HTTPTimeout:             time.Minute,
		ReadTimeout:             30 * time.Second,
		URLRedirectProbeTimeout: 5 * time.Second,
	}
	items := []config.URLRedirectItem{
		{Template: "https://a.example.com/$1"}, // 全继承 eff
		{Template: "https://b.example.com/$1", // 慢代理候选：走 socks5 并放宽全部预算
			HTTPProxy:      strPtr("socks5://slow.proxy:1080"),
			ConnectTimeout: durPtr(30 * time.Second), HTTPTimeout: durPtr(20 * time.Minute),
			ReadTimeout: durPtr(2 * time.Minute), ProbeTimeout: durPtr(20 * time.Second)},
		{Template: "https://c.example.com/$1", HTTPProxy: strPtr("")}, // 强制直连（仅代理维度覆盖）
	}
	got := ExpandCandidates(items, NewRequestVars(r, "v2/x"), eff)
	if len(got) != 3 {
		t.Fatalf("应展开出 3 个候选，得到 %d", len(got))
	}
	if got[0].httpProxy != eff.HTTPProxy || got[0].connectTimeout != eff.ConnectTimeout ||
		got[0].probeTimeout != eff.URLRedirectProbeTimeout {
		t.Errorf("候选0 应全量继承 eff，得到 %+v", got[0])
	}
	if got[1].httpProxy != "socks5://slow.proxy:1080" || got[1].connectTimeout != 30*time.Second ||
		got[1].httpTimeout != 20*time.Minute || got[1].readTimeout != 2*time.Minute ||
		got[1].probeTimeout != 20*time.Second {
		t.Errorf("候选1 应取 item 显式值，得到 %+v", got[1])
	}
	if got[2].httpProxy != "" { // 强制直连只覆盖代理维度，超时仍继承 eff
		t.Errorf("候选2 应为强制直连（空代理），得到 %q", got[2].httpProxy)
	}
	if got[2].connectTimeout != eff.ConnectTimeout || got[2].probeTimeout != eff.URLRedirectProbeTimeout {
		t.Errorf("候选2 未显式配置的超时应继承 eff，得到 connect=%s probe=%s", got[2].connectTimeout, got[2].probeTimeout)
	}
}

// TestNewPrebuildsCandidateProxyCombos 候选级 (proxy, connect-timeout) 组合的启动期预构建：
//   - 全局候选列表按全局 eff 解析（候选显式 socks5 + connect 10s → 独立组合）；
//   - 规则候选列表按该规则 eff 解析（规则代理 http://rule.proxy + 候选显式空串/0 →
//     强制直连且 connect 不限制的组合）；
//   - 组合的 Transport 真正携带对应代理与建连超时，且 Server.client 命中预构建实例。
func TestNewPrebuildsCandidateProxyCombos(t *testing.T) {
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.ConnectTimeout = durPtr(3 * time.Second) // 全局建连超时（未显式覆盖的维度沿用）
	cfg.URLRedirect = []config.URLRedirectItem{  // 全局候选：普通候选继承全局组合、慢代理候选显式覆盖
		{Template: "https://a.example.com/$1"},
		{Template: "https://b.example.com/$1",
			HTTPProxy:      strPtr("socks5://cand.proxy:1080"),
			ConnectTimeout: durPtr(10 * time.Second)},
	}
	cfg.DomainRules = []config.DomainRule{{
		Name:      "docker-rule",
		Match:     `http://reg\.example\.com/`,
		HTTPProxy: strPtr("http://rule.proxy:7890"),
		URLRedirect: &[]config.URLRedirectItem{
			{Template: "https://c.example.com/$1"}, // 继承规则组合（与规则 base 去重，不新增）
			{Template: "https://d.example.com/$1", // 强制直连 + 建连不限制
				HTTPProxy:      strPtr(""),
				ConnectTimeout: durPtr(0)},
		},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	// 组合全集：全局直连(3s)、全局候选 socks5(10s)、规则代理(3s)、规则候选强制直连(0)
	wantCombos := []upstreamClientKey{
		{proxy: "", connectTimeout: 3 * time.Second},
		{proxy: "socks5://cand.proxy:1080", connectTimeout: 10 * time.Second},
		{proxy: "http://rule.proxy:7890", connectTimeout: 3 * time.Second},
		{proxy: "", connectTimeout: 0},
	}
	for _, k := range wantCombos {
		cl, ok := s.clients[k]
		if !ok || cl == nil {
			t.Errorf("组合 %+v 应被 New 预构建，实际缺失", k)
			continue
		}
		// Transport 落点检视：代理 URL 与建连/TLS 握手超时必须与组合一致
		tr, ok := cl.Transport.(*http.Transport)
		if !ok {
			t.Errorf("组合 %+v 的 Transport 类型异常: %T", k, cl.Transport)
			continue
		}
		if k.proxy == "" && tr.Proxy != nil {
			t.Errorf("组合 %+v 不应有代理（Transport.Proxy 非 nil）", k)
		}
		if k.proxy != "" && tr.Proxy == nil {
			t.Errorf("组合 %+v 应设置代理 Transport.Proxy", k)
		}
		if tr.TLSHandshakeTimeout != k.connectTimeout {
			t.Errorf("组合 %+v 的 TLSHandshakeTimeout = %s, want %s（connect-timeout 落点）",
				k, tr.TLSHandshakeTimeout, k.connectTimeout)
		}
		// Server.client 对该组合应命中预构建实例（同一 key，不触发惰性补建）
		if got := s.client(k.proxy, k.connectTimeout, false); got != cl {
			t.Errorf("Server.client(%q, %s) 应返回 New 预构建实例", k.proxy, k.connectTimeout)
		}
	}
}
