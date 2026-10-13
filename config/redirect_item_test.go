package config

// redirect_item_test.go —— url-redirect 候选对象化与三级覆盖（insecure-skip-verify、
// http-proxy 与超时）单测：
//   - URLRedirectItem.UnmarshalYAML：对象形态正常解析（template + insecure-skip-verify +
//     http-proxy + 四个超时字段）；纯字符串旧格式报错并给出迁移示例；
//     未知字段（typo）报错并列出支持字段；预留字段（enabled / weight / hedge-delay 任一）
//     报错且提示未实现；
//   - 三级优先级解析（候选 > 规则 > 全局）：ResolveOptions 合并出 o 内「规则 ?? 全局」值，
//     CandidateHTTPProxy / CandidateConnectTimeout / CandidateHTTPTimeout /
//     CandidateReadTimeout / CandidateProbeTimeout 再叠加候选显式值
//    （含候选显式空串代理 = 强制直连、显式 0 超时 = 不限制）；
//   - Validate：候选级 http-proxy 非法与候选级超时负数均启动报错（全局与规则级）。

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// redirectItemWrapper 是 YAML 解析测试用的最小包装（与配置文件里的 url-redirect 段同构）。
type redirectItemWrapper struct {
	URLRedirect []URLRedirectItem `yaml:"url-redirect"`
}

// TestURLRedirectItemUnmarshalObjectForm 对象形态正常解析：
// template 与 insecure-skip-verify 均落到字段；未配置 insecure 的候选拒绝保持 nil（逐级继承语义）。
func TestURLRedirectItemUnmarshalObjectForm(t *testing.T) {
	var w redirectItemWrapper
	data := "url-redirect:\n" +
		"  - template: https://m1.example.com/$1\n" +
		"    insecure-skip-verify: true\n" +
		"  - template: https://m2.example.com/${1}\n"
	if err := yaml.Unmarshal([]byte(data), &w); err != nil {
		t.Fatalf("对象形态应解析成功，意外报错: %v", err)
	}
	if len(w.URLRedirect) != 2 {
		t.Fatalf("应解析出 2 个候选，得到 %d", len(w.URLRedirect))
	}
	if got := w.URLRedirect[0].Template; got != "https://m1.example.com/$1" {
		t.Errorf("候选0 Template = %q, want %q", got, "https://m1.example.com/$1")
	}
	if w.URLRedirect[0].InsecureSkipVerify == nil || !*w.URLRedirect[0].InsecureSkipVerify {
		t.Errorf("候选0 insecure-skip-verify 应解析为显式 true，得到 %v", w.URLRedirect[0].InsecureSkipVerify)
	}
	if got := w.URLRedirect[1].Template; got != "https://m2.example.com/${1}" {
		t.Errorf("候选1 Template = %q, want %q", got, "https://m2.example.com/${1}")
	}
	if w.URLRedirect[1].InsecureSkipVerify != nil {
		t.Errorf("候选1 未配置 insecure-skip-verify 应保持 nil（逐级继承），得到 %v", *w.URLRedirect[1].InsecureSkipVerify)
	}
}

// TestURLRedirectItemUnmarshalLegacyStringRejected 纯字符串旧格式不再支持：
// 解析期报错且错误信息包含「迁移示例」。
func TestURLRedirectItemUnmarshalLegacyStringRejected(t *testing.T) {
	var w redirectItemWrapper
	data := "url-redirect:\n  - https://gh-proxy.com/$1\n"
	err := yaml.Unmarshal([]byte(data), &w)
	if err == nil {
		t.Fatal("纯字符串候选应解析报错（旧格式已移除），实际为 nil")
	}
	if !strings.Contains(err.Error(), "迁移示例") {
		t.Errorf("错误信息应包含「迁移示例」，实际: %s", err.Error())
	}
}

// TestURLRedirectItemUnmarshalUnknownFieldRejected 未知字段（拼写错误）报错：
// 错误信息列出全部支持字段，避免配置被静默忽略。
func TestURLRedirectItemUnmarshalUnknownFieldRejected(t *testing.T) {
	var w redirectItemWrapper
	data := "url-redirect:\n" +
		"  - template: https://m.example.com/$1\n" +
		"    templte: https://typo.example.com/$1\n"
	err := yaml.Unmarshal([]byte(data), &w)
	if err == nil {
		t.Fatal("未知字段应解析报错，实际为 nil")
	}
	for _, want := range []string{"templte", "template / insecure-skip-verify / http-proxy / connect-timeout / http-timeout / read-timeout / probe-timeout"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息 %q 应包含 %q（列出支持字段）", err.Error(), want)
		}
	}
}

// TestURLRedirectItemUnmarshalReservedFieldsRejected 预留字段（本期未实现）配置即报错：
// enabled / weight / hedge-delay 任一都拒绝，且提示未实现
// （历史预留的 http-proxy / probe-timeout 已实现，不再是预留字段）。
func TestURLRedirectItemUnmarshalReservedFieldsRejected(t *testing.T) {
	for _, field := range []string{"enabled", "weight", "hedge-delay"} {
		t.Run(field, func(t *testing.T) {
			var w redirectItemWrapper
			data := "url-redirect:\n" +
				"  - template: https://m.example.com/$1\n" +
				"    " + field + ": true\n"
			err := yaml.Unmarshal([]byte(data), &w)
			if err == nil {
				t.Fatalf("预留字段 %s 应解析报错，实际为 nil", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("错误信息应包含字段名 %q，实际: %s", field, err.Error())
			}
			if !strings.Contains(err.Error(), "未实现") {
				t.Errorf("错误信息应提示「未实现」，实际: %s", err.Error())
			}
		})
	}
}

// TestURLRedirectItemUnmarshalNonMappingRejected 列表等非法形态报错（必须是键值对象）。
func TestURLRedirectItemUnmarshalNonMappingRejected(t *testing.T) {
	var w redirectItemWrapper
	err := yaml.Unmarshal([]byte("url-redirect:\n  - [https://a.example.com/$1]\n"), &w)
	if err == nil {
		t.Fatal("列表形态候选应解析报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "键值对象") {
		t.Errorf("错误信息应说明必须是键值对象，实际: %s", err.Error())
	}
}

// TestInsecureSkipVerifyThreeLevelPriority 表驱动验证三级优先级解析
// （候选 insecure-skip-verify > 规则 insecure-skip-verify > 全局 insecure-skip-verify）：
//   - wantOpts 断言 o.InsecureSkipVerify = 规则显式 ?? 全局（ResolveOptions 合并结果）；
//   - wantCand 断言 o.CandidateInsecureSkipVerify(item) = 候选显式 ?? wantOpts；
//   - item 为 nil 的四行覆盖（全局,规则）的全部 4 向组合（nil 逐级继承）。
func TestInsecureSkipVerifyThreeLevelPriority(t *testing.T) {
	tests := []struct {
		name     string
		global   bool
		rule     *bool
		item     *bool
		wantOpts bool // 期望 Options.InsecureSkipVerify（规则显式 ?? 全局）
		wantCand bool // 期望 Options.CandidateInsecureSkipVerify(item)
	}{
		{name: "全局true_规则nil_逐级继承为true", global: true, rule: nil, item: nil, wantOpts: true, wantCand: true},
		{name: "全局true_规则显式false_规则覆盖全局", global: true, rule: boolPtr(false), item: nil, wantOpts: false, wantCand: false},
		{name: "全局false_规则nil_逐级继承为false", global: false, rule: nil, item: nil, wantOpts: false, wantCand: false},
		{name: "全局false_规则显式true_规则覆盖全局", global: false, rule: boolPtr(true), item: nil, wantOpts: true, wantCand: true},
		{name: "规则false_候选显式true_候选覆盖规则", global: false, rule: boolPtr(false), item: boolPtr(true), wantOpts: false, wantCand: true},
		{name: "规则true_候选显式false_候选覆盖上层", global: true, rule: boolPtr(true), item: boolPtr(false), wantOpts: true, wantCand: false},
		{name: "全局true_规则nil_候选显式false_候选覆盖全局", global: true, rule: nil, item: boolPtr(false), wantOpts: true, wantCand: false},
		{name: "全局false_规则nil_候选显式true_候选覆盖全局", global: false, rule: nil, item: boolPtr(true), wantOpts: false, wantCand: true},
	}
	const (
		host       = "reg.example.com"
		target     = "v2/x/manifests/latest"
		ruleRegexp = `http://reg\.example\.com/`
	)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.InsecureSkipVerify = tt.global
			cfg.DomainRules = []DomainRule{{
				Name:               "insecure-rule",
				Match:              ruleRegexp,
				InsecureSkipVerify: tt.rule,
				URLRedirect: &[]URLRedirectItem{{
					Template:           "https://mirror.example.com/$1",
					InsecureSkipVerify: tt.item,
				}},
			}}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions(host, target, false)
			if o.Rule != "insecure-rule" {
				t.Fatalf("规则应命中，得到 Rule=%q", o.Rule)
			}
			if o.InsecureSkipVerify != tt.wantOpts {
				t.Errorf("Options.InsecureSkipVerify = %v, want %v（规则显式 ?? 全局）", o.InsecureSkipVerify, tt.wantOpts)
			}
			if len(o.URLRedirect) != 1 {
				t.Fatalf("生效候选数 = %d, want 1", len(o.URLRedirect))
			}
			if got := o.CandidateInsecureSkipVerify(o.URLRedirect[0]); got != tt.wantCand {
				t.Errorf("CandidateInsecureSkipVerify(item) = %v, want %v（候选显式 ?? 上级 %v）", got, tt.wantCand, tt.wantOpts)
			}
		})
	}
}

// 候选用例复用本包既有辅助：strPtr（config_cache_test.go）与 durPtr（config_test.go）。

// TestURLRedirectItemUnmarshalNewFields 候选新字段（http-proxy 与四个超时）解析：
// 对象含全部新字段时逐一落到指针字段；未配置的字段保持 nil（逐级继承语义）。
func TestURLRedirectItemUnmarshalNewFields(t *testing.T) {
	var w redirectItemWrapper
	data := "url-redirect:\n" +
		"  - template: https://slow.example.com/$1\n" +
		"    http-proxy: socks5://127.0.0.1:7890\n" +
		"    connect-timeout: 30s\n" +
		"    http-timeout: 20m\n" +
		"    read-timeout: 90s\n" +
		"    probe-timeout: 15s\n" +
		"    insecure-skip-verify: true\n" +
		"  - template: https://fast.example.com/$1\n"
	if err := yaml.Unmarshal([]byte(data), &w); err != nil {
		t.Fatalf("候选新字段应解析成功，意外报错: %v", err)
	}
	if len(w.URLRedirect) != 2 {
		t.Fatalf("应解析出 2 个候选，得到 %d", len(w.URLRedirect))
	}
	slow := w.URLRedirect[0]
	if slow.HTTPProxy == nil || *slow.HTTPProxy != "socks5://127.0.0.1:7890" {
		t.Errorf("候选0 http-proxy 应解析为 socks5://127.0.0.1:7890，得到 %v", slow.HTTPProxy)
	}
	if slow.ConnectTimeout == nil || slow.ConnectTimeout.D() != 30*time.Second {
		t.Errorf("候选0 connect-timeout 应解析为 30s，得到 %v", slow.ConnectTimeout)
	}
	if slow.HTTPTimeout == nil || slow.HTTPTimeout.D() != 20*time.Minute {
		t.Errorf("候选0 http-timeout 应解析为 20m，得到 %v", slow.HTTPTimeout)
	}
	if slow.ReadTimeout == nil || slow.ReadTimeout.D() != 90*time.Second {
		t.Errorf("候选0 read-timeout 应解析为 90s，得到 %v", slow.ReadTimeout)
	}
	if slow.ProbeTimeout == nil || slow.ProbeTimeout.D() != 15*time.Second {
		t.Errorf("候选0 probe-timeout 应解析为 15s，得到 %v", slow.ProbeTimeout)
	}
	if slow.InsecureSkipVerify == nil || !*slow.InsecureSkipVerify {
		t.Errorf("候选0 insecure-skip-verify 应解析为 true，得到 %v", slow.InsecureSkipVerify)
	}
	fast := w.URLRedirect[1]
	if fast.HTTPProxy != nil {
		t.Errorf("候选1 未配置 http-proxy 应保持 nil（逐级继承），得到 %q", *fast.HTTPProxy)
	}
	if fast.ConnectTimeout != nil || fast.HTTPTimeout != nil || fast.ReadTimeout != nil || fast.ProbeTimeout != nil {
		t.Errorf("候选1 未配置超时应全部保持 nil（逐级继承），得到 connect=%v http=%v read=%v probe=%v",
			fast.ConnectTimeout, fast.HTTPTimeout, fast.ReadTimeout, fast.ProbeTimeout)
	}
}

// TestCandidateHTTPProxyThreeLevelPriority 表驱动验证候选 http-proxy 三级优先级
// （候选对象 http-proxy > 规则 http-proxy > 全局 http-proxy）：
//   - wantOpts 断言 o.HTTPProxy = 规则显式 ?? 全局（ResolveOptions 合并结果）；
//   - wantCand 断言 o.CandidateHTTPProxy(item) = 候选显式 trim 后值 ?? wantOpts；
//   - 候选显式空串 = 强制直连（覆盖上层已配置的代理），空白串 trim 后同为直连。
func TestCandidateHTTPProxyThreeLevelPriority(t *testing.T) {
	const (
		globalProxy = "http://global.proxy:7890"
		ruleProxy   = "socks5://rule.proxy:1080"
		itemProxy   = "http://item.proxy:7890"
		host        = "reg.example.com"
		target      = "v2/x/manifests/latest"
		ruleRegexp  = `http://reg\.example\.com/`
	)
	tests := []struct {
		name      string
		globalSet bool    // 全局是否配置代理（false = 全局直连）
		rule      *string // 规则级显式值（nil = 继承全局）
		item      *string // 候选显式值（nil = 继承规则级再全局）
		wantOpts  string  // 期望 Options.HTTPProxy（规则显式 ?? 全局）
		wantCand  string  // 期望 Options.CandidateHTTPProxy(item)
	}{
		{name: "全局代理_规则候选nil_逐级继承", globalSet: true, rule: nil, item: nil, wantOpts: globalProxy, wantCand: globalProxy},
		{name: "全局代理_规则显式空串直连_候选继承直连", globalSet: true, rule: strPtr(""), item: nil, wantOpts: "", wantCand: ""},
		{name: "全局直连_规则代理_候选继承规则", globalSet: false, rule: strPtr(ruleProxy), item: nil, wantOpts: ruleProxy, wantCand: ruleProxy},
		{name: "规则代理_候选显式空串_强制直连覆盖规则", globalSet: true, rule: strPtr(ruleProxy), item: strPtr(""), wantOpts: ruleProxy, wantCand: ""},
		{name: "规则直连_候选显式代理_候选覆盖规则", globalSet: true, rule: strPtr(""), item: strPtr(itemProxy), wantOpts: "", wantCand: itemProxy},
		{name: "全局代理_规则nil_候选显式代理_候选覆盖全局", globalSet: true, rule: nil, item: strPtr(itemProxy), wantOpts: globalProxy, wantCand: itemProxy},
		{name: "候选显式空白串_trim后强制直连", globalSet: true, rule: strPtr(ruleProxy), item: strPtr("   "), wantOpts: ruleProxy, wantCand: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			if tt.globalSet {
				cfg.HTTPProxy = globalProxy
			}
			cfg.DomainRules = []DomainRule{{
				Name:        "proxy-rule",
				Match:       ruleRegexp,
				HTTPProxy:   tt.rule,
				URLRedirect: &[]URLRedirectItem{{Template: "https://mirror.example.com/$1", HTTPProxy: tt.item}},
			}}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions(host, target, false)
			if o.Rule != "proxy-rule" {
				t.Fatalf("规则应命中，得到 Rule=%q", o.Rule)
			}
			if o.HTTPProxy != tt.wantOpts {
				t.Errorf("Options.HTTPProxy = %q, want %q（规则显式 ?? 全局）", o.HTTPProxy, tt.wantOpts)
			}
			if len(o.URLRedirect) != 1 {
				t.Fatalf("生效候选数 = %d, want 1", len(o.URLRedirect))
			}
			if got := o.CandidateHTTPProxy(o.URLRedirect[0]); got != tt.wantCand {
				t.Errorf("CandidateHTTPProxy(item) = %q, want %q（候选显式 ?? 上层 %q）", got, tt.wantCand, tt.wantOpts)
			}
		})
	}
}

// TestCandidateTimeoutsThreeLevelPriority 表驱动验证候选超时三级优先级
// （候选对象 connect-timeout / http-timeout / read-timeout > 规则级 > 全局级）：
//   - wantEff 断言 o 内对应字段 = 规则显式 ?? 全局（ResolveOptions 合并结果）；
//   - wantCand 断言 o.Candidate<Timeout>(item) = 候选显式值 ?? wantEff；
//   - 候选显式 0 = 不限制（覆盖上层已配置的预算），nil = 逐级继承。
//
// probe-timeout 的全局缺省语义（nil = 缺省 5s，显式 0 = 不限制）单独在
// TestCandidateProbeTimeoutPriority 覆盖。
func TestCandidateTimeoutsThreeLevelPriority(t *testing.T) {
	const (
		globalT    = 5 * time.Second
		ruleT      = 8 * time.Second
		itemT      = 30 * time.Second
		host       = "reg.example.com"
		target     = "v2/x/manifests/latest"
		ruleRegexp = `http://reg\.example\.com/`
	)
	// 三层取值的公共用例集（对 connect-timeout / http-timeout / read-timeout 语义一致）：
	// global 恒配置 globalT（缺省语义另测），rule/item 的 nil = 逐级继承。
	cases := []struct {
		name     string
		rule     *Duration
		item     *Duration
		wantEff  time.Duration // 期望 o 内对应字段（规则显式 ?? 全局）
		wantCand time.Duration // 期望 o.Candidate<Timeout>(item)
	}{
		{name: "规则候选均未配置_逐级继承全局", rule: nil, item: nil, wantEff: globalT, wantCand: globalT},
		{name: "规则显式覆盖全局_候选继承规则", rule: durPtr(ruleT), item: nil, wantEff: ruleT, wantCand: ruleT},
		{name: "候选显式覆盖规则", rule: durPtr(ruleT), item: durPtr(itemT), wantEff: ruleT, wantCand: itemT},
		{name: "候选显式覆盖全局", rule: nil, item: durPtr(itemT), wantEff: globalT, wantCand: itemT},
		{name: "候选显式0_不限制覆盖上层预算", rule: durPtr(ruleT), item: durPtr(0), wantEff: ruleT, wantCand: 0},
	}
	// fields 把公共用例集映射到三个超时字段各自的「写全局/写规则/写候选/读生效值」。
	fields := []struct {
		name      string
		setGlobal func(cfg *Config, d time.Duration)
		setRule   func(r *DomainRule, d *Duration)
		setItem   func(it *URLRedirectItem, d *Duration)
		candidate func(o Options, it URLRedirectItem) time.Duration
		eff       func(o Options) time.Duration
	}{
		{
			name:      "connect-timeout",
			setGlobal: func(cfg *Config, d time.Duration) { cfg.ConnectTimeout = durationPtr(d) },
			setRule:   func(r *DomainRule, d *Duration) { r.ConnectTimeout = d },
			setItem:   func(it *URLRedirectItem, d *Duration) { it.ConnectTimeout = d },
			candidate: func(o Options, it URLRedirectItem) time.Duration { return o.CandidateConnectTimeout(it) },
			eff:       func(o Options) time.Duration { return o.ConnectTimeout },
		},
		{
			name:      "http-timeout",
			setGlobal: func(cfg *Config, d time.Duration) { cfg.HTTPTimeout = durationPtr(d) },
			setRule:   func(r *DomainRule, d *Duration) { r.HTTPTimeout = d },
			setItem:   func(it *URLRedirectItem, d *Duration) { it.HTTPTimeout = d },
			candidate: func(o Options, it URLRedirectItem) time.Duration { return o.CandidateHTTPTimeout(it) },
			eff:       func(o Options) time.Duration { return o.HTTPTimeout },
		},
		{
			name:      "read-timeout",
			setGlobal: func(cfg *Config, d time.Duration) { cfg.ReadTimeout = durationPtr(d) },
			setRule:   func(r *DomainRule, d *Duration) { r.ReadTimeout = d },
			setItem:   func(it *URLRedirectItem, d *Duration) { it.ReadTimeout = d },
			candidate: func(o Options, it URLRedirectItem) time.Duration { return o.CandidateReadTimeout(it) },
			eff:       func(o Options) time.Duration { return o.ReadTimeout },
		},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					cfg := Default()
					f.setGlobal(cfg, globalT)
					rule := DomainRule{Name: "timeout-rule", Match: ruleRegexp}
					f.setRule(&rule, tc.rule)
					item := URLRedirectItem{Template: "https://mirror.example.com/$1"}
					f.setItem(&item, tc.item)
					rule.URLRedirect = &[]URLRedirectItem{item}
					cfg.DomainRules = []DomainRule{rule}
					if err := cfg.Validate(); err != nil {
						t.Fatalf("Validate() 意外报错: %v", err)
					}
					o := cfg.ResolveOptions(host, target, false)
					if got := f.eff(o); got != tc.wantEff {
						t.Errorf("o.%s = %s, want %s（规则显式 ?? 全局）", f.name, got, tc.wantEff)
					}
					if len(o.URLRedirect) != 1 {
						t.Fatalf("生效候选数 = %d, want 1", len(o.URLRedirect))
					}
					if got := f.candidate(o, o.URLRedirect[0]); got != tc.wantCand {
						t.Errorf("Candidate%s(item) = %s, want %s（候选显式 ?? 上层 %s）", f.name, got, tc.wantCand, tc.wantEff)
					}
				})
			}
		})
	}
}

// TestCandidateProbeTimeoutPriority 候选 probe-timeout 三级优先级与全局缺省语义：
//   - 三级均未配置：候选继承规则/全局 → 全局缺省 5s（DefaultURLRedirectProbeTimeout）；
//   - 全局显式 0 = 不限制：候选未配置时继承 0；候选显式 15s 覆盖全局 0；
//   - 规则显式 8s：候选 nil 继承 8s；候选显式 0 不限制覆盖规则。
func TestCandidateProbeTimeoutPriority(t *testing.T) {
	const (
		host       = "reg.example.com"
		target     = "v2/x/manifests/latest"
		ruleRegexp = `http://reg\.example\.com/`
	)
	tests := []struct {
		name     string
		global   *Duration     // 全局 url-redirect-probe-timeout（nil = 缺省 5s）
		rule     *Duration     // 规则级（nil = 继承全局）
		item     *Duration     // 候选级（nil = 继承规则级再全局）
		wantEff  time.Duration // 期望 o.URLRedirectProbeTimeout
		wantCand time.Duration // 期望 o.CandidateProbeTimeout(item)
	}{
		{name: "三级均未配置_继承全局缺省5s", global: nil, rule: nil, item: nil, wantEff: 5 * time.Second, wantCand: 5 * time.Second},
		{name: "全局显式0不限制_候选继承0", global: durPtr(0), rule: nil, item: nil, wantEff: 0, wantCand: 0},
		{name: "全局0_候选显式15s_候选覆盖全局", global: durPtr(0), rule: nil, item: durPtr(15 * time.Second), wantEff: 0, wantCand: 15 * time.Second},
		{name: "规则显式8s_候选nil_继承规则", global: durPtr(5 * time.Second), rule: durPtr(8 * time.Second), item: nil, wantEff: 8 * time.Second, wantCand: 8 * time.Second},
		{name: "规则8s_候选显式0_不限制覆盖规则", global: durPtr(5 * time.Second), rule: durPtr(8 * time.Second), item: durPtr(0), wantEff: 8 * time.Second, wantCand: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.URLRedirectProbeTimeout = tt.global
			cfg.DomainRules = []DomainRule{{
				Name:                    "probe-rule",
				Match:                   ruleRegexp,
				URLRedirectProbeTimeout: tt.rule,
				URLRedirect: &[]URLRedirectItem{{
					Template:     "https://mirror.example.com/$1",
					ProbeTimeout: tt.item,
				}},
			}}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions(host, target, false)
			if o.URLRedirectProbeTimeout != tt.wantEff {
				t.Errorf("o.URLRedirectProbeTimeout = %s, want %s", o.URLRedirectProbeTimeout, tt.wantEff)
			}
			if len(o.URLRedirect) != 1 {
				t.Fatalf("生效候选数 = %d, want 1", len(o.URLRedirect))
			}
			if got := o.CandidateProbeTimeout(o.URLRedirect[0]); got != tt.wantCand {
				t.Errorf("CandidateProbeTimeout(item) = %s, want %s", got, tt.wantCand)
			}
		})
	}
}

// TestValidateCandidateHTTPProxyRejected 候选级 http-proxy 非法时 Validate 报错
// （全局与规则级候选列表都校验；显式空串 = 强制直连、合法 socks5 地址均放行）。
func TestValidateCandidateHTTPProxyRejected(t *testing.T) {
	t.Run("全局候选http-proxy非法", func(t *testing.T) {
		cfg := Default()
		cfg.URLRedirect = []URLRedirectItem{{
			Template:  "https://m.example.com/$1",
			HTTPProxy: strPtr("ftp://x"),
		}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("候选 http-proxy 非法应校验失败")
		}
		for _, want := range []string{"url-redirect 第 0 个候选的 http-proxy 无效", "ftp://x"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
			}
		}
	})
	t.Run("规则候选http-proxy非法_带规则与候选序号定位", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:  "proxy-rule",
			Match: `http://reg\.example\.com/`,
			URLRedirect: &[]URLRedirectItem{
				{Template: "https://m1.example.com/$1"},
				{Template: "https://m2.example.com/$1", HTTPProxy: strPtr("notaurl")},
			},
		}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("规则候选 http-proxy 非法应校验失败")
		}
		for _, want := range []string{"domain-rules[0] (proxy-rule) 的 url-redirect 第 1 个候选的 http-proxy 无效", "notaurl"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
			}
		}
	})
	t.Run("候选显式空串与合法socks5均放行", func(t *testing.T) {
		cfg := Default()
		cfg.URLRedirect = []URLRedirectItem{
			{Template: "https://m1.example.com/$1", HTTPProxy: strPtr("  ")}, // 强制直连，合法
			{Template: "https://m2.example.com/$1", HTTPProxy: strPtr("socks5://127.0.0.1:1080")},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("候选显式空串/合法代理应校验通过，意外报错: %v", err)
		}
	})
}

// TestValidateCandidateTimeoutsNegativeRejected 候选级超时负数为配置错误（全局与规则级
// 候选列表都校验，错误信息带候选序号定位）；显式 0 = 不限制，合法。
func TestValidateCandidateTimeoutsNegativeRejected(t *testing.T) {
	fields := []struct {
		name    string
		setItem func(it *URLRedirectItem, d *Duration)
	}{
		{name: "connect-timeout", setItem: func(it *URLRedirectItem, d *Duration) { it.ConnectTimeout = d }},
		{name: "http-timeout", setItem: func(it *URLRedirectItem, d *Duration) { it.HTTPTimeout = d }},
		{name: "read-timeout", setItem: func(it *URLRedirectItem, d *Duration) { it.ReadTimeout = d }},
		{name: "probe-timeout", setItem: func(it *URLRedirectItem, d *Duration) { it.ProbeTimeout = d }},
	}
	for _, f := range fields {
		t.Run("全局_"+f.name, func(t *testing.T) {
			cfg := Default()
			item := URLRedirectItem{Template: "https://m.example.com/$1"}
			f.setItem(&item, durPtr(-time.Second))
			cfg.URLRedirect = []URLRedirectItem{item}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("候选 %s 负数应校验失败", f.name)
			}
			for _, want := range []string{"url-redirect 第 0 个候选的", f.name, "不能为负数"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
				}
			}
		})
		t.Run("规则_"+f.name, func(t *testing.T) {
			cfg := Default()
			item := URLRedirectItem{Template: "https://m.example.com/$1"}
			f.setItem(&item, durPtr(-time.Second))
			cfg.DomainRules = []DomainRule{{
				Name:        "timeout-rule",
				Match:       `http://reg\.example\.com/`,
				URLRedirect: &[]URLRedirectItem{item},
			}}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("规则候选 %s 负数应校验失败", f.name)
			}
			for _, want := range []string{"domain-rules[0] (timeout-rule) 的 url-redirect 第 0 个候选的", f.name, "不能为负数"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
				}
			}
		})
	}
	// 显式 0 = 不限制（合法），Validate 放行且生效值为 0
	t.Run("候选显式0超时全部放行", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:  "timeout-rule",
			Match: `http://reg\.example\.com/`,
			URLRedirect: &[]URLRedirectItem{{
				Template:       "https://m.example.com/$1",
				ConnectTimeout: durPtr(0),
				HTTPTimeout:    durPtr(0),
				ReadTimeout:    durPtr(0),
				ProbeTimeout:   durPtr(0),
			}},
		}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("候选显式 0 超时应校验通过，意外报错: %v", err)
		}
		o := cfg.ResolveOptions("reg.example.com", "v2/x/manifests/latest", false)
		it := o.URLRedirect[0]
		if o.CandidateConnectTimeout(it) != 0 || o.CandidateHTTPTimeout(it) != 0 ||
			o.CandidateReadTimeout(it) != 0 || o.CandidateProbeTimeout(it) != 0 {
			t.Errorf("候选显式 0 超时应全部生效为不限制（0）")
		}
	})
}
