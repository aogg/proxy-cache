package config

// redirect_item_test.go —— url-redirect 候选对象化与三级 insecure-skip-verify（5b13a52）单测：
//   - URLRedirectItem.UnmarshalYAML：对象形态正常解析（template + insecure-skip-verify）；
//     纯字符串旧格式报错并给出迁移示例；未知字段（typo）报错并列出支持字段；
//     预留字段（enabled / http-proxy / weight / probe-timeout / hedge-delay 任一）
//     报错且提示未实现；
//   - 三级优先级解析（候选 > 规则 > 全局）：ResolveOptions 合并出 o.InsecureSkipVerify
//    （规则显式 ?? 全局），CandidateInsecureSkipVerify 再叠加候选显式值（item nil 逐级继承
//     全 4 向断言）。

import (
	"strings"
	"testing"

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
	for _, want := range []string{"templte", "template / insecure-skip-verify"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息 %q 应包含 %q（列出支持字段）", err.Error(), want)
		}
	}
}

// TestURLRedirectItemUnmarshalReservedFieldsRejected 预留字段（本期未实现）配置即报错：
// enabled / http-proxy / weight / probe-timeout / hedge-delay 任一都拒绝，且提示未实现。
func TestURLRedirectItemUnmarshalReservedFieldsRejected(t *testing.T) {
	for _, field := range []string{"enabled", "http-proxy", "weight", "probe-timeout", "hedge-delay"} {
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
