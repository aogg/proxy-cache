package config

import (
	"strings"
	"testing"
	"time"
)

// durPtr 生成 Duration 指针，便于规则内写覆盖项。
func durPtr(d time.Duration) *Duration {
	x := Duration(d)
	return &x
}

// TestResolveOptionsExclude 表驱动验证 domain-rules 的 exclude 排除语义：
// match 命中且所有 exclude 均不命中才应用该规则；任一 exclude 命中即视为不匹配，
// 继续向下尝试后续规则（都不命中则走全局默认配置）。
func TestResolveOptionsExclude(t *testing.T) {
	const (
		globalTTL = 30 * time.Minute // Default() 的全局默认缓存时长
		rawURL    = "https://raw.githubusercontent.com/foo/bar/main/x.yaml"
	)
	// rawRule 返回一条 match githubusercontent、TTL 6h 的规则（可附带 exclude）。
	rawRule := func(name string, ttl time.Duration, exclude ...string) DomainRule {
		return DomainRule{
			Name:    name,
			Match:   `.*https://[^.]+\.githubusercontent\.com`,
			Exclude: exclude,
			Cache:   RuleCache{TTL: durPtr(ttl)},
		}
	}

	tests := []struct {
		name     string
		rules    []DomainRule
		target   string
		wantRule string
		wantTTL  time.Duration
	}{
		{
			name:     "match命中且无exclude_应用该规则",
			rules:    []DomainRule{rawRule("githubusercontent", 6*time.Hour)},
			target:   rawURL,
			wantRule: "githubusercontent",
			wantTTL:  6 * time.Hour,
		},
		{
			name: "exclude单个命中_被排除落到下一条规则",
			rules: []DomainRule{
				rawRule("githubusercontent", 6*time.Hour, `foo/bar`),
				{Name: "raw-foo", Match: `raw\.githubusercontent\.com/foo`, Cache: RuleCache{TTL: durPtr(1 * time.Hour)}},
			},
			target:   rawURL,
			wantRule: "raw-foo",
			wantTTL:  1 * time.Hour,
		},
		{
			name: "多个exclude任一命中_被排除落到下一条规则",
			rules: []DomainRule{
				rawRule("githubusercontent", 6*time.Hour, `nomatch/repo`, `foo/bar`),
				rawRule("raw-fallback", 1*time.Hour),
			},
			target:   rawURL,
			wantRule: "raw-fallback",
			wantTTL:  1 * time.Hour,
		},
		{
			name:     "exclude正则不命中_正常应用该规则",
			rules:    []DomainRule{rawRule("githubusercontent", 6*time.Hour, `other/repo`, `/releases/download/`)},
			target:   rawURL,
			wantRule: "githubusercontent",
			wantTTL:  6 * time.Hour,
		},
		{
			name: "所有规则均被exclude排除_走全局默认配置",
			rules: []DomainRule{
				rawRule("githubusercontent", 6*time.Hour, `foo/`),
				rawRule("raw-second", 2*time.Hour, `bar`),
			},
			target:   rawURL,
			wantRule: "",
			wantTTL:  globalTTL,
		},
		{
			name: "首条被排除_第二条exclude不命中_应用第二条",
			rules: []DomainRule{
				rawRule("githubusercontent", 6*time.Hour, `foo/bar`),
				rawRule("raw-second", 2*time.Hour, `other/repo`),
			},
			target:   rawURL,
			wantRule: "raw-second",
			wantTTL:  2 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.DomainRules = tt.rules
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions("", tt.target)
			if o.Rule != tt.wantRule {
				t.Errorf("Rule = %q, want %q", o.Rule, tt.wantRule)
			}
			if o.CacheTTL != tt.wantTTL {
				t.Errorf("CacheTTL = %v, want %v", o.CacheTTL, tt.wantTTL)
			}
		})
	}
}

// TestValidateExclude 校验 exclude 配置的合法性检查：
// 正则非法与空串均应在 Validate 阶段报错，且错误信息定位到规则与第几项。
func TestValidateExclude(t *testing.T) {
	t.Run("exclude正则非法时报错", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:    "githubusercontent",
			Match:   `raw\.githubusercontent\.com`,
			Exclude: []string{`ok-pattern`, `[invalid`},
		}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() 期望报错，实际为 nil")
		}
		for _, want := range []string{"githubusercontent", "exclude", "第 1 项"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
			}
		}
	})

	t.Run("exclude项为空时报错", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:    "githubusercontent",
			Match:   `raw\.githubusercontent\.com`,
			Exclude: []string{`  `},
		}}
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() 期望报错（exclude 第 0 项为空），实际为 nil")
		}
	})

	t.Run("exclude合法时校验通过并正常工作", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{rawRuleForValidate}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() 意外报错: %v", err)
		}
		o := cfg.ResolveOptions("", "https://raw.githubusercontent.com/foo/bar/main/x.yaml")
		if o.Rule != "githubusercontent" {
			t.Errorf("Rule = %q, want %q", o.Rule, "githubusercontent")
		}
		o = cfg.ResolveOptions("", "https://raw.githubusercontent.com/excluded/repo/main/x.yaml")
		if o.Rule != "" {
			t.Errorf("Rule = %q, want %q（被 exclude 排除后走全局默认）", o.Rule, "")
		}
	})
}

// rawRuleForValidate 是合法 exclude 的规则样例（match githubusercontent，排除 excluded 仓库）。
var rawRuleForValidate = DomainRule{
	Name:    "githubusercontent",
	Match:   `raw\.githubusercontent\.com`,
	Exclude: []string{`excluded/repo`},
}
