package config

// rule_type_test.go —— domain-rules 规则 type 字段（请求类型分流）的单测：
//   - Validate：type 合法值（"" / "docker" / "web" / " Docker " 等大小写与空白变体）
//     校验通过并归一化为小写；非法值（如 "abc" / 易混淆词 "registry"）启动快速失败，
//     错误信息定位到规则 label 并提示「type 仅支持 docker / web」；
//   - ResolveOptions 类型过滤语义：type: docker 仅承接路径目标请求（fullURL=false）、
//     type: web 仅承接完整 URL 请求（fullURL=true）、留空不过滤（旧配置兼容回归）；
//   - 过滤发生在 match/exclude 判定之前：类型不符的规则直接跳过（即使其 match 会命中、
//     甚至 match/exclude 都会命中），继续向下尝试后续规则；
//   - 同一 match 下 docker / web 两条规则各自的 cache 覆盖互不影响（按请求类型各取所得）。

import (
	"strings"
	"testing"
	"time"
)

// rule_type_test.go 专用常量：入站 Host 与两类目标串。
// 同一条 match 正则可同时命中两类请求的匹配串（「http://<Host>/<目标>」），
// 从而保证「命中与否只由 type 过滤决定」，与 match 正则无关。
const (
	hostRuleType   = "registry-proxy-cache.example.com:5480"          // 入站 Host（docker registry 场景）
	fullTargetType = "https://example.com/some/file.yaml"             // 完整 URL 请求的目标串（fullURL=true）
	pathTargetType = "v2/adockero/proxy-cache/manifests/latest"       // 路径目标请求的目标串（fullURL=false，无前导斜杠）
	ruleTypeMatch  = `http://registry-proxy-cache\.example\.com:5480/` // 同时覆盖两类请求串的 match 前缀
)

// ruleTypeRule 构造一条带 type 的规则：match 恒命中两类请求串，
// cache.ttl 作为命中标记覆盖项（未命中时走全局默认 DefaultCacheTTL）。
func ruleTypeRule(name, ruleType string) DomainRule {
	return DomainRule{
		Name:  name,
		Type:  ruleType,
		Match: ruleTypeMatch,
		Cache: RuleCache{TTL: durPtr(6 * time.Hour)},
	}
}

// TestValidateRuleType 验证规则 type 的校验与归一化：
// 合法值（含大小写/空白变体）通过并归一化为小写；非法值启动报错并定位到规则。
func TestValidateRuleType(t *testing.T) {
	t.Run("合法值通过并归一化为小写", func(t *testing.T) {
		tests := []struct {
			raw  string // 配置原始值
			want string // Validate 归一化后的生效值
		}{
			{raw: "", want: ""}, // 留空 = 不过滤请求类型（默认，兼容旧配置）
			{raw: "docker", want: "docker"},
			{raw: "web", want: "web"},
			{raw: " Docker ", want: "docker"}, // trim + 小写归一化
			{raw: "WEB", want: "web"},
		}
		for _, tt := range tests {
			cfg := Default()
			cfg.DomainRules = []DomainRule{ruleTypeRule("type-rule", tt.raw)}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("type=%q: Validate() 意外报错: %v", tt.raw, err)
			}
			if got := cfg.DomainRules[0].Type; got != tt.want {
				t.Errorf("type=%q: 归一化结果 = %q, want %q", tt.raw, got, tt.want)
			}
		}
	})

	t.Run("非法值启动报错并定位规则", func(t *testing.T) {
		tests := []struct {
			raw  string // 非法原始值
			want string // 错误信息中应出现的归一化取值（%q 形式）
		}{
			{raw: "abc", want: `"abc"`},
			{raw: "registry", want: `"registry"`}, // 易混淆词：registry 是目标类型语义，不是规则 type 取值
		}
		for _, tt := range tests {
			cfg := Default()
			cfg.DomainRules = []DomainRule{ruleTypeRule("type-rule", tt.raw)}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("type=%q: Validate() 期望报错（启动快速失败），实际为 nil", tt.raw)
			}
			for _, part := range []string{"type-rule", "type 仅支持 docker / web", tt.want} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("type=%q: 错误信息 %q 缺少 %q", tt.raw, err.Error(), part)
				}
			}
		}
	})
}

// TestResolveOptionsRuleTypeFilter 表驱动验证 ResolveOptions 的请求类型过滤语义：
// type: docker 仅承接路径目标请求（fullURL=false）；type: web 仅承接完整 URL 请求
//（fullURL=true）；留空不过滤、两类请求都命中（旧配置兼容回归）。
// 类型不符时落全局默认（Rule 为空串、ttl 沿用全局），而非报错。
func TestResolveOptionsRuleTypeFilter(t *testing.T) {
	const (
		ruleName = "type-rule"
		ruleTTL  = 6 * time.Hour   // 命中规则的 ttl 标记
	)

	tests := []struct {
		name     string
		ruleType string        // 规则 type 配置值
		fullURL  bool          // 请求类型：true=完整 URL 请求 / false=路径目标请求
		target   string        // 参与匹配的目标串（与请求类型配套）
		wantRule string        // 期望命中规则（"" = 落全局默认）
		wantTTL  time.Duration // 期望生效 ttl（命中规则覆盖项 / 全局默认）
	}{
		{
			name:     "type_docker_完整URL请求_不命中落全局默认",
			ruleType: RuleTypeDocker,
			fullURL:  true,
			target:   fullTargetType,
			wantRule: "",
			wantTTL:  DefaultCacheTTL,
		},
		{
			name:     "type_docker_路径目标请求_命中",
			ruleType: RuleTypeDocker,
			fullURL:  false,
			target:   pathTargetType,
			wantRule: ruleName,
			wantTTL:  ruleTTL,
		},
		{
			name:     "type_web_路径目标请求_不命中落全局默认",
			ruleType: RuleTypeWeb,
			fullURL:  false,
			target:   pathTargetType,
			wantRule: "",
			wantTTL:  DefaultCacheTTL,
		},
		{
			name:     "type_web_完整URL请求_命中",
			ruleType: RuleTypeWeb,
			fullURL:  true,
			target:   fullTargetType,
			wantRule: ruleName,
			wantTTL:  ruleTTL,
		},
		{
			name:     "type留空_完整URL请求_命中_旧配置兼容",
			ruleType: "",
			fullURL:  true,
			target:   fullTargetType,
			wantRule: ruleName,
			wantTTL:  ruleTTL,
		},
		{
			name:     "type留空_路径目标请求_命中_旧配置兼容",
			ruleType: "",
			fullURL:  false,
			target:   pathTargetType,
			wantRule: ruleName,
			wantTTL:  ruleTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.DomainRules = []DomainRule{ruleTypeRule(ruleName, tt.ruleType)}
			mustValidate(t, cfg)

			o := cfg.ResolveOptions(hostRuleType, tt.target, tt.fullURL)
			if o.Rule != tt.wantRule {
				t.Errorf("Rule = %q, want %q（fullURL=%v target=%q）", o.Rule, tt.wantRule, tt.fullURL, tt.target)
			}
			if o.CacheTTL != tt.wantTTL {
				t.Errorf("CacheTTL = %v, want %v", o.CacheTTL, tt.wantTTL)
			}
		})
	}
}

// TestResolveOptionsRuleTypeOrderSkip 验证顺序跳过语义：过滤发生在 match/exclude 判定之前，
// 类型不符的规则直接跳过（不消耗 match/exclude 判定），继续向下尝试后续规则。
func TestResolveOptionsRuleTypeOrderSkip(t *testing.T) {
	// newDockerRule 构造第二条 type: docker 规则（同样的 match），ttl 1h 作为命中标记。
	newDockerRule := func() DomainRule {
		r := ruleTypeRule("docker-second", RuleTypeDocker)
		r.Cache.TTL = durPtr(1 * time.Hour)
		return r
	}

	t.Run("路径目标请求_match会命中的web规则被类型过滤跳过_命中docker规则", func(t *testing.T) {
		// web-first 的 match 会命中路径目标请求串且无 exclude：若没有 type 过滤，
		// 它会被应用（ttl 6h 标记）；类型不符应在 match 判定前跳过，落到 docker-second。
		cfg := Default()
		cfg.DomainRules = []DomainRule{ruleTypeRule("web-first", RuleTypeWeb), newDockerRule()}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostRuleType, pathTargetType, false)
		if o.Rule != "docker-second" {
			t.Errorf("Rule = %q, want %q（web 规则类型不符应被跳过并继续向下尝试）", o.Rule, "docker-second")
		}
		if o.CacheTTL != 1*time.Hour {
			t.Errorf("CacheTTL = %v, want %v（应应用 docker-second 的覆盖项）", o.CacheTTL, 1*time.Hour)
		}
	})

	t.Run("路径目标请求_即使match与exclude都会命中_类型不符同样不应用", func(t *testing.T) {
		// web-first 对路径目标串 match 与 exclude 都会命中：无论从哪个判定看都不应
		// 应用到路径目标请求，且不阻断后续 docker-second 的命中。
		webRule := ruleTypeRule("web-first", RuleTypeWeb)
		webRule.Exclude = []string{`manifests`} // 对路径目标串必然命中
		cfg := Default()
		cfg.DomainRules = []DomainRule{webRule, newDockerRule()}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostRuleType, pathTargetType, false)
		if o.Rule != "docker-second" {
			t.Errorf("Rule = %q, want %q（类型不符时即使 match/exclude 都会命中也不应用该规则）", o.Rule, "docker-second")
		}
		if o.CacheTTL != 1*time.Hour {
			t.Errorf("CacheTTL = %v, want %v", o.CacheTTL, 1*time.Hour)
		}
	})

	t.Run("完整URL请求_命中web规则", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{ruleTypeRule("web-first", RuleTypeWeb), newDockerRule()}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostRuleType, fullTargetType, true)
		if o.Rule != "web-first" {
			t.Errorf("Rule = %q, want %q（完整 URL 请求应命中排在前面的 web 规则）", o.Rule, "web-first")
		}
		if o.CacheTTL != 6*time.Hour {
			t.Errorf("CacheTTL = %v, want %v", o.CacheTTL, 6*time.Hour)
		}
	})
}

// TestResolveOptionsRuleTypeCacheIndependence 验证同一 match 下 docker / web 两条规则
// 各自的 cache 覆盖互不影响：按请求类型各取所得（docker 规则 ttl 666h + 独立目录，
// web 规则 ttl 1h + 另一独立目录）。
func TestResolveOptionsRuleTypeCacheIndependence(t *testing.T) {
	dockerRule := ruleTypeRule("docker-rule", RuleTypeDocker)
	dockerRule.Cache = RuleCache{TTL: durPtr(666 * time.Hour), Path: strPtr("./docker-cache")}
	webRule := ruleTypeRule("web-rule", RuleTypeWeb)
	webRule.Cache = RuleCache{TTL: durPtr(1 * time.Hour), Path: strPtr("./web-cache")}

	cfg := Default()
	cfg.DomainRules = []DomainRule{dockerRule, webRule}
	mustValidate(t, cfg)

	t.Run("路径目标请求_取docker规则的cache覆盖", func(t *testing.T) {
		o := cfg.ResolveOptions(hostRuleType, pathTargetType, false)
		if o.Rule != "docker-rule" {
			t.Fatalf("Rule = %q, want %q（规则未命中则后续断言失真）", o.Rule, "docker-rule")
		}
		if o.CacheTTL != 666*time.Hour {
			t.Errorf("CacheTTL = %v, want %v（应取 docker 规则覆盖项，而非 web 规则的）", o.CacheTTL, 666*time.Hour)
		}
		if o.CachePath != "./docker-cache" {
			t.Errorf("CachePath = %q, want %q", o.CachePath, "./docker-cache")
		}
	})

	t.Run("完整URL请求_取web规则的cache覆盖", func(t *testing.T) {
		o := cfg.ResolveOptions(hostRuleType, fullTargetType, true)
		if o.Rule != "web-rule" {
			t.Fatalf("Rule = %q, want %q（规则未命中则后续断言失真）", o.Rule, "web-rule")
		}
		if o.CacheTTL != 1*time.Hour {
			t.Errorf("CacheTTL = %v, want %v（应取 web 规则覆盖项，而非 docker 规则的）", o.CacheTTL, 1*time.Hour)
		}
		if o.CachePath != "./web-cache" {
			t.Errorf("CachePath = %q, want %q", o.CachePath, "./web-cache")
		}
	})
}
