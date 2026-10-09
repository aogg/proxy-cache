package config

// config_cache_test.go —— 规则级 cache 完整独立配置（enabled / path / clean-interval）的单测：
//   - ResolveOptions：规则覆盖项与全局默认的合并语义（含「全局关、规则开」核心场景）；
//   - ActiveCacheDirs：可能被写入的缓存目录 -> 生效清理间隔；
//   - Validate：规则 cache.clean-interval / cache.path 的合法性校验。

import (
	"strings"
	"testing"
	"time"
)

// boolPtr 生成 bool 指针，便于规则内写 cache.enabled 覆盖项。
func boolPtr(b bool) *bool { return &b }

// strPtr 生成 string 指针，便于规则内写 cache.path 覆盖项。
func strPtr(s string) *string { return &s }

// testTargetURL 是测试用的目标 URL，能被 ruleCacheCfg 生成的规则命中。
const testTargetURL = "https://raw.githubusercontent.com/foo/bar/main/x.yaml"

// ruleCacheCfg 返回一条仅配置 cache 覆盖项的规则（match githubusercontent，可命中测试 URL）。
func ruleCacheCfg(name string, rc RuleCache) DomainRule {
	return DomainRule{
		Name:  name,
		Match: `raw\.githubusercontent\.com`,
		Cache: rc,
	}
}

// TestResolveOptionsRuleCache 表驱动验证规则级 cache 覆盖项的合并语义：
// 显式覆盖生效；未配置（或 path 为空白串）沿用全局；enabled 显式值优先、否则沿用全局。
func TestResolveOptionsRuleCache(t *testing.T) {
	const (
		globalPath     = "./global-cache"   // 全局缓存目录
		globalInterval = 10 * time.Minute   // 全局清理间隔
		ruleName       = "githubusercontent" // 规则名（用于断言规则确实命中）
	)

	tests := []struct {
		name          string
		globalEnabled bool      // 全局 cache.enabled
		rule          RuleCache // 命中规则的 cache 覆盖项
		wantEnabled   bool
		wantPath      string
		wantInterval  time.Duration
	}{
		{
			name:         "规则覆盖path与clean-interval生效",
			rule:         RuleCache{Path: strPtr("./rule-cache"), CleanInterval: durPtr(5 * time.Minute)},
			wantEnabled:  true, // 未配 enabled 沿用全局 true
			wantPath:     "./rule-cache",
			wantInterval: 5 * time.Minute,
		},
		{
			name:         "规则不配path与interval_沿用全局",
			rule:         RuleCache{TTL: durPtr(time.Hour)},
			wantEnabled:  true,
			wantPath:     globalPath,
			wantInterval: globalInterval,
		},
		{
			name:         "规则path为空白串_视为未配置仍用全局_interval显式0生效",
			rule:         RuleCache{Path: strPtr("   "), CleanInterval: durPtr(0)},
			wantEnabled:  true,
			wantPath:     globalPath,
			wantInterval: 0, // 显式 0 = 关闭该目录后台清理
		},
		{
			name:          "全局关_规则开_核心场景",
			globalEnabled: false,
			rule:          RuleCache{Enabled: boolPtr(true), Path: strPtr("./rule-cache"), CleanInterval: durPtr(5 * time.Minute)},
			wantEnabled:   true,
			wantPath:      "./rule-cache",
			wantInterval:  5 * time.Minute,
		},
		{
			name:          "全局开_规则关_该流量不缓存",
			globalEnabled: true,
			rule:          RuleCache{Enabled: boolPtr(false), Path: strPtr("./rule-cache")},
			wantEnabled:   false,
			wantPath:      "./rule-cache", // 关缓存不影响 path 覆盖语义
			wantInterval:  globalInterval,
		},
		{
			name:          "全局关_规则未配enabled_沿用全局仍关",
			globalEnabled: false,
			rule:          RuleCache{Path: strPtr("./rule-cache")},
			wantEnabled:   false,
			wantPath:      "./rule-cache",
			wantInterval:  globalInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Cache = CacheConfig{
				Enabled:       tt.globalEnabled,
				Path:          globalPath,
				TTL:           Duration(30 * time.Minute),
				CleanInterval: Duration(globalInterval),
			}
			cfg.DomainRules = []DomainRule{ruleCacheCfg(ruleName, tt.rule)}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}

			o := cfg.ResolveOptions(testTargetURL)
			if o.Rule != ruleName {
				t.Fatalf("Rule = %q, want %q（规则未命中则后续断言失真）", o.Rule, ruleName)
			}
			if o.CacheEnabled != tt.wantEnabled {
				t.Errorf("CacheEnabled = %v, want %v", o.CacheEnabled, tt.wantEnabled)
			}
			if o.CachePath != tt.wantPath {
				t.Errorf("CachePath = %q, want %q", o.CachePath, tt.wantPath)
			}
			if o.CacheCleanInterval != tt.wantInterval {
				t.Errorf("CacheCleanInterval = %v, want %v", o.CacheCleanInterval, tt.wantInterval)
			}
		})
	}
}

// TestActiveCacheDirs 表驱动验证「可能被写入的缓存目录 -> 生效清理间隔」：
// 全局开含全局目录；逐规则按显式 enabled 优先否则沿用全局判定；同路径首次出现的间隔生效。
func TestActiveCacheDirs(t *testing.T) {
	const (
		globalPath     = "./global-cache" // 全局缓存目录
		globalInterval = 10 * time.Minute // 全局清理间隔
	)

	tests := []struct {
		name          string
		globalEnabled bool         // 全局 cache.enabled
		rules         []DomainRule // 域名规则列表
		want          map[string]time.Duration
	}{
		{
			name:          "全局开_无规则_仅含全局目录与全局interval",
			globalEnabled: true,
			want:          map[string]time.Duration{globalPath: globalInterval},
		},
		{
			name:          "全局开_规则自有path_全局与规则两个目录都在",
			globalEnabled: true,
			rules: []DomainRule{
				ruleCacheCfg("rule-cache", RuleCache{Enabled: boolPtr(true), Path: strPtr("./rule-cache"), CleanInterval: durPtr(3 * time.Minute)}),
			},
			want: map[string]time.Duration{
				globalPath:     globalInterval,
				"./rule-cache": 3 * time.Minute,
			},
		},
		{
			name:          "全局关_规则开_规则自有path_仅含规则目录",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("rule-cache", RuleCache{Enabled: boolPtr(true), Path: strPtr("./rule-cache"), CleanInterval: durPtr(3 * time.Minute)}),
			},
			want: map[string]time.Duration{"./rule-cache": 3 * time.Minute},
		},
		{
			name:          "全局关_规则开_未配path_含全局目录_间隔取规则的",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("rule-cache", RuleCache{Enabled: boolPtr(true), CleanInterval: durPtr(3 * time.Minute)}),
			},
			want: map[string]time.Duration{globalPath: 3 * time.Minute},
		},
		{
			name:          "全局关_规则开_path空白串_视为未配置_含全局目录",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("rule-cache", RuleCache{Enabled: boolPtr(true), Path: strPtr("   ")}),
			},
			want: map[string]time.Duration{globalPath: globalInterval},
		},
		{
			name:          "两条规则同path不同interval_首条规则的interval生效",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("first", RuleCache{Enabled: boolPtr(true), Path: strPtr("./shared"), CleanInterval: durPtr(2 * time.Minute)}),
				ruleCacheCfg("second", RuleCache{Enabled: boolPtr(true), Path: strPtr("./shared"), CleanInterval: durPtr(9 * time.Minute)}),
			},
			want: map[string]time.Duration{"./shared": 2 * time.Minute},
		},
		{
			name:          "全局关_规则显式关_空map",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("off-rule", RuleCache{Enabled: boolPtr(false), Path: strPtr("./rule-cache")}),
			},
			want: map[string]time.Duration{},
		},
		{
			name:          "全局关_规则未配enabled_沿用全局_空map",
			globalEnabled: false,
			rules: []DomainRule{
				ruleCacheCfg("inherit-off", RuleCache{Path: strPtr("./rule-cache")}),
			},
			want: map[string]time.Duration{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Cache = CacheConfig{
				Enabled:       tt.globalEnabled,
				Path:          globalPath,
				TTL:           Duration(30 * time.Minute),
				CleanInterval: Duration(globalInterval),
			}
			cfg.DomainRules = tt.rules
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}

			got := cfg.ActiveCacheDirs()
			if len(got) != len(tt.want) {
				t.Fatalf("ActiveCacheDirs() = %v, want 目录数 %d", got, len(tt.want))
			}
			for path, wantInterval := range tt.want {
				gotInterval, ok := got[path]
				if !ok {
					t.Errorf("ActiveCacheDirs() 缺少目录 %q", path)
					continue
				}
				if gotInterval != wantInterval {
					t.Errorf("目录 %q 清理间隔 = %v, want %v", path, gotInterval, wantInterval)
				}
			}
		})
	}
}

// TestValidateRuleCache 校验规则级 cache 的合法性检查：
// clean-interval 为负报错；为 0（关闭清理）与 path 为空串（未配置）均合法。
func TestValidateRuleCache(t *testing.T) {
	t.Run("规则clean-interval为负_报错", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{
			ruleCacheCfg("githubusercontent", RuleCache{CleanInterval: durPtr(-time.Second)}),
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() 期望报错（clean-interval 为负），实际为 nil")
		}
		// 错误信息需定位到规则与字段
		for _, want := range []string{"githubusercontent", "clean-interval"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
			}
		}
	})

	t.Run("规则clean-interval为0_通过", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{
			ruleCacheCfg("githubusercontent", RuleCache{CleanInterval: durPtr(0)}),
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() 意外报错: %v", err)
		}
	})

	t.Run("规则path为空串_通过", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{
			ruleCacheCfg("githubusercontent", RuleCache{Path: strPtr("")}),
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() 意外报错: %v", err)
		}
	})
}
