package config

// config_cache_test.go —— 规则级 cache 完整独立配置（enabled / ttl / path / clean-interval）的单测：
//   - ResolveOptions：规则覆盖项与全局默认的合并语义（含「全局关、规则开」核心场景）；
//   - cache.ttl 指针化后的 0/nil 语义：显式 0=永不过期（全局与规则级一致），
//     未配置（nil）兜底 DefaultCacheTTL，负数 Validate 报错并提示「0 表示永不过期」；
//   - ActiveCacheDirs：可能被写入的缓存目录 -> 生效清理间隔；
//   - Validate：规则 cache.clean-interval / cache.path 的合法性校验。

import (
	"os"
	"path/filepath"
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
		globalPath     = "./global-cache"    // 全局缓存目录
		globalInterval = 10 * time.Minute    // 全局清理间隔
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
			name:          "规则覆盖path与clean-interval生效",
			globalEnabled: true,
			rule:          RuleCache{Path: strPtr("./rule-cache"), CleanInterval: durPtr(5 * time.Minute)},
			wantEnabled:   true, // 未配 enabled 沿用全局 true
			wantPath:      "./rule-cache",
			wantInterval:  5 * time.Minute,
		},
		{
			name:          "规则不配path与interval_沿用全局",
			globalEnabled: true,
			rule:          RuleCache{TTL: durPtr(time.Hour)},
			wantEnabled:   true,
			wantPath:      globalPath,
			wantInterval:  globalInterval,
		},
		{
			name:          "规则path为空白串_视为未配置仍用全局_interval显式0生效",
			globalEnabled: true,
			rule:          RuleCache{Path: strPtr("   "), CleanInterval: durPtr(0)},
			wantEnabled:   true,
			wantPath:      globalPath,
			wantInterval:  0, // 显式 0 = 关闭该目录后台清理
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
				TTL:           durPtr(30 * time.Minute), // CacheConfig.TTL 已改为 *Duration：nil=未配置→默认 30m
				CleanInterval: Duration(globalInterval),
			}
			cfg.DomainRules = []DomainRule{ruleCacheCfg(ruleName, tt.rule)}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}

			o := cfg.ResolveOptions("", testTargetURL)
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
				TTL:           durPtr(30 * time.Minute), // CacheConfig.TTL 已改为 *Duration：nil=未配置→默认 30m
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

// TestResolveOptionsCacheTTL 表驱动验证 CacheConfig.TTL / RuleCache.TTL 指针化后的合并语义：
//   - 全局 ttl 显式 0 = 永不过期：Validate 合法，ResolveOptions 生效 CacheTTL=0；
//   - 规则 ttl 显式 0 覆盖全局生效（全局 30m / 全局 0 两种底版下都为 0）；
//   - 全局未配置（TTL=nil）：Validate 兜底 DefaultCacheTTL(30m)，规则覆盖项照常生效；
//   - Default() 预置路径：未解析 YAML 时全局 TTL 已预置 30m，GlobalCacheTTL 直接可用。
func TestResolveOptionsCacheTTL(t *testing.T) {
	tests := []struct {
		name      string
		globalTTL *Duration // 全局 cache.ttl（nil = 未配置，走 Validate 兜底）
		withRule  bool      // 是否挂载命中测试 URL 的规则
		ruleTTL   *Duration // 规则 cache.ttl 覆盖项（nil = 未配置，沿用全局）
		wantTTL   time.Duration
	}{
		{
			name:      "全局ttl显式0_永不过期",
			globalTTL: durPtr(0),
			wantTTL:   0,
		},
		{
			name:      "规则ttl显式0_覆盖全局30m",
			globalTTL: durPtr(30 * time.Minute),
			withRule:  true,
			ruleTTL:   durPtr(0),
			wantTTL:   0,
		},
		{
			name:      "全局ttl为0_规则未配ttl_沿用全局仍为0",
			globalTTL: durPtr(0),
			withRule:  true,
			wantTTL:   0,
		},
		{
			name:      "全局与规则ttl都显式0_仍为0",
			globalTTL: durPtr(0),
			withRule:  true,
			ruleTTL:   durPtr(0),
			wantTTL:   0,
		},
		{
			name:      "全局未配置nil_Validate兜底30m",
			globalTTL: nil,
			wantTTL:   DefaultCacheTTL,
		},
		{
			name:      "全局未配置nil_规则配1h_规则生效",
			globalTTL: nil,
			withRule:  true,
			ruleTTL:   durPtr(time.Hour),
			wantTTL:   time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Cache.TTL = tt.globalTTL
			if tt.withRule {
				cfg.DomainRules = []DomainRule{ruleCacheCfg("ttl-rule", RuleCache{TTL: tt.ruleTTL})}
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions("", testTargetURL)
			if o.CacheTTL != tt.wantTTL {
				t.Errorf("CacheTTL = %v, want %v", o.CacheTTL, tt.wantTTL)
			}
		})
	}

	// Default() 预置路径：全局 TTL 在 Default 阶段即预置 30m，
	// 未经额外兜底 GlobalCacheTTL 也应直接返回默认值。
	t.Run("Default预置ttl为30m", func(t *testing.T) {
		cfg := Default()
		if cfg.Cache.TTL == nil {
			t.Fatal("Default() 全局 TTL = nil, want 预置 DefaultCacheTTL")
		}
		if got := cfg.Cache.TTL.D(); got != DefaultCacheTTL {
			t.Errorf("Default() 全局 TTL = %v, want %v", got, DefaultCacheTTL)
		}
		if got := cfg.GlobalCacheTTL(); got != DefaultCacheTTL {
			t.Errorf("GlobalCacheTTL() = %v, want %v", got, DefaultCacheTTL)
		}
	})

	// YAML 加载路径：验证 ttl 经 UnmarshalYAML 解析后语义不变。
	t.Run("YAML加载_ttl显式0_生效0", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		yamlBuf := []byte("cache:\n  enabled: true\n  ttl: 0\n")
		if err := os.WriteFile(path, yamlBuf, 0o644); err != nil {
			t.Fatalf("写临时配置文件失败: %v", err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) 意外报错: %v", path, err)
		}
		if got := cfg.GlobalCacheTTL(); got != 0 {
			t.Errorf("GlobalCacheTTL() = %v, want 0（YAML 显式 ttl: 0 = 永不过期）", got)
		}
		if got := cfg.ResolveOptions("", testTargetURL).CacheTTL; got != 0 {
			t.Errorf("ResolveOptions().CacheTTL = %v, want 0", got)
		}
	})

	// YAML 加载路径：全局未配 ttl（nil）应兜底默认 30m。
	t.Run("YAML加载_未配ttl_兜底30m", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		yamlBuf := []byte("cache:\n  enabled: true\n")
		if err := os.WriteFile(path, yamlBuf, 0o644); err != nil {
			t.Fatalf("写临时配置文件失败: %v", err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) 意外报错: %v", path, err)
		}
		if got := cfg.ResolveOptions("", testTargetURL).CacheTTL; got != DefaultCacheTTL {
			t.Errorf("ResolveOptions().CacheTTL = %v, want %v（未配置默认 30m）", got, DefaultCacheTTL)
		}
	})

	// YAML 加载路径：规则级 ttl: 0 显式覆盖全局默认 30m，验证 *Duration 的 YAML 解析。
	t.Run("YAML加载_规则ttl显式0_覆盖全局", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		yamlBuf := []byte("domain-rules:\n" +
			"  - name: r0\n" +
			"    match: 'raw\\.githubusercontent\\.com'\n" +
			"    cache:\n" +
			"      ttl: 0\n")
		if err := os.WriteFile(path, yamlBuf, 0o644); err != nil {
			t.Fatalf("写临时配置文件失败: %v", err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) 意外报错: %v", path, err)
		}
		o := cfg.ResolveOptions("", testTargetURL)
		if o.Rule != "r0" {
			t.Fatalf("Rule = %q, want %q（规则未命中则后续断言失真）", o.Rule, "r0")
		}
		if got := o.CacheTTL; got != 0 {
			t.Errorf("CacheTTL = %v, want 0（规则 ttl: 0 覆盖全局默认 30m）", got)
		}
	})
}

// TestValidateCacheTTLNegative 校验 cache.ttl 负数为配置错误，且错误信息
// 提示「0 表示永不过期」；同时覆盖 GlobalCacheTTL 的 nil 防御与显式 0 透传。
func TestValidateCacheTTLNegative(t *testing.T) {
	t.Run("全局ttl为负_报错", func(t *testing.T) {
		cfg := Default()
		cfg.Cache.TTL = durPtr(-time.Second)
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() 期望报错（cache.ttl 为负），实际为 nil")
		}
		for _, want := range []string{"cache.ttl", "0 表示永不过期"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
			}
		}
	})

	t.Run("规则ttl为负_报错", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{
			ruleCacheCfg("githubusercontent", RuleCache{TTL: durPtr(-time.Minute)}),
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() 期望报错（规则 cache.ttl 为负），实际为 nil")
		}
		// 错误信息需定位到规则与字段，并提示 0 的合法语义
		for _, want := range []string{"githubusercontent", "cache.ttl", "0 表示永不过期"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
			}
		}
	})

	t.Run("GlobalCacheTTL_nil防御_兜底默认值", func(t *testing.T) {
		cfg := &Config{} // 未经 Validate 的构造场景：TTL 为 nil
		if got := cfg.GlobalCacheTTL(); got != DefaultCacheTTL {
			t.Errorf("GlobalCacheTTL() = %v, want %v（nil 防御兜底默认值）", got, DefaultCacheTTL)
		}
	})

	t.Run("GlobalCacheTTL_显式0_原样返回0", func(t *testing.T) {
		cfg := Default()
		cfg.Cache.TTL = durPtr(0) // 不经 Validate 直接读：0 = 永不过期，不应被改写为默认值
		if got := cfg.GlobalCacheTTL(); got != 0 {
			t.Errorf("GlobalCacheTTL() = %v, want 0（显式 0 原样透传）", got)
		}
	})
}
