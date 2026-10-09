package config

// log_host_test.go —— 域名规则匹配纳入入站 Host（aaf29ca）与 log.level 配置的单测：
//   - Host 参与匹配：match 对「http://<入站Host>/<目标URL>」整串非锚定匹配，
//     覆盖用户真实场景（github.path.* 子域路由 + 规则级 url-redirect / cache 覆盖）；
//   - Host 归一化：入站 Host 小写化 + trim 后参与匹配；
//   - 旧写法兼容：纯目标 URL 正则（不含 Host 段）照常命中；
//   - exclude 对整串匹配：命中 exclude 的规则被跳过，链式落位到后续规则或全局默认；
//   - log.level 解析：空串默认 info、大小写/空白归一化、非法值 Validate 快速失败、四级映射；
//   - ResolveOptions 匹配过程分级日志：debug 输出逐条匹配明细，info 只输出最终结果。

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// 用户真实场景的入站 Host 与目标 URL（sslip.io 子域路由到本服务的端口域名）。
const (
	hostGitHubPath = "github.path.proxy-cache.port.8080.proxy_sslip.192.168.137.2.sslip.io"
	urlVlessYAML   = "https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
)

// githubPathTemplates 是用户真实场景规则级 url-redirect 的三条候选模板。
var githubPathTemplates = []string{
	"https://gh-proxy.com/$1",
	"https://ghproxy.net/$1",
	"https://mirror.ghproxy.com/$1",
}

// githubPathRule 构造一条按入站子域 github.path.* 路由的规则：
// 规则级 url-redirect 三条候选 + cache ttl/path 覆盖。
func githubPathRule() DomainRule {
	return DomainRule{
		Name:        "github-path",
		Match:       `http://github\.path\..+`,
		Cache:       RuleCache{TTL: durPtr(2 * time.Hour), Path: strPtr("./rule-cache")},
		URLRedirect: &githubPathTemplates,
	}
}

// mustValidate 校验配置，失败即 Fatal（后续断言依赖 Validate 填充编译结果与生效级别）。
func mustValidate(t *testing.T, cfg *Config) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
}

// TestResolveOptionsHostRouting 回归用户真实场景：入站 Host 命中子域规则时
// 规则级 url-redirect / cache.ttl / cache.path 全部生效；Host 不命中时走全局默认、无候选。
func TestResolveOptionsHostRouting(t *testing.T) {
	t.Run("Host命中子域规则_规则级覆盖全部生效", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{githubPathRule()}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostGitHubPath, urlVlessYAML)
		if o.Rule != "github-path" {
			t.Fatalf("Rule = %q, want %q（规则未命中则后续断言失真）", o.Rule, "github-path")
		}
		if len(o.URLRedirect) != len(githubPathTemplates) {
			t.Fatalf("URLRedirect 长度 = %d, want %d（规则候选应为三条模板）", len(o.URLRedirect), len(githubPathTemplates))
		}
		for i, want := range githubPathTemplates {
			if o.URLRedirect[i] != want {
				t.Errorf("URLRedirect[%d] = %q, want 规则候选 %q", i, o.URLRedirect[i], want)
			}
		}
		if o.CacheTTL != 2*time.Hour {
			t.Errorf("CacheTTL = %v, want 规则覆盖的 %v", o.CacheTTL, 2*time.Hour)
		}
		if o.CachePath != "./rule-cache" {
			t.Errorf("CachePath = %q, want 规则覆盖的 %q", o.CachePath, "./rule-cache")
		}
	})

	t.Run("Host不命中_走全局默认且无候选", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{githubPathRule()}
		mustValidate(t, cfg)

		for _, host := range []string{"", "other.example.com", "githubpath.example.com"} {
			o := cfg.ResolveOptions(host, urlVlessYAML)
			if o.Rule != "" {
				t.Errorf("host=%q: Rule = %q, want 空串（不应命中任何规则）", host, o.Rule)
			}
			if len(o.URLRedirect) != 0 {
				t.Errorf("host=%q: URLRedirect = %v, want 空（全局未配置候选）", host, o.URLRedirect)
			}
			if o.CacheTTL != DefaultCacheTTL {
				t.Errorf("host=%q: CacheTTL = %v, want 全局默认 %v", host, o.CacheTTL, DefaultCacheTTL)
			}
			if o.CachePath != "./cache-data" {
				t.Errorf("host=%q: CachePath = %q, want 全局默认 %q", host, o.CachePath, "./cache-data")
			}
		}
	})
}

// TestResolveOptionsHostCaseInsensitive 验证入站 Host 大小写归一化：
// Host 先转小写再参与匹配，混合大小写 Host 命中小写 match 正则。
func TestResolveOptionsHostCaseInsensitive(t *testing.T) {
	cfg := Default()
	cfg.DomainRules = []DomainRule{{Name: "github-path", Match: `http://github\.path\.`}}
	mustValidate(t, cfg)

	o := cfg.ResolveOptions("GitHub.Path.Example.COM", urlVlessYAML)
	if o.Rule != "github-path" {
		t.Errorf("Rule = %q, want %q（Host 应归一化为小写后命中）", o.Rule, "github-path")
	}
}

// TestResolveOptionsLegacyTargetURLMatch 验证旧目标 URL 正则写法兼容：
// match 只含目标 URL 特征（不含 Host 段）时，任意 Host（含空）都能照常命中。
func TestResolveOptionsLegacyTargetURLMatch(t *testing.T) {
	cfg := Default()
	cfg.DomainRules = []DomainRule{{Name: "raw-legacy", Match: `raw\.githubusercontent\.com`}}
	mustValidate(t, cfg)

	for _, host := range []string{"", "anything.example.com", hostGitHubPath} {
		o := cfg.ResolveOptions(host, urlVlessYAML)
		if o.Rule != "raw-legacy" {
			t.Errorf("host=%q: Rule = %q, want %q（旧写法应对整串非锚定命中）", host, o.Rule, "raw-legacy")
		}
	}
}

// TestResolveOptionsExcludeWholeString 验证 exclude 对「http://<Host>/<目标URL>」整串匹配：
// match 命中但 exclude 命中整串（Host 段）时该规则被排除，落到全局默认或链式落位到后续规则。
func TestResolveOptionsExcludeWholeString(t *testing.T) {
	t.Run("exclude命中整串Host段_规则被排除走全局默认", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:    "github-path",
			Match:   `http://github\.path\..+`,
			Exclude: []string{`github\.path\.`}, // 整串中的 Host 段命中 → 该规则被排除
			Cache:   RuleCache{TTL: durPtr(6 * time.Hour)},
		}}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostGitHubPath, urlVlessYAML)
		if o.Rule != "" {
			t.Errorf("Rule = %q, want 空串（被 exclude 排除后应走全局默认）", o.Rule)
		}
		if o.CacheTTL != DefaultCacheTTL {
			t.Errorf("CacheTTL = %v, want 全局默认 %v（规则被排除不应覆盖）", o.CacheTTL, DefaultCacheTTL)
		}
	})

	t.Run("首条被exclude排除_链式落位到第二条规则", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{
			{
				Name:    "github-path-excluded",
				Match:   `http://github\.path\..+`,
				Exclude: []string{`github\.path\.`},
				Cache:   RuleCache{TTL: durPtr(6 * time.Hour)},
			},
			{
				Name:  "github-path-second",
				Match: `http://github\.path\.`,
				Cache: RuleCache{TTL: durPtr(1 * time.Hour), Path: strPtr("./second-cache")},
			},
		}
		mustValidate(t, cfg)

		o := cfg.ResolveOptions(hostGitHubPath, urlVlessYAML)
		if o.Rule != "github-path-second" {
			t.Errorf("Rule = %q, want %q（第一条被排除后应命中第二条）", o.Rule, "github-path-second")
		}
		if o.CacheTTL != 1*time.Hour || o.CachePath != "./second-cache" {
			t.Errorf("CacheTTL/CachePath = %v/%q, want 第二条规则覆盖的 %v/%q",
				o.CacheTTL, o.CachePath, 1*time.Hour, "./second-cache")
		}
	})
}

// TestLogLevelParsing 验证 log.level 的解析与映射：
// 空串默认 info；大小写与首尾空白归一化；非法值 Validate 快速失败；四级映射到 slog.Level。
func TestLogLevelParsing(t *testing.T) {
	t.Run("空串默认info", func(t *testing.T) {
		cfg := Default()
		cfg.Log.Level = ""
		mustValidate(t, cfg)
		if cfg.Log.Level != "info" {
			t.Errorf("Log.Level = %q, want 归一化默认 %q", cfg.Log.Level, "info")
		}
		if got := cfg.LogLevel(); got != slog.LevelInfo {
			t.Errorf("LogLevel() = %v, want %v", got, slog.LevelInfo)
		}
	})

	t.Run("大小写与空白归一化后生效", func(t *testing.T) {
		tests := []struct {
			raw     string
			wantStr string
			wantLvl slog.Level
		}{
			{"DEBUG", "debug", slog.LevelDebug},
			{" Info ", "info", slog.LevelInfo},
			{"  WARN  ", "warn", slog.LevelWarn},
			{"Error", "error", slog.LevelError},
		}
		for _, tt := range tests {
			cfg := Default()
			cfg.Log.Level = tt.raw
			mustValidate(t, cfg)
			if cfg.Log.Level != tt.wantStr {
				t.Errorf("Log.Level = %q, want 归一化 %q（输入 %q）", cfg.Log.Level, tt.wantStr, tt.raw)
			}
			if got := cfg.LogLevel(); got != tt.wantLvl {
				t.Errorf("LogLevel() = %v, want %v（输入 %q）", got, tt.wantLvl, tt.raw)
			}
		}
	})

	t.Run("verbose非法_Validate快速失败", func(t *testing.T) {
		cfg := Default()
		cfg.Log.Level = "verbose"
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() 期望报错（log.level 非法值），实际为 nil")
		}
		if !strings.Contains(err.Error(), "log.level") {
			t.Errorf("错误信息 %q 应包含 %q", err.Error(), "log.level")
		}
	})

	t.Run("四级各自映射slog.Level", func(t *testing.T) {
		tests := []struct {
			level string
			want  slog.Level
		}{
			{"debug", slog.LevelDebug},
			{"info", slog.LevelInfo},
			{"warn", slog.LevelWarn},
			{"error", slog.LevelError},
		}
		for _, tt := range tests {
			cfg := Default()
			cfg.Log.Level = tt.level
			mustValidate(t, cfg)
			if got := cfg.LogLevel(); got != tt.want {
				t.Errorf("LogLevel() = %v, want %v（log.level=%q）", got, tt.want, tt.level)
			}
		}
	})
}

// captureResolveLog 在指定日志级别下捕获一次 ResolveOptions 的全部日志输出并返回文本。
func captureResolveLog(t *testing.T, level slog.Level, host string) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
	defer slog.SetDefault(prev)

	cfg := Default()
	cfg.DomainRules = []DomainRule{githubPathRule()}
	mustValidate(t, cfg)
	cfg.ResolveOptions(host, urlVlessYAML)
	return buf.String()
}

// TestResolveOptionsLogging 验证匹配过程日志的分级输出：
// debug 级别含逐条「域名规则匹配」明细与「规则匹配结果」（命中含 rule=<规则名>，未命中含 global-default）；
// info 级别只输出结果行、无逐条明细。
func TestResolveOptionsLogging(t *testing.T) {
	t.Run("debug级别_命中含规则名与匹配明细", func(t *testing.T) {
		out := captureResolveLog(t, slog.LevelDebug, hostGitHubPath)
		for _, want := range []string{"域名规则匹配", "规则匹配结果", "rule=github-path"} {
			if !strings.Contains(out, want) {
				t.Errorf("debug 日志缺少 %q，实际输出:\n%s", want, out)
			}
		}
	})

	t.Run("debug级别_未命中含global-default", func(t *testing.T) {
		out := captureResolveLog(t, slog.LevelDebug, "other.example.com")
		for _, want := range []string{"域名规则匹配", "规则匹配结果", "rule=global-default", "未命中任何域名规则"} {
			if !strings.Contains(out, want) {
				t.Errorf("debug 日志缺少 %q，实际输出:\n%s", want, out)
			}
		}
	})

	t.Run("info级别_只有结果行无逐条明细", func(t *testing.T) {
		out := captureResolveLog(t, slog.LevelInfo, hostGitHubPath)
		if !strings.Contains(out, "规则匹配结果") || !strings.Contains(out, "rule=github-path") {
			t.Errorf("info 日志应包含结果行 rule=github-path，实际输出:\n%s", out)
		}
		if strings.Contains(out, "域名规则匹配") {
			t.Errorf("info 级别不应输出逐条匹配明细，实际输出:\n%s", out)
		}
	})
}
