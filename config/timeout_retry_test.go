// timeout_retry_test.go —— 分级超时（http-timeout / read-timeout / connect-timeout）、
// 旧 timeout 退化别名回落链、url-redirect 两级重试的配置解析/优先级合并/合法性校验单测。

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// loadConfigFromYAML 把 YAML 文本写入临时文件并经 Load 解析校验（与真实加载路径一致）。
func loadConfigFromYAML(t *testing.T, s string) *Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	return cfg
}

// TestTimeoutRetryDefaultsUnlimited 未配置任何超时/重试字段时全部为 0：
// 不限制、不重试（旧版 timeout 会兜底 60s，属有意移除的行为变更）。
func TestTimeoutRetryDefaultsUnlimited(t *testing.T) {
	cfg := loadConfigFromYAML(t, "listen: \"127.0.0.1:0\"\n")
	if cfg.Timeout != 0 {
		t.Errorf("默认 timeout 应为 0（不限制），得到 %s", cfg.Timeout)
	}
	if cfg.GlobalReadTimeout() != 0 || cfg.GlobalHTTPTimeout() != 0 || cfg.GlobalConnectTimeout() != 0 {
		t.Errorf("默认三个超时均应不限制（0），得到 read=%s http=%s connect=%s",
			cfg.GlobalReadTimeout(), cfg.GlobalHTTPTimeout(), cfg.GlobalConnectTimeout())
	}
	if cfg.GlobalURLRedirectItemErrorRetry() != 0 || cfg.GlobalURLRedirectAllErrorRetry() != 0 {
		t.Errorf("默认重试次数应为 0，得到 item=%d all=%d",
			cfg.GlobalURLRedirectItemErrorRetry(), cfg.GlobalURLRedirectAllErrorRetry())
	}
	o := cfg.ResolveOptions("h.example.com", "https://example.com/a", true)
	if o.HTTPTimeout != 0 || o.ReadTimeout != 0 || o.ConnectTimeout != 0 {
		t.Errorf("Options 默认超时应全为 0，得到 %+v", o)
	}
	if o.URLRedirectItemErrorRetry != 0 || o.URLRedirectAllErrorRetry != 0 {
		t.Errorf("Options 默认重试应为 0，得到 item=%d all=%d",
			o.URLRedirectItemErrorRetry, o.URLRedirectAllErrorRetry)
	}
}

// TestTimeoutRetryGlobalResolved 全局配置解析生效。
func TestTimeoutRetryGlobalResolved(t *testing.T) {
	cfg := loadConfigFromYAML(t, `
listen: "127.0.0.1:0"
http-timeout: 5m
read-timeout: 45s
connect-timeout: 3s
timeout: 90s
url-redirect-item-error-retry: 2
url-redirect-all-error-retry: 3
`)
	o := cfg.ResolveOptions("h.example.com", "https://example.com/a", true)
	if o.HTTPTimeout != 5*time.Minute {
		t.Errorf("http-timeout 应为 5m，得到 %s", o.HTTPTimeout)
	}
	// read-timeout 已配置：优先于旧 timeout（90s）
	if o.ReadTimeout != 45*time.Second {
		t.Errorf("read-timeout 应为 45s（优先于旧 timeout 90s），得到 %s", o.ReadTimeout)
	}
	if o.ConnectTimeout != 3*time.Second {
		t.Errorf("connect-timeout 应为 3s，得到 %s", o.ConnectTimeout)
	}
	if o.URLRedirectItemErrorRetry != 2 || o.URLRedirectAllErrorRetry != 3 {
		t.Errorf("重试应为 item=2 all=3，得到 item=%d all=%d",
			o.URLRedirectItemErrorRetry, o.URLRedirectAllErrorRetry)
	}
}

// TestReadTimeoutFallbackChain read-timeout 四级回落链：
// rule.read-timeout > rule.timeout > global.read-timeout > global.timeout > 0。
func TestReadTimeoutFallbackChain(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want time.Duration
	}{
		{
			name: "rule.read-timeout 优先于一切",
			yaml: `
listen: "127.0.0.1:0"
read-timeout: 5s
timeout: 90s
domain-rules:
  - name: r
    match: 'example\.com'
    read-timeout: 7s
    timeout: 8s
`,
			want: 7 * time.Second,
		},
		{
			name: "rule 未配 read-timeout 回落 rule.timeout",
			yaml: `
listen: "127.0.0.1:0"
read-timeout: 5s
timeout: 90s
domain-rules:
  - name: r
    match: 'example\.com'
    timeout: 8s
`,
			want: 8 * time.Second,
		},
		{
			name: "rule 都没配回落 global.read-timeout",
			yaml: `
listen: "127.0.0.1:0"
read-timeout: 5s
timeout: 90s
domain-rules:
  - name: r
    match: 'example\.com'
`,
			want: 5 * time.Second,
		},
		{
			name: "global 无 read-timeout 回落 global.timeout",
			yaml: `
listen: "127.0.0.1:0"
timeout: 90s
domain-rules:
  - name: r
    match: 'example\.com'
`,
			want: 90 * time.Second,
		},
		{
			name: "全都没配 = 0 不限制",
			yaml: `
listen: "127.0.0.1:0"
domain-rules:
  - name: r
    match: 'example\.com'
`,
			want: 0,
		},
		{
			name: "rule.timeout 显式 0 = 规则级不限制（覆盖全局 read-timeout）",
			yaml: `
listen: "127.0.0.1:0"
read-timeout: 5s
domain-rules:
  - name: r
    match: 'example\.com'
    timeout: 0
`,
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadConfigFromYAML(t, tc.yaml)
			o := cfg.ResolveOptions("www.example.com", "https://example.com/a", true)
			if o.ReadTimeout != tc.want {
				t.Errorf("生效 read-timeout = %s, want %s", o.ReadTimeout, tc.want)
			}
		})
	}
}

// TestRuleOverridesGlobalTimeoutsAndRetries 规则级覆盖全局（http-timeout /
// connect-timeout / 两个重试；read-timeout 链已在 TestReadTimeoutFallbackChain 覆盖）。
func TestRuleOverridesGlobalTimeoutsAndRetries(t *testing.T) {
	cfg := loadConfigFromYAML(t, `
listen: "127.0.0.1:0"
http-timeout: 1m
connect-timeout: 2s
read-timeout: 10s
url-redirect-item-error-retry: 1
url-redirect-all-error-retry: 0
domain-rules:
  - name: r
    match: 'example\.com'
    http-timeout: 2m
    read-timeout: 20s
    connect-timeout: 4s
    url-redirect-item-error-retry: 3
    url-redirect-all-error-retry: 2
`)
	o := cfg.ResolveOptions("www.example.com", "https://example.com/a", true)
	if o.HTTPTimeout != 2*time.Minute || o.ReadTimeout != 20*time.Second || o.ConnectTimeout != 4*time.Second {
		t.Errorf("规则覆盖未生效：http=%s read=%s connect=%s", o.HTTPTimeout, o.ReadTimeout, o.ConnectTimeout)
	}
	if o.URLRedirectItemErrorRetry != 3 || o.URLRedirectAllErrorRetry != 2 {
		t.Errorf("规则重试覆盖未生效：item=%d all=%d", o.URLRedirectItemErrorRetry, o.URLRedirectAllErrorRetry)
	}
	// 未命中规则的请求沿用全局
	o2 := cfg.ResolveOptions("other.example.net", "https://other.example.net/a", true)
	if o2.HTTPTimeout != time.Minute || o2.ReadTimeout != 10*time.Second || o2.ConnectTimeout != 2*time.Second {
		t.Errorf("全局值未生效：http=%s read=%s connect=%s", o2.HTTPTimeout, o2.ReadTimeout, o2.ConnectTimeout)
	}
	if o2.URLRedirectItemErrorRetry != 1 || o2.URLRedirectAllErrorRetry != 0 {
		t.Errorf("全局重试未生效：item=%d all=%d", o2.URLRedirectItemErrorRetry, o2.URLRedirectAllErrorRetry)
	}
}

// TestNegativeTimeoutRetryRejected 负数超时/重试为配置错误（全局与规则级）。
func TestNegativeTimeoutRetryRejected(t *testing.T) {
	globals := []string{
		"timeout: -5s",
		"http-timeout: -1s",
		"read-timeout: -1s",
		"connect-timeout: -1s",
		"url-redirect-item-error-retry: -1",
		"url-redirect-all-error-retry: -1",
	}
	for _, g := range globals {
		_, err := Load(writeTemp(t, "listen: \"127.0.0.1:0\"\n"+g+"\n"))
		if err == nil {
			t.Errorf("全局 %s 应校验失败（负数非法）", g)
		} else if !strings.Contains(err.Error(), "不能为负数") {
			t.Errorf("全局 %s 错误信息应含「不能为负数」，得到 %v", g, err)
		}
	}
	_, err := Load(writeTemp(t, `
listen: "127.0.0.1:0"
domain-rules:
  - name: r
    match: 'example\.com'
    http-timeout: -1s
    read-timeout: -1s
    connect-timeout: -1s
    timeout: -1s
    url-redirect-item-error-retry: -1
    url-redirect-all-error-retry: -1
`))
	if err == nil {
		t.Fatal("规则级负数超时/重试应校验失败")
	}
	if !strings.Contains(err.Error(), "不能为负数") {
		t.Errorf("错误信息应含「不能为负数」，得到 %v", err)
	}
}

// TestRuleTimeoutZeroAllowed 旧校验「规则 timeout 必须大于 0」已移除：
// 显式 0 = read-timeout 不限制，属合法配置。
func TestRuleTimeoutZeroAllowed(t *testing.T) {
	cfg := loadConfigFromYAML(t, `
listen: "127.0.0.1:0"
domain-rules:
  - name: r
    match: 'example\.com'
    timeout: 0
`)
	o := cfg.ResolveOptions("www.example.com", "https://example.com/a", true)
	if o.ReadTimeout != 0 {
		t.Errorf("规则 timeout: 0 应为不限制（0），得到 %s", o.ReadTimeout)
	}
}

// writeTemp 把文本写入临时文件并返回路径（供 Load 使用）。
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	return path
}
