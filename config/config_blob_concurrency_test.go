package config

// config_blob_concurrency_test.go —— 时间驱动的单层 blob 多源并发补充间隔
//（blob-concurrency-interval，全局 + 规则级，提交 a28a014）的配置层单测：
//   - 默认值：Default() / 零值 Config 的 nil 防御访问器 = DefaultBlobConcurrencyInterval（5m），
//     ResolveOptions 未命中规则时的生效值同缺省；
//   - Validate：nil 合法（沿用缺省）、负数启动失败（全局与规则级，报错点名键名与规则）、
//     0 合法（关闭时间驱动补充，判速触发仍独立生效）、0 < 值 < 1s 就地钳制为 1s；
//   - 规则级覆盖全局：规则显式值（含显式 0 = 关闭）覆盖全局，规则未配置继承全局；
//   - YAML 加载路径（Load → Validate）：负数启动失败、500ms 被钳制为 1s。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestBlobConcurrencyIntervalDefaults 默认值与 nil 防御：
// Default() 未显式配置时访问器返回 DefaultBlobConcurrencyInterval（5m）；零值 Config
// （未经 Validate 的构造场景）经 nil 防御返回同样的默认；ResolveOptions 未命中规则时
// 生效值同为缺省。
func TestBlobConcurrencyIntervalDefaults(t *testing.T) {
	if DefaultBlobConcurrencyInterval != 5*time.Minute {
		t.Errorf("DefaultBlobConcurrencyInterval = %s, want 5m（配置文档承诺的缺省值）", DefaultBlobConcurrencyInterval)
	}
	cfg := Default()
	if got := cfg.GlobalBlobConcurrencyInterval(); got != DefaultBlobConcurrencyInterval {
		t.Errorf("Default().GlobalBlobConcurrencyInterval() = %s, want %s", got, DefaultBlobConcurrencyInterval)
	}
	var zero Config
	if got := zero.GlobalBlobConcurrencyInterval(); got != DefaultBlobConcurrencyInterval {
		t.Errorf("零值 Config 的 nil 防御 = %s, want %s", got, DefaultBlobConcurrencyInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	o := cfg.ResolveOptions("reg.example.com", "v2/app/blobs/sha256:abc", false)
	if o.BlobConcurrencyInterval != DefaultBlobConcurrencyInterval {
		t.Errorf("ResolveOptions 未命中规则时 BlobConcurrencyInterval = %s, want %s",
			o.BlobConcurrencyInterval, DefaultBlobConcurrencyInterval)
	}
}

// TestValidateBlobConcurrencyInterval 表驱动验证全局间隔的合法性/钳制语义：
// nil = 未配置（沿用缺省，合法）；负数 = 配置错误（启动快速失败）；0 = 关闭（合法）；
// 0 < 值 < 1s 就地钳制为 1s；>=1s 原样保留。
func TestValidateBlobConcurrencyInterval(t *testing.T) {
	tests := []struct {
		name    string
		in      *Duration
		wantErr bool
		// want 生效值（wantErr 时无意义）
		want time.Duration
	}{
		{name: "未配置nil合法沿用缺省", in: nil, want: DefaultBlobConcurrencyInterval},
		{name: "负数启动失败", in: durPtr(-time.Second), wantErr: true},
		{name: "负一分钟启动失败", in: durPtr(-time.Minute), wantErr: true},
		{name: "零合法关闭", in: durPtr(0), want: 0},
		{name: "1毫秒钳制为1秒", in: durPtr(time.Millisecond), want: time.Second},
		{name: "500毫秒钳制为1秒", in: durPtr(500 * time.Millisecond), want: time.Second},
		{name: "999毫秒钳制为1秒", in: durPtr(999 * time.Millisecond), want: time.Second},
		{name: "恰好1秒不钳制", in: durPtr(time.Second), want: time.Second},
		{name: "十分钟原样保留", in: durPtr(10 * time.Minute), want: 10 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.BlobConcurrencyInterval = tt.in
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("负数应被 Validate 拒绝（启动快速失败），实际通过")
				}
				msg := err.Error()
				if !strings.Contains(msg, "blob-concurrency-interval") {
					t.Errorf("报错 %q 应点名 blob-concurrency-interval", msg)
				}
				if !strings.Contains(msg, "不能为负数") {
					t.Errorf("报错 %q 应说明不能为负数（0 = 关闭）", msg)
				}
				return
			}
			if err != nil {
				t.Fatalf("合法配置 Validate() 意外报错: %v", err)
			}
			if got := cfg.GlobalBlobConcurrencyInterval(); got != tt.want {
				t.Errorf("生效值 = %s, want %s", got, tt.want)
			}
		})
	}
}

// TestValidateBlobConcurrencyIntervalRuleLevel 规则级间隔的校验与钳制：
// 负数报错点名 domain-rules[下标] 与规则名；0 与 <1s（钳制 1s）合法；nil 继承全局。
func TestValidateBlobConcurrencyIntervalRuleLevel(t *testing.T) {
	t.Run("规则级负数启动失败", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:                    "blob-conc-neg",
			Type:                    RuleTypeDocker,
			Match:                   `http://reg\.example\.com/`,
			BlobConcurrencyInterval: durPtr(-time.Second),
		}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("规则级负数应被 Validate 拒绝，实际通过")
		}
		msg := err.Error()
		for _, want := range []string{"domain-rules[0]", "blob-conc-neg", "blob-concurrency-interval", "不能为负数"} {
			if !strings.Contains(msg, want) {
				t.Errorf("报错 %q 应包含 %q（快速定位配置位置与键名）", msg, want)
			}
		}
	})

	t.Run("规则级500毫秒钳制为1秒", func(t *testing.T) {
		cfg := Default()
		cfg.DomainRules = []DomainRule{{
			Name:                    "blob-conc-ms",
			Type:                    RuleTypeDocker,
			Match:                   `http://reg\.example\.com/`,
			BlobConcurrencyInterval: durPtr(500 * time.Millisecond),
		}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() 意外报错: %v", err)
		}
		o := cfg.ResolveOptions("reg.example.com", "v2/app/blobs/sha256:abc", false)
		if o.BlobConcurrencyInterval != time.Second {
			t.Errorf("规则级 500ms 应被钳制为 1s，生效值 = %s", o.BlobConcurrencyInterval)
		}
	})

	t.Run("规则级零合法关闭_覆盖全局", func(t *testing.T) {
		cfg := Default()
		cfg.BlobConcurrencyInterval = durPtr(3 * time.Minute)
		cfg.DomainRules = []DomainRule{{
			Name:                    "blob-conc-off",
			Type:                    RuleTypeDocker,
			Match:                   `http://reg\.example\.com/`,
			BlobConcurrencyInterval: durPtr(0),
		}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("规则级 0（关闭）应合法，Validate() 报错: %v", err)
		}
		o := cfg.ResolveOptions("reg.example.com", "v2/app/blobs/sha256:abc", false)
		if o.BlobConcurrencyInterval != 0 {
			t.Errorf("规则级显式 0 应覆盖全局（关闭时间驱动补充），生效值 = %s", o.BlobConcurrencyInterval)
		}
	})
}

// TestResolveOptionsBlobConcurrencyInterval 规则级覆盖全局的合并语义（同 slow-download-*）：
// 均未配置 = 缺省 5m；规则未配置继承全局；规则显式值覆盖全局；规则显式 0 关闭；
// 未命中规则（Host 不符）沿用全局。
func TestResolveOptionsBlobConcurrencyInterval(t *testing.T) {
	const (
		host    = "reg.example.com"
		other   = "other.example.com"
		target  = "v2/app/blobs/sha256:abc"
		ruleRe  = `http://reg\.example\.com/`
		ruleLbl = "blob-conc-rule"
	)
	tests := []struct {
		name   string
		global *Duration
		rule   *Duration
		want   time.Duration
	}{
		{name: "均未配置_缺省5m", global: nil, rule: nil, want: DefaultBlobConcurrencyInterval},
		{name: "全局3m_规则nil继承全局", global: durPtr(3 * time.Minute), rule: nil, want: 3 * time.Minute},
		{name: "规则30s覆盖全局3m", global: durPtr(3 * time.Minute), rule: durPtr(30 * time.Second), want: 30 * time.Second},
		{name: "规则显式0关闭覆盖全局", global: durPtr(3 * time.Minute), rule: durPtr(0), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.BlobConcurrencyInterval = tt.global
			cfg.DomainRules = []DomainRule{{
				Name:                    ruleLbl,
				Type:                    RuleTypeDocker,
				Match:                   ruleRe,
				BlobConcurrencyInterval: tt.rule,
			}}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() 意外报错: %v", err)
			}
			o := cfg.ResolveOptions(host, target, false)
			if o.BlobConcurrencyInterval != tt.want {
				t.Errorf("o.BlobConcurrencyInterval = %s, want %s", o.BlobConcurrencyInterval, tt.want)
			}
			if o.RuleType != RuleTypeDocker {
				t.Errorf("o.RuleType = %q, want %q（类型门控依赖它）", o.RuleType, RuleTypeDocker)
			}
			if o.Rule != ruleLbl {
				t.Errorf("o.Rule = %q, want %q（生效规则名）", o.Rule, ruleLbl)
			}
			// 未命中规则的 Host 沿用全局（规则级配置不泄漏到其他入站域名）
			global := cfg.GlobalBlobConcurrencyInterval()
			if got := cfg.ResolveOptions(other, target, false).BlobConcurrencyInterval; got != global {
				t.Errorf("未命中规则时生效值 = %s, want 全局 %s", got, global)
			}
		})
	}
}

// TestLoadBlobConcurrencyIntervalYAML YAML 加载路径（Load = 解析 + Validate，与真实启动
// 同一条链路）：负数启动失败；500ms 被钳制为 1s；规则级显式值覆盖全局；缺省时 5m。
func TestLoadBlobConcurrencyIntervalYAML(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("写临时配置失败: %v", err)
		}
		return path
	}

	t.Run("全局负数启动失败", func(t *testing.T) {
		path := write(t, "listen: \"127.0.0.1:0\"\nblob-concurrency-interval: -1s\n")
		if _, err := Load(path); err == nil {
			t.Fatal("全局 blob-concurrency-interval 为负数时应启动失败")
		}
	})

	t.Run("规则级负数启动失败", func(t *testing.T) {
		path := write(t, `
listen: "127.0.0.1:0"
domain-rules:
  - name: "blob-conc-yaml-neg"
    type: docker
    match: 'http://reg\.example\.com/'
    blob-concurrency-interval: -2s
`)
		if _, err := Load(path); err == nil {
			t.Fatal("规则级 blob-concurrency-interval 为负数时应启动失败")
		}
	})

	t.Run("全局500毫秒钳制为1秒", func(t *testing.T) {
		path := write(t, "listen: \"127.0.0.1:0\"\nblob-concurrency-interval: 500ms\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("500ms 合法（钳制 1s），Load 意外报错: %v", err)
		}
		if got := cfg.GlobalBlobConcurrencyInterval(); got != time.Second {
			t.Errorf("500ms 应被钳制为 1s，生效值 = %s", got)
		}
	})

	t.Run("规则级覆盖全局与零关闭", func(t *testing.T) {
		path := write(t, `
listen: "127.0.0.1:0"
blob-concurrency-interval: 3m
domain-rules:
  - name: "blob-conc-yaml-on"
    type: docker
    match: 'http://reg\.example\.com/'
    blob-concurrency-interval: 45s
  - name: "blob-conc-yaml-off"
    type: docker
    match: 'http://off\.example\.com/'
    blob-concurrency-interval: 0
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load 意外报错: %v", err)
		}
		if got := cfg.GlobalBlobConcurrencyInterval(); got != 3*time.Minute {
			t.Errorf("全局生效值 = %s, want 3m", got)
		}
		if got := cfg.ResolveOptions("reg.example.com", "v2/app/blobs/sha256:abc", false).BlobConcurrencyInterval; got != 45*time.Second {
			t.Errorf("规则级生效值 = %s, want 45s（规则覆盖全局）", got)
		}
		if got := cfg.ResolveOptions("off.example.com", "v2/app/blobs/sha256:abc", false).BlobConcurrencyInterval; got != 0 {
			t.Errorf("规则级 0 应关闭时间驱动补充，生效值 = %s, want 0", got)
		}
	})

	t.Run("缺省5分钟", func(t *testing.T) {
		path := write(t, "listen: \"127.0.0.1:0\"\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load 意外报错: %v", err)
		}
		if got := cfg.GlobalBlobConcurrencyInterval(); got != 5*time.Minute {
			t.Errorf("缺省生效值 = %s, want 5m", got)
		}
	})
}
