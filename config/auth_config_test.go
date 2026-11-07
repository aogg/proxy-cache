package config

// auth_config_test.go —— 上游凭据（CandidateAuth：URLRedirectItem.Auth / DomainRule.Auth）
// 的解析、校验与两级解析单测。覆盖点：
//   1. YAML 解析：候选对象 auth 字段正常落字段（不再是未知字段报错）；
//   2. Validate：username 缺失（空 / 纯空白）启动报错且不回显凭据；password 允许空串；
//      规则级 auth 同规则；合法配置通过；
//   3. 两级解析：ResolveOptions 把规则 auth 填进 Options.Auth（nil = 未配置）；
//      Options.CandidateAuth 候选显式 ?? 规则级。

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestCandidateAuthUnmarshal 覆盖点 1：auth 字段 YAML 解析。
func TestCandidateAuthUnmarshal(t *testing.T) {
	var w redirectItemWrapper
	data := "url-redirect:\n" +
		"  - template: https://m.example.com/$1\n" +
		"    auth:\n" +
		"      username: demo@gmail.com\n" +
		"      password: tok-123\n"
	if err := yaml.Unmarshal([]byte(data), &w); err != nil {
		t.Fatalf("auth 字段应正常解析（不再是未知字段），意外报错: %v", err)
	}
	if len(w.URLRedirect) != 1 || w.URLRedirect[0].Auth == nil {
		t.Fatalf("应解析出带 auth 的 1 个候选，得到 %+v", w.URLRedirect)
	}
	if w.URLRedirect[0].Auth.Username != "demo@gmail.com" || w.URLRedirect[0].Auth.Password != "tok-123" {
		t.Errorf("auth 落字段不符: %+v", w.URLRedirect[0].Auth)
	}
}

// TestCandidateAuthValidate 覆盖点 2：username 必填校验（候选级与规则级同规则）。
func TestCandidateAuthValidate(t *testing.T) {
	base := func(authYAML string) string {
		return "url-redirect:\n" +
			"  - template: https://m.example.com/$1\n" +
			authYAML +
			"domain-rules:\n" +
			"  - name: docker\n" +
			"    match: 'http://registry\\.example\\.com/'\n" +
			"    url-redirect:\n" +
			"      - template: https://n.example.com/$1\n"
	}
	// 候选级 auth 缺 username → 报错（且错误信息不回显密码值）
	for _, authYAML := range []string{
		"    auth:\n      password: tok-123\n",
		"    auth:\n      username: \"   \"\n      password: tok-123\n",
	} {
		var cfg Config
		if err := yaml.Unmarshal([]byte(base(authYAML)), &cfg); err != nil {
			t.Fatalf("解析应成功（校验在 Validate），意外报错: %v", err)
		}
		err := cfg.Validate()
		if err == nil {
			t.Errorf("候选 auth 缺 username（%q）应启动报错", authYAML)
			continue
		}
		if strings.Contains(err.Error(), "tok-123") {
			t.Errorf("校验错误信息 %q 不应回显凭据内容", err.Error())
		}
	}
	// 规则级 auth 缺 username → 报错
	var cfgRule Config
	ruleData := "domain-rules:\n" +
		"  - name: docker\n" +
		"    match: 'http://registry\\.example\\.com/'\n" +
		"    auth:\n" +
		"      password: tok-123\n" +
		"    url-redirect:\n" +
		"      - template: https://n.example.com/$1\n"
	if err := yaml.Unmarshal([]byte(ruleData), &cfgRule); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := cfgRule.Validate(); err == nil {
		t.Error("规则级 auth 缺 username 应启动报错")
	}
	// 合法：username 有值 + password 空串（个别 registry 支持空密码）→ 通过
	var cfgOK Config
	okData := base("    auth:\n      username: demo@gmail.com\n      password: \"\"\n")
	if err := yaml.Unmarshal([]byte(okData), &cfgOK); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := cfgOK.Validate(); err != nil {
		t.Errorf("合法 auth（password 空串）不应报错: %v", err)
	}
}

// TestCandidateAuthResolution 覆盖点 3：规则级 auth 填充 Options 并按候选两级解析。
func TestCandidateAuthResolution(t *testing.T) {
	ruleData := "domain-rules:\n" +
		"  - name: docker\n" +
		"    match: 'http://registry\\.example\\.com/'\n" +
		"    auth:\n" +
		"      username: rule-user\n" +
		"      password: rule-pass\n" +
		"    url-redirect:\n" +
		"      - template: https://a.example.com/$1\n" +
		"      - template: https://b.example.com/$1\n" +
		"        auth:\n" +
		"          username: item-user\n" +
		"          password: item-pass\n"
	var cfg Config
	if err := yaml.Unmarshal([]byte(ruleData), &cfg); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate 意外报错: %v", err)
	}
	o := cfg.ResolveOptions("registry.example.com", "v2/x/manifests/latest", false)
	if o.Auth == nil || o.Auth.Username != "rule-user" {
		t.Fatalf("ResolveOptions 应填充规则级 Auth, got %+v", o.Auth)
	}
	items := o.URLRedirect
	if len(items) != 2 {
		t.Fatalf("应解析出 2 个候选, got %d", len(items))
	}
	if got := o.CandidateAuth(items[0]); got == nil || got.Username != "rule-user" {
		t.Errorf("候选0（未显式 auth）应继承规则级, got %+v", got)
	}
	if got := o.CandidateAuth(items[1]); got == nil || got.Username != "item-user" {
		t.Errorf("候选1（显式 auth）应覆盖规则级, got %+v", got)
	}
	// 未配置规则级 auth：候选也未显式 → nil = 匿名
	var plain Config
	plainData := "domain-rules:\n" +
		"  - name: docker\n" +
		"    match: 'http://registry\\.example\\.com/'\n" +
		"    url-redirect:\n" +
		"      - template: https://a.example.com/$1\n"
	if err := yaml.Unmarshal([]byte(plainData), &plain); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := plain.Validate(); err != nil {
		t.Fatalf("Validate 意外报错: %v", err)
	}
	po := plain.ResolveOptions("registry.example.com", "v2/x/manifests/latest", false)
	if po.Auth != nil {
		t.Errorf("未配置 auth 时 Options.Auth 应为 nil, got %+v", po.Auth)
	}
	if got := po.CandidateAuth(po.URLRedirect[0]); got != nil {
		t.Errorf("无任何 auth 来源时候选应为匿名（nil）, got %+v", got)
	}
}

// TestCandidateAuthEnvExpansion 不明文方案：password/username 的 ${VAR} 环境变量引用
// 展开（仅显式 ${..} 形态；纯文本含 $ 原样保留，避免字面量密码被误展开）。
func TestCandidateAuthEnvExpansion(t *testing.T) {
	t.Setenv("HL_HOULANG_TOKEN", "tok-secret-123")
	t.Setenv("HL_HOULANG_USER", "real-user@example.com")
	data := "url-redirect:\n" +
		"  - template: https://m.example.com/$1\n" +
		"    auth:\n" +
		"      username: \"${HL_HOULANG_USER}\"\n" +
		"      password: \"${HL_HOULANG_TOKEN}\"\n" +
		"  - template: https://n.example.com/$1\n" +
		"    auth:\n" +
		"      username: plain$user\n" + // 纯文本含 $（非 ${..} 形态）：原样保留
		"      password: \"pw$123\"\n"
	var cfg Config
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate 意外报错: %v", err)
	}
	if got := cfg.URLRedirect[0].Auth; got.Username != "real-user@example.com" || got.Password != "tok-secret-123" {
		t.Errorf("候选0 凭据应展开环境变量, got %+v", got)
	}
	if got := cfg.URLRedirect[1].Auth; got.Username != "plain$user" || got.Password != "pw$123" {
		t.Errorf("候选1 纯文本凭据应原样保留, got %+v", got)
	}

	// 引用的变量不存在：展开为空串 → username 必填校验拦下（快速失败而非静默匿名）
	var cfg2 Config
	data2 := "url-redirect:\n" +
		"  - template: https://m.example.com/$1\n" +
		"    auth:\n" +
		"      username: \"${HL_NOT_SET_AT_ALL}\"\n" +
		"      password: x\n"
	if err := yaml.Unmarshal([]byte(data2), &cfg2); err != nil {
		t.Fatalf("解析意外报错: %v", err)
	}
	if err := cfg2.Validate(); err == nil {
		t.Error("引用不存在的环境变量导致 username 为空，应启动报错")
	}
}
