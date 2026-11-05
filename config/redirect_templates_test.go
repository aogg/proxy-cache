package config

// redirect_templates_test.go —— validateRedirectItems 的单测（c470a9c 语义）：
// 校验规则从「必须含 $1」放宽为「至少含一个已知占位符」（$1 / ${1} 兼容保留，
// 以及 $http.server.header.full_url / full_url_no_server 的 $ 与 ${} 两种写法），
// 新错误文案需列出全部合法占位符写法；模板仍必须以 http(s):// 开头。
//
// bbf0b2c 补充：template trim 后恰为 $1 / ${1}（原始 URL 独立候选）放行，
// 跳过 http(s):// 前缀检查并就地 trim；独立写 full_url / full_url_no_server
// 占位符仍在报错路径上，错误文案说明展开语义并指引改用 template: $1。

import (
	"strings"
	"testing"
)

// TestValidateRedirectTemplates 表驱动验证 url-redirect 模板校验：
// 合法模板 Validate 通过；无占位符 / 非 http(s):// 开头报错，错误信息定位准确。
func TestValidateRedirectTemplates(t *testing.T) {
	tests := []struct {
		name        string
		tpl         string
		wantErr     string // 非空表示期望报错并包含该子串
		wantTrimmed string // 非空表示校验通过后就地 trim 生效：cfg.URLRedirect[0].Template 应等于该值
	}{
		{name: "dollar1兼容合法", tpl: "https://gh-proxy.com/$1"},
		{name: "大括号1合法", tpl: "https://gh-proxy.com/${1}"},
		{name: "dollar-full_url合法", tpl: "https://mirror.example.com/$http.server.header.full_url"},
		{name: "大括号-full_url_no_server合法", tpl: "https://docker.1panel.live/${http.server.header.full_url_no_server}"},
		{name: "dollar-full_url_no_server合法", tpl: "https://docker.1panel.live/$http.server.header.full_url_no_server"},
		{name: "大括号-full_url合法", tpl: "https://mirror.example.com/${http.server.header.full_url}"},
		// bbf0b2c：$1 / ${1} 原始 URL 独立候选放行（跳过 http(s):// 前缀检查，占位符检查天然满足）
		{name: "原始URL独立候选合法", tpl: "$1"},
		{name: "原始URL独立候选大括号合法", tpl: "${1}"},
		// 带首尾空白的 $1：trim 后放行，且断言校验后就地 trim 生效
		{name: "原始URL独立候选带空白合法且就地trim", tpl: "  $1  ", wantTrimmed: "$1"},
		{
			name:    "无占位符报错",
			tpl:     "https://mirror.example.com/raw/foo/bar",
			wantErr: "缺少占位符",
		},
		{
			name:    "非http开头报错",
			tpl:     "ftp://mirror.example.com/$1",
			wantErr: "必须以 http:// 或 https:// 开头",
		},
		// 有占位符但无 scheme：不因含占位符而放行，仍走 http(s):// 前缀报错路径
		{
			name:    "无scheme有占位符报错",
			tpl:     "mirror.example.com/$1",
			wantErr: "必须以 http:// 或 https:// 开头",
		},
		// 独立写 full_url / full_url_no_server 占位符仍在报错路径上
		//（展开语义与 template: $1 指引文案见 TestValidateRedirectStandalonePlaceholderGuidance）
		{
			name:    "独立full_url占位符报错",
			tpl:     "$http.server.header.full_url",
			wantErr: "必须以 http:// 或 https:// 开头",
		},
		{
			name:    "大括号独立full_url_no_server占位符报错",
			tpl:     "${http.server.header.full_url_no_server}",
			wantErr: "必须以 http:// 或 https:// 开头",
		},
		{
			name:    "空模板报错",
			tpl:     "   ",
			wantErr: "为空",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.URLRedirect = []URLRedirectItem{{Template: tt.tpl}}
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() 意外报错: %v（模板 %q）", err, tt.tpl)
				}
				if tt.wantTrimmed != "" && cfg.URLRedirect[0].Template != tt.wantTrimmed {
					t.Errorf("校验后就地 trim 未生效: cfg.URLRedirect[0].Template = %q，期望 %q",
						cfg.URLRedirect[0].Template, tt.wantTrimmed)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() 期望报错（含 %q），实际为 nil（模板 %q）", tt.wantErr, tt.tpl)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("错误信息 %q 应包含 %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestValidateRedirectTemplatesErrorListsAllPlaceholders 验证「无占位符」的新错误文案
// 列出全部合法占位符写法：RedirectPlaceholders() 每个变量的 $name 与 ${name} 两种形式都出现。
func TestValidateRedirectTemplatesErrorListsAllPlaceholders(t *testing.T) {
	cfg := Default()
	cfg.URLRedirect = []URLRedirectItem{{Template: "https://mirror.example.com/raw/foo/bar"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() 期望报错（模板无占位符），实际为 nil")
	}
	for _, name := range RedirectPlaceholders() {
		for _, form := range []string{"$" + name, "${" + name + "}"} {
			if !strings.Contains(err.Error(), form) {
				t.Errorf("错误信息应列出合法占位符写法 %q，实际: %s", form, err.Error())
			}
		}
	}
	// 直接对照文案生成函数，保证帮助信息与占位符注册表一致
	if want := placeholderHelp(); !strings.Contains(err.Error(), want) {
		t.Errorf("错误信息应包含 placeholderHelp() 全文 %q，实际: %s", want, err.Error())
	}
}

// TestValidateRedirectTemplatesRuleScope 验证 domain-rules 规则级 url-redirect
// 同样走占位符校验：无占位符报错且错误信息定位到规则与字段。
func TestValidateRedirectTemplatesRuleScope(t *testing.T) {
	cfg := Default()
	bad := []URLRedirectItem{{Template: "https://mirror.example.com/raw/foo/bar"}}
	cfg.DomainRules = []DomainRule{{
		Name:        "no-placeholder-rule",
		Match:       `http://github\.path\..+`,
		URLRedirect: &bad,
	}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() 期望报错（规则级模板无占位符），实际为 nil")
	}
	for _, want := range []string{"no-placeholder-rule", "url-redirect", "缺少占位符"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
		}
	}
}

// TestValidateRedirectStandalonePlaceholderGuidance 验证独立写 full_url /
// full_url_no_server 占位符的错误文案（bbf0b2c）：说明该占位符展开语义不是合法
// 上游地址，并指引改用 template: $1 / ${1} 表示原始目标 URL（子串按实现文案取）。
func TestValidateRedirectStandalonePlaceholderGuidance(t *testing.T) {
	dollar1Guidance := "template: $" + PlaceholderTarget
	tests := []struct {
		name  string
		tpl   string
		wants []string // 错误信息必须同时包含的子串
	}{
		{
			name: "独立full_url展开语义与指引",
			tpl:  "$" + PlaceholderFullURL,
			wants: []string{
				"必须以 http:// 或 https:// 开头",
				"含入站 Host",
				"不是合法上游地址",
				dollar1Guidance,
			},
		},
		{
			name: "大括号独立full_url展开语义与指引",
			tpl:  "${" + PlaceholderFullURL + "}",
			wants: []string{
				"必须以 http:// 或 https:// 开头",
				"含入站 Host",
				"不是合法上游地址",
				dollar1Guidance,
			},
		},
		{
			name: "独立full_url_no_server展开语义与指引",
			tpl:  "$" + PlaceholderFullURLNoServer,
			wants: []string{
				"必须以 http:// 或 https:// 开头",
				"/path?query",
				"不是合法上游地址",
				dollar1Guidance,
			},
		},
		{
			name: "大括号独立full_url_no_server展开语义与指引",
			tpl:  "${" + PlaceholderFullURLNoServer + "}",
			wants: []string{
				"必须以 http:// 或 https:// 开头",
				"/path?query",
				"不是合法上游地址",
				dollar1Guidance,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.URLRedirect = []URLRedirectItem{{Template: tt.tpl}}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() 期望报错（模板 %q），实际为 nil", tt.tpl)
			}
			for _, want := range tt.wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("错误信息 %q 缺少 %q", err.Error(), want)
				}
			}
		})
	}
}

// TestValidateRedirectRawURLCandidateRuleScope 验证 domain-rules 规则级
// url-redirect 同样放行 $1 原始 URL 独立候选（bbf0b2c）：规则 Validate 通过。
func TestValidateRedirectRawURLCandidateRuleScope(t *testing.T) {
	cfg := Default()
	good := []URLRedirectItem{{Template: "$" + PlaceholderTarget}}
	cfg.DomainRules = []DomainRule{{
		Name:        "raw-url-candidate-rule",
		Match:       `http://github\.path\..+`,
		URLRedirect: &good,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	if got := (*cfg.DomainRules[0].URLRedirect)[0].Template; got != "$"+PlaceholderTarget {
		t.Errorf("规则级候选 template = %q，期望 %q", got, "$"+PlaceholderTarget)
	}
}
