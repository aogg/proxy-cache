package proxy

// vars_test.go —— url-redirect 模板变量（RequestVars / NewRequestVars / ExpandCandidates）
// 与目标提取（ExtractTarget）的单测：
//   - ExpandCandidates 表驱动：docker / github 两个规格示例逐字符断言（斜杠接缝去重，
//     不得出现双斜杠），$1 与 ${1} 兼容，${...} 大括号写法，同前缀占位符互不遮蔽，
//     未知 $ 序列原样保留，X-Forwarded-Proto / r.TLS 影响 full_url 的 scheme，
//     查询串拼接到两个变量，多模板保持顺序与个数；
//   - ExtractTarget 三类：完整 URL 目标 / 路径目标（不再报错）/ 空路径报错。

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 规格示例里的入站 Host 与目标（来自 docker / github 真实使用场景，仅用于构造请求，不出网）。
const (
	hostDockerRegistry = "registry-proxy-cache.linkease.net:5480"
	pathDockerManifest = "/v2/adockero/proxy-cache/manifests/latest"
	urlVlessYAMLFull   = "https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
)

// TestExpandCandidates 表驱动验证模板变量展开：
// 每行构造一个入站请求（url 含入站 Host 与路径，target 为 $1 的目标串），
// 断言每个模板展开后的候选 URL 与期望逐字符相等。
func TestExpandCandidates(t *testing.T) {
	tests := []struct {
		name      string   // 用例名
		method    string   // 入站方法（仅构造请求，不影响变量）
		url       string   // 入站请求完整 URL（scheme://host/path?query）
		xfp       string   // 可选 X-Forwarded-Proto 请求头（空串表示不设置）
		useTLS    bool     // 是否模拟 TLS 入站（影响 full_url 的 scheme）
		target    string   // $1 的目标串（完整 URL 原文或路径目标本身）
		templates []string // url-redirect 模板列表
		want      []string // 期望的候选 URL（与 templates 等长同序）
	}{
		{
			name:   "docker规格-full_url_no_server-斜杠接缝去重不得双斜杠",
			method: http.MethodGet,
			url:    "http://" + hostDockerRegistry + pathDockerManifest,
			target: "v2/adockero/proxy-cache/manifests/latest",
			templates: []string{
				"https://docker.1panel.live/$http.server.header.full_url_no_server",
			},
			want: []string{
				"https://docker.1panel.live/v2/adockero/proxy-cache/manifests/latest",
			},
		},
		{
			name:   "github规格-full_url_no_server-整段https目标原样拼接",
			method: http.MethodGet,
			url:    "http://github.path.proxy-cache.port.8080.proxy_sslip.192.168.137.2.sslip.io/" + urlVlessYAMLFull,
			target: urlVlessYAMLFull, // 完整 URL 目标 = 目标原文
			templates: []string{
				"https://hk.gh-proxy.org/$http.server.header.full_url_no_server",
			},
			want: []string{
				"https://hk.gh-proxy.org/" + urlVlessYAMLFull,
			},
		},
		{
			name:   "dollar1与大括号兼容-完整URL目标等于目标原文",
			method: http.MethodGet,
			url:    "http://gh.example.com/" + urlVlessYAMLFull,
			target: urlVlessYAMLFull,
			templates: []string{
				"https://gh-proxy.com/$1",
				"https://ghproxy.net/${1}",
			},
			want: []string{
				"https://gh-proxy.com/" + urlVlessYAMLFull,
				"https://ghproxy.net/" + urlVlessYAMLFull,
			},
		},
		{
			name:   "dollar1路径目标等于路径目标本身",
			method: http.MethodGet,
			url:    "http://" + hostDockerRegistry + pathDockerManifest,
			target: "v2/adockero/proxy-cache/manifests/latest",
			templates: []string{
				"https://docker.1panel.live/$1",
			},
			want: []string{
				"https://docker.1panel.live/v2/adockero/proxy-cache/manifests/latest",
			},
		},
		{
			name:   "full_url大括号写法-含入站scheme与Host",
			method: http.MethodGet,
			url:    "http://" + hostDockerRegistry + pathDockerManifest,
			target: "v2/adockero/proxy-cache/manifests/latest",
			templates: []string{
				"https://mirror.example.com/${http.server.header.full_url}",
			},
			want: []string{
				"https://mirror.example.com/http://" + hostDockerRegistry + pathDockerManifest,
			},
		},
		{
			name:   "前缀遮蔽-同串两个占位符各归位",
			method: http.MethodGet,
			url:    "http://reg.example.com/v2/x/manifests/latest",
			target: "v2/x/manifests/latest",
			templates: []string{
				"A=$http.server.header.full_url_no_server B=$http.server.header.full_url",
			},
			want: []string{
				"A=/v2/x/manifests/latest B=http://reg.example.com/v2/x/manifests/latest",
			},
		},
		{
			name:   "未知dollar序列原样保留",
			method: http.MethodGet,
			url:    "http://gh.example.com/" + urlVlessYAMLFull,
			target: urlVlessYAMLFull,
			templates: []string{
				"https://mirror.example.com/$unknown/$http.server.header.other/${nope}/$1",
			},
			want: []string{
				"https://mirror.example.com/$unknown/$http.server.header.other/${nope}/" + urlVlessYAMLFull,
			},
		},
		{
			name:   "XForwardedProto影响full_url的scheme-取首个值并小写",
			method: http.MethodGet,
			url:    "http://reg.example.com/v2/x/manifests/latest",
			xfp:    "HTTPS,http",
			target: "v2/x/manifests/latest",
			templates: []string{
				"$http.server.header.full_url",
			},
			want: []string{
				"https://reg.example.com/v2/x/manifests/latest",
			},
		},
		{
			name:   "无XForwardedProto时按TLS判定https",
			method: http.MethodGet,
			url:    "http://reg.example.com/v2/x/manifests/latest",
			useTLS: true,
			target: "v2/x/manifests/latest",
			templates: []string{
				"$http.server.header.full_url",
			},
			want: []string{
				"https://reg.example.com/v2/x/manifests/latest",
			},
		},
		{
			name:   "查询串拼接到两个变量",
			method: http.MethodGet,
			url:    "http://reg.example.com/v2/x/manifests/latest?service=registry&a=1",
			target: "v2/x/manifests/latest?service=registry&a=1",
			templates: []string{
				"https://docker.1panel.live/$http.server.header.full_url_no_server",
				"$http.server.header.full_url",
			},
			want: []string{
				"https://docker.1panel.live/v2/x/manifests/latest?service=registry&a=1",
				"http://reg.example.com/v2/x/manifests/latest?service=registry&a=1",
			},
		},
		{
			name:   "HEAD请求变量与GET一致",
			method: http.MethodHead,
			url:    "http://" + hostDockerRegistry + pathDockerManifest,
			target: "v2/adockero/proxy-cache/manifests/latest",
			templates: []string{
				"https://docker.1panel.live/$http.server.header.full_url_no_server",
			},
			want: []string{
				"https://docker.1panel.live/v2/adockero/proxy-cache/manifests/latest",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.url, nil)
			if tt.xfp != "" {
				r.Header.Set("X-Forwarded-Proto", tt.xfp)
			}
			if tt.useTLS {
				r.TLS = &tls.ConnectionState{} // 模拟 TLS 入站（影响 scheme 推断）
			}
			got := ExpandCandidates(tt.templates, NewRequestVars(r, tt.target))
			if len(got) != len(tt.want) {
				t.Fatalf("ExpandCandidates() 返回 %d 个候选, want %d（got: %v）", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("候选[%d] = %q, want 逐字符相等 %q\n模板: %q", i, got[i], tt.want[i], tt.templates[i])
				}
			}
		})
	}
}

// TestExpandCandidatesEmpty 验证空模板列表返回 nil（无候选）。
func TestExpandCandidatesEmpty(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://h.example.com/v2/x", nil)
	if got := ExpandCandidates(nil, NewRequestVars(r, "v2/x")); got != nil {
		t.Errorf("ExpandCandidates(nil, ...) = %v, want nil", got)
	}
}

// TestExtractTargetKinds 表驱动验证 ExtractTarget 的三返回值语义：
// 完整 http/https URL 目标 / 路径目标（fullURL=false，不再报错）/ 空路径报错。
func TestExtractTargetKinds(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		url        string // 入站请求 URL（路径部分即目标）
		wantTarget string
		wantFull   bool
		wantErr    string // 非空表示期望报错并包含该子串
	}{
		{
			name:       "完整URL目标",
			method:     http.MethodGet,
			url:        "http://gh.example.com/" + urlVlessYAMLFull,
			wantTarget: urlVlessYAMLFull,
			wantFull:   true,
		},
		{
			name:       "路径目标-docker-manifest",
			method:     http.MethodGet,
			url:        "http://" + hostDockerRegistry + pathDockerManifest,
			wantTarget: "v2/adockero/proxy-cache/manifests/latest",
			wantFull:   false,
		},
		{
			name:       "路径目标-非http协议也不算完整URL",
			method:     http.MethodGet,
			url:        "http://h.example.com/ftp://mirror.example.com/x.yaml",
			wantTarget: "ftp://mirror.example.com/x.yaml",
			wantFull:   false,
		},
		{
			name:    "空路径报错",
			method:  http.MethodGet,
			url:     "http://h.example.com/",
			wantErr: "缺少目标 URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.url, nil)
			target, fullURL, err := ExtractTarget(r)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ExtractTarget() 期望报错（包含 %q），实际为 nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("错误信息 %q 应包含 %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExtractTarget() 意外报错: %v", err)
			}
			if target != tt.wantTarget {
				t.Errorf("target = %q, want %q", target, tt.wantTarget)
			}
			if fullURL != tt.wantFull {
				t.Errorf("fullURL = %v, want %v", fullURL, tt.wantFull)
			}
		})
	}
}
