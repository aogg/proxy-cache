package proxy

// upstream_status_test.go —— 候选全败时「上游明确 4xx 透传」（upstream_status.go，提交 7627c61）的单测：
// 全部使用 httptest 假上游（不启动本服务、不发真实外网请求），经 Server.ServeHTTP 走完整链路
//（与 registry_test.go 同风格，缓存一律显式关闭避免缓存写入干扰断言），不含任何 e2e。
// 覆盖点（A 系列）：
//   1. 路径目标候选全败且上游 404（模拟现场动机：镜像站对 /v2/<name>/referrers/<digest>
//      回 404，docker 客户端本可容忍）→ 客户端收 404（含上游 body / 可透传响应头 +
//      X-Proxy-Upstream 观测头），而非一律 502；
//   2. 405（未知端点）/ 429（限流）同理透传（含 Retry-After / Allow 等可透传头）；
//   3. 401 不透传：匿名 token dance 后仍 401 属硬失败 → 仍 502（既有语义不变）；
//   4. 网络类失败（连接拒绝 / 候选探测预算超时）不产生载荷 → 仍 502；
//   5. 换候选重试语义不变：候选 A 回 404、候选 B 回 200 → 客户端正常拿 200（A 的 404
//      载荷只在全部候选失败后才上浮，不干扰轮询）；
//   6. HEAD 的 404 透传：无 body、Content-Length 沿用上游声明值；
//   7. 完整 URL（web）目标全败不受影响：不透传 4xx，仍 502；
//   8. 白盒：newUpstreamStatusError 载荷边界（399/400/401/499/500、nil 防御）、
//      asUpstreamStatusError 经多层 %w 包装穿透、stripUpstreamStatus 剥离与原样返回。

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/config"
)

// referrersName / referrersDigest 模拟现场报错的 referrers 请求目标
// （registry-proxy-cache.linkease.net:5480/foxhui/webai-2api 的 OCI referrers 端点）。
var (
	referrersName   = "foxhui/webai-2api"
	referrersDigest = "sha256:" + strings.Repeat("ab", 32)
)

// referrersTarget 返回 referrers 端点的路径目标原文（ExtractTarget 输出形态：不带 scheme 与前导 /）。
func referrersTarget() string {
	return "v2/" + referrersName + "/referrers/" + referrersDigest
}

// referrersRequest 构造指向 referrers 端点的入站 GET 请求（Host 与既有 registry 测试一致）。
func referrersRequest() *http.Request {
	return httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+referrersTarget(), nil)
}

// statusUpstream 启动一个恒返回指定状态码 + body + 附加响应头的假上游（判据必拒：
// 非 200/304 在 content/status 判据下都不成功），模拟镜像站对 referrers 端点的明确 4xx。
func statusUpstream(t *testing.T, status int, body string, headers map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestUpstreamStatus4xxPassthrough 表驱动验证候选全败时上游明确 4xx 的透传语义（覆盖点 1/2）：
//   - 404：现场动机场景（referrers 端点），docker 客户端对该语义自带 fallback；
//   - 405：未知端点（Allow 头应随响应透传）；
//   - 429：限流（Retry-After 头应随响应透传）。
//
// 全部断言：客户端状态码 = 上游状态码、body 原样透传（探测量级）、可透传响应头保留、
// X-Proxy-Upstream 标明产生该响应的候选 URL、响应不带 X-Cache（走透传出口而非缓存回放）、
// body 不是 JSON 502 错误体（与 writeError 的旧行为区分）。
func TestUpstreamStatus4xxPassthrough(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		headers map[string]string
	}{
		{
			name:   "404-referrers端点透传（现场动机）",
			status: http.StatusNotFound,
			body:   `{"errors":[{"code":"NAME_UNKNOWN","message":"repository name not known to registry"}]}`,
			headers: map[string]string{
				"Content-Type":                    "application/json",
				"Docker-Distribution-Api-Version": "registry/2.0",
			},
		},
		{
			name:   "405-未知端点透传",
			status: http.StatusMethodNotAllowed,
			body:   "method not allowed",
			headers: map[string]string{
				"Content-Type": "text/plain; charset=utf-8",
				"Allow":        "GET, HEAD",
			},
		},
		{
			name:   "429-限流透传",
			status: http.StatusTooManyRequests,
			body:   "too many requests",
			headers: map[string]string{
				"Content-Type": "text/plain; charset=utf-8",
				"Retry-After":  "30",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := statusUpstream(t, tt.status, tt.body, tt.headers)
			s := newRegistryServer(t,
				`http://registry-proxy-cache\.linkease\.net:5480/`,
				"", // 缓存关闭：聚焦透传出口，避免缓存写入干扰
				[]config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}},
				nil)

			w := httptest.NewRecorder()
			s.ServeHTTP(w, referrersRequest())

			if w.Code != tt.status {
				t.Fatalf("Code = %d, want 透传上游 %d（body: %s）", w.Code, tt.status, w.Body.String())
			}
			if got := w.Body.String(); got != tt.body {
				t.Errorf("Body = %q, want 原样透传上游 body %q", got, tt.body)
			}
			for k, v := range tt.headers {
				if got := w.Header().Get(k); got != v {
					t.Errorf("响应头 %s = %q, want 透传上游声明值 %q", k, got, v)
				}
			}
			wantUpstream := upstream.URL + "/" + referrersTarget()
			if got := w.Header().Get("X-Proxy-Upstream"); got != wantUpstream {
				t.Errorf("X-Proxy-Upstream = %q, want 产生该响应的候选 %q", got, wantUpstream)
			}
			if got := w.Header().Get("X-Cache"); got != "" {
				t.Errorf("X-Cache = %q, want 不设置（4xx 透传出口非缓存回放）", got)
			}
			if strings.Contains(w.Body.String(), `"error"`) {
				t.Errorf("body %q 不应是 JSON 502 错误体（旧行为的一律 502 未修正）", w.Body.String())
			}
		})
	}
}

// TestUpstreamStatus401NotPassthrough 覆盖点 3：401 不透传——匿名 token dance 后仍 401
// 属硬失败（dance-still-401），候选失败错误不装 4xx 载荷（newUpstreamStatusError 排除 401），
// 全败后维持既有 502 行为；镜像站收到 401 与带 token 重试共 2 次请求，token 端点 1 次。
func TestUpstreamStatus401NotPassthrough(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, true) // alwaysUnauthorized：dance 后仍 401

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{{Template: mirror.URL + "/$http.server.header.full_url_no_server"}},
		nil)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, referrersRequest())

	if w.Code != http.StatusBadGateway {
		t.Fatalf("Code = %d, want %d（401 dance 后仍 401 维持既有 502，不透传）, body: %s",
			w.Code, http.StatusBadGateway, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"error"`) {
		t.Errorf("body = %q, want JSON 502 错误体", w.Body.String())
	}
	if w.Header().Get("X-Proxy-Upstream") != "" {
		t.Errorf("X-Proxy-Upstream = %q, want 不设置（401 无透传载荷）", w.Header().Get("X-Proxy-Upstream"))
	}
	if n := rec.mirrorCalls(); n != 2 {
		t.Errorf("镜像站收到 %d 次请求, want 2（401 探测 + dance 带 token 重试）", n)
	}
	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点收到 %d 次请求, want 1", n)
	}
}

// TestUpstreamStatusNetworkFailureStill502 覆盖点 4：网络类失败不产生 4xx 载荷，仍 502。
//   - 连接拒绝：候选上游已关闭（端口无人监听），候选全败为纯网络失败；
//   - 探测预算超时：候选 probe-timeout=50ms、上游 400ms 后才回响应头 → probe-timeout
//     硬失败快切（错误信息含 probe-timeout），同样无载荷。
func TestUpstreamStatusNetworkFailureStill502(t *testing.T) {
	t.Run("连接拒绝", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		deadURL := dead.URL
		dead.Close() // 关闭监听：对 deadURL 的请求必然连接拒绝

		s := newRegistryServer(t,
			`http://registry-proxy-cache\.linkease\.net:5480/`,
			"",
			[]config.URLRedirectItem{{Template: deadURL + "/$http.server.header.full_url_no_server"}},
			nil)

		w := httptest.NewRecorder()
		s.ServeHTTP(w, referrersRequest())

		if w.Code != http.StatusBadGateway {
			t.Fatalf("Code = %d, want %d（网络类失败仍 502）, body: %s", w.Code, http.StatusBadGateway, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"error"`) {
			t.Errorf("body = %q, want JSON 502 错误体", w.Body.String())
		}
	})

	t.Run("候选探测预算超时", func(t *testing.T) {
		slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(400 * time.Millisecond) // 远超下方 50ms 探测预算：响应头前必被取消
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "too late")
		}))
		defer slow.Close()

		pt := config.Duration(50 * time.Millisecond)
		s := newRegistryServer(t,
			`http://registry-proxy-cache\.linkease\.net:5480/`,
			"",
			[]config.URLRedirectItem{{
				Template:     slow.URL + "/$http.server.header.full_url_no_server",
				ProbeTimeout: &pt,
			}},
			nil)

		w := httptest.NewRecorder()
		s.ServeHTTP(w, referrersRequest())

		if w.Code != http.StatusBadGateway {
			t.Fatalf("Code = %d, want %d（probe-timeout 属超时类失败，仍 502）, body: %s",
				w.Code, http.StatusBadGateway, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "too late") {
			t.Error("上游迟到响应不应透传给客户端（探测预算到期已放弃该候选）")
		}
	})
}

// TestUpstreamStatusCandidateRetryUnaffected 覆盖点 5：换候选重试语义不变。
// 候选 A（起始下标 0，全新 Server 首次请求）回 404（装载荷但不中断轮询，不原地重试）、
// 候选 B 回 200 → 候选 B 胜出，客户端正常拿 200；A 的 404 载荷只在全部候选失败后才上浮。
func TestUpstreamStatusCandidateRetryUnaffected(t *testing.T) {
	a := &countedUpstream{status: http.StatusNotFound, body: "refs not found here"}
	srvA := httptest.NewServer(a)
	defer srvA.Close()
	b := &countedUpstream{status: http.StatusOK, body: "refs-ok"}
	srvB := httptest.NewServer(b)
	defer srvB.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{
			{Template: srvA.URL + "/$http.server.header.full_url_no_server"},
			{Template: srvB.URL + "/$http.server.header.full_url_no_server"},
		},
		nil)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, referrersRequest())

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（候选 A 的 404 不应干扰轮询）, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Body.String(); got != "refs-ok" {
		t.Errorf("Body = %q, want 来自候选 B 的 %q", got, "refs-ok")
	}
	wantUpstream := srvB.URL + "/" + referrersTarget()
	if got := w.Header().Get("X-Proxy-Upstream"); got != wantUpstream {
		t.Errorf("X-Proxy-Upstream = %q, want 胜出候选 B %q", got, wantUpstream)
	}
	if got := w.Header().Get("X-Cache"); got != "BYPASS" {
		t.Errorf("X-Cache = %q, want BYPASS（候选回源成功路径）", got)
	}
	if n := a.calls(); n != 1 {
		t.Errorf("候选 A 收到 %d 次请求, want 1（404 装载荷后仍快切下一候选，不原地重试）", n)
	}
	if n := b.calls(); n != 1 {
		t.Errorf("候选 B 收到 %d 次请求, want 1", n)
	}
}

// countedUpstream 带请求计数的假上游（状态码 + body 可配，供轮询次数断言）。
type countedUpstream struct {
	mu     sync.Mutex
	hits   int
	status int
	body   string
}

func (u *countedUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.hits++
	status, body := u.status, u.body
	u.mu.Unlock()
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func (u *countedUpstream) calls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

// TestUpstreamStatusHeadPassthrough 覆盖点 6：HEAD 的 404 透传——客户端无 body，
// Content-Length 沿用上游声明值（描述 GET 语义的完整长度），X-Proxy-Upstream 照常输出。
func TestUpstreamStatusHeadPassthrough(t *testing.T) {
	const upstreamCL = "57" // 上游为 HEAD 声明的 Content-Length（GET 语义长度）
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", upstreamCL)
		w.WriteHeader(http.StatusNotFound)
		if r.Method != http.MethodHead {
			_, _ = io.WriteString(w, `{"errors":[{"code":"NAME_UNKNOWN"}]}`) // HEAD 天然无 body
		}
	}))
	defer upstream.Close()

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"",
		[]config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}},
		nil)

	r := httptest.NewRequest(http.MethodHead, "http://"+hostDockerRegistry+"/"+referrersTarget(), nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("Code = %d, want 透传上游 404", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Errorf("HEAD 透传响应不应有 body, got %d 字节", w.Body.Len())
	}
	if got := w.Header().Get("Content-Length"); got != upstreamCL {
		t.Errorf("Content-Length = %q, want 沿用上游声明值 %q", got, upstreamCL)
	}
	wantUpstream := upstream.URL + "/" + referrersTarget()
	if got := w.Header().Get("X-Proxy-Upstream"); got != wantUpstream {
		t.Errorf("X-Proxy-Upstream = %q, want %q", got, wantUpstream)
	}
}

// TestUpstreamStatusFullURLTargetStill502 覆盖点 7：完整 URL（web）目标全败不受影响。
// 候选全败且最后失败为上游 404（含载荷）时，web 目标不透传 4xx（仅流式路径目标适用），
// 维持既有 502 行为（fallback-direct 显式关闭使结果确定为 502）。
func TestUpstreamStatusFullURLTargetStill502(t *testing.T) {
	no := false
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.FallbackDirect = &no // 显式关闭直连回退：候选全败必然 502
	upstream := statusUpstream(t, http.StatusNotFound, "not found", map[string]string{"Content-Type": "text/plain"})
	cfg.URLRedirect = []config.URLRedirectItem{{Template: upstream.URL + "/$1"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:  "web-4xx",
		Match: `http://web-proxy\.local/`,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}

	fullTarget := "https://web.example.com/pkg/file.yaml"
	r := httptest.NewRequest(http.MethodGet, "http://web-proxy.local/"+fullTarget, nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("Code = %d, want %d（完整 URL 目标全败仍 502，不透传上游 404）, body: %s",
			w.Code, http.StatusBadGateway, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "not found") {
		t.Errorf("body %q 不应包含上游 404 body（web 目标不透传 4xx 载荷）", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"error"`) {
		t.Errorf("body = %q, want JSON 502 错误体", w.Body.String())
	}
}

// TestNewUpstreamStatusErrorBoundaries 覆盖点 8（白盒）：newUpstreamStatusError 的载荷边界——
// nil 结果 / nil 原因防御、状态码 <400 或 >=500 不装载荷、401 排除（dance 语义）、
// [400,500) 且 ≠401 装载荷且字段完整（header/body/url/err）。
func TestNewUpstreamStatusErrorBoundaries(t *testing.T) {
	mkRes := func(status int) *probeResult {
		h := http.Header{}
		h.Set("X-Trace", "t1")
		h.Set("Content-Length", "3")
		return &probeResult{status: status, header: h, first: []byte("abc")}
	}
	cause := errors.New("候选未通过 success-check=content")
	tests := []struct {
		name      string
		status    int
		wantNil   bool
		wantStrip bool // 装载荷时同时校验字段
	}{
		{"399不产生载荷", 399, true, false},
		{"400产生载荷", 400, false, true},
		{"401排除（dance语义）", 401, true, false},
		{"404产生载荷", 404, false, true},
		{"429产生载荷", 429, false, true},
		{"499产生载荷", 499, false, true},
		{"500不产生载荷", 500, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			use := newUpstreamStatusError(mkRes(tt.status), "http://cand/v2/x", cause)
			if tt.wantNil {
				if use != nil {
					t.Fatalf("status=%d 应不产生载荷, got %+v", tt.status, use)
				}
				return
			}
			if use == nil {
				t.Fatalf("status=%d 应产生载荷, got nil", tt.status)
			}
			if use.status != tt.status {
				t.Errorf("status = %d, want %d", use.status, tt.status)
			}
			if use.url != "http://cand/v2/x" {
				t.Errorf("url = %q, want 候选 URL 原样", use.url)
			}
			if string(use.body) != "abc" {
				t.Errorf("body = %q, want 探测首块 %q", use.body, "abc")
			}
			if use.header.Get("X-Trace") != "t1" {
				t.Errorf("header 应保留上游响应头（深拷贝）, got %v", use.header)
			}
			if use.err != cause || use.Error() != cause.Error() {
				t.Errorf("err/Error() 应为原始失败描述（日志文案不变）, got %v", use.err)
			}
			if use.Unwrap() != cause {
				t.Error("Unwrap 应暴露原始失败错误（errors.Is/As 穿透）")
			}
		})
	}

	t.Run("nil防御", func(t *testing.T) {
		if got := newUpstreamStatusError(nil, "http://cand", cause); got != nil {
			t.Errorf("nil 探测结果应返回 nil, got %+v", got)
		}
		if got := newUpstreamStatusError(mkRes(404), "http://cand", nil); got != nil {
			t.Errorf("nil 原因应返回 nil, got %+v", got)
		}
	})
}

// TestUpstreamStatusErrorChainHelpers 覆盖点 8（白盒）：asUpstreamStatusError 经多层
// %w 包装穿透（与流式出口的「全部 url-redirect 候选失败…: %w」包装一致）、非载荷错误
// 返回 false；stripUpstreamStatus 剥离载荷留裸错误、非载荷原样返回。
func TestUpstreamStatusErrorChainHelpers(t *testing.T) {
	cause := errors.New("候选未通过 success-check=content")
	payload := newUpstreamStatusError(&probeResult{status: http.StatusNotFound, header: http.Header{}}, "http://cand", cause)
	if payload == nil {
		t.Fatal("404 应产生载荷")
	}

	// 直接提取
	if use, ok := asUpstreamStatusError(payload); !ok || use != payload {
		t.Fatalf("载荷错误应可直接提取, got (%v, %v)", use, ok)
	}

	// 多层 %w 包装穿透（模拟 streaming 出口的既有包装链）
	inner := fmt.Errorf("路径目标无直连回退: %w", payload)
	wrapped := fmt.Errorf("全部 url-redirect 候选失败: %w", inner)
	if use, ok := asUpstreamStatusError(wrapped); !ok || use != payload {
		t.Fatalf("多层 %%w 包装后应仍能提取载荷, got (%v, %v)", use, ok)
	}
	if !errors.Is(wrapped, cause) {
		t.Error("包装链应保持 errors.Is 穿透到原始失败错误")
	}

	// 非载荷错误
	if _, ok := asUpstreamStatusError(errors.New("普通网络错误")); ok {
		t.Error("普通错误不应提取出载荷")
	}
	if _, ok := asUpstreamStatusError(nil); ok {
		t.Error("nil 错误不应提取出载荷")
	}

	// stripUpstreamStatus：剥离载荷留裸错误；非载荷原样返回
	stripped := stripUpstreamStatus(wrapped)
	if stripped != cause {
		t.Errorf("stripUpstreamStatus 应剥离载荷留裸错误, got %v, want %v", stripped, cause)
	}
	plain := errors.New("普通网络错误")
	if got := stripUpstreamStatus(plain); got != plain {
		t.Errorf("非载荷错误应原样返回, got %v", got)
	}
}
