// timeout_retry_test.go —— read-timeout 看门狗（有进度不超时/无进度超时）、
// http-timeout 生效、connect-timeout 传递到 Transport、item/all 两级重试计数
// 与 token dance 全程看门狗覆盖的单元测试（全部基于 httptest，无 e2e）。

package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/config"
)

// effFor 组装候选拉取用的生效配置（content 判据 + 指定候选对象 + 可选超时/重试）。
func effFor(templates []config.URLRedirectItem, mutate func(o *config.Options)) config.Options {
	o := config.Options{
		URLRedirect:    templates,
		SuccessCheck:   config.SuccessCheckContent,
		FallbackDirect: false,
	}
	if mutate != nil {
		mutate(&o)
	}
	return o
}

// TestProgressWatchdogUnit 看门狗纯单元行为：无进度触发、有进度不触发、Stop 取消、错误标注。
func TestProgressWatchdogUnit(t *testing.T) {
	// 无任何进度：窗口到期必须触发并取消 ctx
	wd := newProgressWatchdog(context.Background(), 80*time.Millisecond)
	defer wd.Stop()
	time.Sleep(200 * time.Millisecond)
	if !wd.Fired() {
		t.Fatal("窗口内无进度，看门狗应触发")
	}
	if wd.ctx.Err() != context.Canceled {
		t.Errorf("触发后 ctx 应为 Canceled，得到 %v", wd.ctx.Err())
	}
	if got := wd.wrapErr(fmt.Errorf("boom")); !strings.Contains(got.Error(), "read-timeout") {
		t.Errorf("触发后 wrapErr 应含 read-timeout，得到 %v", got)
	}

	// 有进度（每 50ms 收到 1 字节，累计超过 100ms 窗口数倍）：不触发
	wd2 := newProgressWatchdog(context.Background(), 100*time.Millisecond)
	defer wd2.Stop()
	for i := 0; i < 6; i++ {
		time.Sleep(50 * time.Millisecond)
		wd2.addBytes(1)
	}
	if wd2.Fired() {
		t.Fatal("持续有进度时看门狗不应触发")
	}
	if wd2.bytes.Load() != 6 {
		t.Errorf("累计字节数应为 6，得到 %d", wd2.bytes.Load())
	}
	if got := wd2.wrapErr(fmt.Errorf("boom")); got.Error() != "boom" {
		t.Errorf("未触发时 wrapErr 应原样返回，得到 %v", got)
	}
	if wd2.wrapErr(nil) != nil {
		t.Error("未触发且 err 为 nil 时 wrapErr 应返回 nil")
	}

	// 未启用（timeout<=0）：Fired 恒 false，ctx 不被取消
	wd3 := newProgressWatchdog(context.Background(), 0)
	defer wd3.Stop()
	time.Sleep(30 * time.Millisecond)
	if wd3.Fired() || wd3.ctx.Err() != nil {
		t.Error("timeout=0 表示不限制，看门狗不应触发")
	}

	// Stop 后 ctx 立即取消（防计时器泄漏）
	wd4 := newProgressWatchdog(context.Background(), time.Second)
	wd4.Stop()
	if wd4.ctx.Err() != context.Canceled {
		t.Error("Stop 后 ctx 应被取消")
	}
}

// TestDoRequestReadTimeoutNoProgress 响应头 + 首字节之后上游彻底静默：
// read-timeout 到期触发取消，错误信息含 "read-timeout"。
func TestDoRequestReadTimeoutNoProgress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "A")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 之后不再发送任何数据，挂住直到客户端放弃
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "https://upstream.example.com/blob", nil)
	start := time.Now()
	_, err := s.doRequest(req, http.MethodGet, srv.URL, nil,
		effFor(nil, func(o *config.Options) { o.ReadTimeout = 200 * time.Millisecond }))
	if err == nil {
		t.Fatal("上游静默应触发 read-timeout 失败")
	}
	if !strings.Contains(err.Error(), "read-timeout") {
		t.Errorf("错误信息应含 read-timeout，得到 %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("read-timeout 应在窗口附近触发，实际耗时 %s", elapsed)
	}
}

// TestDoRequestReadTimeoutSlowButSteady 慢速但持续传输（每 40ms 到达 1 字节）：
// 窗口 300ms 内始终有新数据，不允许被 read-timeout 砍掉。
func TestDoRequestReadTimeoutSlowButSteady(t *testing.T) {
	const total = 15
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		for i := 0; i < total; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(40 * time.Millisecond):
			}
			_, _ = io.WriteString(w, "x")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "https://upstream.example.com/blob", nil)
	e, err := s.doRequest(req, http.MethodGet, srv.URL, nil,
		effFor(nil, func(o *config.Options) { o.ReadTimeout = 300 * time.Millisecond }))
	if err != nil {
		t.Fatalf("慢速但持续传输不应超时: %v", err)
	}
	if string(e.Body) != strings.Repeat("x", total) {
		t.Errorf("body 应完整读到 %d 字节，得到 %d 字节", total, len(e.Body))
	}
}

// TestDoRequestHTTPTimeout 单次尝试总超时生效：整体拖太久（而非无数据）时由
// http-timeout 取消，错误链为 DeadlineExceeded 且不含 read-timeout。
func TestDoRequestHTTPTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "https://upstream.example.com/slow", nil)
	start := time.Now()
	_, err := s.doRequest(req, http.MethodGet, srv.URL, nil,
		effFor(nil, func(o *config.Options) { o.HTTPTimeout = 150 * time.Millisecond }))
	if err == nil {
		t.Fatal("超过 http-timeout 应失败")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("错误链应含 DeadlineExceeded，得到 %v", err)
	}
	if strings.Contains(err.Error(), "read-timeout") {
		t.Errorf("http-timeout 触发不应被标注为 read-timeout，得到 %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("应在 http-timeout 处中断，实际耗时 %s", elapsed)
	}
}

// newTimeoutTestServer 构造无缓存、丢弃日志的 Server（默认配置：全部超时不限制）。
func newTimeoutTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("默认配置校验失败: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("构造 Server 失败: %v", err)
	}
	return s
}

// TestNewUpstreamTransportConnectTimeout connect-timeout 必须落到拨号器与 TLS 握手超时；
// 0 = 不限制；代理地址非法时报错。
func TestNewUpstreamTransportConnectTimeout(t *testing.T) {
	if d := newUpstreamDialer(1500 * time.Millisecond); d.Timeout != 1500*time.Millisecond {
		t.Errorf("拨号器 Timeout 应为 1.5s，得到 %s", d.Timeout)
	}
	if d := newUpstreamDialer(0); d.Timeout != 0 {
		t.Errorf("connect-timeout=0 时拨号器应不限制（0），得到 %s", d.Timeout)
	}
	tr, err := newUpstreamTransport("", 1500*time.Millisecond, false)
	if err != nil {
		t.Fatalf("构建 Transport 失败: %v", err)
	}
	if tr.TLSHandshakeTimeout != 1500*time.Millisecond {
		t.Errorf("TLS 握手超时应为 1.5s，得到 %s", tr.TLSHandshakeTimeout)
	}
	if tr.Proxy != nil {
		t.Error("直连 Transport 不应配置代理")
	}
	tr2, err := newUpstreamTransport("http://127.0.0.1:7890", time.Second, false)
	if err != nil {
		t.Fatalf("带代理构建 Transport 失败: %v", err)
	}
	if tr2.Proxy == nil {
		t.Error("http-proxy 应体现在 Transport.Proxy")
	}
	if _, err := newUpstreamTransport("ftp://example.com", 0, false); err == nil {
		t.Error("不支持的代理协议应报错")
	}
}

// TestClientCacheByConnectTimeout 相同 (代理, connect-timeout) 组合复用同一 Client
// （连接复用不被破坏）；不同 connect-timeout 各一个 Client。
func TestClientCacheByConnectTimeout(t *testing.T) {
	ct := config.Duration(2 * time.Second)
	cfg := config.Default()
	cfg.ConnectTimeout = &ct
	if err := cfg.Validate(); err != nil {
		t.Fatalf("配置校验失败: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("构造 Server 失败: %v", err)
	}
	pre := s.clients[upstreamClientKey{proxy: "", connectTimeout: 2 * time.Second}]
	if pre == nil {
		t.Fatal("New 应预构建 (直连, 全局 connect-timeout) 组合")
	}
	if got := s.client("", 2*time.Second, false); got != pre {
		t.Error("相同组合应复用预构建 Client")
	}
	if got := s.client("", 2*time.Second, false); got != pre {
		t.Error("相同组合重复获取应复用同一 Client")
	}
	other := s.client("", 4*time.Second, false)
	if other == nil || other == pre {
		t.Error("不同 connect-timeout 应得到不同 Client")
	}
	if other.Transport.(*http.Transport).TLSHandshakeTimeout != 4*time.Second {
		t.Errorf("新组合 Client 的 TLS 握手超时应为 4s，得到 %s",
			other.Transport.(*http.Transport).TLSHandshakeTimeout)
	}
}

// TestFetchViaCandidatesItemRetry 单候选失败后原地重试：总尝试 = 1+N，
// 前面失败、末次成功即整体成功。
func TestFetchViaCandidatesItemRetry(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= 2 { // 前两次未过 success-check（500）
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "bad")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/a", nil)
	target := "https://example.com/a"
	eff := effFor([]config.URLRedirectItem{{Template: srv.URL + "/$1"}}, func(o *config.Options) {
		o.URLRedirectItemErrorRetry = 3 // 总尝试 = 4，第 3 次命中
	})
	e, err := s.fetchViaCandidates(req, target, eff)
	if err != nil {
		t.Fatalf("item 重试内命中应成功: %v", err)
	}
	if got := hits.Load(); got != 3 {
		t.Errorf("总尝试次数应为 3（1+2 次失败），得到 %d", got)
	}
	if string(e.Body) != "hello" || e.Source != srv.URL+"/"+target {
		t.Errorf("命中 Entry 不正确：body=%q source=%q", e.Body, e.Source)
	}
}

// TestFetchViaCandidatesItemRetryCoversNetworkError 网络错误（连接被掐断）同样触发 item 重试。
func TestFetchViaCandidatesItemRetryCoversNetworkError(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			// 首次直接掐断连接：客户端得到网络层错误（而非可判定的状态码）
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("httptest 响应应支持 Hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Fatalf("劫持连接失败: %v", err)
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/a", nil)
	target := "https://example.com/a"
	eff := effFor([]config.URLRedirectItem{{Template: srv.URL + "/$1"}}, func(o *config.Options) {
		o.URLRedirectItemErrorRetry = 1
	})
	e, err := s.fetchViaCandidates(req, target, eff)
	if err != nil {
		t.Fatalf("网络错误后的重试应成功: %v", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("总尝试次数应为 2，得到 %d", got)
	}
	if string(e.Body) != "ok" {
		t.Errorf("body 应为 ok，得到 %q", e.Body)
	}
}

// TestFetchViaCandidatesAllRetryRounds 整轮全败后按 url-redirect-all-error-retry
// 整体重来：总轮数 = 1+N，第二轮命中。
func TestFetchViaCandidatesAllRetryRounds(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 { // 第一轮唯一候选失败
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "down")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "round2")
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/a", nil)
	target := "https://example.com/a"
	eff := effFor([]config.URLRedirectItem{{Template: srv.URL + "/$1"}}, func(o *config.Options) {
		o.URLRedirectAllErrorRetry = 1
	})
	e, err := s.fetchViaCandidates(req, target, eff)
	if err != nil {
		t.Fatalf("第二轮命中应成功: %v", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("总请求数应为 2（两轮各一次），得到 %d", got)
	}
	if string(e.Body) != "round2" {
		t.Errorf("body 应为 round2，得到 %q", e.Body)
	}
}

// TestFetchViaCandidatesRetryOrderAndExhaustion item×round 全失败的尝试矩阵与顺序：
// 2 候选 × (1+1) item × (1+1) round = 8 次尝试；同一请求内重试轮沿用同一候选顺序；
// 全部失败后按 fallback-direct=false 直接报错。
func TestFetchViaCandidatesRetryOrderAndExhaustion(t *testing.T) {
	var mu = make(chan string, 64) // 记录请求顺序（候选标识）
	var total atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		total.Add(1)
		idx := "c0"
		if strings.HasPrefix(r.URL.Path, "/c1/") {
			idx = "c1"
		}
		select {
		case mu <- idx:
		default:
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "always-down")
	}))
	defer srv.Close()

	s := newTimeoutTestServer(t) // 全新 Server：rr 从 0 开始，start=0
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/a", nil)
	target := "https://example.com/a"
	eff := effFor([]config.URLRedirectItem{{Template: srv.URL + "/c0/$1"}, {Template: srv.URL + "/c1/$1"}}, func(o *config.Options) {
		o.URLRedirectItemErrorRetry = 1
		o.URLRedirectAllErrorRetry = 1
	})
	_, err := s.fetchViaCandidates(req, target, eff)
	if err == nil {
		t.Fatal("所有尝试都失败应报错")
	}
	if !strings.Contains(err.Error(), "全部 url-redirect 候选失败") {
		t.Errorf("错误应说明候选全败，得到 %v", err)
	}
	if got := total.Load(); got != 8 {
		t.Errorf("总尝试次数应为 2候选×2次×2轮=8，得到 %d", got)
	}
	close(mu)
	var order []string
	for v := range mu {
		order = append(order, v)
	}
	want := []string{"c0", "c0", "c1", "c1", "c0", "c0", "c1", "c1"}
	if len(order) != len(want) {
		t.Fatalf("请求顺序记录不完整：得到 %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("尝试顺序不符（重试轮应沿用同一候选顺序）：得到 %v, want %v", order, want)
		}
	}
}

// danceMirror 构造「首问 401 + Bearer 挑战，带 token 重试按脚本响应」的镜像站。
func danceMirror(t *testing.T, tokenURL string, retryBody func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Bearer realm=%q", tokenURL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		retryBody(w, r)
	}))
}

// newTokenServer 构造匿名 token 签发端点（返回固定 token）。
func newTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "tok-1", "expires_in": 300})
	}))
}

// TestTokenDanceReadTimeoutMarked 复现线上案例：token dance 重试已返回 200，
// 但读 body 阶段上游静默——必须以 read-timeout 语义失败（日志可辨认），
// 该候选按未过判据处理并正常进入既有失败路径。
func TestTokenDanceReadTimeoutMarked(t *testing.T) {
	tokenSrv := newTokenServer(t)
	defer tokenSrv.Close()
	mirror := danceMirror(t, tokenSrv.URL, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "x")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 重试响应发出首字节后彻底静默
	})
	defer mirror.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("默认配置校验失败: %v", err)
	}
	s, err := New(cfg, nil, logger)
	if err != nil {
		t.Fatalf("构造 Server 失败: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/library/a/manifests/latest", nil)
	target := "https://example.com/library/a/manifests/latest"
	eff := effFor([]config.URLRedirectItem{{Template: mirror.URL + "/$1"}}, func(o *config.Options) {
		o.ReadTimeout = 200 * time.Millisecond
	})
	_, err = s.fetchViaCandidates(req, target, eff)
	if err == nil {
		t.Fatal("dance 重试读 body 静默应导致候选失败")
	}
	if !strings.Contains(logBuf.String(), "read-timeout") {
		t.Errorf("日志应含 read-timeout 便于辨认，得到：%s", logBuf.String())
	}
}

// TestTokenDanceSlowButSteadySucceeds dance 重试响应慢速但持续传输：
// read-timeout 不应误砍，最终按 200 成功返回。
func TestTokenDanceSlowButSteadySucceeds(t *testing.T) {
	tokenSrv := newTokenServer(t)
	defer tokenSrv.Close()
	const total = 10
	mirror := danceMirror(t, tokenSrv.URL, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		for i := 0; i < total; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(40 * time.Millisecond):
			}
			_, _ = io.WriteString(w, "y")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})
	defer mirror.Close()

	s := newTimeoutTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/https://example.com/library/a/manifests/latest", nil)
	target := "https://example.com/library/a/manifests/latest"
	eff := effFor([]config.URLRedirectItem{{Template: mirror.URL + "/$1"}}, func(o *config.Options) {
		o.ReadTimeout = 300 * time.Millisecond
	})
	e, err := s.fetchViaCandidates(req, target, eff)
	if err != nil {
		t.Fatalf("dance 重试慢速但持续传输不应失败: %v", err)
	}
	if e.Status != http.StatusOK || string(e.Body) != strings.Repeat("y", total) {
		t.Errorf("应按 200 完整返回 %d 字节，得到 status=%d body=%d 字节", total, e.Status, len(e.Body))
	}
}
