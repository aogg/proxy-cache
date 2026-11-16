// Package tests 是 proxy-cache 的黑盒集成级单元测试：
// 只经公开 API（proxy.New / Server.ServeHTTP / config.Config / cache 包）驱动，
// 全部上游用 httptest 本地假上游，不发真实外网请求、不起真实服务进程（无 e2e）。
//
// 本文件是共享夹具：配置构造（Validate 后注入 proxy.New）、httptest 上游构造器、
// 客户端请求驱动、半截文件种子、状态轮询与 goroutine 收敛检查。
//
// 覆盖主题（对应 .dsh/tmp/history-analysis.md 缺口清单）：
//   - 多源拼接三道内容级对账（reconcile_crosscheck_test.go）
//   - 续传扫描取消权移交 / 416 判完整矩阵（resume_scan_test.go）
//   - flight 等待上限超限路径（flight_wait_limit_test.go）
//   - 规则缺 type: docker 的门控静默失效族（docker_type_gating_test.go）
//   - 起点>0 Range 断开保留半截（range_resume_disconnect_test.go）
//   - token dance 按 realm 隔离（token_realm_isolation_test.go）
//   - gzip Content-Encoding 回放（gzip_replay_test.go）
//   - janitor 对异常文件容错（cache_janitor_tolerance_test.go）
//   - slowMerger pump 并发时序压力（merger_pump_race_test.go）
//   - tags 并集 / referrers 去重 / manifest 摘要共识（multisource_merge_test.go）
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
	"proxy-cache/proxy"
)

// testHost 测试用入站 Host：ServeHTTP 直接驱动，不做 DNS 解析。
const testHost = "registry.proxycache.test"

// testMatchRe 命中 testHost 全部请求的规则 match 正则（对 BuildMatchStr 整串非锚定匹配）。
const testMatchRe = `http://registry\.proxycache\.test/`

// ptrDuration / ptrBytes / ptrInt / ptrBool / ptrString 配置指针字段的小工具。
func ptrDuration(d time.Duration) *config.Duration { v := config.Duration(d); return &v }
func ptrBytes(n int64) *config.Bytes               { v := config.Bytes(n); return &v }
func ptrInt(n int) *int                            { return &n }
func ptrBool(b bool) *bool                         { return &b }
func ptrString(s string) *string                   { return &s }

// blobPattern 生成确定性测试数据：byte(i%251) 模式，任何错位/丢失/重复都可逐字节比对。
func blobPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// wrongPattern 生成与 blobPattern 逐字节不同的「错位载荷」：同长度、全部取反。
// 用于构造「206 头声明合法但载荷错位」的坏镜像源。
func wrongPattern(data []byte) []byte {
	b := make([]byte, len(data))
	for i := range b {
		b[i] = ^data[i]
	}
	return b
}

// sha256Hex 返回 b 的 sha256 hex 摘要。
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// digestOf 返回 "sha256:<hex>" 形态的 blob digest。
func digestOf(b []byte) string { return "sha256:" + sha256Hex(b) }

// blobTarget 构造 /v2/<image>/blobs/<digest> 路径目标（不含前导 /，ExtractTarget 输出形态）。
func blobTarget(image, digest string) string { return "v2/" + image + "/blobs/" + digest }

// newTestEnv 构造一套完整测试环境：Validate 后的配置 + cache.Manager + proxy.Server，
// 日志按 Debug 级捕获进 buffer（断言 Warn/Info/Debug 留痕）。
// mutate 在 Validate 前对配置做定制（通常设置规则 url-redirect 候选与特性开关）。
type testEnv struct {
	t      *testing.T
	srv    *proxy.Server
	cfg    *config.Config
	log    *bytes.Buffer
	dir    string
	caches *cache.Manager
}

func newTestEnv(t *testing.T, mutate func(cfg *config.Config, rule *config.DomainRule)) *testEnv {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		Cache: config.CacheConfig{
			Enabled: true,
			Path:    dir,
			TTL:     ptrDuration(time.Hour),
		},
		SuccessCheck: config.SuccessCheckContent,
		// 探测预算显式收紧（响应头 3s 内必须到达；body 流式不受它约束）
		URLRedirectProbeTimeout: ptrDuration(3 * time.Second),
		// 尝试总超时兜底：任何上游尝试 60s 必收敛（防单测悬挂）
		HTTPTimeout: ptrDuration(60 * time.Second),
		// 进度日志关闭：减少时间相关噪音，断言更确定
		Log: config.LogConfig{ProgressInterval: ptrDuration(0)},
		// 默认关掉两类后台特性，需要时在 mutate 里显式打开（避免异步噪音）
		DockerPrefetchEnabled:    ptrBool(false),
		DockerMultiSourceEnabled: ptrBool(false),
		// 宽限终止收紧到 300ms：断开续传相关用例快速收敛
		DockerResumeClientGoneGrace: ptrDuration(300 * time.Millisecond),
	}
	rule := config.DomainRule{
		Name:  "docker-test",
		Type:  config.RuleTypeDocker,
		Match: testMatchRe,
		Cache: config.RuleCache{
			Enabled: ptrBool(true),
			TTL:     ptrDuration(time.Hour),
		},
	}
	if mutate != nil {
		mutate(cfg, &rule)
	}
	cfg.DomainRules = []config.DomainRule{rule}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("测试配置 Validate 失败: %v", err)
	}
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	caches := cache.NewManager(context.Background())
	srv, err := proxy.New(cfg, caches, logger)
	if err != nil {
		t.Fatalf("proxy.New 失败: %v", err)
	}
	env := &testEnv{t: t, srv: srv, cfg: cfg, log: &logBuf, dir: dir, caches: caches}
	t.Cleanup(func() { env.dumpLogOnFail() })
	return env
}

// dumpLogOnFail 测试失败时把捕获的日志倒出来，便于定位。
func (env *testEnv) dumpLogOnFail() {
	if env.t.Failed() {
		env.t.Logf("=== 捕获日志 ===\n%s", env.log.String())
	}
}

// logText 返回目前捕获的全部日志文本。
func (env *testEnv) logText() string { return env.log.String() }

// serveRequest 以 httptest 内存 Recorder 驱动一次 ServeHTTP（不发真实网络请求）。
func (env *testEnv) serveRequest(method, path string, header http.Header) *httptest.ResponseRecorder {
	env.t.Helper()
	req := httptest.NewRequest(method, "http://"+testHost+path, nil)
	req.Host = testHost
	for k, vv := range header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	env.srv.ServeHTTP(rec, req)
	return rec
}

// get 驱动一次 GET。
func (env *testEnv) get(path string, header http.Header) *httptest.ResponseRecorder {
	return env.serveRequest(http.MethodGet, "/"+path, header)
}

// keyFor 计算路径目标的缓存 key（与 cacheKeyInput 同构：http://<Host>/<target>）。
func (env *testEnv) keyFor(target string) string {
	return cache.Key(config.BuildMatchStr(testHost, target))
}

// partPath 返回路径目标对应半截文件路径（cache 包公开 API 同构：.part-<key>）。
func (env *testEnv) partPath(target string) string {
	return filepath.Join(env.dir, ".part-"+env.keyFor(target))
}

// entryPath 返回路径目标对应正式缓存条目文件路径。
func (env *testEnv) entryPath(target string) string {
	return filepath.Join(env.dir, env.keyFor(target))
}

// seedPart 预置半截文件（内容 = blob 原文前缀 data[:n]），模拟上次中断残留。
func (env *testEnv) seedPart(target string, data []byte, n int) {
	env.t.Helper()
	if err := os.WriteFile(env.partPath(target), data[:n], 0o644); err != nil {
		env.t.Fatalf("预置半截文件失败: %v", err)
	}
}

// partSize 返回半截文件当前大小；不存在返回 (-1, false)。
func (env *testEnv) partSize(target string) (int64, bool) {
	st, err := os.Stat(env.partPath(target))
	if err != nil {
		return 0, false
	}
	return st.Size(), true
}

// waitFor 轮询等待 cond 成立（状态驱动而非 sleep 猜时序）；超时 fatal。
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待 %s 超时（%s）", what, timeout)
}

// //////////////////  httptest 假上游  /////////////////////////////////////////////////////////////////////////

// hitsCounter 便于在多个断言间共享命中计数。
type hitsCounter = atomic.Int64

// newUpstream 启动一个 httptest 假上游（本机回环，无真实外网）。
func newUpstream(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// upstreamFull 一次性吐 200 + 完整 body 的假上游（记录命中数与收到的 Range 头）。
type upstreamRecord struct {
	srv       *httptest.Server
	hits      *hitsCounter
	ranges    atomic.Int64 // 收到的带 Range 请求次数
	lastRange atomic.Value // 最后一次收到的 Range 原文（string）
}

func (u *upstreamRecord) rangeHits() int64 { return u.ranges.Load() }

func (u *upstreamRecord) lastRangeHeader() string {
	v, _ := u.lastRange.Load().(string)
	return v
}

// upstreamRangeCapable 支持 Range 的假上游：带 Range 按 bytes=N- / bytes=N-M 切片回
// 206 + Content-Range；无 Range 回 200 完整内容。startOut 记录每次 Range 的起始偏移。
func upstreamRangeCapable(t *testing.T, data []byte) *upstreamRecord {
	t.Helper()
	u := &upstreamRecord{hits: &hitsCounter{}}
	u.srv = newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		rng := r.Header.Get("Range")
		if rng != "" {
			u.ranges.Add(1)
			u.lastRange.Store(rng)
		}
		start, end := parseTestRange(rng, len(data))
		if rng != "" && start < 0 {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if rng == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	})
	return u
}

// upstreamPacedRange 限速吐 206 的假上游：按请求 Range 起始切片，每 chunk 字节歇 delay
// （模拟慢源），保证「下载中途」窗口可被时间驱动事件命中。
func upstreamPacedRange(t *testing.T, data []byte, chunk int, delay time.Duration) *upstreamRecord {
	t.Helper()
	u := &upstreamRecord{hits: &hitsCounter{}}
	u.srv = newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		start, _ := parseTestRange(r.Header.Get("Range"), len(data))
		if start < 0 {
			start = 0
		}
		if rng := r.Header.Get("Range"); rng != "" {
			u.ranges.Add(1)
			u.lastRange.Store(rng)
		}
		body := data[start:]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		for off := 0; off < len(body); off += chunk {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk
			if end > len(body) {
				end = len(body)
			}
			if _, err := w.Write(body[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(delay)
		}
	})
	return u
}

// upstreamCustom 自定义 handler 的假上游（记录命中数）。
func upstreamCustom(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *hitsCounter) {
	t.Helper()
	hits := &hitsCounter{}
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	})
	return srv, hits
}

// parseTestRange 解析测试用 Range 头（bytes=N- / bytes=N-M），钳制到 [0, size-1]；
// 无 Range 或非法返回 (-1, -1)。
func parseTestRange(v string, size int) (int, int) {
	spec := strings.TrimPrefix(v, "bytes=")
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return -1, -1
	}
	start, err := strconv.Atoi(strings.TrimSpace(first))
	if err != nil || start < 0 || start >= size {
		return -1, -1
	}
	end := size - 1
	if s := strings.TrimSpace(last); s != "" {
		if end, err = strconv.Atoi(s); err != nil || end < start {
			return -1, -1
		}
	}
	if end >= size {
		end = size - 1
	}
	return start, end
}

// //////////////////  可观测的客户端 Writer  /////////////////////////////////////////////////////////////////////////

// countingWriter 包一层响应写入计数器：代理向客户端每写一批字节计数 +1。
// 供假上游 handler 轮询（状态驱动同步），替代 sleep 猜时序。
type countingWriter struct {
	http.ResponseWriter
	n atomic.Int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n.Add(int64(n))
	return n, err
}

// failAfterWriter 收到超过 limit 字节后写失败的客户端 Writer（模拟 broken pipe /
// docker 断开）：先尽量写满剩余额度（半写 + 报错），之后一律失败——复刻真实断开形态。
type failAfterWriter struct {
	http.ResponseWriter
	remaining int64
	failed    atomic.Bool
}

var errClientGone = fmt.Errorf("test: broken pipe（客户端已断开）")

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		w.failed.Store(true)
		return 0, errClientGone
	}
	if int64(len(p)) > w.remaining {
		n := w.remaining
		wrote, _ := w.ResponseWriter.Write(p[:n])
		w.remaining = 0
		w.failed.Store(true)
		return int(wrote), errClientGone
	}
	w.remaining -= int64(len(p))
	return w.ResponseWriter.Write(p)
}

// serveWithWriter 用自定义 ResponseWriter 驱动一次 ServeHTTP（异步 goroutine 内执行，
// done 在响应处理完全结束后关闭）。返回 (writer, done)，供断开类场景观察中间状态。
func (env *testEnv) serveWithWriter(method, path string, header http.Header, w http.ResponseWriter) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(method, "http://"+testHost+path, nil)
		req.Host = testHost
		for k, vv := range header {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}
		env.srv.ServeHTTP(w, req)
	}()
	return done
}

// getWithWriter 同步驱动（当前 goroutine 内 ServeHTTP），返回自定义 writer。
func (env *testEnv) getWithWriter(path string, header http.Header, w http.ResponseWriter) {
	req := httptest.NewRequest(http.MethodGet, "http://"+testHost+path, nil)
	req.Host = testHost
	for k, vv := range header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	env.srv.ServeHTTP(w, req)
}

// //////////////////  goroutine 收敛检查（goleak 思路的轻量实现）  ///////////////////////////////////////////////

// assertGoroutinesSettle 等待 goroutine 数回落到 baseline+slack 内（超时 fatal）。
// 用于捕获「失败出口 cancel 不泄漏」类回归：看门狗 / AfterFunc / pump 协程泄漏会让
// 计数降不回来。
func assertGoroutinesSettle(t *testing.T, baseline int, slack int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		runtime.Gosched()
		n := runtime.NumGoroutine()
		if n <= baseline+slack {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			nb := runtime.Stack(buf, true)
			t.Fatalf("goroutine 数未收敛：now=%d baseline=%d slack=%d\n堆栈样例:\n%s",
				n, baseline, slack, buf[:nb])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// drainAndClose 读尽并关闭 body（防连接悬挂）。
func drainAndClose(rc io.ReadCloser) {
	if rc == nil {
		return
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
}

// countLines 统计日志文本中包含子串 substr 的行数。
func countLines(log, substr string) int {
	n := 0
	for _, ln := range strings.Split(log, "\n") {
		if strings.Contains(ln, substr) {
			n++
		}
	}
	return n
}

// logLines 返回日志中包含 substr 的全部行。
func logLines(log, substr string) []string {
	var out []string
	for _, ln := range strings.Split(log, "\n") {
		if strings.Contains(ln, substr) {
			out = append(out, ln)
		}
	}
	return out
}

// itoa int → 十进制字符串（测试断言拼接用）。
func itoa(n int) string { return strconv.Itoa(n) }

// //////////////////  内存 Recorder 适配（可被包装的客户端 Writer）  ///////////////////////////////////////////

// recorderAdapter httptest.ResponseRecorder 的显式适配器：包装进 countingWriter /
// failAfterWriter 等自定义 Writer 时类型清晰，且能读回状态码与 body。
type recorderAdapter struct {
	rec *httptest.ResponseRecorder
}

// httpRecorder 新建一个 recorder 适配器。
func httpRecorder() *recorderAdapter { return &recorderAdapter{rec: httptest.NewRecorder()} }

func (a *recorderAdapter) Header() http.Header         { return a.rec.Header() }
func (a *recorderAdapter) Write(p []byte) (int, error) { return a.rec.Write(p) }
func (a *recorderAdapter) WriteHeader(code int)        { a.rec.WriteHeader(code) }

// code 返回已写出的状态码（WriteHeader 后有效）。
func (a *recorderAdapter) code() int { return a.rec.Code }

// body 返回已收到的响应体。
func (a *recorderAdapter) body() []byte { return a.rec.Body.Bytes() }

// header 返回响应头。
func (a *recorderAdapter) header() http.Header { return a.rec.Header() }

// Bytes countingWriter 已写给客户端的字节数（假上游 handler 轮询的状态信号）。
func (w *countingWriter) Bytes() int64 { return w.n.Load() }

// body countingWriter 底层 recorder 收到的响应体。
func (w *countingWriter) body() []byte {
	if a, ok := w.ResponseWriter.(*recorderAdapter); ok {
		return a.body()
	}
	return nil
}

// requestTo 构造指向指定完整 URL 的 GET 请求（Host 取自 URL，ServeHTTP 直接驱动）。
func requestTo(rawURL string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	req.Host = mustHost(rawURL)
	return req
}

// mustHost 取完整 URL 的 host[:port] 段（测试内 URL 恒合法，出错即 panic 便于发现）。
func mustHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("requestTo URL 解析失败: " + rawURL)
	}
	return u.Host
}

// runServe 以内存 Recorder 同步驱动一次 ServeHTTP。
func runServe(srv *proxy.Server, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, r)
	return rec
}
