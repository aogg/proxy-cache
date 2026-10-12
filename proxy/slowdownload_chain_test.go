package proxy

// slowdownload_chain_test.go —— 慢速升级多源并发续传的全链路单测：
// 全部基于 httptest 内存服务器（快慢两台假上游，无任何真实网络），直接驱动
// fetchViaCandidatesStreaming 或经 Server.ServeHTTP 走完整链路：
//   - type=docker 规则 + 高判慢阈值：胜者限速吐 200 + 大 body → 触发「下载过慢，升级
//     多源并发续传」，第二候选收到 Range: bytes=N-{end} 续传请求，客户端字节与原始数据
//     逐字节一致；finalize 收到完整 entry（200 场景）；
//   - type=web 同样慢：不升级（RuleType 门控），第二候选零请求；
//   - 206 胜者（客户端带 Range）：Content-Range 解析正确、续传 Range 尾界正确；
//   - ServeHTTP 全链路（规则配置走 Validate，window 钳制 1s）：升级下载完整后写缓存，
//     第二次 GET 命中 HIT 且不再回源。
//
// 时序断言均用宽松余量（以最终字节一致为准，不断言精确日志条数/精确偏移），防 flaky。

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// slowdownload 链路测试专用常量：入站 Host 与路径目标（match 正则只写 Host 前缀恒命中）。
const (
	slowHost   = "registry-slow.example.com:5480"
	slowPath   = "v2/test/big/layer"
	slowRuleRe = `http://registry-slow\.example\.com:5480/`
)

// //////////////////  指针 helper（proxy 包测试用）  ////////////////////////////////////////////////////////

func bytesPtr(v int64) *config.Bytes          { b := config.Bytes(v); return &b }
func durPtr(v time.Duration) *config.Duration { d := config.Duration(v); return &d }
func strPtr(v string) *string                 { return &v }
func boolPtr(v bool) *bool                    { return &v }

// rangeRecorder 并发记录续传候选收到的 Range 请求头。
type rangeRecorder struct {
	mu     sync.Mutex
	ranges []string
}

func (rec *rangeRecorder) add(v string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.ranges = append(rec.ranges, v)
}

func (rec *rangeRecorder) all() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.ranges...)
}

// newSlowTestServer 构建全局关缓存、日志写入 buf 的 Server（配置走 Validate）。
func newSlowTestServer(t *testing.T, buf *bytes.Buffer) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Cache.Enabled = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, captureLogger(buf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// slowEff 组装慢速升级直驱测试的生效配置：
// 候选=items、content 判据、指定 RuleType；慢速升级参数用「阈值极高 + 窗口 200ms +
// 剩余门槛极小」保证限速胜者必然触发；mutate 可再覆盖。
func slowEff(items []config.URLRedirectItem, ruleType string, mutate func(o *config.Options)) config.Options {
	o := config.Options{
		URLRedirect:              items,
		SuccessCheck:             config.SuccessCheckContent,
		Rule:                     "slow-test",
		RuleType:                 ruleType,
		SlowDownloadSpeed:        100 << 20, // 阈值极高：限速胜者的窗口速率必然低于它
		SlowDownloadWindow:       200 * time.Millisecond,
		SlowDownloadMinRemaining: 1024, // 剩余门槛极小：窗口满时必然满足
		SlowDownloadMaxBehind:    1 << 20,
	}
	if mutate != nil {
		mutate(&o)
	}
	return o
}

// slowChunkUpstream 启动限速吐 200 + 完整 body 的假上游（每 chunk 字节歇 delay，模拟慢源），
// hits 记录收到的请求次数。
func slowChunkUpstream(t *testing.T, data []byte, chunk int, delay time.Duration, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		for off := 0; off < len(data); off += chunk {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk
			if end > len(data) {
				end = len(data)
			}
			if _, err := w.Write(data[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(delay)
		}
	}))
}

// slowRangeUpstream 启动限速吐 206 的假上游：按 start 参数返回 Content-Range
// 「bytes start-(len-1)/len」并限速流出 data[start:]（模拟 206 胜者）。
func slowRangeUpstream(t *testing.T, data []byte, start int64, chunk int, delay time.Duration, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
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
	}))
}

// fastFullUpstream 启动一次性吐完整 body 的快上游。
func fastFullUpstream(t *testing.T, data []byte, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
}

// rangeCapableUpstream 启动支持 Range 的假上游：带 Range 头时按「bytes=N-M / bytes=N-」
// 切片回 206 + Content-Range（并记录收到的 Range 原文）；无 Range 回 200 完整内容。
func rangeCapableUpstream(t *testing.T, data []byte, rec *rangeRecorder, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rng := r.Header.Get("Range")
		if rng != "" {
			rec.add(rng)
			start, end := parseBytesRange(rng, len(data))
			if start < 0 || end < start {
				http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
}

// parseBytesRange 解析测试用 Range 头（bytes=N-M / bytes=N-），越界钳制到 [0, size-1]；
// 非法返回 (-1, -1)。
func parseBytesRange(v string, size int) (int, int) {
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

// runStreamFetch 以路径目标请求直驱 fetchViaCandidatesStreaming（httptest 内存 Recorder）。
func runStreamFetch(t *testing.T, s *Server, eff config.Options, target, rangeHeader string, finalize func(*cache.Entry)) (*httptest.ResponseRecorder, *cache.Entry, bool, error) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/"+target, nil)
	if rangeHeader != "" {
		r.Header.Set("Range", rangeHeader)
	}
	rec := httptest.NewRecorder()
	entry, committed, err := s.fetchViaCandidatesStreaming(rec, r, target, eff, "MISS", finalize, time.Now())
	return rec, entry, committed, err
}

// progressLines 从捕获日志中取出全部「流式透传进度」行。
func progressLines(log string) []string {
	var lines []string
	for _, ln := range strings.Split(log, "\n") {
		if strings.Contains(ln, "流式透传进度") {
			lines = append(lines, ln)
		}
	}
	return lines
}

// assertRangeTail 断言一条续传 Range 头形如 bytes=N-{wantEnd} 且 0 < N < wantEnd（宽松偏移，
// 以尾界与格式为准，不断言精确 N，防 flaky）。
func assertRangeTail(t *testing.T, rng string, wantEnd int64, minStart int64) {
	t.Helper()
	if !strings.HasPrefix(rng, "bytes=") {
		t.Fatalf("续传 Range 头应形如 bytes=N-M，得到 %q", rng)
	}
	spec := strings.TrimPrefix(rng, "bytes=")
	first, last, found := strings.Cut(spec, "-")
	if !found {
		t.Fatalf("续传 Range 头应含尾界，得到 %q", rng)
	}
	n, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		t.Fatalf("续传 Range 起始偏移应可解析，得到 %q", rng)
	}
	if n <= minStart || n >= wantEnd {
		t.Errorf("续传 Range 起始偏移 %d 应在 (%d, %d) 内（续传自当前进度开始），得到 %q", n, minStart, wantEnd, rng)
	}
	if last != strconv.FormatInt(wantEnd, 10) {
		t.Errorf("续传 Range 尾界 = %q, want %q（已知总长-1）", last, strconv.FormatInt(wantEnd, 10))
	}
}

// TestSlowDownloadEscalationDockerFullChain docker 规则 + 限速胜者 + Range 能力第二候选：
// 触发「下载过慢，升级多源并发续传」，第二候选收到 bytes=N-{end} 续传请求，
// 客户端收到与原始数据逐字节一致的完整 200 响应，finalize 收到完整 entry。
func TestSlowDownloadEscalationDockerFullChain(t *testing.T) {
	data := blobPattern(1 << 20) // 1MB
	var winnerHits, resumeHits atomic.Int64
	var ranges rangeRecorder
	winner := slowChunkUpstream(t, data, 32<<10, 20*time.Millisecond, &winnerHits)
	defer winner.Close()
	resume := rangeCapableUpstream(t, data, &ranges, &resumeHits)
	defer resume.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	// finalize 回调捕获（200 场景：完整 entry 应在这里交给缓存写入方）
	var finEntry *cache.Entry
	finalize := func(e *cache.Entry) { finEntry = e }

	target := slowPath
	eff := slowEff([]config.URLRedirectItem{
		{Template: winner.URL + "/$1"},
		{Template: resume.URL + "/$1"},
	}, config.RuleTypeDocker, nil)

	rec, _, committed, err := runStreamFetch(t, s, eff, target, "", finalize)
	if err != nil {
		t.Fatalf("升级续传后应完整返回: %v", err)
	}
	if !committed {
		t.Error("流式回源应已 commit（响应头已写出）")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("Code = %d, want %d（升级绝不动客户端已见状态码）", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Proxy-Upstream"); got != winner.URL+"/"+target {
		t.Errorf("X-Proxy-Upstream = %q, want 胜者候选 %q（连续性：绝不切换候选）", got, winner.URL+"/"+target)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Errorf("客户端应收到与原始数据逐字节一致的完整 body（got %d 字节, want %d）", rec.Body.Len(), len(data))
	}
	if !strings.Contains(logBuf.String(), "下载过慢，升级多源并发续传") {
		t.Errorf("日志应捕获「下载过慢，升级多源并发续传」，实际:\n%s", logBuf.String())
	}
	if got := resumeHits.Load(); got != 1 {
		t.Errorf("第二候选应恰好收到 1 次续传请求，得到 %d", got)
	}
	if got := winnerHits.Load(); got != 1 {
		t.Errorf("胜者应保持既有连接（1 次请求），得到 %d", got)
	}
	if rs := ranges.all(); len(rs) != 1 {
		t.Fatalf("第二候选收到的 Range 头 = %v, want 恰好 1 条", rs)
	} else {
		assertRangeTail(t, rs[0], int64(len(data)-1), 0)
	}
	if finEntry == nil {
		t.Fatal("200 场景 finalize 应收到完整 entry")
	}
	if finEntry.Status != http.StatusOK || !bytes.Equal(finEntry.Body, data) {
		t.Errorf("finalize entry 应为 200 + 完整 body（status=%d, body %d 字节）", finEntry.Status, len(finEntry.Body))
	}
}

// TestSlowDownloadRuleTypeWebNoEscalation 同样慢的胜者 + type=web 生效规则：
// 慢速升级被 RuleType 门控关闭——无升级日志、第二候选零请求，流仍由胜者单源完整返回。
func TestSlowDownloadRuleTypeWebNoEscalation(t *testing.T) {
	data := blobPattern(1 << 20)
	var winnerHits, resumeHits atomic.Int64
	winner := slowChunkUpstream(t, data, 32<<10, 20*time.Millisecond, &winnerHits)
	defer winner.Close()
	resume := rangeCapableUpstream(t, data, &rangeRecorder{}, &resumeHits)
	defer resume.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	eff := slowEff([]config.URLRedirectItem{
		{Template: winner.URL + "/$1"},
		{Template: resume.URL + "/$1"},
	}, config.RuleTypeWeb, nil) // type=web：仅门控差异

	rec, _, committed, err := runStreamFetch(t, s, eff, slowPath, "", nil)
	if err != nil {
		t.Fatalf("web 规则下单源慢速流应完整返回: %v", err)
	}
	if !committed {
		t.Error("流式回源应已 commit")
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Errorf("客户端应收到完整 body（got %d 字节, want %d）", rec.Body.Len(), len(data))
	}
	if strings.Contains(logBuf.String(), "下载过慢，升级多源并发续传") {
		t.Errorf("type=web 不应触发升级，日志却含升级信息:\n%s", logBuf.String())
	}
	if got := resumeHits.Load(); got != 0 {
		t.Errorf("type=web 不应发起续传，第二候选却收到 %d 次请求", got)
	}
}

// TestSlowDownloadEscalationRangeWinner206 206 胜者（客户端带 Range 续传进入）：
// 胜者 206 的 Content-Range 被解析为升级目标（起始偏移/总长/尾界），
// 续传 Range 头尾界 == 总长-1，客户端收到与原始切片一致的 206 响应。
func TestSlowDownloadEscalationRangeWinner206(t *testing.T) {
	const offset = 100
	data := blobPattern(1 << 20)
	var winnerHits, resumeHits atomic.Int64
	var ranges rangeRecorder
	winner := slowRangeUpstream(t, data, offset, 32<<10, 20*time.Millisecond, &winnerHits)
	defer winner.Close()
	resume := rangeCapableUpstream(t, data, &ranges, &resumeHits)
	defer resume.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	eff := slowEff([]config.URLRedirectItem{
		{Template: winner.URL + "/$1"},
		{Template: resume.URL + "/$1"},
	}, config.RuleTypeDocker, nil)

	rec, _, committed, err := runStreamFetch(t, s, eff, slowPath, fmt.Sprintf("bytes=%d-", offset), nil)
	if err != nil {
		t.Fatalf("206 胜者升级续传后应完整返回: %v", err)
	}
	if !committed {
		t.Error("流式回源应已 commit")
	}
	if rec.Code != http.StatusPartialContent {
		t.Errorf("Code = %d, want %d（升级绝不改变已 commit 的 206）", rec.Code, http.StatusPartialContent)
	}
	if got := rec.Header().Get("Content-Range"); got != fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)) {
		t.Errorf("Content-Range = %q, want 胜者声明值原样透传", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), data[offset:]) {
		t.Errorf("客户端应收到 data[%d:] 完整切片（got %d 字节, want %d）", offset, rec.Body.Len(), len(data)-offset)
	}
	if !strings.Contains(logBuf.String(), "下载过慢，升级多源并发续传") {
		t.Errorf("206 胜者同样应触发升级日志，实际:\n%s", logBuf.String())
	}
	if got := resumeHits.Load(); got != 1 {
		t.Errorf("第二候选应恰好收到 1 次续传请求，得到 %d", got)
	}
	if rs := ranges.all(); len(rs) != 1 {
		t.Fatalf("第二候选收到的 Range 头 = %v, want 恰好 1 条", rs)
	} else {
		// 尾界 = 对象总长-1；起始偏移必须大于 206 胜者的起始偏移（自当前进度续传）
		assertRangeTail(t, rs[0], int64(len(data)-1), offset)
	}
}

// TestSlowDownloadEscalationServeHTTPCacheHit ServeHTTP 全链路（规则配置走 Validate，
// window 取合法最小 1s）：限速胜者 3MB + Range 能力第二候选 → 升级续传下载完整并写缓存，
// 第二次 GET 命中 HIT 且不再回源。
func TestSlowDownloadEscalationServeHTTPCacheHit(t *testing.T) {
	data := blobPattern(3 << 20) // 3MB：保证 1s 窗口满时仍有充足剩余量
	var winnerHits, resumeHits atomic.Int64
	var ranges rangeRecorder
	winner := slowChunkUpstream(t, data, 32<<10, 20*time.Millisecond, &winnerHits)
	defer winner.Close()
	resume := rangeCapableUpstream(t, data, &ranges, &resumeHits)
	defer resume.Close()

	ruleCacheDir := t.TempDir()
	var logBuf bytes.Buffer
	cfg := config.Default()
	cfg.Cache.Enabled = false // 全局关缓存，规则级显式开启（覆盖验证规则级 slow-download 配置）
	cfg.DomainRules = []config.DomainRule{{
		Name:  "slow-docker",
		Type:  config.RuleTypeDocker,
		Match: slowRuleRe,
		URLRedirect: &[]config.URLRedirectItem{
			{Template: winner.URL + "/$1"},
			{Template: resume.URL + "/$1"},
		},
		SlowDownloadSpeed:        bytesPtr(100 << 20), // 阈值极高：限速胜者必然判慢
		SlowDownloadWindow:       durPtr(time.Second), // Validate 允许的最小窗口
		SlowDownloadMinRemaining: bytesPtr(1),
		SlowDownloadMaxBehind:    bytesPtr(1 << 20),
		Cache:                    config.RuleCache{Enabled: boolPtr(true), Path: strPtr(ruleCacheDir)},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, cache.NewManager(context.Background()), captureLogger(&logBuf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}

	do := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/"+slowPath, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}

	// 第一次 GET：MISS 流式回源，途中升级多源并发续传
	w1 := do()
	if w1.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（body: %.100s）", w1.Code, http.StatusOK, w1.Body.String())
	}
	if got := w1.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want %q", got, "MISS")
	}
	if !bytes.Equal(w1.Body.Bytes(), data) {
		t.Errorf("客户端应收到完整 body（got %d 字节, want %d）", w1.Body.Len(), len(data))
	}
	if !strings.Contains(logBuf.String(), "下载过慢，升级多源并发续传") {
		t.Errorf("日志应捕获「下载过慢，升级多源并发续传」，实际:\n%s", logBuf.String())
	}
	if got := resumeHits.Load(); got != 1 {
		t.Errorf("第二候选应恰好收到 1 次续传请求，得到 %d", got)
	}
	if rs := ranges.all(); len(rs) != 1 {
		t.Fatalf("第二候选收到的 Range 头 = %v, want 恰好 1 条", rs)
	} else {
		assertRangeTail(t, rs[0], int64(len(data)-1), 0)
	}

	// 第二次 GET：HIT 读缓存回放完整条目，不再回源
	w2 := do()
	if got := w2.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("第二次 GET X-Cache = %q, want %q（升级下载完整后应写缓存）", got, "HIT")
	}
	if !bytes.Equal(w2.Body.Bytes(), data) {
		t.Errorf("HIT body 应与原始数据一致（got %d 字节, want %d）", w2.Body.Len(), len(data))
	}
	if got := resumeHits.Load(); got != 1 {
		t.Errorf("HIT 不应回源，第二候选请求次数变为 %d", got)
	}
	if got := winnerHits.Load(); got != 1 {
		t.Errorf("HIT 不应回源，胜者请求次数变为 %d", got)
	}
}
