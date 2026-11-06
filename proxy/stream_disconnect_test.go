package proxy

// stream_disconnect_test.go —— 客户端断开即停（stream.go，提交 b251149）与日志规范
//（log_attrs.go，提交 391a95c）的单测：全部基于 httptest 假上游 + 注入写失败的
// ResponseWriter（模拟 docker 客户端 broken pipe——与真实断开同一感知机制），缓存目录
// 一律 t.TempDir()，测试只写临时目录，不含任何 e2e。复用 partial_resume_test.go 的
// resumeUpstream / prefetchBlobKey / prefetchBlobTestRequest / partCount / partBytes /
// resumeParseRangeStart 与 prefetch_test.go 的常量、slowdownload_chain_test.go 的
// slowEff / progressLines。覆盖点：
//   1. 客户端中途断开（写失败注入）→ 上游读取立即终止（上游发送计数停住）、返回
//      errClientDisconnected 且 committed=true、进度日志停止、不再向客户端写字节；
//   2. blob 主路径断开 → 半截保留（.part- 文件存在且内容为原文前缀），断开 Warn 运维
//      事件带 source=client + image/digest + client=disconnected + partial=true；下次
//      无 Range 完整 GET 从断点无感续传拿到完整 body（串起 resume 链路）；
//   3. 非 blob（manifest）断开 → partial=false、无半截文件（覆盖 writeFirst 首块即失败
//      分支：客户端在 commit 后、读上游前已断开）；
//   4. 半截开关关闭断开 → partial=false、无半截文件（维持既有「失败即丢弃」行为）；
//   5. 206/Range 请求断开 → 终止且无半截（既有边界：入口分流本不带 sink，Range 防御
//      分支兜底——本用例带 sink 直驱验证该防御）；
//   6. 慢速升级多源合并中断开 → 全部上游源被关闭（胜者与续传源活跃连接数均归零，
//      abortAll 生效）；
//   7. 日志规范冒烟：客户端主路径 MISS / HIT / BYPASS 日志含 source=client 与 image 键，
//      预取日志含 source=prefetch 与 image 键。

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// //////////////////  断开注入与计数辅助  //////////////////////////////////////////////////////////////

// breakAfterWriter 模拟客户端中途断开的 ResponseWriter：响应头照常写出（commit 语义
// 不变），body 写出前 limit 字节后 Write 一律报错——http 服务端在客户端连接断开后
// w.Write 即返回错误，流式实现据此感知断开（与 stream.go writeOut/writeFirst 的写
// 失败检测同一机制）。writeErrs 统计断开后的继续写尝试次数，供断言「断开后不再向
// 客户端写字节」。
type breakAfterWriter struct {
	rec       *httptest.ResponseRecorder
	limit     int
	mu        sync.Mutex
	sent      int // 已成功写给客户端的字节数
	writeErrs int // 断开后的 Write 调用次数（应随实现停止写客户端而停住）
}

func newBreakAfterWriter(limit int) *breakAfterWriter {
	return &breakAfterWriter{rec: httptest.NewRecorder(), limit: limit}
}

func (w *breakAfterWriter) Header() http.Header  { return w.rec.Header() }
func (w *breakAfterWriter) WriteHeader(code int) { w.rec.WriteHeader(code) }

func (w *breakAfterWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sent >= w.limit {
		w.writeErrs++
		return 0, errors.New("模拟客户端断开（broken pipe）")
	}
	w.sent += len(p)
	return w.rec.Write(p)
}

// sentBytes 已成功写给客户端的字节数（并发安全）。
func (w *breakAfterWriter) sentBytes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sent
}

// writeErrCount 断开后客户端侧的 Write 尝试次数（并发安全）。
func (w *breakAfterWriter) writeErrCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeErrs
}

// countingSlowUpstream 带发送/连接计数的慢速分块假上游（200 完整 body；rangeable=true
// 时支持 Range 回 206 剩余区间，供多源合并用例的第二候选）：
//   - hits 请求计数、sent 已成功写出字节数（客户端断开后代理 Close 上游连接，handler
//     Write 报错即退出 → sent 停住，断言「上游读取立即终止」）；
//   - active 当前活跃连接数（abortAll 逐路关闭全部上游源后应归零）。
type countingSlowUpstream struct {
	srv       *httptest.Server
	mu        sync.Mutex
	data      []byte
	chunk     int
	delay     time.Duration
	rangeable bool
	ranges    []string
	hits      atomic.Int64
	sent      atomic.Int64
	active    atomic.Int64
}

// newCountingSlowUpstream 启动计数慢速上游并注册 Cleanup。
func newCountingSlowUpstream(t *testing.T, data []byte, chunk int, delay time.Duration, rangeable bool) *countingSlowUpstream {
	t.Helper()
	u := &countingSlowUpstream{data: data, chunk: chunk, delay: delay, rangeable: rangeable}
	u.srv = httptest.NewServer(u)
	t.Cleanup(u.srv.Close)
	return u
}

func (u *countingSlowUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.hits.Add(1)
	u.active.Add(1)
	defer u.active.Add(-1)
	u.mu.Lock()
	data, chunk, delay, rangeable := u.data, u.chunk, u.delay, u.rangeable
	u.mu.Unlock()

	var body []byte
	status := http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" && rangeable {
		u.mu.Lock()
		u.ranges = append(u.ranges, rng)
		u.mu.Unlock()
		start := resumeParseRangeStart(rng)
		if start >= int64(len(data)) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body = data[start:]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
		status = http.StatusPartialContent
	} else {
		body = data
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	for off := 0; off < len(body); off += chunk {
		if r.Context().Err() != nil {
			return
		}
		end := off + chunk
		if end > len(body) {
			end = len(body)
		}
		if _, err := w.Write(body[off:end]); err != nil {
			return // 代理已 Close 上游连接（断开即停 / abortAll）：写失败即退出
		}
		u.sent.Add(int64(end - off))
		hubFlush(w)
		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}
	}
}

// rangeHeaders 返回收到的 Range 头快照（并发安全拷贝）。
func (u *countingSlowUpstream) rangeHeaders() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.ranges...)
}

// //////////////////  测试环境  ////////////////////////////////////////////////////////////////////////

// disconnectFixture 聚合一个客户端断开测试环境：支持 Range 的假 registry（复用
// resumeUpstream）+ 带 docker 规则的 Server（日志捕获到 logBuf，断言 source/image 键
// 与断开运维事件用）+ 独立缓存实例（与 Server 同目录，可断言半截文件与落盘结果）。
// docker-prefetch 显式关闭（隔离预取旁路，与 newResumeFixture 同款）。
type disconnectFixture struct {
	upstream *httptest.Server
	reg      *resumeUpstream
	s        *Server
	disk     *cache.Cache
	logBuf   *bytes.Buffer
}

// newDisconnectFixture 构建断开测试环境（resumeEnabled 传 nil 沿用全局默认开）。
func newDisconnectFixture(t *testing.T, resumeEnabled *bool) *disconnectFixture {
	t.Helper()
	reg := newResumeUpstream()
	upstream := httptest.NewServer(reg)
	t.Cleanup(upstream.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	if resumeEnabled != nil {
		cfg.DockerPartialResumeEnabled = resumeEnabled
	}
	prefetchOff := false
	cfg.DockerPrefetchEnabled = &prefetchOff
	templates := []config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "stream-disconnect",
		Match:       `http://registry-proxy-cache\.linkease\.net:5480/`,
		URLRedirect: &templates,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	var logBuf bytes.Buffer
	s, err := New(cfg, cache.NewManager(context.Background()), captureLogger(&logBuf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	disk, err := cache.New(cacheDir)
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}
	return &disconnectFixture{upstream: upstream, reg: reg, s: s, disk: disk, logBuf: &logBuf}
}

// effFor 返回某路径目标在本环境的生效配置。
func (fx *disconnectFixture) effFor(r *http.Request, target string) config.Options {
	return fx.s.cfg.ResolveOptions(r.Host, target, false)
}

// logLinesOf 返回日志中包含 substr 的全部行（断言单条日志字段用，与 progressLines 同款风格）。
func logLinesOf(log, substr string) []string {
	var lines []string
	for _, ln := range strings.Split(log, "\n") {
		if strings.Contains(ln, substr) {
			lines = append(lines, ln)
		}
	}
	return lines
}

// wantLineField 断言日志行包含全部字段片段（key=value 形态的 slog 文本格式）。
func wantLineField(t *testing.T, line string, fields ...string) {
	t.Helper()
	for _, f := range fields {
		if !strings.Contains(line, f) {
			t.Errorf("日志行应包含 %q，实际: %s", f, line)
		}
	}
}

// //////////////////  1：断开即停——上游读取立即终止  /////////////////////////////////////////////////

// TestClientDisconnectStopsUpstreamImmediately 客户端中途断开（写失败注入）：
// 流式回源立即终止上游读取并返回 errClientDisconnected（committed=true）——上游发送
// 计数停住（不再继续读完填缓存，废止旧语义）、进度日志停止输出、不再向客户端写字节。
func TestClientDisconnectStopsUpstreamImmediately(t *testing.T) {
	data := blobPattern(1 << 20) // 1MB：断开点远在流中段
	up := newCountingSlowUpstream(t, data, 32<<10, 20*time.Millisecond, false)
	// blob 形态目标（v2/<name>/blobs/<digest>）：docker 链路日志统一标签可解析，
	// 断开运维事件应带 source=client 与 image（非 /v2/ blob 目标按兜底设计省略新键）
	fx := newDisconnectFixture(t, nil)

	// 单候选（候选数=1 恒不升级）+ 关闭慢速升级，隔离断开即停语义；进度日志 30ms 一条
	eff := slowEff([]config.URLRedirectItem{{Template: up.srv.URL + "/$1"}}, config.RuleTypeDocker, func(o *config.Options) {
		o.SlowDownloadSpeed = 0
		o.ProgressLogInterval = 30 * time.Millisecond
	})

	w := newBreakAfterWriter(256 << 10) // 首块放行后写满 256KB 断开（≈160ms，断开前有进度）
	target := "v2/disco-app/layer/blobs/sha256:" + strings.Repeat("ff", 32)
	r := httptest.NewRequest(http.MethodGet, "http://registry-proxy-cache.linkease.net:5480/"+target, nil)
	entry, committed, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now())

	// 断开终止：committed=true（响应头已写出）+ errClientDisconnected + 无完整 entry
	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected（断开即停哨兵错误）", err)
	}
	if entry != nil {
		t.Error("断开终止不应返回完整 entry（半截数据绝不入库）")
	}

	// 客户端侧不再写字节：断开后 Write 不应再被调用（writeOut 置 clientOK=false 后不再写）
	sentAtReturn := w.sentBytes()
	errsAtReturn := w.writeErrCount()

	// 上游发送计数停住：代理已 Close 上游连接，handler 写失败退出，sent 不再增长。
	// 断开瞬间可能恰有 1 块已写入 TCP 缓冲（Close 与对端 Write 的竞态窗口，实测最多
	// 1 块），因此允许 ≤2 块的在途增量，随后必须在窗口内稳定——旧语义「继续读完」会
	// 持续每 20ms 送出一块（300ms 内 ~15 块 / ~480KB），稳定窗口足以区分。
	const connChunk = 32 << 10
	sent1 := up.sent.Load()
	deadline := time.Now().Add(3 * time.Second)
	var sent2 int64
	for {
		a := up.sent.Load()
		time.Sleep(150 * time.Millisecond) // 5 块时间：仍在读上游则必然增长
		b := up.sent.Load()
		sent2 = b
		if a == b {
			break // 已稳定：连接关闭、handler 退出
		}
		if time.Now().After(deadline) {
			t.Fatalf("断开后上游发送计数仍在增长（%d -> %d）——客户端已断开就不该继续读上游", a, b)
		}
	}
	if sent2-sent1 > int64(2*connChunk) {
		t.Errorf("断开后上游最多允许 2 块在途增量, got %d 字节（sent1=%d, sent2=%d）", sent2-sent1, sent1, sent2)
	}
	if sent2 >= int64(len(data)) {
		t.Errorf("断开后上游不应被读完（sent=%d, total=%d）", sent2, len(data))
	}
	if w.sentBytes() != sentAtReturn {
		t.Errorf("断开后不应再向客户端写字节（断开时 %d 字节，现 %d 字节）", sentAtReturn, w.sentBytes())
	}
	if w.writeErrCount() != errsAtReturn {
		t.Errorf("断开后客户端侧 Write 尝试应停止（断开时 %d 次，现 %d 次）", errsAtReturn, w.writeErrCount())
	}

	// 进度日志停止：断开前应有进度输出，断开后不再新增（读循环已退出）
	n1 := len(progressLines(fx.logBuf.String()))
	if n1 < 1 {
		t.Fatalf("断开前（≈256KB@1.6MB/s≈160ms）应输出 ≥1 条进度日志，实际:\n%s", fx.logBuf.String())
	}
	time.Sleep(150 * time.Millisecond)
	if n2 := len(progressLines(fx.logBuf.String())); n2 != n1 {
		t.Errorf("断开后进度日志应停止（断开时 %d 条，现 %d 条）", n1, n2)
	}

	// 断开运维事件留痕：Warn 级（含 source=client 统一标签与 image 镜像名）
	gone := logLinesOf(fx.logBuf.String(), "客户端已断开，终止上游读取")
	if len(gone) == 0 {
		t.Fatalf("应输出断开终止运维事件（Warn），实际:\n%s", fx.logBuf.String())
	}
	wantLineField(t, gone[len(gone)-1], "source=client", "image=disco-app", "level=WARN")
}

// //////////////////  2：blob 主路径断开 → 半截保留 + 无感续传  ///////////////////////////////////////

// TestBlobDisconnectPartialKeptThenResume blob 主路径（缓存可读、无 Range）中途断开：
// 半截保留（.part- 文件存在且内容为上游原文前缀，进度不回退），断开 Warn 运维事件带
// source=client + image/digest + client=disconnected + partial=true；随后无 Range 完整
// GET 从断点无感续传拿到完整 body（X-Cache: RESUME、上游恰好一路 Range 请求、完成后
// 半截删除且二次 GET 命中 HIT）——串起「断开即停 → 半截保留 → 断点续传」全链路。
func TestBlobDisconnectPartialKeptThenResume(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	// 256KB >> 探测首块上限 64KB：writeFirst 整块写出首块（breakAfterWriter 原子语义：
	// 首块经 sent<limit 放行）后，读循环的次一块写出即断开——上游仍有大量剩余未读
	content := hubBlobContent(0xd1, 256<<10)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 20*time.Millisecond) // 慢速分块：拉长流窗口供断开注入

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	w := newBreakAfterWriter(3072)
	entry, committed, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now(),
		partialSink{key: key, disk: fx.disk})

	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected", err)
	}
	if entry != nil {
		t.Error("断开终止不应返回完整 entry")
	}
	clientGot := w.sentBytes()
	if clientGot == 0 {
		t.Fatal("断开前客户端应已收到至少一个首块")
	}

	// 半截保留：.part-<key> 存在，内容为上游原文的非空前缀（断开时刻 accum ≥ 客户端已收字节）
	part := partBytes(t, fx.disk.PartPath(key))
	if len(part) < clientGot {
		t.Fatalf("半截文件应保留至少客户端已收的 %d 字节, got %d 字节", clientGot, len(part))
	}
	if !bytes.HasPrefix(content, part) {
		t.Errorf("半截内容应为上游原文前缀（got %d 字节）", len(part))
	}
	// 客户端已收 body 同样是原文前缀且不超过半截（半截 ≥ 已收：失败块也计入 accum）
	if got := w.rec.Body.Bytes(); !bytes.HasPrefix(content, got) {
		t.Errorf("客户端已收 body 应为原文前缀（got %d 字节）", len(got))
	}

	// 断开运维事件：source=client + image/digest 统一标签 + client=disconnected + partial=true
	gone := logLinesOf(fx.logBuf.String(), "客户端已断开，终止上游读取")
	if len(gone) == 0 {
		t.Fatalf("应输出断开终止运维事件，实际:\n%s", fx.logBuf.String())
	}
	wantLineField(t, gone[len(gone)-1],
		"source=client", "image="+name, "digest="+digest, "client=disconnected", "partial=true")

	// 断点续传链路：下次无 Range 完整 GET → 一次完整 200（docker 无感知），上游仅一路 Range 请求
	w2 := httptest.NewRecorder()
	fx.s.ServeHTTP(w2, prefetchBlobTestRequest(name, digest))
	if w2.Code != http.StatusOK {
		t.Fatalf("续传 GET Code = %d, want %d, body: %.100s", w2.Code, http.StatusOK, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("X-Cache = %q, want %q（应走续传执行方）", got, resumeCacheStatus)
	}
	if !bytes.Equal(w2.Body.Bytes(), content) {
		t.Errorf("续传 body 应与上游完整原文逐字节一致（got %d 字节, want %d 字节）", w2.Body.Len(), len(content))
	}
	// 上游视角：断开 1 路（200 全量，被断开即停终止）+ 续传 1 路 Range 请求，共 2 路；
	// 续传请求起点 = 半截长度
	if n := fx.reg.rec.calls(digest); n != 2 {
		t.Errorf("上游 blob 回源 %d 次, want 2（断开终止 1 路 + 续传 1 路；断开后不再继续读完）", n)
	}
	rs := fx.reg.rangesOf(digest)
	wantRange := fmt.Sprintf("bytes=%d-", len(part))
	if len(rs) != 2 || rs[0] != "" || rs[1] != wantRange {
		t.Errorf("上游收到的 Range 头 = %v, want [\"\" %q]（首路无 Range 断开终止，次路自半截断点续传）", rs, wantRange)
	}

	// 生命周期：续传完成后半截删除、正式缓存命中、二次 GET 走 HIT 不再回源
	if _, ok := fx.disk.Get(key); !ok {
		t.Fatal("续传完成后缓存应命中，disk.Get 未命中")
	}
	if n := partCount(t, fx.disk.Dir()); n != 0 {
		t.Errorf("续传完成后半截应已删除, 缓存目录仍有 %d 个半截文件", n)
	}
	w3 := httptest.NewRecorder()
	fx.s.ServeHTTP(w3, prefetchBlobTestRequest(name, digest))
	if got := w3.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("二次 GET X-Cache = %q, want HIT", got)
	}
	if n := fx.reg.rec.calls(digest); n != 2 {
		t.Errorf("二次 GET 后上游回源仍应 2 次（HIT 不回源）, got %d", n)
	}
}

// //////////////////  3/4：断开不保留半截的边界  /////////////////////////////////////////////////////

// TestManifestDisconnectNoPartial 非 blob（manifest 会变不适用半截）断开：
// 终止上游读取并返回 errClientDisconnected，partial=false、无半截文件。本用例同时覆盖
// writeFirst 首块写出即失败的分支（客户端在 commit 后、读上游前已断开——limit=0）。
func TestManifestDisconnectNoPartial(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name := prefetchName
	manifest := hubBlobContent(0xd2, 256<<10)
	fx.reg.setManifest(manifest, false)

	target := "v2/" + name + "/manifests/latest"
	r := httptest.NewRequest(http.MethodGet, "http://registry-proxy-cache.linkease.net:5480/"+target, nil)
	eff := fx.effFor(r, target)

	w := newBreakAfterWriter(0) // WriteHeader 后 Write 一律失败：首块写出即断开
	_, committed, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now(),
		partialSink{key: "unused", disk: fx.disk}) // sink 传了也必须被非 blob 防御拒绝

	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected", err)
	}
	if n := partCount(t, fx.disk.Dir()); n != 0 {
		t.Errorf("manifest 断开不应保留半截, 缓存目录有 %d 个半截文件", n)
	}
	// 运维事件 partial=false（不适用半截保留）
	gone := logLinesOf(fx.logBuf.String(), "客户端已断开，终止上游读取")
	if len(gone) == 0 {
		t.Fatalf("应输出断开终止运维事件，实际:\n%s", fx.logBuf.String())
	}
	wantLineField(t, gone[len(gone)-1], "source=client", "image="+name, "partial=false")
}

// TestDisconnectDisabledSwitchNoPartial 半截开关关闭时 blob 主路径断开：
// 终止语义不变（errClientDisconnected），但维持既有「失败即丢弃」行为——partial=false、
// 无半截文件。
func TestDisconnectDisabledSwitchNoPartial(t *testing.T) {
	fx := newDisconnectFixture(t, boolPtr(false)) // docker-partial-resume-enabled=false
	name, digest := prefetchName, prefetchDigestLayer2
	content := hubBlobContent(0xd3, 256<<10) // >> 探测首块 64KB：断开注入落在读循环内
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 20*time.Millisecond)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	w := newBreakAfterWriter(3072)
	_, committed, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now(),
		partialSink{key: key, disk: fx.disk})

	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected（开关关闭只影响半截，不影响断开即停）", err)
	}
	if n := partCount(t, fx.disk.Dir()); n != 0 {
		t.Errorf("开关关闭断开不应保留半截, 缓存目录有 %d 个半截文件", n)
	}
	gone := logLinesOf(fx.logBuf.String(), "客户端已断开，终止上游读取")
	if len(gone) == 0 {
		t.Fatalf("应输出断开终止运维事件，实际:\n%s", fx.logBuf.String())
	}
	wantLineField(t, gone[len(gone)-1], "partial=false")
}

// TestRangeDisconnectNoPartial 206/Range 请求断开（既有边界）：终止且无半截——
// 带 sink 直驱验证 captureStreamPartial 的 Range 防御分支（入口分流本不会带 sink，
// 该防御兜底「任何路径下带 Range 都不做本地半截」），上游 206 分块流同样被立即终止。
func TestRangeDisconnectNoPartial(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestOther
	content := hubBlobContent(0xd4, 256<<10) // >> 探测首块 64KB：断开注入落在读循环内
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(2048, 15*time.Millisecond)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	r.Header.Set("Range", "bytes=100-") // 206 续传请求（BYPASS 分流）
	eff := fx.effFor(r, target)

	w := newBreakAfterWriter(4096)
	_, committed, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "BYPASS", nil, time.Now(),
		partialSink{key: key, disk: fx.disk}) // 防御验证：带 Range 即使带 sink 也绝不保留半截

	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected", err)
	}
	if got := w.rec.Code; got != http.StatusPartialContent {
		t.Errorf("已 commit 的状态码 = %d, want %d（206 透传不被断开改变）", got, http.StatusPartialContent)
	}
	if n := partCount(t, fx.disk.Dir()); n != 0 {
		t.Errorf("Range 请求断开不应保留半截, 缓存目录有 %d 个半截文件", n)
	}
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Range 请求断开不应产生半截文件, stat err = %v", err)
	}
}

// //////////////////  6：慢速升级多源合并中断开 → 全部源被关闭  ///////////////////////////////////////

// TestSlowMergeDisconnectAbortsAllSources 慢速升级多源合并中的客户端断开：
// writeOut 检测写失败即 abortAll 逐路关闭全部上游源（含尚未定局的 pending 续传源）——
// 胜者与续传源的活跃连接数均归零（上游 handler 全部退出），合并循环退出后返回
// errClientDisconnected；上游对续传源恰好一路 Range 请求（升级发生过）。
func TestSlowMergeDisconnectAbortsAllSources(t *testing.T) {
	data := blobPattern(1 << 20) // 1MB
	winner := newCountingSlowUpstream(t, data, 32<<10, 20*time.Millisecond, false)
	second := newCountingSlowUpstream(t, data, 32<<10, 20*time.Millisecond, true) // 支持 Range：续传候选

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	// 默认 slowEff 阈值（100MB/s）下限速胜者必判慢：窗口 200ms 满时已送 ~320KB < 100MB/s
	eff := slowEff([]config.URLRedirectItem{
		{Template: winner.srv.URL + "/$1"},
		{Template: second.srv.URL + "/$1"},
	}, config.RuleTypeDocker, nil)

	// 断开点 480KB：升级点 ~320KB 之后（确认已进入多源合并循环），第二源仍在传输中
	w := newBreakAfterWriter(480 << 10)
	target := slowPath
	r := httptest.NewRequest(http.MethodGet, "http://"+slowHost+"/"+target, nil)
	_, committed, err := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now())

	if !committed {
		t.Fatal("断开终止应发生在 commit 之后（committed=true）")
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("err = %v, want errClientDisconnected（多源合并中的断开同样断开即停）", err)
	}

	// 升级确实发生过（断开前已进入合并循环）：升级日志 + 第二候选收到一路 Range 续传
	if !strings.Contains(logBuf.String(), "下载过慢，升级多源并发续传") {
		t.Errorf("日志应捕获「下载过慢，升级多源并发续传」，实际:\n%s", logBuf.String())
	}
	if n := second.hits.Load(); n != 1 {
		t.Errorf("第二候选应恰好收到 1 次续传请求, got %d", n)
	}
	if rs := second.rangeHeaders(); len(rs) == 1 {
		assertRangeTail(t, rs[0], int64(len(data)-1), 0)
	} else {
		t.Errorf("第二候选收到的 Range 头 = %v, want 恰好 1 条", rs)
	}

	// abortAll 生效：全部上游源被关闭——胜者与续传源活跃连接数均归零
	resumeWaitUntil(t, 3*time.Second, func() bool {
		return winner.active.Load() == 0 && second.active.Load() == 0
	}, "断开后全部上游源应被关闭（两源活跃连接数均归零）")
}

// //////////////////  7：日志规范冒烟  ////////////////////////////////////////////////////////////////

// TestDockerLogAttrsSmoke 日志规范（log_attrs.go）冒烟：docker 链路日志统一 source 来源
// 标签与 image 镜像标签——客户端主路径 MISS（流式透传）/ HIT / BYPASS 三类日志均含
// source=client 与 image 键（blob 目标附带 digest），预取日志含 source=prefetch 与 image。
func TestDockerLogAttrsSmoke(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xd5, 8<<10)
	fx.reg.setContent(digest, content)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	// 主路径 MISS：完整流式回源（正常 recorder，客户端不断开）
	w := httptest.NewRecorder()
	if _, _, err := fx.s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", nil, time.Now(),
		partialSink{key: key, disk: fx.disk}); err != nil {
		t.Fatalf("完整流应成功返回: %v", err)
	}
	start := logLinesOf(fx.logBuf.String(), "流式透传开始")
	if len(start) == 0 {
		t.Fatalf("应输出流式透传开始日志，实际:\n%s", fx.logBuf.String())
	}
	wantLineField(t, start[0], "source=client", "image="+name, "digest="+digest)
	done := logLinesOf(fx.logBuf.String(), "代理请求完成")
	if len(done) == 0 {
		t.Fatal("应输出代理请求完成日志")
	}
	wantLineField(t, done[len(done)-1], "source=client", "image="+name)

	// HIT：预写正式缓存后 GET，命中日志带 source=client + image
	if err := fx.disk.Set(key, &cache.Entry{
		Status:   http.StatusOK,
		Header:   http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:     content,
		StoredAt: time.Now(),
	}); err != nil {
		t.Fatalf("预写缓存失败: %v", err)
	}
	logBufLen := fx.logBuf.Len()
	wHit := httptest.NewRecorder()
	fx.s.ServeHTTP(wHit, prefetchBlobTestRequest(name, digest))
	if got := wHit.Header().Get("X-Cache"); got != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", got)
	}
	hitLog := fx.logBuf.String()[logBufLen:]
	hit := logLinesOf(hitLog, "缓存命中（HIT）")
	if len(hit) == 0 {
		t.Fatalf("应输出缓存命中日志，实际:\n%s", hitLog)
	}
	wantLineField(t, hit[0], "source=client", "image="+name)

	// BYPASS：起点 >0 的 Range（本地无缓存、无半截）走旧版 206 透传，日志带 source=client + image。
	// 注：起点 0 的 blob Range 本轮已按完整 GET 接管（见 client_range.go），不再进 BYPASS，
	// 因此这里必须用起点 >0 且本地确实没有内容的 digest 才能触发 BYPASS 决策日志。
	logBufLen = fx.logBuf.Len()
	bypassDigest := prefetchDigestConfig
	fx.reg.setContent(bypassDigest, hubBlobContent(0xd7, 4<<10))
	wBypass := httptest.NewRecorder()
	rb := prefetchBlobTestRequest(name, bypassDigest)
	rb.Header.Set("Range", "bytes=100-")
	fx.s.ServeHTTP(wBypass, rb)
	if wBypass.Code != http.StatusPartialContent {
		t.Fatalf("BYPASS Code = %d, want %d（起点 >0 且本地无内容维持 206 透传）", wBypass.Code, http.StatusPartialContent)
	}
	bypassLog := fx.logBuf.String()[logBufLen:]
	bypass := logLinesOf(bypassLog, "GET 请求绕过缓存（BYPASS）")
	if len(bypass) == 0 {
		t.Fatalf("应输出 BYPASS 决策日志，实际:\n%s", bypassLog)
	}
	wantLineField(t, bypass[0], "source=client", "image="+name, "reason=blob-range")

	// 预取：后台预取完成日志带 source=prefetch + image + digest（与客户端来源区分）
	logBufLen = fx.logBuf.Len()
	d2 := prefetchDigestLayer2
	content2 := hubBlobContent(0xd6, 4<<10)
	fx.reg.setContent(d2, content2)
	r2 := prefetchBlobTestRequest(name, d2)
	fx.s.prefetchOneBlob(context.Background(), r2, name, d2, fx.effFor(r2, "v2/"+name+"/blobs/"+d2), fx.disk)
	prefetchLog := fx.logBuf.String()[logBufLen:]
	pf := logLinesOf(prefetchLog, "预取 blob 写入缓存完成")
	if len(pf) == 0 {
		t.Fatalf("应输出预取写缓存完成日志，实际:\n%s", prefetchLog)
	}
	wantLineField(t, pf[0], "source=prefetch", "image="+name, "digest="+d2)
}
