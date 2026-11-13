package proxy

// blob_concurrency_test.go —— 时间驱动的单层 blob 多源并发补充（blob-concurrency-interval，
// 提交 a28a014）的单测与集成测试。全部使用 httptest 假上游（不启动本服务、不发真实外网
// 请求），缓存目录一律 t.TempDir()，只写临时目录，不含任何 e2e。与 slowdownload.go /
// stream.go / partial_resume.go / blob_size_index.go 同包（白盒），可直接构造
// blobConcurrencyTrack / blobSupplementInput 并调用 timeBlobSupplement / joinSlowSources /
// recordManifestBlobSizes 等未导出成员；复用既有辅助（prefetchName / prefetchDigest* /
// prefetchBlobKey / prefetchBlobTestRequest / realDigestOf / parseBytesRange / hubFlush /
// durPtr / bytesPtr / wantLineField / logLinesOf / resumeWaitUntil / partBytes /
// newPartialTestServer 等）。覆盖：
//   1. blobConcurrencyTrack.due：未到点 false；首次满 interval 返回 round=1；之后逐轮递增；
//      错过多个时间点只补一次判定、下一轮从 now 重新计时；nil 轨道 / interval<=0 /
//      enabled=false 都不建轨道；
//   2. timeBlobSupplement 判定矩阵：client_gone / candidates_lt_2 / content_length_unknown /
//      all_joined / remaining_too_small 各自 created=false、不创建（或保留）合并器，且
//      **不发起任何补源请求**（假上游零回源）；优先级 client_gone > candidates_lt_2；
//      可补时单源阶段 created=true 且合并器源数增加（加入源 Range 起点 = 合并游标），
//      合并阶段再调用只增量加源、绝不重复拉取同一候选；
//   3. joinSlowSources 去重：胜者 / 已在合并中的源 / 已剪枝的源都不重复加入且状态不被重置；
//      新加入源 Range 起点 == 调用方游标、尾界 == 已知总长-1；加入日志字段齐全；
//   4. 客户端主路径（stream.go）时间驱动：胜者 200 **无 Content-Length** + manifest 记录
//      size → 满间隔后补充快源，客户端收到与原文逐字节一致的完整数据、写缓存条目完整
//      （生产默认 sha256 强校验通过）；未到 interval 时补充绝不发生（候选零请求、无评估
//      日志）；ServeHTTP 全链路（规则级 1s）首次 MISS 写缓存、二次 GET 命中 HIT；
//   5. 续传执行方（partial_resume.go）时间驱动：缓存半截前缀 + 慢速续传源 + 快源补充 →
//      客户端收到完整 200、半截删除、写缓存条目完整（sha256 校验通过）；客户端断开且
//      无搭车订阅者的宽限终止出口 → 合并器内**全部**上游源被 abort（连接关闭、写出停住）；
//   6. 日志契约：评估 / 补充 / 未补充三类日志的键存在，且 reason / total_source 枚举常量
//      值符合运维 grep 契约（含 upstream_no_range 的真实留痕）；
//   7. manifest blob size 索引（blob_size_index.go）：put/get、非法项忽略、重复 put 不占
//      淘汰队列、FIFO 淘汰最旧、recordManifestBlobSizes 记录 config+layers。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// //////////////////  假上游：可配 200/206 的 blob registry  //////////////////////////////////////

// blobConcUpstream 时间驱动补充测试用假 blob 上游（实现 http.Handler）：
//   - 无 Range → 200 全量；带 Range → 206（按 RFC 7233 切片，起点与请求一致）；
//   - noLength：200 响应**不设 Content-Length**（写响应头即 Flush → Go 用 chunked 编码），
//     精确复现线上故障形态「上游 200 无 Content-Length → 判速升级被静默跳过」；
//   - ignoreRange：对带 Range 的请求一律回 200 全量（上游不支持断点续传）；
//   - chunk/delay 分块写出（块间 sleep，拉长流式下载窗口供时间节拍观察）；
//   - hits 请求数、ranges 逐次收到的 Range 头、sent 累计成功写出字节数、active 当前活跃
//     连接数（宽限终止关闭上游连接 → handler 写失败退出，sent 停住、active 归零）。
type blobConcUpstream struct {
	mu          sync.Mutex
	data        []byte
	chunk       int
	delay       time.Duration
	noLength    bool
	ignoreRange bool
	hits        int
	active      int
	sent        int64
	ranges      []string
	pathHits    map[string]int      // 请求路径 -> 命中次数（区分同 server 上的多个候选 URL）
	pathRanges  map[string][]string // 请求路径 -> 逐次 Range 头
}

// newBlobConcUpstream 构建假上游（默认整段一次写出、200 带 Content-Length）。
func newBlobConcUpstream(data []byte) *blobConcUpstream {
	return &blobConcUpstream{
		data:       data,
		pathHits:   make(map[string]int),
		pathRanges: make(map[string][]string),
	}
}

// setData 设置 blob 完整内容（夹具在构造后按用例注入）。
func (u *blobConcUpstream) setData(data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.data = data
}

// setChunk 设置分块大小与块间隔（chunk<=0 = 整段一次写出）。
func (u *blobConcUpstream) setChunk(chunk int, delay time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.chunk, u.delay = chunk, delay
}

// setNoContentLength 让 200 响应不带 Content-Length（chunked 编码，见类型注释）。
func (u *blobConcUpstream) setNoContentLength() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.noLength = true
}

// setIgnoreRange 让上游忽略 Range 一律回 200 全量。
func (u *blobConcUpstream) setIgnoreRange() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ignoreRange = true
}

// rangesOf 返回收到的全部 Range 头快照（并发安全拷贝）。
func (u *blobConcUpstream) rangesOf() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.ranges...)
}

// hitsOf 返回进入 blob 处理的请求数。
func (u *blobConcUpstream) hitsOf() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

// hitsOfPath 返回某请求路径的命中次数。
func (u *blobConcUpstream) hitsOfPath(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.pathHits[path]
}

// rangesOfPath 返回某请求路径收到的 Range 头快照。
func (u *blobConcUpstream) rangesOfPath(path string) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.pathRanges[path]...)
}

// sentOf 返回累计成功写出的字节数（上游连接被关闭后停住）。
func (u *blobConcUpstream) sentOf() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.sent
}

// activeOf 返回当前活跃连接数（abortAll 逐路关闭全部上游源后应归零）。
func (u *blobConcUpstream) activeOf() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.active
}

// ServeHTTP 按 Range 头分流 206 / 200（语义见类型注释）。
func (u *blobConcUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.hits++
	u.active++
	data, chunk, delay, noLength, ignoreRange := u.data, u.chunk, u.delay, u.noLength, u.ignoreRange
	rng := r.Header.Get("Range")
	path := r.URL.Path
	u.pathHits[path]++
	if rng != "" {
		u.ranges = append(u.ranges, rng)
		u.pathRanges[path] = append(u.pathRanges[path], rng)
	}
	u.mu.Unlock()
	defer func() {
		u.mu.Lock()
		u.active--
		u.mu.Unlock()
	}()
	if len(data) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")

	if rng != "" && !ignoreRange {
		start, end := parseBytesRange(rng, len(data))
		if start < 0 || end < start {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		u.writeChunks(w, body, chunk, delay)
		return
	}
	if noLength {
		// 先写响应头再 Flush：Go 不会自动补 Content-Length（chunked），复现现场故障形态
		w.WriteHeader(http.StatusOK)
		hubFlush(w)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
	}
	u.writeChunks(w, data, chunk, delay)
}

// writeChunks 分块写出 body：逐块 Write + Flush，块间 sleep；代理关闭上游连接后 Write
// 失败即返回（sent 停在断开点，供断言宽限终止确实关闭了上游连接）。
func (u *blobConcUpstream) writeChunks(w http.ResponseWriter, body []byte, chunk int, delay time.Duration) {
	if chunk <= 0 {
		chunk = len(body)
	}
	if chunk <= 0 {
		return
	}
	for off := 0; off < len(body); off += chunk {
		end := off + chunk
		if end > len(body) {
			end = len(body)
		}
		if _, err := w.Write(body[off:end]); err != nil {
			return
		}
		u.mu.Lock()
		u.sent += int64(end - off)
		u.mu.Unlock()
		hubFlush(w)
		if delay > 0 && end < len(body) {
			time.Sleep(delay)
		}
	}
}

// blobRef manifest 里单个 blob 的 digest + size（测试侧 JSON 构造用）。
type blobRef struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

// blobConcManifestBody 构造单平台 image manifest 的解析结果（config + layers 的 digest/size）：
// 经 JSON 序列化→反序列化走生产同一条解析映射，保证 imageManifestBody 的字段标签被覆盖。
func blobConcManifestBody(t *testing.T, configDigest string, configSize int64, layers ...blobRef) imageManifestBody {
	t.Helper()
	m := struct {
		SchemaVersion int       `json:"schemaVersion"`
		MediaType     string    `json:"mediaType"`
		Config        blobRef   `json:"config"`
		Layers        []blobRef `json:"layers"`
	}{
		SchemaVersion: 2,
		MediaType:     prefetchCTSchema2,
		Config:        blobRef{Digest: configDigest, Size: configSize},
		Layers:        layers,
	}
	b, err := json.Marshal(&m)
	if err != nil {
		t.Fatalf("序列化测试用 manifest 失败: %v", err)
	}
	var body imageManifestBody
	if err := json.Unmarshal(b, &body); err != nil {
		t.Fatalf("解析测试用 manifest 失败: %v", err)
	}
	return body
}

// //////////////////  测试环境  //////////////////////////////////////////////////////////////////

// blobConcLog 并发安全的日志捕获缓冲：时间驱动的补源尝试（settleSlowResume goroutine）
// 与宽限计时器 goroutine 都会写日志，因此断言必须经本类型的锁读，避免与后台协程的
// bytes.Buffer 写入构成数据竞争。
type blobConcLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write 实现 io.Writer（供 slog.TextHandler 使用）。
func (l *blobConcLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// String 返回当前日志全文（并发安全快照）。
func (l *blobConcLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// blobConcFixture 聚合「规则配置 + 两个假上游（慢胜者 / 补充源）+ 独立缓存实例」的
// 全链路测试环境（ServeHTTP 驱动，与真实请求链路同构）。日志按 Debug 级捕获（宽限终止
// 的 abortAll 关闭源日志是 Debug 级，要断言「全部源被 abort」必须能拿到）。
type blobConcFixture struct {
	slow, fast       *blobConcUpstream
	slowSrv, fastSrv *httptest.Server
	s                *Server
	disk             *cache.Cache
	log              *blobConcLog
	cacheDir         string
}

// newBlobConcFixture 构建全链路测试环境：域名规则 type=docker、候选=[slow, fast]、
// 判速通道显式关闭（speed=0，只测时间驱动）、剩余量门槛 0、补充间隔缺省 1 分钟
// （用例经 mutate 覆盖）。docker-prefetch 显式关闭（隔离预取旁路干扰）。
// 注意：**不注入 blobDigestVerify stub**——本文件用例需要走生产默认的 sha256 强校验，
// 以证明「补充后写出的缓存条目内容寻址一致」。
func newBlobConcFixture(t *testing.T, mutate func(*config.Config)) *blobConcFixture {
	t.Helper()
	slow, fast := newBlobConcUpstream(nil), newBlobConcUpstream(nil)
	slowSrv := httptest.NewServer(slow)
	t.Cleanup(slowSrv.Close)
	fastSrv := httptest.NewServer(fast)
	t.Cleanup(fastSrv.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	prefetchOff := false
	cfg.DockerPrefetchEnabled = &prefetchOff
	templates := []config.URLRedirectItem{
		{Template: slowSrv.URL + "/$1"},
		{Template: fastSrv.URL + "/$1"},
	}
	cfg.DomainRules = []config.DomainRule{{
		Name:                     "blob-concurrency",
		Type:                     config.RuleTypeDocker,
		Match:                    `http://registry-proxy-cache\.linkease\.net:5480/`,
		URLRedirect:              &templates,
		SlowDownloadSpeed:        bytesPtr(0),         // 判速通道关闭：只测时间驱动
		SlowDownloadMinRemaining: bytesPtr(0),         // 剩余量门槛不挡补充
		SlowDownloadMaxBehind:    bytesPtr(1 << 20),   // 多源合并剪枝阈值
		BlobConcurrencyInterval:  durPtr(time.Minute), // 用例按需覆盖
	}}
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	log := &blobConcLog{}
	s, err := New(cfg, cache.NewManager(context.Background()),
		slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	disk, err := cache.New(cacheDir)
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}
	return &blobConcFixture{
		slow: slow, fast: fast, slowSrv: slowSrv, fastSrv: fastSrv,
		s: s, disk: disk, log: log, cacheDir: cacheDir,
	}
}

// newBlobConcDirectServer 构建「直驱 fetchViaCandidatesStreaming」用的 Server + 缓存实例：
// 默认配置 + 独立缓存目录（eff 由用例手工组装，可把补充间隔压到毫秒级，不必受 Validate
// 的 1s 下限钳制）。
func newBlobConcDirectServer(t *testing.T, log *blobConcLog) (*Server, *cache.Cache) {
	t.Helper()
	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, cache.NewManager(context.Background()),
		slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	disk, err := cache.New(cacheDir)
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}
	return s, disk
}

// blobConcBaseInput 构造一份「可补充」的 timeBlobSupplement 输入（各用例按需改写）：
// 单源阶段（merger=nil）、候选 = 胜者 + candURLs、已知总长 8192、已写 4096 字节、
// 剩余门槛 0（不挡补充）。
func blobConcBaseInput(now time.Time, winnerURL string, candURLs ...string) blobSupplementInput {
	cands := make([]candidate, 0, len(candURLs)+1)
	cands = append(cands, candidate{url: winnerURL})
	for _, u := range candURLs {
		cands = append(cands, candidate{url: u})
	}
	return blobSupplementInput{
		r:           httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/v2/"+prefetchName+"/blobs/"+prefetchDigestLayer1, nil),
		res:         &probeResult{url: winnerURL},
		labels:      dockerLogInfo{image: prefetchName, digest: prefetchDigestLayer1},
		logSource:   dockerLogSourceClient,
		eff:         config.Options{Rule: "blob-conc", RuleType: config.RuleTypeDocker, SlowDownloadMinRemaining: 0, SlowDownloadMaxBehind: 1 << 20, BlobConcurrencyInterval: time.Minute},
		candidates:  cands,
		round:       1,
		now:         now,
		startedAt:   now.Add(-10 * time.Minute),
		bytes:       4096,
		offset:      4096,
		knownTotal:  8192,
		totalSource: blobTotalSourceUpstream,
		end:         8191,
	}
}

// mustLogLine 返回日志中第一条同时包含全部 substr 的行（找不到即 fatal）。
// 复用 progress_summary_test.go 的 firstLogLine（无匹配返回空串）。
func mustLogLine(t *testing.T, log string, subs ...string) string {
	t.Helper()
	line := firstLogLine(log, subs...)
	if line == "" {
		t.Fatalf("日志中找不到同时包含 %v 的行，实际日志:\n%s", subs, log)
	}
	return line
}

// //////////////////  1：时间驱动判定节拍  //////////////////////////////////////////////////////

// TestBlobConcurrencyTrackDue 表驱动验证 blobConcurrencyTrack.due 的时间语义：
// 未到点 false；首次满 interval 返回 round=1；之后每满 interval 递增；读循环错过多个
// 时间点时只补一次判定（不补轮次），下一次判定时刻从「当前时刻」重新计时；nil 轨道与
// interval<=0 / enabled=false 都不建轨道（due 恒 false）。
func TestBlobConcurrencyTrackDue(t *testing.T) {
	base := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

	t.Run("nil轨道与关闭不建轨道", func(t *testing.T) {
		var nilTrack *blobConcurrencyTrack
		if round, due := nilTrack.due(base); due || round != 0 {
			t.Errorf("nil 轨道 due = (%d, %v), want (0, false)", round, due)
		}
		if tr := newBlobConcurrencyTrack(false, base, time.Minute); tr != nil {
			t.Errorf("enabled=false 应返回 nil 轨道，得到 %+v", tr)
		}
		for _, interval := range []time.Duration{0, -time.Second, -time.Hour} {
			if tr := newBlobConcurrencyTrack(true, base, interval); tr != nil {
				t.Errorf("interval=%s 应返回 nil 轨道（零开销关闭），得到 %+v", interval, tr)
			}
		}
		if tr := newBlobConcurrencyTrack(true, base, time.Minute); tr == nil {
			t.Fatal("enabled 且 interval>0 时应返回轨道")
		}
	})

	t.Run("到点轮次递增", func(t *testing.T) {
		tr := newBlobConcurrencyTrack(true, base, time.Minute)
		steps := []struct {
			name      string
			now       time.Time
			wantDue   bool
			wantRound int
		}{
			{"未到首轮", base.Add(59 * time.Second), false, 0},
			{"首次满间隔", base.Add(time.Minute), true, 1},
			{"未到第二轮", base.Add(119 * time.Second), false, 0},
			{"第二轮", base.Add(2 * time.Minute), true, 2},
			{"未到第三轮", base.Add(179 * time.Second), false, 0},
			{"第三轮", base.Add(3 * time.Minute), true, 3},
		}
		for _, st := range steps {
			round, due := tr.due(st.now)
			if due != st.wantDue || round != st.wantRound {
				t.Errorf("%s：due(%s) = (%d, %v), want (%d, %v)",
					st.name, st.now.Format(time.RFC3339), round, due, st.wantRound, st.wantDue)
			}
		}
	})

	t.Run("错过多个时间点只补一次并从now重新计时", func(t *testing.T) {
		tr := newBlobConcurrencyTrack(true, base, time.Minute)
		if round, due := tr.due(base.Add(time.Minute)); !due || round != 1 {
			t.Fatalf("首次到点应 (1, true)，得到 (%d, %v)", round, due)
		}
		// 读循环被上游停滞阻塞 9 分钟：错过第 2..9 轮，只补一次判定（round=2 而非 10）
		jumped := base.Add(10 * time.Minute)
		round, due := tr.due(jumped)
		if !due || round != 2 {
			t.Errorf("错过多个时间点应只补一次判定 round=2，得到 (%d, %v)", round, due)
		}
		// 下一轮的计时基线是「本次判定时刻」（now），不是按原节奏补齐
		if round, due := tr.due(jumped.Add(time.Minute - time.Nanosecond)); due || round != 0 {
			t.Errorf("从 now 重新计时：未满 interval 不应到点，得到 (%d, %v)", round, due)
		}
		if round, due := tr.due(jumped.Add(time.Minute)); !due || round != 3 {
			t.Errorf("从 now 起满 interval 应到点 round=3，得到 (%d, %v)", round, due)
		}
	})
}

// //////////////////  2：timeBlobSupplement 判定矩阵  //////////////////////////////////////////

// TestTimeBlobSupplementReasons 表驱动验证「未补充」原因矩阵：client_gone / candidates_lt_2 /
// content_length_unknown / all_joined / remaining_too_small 各自 created=false、合并器不被
// 创建（all_joined 保留调用方传入的合并器）、且**不发起任何补源请求**（假上游零回源）；
// 同时验证原因优先级（client_gone 高于 candidates_lt_2）。日志断言「未补充多源并发」+
// reason=<枚举> 与统一键。
func TestTimeBlobSupplementReasons(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*blobSupplementInput, string, string)
		wantReason    string
		wantMergerNil bool
	}{
		{
			name:          "client_gone_客户端已断开不补源",
			mutate:        func(in *blobSupplementInput, _, _ string) { in.clientGone = true },
			wantReason:    blobAddReasonClientGone,
			wantMergerNil: true,
		},
		{
			name: "client_gone_优先于candidates_lt_2",
			mutate: func(in *blobSupplementInput, _, _ string) {
				in.clientGone = true
				in.candidates = in.candidates[:1]
			},
			wantReason:    blobAddReasonClientGone,
			wantMergerNil: true,
		},
		{
			name:          "candidates_lt_2_只有一路源",
			mutate:        func(in *blobSupplementInput, _, _ string) { in.candidates = in.candidates[:1] },
			wantReason:    blobAddReasonCandidatesLT2,
			wantMergerNil: true,
		},
		{
			name: "content_length_unknown_总长不可知",
			mutate: func(in *blobSupplementInput, _, _ string) {
				in.knownTotal = 0
				in.totalSource = blobTotalSourceUnknown
			},
			wantReason:    blobAddReasonTotalUnknown,
			wantMergerNil: true,
		},
		{
			name: "all_joined_全部候选已在合并中",
			mutate: func(in *blobSupplementInput, winnerURL, candURL string) {
				in.merger = newSlowMerger([]*slowSource{
					{name: winnerURL, start: 4096, readPos: 4096},
					{name: candURL, start: 4096, readPos: 4096},
				}, 4096, 8192, 1<<20, quietLogger())
			},
			wantReason:    blobAddReasonAllJoined,
			wantMergerNil: false,
		},
		{
			name: "remaining_too_small_尾巴太小",
			mutate: func(in *blobSupplementInput, _, _ string) {
				in.offset = 8180 // 剩余 12 字节 < 门槛 64
				in.eff.SlowDownloadMinRemaining = 64
			},
			wantReason:    blobAddReasonRemainingSmall,
			wantMergerNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := newBlobConcUpstream(blobPattern(8192))
			srv := httptest.NewServer(up)
			t.Cleanup(srv.Close)

			log := &blobConcLog{}
			s, _ := newBlobConcDirectServer(t, log)

			winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
			candURL := srv.URL + "/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
			in := blobConcBaseInput(time.Now(), winnerURL, candURL)
			tt.mutate(&in, winnerURL, candURL)
			preMerger := in.merger

			if created := s.timeBlobSupplement(&in); created {
				t.Error("未补充原因命中时 timeBlobSupplement 应返回 created=false")
			}
			if tt.wantMergerNil && in.merger != nil {
				t.Errorf("未补充时不应创建合并器，得到 %+v", in.merger)
			}
			if !tt.wantMergerNil && in.merger != preMerger {
				t.Error("all_joined 场景应保留调用方传入的合并器（绝不替换）")
			}
			// 补源动作绝不发生：候选上游零回源、零 Range 请求
			if got := up.hitsOf(); got != 0 {
				t.Errorf("未补充场景不应发起补源请求，上游却收到 %d 次请求（Range=%v）", got, up.rangesOf())
			}
			line := mustLogLine(t, log.String(), "未补充多源并发")
			wantLineField(t, line, "reason="+tt.wantReason, "joined_sources=", "pending_sources=",
				"bytes=", "total=", "total_source=", "round=1")
			if tt.wantReason == blobAddReasonRemainingSmall {
				wantLineField(t, line, "remaining=12", "min_remaining=64")
			}
		})
	}
}

// TestTimeBlobSupplementCreatesMergerAndJoinsIncrementally 可补时的两阶段语义：
//   - 单源阶段（merger=nil）：created=true，合并器以胜者为首源、其余候选并发加入，
//     加入源的 Range 起点 == 入参 offset、尾界 == knownTotal-1；
//   - 合并阶段（merger 非 nil）：再调用 created=false 且**只增量加源**——已在合并中的
//     候选绝不被重复拉取（上游请求数不增），新候选以「合并游标」为起点加入。
func TestTimeBlobSupplementCreatesMergerAndJoinsIncrementally(t *testing.T) {
	up := newBlobConcUpstream(blobPattern(8192))
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	log := &blobConcLog{}
	s, _ := newBlobConcDirectServer(t, log)

	winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
	c2, c3 := srv.URL+"/c2", srv.URL+"/c3"
	now := time.Now()
	in := blobConcBaseInput(now, winnerURL, c2)

	// 单源阶段：首轮补充创建合并器
	if created := s.timeBlobSupplement(&in); !created {
		t.Fatal("可补场景单源阶段应返回 created=true（调用方据此转入合并循环）")
	}
	if in.merger == nil {
		t.Fatal("首轮补充后 in.merger 应被回填（胜者连接移交合并器）")
	}
	if got := in.merger.sourceCount(); got != 2 {
		t.Errorf("首轮补充后合并器源数 = %d, want 2（胜者 + 1 个补充源）", got)
	}
	if got := in.merger.winnerURL(); got != winnerURL {
		t.Errorf("合并器首源（胜者）= %q, want %q（绝不切换胜者）", got, winnerURL)
	}
	resumeWaitUntil(t, 3*time.Second, func() bool { return len(up.rangesOfPath("/c2")) > 0 }, "等待补充源收到 Range 续传请求超时")
	if rs := up.rangesOfPath("/c2"); len(rs) != 1 || rs[0] != "bytes=4096-8191" {
		t.Errorf("补充源 Range 头 = %v, want [bytes=4096-8191]（起点=入参 offset，尾界=总长-1）", rs)
	}
	hitsAfterFirst := up.hitsOf()

	// 合并阶段：同一候选集合再判定 —— 全部已在合并中，只做加法、不重复拉取
	in.round = 2
	in.now = now.Add(time.Minute)
	in.offset = in.merger.cursor()
	if created := s.timeBlobSupplement(&in); created {
		t.Error("合并阶段应返回 created=false（不重建合并器）")
	}
	if got := in.merger.sourceCount(); got != 2 {
		t.Errorf("合并阶段重复判定后源数 = %d, want 2（绝不重复加入同一候选）", got)
	}
	if got := up.hitsOf(); got != hitsAfterFirst {
		t.Errorf("已在合并中的候选被重复拉取：上游请求数 %d → %d", hitsAfterFirst, got)
	}

	// 合并阶段：新增一个尚未加入的候选 —— 只增量加源，起点 = 合并游标。
	// 模拟合并循环已按序写出一段（游标从 4096 推进到 5000）：新源必须以新游标为起点，
	// 绝不重复拉取已写给客户端的区间。
	in.merger.mu.Lock()
	in.merger.next = 5000
	in.merger.mu.Unlock()
	in.candidates = append(in.candidates, candidate{url: c3})
	in.round = 3
	in.offset = in.merger.cursor()
	if created := s.timeBlobSupplement(&in); created {
		t.Error("合并阶段增量加源仍应返回 created=false")
	}
	if got := in.merger.sourceCount(); got != 3 {
		t.Errorf("增量加源后源数 = %d, want 3", got)
	}
	wantRange := fmt.Sprintf("bytes=%d-8191", in.merger.cursor())
	resumeWaitUntil(t, 3*time.Second, func() bool {
		return len(up.rangesOfPath("/c3")) > 0
	}, "等待新候选以合并游标为起点收到 Range 请求超时（want "+wantRange+"）")
	if rs := up.rangesOfPath("/c3"); len(rs) != 1 || rs[0] != wantRange {
		t.Errorf("新候选 Range 头 = %v, want [%q]（起点 = 当时合并游标）", rs, wantRange)
	}
	if got := up.hitsOfPath("/c2"); got != 1 {
		t.Errorf("已在合并中的 c2 被重复拉取：请求数 = %d, want 1", got)
	}
	if got := up.hitsOf(); got != 2 {
		t.Errorf("增量加源后上游请求总数 = %d, want 2（c2 一次、c3 一次，绝不重复）", got)
	}
	wantLineField(t, mustLogLine(t, log.String(), "补充多源并发下载"), "sources=1", "joined_sources=", "alive_sources=", "offset=", "total=8192")
}

// //////////////////  3：joinSlowSources 去重与游标  ////////////////////////////////////////////

// TestJoinSlowSourcesDedupAndCursor 白盒验证 joinSlowSources 的「只做加法」语义：
// 胜者 / 已在合并中的候选 / 已剪枝的候选都不重复加入（也不重置其状态），只有新候选以
// 调用方给的 offset 为 Range 起点并发加入；日志的 added / sources / joined_sources /
// alive_sources / offset / total 字段齐全。
func TestJoinSlowSourcesDedupAndCursor(t *testing.T) {
	up := newBlobConcUpstream(blobPattern(8192))
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	log := &blobConcLog{}
	s, _ := newBlobConcDirectServer(t, log)

	winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
	joinedURL := srv.URL + "/joined"
	prunedURL := srv.URL + "/pruned"
	freshURL := srv.URL + "/fresh"
	const cursor, knownTotal = int64(1234), int64(8192)

	winner := &slowSource{name: winnerURL, start: 0, readPos: 0}
	joined := &slowSource{name: joinedURL, start: 0, readPos: 0}
	pruned := &slowSource{name: prunedURL, start: 0, readPos: 0, pruned: "behind: 落后领先源 9999999 字节"}
	m := newSlowMerger([]*slowSource{winner, joined, pruned}, cursor, knownTotal, 1<<20, quietLogger())

	cands := []candidate{{url: winnerURL}, {url: joinedURL}, {url: prunedURL}, {url: freshURL}}
	added := s.joinSlowSources(
		httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/v2/"+prefetchName+"/blobs/"+prefetchDigestLayer1, nil),
		m, cands, config.Options{Rule: "blob-conc", RuleType: config.RuleTypeDocker, SlowDownloadMaxBehind: 1 << 20},
		cursor, knownTotal-1,
		dockerLogInfo{image: prefetchName, digest: prefetchDigestLayer1}, dockerLogSourceClient,
		"total_source", blobTotalSourceUpstream, "elapsed", "10m0s")

	if len(added) != 1 || added[0] != freshURL {
		t.Errorf("本次真正加入的源 = %v, want [%q]（胜者/已加入/已剪枝均须去重）", added, freshURL)
	}
	if got := m.sourceCount(); got != 4 {
		t.Errorf("加入后源总数 = %d, want 4（胜者 + 已在合并 + 已剪枝 + 新加入）", got)
	}
	// 已加入的源状态不被重置（去重是「跳过」，不是「重新加入」）
	m.mu.Lock()
	joinedStart, joinedReadPos, prunedReason := joined.start, joined.readPos, pruned.pruned
	var freshStart, freshReadPos int64
	var freshPending bool
	for _, src := range m.sources {
		if src.name == freshURL {
			freshStart, freshReadPos, freshPending = src.start, src.readPos, src.pending
		}
	}
	m.mu.Unlock()
	if joinedStart != 0 || joinedReadPos != 0 {
		t.Errorf("已在合并中的源被重置：start=%d readPos=%d, want 0/0", joinedStart, joinedReadPos)
	}
	if prunedReason == "" {
		t.Error("已剪枝源的剪枝标记应保持（去重依据）")
	}
	if freshStart != cursor || freshReadPos != cursor {
		t.Errorf("新加入源起点 = (%d, %d), want (%d, %d)（起点 = 合并游标，绝不重复拉已送区间）",
			freshStart, freshReadPos, cursor, cursor)
	}
	if !freshPending {
		t.Error("新加入源在续传尝试定局前应保持 pending 占位")
	}
	resumeWaitUntil(t, 3*time.Second, func() bool { return len(up.rangesOf()) > 0 }, "等待新加入源收到 Range 请求超时")
	wantRange := fmt.Sprintf("bytes=%d-%d", cursor, knownTotal-1)
	if rs := up.rangesOf(); len(rs) != 1 || rs[0] != wantRange {
		t.Errorf("新加入源 Range 头 = %v, want [%q]", rs, wantRange)
	}
	if got := up.hitsOf(); got != 1 {
		t.Errorf("去重后上游应只收到 1 次请求（仅新候选），得到 %d", got)
	}
	line := mustLogLine(t, log.String(), "补充多源并发下载")
	wantLineField(t, line, "added="+freshURL, "sources=1", "joined_sources=4",
		"alive_sources=3", "offset=1234", "total=8192", "total_source=upstream", "elapsed=10m0s")
}

// //////////////////  4：客户端主路径（stream.go）时间驱动  ////////////////////////////////////

// TestBlobConcurrencyClientPathTimeDriven 直驱 fetchViaCandidatesStreaming 验证客户端主路径
// 的时间驱动补充（毫秒级 interval，避免 Validate 的 1s 下限拖长用例）：
//   - 可补充：胜者 200 **无 Content-Length**（现场故障形态）+ manifest 记录了 layer size
//     → 满 interval 后向第二候选并发发起 Range 续传，客户端收到与原文逐字节一致的完整
//     body、finalize entry 完整；写缓存条目完整（生产默认 sha256 强校验通过）；
//   - 未到 interval：整条流在 interval 前就下完 → 补充绝不发生（候选零请求、无评估日志）。
func TestBlobConcurrencyClientPathTimeDriven(t *testing.T) {
	t.Run("满间隔后补充快源_客户端数据与缓存条目完整", func(t *testing.T) {
		content := blobPattern(2 << 20) // 2MB
		digest := realDigestOf(content) // 真实摘要：写缓存侧走生产默认 sha256 强校验

		slow := newBlobConcUpstream(content)
		slow.setNoContentLength()                  // 200 无 Content-Length（chunked）
		slow.setChunk(16<<10, 40*time.Millisecond) // 128 块 × 40ms：留出充足补充窗口
		slowSrv := httptest.NewServer(slow)
		t.Cleanup(slowSrv.Close)
		fast := newBlobConcUpstream(content) // 快源：Range 立即整段返回
		fastSrv := httptest.NewServer(fast)
		t.Cleanup(fastSrv.Close)

		log := &blobConcLog{}
		s, disk := newBlobConcDirectServer(t, log)
		// manifest 记录 layer size：上游 200 无 Content-Length 时的 knownTotal 兜底
		s.recordManifestBlobSizes(blobConcManifestBody(t, prefetchDigestConfig, 1024, blobRef{Digest: digest, Size: int64(len(content))}))
		if got, ok := s.manifestBlobSize(digest); !ok || got != int64(len(content)) {
			t.Fatalf("manifest size 索引 = (%d, %v), want (%d, true)", got, ok, len(content))
		}

		target := "v2/" + prefetchName + "/blobs/" + digest
		key := prefetchBlobKey(prefetchName, digest)
		eff := config.Options{
			URLRedirect: []config.URLRedirectItem{
				{Template: slowSrv.URL + "/$1"},
				{Template: fastSrv.URL + "/$1"},
			},
			SuccessCheck:             config.SuccessCheckContent,
			Rule:                     "blob-conc",
			RuleType:                 config.RuleTypeDocker,
			SlowDownloadMinRemaining: 0,
			SlowDownloadMaxBehind:    1 << 20,
			BlobConcurrencyInterval:  50 * time.Millisecond,
		}
		r := prefetchBlobTestRequest(prefetchName, digest)
		var finEntry *cache.Entry
		finalize := func(e *cache.Entry) {
			finEntry = e
			s.finalizeFetchEntry(r, key, target, e, eff, disk, dockerLogSourceClient)
		}

		rec := httptest.NewRecorder()
		entry, committed, err := s.fetchViaCandidatesStreaming(rec, r, target, eff, "MISS", finalize, time.Now())
		if err != nil {
			t.Fatalf("时间驱动补充后应完整返回: %v", err)
		}
		if !committed {
			t.Error("流式回源应已 commit（响应头已写出）")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("Code = %d, want %d（补充绝不改变已 commit 的状态码）", rec.Code, http.StatusOK)
		}
		if !bytes.Equal(rec.Body.Bytes(), content) {
			t.Errorf("客户端应收到与原文逐字节一致的完整 body（got %d 字节, want %d）", rec.Body.Len(), len(content))
		}
		if entry == nil || finEntry == nil {
			t.Fatal("200 场景 finalize 应收到完整 entry")
		}
		if !bytes.Equal(finEntry.Body, content) {
			t.Errorf("finalize entry body 应与原文一致（got %d 字节, want %d）", len(finEntry.Body), len(content))
		}
		// 缓存条目完整：生产默认 sha256 强校验已通过（摘要不符会被 finalizeFetchEntry 拒绝）
		cached, ok := disk.Get(key)
		if !ok {
			t.Fatal("补充完成后缓存应命中，disk.Get 未命中（摘要校验拒绝或未写盘）")
		}
		if !bytes.Equal(cached.Body, content) || realDigestOf(cached.Body) != digest {
			t.Errorf("缓存条目不完整或摘要不符（got %d 字节, 摘要 %s, want %d 字节, 摘要 %s）",
				len(cached.Body), realDigestOf(cached.Body), len(content), digest)
		}
		// 快源恰好一路 Range 续传，起点在流中段、尾界 = 总长-1
		rs := fast.rangesOf()
		if len(rs) != 1 {
			t.Fatalf("快源收到的 Range 头 = %v, want 恰好 1 条", rs)
		}
		start, end := parseBytesRange(rs[0], len(content))
		if start <= 0 || start >= len(content) || end != len(content)-1 {
			t.Errorf("补充 Range = %q，应形如 bytes=N-%d 且 0<N<%d（起点=当前进度，绝不回退）",
				rs[0], len(content)-1, len(content))
		}
		if got := slow.hitsOf(); got != 1 {
			t.Errorf("胜者应保持既有连接（1 次请求），得到 %d", got)
		}
		logText := log.String()
		wantLineField(t, mustLogLine(t, logText, "docker blob 单层下载开始"), "total_source=manifest", "candidates=2", "sources=1")
		wantLineField(t, mustLogLine(t, logText, "评估多源并发补充"), "total_source=manifest", "joined_sources=1", "pending_sources=1")
		wantLineField(t, mustLogLine(t, logText, "补充多源并发下载"), "sources=1", "joined_sources=2", "total=2097152")
	})

	t.Run("未满间隔不触发", func(t *testing.T) {
		content := blobPattern(64 << 10) // 64KB
		digest := realDigestOf(content)
		slow := newBlobConcUpstream(content)
		slow.setChunk(16<<10, time.Millisecond) // 4 块：几十毫秒内必下完（远小于 1s 间隔）
		slowSrv := httptest.NewServer(slow)
		t.Cleanup(slowSrv.Close)
		fast := newBlobConcUpstream(content)
		fastSrv := httptest.NewServer(fast)
		t.Cleanup(fastSrv.Close)

		log := &blobConcLog{}
		s, disk := newBlobConcDirectServer(t, log)
		s.recordManifestBlobSizes(blobConcManifestBody(t, prefetchDigestConfig, 1024, blobRef{Digest: digest, Size: int64(len(content))}))

		target := "v2/" + prefetchName + "/blobs/" + digest
		key := prefetchBlobKey(prefetchName, digest)
		eff := config.Options{
			URLRedirect: []config.URLRedirectItem{
				{Template: slowSrv.URL + "/$1"},
				{Template: fastSrv.URL + "/$1"},
			},
			SuccessCheck:             config.SuccessCheckContent,
			Rule:                     "blob-conc",
			RuleType:                 config.RuleTypeDocker,
			SlowDownloadMinRemaining: 0,
			SlowDownloadMaxBehind:    1 << 20,
			BlobConcurrencyInterval:  time.Second, // 流在间隔前完成
		}
		r := prefetchBlobTestRequest(prefetchName, digest)
		var finEntry *cache.Entry
		finalize := func(e *cache.Entry) {
			finEntry = e
			s.finalizeFetchEntry(r, key, target, e, eff, disk, dockerLogSourceClient)
		}
		rec := httptest.NewRecorder()
		if _, _, err := s.fetchViaCandidatesStreaming(rec, r, target, eff, "MISS", finalize, time.Now()); err != nil {
			t.Fatalf("未到间隔的单源流应完整返回: %v", err)
		}
		if !bytes.Equal(rec.Body.Bytes(), content) {
			t.Errorf("客户端应收到完整 body（got %d 字节, want %d）", rec.Body.Len(), len(content))
		}
		if finEntry == nil || !bytes.Equal(finEntry.Body, content) {
			t.Error("单源完成的 finalize entry 应为完整 body")
		}
		if got := fast.hitsOf(); got != 0 {
			t.Errorf("未满间隔不应补充源，第二候选却收到 %d 次请求（Range=%v）", got, fast.rangesOf())
		}
		if strings.Contains(log.String(), "评估多源并发补充") {
			t.Errorf("未满间隔不应输出评估日志，实际:\n%s", log.String())
		}
		if strings.Contains(log.String(), "补充多源并发下载") {
			t.Errorf("未满间隔不应输出补充日志，实际:\n%s", log.String())
		}
	})
}

// TestBlobConcurrencyClientPathFullChain ServeHTTP 全链路（规则走 Validate，间隔取合法
// 最小 1s）：胜者 200 无 Content-Length + manifest size 兜底 → 满 1s 补充快源，
// 客户端收到完整 body 且写缓存（生产默认 sha256 强校验通过），第二次 GET 命中 HIT 不再回源。
func TestBlobConcurrencyClientPathFullChain(t *testing.T) {
	content := blobPattern(2 << 20) // 2MB：慢源 64 块 × 20ms ≈ 1.28s > 1s 间隔
	digest := realDigestOf(content)
	fx := newBlobConcFixture(t, func(cfg *config.Config) {
		cfg.DomainRules[0].BlobConcurrencyInterval = durPtr(time.Second)
	})
	fx.slow.setData(content)
	fx.slow.setNoContentLength()
	fx.slow.setChunk(32<<10, 20*time.Millisecond)
	fx.fast.setData(content)
	fx.s.recordManifestBlobSizes(blobConcManifestBody(t, prefetchDigestConfig, 1024, blobRef{Digest: digest, Size: int64(len(content))}))

	req := prefetchBlobTestRequest(prefetchName, digest)
	first := httptest.NewRecorder()
	fx.s.ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d", first.Code, http.StatusOK)
	}
	if got := first.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("首次 X-Cache = %q, want MISS", got)
	}
	if !bytes.Equal(first.Body.Bytes(), content) {
		t.Errorf("客户端应收到与原文一致的完整 body（got %d 字节, want %d）", first.Body.Len(), len(content))
	}
	// 缓存条目完整（真实 sha256 强校验通过后落盘）
	key := prefetchBlobKey(prefetchName, digest)
	cached, ok := fx.disk.Get(key)
	if !ok {
		t.Fatalf("补充完成后缓存应命中，disk.Get 未命中（摘要校验拒绝或未写盘），日志:\n%s", fx.log.String())
	}
	if !bytes.Equal(cached.Body, content) || realDigestOf(cached.Body) != digest {
		t.Errorf("缓存条目不完整或摘要不符（got %d 字节, 摘要 %s, want %d 字节）",
			len(cached.Body), realDigestOf(cached.Body), len(content))
	}
	rs := fx.fast.rangesOf()
	if len(rs) != 1 {
		t.Fatalf("补充源收到的 Range 头 = %v, want 恰好 1 条", rs)
	}
	if start, end := parseBytesRange(rs[0], len(content)); start <= 0 || end != len(content)-1 {
		t.Errorf("补充 Range = %q，应形如 bytes=N-%d 且 N>0", rs[0], len(content)-1)
	}
	// 二次 GET：缓存命中（HIT）且不再回源
	slowHitsBefore := fx.slow.hitsOf()
	second := httptest.NewRecorder()
	fx.s.ServeHTTP(second, req)
	if got := second.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("二次 X-Cache = %q, want HIT", got)
	}
	if !bytes.Equal(second.Body.Bytes(), content) {
		t.Errorf("二次命中 body 应与原文一致（got %d 字节, want %d）", second.Body.Len(), len(content))
	}
	if got := fx.slow.hitsOf(); got != slowHitsBefore {
		t.Errorf("二次 GET 命中缓存后不应再回源，上游请求数 %d → %d", slowHitsBefore, got)
	}
	logText := fx.log.String()
	wantLineField(t, mustLogLine(t, logText, "docker blob 单层下载开始"), "total_source=manifest", "source=client")
	wantLineField(t, mustLogLine(t, logText, "评估多源并发补充"), "total_source=manifest", "source=client")
	wantLineField(t, mustLogLine(t, logText, "补充多源并发下载"), "total_source=manifest", "sources=1")
}

// //////////////////  5：续传执行方（partial_resume.go）时间驱动  ////////////////////////////////

// TestBlobConcurrencyResumePathFullChain 续传执行方的时间驱动补充（ServeHTTP 全链路）：
// 缓存里已有半截前缀 → 续传执行方向慢候选发 Range 续传；满 1s 后向第二（快）候选补充
// 并发 Range 续传 → 客户端收到完整 200（本地前缀 + 合并剩余）、半截删除、写缓存条目
// 完整（生产默认 sha256 强校验通过）；补充源 Range 起点 = 当时的合并进度（> 半截长度）。
func TestBlobConcurrencyResumePathFullChain(t *testing.T) {
	content := blobPattern(1 << 20) // 1MB
	digest := realDigestOf(content)
	half := 512 << 10 // 半截前缀 512KB；剩余 512KB 慢源 32 块 × 60ms ≈ 1.9s > 1s 间隔
	fx := newBlobConcFixture(t, func(cfg *config.Config) {
		cfg.DomainRules[0].BlobConcurrencyInterval = durPtr(time.Second)
	})
	fx.slow.setData(content)
	fx.slow.setChunk(16<<10, 60*time.Millisecond) // 续传胜者：慢源
	fx.fast.setData(content)                      // 补充源：快源（整段立即返回）

	key := prefetchBlobKey(prefetchName, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(prefetchName, digest))
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（续传对客户端回一次完整 200）, body: %.100s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("X-Cache = %q, want %q（走续传执行方）", got, resumeCacheStatus)
	}
	if !bytes.Equal(w.Body.Bytes(), content) {
		t.Errorf("客户端应收到完整 body（got %d 字节, want %d）——半截前缀 + 合并剩余应恰好拼出完整 blob",
			w.Body.Len(), len(content))
	}
	// 补充源：恰好一路 Range 续传，起点 > 半截长度（= 当时的合并进度，绝不重复拉已送区间）
	rs := fx.fast.rangesOf()
	if len(rs) != 1 {
		t.Fatalf("补充源收到的 Range 头 = %v, want 恰好 1 条", rs)
	}
	start, end := parseBytesRange(rs[0], len(content))
	if start <= half || start >= len(content) || end != len(content)-1 {
		t.Errorf("补充 Range = %q，应形如 bytes=N-%d 且 %d<N<%d（起点=合并游标）",
			rs[0], len(content)-1, half, len(content))
	}
	// 生命周期：正式缓存落盘后半截删除；缓存条目完整（sha256 校验通过）
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("续传完成后半截应已删除, stat err = %v", err)
	}
	cached, ok := fx.disk.Get(key)
	if !ok {
		t.Fatalf("续传完成后缓存应命中，disk.Get 未命中，日志:\n%s", fx.log.String())
	}
	if !bytes.Equal(cached.Body, content) || realDigestOf(cached.Body) != digest {
		t.Errorf("缓存条目不完整或摘要不符（got %d 字节, 摘要 %s, want %d 字节, 摘要 %s）",
			len(cached.Body), realDigestOf(cached.Body), len(content), digest)
	}
	logText := fx.log.String()
	wantLineField(t, mustLogLine(t, logText, "blob 半截续传开始"), "source=resume", "part_bytes=524288")
	wantLineField(t, mustLogLine(t, logText, "评估多源并发补充"), "source=resume", "total_source=upstream")
	wantLineField(t, mustLogLine(t, logText, "补充多源并发下载"), "source=resume", "sources=1")
}

// blobConcGatedWriter 可控断开的客户端 ResponseWriter：failFromNow 之前的 Write 正常写入
// recorder（响应头照常 commit），之后一律返回错误——用于「先让时间驱动补充把合并器建起来、
// 再让客户端断开」，精确驱动续传执行方的宽限终止出口（宽限到期 → 合并器全部源被 abort）。
type blobConcGatedWriter struct {
	rec *httptest.ResponseRecorder
	mu  sync.Mutex
	// failing true = 客户端已断开（Write 返回错误）
	failing bool
	// writeErrs 断开后客户端侧 Write 尝试次数
	writeErrs int
}

// newBlobConcGatedWriter 构建「先成功、后断开」的客户端写入器。
func newBlobConcGatedWriter() *blobConcGatedWriter {
	return &blobConcGatedWriter{rec: httptest.NewRecorder()}
}

// Header 实现 http.ResponseWriter。
func (w *blobConcGatedWriter) Header() http.Header { return w.rec.Header() }

// WriteHeader 实现 http.ResponseWriter（commit 点照常记录）。
func (w *blobConcGatedWriter) WriteHeader(code int) { w.rec.WriteHeader(code) }

// Write 实现 http.ResponseWriter：断开后一律报错（与真实 broken pipe 同一感知机制）。
func (w *blobConcGatedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	failing := w.failing
	if failing {
		w.writeErrs++
	}
	w.mu.Unlock()
	if failing {
		return 0, errors.New("模拟客户端断开（broken pipe）")
	}
	return w.rec.Write(p)
}

// failFromNow 让此后所有 Write 失败（模拟客户端断开）。
func (w *blobConcGatedWriter) failFromNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failing = true
}

// writeErrCount 返回断开后客户端侧的 Write 尝试次数。
func (w *blobConcGatedWriter) writeErrCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeErrs
}

// TestBlobConcurrencyResumeGraceAbortsAllSources 续传执行方的宽限终止出口与时间驱动补充
// 的配合：合并器（胜者 + 补充源，共 2 路慢源）已建立后客户端断开且无搭车订阅者 →
// 宽限到期 abortAll 逐路关闭**全部**上游源（两路都留 Debug 关闭痕、连接归零、写出停住），
// 本次响应被截断（半截保留、进度不回退、绝不写缓存）。
func TestBlobConcurrencyResumeGraceAbortsAllSources(t *testing.T) {
	content := blobPattern(4 << 20) // 4MB：宽限到期前远下不完（两路慢源合计约 0.55MB/s）
	digest := realDigestOf(content)
	half := 512 << 10
	fx := newBlobConcFixture(t, func(cfg *config.Config) {
		cfg.DomainRules[0].BlobConcurrencyInterval = durPtr(time.Second)
		cfg.DockerResumeClientGoneGrace = durPtr(1200 * time.Millisecond)
	})
	fx.slow.setData(content)
	fx.slow.setChunk(16<<10, 60*time.Millisecond)
	fx.fast.setData(content)
	fx.fast.setChunk(16<<10, 60*time.Millisecond) // 补充源同样慢：宽限到期前下不完

	key := prefetchBlobKey(prefetchName, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	gw := newBlobConcGatedWriter()
	served := make(chan struct{})
	go func() {
		defer close(served)
		fx.s.ServeHTTP(gw, prefetchBlobTestRequest(prefetchName, digest))
	}()

	// 等时间驱动补充真正把第二路源加进合并器（此刻合并器与 graceMerger 均已登记）
	resumeWaitUntil(t, 8*time.Second, func() bool { return len(fx.fast.rangesOf()) > 0 },
		"等待时间驱动补充加入第二路源超时（合并器未建立则宽限终止无法覆盖多源）")
	// 客户端从此刻断开：无搭车订阅者 → 宽限计时开始
	gw.failFromNow()
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("等待续传宽限终止收场超时（上游源可能未被 abort 关闭）")
	}

	if gw.rec.Code != http.StatusOK {
		t.Errorf("Code = %d, want %d（断开发生在 commit 之后，状态码绝不改变）", gw.rec.Code, http.StatusOK)
	}
	if got := gw.rec.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("X-Cache = %q, want %q", got, resumeCacheStatus)
	}
	logText := fx.log.String()
	wantLineField(t, mustLogLine(t, logText, "写响应体失败（客户端已断开，续传按宽限语义处置）"),
		"source=resume", "grace=1.2s")
	wantLineField(t, mustLogLine(t, logText, "宽限终止续传"), "source=resume", "part_bytes=")
	// 合并器内全部源（胜者 + 补充源）都被 abortAll 逐路关闭：两路各一条 Debug 关闭痕
	abortLines := logLinesOf(logText, "慢速升级多源续传源已关闭")
	reason := "续传客户端断开且宽限期内无搭车订阅者，终止全部上游源"
	aborted := 0
	for _, line := range abortLines {
		if strings.Contains(line, reason) {
			aborted++
		}
	}
	if aborted != 2 {
		t.Fatalf("宽限终止应 abort 合并器内全部 2 路源，实际 %d 条 abort 日志（全部关闭日志 %v）", aborted, abortLines)
	}
	// 两路上游连接都被关闭：handler 写失败退出，活跃连接归零、写出停在断开点
	resumeWaitUntil(t, 3*time.Second, func() bool {
		return fx.slow.activeOf() == 0 && fx.fast.activeOf() == 0
	}, "宽限终止后两路上游连接应全部关闭（活跃连接归零）")
	if got := fx.slow.sentOf(); got >= int64(len(content)) {
		t.Errorf("胜者写出 %d 字节（应停在断开点，远小于总长 %d）", got, len(content))
	}
	if got := fx.fast.sentOf(); got >= int64(len(content)) {
		t.Errorf("补充源写出 %d 字节（应停在断开点，远小于总长 %d）", got, len(content))
	}
	if got := gw.writeErrCount(); got == 0 {
		t.Error("客户端断开后应至少有一次写失败（宽限语义处置的触发点）")
	}
	// 半截保留（进度不回退）、绝不写缓存
	part := partBytes(t, fx.disk.PartPath(key))
	if int64(len(part)) < int64(half) {
		t.Errorf("宽限终止后半截应保留至少 %d 字节（进度不回退），实际 %d", half, len(part))
	}
	if _, ok := fx.disk.Get(key); ok {
		t.Error("宽限终止（客户端已断开、流未完成）绝不应写缓存")
	}
}

// //////////////////  6：日志契约  //////////////////////////////////////////////////////////////

// TestBlobConcurrencyLogContract 时间驱动补充的日志契约：
//   - reason / total_source 枚举常量值固定（运维按值 grep / 聚合）；
//   - blobConcurrencyOffReason 的 disabled / not_docker_rule 判定；
//   - 评估 / 补充 / 未补充三类日志的键存在（sources 相关键、total_source、offset 等）；
//   - 源不支持 Range（对续传请求回 200）时真实留痕 reason=upstream_no_range。
func TestBlobConcurrencyLogContract(t *testing.T) {
	t.Run("reason与total_source枚举常量", func(t *testing.T) {
		cases := []struct {
			name string
			got  string
			want string
		}{
			{"disabled", blobAddReasonDisabled, "disabled"},
			{"not_docker_rule", blobAddReasonNotDockerRule, "not_docker_rule"},
			{"content_length_unknown", blobAddReasonTotalUnknown, "content_length_unknown"},
			{"candidates_lt_2", blobAddReasonCandidatesLT2, "candidates_lt_2"},
			{"all_joined", blobAddReasonAllJoined, "all_joined"},
			{"client_gone", blobAddReasonClientGone, "client_gone"},
			{"upstream_no_range", blobAddReasonNoRange, "upstream_no_range"},
			{"remaining_too_small", blobAddReasonRemainingSmall, "remaining_too_small"},
			{"total_source_upstream", blobTotalSourceUpstream, "upstream"},
			{"total_source_manifest", blobTotalSourceManifest, "manifest"},
			{"total_source_unknown", blobTotalSourceUnknown, "unknown"},
		}
		for _, tt := range cases {
			if tt.got != tt.want {
				t.Errorf("%s 常量 = %q, want %q（运维 grep 契约值）", tt.name, tt.got, tt.want)
			}
		}
	})

	t.Run("未启用原因blobConcurrencyOffReason", func(t *testing.T) {
		cases := []struct {
			name string
			eff  config.Options
			want string
		}{
			{"非docker规则", config.Options{RuleType: config.RuleTypeWeb, BlobConcurrencyInterval: time.Minute, SlowDownloadSpeed: 1}, blobAddReasonNotDockerRule},
			{"docker但时间与判速都关", config.Options{RuleType: config.RuleTypeDocker, BlobConcurrencyInterval: 0, SlowDownloadSpeed: 0}, blobAddReasonDisabled},
			{"docker时间关但判速仍开", config.Options{RuleType: config.RuleTypeDocker, BlobConcurrencyInterval: 0, SlowDownloadSpeed: 1 << 20}, ""},
			{"docker时间开", config.Options{RuleType: config.RuleTypeDocker, BlobConcurrencyInterval: time.Minute}, ""},
		}
		for _, tt := range cases {
			if got := blobConcurrencyOffReason(tt.eff); got != tt.want {
				t.Errorf("%s：blobConcurrencyOffReason = %q, want %q", tt.name, got, tt.want)
			}
		}
	})

	t.Run("评估与补充日志键", func(t *testing.T) {
		up := newBlobConcUpstream(blobPattern(8192))
		srv := httptest.NewServer(up)
		t.Cleanup(srv.Close)
		log := &blobConcLog{}
		s, _ := newBlobConcDirectServer(t, log)

		winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
		in := blobConcBaseInput(time.Now(), winnerURL, srv.URL+"/c2")
		if created := s.timeBlobSupplement(&in); !created {
			t.Fatal("可补场景应创建合并器（created=true）")
		}
		logText := log.String()
		wantLineField(t, mustLogLine(t, logText, "评估多源并发补充"),
			"source=client", "image="+prefetchName, "digest="+prefetchDigestLayer1,
			"rule=blob-conc", "round=1", "elapsed=", "bytes=4096", "total=8192",
			"total_source=upstream", "rate=", "joined_sources=1", "pending_sources=1")
		wantLineField(t, mustLogLine(t, logText, "补充多源并发下载"),
			"rule=blob-conc", "added="+srv.URL+"/c2", "sources=1", "joined_sources=2",
			"alive_sources=2", "offset=4096", "total=8192", "total_source=upstream", "elapsed=")
	})

	t.Run("未补充日志键", func(t *testing.T) {
		log := &blobConcLog{}
		s, _ := newBlobConcDirectServer(t, log)
		winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
		in := blobConcBaseInput(time.Now(), winnerURL) // 仅胜者一路：candidates_lt_2
		if created := s.timeBlobSupplement(&in); created {
			t.Fatal("候选不足时不应创建合并器")
		}
		line := mustLogLine(t, log.String(), "未补充多源并发")
		wantLineField(t, line, "source=client", "image="+prefetchName, "digest="+prefetchDigestLayer1,
			"rule=blob-conc", "candidate="+winnerURL, "round=1", "reason="+blobAddReasonCandidatesLT2,
			"joined_sources=1", "pending_sources=0", "bytes=4096", "total=8192", "total_source=upstream")
	})

	t.Run("源不支持Range时留痕upstream_no_range", func(t *testing.T) {
		up := newBlobConcUpstream(blobPattern(8192))
		up.setIgnoreRange() // 对 Range 回 200 全量：该源被跳过
		srv := httptest.NewServer(up)
		t.Cleanup(srv.Close)
		log := &blobConcLog{}
		s, _ := newBlobConcDirectServer(t, log)

		winnerURL := "http://winner.invalid/v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
		m := newSlowMerger([]*slowSource{{name: winnerURL, start: 4096, readPos: 4096}}, 4096, 8192, 1<<20, quietLogger())
		s.joinSlowSources(
			httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/v2/"+prefetchName+"/blobs/"+prefetchDigestLayer1, nil),
			m, []candidate{{url: srv.URL + "/no-range"}},
			config.Options{Rule: "blob-conc", RuleType: config.RuleTypeDocker},
			4096, 8191,
			dockerLogInfo{image: prefetchName, digest: prefetchDigestLayer1}, dockerLogSourceClient)

		resumeWaitUntil(t, 3*time.Second, func() bool {
			return len(logLinesOf(log.String(), "reason="+blobAddReasonNoRange)) > 0
		}, "上游对 Range 回 200 时应留痕 reason=upstream_no_range，实际日志:\n"+log.String())
		line := mustLogLine(t, log.String(), "reason="+blobAddReasonNoRange)
		wantLineField(t, line, "未补充多源并发", "source=client", "status=200", "offset=4096")
	})
}

// //////////////////  7：manifest blob size 索引  ///////////////////////////////////////////////

// TestManifestBlobSizeIndex blob_size_index.go 的索引单测：put/get、非法项静默忽略、
// 重复 put 只覆盖 size 不重复占用淘汰队列、容量满按 FIFO 淘汰最旧、Server 侧
// recordManifestBlobSizes 记录 config + layers（与预取开关无关）、nil 索引安全。
func TestManifestBlobSizeIndex(t *testing.T) {
	t.Run("put与get", func(t *testing.T) {
		idx := newManifestBlobSizeIndex()
		idx.put(prefetchDigestLayer1, 4096)
		if got, ok := idx.get(prefetchDigestLayer1); !ok || got != 4096 {
			t.Errorf("get = (%d, %v), want (4096, true)", got, ok)
		}
		if got, ok := idx.get(prefetchDigestLayer2); ok || got != 0 {
			t.Errorf("未记录 digest 的 get = (%d, %v), want (0, false)", got, ok)
		}
		if got := idx.len(); got != 1 {
			t.Errorf("len = %d, want 1", got)
		}
	})

	t.Run("非法项静默忽略", func(t *testing.T) {
		idx := newManifestBlobSizeIndex()
		idx.put("", 100)
		idx.put(prefetchDigestLayer1, 0)
		idx.put(prefetchDigestLayer1, -1)
		if got := idx.len(); got != 0 {
			t.Errorf("非法项（空 digest / size<=0）不应入索引，len = %d", got)
		}
	})

	t.Run("重复put只覆盖不占淘汰队列", func(t *testing.T) {
		idx := newManifestBlobSizeIndex()
		idx.put(prefetchDigestLayer1, 100)
		idx.put(prefetchDigestLayer1, 200)
		if got, ok := idx.get(prefetchDigestLayer1); !ok || got != 200 {
			t.Errorf("重复 put 应覆盖 size，get = (%d, %v), want (200, true)", got, ok)
		}
		idx.mu.Lock()
		orderLen := len(idx.order)
		idx.mu.Unlock()
		if orderLen != 1 {
			t.Errorf("重复 put 不应重复占用淘汰队列，order 长度 = %d, want 1", orderLen)
		}
	})

	t.Run("容量满FIFO淘汰最旧", func(t *testing.T) {
		idx := newManifestBlobSizeIndex()
		oldest := fmt.Sprintf("sha256:%064d", 0)
		for i := 0; i <= manifestBlobSizeCapacity; i++ {
			idx.put(fmt.Sprintf("sha256:%064d", i), int64(i+1))
		}
		if got := idx.len(); got != manifestBlobSizeCapacity {
			t.Errorf("容量上限应为 %d，实际 len = %d", manifestBlobSizeCapacity, got)
		}
		if _, ok := idx.get(oldest); ok {
			t.Error("FIFO 应淘汰最旧插入项")
		}
		newest := fmt.Sprintf("sha256:%064d", manifestBlobSizeCapacity)
		if got, ok := idx.get(newest); !ok || got != int64(manifestBlobSizeCapacity+1) {
			t.Errorf("最新插入项应可查得 (%d, %v)", got, ok)
		}
	})

	t.Run("nil索引安全", func(t *testing.T) {
		var nilIdx *manifestBlobSizeIndex
		nilIdx.put(prefetchDigestLayer1, 10) // 不应 panic
		if got, ok := nilIdx.get(prefetchDigestLayer1); ok || got != 0 {
			t.Errorf("nil 索引 get = (%d, %v), want (0, false)", got, ok)
		}
		if got := nilIdx.len(); got != 0 {
			t.Errorf("nil 索引 len = %d, want 0", got)
		}
		var nilServer *Server
		if _, ok := nilServer.manifestBlobSize(prefetchDigestLayer1); ok {
			t.Error("nil Server 查询不应命中")
		}
	})

	t.Run("Server记录manifest中的config与layers", func(t *testing.T) {
		s := newPartialTestServer(t)
		body := blobConcManifestBody(t, prefetchDigestConfig, 77,
			blobRef{Digest: prefetchDigestLayer1, Size: 1024},
			blobRef{Digest: prefetchDigestLayer2, Size: 2048},
			blobRef{Digest: "not/a-valid-digest", Size: 4096}, // digest 非法（含斜杠）：静默跳过
			blobRef{Digest: prefetchDigestOther, Size: 0},     // size<=0：静默跳过
		)
		s.recordManifestBlobSizes(body)
		for _, tc := range []struct {
			digest string
			size   int64
		}{
			{prefetchDigestConfig, 77},
			{prefetchDigestLayer1, 1024},
			{prefetchDigestLayer2, 2048},
		} {
			got, ok := s.manifestBlobSize(tc.digest)
			if !ok || got != tc.size {
				t.Errorf("manifestBlobSize(%s) = (%d, %v), want (%d, true)", tc.digest, got, ok, tc.size)
			}
		}
		if _, ok := s.manifestBlobSize("sha256:" + strings.Repeat("zz", 32)); ok {
			t.Error("未记录的 digest 不应命中")
		}
		if got := s.blobSizeIndex().len(); got != 3 {
			t.Errorf("索引条目数 = %d, want 3（非法 digest / size<=0 被跳过）", got)
		}
	})
}
