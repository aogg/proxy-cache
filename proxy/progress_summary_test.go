package proxy

// progress_summary_test.go —— 活跃下载周期进度汇总（progress_summary.go，提交 1d3009d）单测：
// 全部白盒 + httptest 假上游（复用 resume/hub 两套既有 fixture），缓存目录 t.TempDir()，
// 不启动本服务、不发外网请求，不含任何 e2e。覆盖：
//   1. 注册表：register / addBytes / setTotal / snapshot 字段语义（固定时刻、无 sleep 的
//      两轮快照即两次 tick：首轮基线 = 登记时刻、次轮 = 两次快照之差）、unregister 后快照
//      为空且幂等、并发 addBytes 不丢更新（-race 覆盖）、percent/total 辅助边界；
//   2. 周期输出：短 interval 启动 StartProgressSummary(ctx) + 两个活跃下载（client /
//      prefetch，带 image/digest/total）→ 汇总头 active=2 与两条「活跃下载进度」；
//      ctx 取消后协程退出（其后注册的下载不再产生汇总日志）；空表整轮静默；
//      enabled=false / interval<=0（含负数归一化）不启动；New 只预构建注册表不启动协程；
//      StartProgressSummary 幂等（sync.Once：第二次调用不启动第二个协程）；
//   3. 配置：log.progress-summary-enabled 默认 true、显式 false 关闭；interval 默认 3m、
//      显式配置生效、负数 Validate 归一化为 0、显式 0 关闭；config.example.yaml 加载后
//      两键为未显式配置（生效值 = 默认）；显式 YAML 两键可解析接线；
//   4. 四个登记点冒烟：客户端流式回源 source=client、后台预取 source=prefetch、半截续传
//      source=resume、搭车订阅 source=ridealong 在下载进行中均存在于注册表（image/digest/
//      method/total 正确），下载结束即注销（不留幽灵条目）。

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/config"
)

// //////////////////  并发安全日志缓冲与 Server 辅助  //////////////////////////////////////////////

// syncLogBuffer 并发安全的日志缓冲：周期汇总 goroutine 写入、测试 goroutine 边等边读
// （bytes.Buffer 非并发安全，直接共用会在 -race 下报数据竞争）。
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write 实现 io.Writer（slog.TextHandler 的落点）。
func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String 返回当前全部日志的并发安全快照。
func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Len 返回当前日志字节数的并发安全快照。
func (b *syncLogBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// newProgressServer 构建禁用缓存的 Server：汇总开关/间隔按参数显式配置（nil = 不配置，
// 走默认值），日志写入并发安全缓冲（供边等边读断言）。
func newProgressServer(t *testing.T, enabled *bool, interval *config.Duration) (*Server, *syncLogBuffer) {
	t.Helper()
	cfg := config.Default()
	cfg.Cache.Enabled = false
	cfg.Log.ProgressSummaryEnabled = enabled
	cfg.Log.ProgressSummaryInterval = interval
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	sb := &syncLogBuffer{}
	s, err := New(cfg, nil, slog.New(slog.NewTextHandler(sb, nil)))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s, sb
}

// waitLogContains 轮询等待日志满足 cond（超时打印当前日志并 fatal）。
func waitLogContains(t *testing.T, sb *syncLogBuffer, timeout time.Duration, cond func(string) bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond(sb.String()) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s（当前日志:\n%s）", msg, sb.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// countLogMsg 统计包含 msg 片段的日志行数。
func countLogMsg(log, msg string) int {
	n := 0
	for _, ln := range strings.Split(log, "\n") {
		if strings.Contains(ln, msg) {
			n++
		}
	}
	return n
}

// firstLogLine 返回日志中同时包含全部 substr 的第一行（无则空串）。
func firstLogLine(log string, subs ...string) string {
	for _, ln := range strings.Split(log, "\n") {
		matched := true
		for _, sub := range subs {
			if !strings.Contains(ln, sub) {
				matched = false
				break
			}
		}
		if matched {
			return ln
		}
	}
	return ""
}

// //////////////////  1：注册表语义  ////////////////////////////////////////////////////////////////

// TestProgressRegistrySnapshot 注册表 register/addBytes/setTotal/snapshot 核心语义
// （固定时刻、无 sleep 的两轮快照即两次 tick）：
//   - 空注册表快照为 nil（调用方据此静默跳过整轮输出）；
//   - 首轮快照基线 = 登记时刻：delta 为自登记以来的增量、window/elapsed 为自登记起时长
//     （实现在 register 时写入 lastWritten/lastAt，因此首轮即有区间速率素材，不省略
//     speed——「首轮无基线」的设想与本实现不符，见返回给 manage 的核对说明）；
//   - 次轮快照 delta/window = 两次快照之间的增量与时长（差值速率语义）；
//   - total 可经 setTotal 补记（非正数忽略、后到响应头可修正已登记值）；
//   - 快照按下载 ID 升序（同一轮输出的日志顺序稳定）；
//   - unregister 后条目从快照消失、重复注销幂等、注销后 addBytes 不复活条目。
func TestProgressRegistrySnapshot(t *testing.T) {
	r := newProgressRegistry()
	if rows := r.snapshot(time.Now()); rows != nil {
		t.Fatalf("空注册表快照应为 nil（调用方据此静默），got %+v", rows)
	}

	t0 := time.Now()
	client := r.register(dockerLogSourceClient, "demo/app", "sha256:aaaa", http.MethodGet, 100, 0, t0)
	prefetch := r.register(dockerLogSourcePrefetch, "", "", http.MethodGet, 0, 0, t0)
	if client.id == 0 || prefetch.id <= client.id {
		t.Fatalf("下载 ID 应自 1 递增（client=%d, prefetch=%d）", client.id, prefetch.id)
	}

	// 写循环累加（非正数忽略）+ 总长补记（非正数忽略，不清空/不覆盖）
	client.addBytes(900)
	client.addBytes(0)
	client.addBytes(-5)
	prefetch.addBytes(300)
	client.setTotal(1000)
	prefetch.setTotal(0)
	prefetch.setTotal(-100)

	// 首轮快照（t0+1s）：delta/window/elapsed 自登记时刻起算
	rows := r.snapshot(t0.Add(time.Second))
	if len(rows) != 2 {
		t.Fatalf("首轮快照应有 2 行, got %d 行: %+v", len(rows), rows)
	}
	if rows[0].id != client.id || rows[1].id != prefetch.id {
		t.Errorf("快照应按下载 ID 升序（got %d, %d）", rows[0].id, rows[1].id)
	}
	wantClient := progressSummaryRow{
		id: client.id, source: dockerLogSourceClient, image: "demo/app", digest: "sha256:aaaa",
		method: http.MethodGet, bytes: 1000, total: 1000,
		delta: 900, window: time.Second, elapsed: time.Second,
	}
	if rows[0] != wantClient {
		t.Errorf("首轮 client 行 = %+v\nwant %+v", rows[0], wantClient)
	}
	wantPrefetch := progressSummaryRow{
		id: prefetch.id, source: dockerLogSourcePrefetch, image: "", digest: "",
		method: http.MethodGet, bytes: 300, total: 0,
		delta: 300, window: time.Second, elapsed: time.Second,
	}
	if rows[1] != wantPrefetch {
		t.Errorf("首轮 prefetch 行 = %+v\nwant %+v", rows[1], wantPrefetch)
	}
	// 首轮速率素材来自「自登记起」的区间（d/window），不是 0
	if got := humanBytesPerSec(rows[0].delta, rows[0].window); got != "900B/s" {
		t.Errorf("首轮速率 = %q, want %q（自登记起算）", got, "900B/s")
	}

	// 次轮快照（t0+3s）：delta/window = 两次快照之差（区间速率）
	client.addBytes(500)
	rows = r.snapshot(t0.Add(3 * time.Second))
	if len(rows) != 2 {
		t.Fatalf("次轮快照应有 2 行, got %d 行", len(rows))
	}
	if rows[0].bytes != 1500 || rows[0].delta != 500 || rows[0].window != 2*time.Second || rows[0].elapsed != 3*time.Second {
		t.Errorf("次轮 client 行 bytes/delta/window/elapsed = %d/%d/%s/%s, want 1500/500/2s/3s",
			rows[0].bytes, rows[0].delta, rows[0].window, rows[0].elapsed)
	}
	if got := humanBytesPerSec(rows[0].delta, rows[0].window); got != "250B/s" {
		t.Errorf("次轮区间速率 = %q, want %q", got, "250B/s")
	}
	// 区间内无新增：delta=0、速率 0B/s（不把累计字节数误当速率）
	if rows[1].delta != 0 || humanBytesPerSec(rows[1].delta, rows[1].window) != "0B/s" {
		t.Errorf("无增量区间速率应为 0B/s（delta=%d, window=%s）", rows[1].delta, rows[1].window)
	}
	// 后到响应头修正总长（setTotal 正数可覆盖）
	prefetch.setTotal(2000)
	rows = r.snapshot(t0.Add(3 * time.Second))
	if rows[1].total != 2000 {
		t.Errorf("setTotal 后 total = %d, want 2000", rows[1].total)
	}

	// 注销：条目从快照消失，重复注销幂等，注销后 addBytes 不复活
	client.unregister()
	client.unregister()
	client.addBytes(1)
	rows = r.snapshot(t0.Add(4 * time.Second))
	if len(rows) != 1 || rows[0].id != prefetch.id {
		t.Errorf("unregister 后快照应只剩 prefetch 一条, got %+v", rows)
	}
	prefetch.unregister()
	if rows := r.snapshot(t0.Add(5 * time.Second)); rows != nil {
		t.Errorf("全部注销后快照应为 nil, got %+v", rows)
	}
}

// TestProgressRegistryConcurrentAddBytes 并发 addBytes 不丢更新：8 路 × 2000 次各加 1，
// 期间另一 goroutine 持续快照（原子累加与注册表锁并发路径，-race 覆盖）。
func TestProgressRegistryConcurrentAddBytes(t *testing.T) {
	r := newProgressRegistry()
	e := r.register(dockerLogSourceClient, "demo/app", "sha256:bbbb", http.MethodGet, 0, 0, time.Now())

	const writers, perWriter = 8, 2000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var snapshotting sync.WaitGroup
	snapshotting.Add(1)
	go func() {
		defer snapshotting.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.snapshot(time.Now())
				time.Sleep(time.Millisecond)
			}
		}
	}()
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				e.addBytes(1)
			}
		}()
	}
	wg.Wait()
	close(stop)
	snapshotting.Wait()

	if got, want := e.written.Load(), int64(writers*perWriter); got != want {
		t.Errorf("并发 addBytes 后 written = %d, want %d（不应丢更新）", got, want)
	}
}

// TestProgressSummaryPercent 表驱动验证占比：总长不可得/非正 → 0；正常取整；越界截断到 0-100。
func TestProgressSummaryPercent(t *testing.T) {
	tests := []struct {
		name  string
		bytes int64
		total int64
		want  int
	}{
		{name: "总长不可得", bytes: 100, total: 0, want: 0},
		{name: "总长为负", bytes: 100, total: -1, want: 0},
		{name: "零进度", bytes: 0, total: 1000, want: 0},
		{name: "四成", bytes: 400, total: 1000, want: 40},
		{name: "整除满进度", bytes: 1000, total: 1000, want: 100},
		{name: "超过总长截到100", bytes: 1500, total: 1000, want: 100},
		{name: "负进度截到0", bytes: -100, total: 1000, want: 0},
		{name: "三分之一取整", bytes: 1, total: 3, want: 33},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := progressSummaryPercent(tt.bytes, tt.total); got != tt.want {
				t.Errorf("progressSummaryPercent(%d, %d) = %d, want %d", tt.bytes, tt.total, got, tt.want)
			}
		})
	}
}

// TestProgressSummaryTotal 表驱动验证响应头总长解析：nil/缺失/非数字/非正 → 0，正数含空白可解析。
func TestProgressSummaryTotal(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		want   int64
	}{
		{name: "nil响应头", header: nil, want: 0},
		{name: "无Content-Length", header: http.Header{}, want: 0},
		{name: "正常长度", header: http.Header{"Content-Length": {"1024"}}, want: 1024},
		{name: "前后空白", header: http.Header{"Content-Length": {" 1024 "}}, want: 1024},
		{name: "非数字", header: http.Header{"Content-Length": {"abc"}}, want: 0},
		{name: "零", header: http.Header{"Content-Length": {"0"}}, want: 0},
		{name: "负数", header: http.Header{"Content-Length": {"-5"}}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := progressSummaryTotal(tt.header); got != tt.want {
				t.Errorf("progressSummaryTotal(%v) = %d, want %d", tt.header, got, tt.want)
			}
		})
	}
}

// //////////////////  2：周期输出  //////////////////////////////////////////////////////////////////

// TestProgressSummaryPeriodicOutput 短 interval（30ms）启动 StartProgressSummary(ctx)，注册
// 一个 client（带 image/digest/total）与一个 prefetch 下载：
//   - 汇总头一条：msg=活跃下载进度汇总 active=2 client=1 prefetch=1 interval=30ms；
//   - 每个下载一条 msg=活跃下载进度：source/image/digest/method/bytes/total/percent/speed/
//     elapsed 字段齐全（client 400/1000=40%，prefetch 1000/2000=50%）；
//   - ctx 取消后协程退出：再注册下载并等 5 个周期，不再产生任何汇总日志（不泄漏）。
func TestProgressSummaryPeriodicOutput(t *testing.T) {
	const interval = 30 * time.Millisecond
	s, sb := newProgressServer(t, nil, durPtr(interval))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartProgressSummary(ctx)

	client := s.progressRegister(dockerLogSourceClient, "demo/app", "sha256:aaaa", http.MethodGet, 0, 1000)
	prefetch := s.progressRegister(dockerLogSourcePrefetch, "demo/app", "sha256:bbbb", http.MethodGet, 0, 2000)
	defer func() {
		client.unregister()
		prefetch.unregister()
	}()
	client.addBytes(400)
	prefetch.addBytes(1000)

	waitLogContains(t, sb, 2*time.Second, func(log string) bool {
		return countLogMsg(log, "msg=活跃下载进度汇总") >= 1 &&
			countLogMsg(log, "msg=活跃下载进度 ") >= 2
	}, "周期汇总应输出汇总头与两条下载进度")

	log := sb.String()
	header := firstLogLine(log, "msg=活跃下载进度汇总")
	if header == "" {
		t.Fatalf("应有汇总头日志，实际:\n%s", log)
	}
	wantLineField(t, header, "active=2", "client=1", "prefetch=1", "interval=30ms")

	clientLine := firstLogLine(log, "msg=活跃下载进度 ", "source=client")
	if clientLine == "" {
		t.Fatalf("应有 client 下载进度行，实际:\n%s", log)
	}
	wantLineField(t, clientLine, "image=demo/app", "digest=sha256:aaaa", "method=GET",
		"bytes=400", "total=1000", "percent=40", "speed=", "elapsed=")

	prefetchLine := firstLogLine(log, "msg=活跃下载进度 ", "source=prefetch")
	if prefetchLine == "" {
		t.Fatalf("应有 prefetch 下载进度行，实际:\n%s", log)
	}
	wantLineField(t, prefetchLine, "image=demo/app", "digest=sha256:bbbb", "method=GET",
		"bytes=1000", "total=2000", "percent=50")

	// ctx 取消：协程退出（先排掉取消瞬间可能已排队的一轮，再注册下载观察零输出）
	cancel()
	time.Sleep(3 * interval)
	marker := sb.Len()
	sentinel := s.progressRegister(dockerLogSourceClient, "after-cancel/app", "sha256:cccc", http.MethodGet, 0, 10)
	defer sentinel.unregister()
	sentinel.addBytes(5)
	time.Sleep(5 * interval)
	if got := sb.String()[marker:]; got != "" {
		t.Errorf("ctx 取消后汇总协程应退出（其后注册的下载不应再触发汇总），新增日志:\n%s", got)
	}
}

// TestProgressSummaryEmptyRegistrySilent 空注册表整轮静默：启动后等 6 个周期，
// 日志中不应出现任何「活跃下载进度」输出（需求：没有被访问时零日志）。
func TestProgressSummaryEmptyRegistrySilent(t *testing.T) {
	const interval = 20 * time.Millisecond
	s, sb := newProgressServer(t, nil, durPtr(interval))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartProgressSummary(ctx)

	time.Sleep(6 * interval) // 6 个周期
	if n := countLogMsg(sb.String(), "活跃下载进度"); n != 0 {
		t.Errorf("空注册表不应输出汇总日志, got %d 条:\n%s", n, sb.String())
	}
}

// TestProgressSummaryNotStartedWhenDisabled 关闭路径不启动协程：enabled 显式 false、
// interval 显式 0、interval 负数（Validate 归一化 0）三种配置下，即使有活跃下载
// 也一条汇总日志都不输出。
func TestProgressSummaryNotStartedWhenDisabled(t *testing.T) {
	const interval = 20 * time.Millisecond
	off := false
	tests := []struct {
		name     string
		enabled  *bool
		interval *config.Duration
	}{
		{name: "enabled显式false", enabled: &off, interval: durPtr(interval)},
		{name: "interval显式0关闭", enabled: nil, interval: durPtr(0)},
		{name: "interval负数归一化为0关闭", enabled: nil, interval: durPtr(-time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, sb := newProgressServer(t, tt.enabled, tt.interval)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.StartProgressSummary(ctx)
			e := s.progressRegister(dockerLogSourceClient, "demo/app", "sha256:dddd", http.MethodGet, 0, 100)
			defer e.unregister()
			e.addBytes(50)
			time.Sleep(4 * interval)
			if n := countLogMsg(sb.String(), "活跃下载进度"); n != 0 {
				t.Errorf("关闭配置下不应输出汇总日志（间隔=%v）, got %d 条:\n%s", tt.interval, n, sb.String())
			}
		})
	}
}

// TestNewPrefillsProgressRegistry New 预构建注册表但不启动定时协程（生命周期归调用方）：
// 未调 StartProgressSummary 时即使有活跃下载也不输出汇总日志。
func TestNewPrefillsProgressRegistry(t *testing.T) {
	s, sb := newProgressServer(t, nil, durPtr(20*time.Millisecond))
	if s.progressReg == nil {
		t.Fatal("New 应预构建 progressReg（各登记点依赖）")
	}
	e := s.progressRegister(dockerLogSourceClient, "demo/app", "sha256:eeee", http.MethodGet, 0, 100)
	defer e.unregister()
	e.addBytes(10)
	time.Sleep(60 * time.Millisecond) // 3 个 interval
	if n := countLogMsg(sb.String(), "活跃下载进度"); n != 0 {
		t.Errorf("New 不应启动汇总协程（未调 StartProgressSummary）, got %d 条:\n%s", n, sb.String())
	}
}

// TestProgressSummaryStartOnce StartProgressSummary 幂等（Server 级 sync.Once）：
// 用两个 ctx 各调一次，取消第一个后汇总必须停止——若第二次调用启动了第二个协程，
// 绑在第二个 ctx 上的循环会继续输出，本用例据此判定。
func TestProgressSummaryStartOnce(t *testing.T) {
	const interval = 30 * time.Millisecond
	s, sb := newProgressServer(t, nil, durPtr(interval))
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	s.StartProgressSummary(ctx1)
	s.StartProgressSummary(ctx2) // Once：不启动第二个协程（ctx2 被忽略）

	e := s.progressRegister(dockerLogSourceClient, "once/app", "sha256:ffff", http.MethodGet, 0, 100)
	defer e.unregister()
	e.addBytes(50)
	waitLogContains(t, sb, 2*time.Second, func(log string) bool {
		return countLogMsg(log, "msg=活跃下载进度汇总") >= 1
	}, "首个协程应输出汇总")

	cancel1()
	time.Sleep(3 * interval) // 排掉取消瞬间可能已排队的一轮
	stable := countLogMsg(sb.String(), "活跃下载进度汇总")
	time.Sleep(6 * interval) // 6 个周期：若第二个协程存在必然新增多条
	if got := countLogMsg(sb.String(), "活跃下载进度汇总"); got != stable {
		t.Errorf("取消第一个 ctx 后汇总应停止（第二次 StartProgressSummary 不应启动新协程）：稳定时 %d 条，现 %d 条", stable, got)
	}
}

// //////////////////  3：配置  //////////////////////////////////////////////////////////////////////

// TestProgressSummaryConfigSemantics 汇总配置生效值：
//   - 默认（未配置）：enabled=true、interval=3m（DefaultProgressSummaryInterval）；
//   - 显式 false 关闭、显式区间生效；
//   - 负数 interval 经 Validate 归一化为显式 0（关闭，指针保留）、显式 0 关闭。
func TestProgressSummaryConfigSemantics(t *testing.T) {
	cfg := config.Default()
	if !cfg.ProgressSummaryEnabled() {
		t.Error("log.progress-summary-enabled 未配置应默认开启")
	}
	if got := cfg.ProgressSummaryInterval(); got != config.DefaultProgressSummaryInterval {
		t.Errorf("log.progress-summary-interval 未配置应默认 %s, got %s", config.DefaultProgressSummaryInterval, got)
	}
	if config.DefaultProgressSummaryInterval != 3*time.Minute {
		t.Errorf("默认汇总间隔应为 3m, got %s", config.DefaultProgressSummaryInterval)
	}

	// 显式配置生效
	off := false
	cfg.Log.ProgressSummaryEnabled = &off
	d := config.Duration(45 * time.Second)
	cfg.Log.ProgressSummaryInterval = &d
	if cfg.ProgressSummaryEnabled() {
		t.Error("显式 progress-summary-enabled=false 应关闭")
	}
	if got := cfg.ProgressSummaryInterval(); got != 45*time.Second {
		t.Errorf("显式 progress-summary-interval=45s 应生效, got %s", got)
	}

	// 负数归一化为 0（关闭；保留显式指针，语义是「显式配置为 0」而非未配置）
	neg := config.Duration(-5 * time.Second)
	cfg.Log.ProgressSummaryInterval = &neg
	if err := cfg.Validate(); err != nil {
		t.Fatalf("负数 interval 应被归一化而非报错, 意外报错: %v", err)
	}
	if got := cfg.ProgressSummaryInterval(); got != 0 {
		t.Errorf("负数 interval 归一化后应为 0, got %s", got)
	}
	if cfg.Log.ProgressSummaryInterval == nil {
		t.Error("归一化后应保留显式 0 指针（区分未配置的默认 3m）")
	}

	// 显式 0 = 关闭
	zero := config.Duration(0)
	cfg.Log.ProgressSummaryInterval = &zero
	if err := cfg.Validate(); err != nil {
		t.Fatalf("显式 0 interval 不应报错: %v", err)
	}
	if got := cfg.ProgressSummaryInterval(); got != 0 {
		t.Errorf("显式 interval=0 应关闭（生效值 0）, got %s", got)
	}
}

// TestExampleConfigProgressSummaryKeys config.example.yaml 能加载（既有 example_load_test
// 机制）且两键以注释示例存在、未显式配置 → 生效值为默认（开启 + 3m）。从 proxy 包运行
// 时 go test 的 cwd 是本包目录，示例文件在上层目录。
func TestExampleConfigProgressSummaryKeys(t *testing.T) {
	raw, err := os.ReadFile("../config.example.yaml")
	if err != nil {
		t.Fatalf("读 config.example.yaml 失败: %v", err)
	}
	for _, key := range []string{"progress-summary-enabled:", "progress-summary-interval:"} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("config.example.yaml 应记录 %s 键（示例/注释形式）", key)
		}
	}
	cfg, err := config.Load("../config.example.yaml")
	if err != nil {
		t.Fatalf("config.example.yaml 应能通过 Load + Validate，意外报错: %v", err)
	}
	if !cfg.ProgressSummaryEnabled() {
		t.Error("示例文件未显式配置启用键，生效值应为默认 true")
	}
	if got := cfg.ProgressSummaryInterval(); got != 3*time.Minute {
		t.Errorf("示例文件未显式配置间隔键，生效值应为默认 3m, got %s", got)
	}
	if cfg.Log.ProgressSummaryEnabled != nil || cfg.Log.ProgressSummaryInterval != nil {
		t.Error("示例文件两键应为注释状态（解析后指针为 nil = 未显式配置）")
	}
}

// TestProgressSummaryConfigYAMLKeys 显式 YAML 两键接线：progress-summary-enabled: false +
// progress-summary-interval: 45s → 生效值 false / 45s（键名与解析链路正确）。
func TestProgressSummaryConfigYAMLKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "log:\n  progress-summary-enabled: false\n  progress-summary-interval: 45s\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("显式两键配置应能加载，意外报错: %v", err)
	}
	if cfg.ProgressSummaryEnabled() {
		t.Error("显式 progress-summary-enabled=false 应关闭汇总")
	}
	if got := cfg.ProgressSummaryInterval(); got != 45*time.Second {
		t.Errorf("显式 progress-summary-interval=45s 应生效, got %s", got)
	}
}

// //////////////////  4：四个登记点冒烟  ////////////////////////////////////////////////////////////

// waitProgressRow 轮询活跃下载注册表快照直到出现满足 cond 的行（超时打印当前快照并 fatal）。
func waitProgressRow(t *testing.T, s *Server, timeout time.Duration, cond func(progressSummaryRow) bool, msg string) progressSummaryRow {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, row := range s.progressOrInit().snapshot(time.Now()) {
			if cond(row) {
				return row
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s（当前快照: %+v）", msg, s.progressOrInit().snapshot(time.Now()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitNoProgressRows 轮询等待注册表清空（下载结束即注销，不留幽灵条目）。
func waitNoProgressRows(t *testing.T, s *Server, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if len(s.progressOrInit().snapshot(time.Now())) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s（残留快照: %+v）", msg, s.progressOrInit().snapshot(time.Now()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestProgressRegisterClientInFlight 客户端流式回源进行中：慢速上游分块发 blob 期间，
// 注册表存在 source=client 条目（image/digest/method/total 正确），下载结束即注销。
func TestProgressRegisterClientInFlight(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xe1, 256<<10)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(8<<10, 10*time.Millisecond) // 32 块 × 10ms ≈ 320ms 在途窗口

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
	}()

	row := waitProgressRow(t, fx.s, 3*time.Second, func(row progressSummaryRow) bool {
		return row.source == dockerLogSourceClient && row.bytes > 0
	}, "客户端流式回源进行中应存在 source=client 的注册条目")
	if row.image != name || row.digest != digest || row.method != http.MethodGet {
		t.Errorf("client 条目标签 = image=%q digest=%q method=%q, want %q/%q/GET",
			row.image, row.digest, row.method, name, digest)
	}
	if row.total != int64(len(content)) {
		t.Errorf("client 条目 total = %d, want %d（上游 Content-Length）", row.total, len(content))
	}
	if row.bytes > row.total {
		t.Errorf("client 条目 bytes=%d 不应超过 total=%d", row.bytes, row.total)
	}

	<-done
	if w.Code != http.StatusOK {
		t.Fatalf("client Code = %d, want %d", w.Code, http.StatusOK)
	}
	if !bytes.Equal(w.Body.Bytes(), content) {
		t.Errorf("client body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
	}
	waitNoProgressRows(t, fx.s, 2*time.Second, "客户端流式回源结束后注册条目应注销")
}

// TestProgressRegisterPrefetchInFlight 后台预取进行中：prefetchOneBlob 慢速下载期间，
// 注册表存在 source=prefetch 条目（image/digest/method/total 正确），结束即注销。
func TestProgressRegisterPrefetchInFlight(t *testing.T) {
	fx := newHubFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer2
	content := hubBlobContent(0xe2, 256<<10)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(8<<10, 10*time.Millisecond) // 32 块 × 10ms ≈ 320ms 在途窗口

	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, "v2/"+name+"/blobs/"+digest)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()

	row := waitProgressRow(t, fx.s, 3*time.Second, func(row progressSummaryRow) bool {
		return row.source == dockerLogSourcePrefetch && row.bytes > 0
	}, "后台预取进行中应存在 source=prefetch 的注册条目")
	if row.image != name || row.digest != digest || row.method != http.MethodGet {
		t.Errorf("prefetch 条目标签 = image=%q digest=%q method=%q, want %q/%q/GET",
			row.image, row.digest, row.method, name, digest)
	}
	if row.total != int64(len(content)) {
		t.Errorf("prefetch 条目 total = %d, want %d（预取响应头快照 Content-Length）", row.total, len(content))
	}

	<-done
	waitNoProgressRows(t, fx.s, 2*time.Second, "预取结束后注册条目应注销")
}

// TestProgressRegisterRidealongInFlight 搭车订阅进行中：预取先行、客户端撞上在途条目搭车，
// 搭车回放期间注册表存在 source=ridealong 条目（image/digest/total 正确、bytes ≤ total），
// 搭车结束后注销（客户端拿到完整 body）。
func TestProgressRegisterRidealongInFlight(t *testing.T) {
	fx := newHubFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xe3, 256<<10)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(8<<10, 10*time.Millisecond) // 32 块 × 10ms ≈ 320ms 在途窗口

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()
	ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
	if !ent.waitHeaderReady(2 * time.Second) {
		t.Fatal("等待预取响应头快照就绪超时")
	}

	rideDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
		rideDone <- w
	}()

	row := waitProgressRow(t, fx.s, 3*time.Second, func(row progressSummaryRow) bool {
		return row.source == dockerLogSourceRidealong
	}, "搭车回放期间应存在 source=ridealong 的注册条目")
	if row.image != name || row.digest != digest || row.method != http.MethodGet {
		t.Errorf("ridealong 条目标签 = image=%q digest=%q method=%q, want %q/%q/GET",
			row.image, row.digest, row.method, name, digest)
	}
	if row.total != int64(len(content)) {
		t.Errorf("ridealong 条目 total = %d, want %d（预取响应头快照 Content-Length）", row.total, len(content))
	}
	if row.bytes > row.total {
		t.Errorf("ridealong 条目 bytes=%d 不应超过 total=%d", row.bytes, row.total)
	}

	w := <-rideDone
	if w.Code != http.StatusOK {
		t.Fatalf("搭车 Code = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("X-Cache"); got != prefetchCacheStatus {
		t.Errorf("搭车 X-Cache = %q, want %q", got, prefetchCacheStatus)
	}
	if !bytes.Equal(w.Body.Bytes(), content) {
		t.Errorf("搭车 body 与上游原文不一致（got %d 字节）", w.Body.Len())
	}
	<-prefetchDone
	waitNoProgressRows(t, fx.s, 2*time.Second, "搭车与预取结束后注册条目应注销")
}

// TestProgressRegisterResumeInFlight 半截续传进行中：预置半截 + 闸门卡住上游剩余区间，
// 续传回放/拼接期间注册表存在 source=resume 条目（bytes ≥ 半截前缀、total = 对象总长），
// 放行后客户端拿到完整 body 且条目注销。
func TestProgressRegisterResumeInFlight(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xe4, 128<<10)
	half := 48 << 10 // 半截前缀：注册初始已下载字节
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(8<<10, 10*time.Millisecond)
	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	hold := fx.reg.armHold(digest) // 首路（续传那一路）首块后阻塞
	var holdOnce sync.Once
	release := func() { holdOnce.Do(func() { close(hold) }) }
	defer release()

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
	}()

	row := waitProgressRow(t, fx.s, 3*time.Second, func(row progressSummaryRow) bool {
		return row.source == dockerLogSourceResume
	}, "半截续传进行中应存在 source=resume 的注册条目")
	if row.image != name || row.digest != digest || row.method != http.MethodGet {
		t.Errorf("resume 条目标签 = image=%q digest=%q method=%q, want %q/%q/GET",
			row.image, row.digest, row.method, name, digest)
	}
	if row.total != int64(len(content)) {
		t.Errorf("resume 条目 total = %d, want %d（Content-Range 总长）", row.total, len(content))
	}
	if row.bytes < int64(half) {
		t.Errorf("resume 条目 bytes=%d 应 ≥ 半截前缀 %d（进度不回退）", row.bytes, half)
	}

	release()
	<-done
	if w.Code != http.StatusOK {
		t.Fatalf("续传 Code = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("续传 X-Cache = %q, want %q", got, resumeCacheStatus)
	}
	if !bytes.Equal(w.Body.Bytes(), content) {
		t.Errorf("续传 body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
	}
	waitNoProgressRows(t, fx.s, 2*time.Second, "续传结束后注册条目应注销")
}
