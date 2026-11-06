// progress_summary.go —— 活跃下载周期进度汇总：把「当前正在下载的流」集中成一个
// Server 级全局视图，按 log.progress-summary-interval（默认 3m）周期输出一次汇总头 +
// 每个活跃下载一条进度（source / image / digest / method / bytes / total / percent /
// speed / elapsed）。
//
// 与 log.progress-interval（单流 10s 节拍，见 stream.go / partial_resume.go）分工：
//   - progress-interval：单条流自己的进节奏拍，只覆盖「该请求自身的回源/续传流」；客户端
//     搭车回放与后台预取那一路默认看不到；
//   - progress-summary-interval（本文件）：Server 级全局视图，每周期把**全部**活跃下载
//     聚合输出——客户端流式回源（client）、搭车订阅（ridealong）、半截续传执行方
//     （resume）、后台预取（prefetch）四类来源齐全。docker 场景下即可直接看到「正在
//     下载哪些镜像层、各到几成」，客户端下载与异步（预取）下载按 source 区分；没有被
//     访问（无活跃下载）时整轮静默，一条日志都不输出。
//
// 注册表语义（progressRegistry）：
//   - 下载开始写时登记（四处登记点：stream.go 的客户端流式回源 commit 后、
//     partial_resume.go 的续传执行方 commit 后、prefetch_hub.go 的搭车订阅 commit 后与
//     后台预取响应头快照就绪后），结束时 defer 注销——任何返回路径都不留幽灵条目；
//   - 每写一块经 addBytes 原子累加（written 为 atomic.Int64，写循环热路径不加锁）；
//   - total 从响应头 Content-Length（或续传扫描出的总长）取得后 setTotal，不可得则不输出
//     total/percent（与既有单流进度日志的取舍一致）；
//   - tick 时在锁内复制快照并推进每条下载的「上次快照」基线（区间速率用），锁外拼日志
//     输出——持锁写日志会被慢速日志 handler 拖住全部下载的登记/注销。
//
// 生命周期：定时 goroutine 由 main.go 调 StartProgressSummary(ctx) 启动（参照
// cache.StartJanitor 的 ctx 模式，ctx 来自 signal.NotifyContext），ctx 取消即退出，
// 无泄漏；New 里不启动。log.progress-summary-enabled=false 或间隔<=0 时不启动。
//
// 日志级别：Info（与单流进度日志同级，运维默认可见）；汇总头一条 + 每活跃下载一条，
// 键序与既有进度日志习惯一致（source 首位、bytes 为原始字节数、speed/elapsed 为人类
// 可读字符串，见 stream.go 的 humanBytesPerSec）。

package proxy

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxy-cache/config"
)

// progressRegistry Server 级活跃下载注册表：下载 ID -> 条目。
// 条目存在 = 该下载仍在进行（登记到 defer 注销之间）。
type progressRegistry struct {
	// mu 保护 m / nextID 以及各条目的 total 与「上次快照」基线；written 为 atomic，
	// 写循环累加不在锁内（避免每块都抢注册表锁）。
	mu sync.Mutex
	// nextID 自增下载 ID：输出按 ID 升序排序，保证同一轮汇总的日志顺序稳定。
	nextID uint64
	// m 活跃下载条目表（登记即入表，注销即删除）。
	m map[uint64]*progressSummaryEntry
}

// progressSummaryEntry 一条活跃下载的登记条目（下载 ID -> 条目）。
type progressSummaryEntry struct {
	// id 下载 ID（登记表键 + 输出排序依据）。
	id uint64
	// source 来源标签，沿用日志规范四值（client / ridealong / resume / prefetch，
	// 见 log_attrs.go）——汇总时据此区分「客户端下载」与「异步预取下载」。
	source string
	// image 镜像仓库名（/v2/<name>/... 的 name；非 docker 下载为空串，输出时省略该键，
	// 绝不填空串，与 log_attrs.go 的兜底约定一致）。
	image string
	// digest blob digest（blob 目标才非空，输出时省略空值）。
	digest string
	// method 本次下载对应的客户端请求方法（GET / HEAD；预取为合成的 GET）。
	method string
	// startedAt 登记时刻：elapsed 起点，同时是首轮速率基线的起点。
	startedAt time.Time
	// reg 所属注册表（注销 / setTotal 加锁用）。
	reg *progressRegistry
	// written 已下载字节数（原子累加：写循环每块调用一次，热路径不加锁）。
	written atomic.Int64
	// //// 以下字段只在注册表锁内读写（total 由 setTotal 在锁内更新） ////
	// total 总长（Content-Length 可得时为正数；0 = 不可得，汇总不输出 total/percent）。
	total int64
	// lastWritten 上次汇总快照时的已下载字节数（区间速率的分子）。
	lastWritten int64
	// lastAt 上次汇总快照时刻（区间速率的分母；首轮为登记时刻）。
	lastAt time.Time
}

// progressSummaryRow 一轮汇总快照的一行：锁内算好 bytes/total/区间速率素材，锁外只做
// 格式化与日志输出。
type progressSummaryRow struct {
	// id 下载 ID（排序用）。
	id uint64
	// source / image / digest / method 与登记条目同名同义。
	source string
	image  string
	digest string
	method string
	// bytes 快照时的已下载字节数。
	bytes int64
	// total 总长（0 = 不可得）。
	total int64
	// delta 自上次快照以来新增的字节数（区间速率分子）。
	delta int64
	// window 自上次快照以来的时长（区间速率分母；可能为 0，humanBytesPerSec 兜底）。
	window time.Duration
	// elapsed 自登记以来的耗时。
	elapsed time.Duration
}

// newProgressRegistry 构建空注册表。
func newProgressRegistry() *progressRegistry {
	return &progressRegistry{m: make(map[uint64]*progressSummaryEntry)}
}

// register 登记一条活跃下载并返回条目（调用方必须 defer entry.unregister()）。
// initial 为登记时已完成的字节数（续传前缀 / 搭车已落盘前缀；其余场景 0），
// total 为已知总长（Content-Length / 续传扫描总长；不可得传 0，可后经 setTotal 补）。
// 条目字段在入表前一次性填好，保证首轮快照即读到正确基线。
func (r *progressRegistry) register(source, image, digest, method string, initial, total int64, now time.Time) *progressSummaryEntry {
	ent := &progressSummaryEntry{
		source:    source,
		image:     image,
		digest:    digest,
		method:    method,
		startedAt: now,
		reg:       r,
		total:     total,
		lastAt:    now,
	}
	ent.written.Store(initial)
	r.mu.Lock()
	r.nextID++
	ent.id = r.nextID
	ent.lastWritten = initial
	r.m[ent.id] = ent
	r.mu.Unlock()
	return ent
}

// snapshot 复制一份汇总快照并推进各条目的「上次快照」基线：无活跃下载返回 nil
// （调用方据此静默跳过整轮输出）。全部字段读取与基线推进都在锁内完成，锁外只做格式化。
func (r *progressRegistry) snapshot(now time.Time) []progressSummaryRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) == 0 {
		return nil
	}
	rows := make([]progressSummaryRow, 0, len(r.m))
	for _, e := range r.m {
		written := e.written.Load()
		rows = append(rows, progressSummaryRow{
			id:      e.id,
			source:  e.source,
			image:   e.image,
			digest:  e.digest,
			method:  e.method,
			bytes:   written,
			total:   e.total,
			delta:   written - e.lastWritten,
			window:  now.Sub(e.lastAt),
			elapsed: now.Sub(e.startedAt),
		})
		e.lastWritten = written
		e.lastAt = now
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows
}

// addBytes 累加已下载字节数（写循环每块调用一次；原子累加，不加注册表锁）。
func (e *progressSummaryEntry) addBytes(n int) {
	if n <= 0 {
		return
	}
	e.written.Add(int64(n))
}

// setTotal 补记总长（响应头 Content-Length 解析出来后调用；非正数忽略，已登记的总长
// 不被 0 覆盖——续传扫描已知总长时登记即带上）。
func (e *progressSummaryEntry) setTotal(total int64) {
	if total <= 0 {
		return
	}
	e.reg.mu.Lock()
	e.total = total
	e.reg.mu.Unlock()
}

// unregister 注销本条下载（defer 调用；幂等——重复调用只是再删一次不存在的键）。
func (e *progressSummaryEntry) unregister() {
	e.reg.mu.Lock()
	delete(e.reg.m, e.id)
	e.reg.mu.Unlock()
}

// progressOrInit 返回 Server 级活跃下载注册表：New 已预构建，绕过 New 构造的 Server
// 惰性补建（防御风格与 prefetchHubOrInit 一致）。
func (s *Server) progressOrInit() *progressRegistry {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	if s.progressReg == nil {
		s.progressReg = newProgressRegistry()
	}
	return s.progressReg
}

// progressRegister 登记一条活跃下载（各下载流共用入口，语义见 progressRegistry.register）。
func (s *Server) progressRegister(source, image, digest, method string, initial, total int64) *progressSummaryEntry {
	return s.progressOrInit().register(source, image, digest, method, initial, total, time.Now())
}

// StartProgressSummary 启动活跃下载周期进度汇总协程（main.go 在启动期调用，参照
// cache.StartJanitor 的 ctx 模式）：log.progress-summary-enabled=false 或间隔<=0 时不
// 启动；协程随 ctx 取消退出（进程退出 signal ctx 取消 → goroutine 退出，无泄漏）。
// 幂等：重复调用只启动一个协程（Server 级 sync.Once）。
func (s *Server) StartProgressSummary(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background() // 防御：与 cache.Manager 的 nil ctx 兜底同风格
	}
	// 未经过 Validate 的构造场景兜底：cfg 为 nil 时按默认值（开启 + 3m）处理
	enabled, interval := true, config.DefaultProgressSummaryInterval
	if s.cfg != nil {
		enabled, interval = s.cfg.ProgressSummaryEnabled(), s.cfg.ProgressSummaryInterval()
	}
	if !enabled || interval <= 0 {
		return
	}
	s.progressOnce.Do(func() { go s.progressSummaryLoop(ctx, interval) })
}

// progressSummaryLoop 定时汇总循环：每 interval 输出一轮（无活跃下载时静默跳过）。
func (s *Server) progressSummaryLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.emitProgressSummary(now, interval)
		}
	}
}

// emitProgressSummary 输出一轮活跃下载进度汇总：先一条汇总头（active 总数 + 各来源
// 数量），再每个活跃下载一条进度。快照为空（无活跃下载）时零日志。
// 只读快照、不持注册表锁写日志（见文件头并发说明）。
func (s *Server) emitProgressSummary(now time.Time, interval time.Duration) {
	rows := s.progressOrInit().snapshot(now)
	if len(rows) == 0 {
		return // 无活跃下载：整轮静默（需求：没有被访问时不输出）
	}
	// 各来源计数（汇总头用）：客户端下载（client/ridealong/resume）与异步下载（prefetch）
	// 在 source 上区分，看日志即可判断「当前带宽是客户端在拉还是后台预取在拉」
	var client, ridealong, resume, prefetch int
	for _, row := range rows {
		switch row.source {
		case dockerLogSourceClient:
			client++
		case dockerLogSourceRidealong:
			ridealong++
		case dockerLogSourceResume:
			resume++
		case dockerLogSourcePrefetch:
			prefetch++
		}
	}
	s.log.Info("活跃下载进度汇总",
		"active", len(rows),
		"client", client,
		"ridealong", ridealong,
		"resume", resume,
		"prefetch", prefetch,
		"interval", interval.String())
	for _, row := range rows {
		// 键序：source 首位（docker 链路日志规范），image/digest 空值省略，其余字段沿用
		// 既有单流进度日志习惯（bytes 原始字节数、total 可得才输出、percent 0-100、
		// speed 人类可读速率、elapsed 自登记起的耗时）
		args := make([]any, 0, 20)
		args = append(args, "source", row.source)
		if row.image != "" {
			args = append(args, "image", row.image)
		}
		if row.digest != "" {
			args = append(args, "digest", row.digest)
		}
		args = append(args, "method", row.method, "bytes", row.bytes)
		if row.total > 0 {
			args = append(args, "total", row.total, "percent", progressSummaryPercent(row.bytes, row.total))
		}
		args = append(args,
			"speed", humanBytesPerSec(row.delta, row.window),
			"elapsed", fmt.Sprintf("%.1fs", row.elapsed.Seconds()),
		)
		s.log.Info("活跃下载进度", args...)
	}
}

// progressSummaryPercent 已下载占比（0-100 整数）：总长不可得时调用方不输出该字段；
// 上游声明值异常偏小时截到 100、异常偏大时截到 0，绝不输出越界值（与单流进度日志一致）。
func progressSummaryPercent(bytes, total int64) int {
	if total <= 0 {
		return 0
	}
	pct := int(float64(bytes) / float64(total) * 100)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// progressSummaryTotal 从响应头取 Content-Length 作为总长：解析失败 / 非正数返回 0
// （总长不可得，汇总不输出 total/percent）。搭车与预取两路各自持有响应头快照，共用本函数。
func progressSummaryTotal(header http.Header) int64 {
	if header == nil {
		return 0
	}
	if v, err := strconv.ParseInt(strings.TrimSpace(header.Get("Content-Length")), 10, 64); err == nil && v > 0 {
		return v
	}
	return 0
}
