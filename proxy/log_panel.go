// log_panel.go —— LOG_TYPE=line / json 的「下载面板」：多行块渲染、汇总块、候选轮询记录。
//
// 与 progress_summary.go 的分工：
//   - progress_summary.go 维护 Server 级活跃下载注册表（entry：id/source/image/digest/
//     bytes/total/elapsed），text 模式每 progress-summary-interval 输出一条汇总 + 每下载
//     一条进度；
//   - 本文件只服务 LOG_TYPE=line|json：每 log.panel-interval（默认 30s）把**每个活跃
//     docker 下载**渲染成一个多行块（客户端进度 / 代理侧进度 / 轮询 / 分块远端 / 拼接
//     游标），末尾追加一个总总结块；json 模式改为每块一行 JSON（JSONL）。
//
// 关键数据来源：
//   - 客户端字节 = entry.written（既有热路径原子累加，写客户端与累积缓冲同源）；
//   - 代理侧字节 = 单源时同 written；挂了多源合并器时 = mergerBase + Σ(各源 readPos-start)
//     （各源可能领先客户端游标，这正是「客户端进度 vs 代理进度」要区分的东西）；
//   - 分块行来自 slowMerger 的锁内快照（slowSource.name/start/readPos/eof/pruned），
//     单源速率用两次面板快照的 readPos 差 / 窗口算（首轮为 0）；
//   - 轮询行来自候选扫描时写入 context 的 panelRecorder（只输出本周期新增尝试）。
//
// 并发约定：面板线程只做「锁内拷快照、锁外格式化与写日志」，绝不持注册表锁或合并器锁
// 做 IO（与 emitProgressSummary 的既有约定一致）。
package proxy

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxy-cache/config"
	"proxy-cache/logging"
)

// //////////////////  候选轮询记录器（挂到下载条目上）  start  ////////////////////////////////

// panelRecorderKey 是 panelRecorder 在 context 中的键（候选扫描经 r.Context() 取用：
// 同一请求的候选尝试无论发生在哪个 goroutine，都写进同一个记录器）。
type panelRecorderKey struct{}

// panelRecorder 记录一个请求的全部候选尝试（面板轮询段的数据源）。
// 面板只展示「本周期新增」的尝试：entries 全量保留（有上限），entry 侧记住上次已展示
// 的条数，渲染时取新增部分。
type panelRecorder struct {
	mu    sync.Mutex
	polls []logging.PollEntry
}

// maxPanelPolls 单个下载保留的候选尝试上限（防病态配置下无限增长；超出丢弃最早的）。
const maxPanelPolls = 64

// newPanelRecorder 创建记录器。
func newPanelRecorder() *panelRecorder { return &panelRecorder{} }

// record 追加一条候选尝试。
func (r *panelRecorder) record(p logging.PollEntry) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if len(r.polls) >= maxPanelPolls {
		r.polls = append(r.polls[:0], r.polls[len(r.polls)-maxPanelPolls+1:]...)
	}
	if p.N <= 0 {
		p.N = len(r.polls) + 1 // 序号由记录器统一分配（跨轮次递增）
	}
	r.polls = append(r.polls, p)
	r.mu.Unlock()
}

// snapshot 返回全部尝试的快照（拷贝，锁外渲染）。
func (r *panelRecorder) snapshot() []logging.PollEntry {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]logging.PollEntry, len(r.polls))
	copy(out, r.polls)
	return out
}

// panelRecorderFrom 从请求 context 取记录器（无则 nil）。
func panelRecorderFrom(ctx context.Context) *panelRecorder {
	if ctx == nil {
		return nil
	}
	rec, _ := ctx.Value(panelRecorderKey{}).(*panelRecorder)
	return rec
}

// //////////////////  候选轮询记录器  end  //////////////////////////////////////////////////

// //////////////////  条目面板方法  start  //////////////////////////////////////////////////

// setPanelMeta 登记面板用的通道/总长来源/请求 id（各下载流登记后立即调用）。
func (e *progressSummaryEntry) setPanelMeta(via, totalSrc, requestID string) {
	if e == nil {
		return
	}
	e.panelMu.Lock()
	e.via = strings.TrimSpace(via)
	e.totalSrc = totalSrc
	e.requestID = requestID
	e.panelMu.Unlock()
}

// setPartial 登记半截续传信息（仅 resume 路径）。
func (e *progressSummaryEntry) setPartial(bytes, start, gap int64) {
	if e == nil {
		return
	}
	e.panelMu.Lock()
	e.partial = logging.PartialInfo{Present: true, Bytes: bytes, Start: start, Gap: gap}
	e.panelMu.Unlock()
}

// attachMerger 挂上多源合并器（挂上时已写给客户端的字节数作为代理侧进度基线）。
func (e *progressSummaryEntry) attachMerger(m *slowMerger) {
	if e == nil || m == nil {
		return
	}
	e.panelMu.Lock()
	e.merger = m
	e.mergerBase = e.written.Load()
	e.panelMu.Unlock()
}

// attachRecorder 挂上候选轮询记录器。
func (e *progressSummaryEntry) attachRecorder(rec *panelRecorder) {
	if e == nil || rec == nil {
		return
	}
	e.panelMu.Lock()
	e.recorder = rec
	e.panelMu.Unlock()
}

// finish 标记下载收尾结果（完成 / 客户端断开 / 失败…）：注销时据此进入面板的收尾块与
// 汇总的完成/失败计数。text 模式下注册表未启用面板，本方法只写一个字段，零行为变化。
func (e *progressSummaryEntry) finish(result string) {
	if e == nil || result == "" {
		return
	}
	e.panelMu.Lock()
	e.result = result
	e.panelMu.Unlock()
}

// panelState 拷贝面板渲染需要的稳定字段（锁内）。
func (e *progressSummaryEntry) panelState() (via, totalSrc, requestID, result string, partial logging.PartialInfo, merger *slowMerger, mergerBase int64, rec *panelRecorder) {
	e.panelMu.Lock()
	defer e.panelMu.Unlock()
	return e.via, e.totalSrc, e.requestID, e.result, e.partial, e.merger, e.mergerBase, e.recorder
}

// //////////////////  条目面板方法  end  ////////////////////////////////////////////////////

// //////////////////  合并器快照（分块段数据源）  start  ////////////////////////////////////////

// slowMerger.panelSnapshot 锁内复制各源状态并推进面板速率基线，锁外格式化：
// 返回每源一行（序号/区间/远端 host/已收/速率/状态）、拼接游标、缺口、存活/总数。
func (m *slowMerger) panelSnapshot(now time.Time) (chunks []logging.ChunkEntry, cursor, gap, gapOff int64, alive, total, pruned int, finished bool) {
	if m == nil {
		return nil, 0, 0, 0, 0, 0, 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cursor = m.next
	finished = m.finished
	if m.knownTotal > 0 && m.next < m.knownTotal && (m.finished || m.allSourcesDeadLocked()) {
		// 全部源耗尽仍未到总长：剩余区间即缺口（合并器保证已写部分连续，缺口只可能在尾部）
		gap = m.knownTotal - m.next
		gapOff = m.next
	}
	for i, src := range m.sources {
		got := src.readPos - src.start
		if got < 0 {
			got = 0
		}
		state := "下载中"
		reason := ""
		switch {
		case src.pruned != "":
			state, reason = "剪枝", src.pruned
		case src.pending:
			state = "待定"
		case src.eof:
			state = "EOF"
		case src.readPos == src.panelLastPos && now.Sub(src.panelLastAt) > 5*time.Second:
			state = "停滞"
		}
		speed := 0.0
		if !src.panelLastAt.IsZero() {
			dt := now.Sub(src.panelLastAt).Seconds()
			if dt > 0 {
				speed = float64(src.readPos-src.panelLastPos) / dt
				if speed < 0 {
					speed = 0
				}
			}
		}
		src.panelLastPos, src.panelLastAt = src.readPos, now
		if src.pruned != "" {
			pruned++
		} else if !src.eof {
			alive++
		}
		total++
		chunks = append(chunks, logging.ChunkEntry{
			N:        i + 1,
			Start:    src.start,
			End:      src.end,
			HasEnd:   src.end > src.start,
			Remote:   hostOf(src.name),
			State:    state,
			Got:      got,
			SpeedBPS: speed,
			Reason:   reason,
		})
	}
	return chunks, cursor, gap, gapOff, alive, total, pruned, finished
}

// allSourcesDeadLocked 是否全部源已死（剪枝/失败/EOF 且无缓冲可消费）；m.mu 下调用。
func (m *slowMerger) allSourcesDeadLocked() bool {
	for _, src := range m.sources {
		if src.pruned != "" {
			continue
		}
		if !src.eof || src.bufBytes > 0 || src.pending {
			return false
		}
	}
	return true
}

// //////////////////  合并器快照  end  ////////////////////////////////////////////////////////

// //////////////////  面板循环与渲染  start  //////////////////////////////////////////////////

// StartLogPanel 启动下载面板协程（main.go 在启动期调用）：
// 仅 LOG_TYPE=line|json 且 log.panel-interval>0 时启动，随 ctx 取消退出（无泄漏）。
// 幂等：重复调用只启动一个协程。
func (s *Server) StartLogPanel(ctx context.Context) {
	if s.cfg == nil || !s.cfg.LogPanelEnabled() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.panelOnce.Do(func() {
		reg := s.progressOrInit()
		reg.panelEnabled.Store(true)
		go s.logPanelLoop(ctx, s.cfg.LogPanelInterval(), s.cfg.LogType() == config.LogTypeJSON)
	})
}

// SetPanelWriter 替换面板输出目标（测试注入 buffer 用；默认 os.Stderr）。
func (s *Server) SetPanelWriter(w io.Writer) {
	if w == nil {
		return
	}
	s.panelOut = w
}

// panelWriter 返回面板输出目标（默认 stderr）。
func (s *Server) panelWriter() io.Writer {
	if s.panelOut != nil {
		return s.panelOut
	}
	return os.Stderr
}

// logPanelLoop 面板定时循环：每 interval 渲染一轮（无变化静默跳过）。
func (s *Server) logPanelLoop(ctx context.Context, interval time.Duration, jsonMode bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.emitLogPanel(now, jsonMode)
		}
	}
}

// emitLogPanel 输出一轮面板：每个活跃下载一块 + 一个汇总块；本轮无变化则整轮静默。
func (s *Server) emitLogPanel(now time.Time, jsonMode bool) {
	reg := s.progressOrInit()
	entries, started, finishedBlocks := reg.panelSnapshot(now)
	if len(entries) == 0 && started == 0 && len(finishedBlocks) == 0 {
		return
	}
	blocks := make([]logging.DownloadBlock, 0, len(entries)+len(finishedBlocks))
	changed := started > 0 || len(finishedBlocks) > 0
	for _, e := range entries {
		b, ch := s.buildDownloadBlock(e, now)
		blocks = append(blocks, b)
		if ch {
			changed = true
		}
	}
	// 收尾块：本周期内已结束的下载，各输出一次（带「结果」行）后丢弃
	blocks = append(blocks, finishedBlocks...)
	if !changed {
		return
	}
	summary := buildPanelSummary(now, started, blocks, finishedBlocks)
	out := s.panelWriter()
	for _, b := range blocks {
		if jsonMode {
			if line := logging.JSONDownload(b); line != "" {
				fmt.Fprintln(out, line)
			}
			continue
		}
		fmt.Fprintln(out, logging.RenderDownload(b))
	}
	if jsonMode {
		if line := logging.JSONSummary(summary); line != "" {
			fmt.Fprintln(out, line)
		}
		return
	}
	fmt.Fprintln(out, logging.RenderSummary(summary))
}

// buildDownloadBlock 把一个活跃下载条目渲染成面板块，并做「本轮是否有变化」判定。
func (s *Server) buildDownloadBlock(e *progressSummaryEntry, now time.Time) (logging.DownloadBlock, bool) {
	via, totalSrc, requestID, result, partial, merger, mergerBase, rec := e.panelState()
	written := e.written.Load()
	upstream := written
	var chunks []logging.ChunkEntry
	var cursor, gap, gapOff int64
	var alive, total, pruned int
	chunkState := ""
	singleHost := ""
	if merger != nil {
		chunks, cursor, gap, gapOff, alive, total, pruned, _ = merger.panelSnapshot(now)
		var pulled int64
		var states []string
		for _, c := range chunks {
			pulled += c.Got
			states = append(states, c.State)
		}
		upstream = mergerBase + pulled
		chunkState = strings.Join(states, ",")
		if len(chunks) == 1 {
			singleHost = chunks[0].Remote
		}
	}
	// 区间速率：与上次面板快照求差（首轮为 0；面板节拍即统计窗口）
	e.panelMu.Lock()
	prev := e.panelPrev
	dt := now.Sub(prev.at).Seconds()
	clientBPS, upstreamBPS := 0.0, 0.0
	if prev.at.IsZero() {
		// 首轮没有上一帧基线：用「自下载开始」当窗口，避免第一块总是 0B/s
		if el := now.Sub(e.startedAt).Seconds(); el > 0 {
			clientBPS = float64(written) / el
			upstreamBPS = float64(upstream) / el
		}
	} else if dt > 0 {
		clientBPS = float64(written-prev.written) / dt
		upstreamBPS = float64(upstream-prev.upstream) / dt
		if clientBPS < 0 {
			clientBPS = 0
		}
		if upstreamBPS < 0 {
			upstreamBPS = 0
		}
	}
	e.panelPrev = struct {
		written  int64
		upstream int64
		at       time.Time
	}{written: written, upstream: upstream, at: now}
	lastPolls := e.lastPanelPolls
	e.panelMu.Unlock()

	elapsed := now.Sub(e.startedAt)
	avg := 0.0
	if elapsed > 0 {
		avg = float64(written) / elapsed.Seconds()
	}
	// 轮询：只展示上次面板之后新增的尝试；没有新增则折叠成「累计 N 次 · 最近 …」
	var polls []logging.PollEntry
	pollTotal := 0
	if rec != nil {
		all := rec.snapshot()
		pollTotal = len(all)
		if lastPolls < len(all) {
			polls = all[lastPolls:]
			if len(polls) > 8 {
				polls = polls[len(polls)-8:]
			}
		}
	}
	hasLast := pollTotal > 0
	var lastPoll logging.PollEntry
	if hasLast {
		lastPoll = rec.snapshot()[pollTotal-1]
	}
	block := logging.DownloadBlock{
		ID: e.id, Mode: e.source, Image: e.image, Digest: e.digest,
		Method: e.method, Target: "", Req: requestID,
		Via: via, Total: e.totalLength(), TotalSrc: totalSrc,
		ClientBytes: written, UpstreamBytes: upstream,
		ClientState: "ok", Partial: partial,
		SpeedBPS: clientBPS, UpstreamSpeedBPS: upstreamBPS, AvgBPS: avg,
		Elapsed: elapsed,
		Polls:   polls, PollTotal: pollTotal, PollsFolded: len(polls) == 0,
		LastPoll: lastPoll, HasLastPoll: hasLast,
		Chunks: chunks, ChunkTotal: total, ChunkAlive: alive, ChunkPruned: pruned,
		SingleSource: total <= 1, SourceHost: singleHost,
		Cursor: cursor, Gap: gap, GapOff: gapOff,
		Result: result,
	}
	if len(chunks) > 8 {
		idle, eof := 0, 0
		for _, c := range chunks[8:] {
			switch c.State {
			case "下载中":
				idle++
			case "EOF":
				eof++
			}
		}
		block.Chunks = chunks[:8]
		block.ChunksFolded = len(chunks) - 8
		block.ChunkFoldedIdle = idle
		block.ChunkFoldedEOF = eof
	}
	// 变化检测：字节数/代理侧/结果/候选新增/分块状态任一变化即视为「本轮有变化」
	fingerprint := fmt.Sprintf("%d|%d|%s|%d|%s", written, upstream, result, pollTotal, chunkState)
	e.panelMu.Lock()
	changed := fingerprint != e.lastPanelFingerprint
	e.lastPanelFingerprint = fingerprint
	e.lastPanelPolls = pollTotal
	e.panelMu.Unlock()
	return block, changed
}

// totalLength 读条目总长（注册表锁内，与 setTotal 一致）。
func (e *progressSummaryEntry) totalLength() int64 {
	e.reg.mu.Lock()
	defer e.reg.mu.Unlock()
	return e.total
}

// buildPanelSummary 组装汇总块：活跃数按来源分布、吞吐、本轮新登记/完成/失败、缓存写入。
func buildPanelSummary(now time.Time, started uint64, blocks []logging.DownloadBlock, finished []logging.DownloadBlock) logging.SummaryBlock {
	sum := logging.SummaryBlock{Time: now, New: int(started)}
	var clientBPS, prefetchBPS float64
	for _, b := range blocks {
		switch b.Mode {
		case dockerLogSourceClient:
			sum.Client++
			clientBPS += b.SpeedBPS
		case dockerLogSourceRidealong:
			sum.Ridealong++
			clientBPS += b.SpeedBPS
		case dockerLogSourceResume:
			sum.Resume++
			clientBPS += b.SpeedBPS
		case dockerLogSourcePrefetch:
			sum.Prefetch++
			prefetchBPS += b.UpstreamSpeedBPS
		}
		if b.Result == "" {
			sum.Details = append(sum.Details, fmt.Sprintf("dl#%d %s %d%%", b.ID, b.Mode, percentOfInt(b.UpstreamBytes, b.Total)))
		}
	}
	for _, b := range finished {
		if b.Result == "完成" || b.Result == "已完成" {
			sum.Done++
			sum.CacheWritten += b.ClientBytes
			continue
		}
		sum.Failed++
	}
	sum.ClientBPS, sum.PrefetchBPS = clientBPS, prefetchBPS
	sum.TotalBPS = clientBPS + prefetchBPS
	sum.Active = sum.Client + sum.Ridealong + sum.Resume + sum.Prefetch
	sort.Slice(sum.Details, func(i, j int) bool { return sum.Details[i] < sum.Details[j] })
	return sum
}

// percentOfInt 0-100 整数百分比（总长未知返回 0）。
func percentOfInt(n, total int64) int {
	if total <= 0 || n <= 0 {
		return 0
	}
	p := int(float64(n) / float64(total) * 100)
	if p > 100 {
		p = 100
	}
	return p
}

// //////////////////  面板循环与渲染  end  ////////////////////////////////////////////////////

// //////////////////  注册表面板快照  start  //////////////////////////////////////////////////

// panelSnapshot 面板一轮的注册表快照：活跃条目（按 id 升序）、本轮新登记数、本周期已
// 结束下载的收尾块（取走即清空）。text 模式不调用本方法。
func (r *progressRegistry) panelSnapshot(now time.Time) ([]*progressSummaryEntry, uint64, []logging.DownloadBlock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	started := r.started
	r.started = 0
	finished := r.finishedBlocks
	r.finishedBlocks = nil
	if len(r.m) == 0 {
		return nil, started, finished
	}
	rows := make([]*progressSummaryEntry, 0, len(r.m))
	for _, e := range r.m {
		rows = append(rows, e)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows, started, finished
}

// //////////////////  注册表面板快照  end  ////////////////////////////////////////////////////

// //////////////////  小工具  start  //////////////////////////////////////////////////////////

// panelActive 面板是否启用（LOG_TYPE=line|json 且 panel-interval>0）；text 模式恒 false。
func (s *Server) panelActive() bool {
	return s.cfg != nil && s.cfg.LogPanelEnabled()
}

// newPanelRecorderIfNeeded 面板模式下创建候选尝试记录器，text 模式返回 nil（零开销）。
func (s *Server) newPanelRecorderIfNeeded() *panelRecorder {
	if !s.panelActive() {
		return nil
	}
	return newPanelRecorder()
}

// resultTagged 条目的收尾结果是否已标记（注销时据此决定是否进收尾块）。
func (e *progressSummaryEntry) resultTagged() bool {
	e.panelMu.Lock()
	defer e.panelMu.Unlock()
	return e.result != ""
}

// recordPanelPoll 把一次候选尝试写进请求上下文里的记录器（无记录器时静默跳过）。
func recordPanelPoll(ctx context.Context, p logging.PollEntry) {
	rec := panelRecorderFrom(ctx)
	if rec == nil {
		return
	}
	rec.record(p)
}

// hostOf 从候选/源 URL 取 host（面板只显示远端 host，不显示整条长 URL）；
// 解析失败回退截断后的原始地址。
func hostOf(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "—"
	}
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Host
	}
	return truncate(raw, 60)
}

// requestIDSep 请求 id 生成器（8 位十六进制，进程内自增；仅用于日志关联）。
var requestIDSeq atomic.Uint64

// newRequestID 生成一个请求关联 id（面板/JSON 的 req 键）。
func newRequestID() string {
	return fmt.Sprintf("%08x", requestIDSeq.Add(1))
}

// //////////////////  小工具  end  ////////////////////////////////////////////////////////////
