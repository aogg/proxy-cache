// slowdownload.go —— 慢速升级多源并发续传（slow-download escalation）：
// docker 类（type=docker 规则）GET 路径目标流式回源 commit 后，滑动窗口平均送出速率
// 低于 slow-download-speed 判「过慢」且剩余量足够、候选数>1 时，向其余全部候选并发发起
// `Range: bytes={offset}-{end}` 续传，胜者既有连接不中断并入合并；合并循环按绝对偏移
// 严格有序写给客户端与 accum（缓存语义不变），落后的源按 max-behind 剪枝关闭收敛带宽放大。
//
// 连续性保证不变：升级只增开续传源，绝不动客户端已见字节（不切状态码/响应头，不回退）。
// 每源读继续走 progressReader+watchdog（无新增数据超时语义不变）；候选并发数上限 =
// 候选列表长度（一般 ≤5），不做额外并发池。

package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"proxy-cache/config"
)

// 慢速升级多源并发续传的内部常量。
const (
	// slowSourceBufferLimit 每源 FIFO 缓冲上限：缓冲满暂停读该源（上游 TCP 窗口随之
	// 收敛）。内存上界 = 源数×4MB（源数 = 候选列表长度，一般 ≤5）。
	slowSourceBufferLimit = 4 << 20
	// slowMergeWaitTick 合并循环在无就绪数据时的最长等待：既兜底通知单 token 被
	// 其他等待方消费的极端情形（防漏唤醒卡死），也让升级后的进度日志保持周期节拍。
	slowMergeWaitTick = 200 * time.Millisecond
)

// //////////////////  纯逻辑（可单测）  //////////////////////////////////////////////////////

// parseContentRange 解析 Content-Range 头（形如 "bytes {start}-{end}/{total}"，
// 前缀与空白容忍、大小写不敏感）：total 为 "*"（总长不可知）或格式不符时 ok=false。
func parseContentRange(v string) (start, end, total int64, ok bool) {
	v = strings.TrimSpace(v)
	if i := strings.IndexByte(v, ' '); i >= 0 {
		v = strings.TrimSpace(v[i+1:]) // 去掉 "bytes " 前缀（scheme 名不参与判定）
	}
	rangePart, totalPart, found := strings.Cut(v, "/")
	if !found {
		return 0, 0, 0, false
	}
	totalPart = strings.TrimSpace(totalPart)
	if totalPart == "*" {
		return 0, 0, 0, false // 总长不可知：无法判定剩余量，不支持升级
	}
	t, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	sPart, ePart, found := strings.Cut(strings.TrimSpace(rangePart), "-")
	if !found {
		return 0, 0, 0, false
	}
	sv, err1 := strconv.ParseInt(strings.TrimSpace(sPart), 10, 64)
	ev, err2 := strconv.ParseInt(strings.TrimSpace(ePart), 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, 0, false
	}
	return sv, ev, t, true
}

// slowEscalationTarget 依据胜者响应解析升级续传的目标区间（纯函数）：
//   - 胜者 200 + Content-Length：胜者流从 0 开始，knownTotal = 总长，end = 总长-1；
//   - 胜者 206：从 Content-Range 解析（起始偏移 / 区间终点 / 对象总长）；
//   - 其余状态码、总长不可知或解析失败 → ok=false（该流不支持升级，调用方静默跳过并
//     debug 留痕）。
func slowEscalationTarget(status int, header http.Header) (winnerStart, knownTotal, end int64, ok bool) {
	switch status {
	case http.StatusOK:
		cl, err := strconv.ParseInt(strings.TrimSpace(header.Get("Content-Length")), 10, 64)
		if err != nil || cl <= 0 {
			return 0, 0, 0, false
		}
		return 0, cl, cl - 1, true
	case http.StatusPartialContent:
		start, e, total, pok := parseContentRange(header.Get("Content-Range"))
		if !pok || total <= 0 {
			return 0, 0, 0, false
		}
		return start, total, e, true
	}
	return 0, 0, 0, false
}

// windowAvgSpeed 计算窗口平均送出速率（B/s）；d<=0（时钟回拨等异常）返回 0 兜底。
func windowAvgSpeed(windowSent int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(windowSent) / d.Seconds()
}

// slowEscalationDue 触发判定（纯函数）：窗口满后窗口平均送出速率低于判慢阈值（B/s）
// 即判「过慢」。
func slowEscalationDue(windowSent int64, windowElapsed time.Duration, speedBPs int64) bool {
	return windowAvgSpeed(windowSent, windowElapsed) < float64(speedBPs)
}

// slowEscalationRemaining 计算剩余字节（已知总长 - 当前绝对位置）；knownTotal<=0
// （总长不可知）返回 -1 表示永不满足触发条件。
func slowEscalationRemaining(knownTotal, winnerStart, consumed int64) int64 {
	if knownTotal <= 0 {
		return -1
	}
	return knownTotal - (winnerStart + consumed)
}

// escalationCandidates 返回除胜者外的全部候选（保留原顺序；此前竞速中硬失败的候选也
// 照常尝试，失败即剪枝留 debug 痕）。
func escalationCandidates(candidates []candidate, winnerURL string) []candidate {
	others := make([]candidate, 0, len(candidates))
	for _, c := range candidates {
		if c.url == winnerURL {
			continue
		}
		others = append(others, c)
	}
	return others
}

// resumeRangeHeader 构造续传 Range 头：bytes={offset}-{end}。
func resumeRangeHeader(offset, end int64) string {
	return fmt.Sprintf("bytes=%d-%d", offset, end)
}

// validateResumeResponse 校验续传响应：必须 206 且 Content-Range 起始等于请求偏移
// （上游不支持 Range 会回 200 整段 → 丢弃该源，debug 留痕）。
func validateResumeResponse(status int, header http.Header, offset int64) error {
	if status != http.StatusPartialContent {
		return fmt.Errorf("续传响应状态码 %d ≠ 206（上游可能不支持 Range）", status)
	}
	start, _, _, ok := parseContentRange(header.Get("Content-Range"))
	if !ok {
		return fmt.Errorf("续传响应缺少可解析的 Content-Range 头")
	}
	if start != offset {
		return fmt.Errorf("续传响应 Content-Range 起始 %d ≠ 请求偏移 %d", start, offset)
	}
	return nil
}

// //////////////////  源与有序合并  //////////////////////////////////////////////////////////

// slowSource 是多源合并中的一个并发续传源：一条活体上游连接（胜者既有连接或新开的
// Range 续传连接）+ 一个 FIFO 缓冲（cap slowSourceBufferLimit）+ 绝对游标。
type slowSource struct {
	// name 源 URL（观测与日志，truncate 后输出）。
	name string
	// res 活体上游结果（pump 的数据来源；续传尝试失败/被丢弃时为 nil）。
	res *probeResult
	// start 本源流的起始绝对偏移（Range 请求 offset；胜者 = 其流自身的起始偏移）。
	start int64
	// readPos 流已读到（并进入缓冲）的绝对偏移：readPos - bufBytes = 缓冲首块的绝对偏移。
	readPos int64
	// buf FIFO 缓冲（各块按绝对偏移连续；pump 追加、take 消费/丢弃）。
	buf      [][]byte
	bufBytes int
	// eof 上游流已到 EOF；err 上游读错误（非 nil 时该源被剪枝）。
	eof bool
	err error
	// pending true = 续传尝试尚未定局（goroutine 进行中）：保持 live 占位不出数据，
	// 定局（成功自行 pump / 失败置 err）后由合并循环按常规规则处理。
	pending bool
	// body/wd pump 数据通道快照：在确定 res 之后、go pump() 之前一次性捕获
	// （普通字段写，goroutine 创建语句自带 happens-before）。pump 全程只读这两个
	// 自有字段，绝不触碰 src.res.body / src.res.wd——take/closeAll 在锁外调用的
	// res.Close()（probeResult.Close）会把 res.body 写置 nil，与 pump 启动瞬间对
	// 该字段的无锁读取构成数据竞争。
	body io.Reader
	wd   *progressWatchdog
	// pumping true = 该源的 pump 读取协程已启动（m.mu 下读写，防止 start 与续传协程
	// 对同一源重复启动 pump 造成数据重复）。
	pumping bool
	// pruned 非空 = 已剪枝（剪枝 reason，用于留痕与去重）。
	pruned string
}

// slowMerger 有序合并器：客户端严格按偏移有序写——每次取「持有下一期望偏移」的就绪
// 数据写给客户端与 accum；源出错、EOF 已尽且游标落后、或落后领先源超过 maxBehind
// 即剪枝关闭；游标推进到已知总长正常收尾，全部源耗尽未到总长则按失败收尾。
type slowMerger struct {
	// mu 保护 sources 状态与 next（读取协程与合并循环并发）。
	mu sync.Mutex
	// sources 全部源（胜者在首位）。
	sources []*slowSource
	// next 下一期望偏移（有序写给客户端的绝对游标）。
	next int64
	// knownTotal 已知对象总长（游标到达即成功收尾）。
	knownTotal int64
	// maxBehind 剪枝阈值：源落后领先源超过该字节数即被关闭。
	maxBehind int64
	// lastErr 最后一个源错误（全部源耗尽时并入失败错误，便于排查）。
	lastErr error
	// notify 源状态变化通知（cap 冗余，changed 非阻塞发送，防漏唤醒）。
	notify chan struct{}
	// log 结构化日志器（剪枝/收尾留痕）。
	log *slog.Logger
}

// newSlowMerger 创建合并器；调用 run 前源尚未开始异步填充。
func newSlowMerger(sources []*slowSource, next, knownTotal, maxBehind int64, log *slog.Logger) *slowMerger {
	return &slowMerger{
		sources:    sources,
		next:       next,
		knownTotal: knownTotal,
		maxBehind:  maxBehind,
		notify:     make(chan struct{}, len(sources)+2),
		log:        log,
	}
}

// changed 非阻塞通知合并循环「某源状态已变化」（追加数据 / EOF / 错误 / 剪枝）。
func (m *slowMerger) changed() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// start 为有活体 body 的既有源（胜者）启动读取协程；pending 续传源由其尝试协程
// 自行启动 pump（定局前保持 live 占位）。pumping 标记（m.mu 下读写）保证同一源
// 绝不被重复启动 pump。候选并发数上限 = 候选列表长度，不做额外并发池。
func (m *slowMerger) start() {
	for _, src := range m.sources {
		m.mu.Lock()
		if src.pending || src.pumping {
			m.mu.Unlock()
			continue
		}
		if src.res != nil && src.res.body != nil && !src.res.eof {
			src.body, src.wd = src.res.body, src.res.wd // pump 数据通道快照：go pump 前一次性捕获
			src.pumping = true
			m.mu.Unlock()
			go src.pump(m)
			continue
		}
		src.eof = true // 无剩余数据（胜者 body 已耗尽）：交由剪枝规则处理
		m.mu.Unlock()
	}
}

// pump 源读取协程：持续把本源上游 body 读入 FIFO 缓冲。经 progressReader 记入看门狗
// （read-timeout 无新增数据超时语义与单源一致）；缓冲满暂停读该源（内存上界 = 源数×4MB），
// 被剪枝（连接已由剪枝方关闭）后立即收手。
func (src *slowSource) pump(m *slowMerger) {
	defer m.changed()
	pr := &progressReader{r: src.body, wd: src.wd} // 每源读继续走 progressReader+watchdog（pump 自有快照字段，不再触碰 src.res）
	buf := make([]byte, streamChunkSize)
	for {
		m.mu.Lock()
		pruned := src.pruned != ""
		full := src.bufBytes >= slowSourceBufferLimit
		m.mu.Unlock()
		if pruned {
			return
		}
		if full {
			// 缓冲满：暂停读该源（内存上界 = 源数×4MB），等合并循环消费/丢弃释放空间；
			// tick 兜底防共享通知的单 token 被其他等待方消费后漏唤醒
			select {
			case <-m.notify:
			case <-time.After(slowMergeWaitTick):
			}
			continue
		}
		n, rerr := pr.Read(buf)
		m.mu.Lock()
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			src.buf = append(src.buf, chunk)
			src.bufBytes += n
			src.readPos += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				src.eof = true
			} else {
				src.err = rerr
			}
			m.mu.Unlock()
			m.changed()
			return
		}
		m.mu.Unlock()
		m.changed()
	}
}

// take 在锁内做一轮「剪枝 + 丢弃过期前缀 + 取就绪块」，返回可写给客户端的块
// （nil = 本轮无进展）。剪枝规则：源出错、EOF 已尽且游标落后（流未完成却再无数据）、
// 或落后领先源（next 游标）超过 maxBehind。
func (m *slowMerger) take() []byte {
	type pruneEvent struct {
		name   string
		reason string
		level  slog.Level
	}
	var (
		events  []pruneEvent
		toClose []*slowSource
	)
	m.mu.Lock()
	for _, src := range m.sources {
		if src.pruned != "" {
			continue
		}
		reason := ""
		level := slog.LevelDebug
		switch {
		case src.err != nil:
			reason = fmt.Sprintf("error: %v", src.err)
			m.lastErr = src.err
		case src.eof && src.bufBytes == 0 && m.next < m.knownTotal:
			reason = fmt.Sprintf("eof-behind: 上游已尽于 %d，流未完成（%d/%d）", src.readPos, m.next, m.knownTotal)
		case m.next-src.readPos > m.maxBehind:
			reason = fmt.Sprintf("behind: 落后领先源 %d 字节（max-behind %d）", m.next-src.readPos, m.maxBehind)
			level = slog.LevelInfo // 超限剪枝是带宽放大收敛的关键动作，Info 留痕
		}
		if reason != "" {
			src.pruned = reason
			events = append(events, pruneEvent{name: src.name, reason: reason, level: level})
			if src.res != nil {
				toClose = append(toClose, src)
			}
		}
	}
	// 丢弃过期前缀（偏移已落后 next 的缓冲字节绝不会再写给客户端），随后取
	//「持有下一期望偏移（缓冲首块绝对偏移 == next）」的就绪块（取到即推进游标）。
	var chunk []byte
	dropped := 0 // 本轮释放的缓冲字节数（丢弃过期 + 消费就绪），需唤醒满缓冲暂停的读取协程
	for _, src := range m.sources {
		if src.pruned != "" {
			continue
		}
		for src.bufBytes > 0 {
			head := src.buf[0]
			headFrom := src.readPos - int64(src.bufBytes)
			if headFrom >= m.next {
				break
			}
			if headFrom+int64(len(head)) <= m.next {
				src.buf = src.buf[1:] // 整块过期：弹出
				src.bufBytes -= len(head)
				dropped += len(head)
				continue
			}
			cut := int(m.next - headFrom) // 跨界块：只丢过期前缀，剩余部分仍有效
			src.buf[0] = head[cut:]
			src.bufBytes -= cut
			dropped += cut
			break
		}
		if src.bufBytes == 0 {
			continue
		}
		if headFrom := src.readPos - int64(src.bufBytes); headFrom == m.next {
			chunk = src.buf[0]
			src.buf = src.buf[1:]
			src.bufBytes -= len(chunk)
			dropped += len(chunk)
			m.next += int64(len(chunk))
			break
		}
	}
	alive := 0
	for _, src := range m.sources {
		if src.pruned == "" {
			alive++
		}
	}
	m.mu.Unlock()
	// 锁外收尾：关闭被剪枝源、唤醒可能在等缓冲空间的读取协程、输出剪枝留痕
	for _, src := range toClose {
		src.res.Close() // 幂等：关闭上游连接、停看门狗、取消超时预算链
	}
	if len(events) > 0 || dropped > 0 {
		m.changed() // 有剪枝或缓冲空间释放：唤醒等通知的合并循环/读取协程（防漏唤醒卡死）
	}
	for _, e := range events {
		m.log.Log(context.Background(), e.level, "慢速升级多源续传源已剪枝",
			"candidate", truncate(e.name, 300), "reason", e.reason,
			"next", m.next, "total", m.knownTotal, "live_sources", alive)
	}
	return chunk
}

// aliveCount 返回当前存活（未剪枝）源数。
func (m *slowMerger) aliveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	alive := 0
	for _, src := range m.sources {
		if src.pruned == "" {
			alive++
		}
	}
	return alive
}

// closeAll 关闭全部仍未剪枝的源（成功收尾 / 失败收尾时统一释放连接与看门狗，幂等）。
func (m *slowMerger) closeAll(reason string) {
	m.mu.Lock()
	var toClose []*slowSource
	for _, src := range m.sources {
		if src.pruned == "" && src.res != nil {
			src.pruned = reason
			toClose = append(toClose, src)
		}
	}
	m.mu.Unlock()
	for _, src := range toClose {
		src.res.Close()
		m.log.Debug("慢速升级多源续传源已关闭", "candidate", truncate(src.name, 300), "reason", reason)
	}
}

// run 有序合并主循环：把就绪块按序交给 write（客户端 + accum），游标推进到已知总长
// 成功返回；全部源耗尽仍未到总长按失败返回（错误交调用方走「commit 后读上游失败」
// 既有语义）。progress 为进度日志回调（与单源读循环同一节拍，参数为当前存活源数）。
func (m *slowMerger) run(write func([]byte), progress func(now time.Time, live int)) (int64, error) {
	m.start()
	timer := time.NewTimer(slowMergeWaitTick)
	defer timer.Stop()
	for {
		for {
			chunk := m.take()
			if chunk == nil {
				break
			}
			write(chunk)
		}
		m.mu.Lock()
		next, total := m.next, m.knownTotal
		m.mu.Unlock()
		if next >= total {
			// 游标推进到已知总长：完整对象已全部写给客户端与 accum，
			// 正常走既有 EOF 收尾（finalize 写缓存/206 不写缓存、成功返回日志）
			m.closeAll("completed")
			return next, nil
		}
		live := m.aliveCount()
		if live == 0 {
			// 所有源全部耗尽且未到总长：沿用「commit 后读上游失败」语义
			//（Error 日志、终止响应、不写缓存，由调用方返回错误）
			return next, fmt.Errorf("多源并发续传所有源耗尽，未达已知总长（%d/%d 字节）: 最后错误: %v",
				next, total, m.lastErr)
		}
		progress(time.Now(), live)
		select {
		case <-m.notify:
		case <-timer.C:
			timer.Reset(slowMergeWaitTick)
		}
	}
}

// //////////////////  升级动作（Server 入口）  ////////////////////////////////////////////////

// escalateSlowSources 升级动作（触发时调用一次）：
//   - 胜者既有连接不中断，作为首个源并入合并（天然从当前已传偏移继续）；
//   - 其余全部候选（排除胜者；此前竞速中硬失败的候选也照常尝试）**并发**发起
//     `Range: bytes={offset}-{end}` 续传——每个候选一个 goroutine（候选并发数上限 =
//     候选列表长度），尝试本体复用 startUpstream 公共底层（请求头白名单、docker token
//     dance、看门狗、read/http-timeout、候选级 insecure），严禁另复制一份 dance 逻辑。
//
// 返回的合并器已开始异步填充（胜者与已定局续传源的 pump 运行中，未定局源保持 pending）。
func (s *Server) escalateSlowSources(r *http.Request, winner *probeResult, winnerStart int64, others []candidate, eff config.Options, rangeHeader string, consumed, knownTotal int64) *slowMerger {
	offset := winnerStart + consumed
	sources := make([]*slowSource, 0, len(others)+1)
	// 胜者源：既有连接不中断（编号首位，合并时天然优先持有下一期望偏移）
	sources = append(sources, &slowSource{name: winner.url, start: winnerStart, readPos: offset, res: winner})
	// 其余全部候选：pending 占位占住候选顺序，续传尝试由下方 goroutine 并发完成
	for _, cand := range others {
		sources = append(sources, &slowSource{name: cand.url, start: offset, readPos: offset, pending: true})
	}
	m := newSlowMerger(sources, offset, knownTotal, eff.SlowDownloadMaxBehind, s.log)
	for i := range others {
		cand, src := others[i], sources[i+1]
		go func() {
			s.settleSlowResume(r, src, m, cand, eff, rangeHeader, offset)
			m.changed() // 定局通知（成功/失败皆然），合并循环免去 200ms tick 等待
		}()
	}
	return m
}

// settleSlowResume 单个候选的 Range 续传尝试（escalateSlowSources 的 goroutine 内执行）：
// 复用 startUpstream 公共底层（与候选探测同一套请求头/token dance/看门狗/超时/insecure）。
// 响应必须 206 且 Content-Range 起始等于请求偏移，否则丢弃该源；尝试失败同样只置 err
// （合并循环剪枝留 debug 痕）——绝不影响胜者流与客户端已见字节。成功且有活体 body 时
// 由本协程启动该源的 pump。全部状态写入在 m.mu 下进行（与合并循环并发安全）。
func (s *Server) settleSlowResume(r *http.Request, src *slowSource, m *slowMerger, cand candidate, eff config.Options, rangeHeader string, offset int64) {
	// 与客户端断开解耦（WithoutCancel）：docker 已断开也继续填缓存（与胜者流同一语义）
	res, err := s.startUpstream(context.WithoutCancel(r.Context()), r, cand, eff, rangeHeader, nil)
	if err != nil {
		m.mu.Lock()
		src.err = err
		m.mu.Unlock()
		s.log.Debug("慢速升级续传候选请求失败，该源将被剪枝",
			"candidate", truncate(cand.url, 300), "range", rangeHeader, "err", err)
		return
	}
	if verr := validateResumeResponse(res.status, res.header, offset); verr != nil {
		res.Close()
		m.mu.Lock()
		src.err = verr
		m.mu.Unlock()
		s.log.Debug("慢速升级续传响应不合规，丢弃该源",
			"candidate", truncate(cand.url, 300), "status", res.status, "offset", offset, "err", verr)
		return
	}
	m.mu.Lock()
	if src.pruned != "" {
		// 尝试期间该源已被剪枝（如落后超限，pending 期无 res 可关）：立即释放连接防泄漏
		m.mu.Unlock()
		res.Close()
		return
	}
	src.res = res
	// pump 数据通道快照：确定 res 之后、go pump 之前一次性捕获（m.mu 下写入，
	// goroutine 创建自带 happens-before；解锁后 take/closeAll 的锁外 res.Close
	// 不再与 pump 对该快照的读取产生竞争）。
	src.body, src.wd = res.body, res.wd
	if len(res.first) > 0 {
		// 探测首块即在请求偏移处：直接作为该源缓冲的第一块
		src.buf = append(src.buf, res.first)
		src.bufBytes = len(res.first)
		src.readPos = offset + int64(len(res.first))
	}
	live := res.body != nil && !res.eof
	if live {
		src.pending = false // 定局成功：源进入常规合并（pump 由本协程在锁外启动）
		src.pumping = true  // pump 启动权由本协程持有（与 merger.start 互斥）
	} else {
		src.pending = false
		src.eof = true // 续传响应已尽（总长恰为偏移等）：交由剪枝规则处理
	}
	m.mu.Unlock()
	if live {
		go src.pump(m)
	}
}
