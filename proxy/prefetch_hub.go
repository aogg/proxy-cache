// prefetch_hub.go —— docker 预取 blob 的在途流共享（hub）+ 磁盘前缀拼接搭车：
// 预取把 blob 流式下载写缓存临时文件时，逐块广播增量给订阅者；客户端 blob GET 缓存
// MISS 撞上「还在预取下载中」的层时，不再进 flight 等待（flightWaitLimit 10s 撑不住
// 大 layer 几十秒到几分钟的下载窗口，超限即自行回源 = 第二路完整下载且从 0 开始），
// 而是搭上预取那一路：响应用预取的响应头快照，body = 已落盘磁盘前缀（从 0 读临时
// 文件）+ 订阅增量 channel 的实时后续——上游只有预取一路下载，已下载部分直接复用。
//
// 生命周期：预取开始下载时登记（条目存在 = 在途）→ EOF 原子写缓存（cache.Set 临时
// 文件 + rename 语义由 finalizeFetchEntry 复用）后置 done 并关闭订阅（条目滞留一小段
// 时间供并发 MISS 兜底检出「done → 直接 HIT」，到期移除）→ 失败置 failed 广播后移除。
//
// 活性与降级（订阅等待增量必须有超时，取值与 docker 客户端约 30s 响应头超时同量级）：
//   - 等待响应头快照 / 等待下一段增量超过 prefetchHubIdleTimeout 即视为预取卡死：
//     尚未向客户端 commit 响应头 → 放弃搭车改走既有原回源路径（含 flight 等待与流式
//     回源，慢速升级等特性全保留）；
//   - 已 commit → 终止本次响应并记日志（客户端 docker 会对该层以 Range 断点续传重试，
//     既有「206 透传不写缓存」机制自然兜底「不从 0 重新下载」——重试命中已写盘的前缀
//     缓存则直接 HIT，缓存未就绪则回源 206，两路都不从头拉整层）。
//
// 边界约束：
//   - 仅无 Range 的完整 GET 搭车：检查点位于 handleGet 的缓存可读（非 BYPASS）MISS
//     分支——HEAD 走 handleHead 原路径；带 Range（206 续传）/ no-cache / 关缓存等
//     BYPASS 请求在进入检查前已分流，天然不搭车（no-cache 是客户端显式要最新内容）；
//   - 搭车路径不叠加 slowdownload 慢速升级多源并发续传：上游速率由预取那一路决定，
//     过慢时靠上述订阅超时降级 + 客户端 Range 重试兜底（不在搭车链路上再开第二路
//     续传源，避免与预取那一路重复下载同一 blob）；
//   - 客户端搭车中途断开：仅停止读取订阅（unsubscribe），预取不受影响、继续填缓存
//     （预取与客户端连接解耦——客户端流式主路径的「断开即停」只取消该请求自身的
//     回源流，见 stream.go，不波及预取那一路）；订阅消费过慢（增量缓冲满）被广播端摘除同理；
//   - 客户端读速快于预取下载速度：reader 天然阻塞等增量（背压），无需额外处理；
//   - 多客户端同时搭车各自独立订阅（独立增量 channel、逐订阅者独立 copy 广播块），
//     互不阻塞、互不干扰；
//   - docker-prefetch-enabled 关闭 → 无预取即无 hub 条目，搭车逻辑自然不生效，
//     不新增配置项。
//
// 与既有机制的分工：
//   - flight（flight.go）：客户端发起的回源合并逻辑保留不动——hub 登记前撞上预取的
//     极小窗口期客户端仍经 flight 合并（等待方语义不变）；反之预取等待客户端在途回源
//     的合并语义也不变（谁先到谁执行、其余等待共享结果）；
//   - prefetchInflight（prefetch.go）：预取排队阶段的防重复排队语义保留——排队阶段
//     尚无 hub 条目（信号量等待 / flight 排队期间客户端查 hub 不可见），该窗口内客户端
//     照走既有路径，属低概率退化而非错误；
//   - 临时文件：预取自管的 body 临时文件用 .tmp- 前缀落在缓存目录（与缓存写入临时
//     文件同前缀），进程崩溃残留由 cache.Clean 后台清理兜底。
//
// 日志规范：搭车侧日志（tryServeFromPrefetchHub 内，客户端请求订阅在途流）统一
// source=ridealong；预取执行方与写侧 hub 事件（条目登记 / 广播摘除订阅者 / 临时文件
// 清理）统一 source=prefetch；镜像名统一键 image（hub 条目登记时传入，搭车/广播日志
// 才能带上）。键名与顺序由 log_attrs.go 统一拼装。

package proxy

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// prefetchCacheStatus 搭车路径 X-Cache 观测头的取值：与 MISS / BYPASS / HIT 风格一致
// 的新值，标示本次内容来自「在途预取那一路」（磁盘前缀 + 实时增量拼接回放）。
const prefetchCacheStatus = "PREFETCH"

// prefetchHubIdleTimeout 搭车订阅的活性超时：等待预取侧响应头快照或下一段增量超过
// 该时长即视为预取那一路卡死/过慢，按「未 commit 降级 / 已 commit 终止」处理。
// 取值与 docker 客户端等响应头的约 30s 超时同量级（与 candidateScanBudget 的取值
// 依据一致，见 candidates.go）。
const prefetchHubIdleTimeout = 30 * time.Second

// prefetchHubSubBuffer 每个搭车订阅者的增量缓冲块数（块大小 = streamChunkSize，
// 单订阅者内存上界 = prefetchHubSubBuffer × 32KB = 8MB）：广播缓冲满即摘除该订阅者
// （消费过慢，客户端 docker 将以 Range 续传重试；预取那一路绝不被慢客户端拖住）。
const prefetchHubSubBuffer = 256

// prefetchHubDoneLinger done 条目在 hub 中的滞留时长：供缓存写入完成瞬间并发 MISS 的
// 请求兜底检出「done → 直接 HIT」（handleGet 开头的 disk.Get 通常已命中，该条目仅为
// 「查缓存在前、写盘在后」的极小竞态窗口兜底）；到期由定时器移除并清理临时文件，防泄漏。
const prefetchHubDoneLinger = time.Minute

// prefetchTmpPattern 预取自管 body 临时文件的创建模板（缓存目录内，.tmp- 前缀与缓存
// 写入临时文件一致——进程崩溃残留由 cache.Clean 后台清理兜底）。
const prefetchTmpPattern = ".tmp-prefetch-*"

// prefetchStreamState 在途流条目状态：running = 预取流式下载中（可搭车）；
// done = 完整落盘且缓存已写；failed = 预取失败（已广播并移除）。
type prefetchStreamState int

const (
	prefetchStreamRunning prefetchStreamState = iota
	prefetchStreamDone
	prefetchStreamFailed
)

// prefetchStreamHub Server 级在途流共享注册表：blob 缓存键 -> 在途下载条目。
// 条目存在 = 该 blob 正被预取流式下载（或在 done 滞留窗口内）。
type prefetchStreamHub struct {
	// mu 保护 m 的并发访问。
	mu sync.Mutex
	// m 在途条目表。
	m map[string]*prefetchStreamEntry
}

// prefetchStreamEntry hub 条目：一次预取 blob 在途流式下载的共享状态。
// 写者只有预取那一路（prefetchBlobStreaming）；订阅者（搭车客户端）只读快照与消费增量。
type prefetchStreamEntry struct {
	// key blob 缓存键（构造后只读）。
	key string
	// rule 生效规则名（日志观测，构造后只读）。
	rule string
	// image 镜像仓库名（日志统一标签，构造后只读；/v2/<name>/blobs/<digest> 的 name，
	// 见 log_attrs.go 的统一键规范——登记时传入，搭车/广播日志才能带 image）。
	image string
	// digest blob digest（日志观测，构造后只读）。
	digest string
	// hub 所属注册表（终态移除 / done 滞留登记用）。
	hub *prefetchStreamHub
	// log 结构化日志器（广播摘除 / 临时文件清理留痕）。
	log *slog.Logger

	mu sync.Mutex
	// state 条目状态（running / done / failed）。
	state prefetchStreamState
	// status 上游响应状态码快照（ready=true 后有效）。
	status int
	// header 上游响应头快照（probeResult.header 构建后只读，直接共享引用）。
	header http.Header
	// source 预取那一路轮询命中的候选 URL（X-Proxy-Upstream 观测头来源）。
	source string
	// ready 响应头快照是否就绪（候选竞速完成）。
	ready bool
	// err 最终错误（failed 时非 nil）。
	err error
	// written 已写入临时文件的字节数（订阅者据此读磁盘前缀）。
	written int64
	// tmpFile 预取 body 临时文件写句柄（EOF/失败后关闭置 nil）。
	tmpFile *os.File
	// tmpPath 临时文件路径（订阅者读前缀用）。
	tmpPath string
	// subs 搭车订阅者集合。
	subs map[*prefetchStreamSub]struct{}
	// changed 状态变化通知（快照就绪 / 终态时 close 广播，close-once）。
	changed chan struct{}
	// changedOnce 保证 changed 只 close 一次。
	changedOnce sync.Once
	// cleanupOnce 临时文件删除只执行一次（幂等）。
	cleanupOnce sync.Once
	// resume 是否为「blob 半截续传」条目（见 partial_resume.go）：true 时 tmpPath 指向
	// 半截文件本体（written 预置为半截前缀长度，订阅者据此读半截前缀），hub 的临时文件
	// 清理必须跳过——半截文件的保留/删除归续传逻辑与 cache.Clean 管理，绝不由 hub 删除。
	resume bool
}

// prefetchStreamSub 一个搭车订阅者：独立增量 channel + 消费过慢摘除标记。
type prefetchStreamSub struct {
	// ch 增量块 channel（缓冲 prefetchHubSubBuffer；块为逐订阅者独立 copy，
	// 绝不共享读循环的复用 buf 底层数组）。终态广播 close；close 后缓冲内数据仍可收完。
	ch chan []byte
	// overflow 广播时缓冲满被摘除（消费过慢）：e.mu 下读写，订阅者在 channel 关闭后读取判定。
	overflow bool
}

// newPrefetchStreamHub 构建空注册表。
func newPrefetchStreamHub() *prefetchStreamHub {
	return &prefetchStreamHub{m: make(map[string]*prefetchStreamEntry)}
}

// prefetchHubOrInit 返回 Server 级在途流共享注册表：New 已预构建；绕过 New 构造的
// Server 惰性补建（防御风格与 prefetchSlotsChan 一致）。
func (s *Server) prefetchHubOrInit() *prefetchStreamHub {
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	if s.prefetchHub == nil {
		s.prefetchHub = newPrefetchStreamHub()
	}
	return s.prefetchHub
}

// register 登记在途条目（预取开始下载时调用；调用方保证同 key 只有一个写者——
// prefetchInflight 已挡住同 blob 的重复预取）。同 key 的 done 滞留条目被直接覆盖
// （其 linger 定时器的 removeIf 指针比对不会误删新条目）。image 为镜像仓库名
// （日志统一标签，登记时传入，见 log_attrs.go）。
func (h *prefetchStreamHub) register(key, rule, image, digest string, log *slog.Logger) *prefetchStreamEntry {
	ent := &prefetchStreamEntry{
		key:     key,
		rule:    rule,
		image:   image,
		digest:  digest,
		hub:     h,
		log:     log,
		subs:    make(map[*prefetchStreamSub]struct{}),
		changed: make(chan struct{}),
	}
	h.mu.Lock()
	h.m[key] = ent
	h.mu.Unlock()
	return ent
}

// registerResume 登记续传在途条目（blob 半截续传执行方在扫描候选前调用，见
// partial_resume.go）：与预取条目同一注册表/同一搭车机制（lookup / subscribe /
// writeBroadcast / setSnapshot / finishDone / finishFailed 全复用），差异仅两点——
// written 预置为半截前缀长度（订阅者据此前缀读半截文件，执行方此后按
// 「先追加磁盘、后推进 written」的既有契约推进）；resume=true（hub 绝不清理半截文件，
// 客户端等待超限重试时按续传条目优先搭车）。条目字段在发布（写入注册表）前一次性
// 填好，保证并发搭车者查到的条目自带正确前缀，无「先注册后补写」的竞态窗口。
// image 为镜像仓库名（日志统一标签，登记时传入，见 log_attrs.go）。
func (h *prefetchStreamHub) registerResume(key, rule, image, digest string, prefix int64, log *slog.Logger) *prefetchStreamEntry {
	ent := &prefetchStreamEntry{
		key:     key,
		rule:    rule,
		image:   image,
		digest:  digest,
		hub:     h,
		log:     log,
		subs:    make(map[*prefetchStreamSub]struct{}),
		changed: make(chan struct{}),
		resume:  true,
		written: prefix,
	}
	h.mu.Lock()
	h.m[key] = ent
	h.mu.Unlock()
	return ent
}

// lookup 返回 key 对应的条目：running（可搭车）或 done 滞留条目（取出即移除，调用方
// 据此做「done → 直接 HIT」兜底）；无条目返回 nil。failed 条目在置位时即移除，查不到。
func (h *prefetchStreamHub) lookup(key string) *prefetchStreamEntry {
	h.mu.Lock()
	ent, ok := h.m[key]
	h.mu.Unlock()
	if !ok {
		return nil
	}
	if ent.stateOf() == prefetchStreamDone {
		h.removeIf(key, ent) // done 条目取出即清（后续查询者走 disk.Get 的既有 HIT 路径）
	}
	return ent
}

// removeIf 按指针比对移除条目（linger 定时器用：条目已被同 key 新预取覆盖时不误删）。
func (h *prefetchStreamHub) removeIf(key string, ent *prefetchStreamEntry) {
	h.mu.Lock()
	if cur, ok := h.m[key]; ok && cur == ent {
		delete(h.m, key)
	}
	h.mu.Unlock()
}

// linger 登记 done 条目的滞留到期清理：到期移除条目并删除临时文件（幂等）。
func (h *prefetchStreamHub) linger(key string, ent *prefetchStreamEntry) {
	time.AfterFunc(prefetchHubDoneLinger, func() {
		h.removeIf(key, ent)
		ent.cleanupFile()
	})
}

// stateOf 读取条目状态。
func (e *prefetchStreamEntry) stateOf() prefetchStreamState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

// terminal 返回条目终态与最终错误（running 时 err 为 nil）。
func (e *prefetchStreamEntry) terminal() (prefetchStreamState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.err
}

// setSnapshot 写入响应头快照（预取侧候选竞速完成 = 预取侧 commit 点）并广播唤醒
// 等待快照的订阅者。header 为 probeResult.header（构建后只读），直接共享引用。
func (e *prefetchStreamEntry) setSnapshot(status int, header http.Header, source string) {
	e.mu.Lock()
	e.status = status
	e.header = header
	e.source = source
	e.ready = true
	e.mu.Unlock()
	e.notifyChanged()
}

// snapshot 读取响应头快照（ready=true 后有效；搭车 commit 用）。
func (e *prefetchStreamEntry) snapshot() (status int, header http.Header, source string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status, e.header, e.source
}

// attachFile 登记预取 body 临时文件（创建成功后调用，先于 selectCandidate——竞速期间
// 搭车者即可读前缀）。
func (e *prefetchStreamEntry) attachFile(f *os.File, path string) {
	e.mu.Lock()
	e.tmpFile = f
	e.tmpPath = path
	e.mu.Unlock()
}

// tmpPathOf 读取临时文件路径。
func (e *prefetchStreamEntry) tmpPathOf() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tmpPath
}

// writeBroadcast 写者推进：写临时文件 + 推进已落盘字节数，并把该块广播给全部订阅者。
// 返回写文件失败（磁盘满等）错误——写者按失败收尾（广播 failed）。
func (e *prefetchStreamEntry) writeBroadcast(p []byte) error {
	e.mu.Lock()
	if e.state != prefetchStreamRunning {
		err := e.err
		e.mu.Unlock()
		return err // 已终态（写者不应再写，防御返回）
	}
	if e.tmpFile != nil {
		if _, err := e.tmpFile.Write(p); err != nil {
			e.mu.Unlock()
			return fmt.Errorf("写预取临时文件失败: %w", err)
		}
	}
	e.written += int64(len(p))
	subs := make([]*prefetchStreamSub, 0, len(e.subs))
	for sub := range e.subs {
		subs = append(subs, sub)
	}
	e.mu.Unlock()

	// 锁外广播：逐订阅者独立 copy（p 是读循环复用 buf 的切片，绝不让订阅者共享其
	// 底层数组——否则下一轮读会覆写订阅者尚未消费的内容）；缓冲满的订阅者摘除
	//（消费过慢），摘除不影响写者与其余订阅者。
	var overflowed []*prefetchStreamSub
	for _, sub := range subs {
		chunk := make([]byte, len(p))
		copy(chunk, p)
		select {
		case sub.ch <- chunk:
		default:
			overflowed = append(overflowed, sub)
		}
	}
	for _, sub := range overflowed {
		e.mu.Lock()
		if _, ok := e.subs[sub]; ok {
			delete(e.subs, sub)
			sub.overflow = true
			close(sub.ch)
			e.mu.Unlock()
			e.log.Warn("搭车订阅者消费过慢已摘除（终止其响应，客户端将以 Range 续传重试；预取继续填缓存不受影响）",
				dockerLogAttrs(e.logSourceOf(), e.image, "rule", e.rule, "key", e.key, "digest", e.digest)...)
			continue
		}
		e.mu.Unlock()
	}
	return nil
}

// subscribe 注册搭车订阅：返回订阅者与已落盘前缀长度（0..prefixLen 为磁盘前缀，
// 之后为增量 channel）。条目已非 running（竞态变终态）时拒绝。
func (e *prefetchStreamEntry) subscribe() (sub *prefetchStreamSub, prefixLen int64, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != prefetchStreamRunning {
		return nil, 0, false
	}
	sub = &prefetchStreamSub{ch: make(chan []byte, prefetchHubSubBuffer)}
	e.subs[sub] = struct{}{}
	return sub, e.written, true
}

// unsubscribe 注销订阅（客户端断开 / 降级 / 完成；幂等）。不 close channel——广播端
// 对已注销订阅者不再发送，channel 随订阅者退出由 GC 回收。
func (e *prefetchStreamEntry) unsubscribe(sub *prefetchStreamSub) {
	e.mu.Lock()
	delete(e.subs, sub)
	e.mu.Unlock()
}

// subOverflow 返回订阅者是否因消费过慢被广播端摘除（e.mu 下读写保证可见性）。
func (e *prefetchStreamEntry) subOverflow(sub *prefetchStreamSub) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return sub.overflow
}

// waitHeaderReady 等待响应头快照就绪（或条目变终态）：超时返回 false（调用方按
// 「预取卡死」降级）。changed 为 close-once 广播——快照就绪与终态都会唤醒，唤醒后
// 调用方须再查 terminal() 分流。
func (e *prefetchStreamEntry) waitHeaderReady(timeout time.Duration) bool {
	e.mu.Lock()
	ready, st := e.ready, e.state
	e.mu.Unlock()
	if ready || st != prefetchStreamRunning {
		return true
	}
	select {
	case <-e.changed:
		return true
	case <-time.After(timeout):
		return false
	}
}

// finishDone 终态收尾（EOF 缓存已原子落盘后调用）：关闭临时文件写句柄、关闭全部订阅
// channel（close 前缓冲内的增量仍可被订阅者收完——恰好拼出完整 body）、登记滞留到期
// 清理。幂等（仅 running 态生效）。
func (e *prefetchStreamEntry) finishDone() {
	e.mu.Lock()
	if e.state == prefetchStreamRunning {
		e.state = prefetchStreamDone
		if e.tmpFile != nil {
			_ = e.tmpFile.Close() // 写句柄用完即关；文件删除延迟到 linger（订阅者可能仍在读前缀）
			e.tmpFile = nil
		}
		for sub := range e.subs {
			close(sub.ch)
		}
	}
	e.mu.Unlock()
	e.notifyChanged()
	e.hub.linger(e.key, e)
}

// finishFailed 失败收尾：置 failed 并广播（关闭全部订阅 channel）后立即移除条目
// （后续请求查不到 → 走既有原回源路径）、清理临时文件（failed 后订阅者不再读前缀：
// 未 commit 的降级、已 commit 的终止，均不再触碰该文件）。幂等。
func (e *prefetchStreamEntry) finishFailed(err error) {
	e.mu.Lock()
	if e.state == prefetchStreamRunning {
		e.state = prefetchStreamFailed
		e.err = err
		if e.tmpFile != nil {
			_ = e.tmpFile.Close()
			e.tmpFile = nil
		}
		for sub := range e.subs {
			close(sub.ch)
		}
	}
	e.mu.Unlock()
	e.notifyChanged()
	e.hub.removeIf(e.key, e)
	e.cleanupFile()
}

// cleanupFile 删除预取临时文件（幂等；删除失败留 Debug 痕——残留 .tmp- 前缀文件由
// cache.Clean 后台清理兜底）。续传条目（resume=true）直接跳过：其 tmpPath 即半截文件
// 本体，保留与删除归续传逻辑（partial_resume.go）与 cache.Clean 孤儿清理管理，hub 的
// failed/done/linger 收尾绝不删除半截文件。
func (e *prefetchStreamEntry) cleanupFile() {
	e.cleanupOnce.Do(func() {
		if e.resume {
			return
		}
		path := e.tmpPathOf()
		if path == "" {
			return
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			// 仅预取条目会走到这里（续传条目上方已跳过）：source=prefetch
			e.log.Debug("删除预取临时文件失败（残留由缓存后台清理兜底）",
				dockerLogAttrs(dockerLogSourcePrefetch, e.image, "file", path, "err", err)...)
		}
	})
}

// notifyChanged close-once 广播状态变化。
func (e *prefetchStreamEntry) notifyChanged() {
	e.changedOnce.Do(func() { close(e.changed) })
}

// tryServeFromPrefetchHub 客户端 blob GET 缓存 MISS 时检查该层是否正被预取流式下载
// （hub 在途条目），命中即「搭车」：响应用预取的响应头快照（X-Cache: PREFETCH），
// body = 已落盘磁盘前缀（从 0 读临时文件）+ 订阅增量 channel 的实时后续——上游只有
// 预取那一路下载，客户端不从 0 重新发起网络下载。返回 handled=true 表示本次请求已由
// 搭车路径处理（含已 commit 后的各类终止），调用方直接返回；false 表示继续走既有
// 回源路径（行为与旧版完全一致）。
//
// 调用位置（handleGet）保证进入本检查的请求必然：路径目标 GET、缓存可读（非 BYPASS）、
// 无 Range / no-cache（可缓存判定已排除）——HEAD / 206 续传 / 强制刷新天然不搭车。
// 搭车不处理条件请求（If-None-Match 等）：docker blob 请求按 digest 校验、不做 ETag
// 协商，按 200 全量回放。
func (s *Server) tryServeFromPrefetchHub(w http.ResponseWriter, r *http.Request, key, target string, eff config.Options, disk *cache.Cache, start time.Time) bool {
	if disk == nil {
		return false // 防御：缓存不可读时本检查不会被调用（handleGet 已分流）
	}
	// docker 日志统一标签：搭车请求必然是 /v2/ 路径目标，按需解析 image/digest
	//（非 /v2/ 路径目标解析不出时 attrs 原样透传既有键，日志零变化）
	labels := parseDockerLogInfo(target)
	ent := s.prefetchHubOrInit().lookup(key)
	if ent == nil {
		return false // 无预取 / 无在途条目（docker-prefetch-enabled 关闭恒走此分支）：与旧版行为一致
	}
	// done 滞留条目（缓存写入完成瞬间撞上的并发 MISS）：缓存此刻已就绪，兜底走既有 HIT
	// 路径（一般到不了——lookup 命中 done 即把条目移除，后续查询走 disk.Get 既有路径）
	if st, _ := ent.terminal(); st == prefetchStreamDone {
		return s.servePrefetchHubDoneHit(w, r, key, eff, disk, start)
	}

	// 订阅在途流（拿到已落盘前缀长度；条目竞态变终态时 subscribe 拒绝）
	sub, prefixLen, ok := ent.subscribe()
	if !ok {
		if st, _ := ent.terminal(); st == prefetchStreamDone {
			return s.servePrefetchHubDoneHit(w, r, key, eff, disk, start)
		}
		return false // failed：走既有原回源路径（慢速升级等特性全保留）
	}
	// 客户端断开 / 降级 / 完成后注销订阅：仅停止读取，预取那一路继续填缓存不受影响
	defer ent.unsubscribe(sub)

	// 等预取侧响应头快照就绪（候选竞速期间需数秒到十几秒）；超时视为预取卡死：
	// 尚未向客户端 commit 响应头 → 放弃搭车，改走既有原回源路径
	if !ent.waitHeaderReady(prefetchHubIdleTimeout) {
		if st, _ := ent.terminal(); st == prefetchStreamDone {
			return s.servePrefetchHubDoneHit(w, r, key, eff, disk, start)
		}
		s.log.Warn("搭车放弃：等待预取响应头快照超时（预取疑似卡死），改走原回源路径",
			labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "key", key,
				"target", truncate(target, 300), "wait", prefetchHubIdleTimeout.String())...)
		return false
	}
	switch st, terr := ent.terminal(); st {
	case prefetchStreamDone:
		// 快照就绪后预取即完成（极小 blob）：缓存已就绪，直接走既有 HIT 路径（兜底）
		return s.servePrefetchHubDoneHit(w, r, key, eff, disk, start)
	case prefetchStreamFailed:
		// 预取失败广播：搭车放弃改走既有原回源路径（客户端自己回源，上游只此一路）
		s.log.Debug("预取失败广播，搭车放弃改走原回源路径",
			labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "key", key,
				"target", truncate(target, 300), "err", terr)...)
		return false
	}

	// commit 点：照抄预取的响应头快照（Content-Length / Docker-Content-Digest /
	// Content-Type / X-Proxy-Upstream 等），X-Cache 用新观测值 PREFETCH。此后响应头
	// 固化，任何失败只能终止本次响应（docker 对失败层以 Range 断点续传重试，既有
	// 206 透传不写缓存机制兜底「不从 0 重新下载」）。
	status, header, source := ent.snapshot()
	s.commitPrefetchHubHeader(w, status, header, source)
	s.log.Info("搭车在途预取层（拼接磁盘前缀+实时增量，上游仅预取一路下载）",
		labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "key", key,
			"prefix_bytes", prefixLen, "candidate", truncate(source, 300), "status", status)...)

	// 活跃下载进度登记（周期汇总，见 progress_summary.go）：commit 后开始回放即登记
	// source=ridealong，初始已下载字节 = 已落盘前缀 prefixLen（回放前缀不重复累加，
	// 只对随后消费的增量 addBytes），total 取预取响应头快照的 Content-Length；
	// defer 注销——完成/断开/超时终止都不留幽灵条目
	prog := s.progressRegister(dockerLogSourceRidealong, labels.image, labels.digest, r.Method,
		prefixLen, progressSummaryTotal(header))
	defer prog.unregister()

	// 读磁盘前缀（0..prefixLen）：预取写者按「先写文件后推进字节数」推进，读到的字节
	// 一定是已完整写入的；文件极小竞态窗口（订阅后预取即失败清理）按已 commit 终止处理
	f, err := os.Open(ent.tmpPathOf())
	if err != nil {
		s.log.Warn("搭车读磁盘前缀失败，终止本次响应（客户端将以 Range 续传重试）",
			labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule,
				"file", ent.tmpPathOf(), "err", err)...)
		return true
	}
	if prefixLen > 0 {
		// 客户端写失败（已断开）：仅停止读取订阅（defer 注销），预取继续填缓存不受影响
		if _, err := io.CopyN(w, f, prefixLen); err != nil {
			_ = f.Close()
			s.log.Warn("搭车写响应体失败（客户端已断开，预取继续填缓存不受影响）",
				labels.attrs(dockerLogSourceRidealong, "target", truncate(r.URL.Path, 200), "err", err)...)
			return true
		}
	}
	_ = f.Close()

	// 增量循环：回放预取广播的后续块；等待增量超过活性超时（预取卡死/过慢）即终止。
	// 每块已是逐订阅者独立 copy，写客户端无需再拷贝。
	total := prefixLen
	for {
		select {
		case chunk, chanOK := <-sub.ch:
			if !chanOK {
				// 订阅结束（预取侧终态广播 close，缓冲内增量已收完）：按终态收尾
				if ent.subOverflow(sub) {
					s.log.Warn("搭车增量消费过慢被摘除，终止本次响应（客户端将以 Range 续传重试）",
						labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "sent", total)...)
					return true
				}
				st, terr := ent.terminal()
				if st == prefetchStreamFailed {
					s.log.Warn("预取失败广播，终止本次搭车响应（客户端将以 Range 续传重试）",
						labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "sent", total, "err", terr)...)
					return true
				}
				// done：磁盘前缀 + 增量恰好拼出完整 body，搭车完成
				s.log.Info("搭车完成（在途预取层已完整回放）",
					labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "bytes", total,
						"upstream", source, "cost", time.Since(start).Round(time.Millisecond).String())...)
				s.log.Info("代理请求完成",
					labels.attrs(dockerLogSourceRidealong,
						"method", r.Method,
						"target", truncate(r.URL.Path, 300),
						"status", status,
						"cache", prefetchCacheStatus,
						"upstream", source,
						"rule", eff.Rule,
						"bytes", total,
						"cost", time.Since(start).Round(time.Millisecond).String(),
					)...)
				return true
			}
			if _, werr := w.Write(chunk); werr != nil {
				// 客户端断开：仅停止读取订阅（defer 注销），预取继续填缓存不受影响
				//（预取与客户端连接解耦，客户端断开不取消预取——客户端流式主路径的
				//「断开即停」语义只作用于该请求自身的回源流，见 stream.go）
				s.log.Warn("搭车写响应体失败（客户端已断开，预取继续填缓存不受影响）",
					labels.attrs(dockerLogSourceRidealong, "target", truncate(r.URL.Path, 200), "err", werr)...)
				return true
			}
			total += int64(len(chunk))
			prog.addBytes(len(chunk)) // 活跃下载周期汇总累加（回放增量；磁盘前缀已计入初始值）
		case <-time.After(prefetchHubIdleTimeout):
			// 预取增量停滞（卡死/过慢）：已 commit，终止本次响应；docker 会以 Range
			// 断点续传重试（命中已写盘的前缀缓存则 HIT、否则回源 206，都不从 0 拉整层）
			s.log.Warn("搭车订阅超时（预取增量停滞），终止本次响应（客户端将以 Range 续传重试）",
				labels.attrs(dockerLogSourceRidealong, "rule", eff.Rule, "sent", total,
					"idle", prefetchHubIdleTimeout.String())...)
			return true
		}
	}
}

// servePrefetchHubDoneHit done 条目的兜底出口：此刻缓存已写盘，直接走既有 HIT 回放；
// 缓存意外缺失（写盘失败等异常）返回 false 让调用方走既有回源路径。
func (s *Server) servePrefetchHubDoneHit(w http.ResponseWriter, r *http.Request, key string, eff config.Options, disk *cache.Cache, start time.Time) bool {
	if e, ok := disk.Get(key); ok {
		// done 条目的兜底 HIT 仍是客户端请求触发的缓存命中回放：source=client
		labels := parseDockerLogInfo(r.URL.Path)
		s.log.Info("缓存命中（HIT）", labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
			"file", disk.FilePath(key), "bytes", len(e.Body), "ttl", ttlDisplay(e.TTL))...)
		s.serveEntry(w, r, e, "HIT", eff, start)
		return true
	}
	return false
}

// commitPrefetchHubHeader 搭车响应的 commit 点：照抄预取的响应头快照（沿用
// skipResponseHeader 过滤逐跳头与自管头——Content-Length / X-Cache / X-Proxy-Upstream
// 在过滤名单内由下方手动补回，与 commitStreamingHeader 同款）；Content-Length 用上游
// 声明值（搭车 body 总长与上游完整对象一致），上游未声明则 Go 按 chunked 编码流式传输。
// 调用后响应头固化，后续失败只能终止本次响应。
func (s *Server) commitPrefetchHubHeader(w http.ResponseWriter, status int, header http.Header, source string) {
	h := w.Header()
	for k, vv := range header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	h.Set("X-Cache", prefetchCacheStatus)
	if source != "" {
		h.Set("X-Proxy-Upstream", source) // 观测内容来源（预取那一路轮询命中的候选 URL）
	}
	if cl := header.Get("Content-Length"); cl != "" {
		h.Set("Content-Length", cl)
	}
	w.WriteHeader(status)
}

// prefetchBlobStreaming 预取执行方的 blob 流式回源（prefetchOneBlob 经 flight.DoLimit
// 以本函数为执行回调）：
//  1. hub 登记在途条目（此后客户端 blob GET 缓存 MISS 即可搭车，见 tryServeFromPrefetchHub）；
//  2. 复用 selectCandidate 竞速选定候选（候选级超时覆盖、http-proxy、insecure TLS、
//     docker 401 匿名 token dance 全在管线内部，与客户端流式路径同一条回源链路）；
//  3. 响应头快照进条目（预取侧 commit 点，搭车方由此拿到响应头）；
//  4. 边读上游边写缓存临时文件（.tmp- 前缀，残留由缓存后台清理兜底）并逐块广播增量
//     给订阅者（逐订阅者独立 copy）；
//  5. EOF：读回临时文件构造完整 Entry，复用 finalizeFetchEntry 原子写缓存（成功判据 /
//     TTL 固化 / 日志全复用），hub 置 done 关闭订阅——磁盘前缀 + 增量恰好拼出完整
//     body，搭车方与最终缓存条目逐字节一致；
//  6. 任何失败：读上游失败时先把已下载前缀保留为半截文件（blob 半截保留与无感续传，
//     见 partial_resume.go；仅全局开关开启），广播 failed 给全部订阅者后移除条目并清理
//     临时文件（订阅方未 commit 的降级走原回源路径、已 commit 的终止本次响应，docker
//     以 Range 续传重试）。
//
// 返回完整 Entry 供 flight 等待方（在 hub 登记前撞上本预取的窗口期客户端）共享回放；
// EOF 时一次性读回，内存峰值 = 单个 blob 全量（与既有缓冲回源路径一致量级）。
// 不叠加 slowdownload 慢速升级多源并发续传：上游速率由预取那一路决定，过慢时靠搭车
// 订阅超时降级 + 客户端 Range 重试兜底（取舍见文件头注释）。
func (s *Server) prefetchBlobStreaming(req *http.Request, key, target, digest string, eff config.Options, disk *cache.Cache) (*cache.Entry, error) {
	// docker 日志统一标签：blob 目标解析 image/digest（登记 hub 条目与本函数全部日志用）
	labels := parseDockerLogInfo(target)
	hub := s.prefetchHubOrInit()
	ent := hub.register(key, eff.Rule, labels.image, digest, s.log)
	// panic 防护：flight.DoLimit 对 fn panic 有自身 recover（清理在途记录后 re-panic，
	// 最终由 prefetchImageLayers 的 goroutine 防护兜住），这里必须先把 hub 条目置
	// failed——否则 running 条目滞留 hub 会泄漏，后续请求会搭上一条永不推进的死流
	defer func() {
		if rec := recover(); rec != nil {
			ent.finishFailed(fmt.Errorf("预取流式下载 panic: %v", rec))
			panic(rec)
		}
	}()

	// 缓存临时文件：body 原文逐块追加（先于 selectCandidate 创建——竞速期间搭车者
	// 即可读前缀；.tmp- 前缀与缓存写入临时文件一致，进程崩溃残留由 cache.Clean 兜底）
	tmp, err := os.CreateTemp(disk.Dir(), prefetchTmpPattern)
	if err != nil {
		werr := fmt.Errorf("创建预取临时文件失败: %w", err)
		ent.finishFailed(werr)
		return nil, werr
	}
	ent.attachFile(tmp, tmp.Name())

	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(req, target), eff)
	if len(candidates) == 0 {
		// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，预取路径同样绝不直连
		werr := errors.New("路径目标请求未配置 url-redirect，无法回源（路径目标必须由命中的域名规则提供 url-redirect）")
		ent.finishFailed(werr)
		return nil, werr
	}

	// 竞速选定候选（commit 前可自由切换/重试/竞速，失败不向搭车方输出任何字节——
	// 此时搭车方还在等快照，会按 failed 广播降级）；全败按既有路径目标语义报错
	res, serr := s.selectCandidate(req, candidates, eff, target)
	if serr != nil {
		s.log.Warn("预取 blob 回源候选全败（路径目标无直连回退，已广播搭车订阅者降级）",
			dockerLogAttrs(dockerLogSourcePrefetch, labels.image, "rule", eff.Rule, "digest", digest,
				"target", truncate(target, 300), "last_error", serr)...)
		werr := fmt.Errorf("全部 url-redirect 候选失败，路径目标无直连回退（无完整原始 URL 可直连）: %w", serr)
		ent.finishFailed(werr)
		return nil, werr
	}

	// 响应头快照 = 预取侧 commit 点：搭车方由此拿到 Content-Length / Content-Type /
	// Docker-Content-Digest 等响应头（res.header 构建后只读，直接共享引用）
	ent.setSnapshot(res.status, res.header, res.url)
	s.log.Info("预取 blob 流式下载开始（在途层可搭车）",
		dockerLogAttrs(dockerLogSourcePrefetch, labels.image, "rule", eff.Rule, "digest", digest,
			"candidate", truncate(res.url, 300),
			"status", res.status, "content_length", res.header.Get("Content-Length"),
			"check", eff.SuccessCheck)...)

	// 失败收尾：关闭上游、广播 failed 并移除 hub 条目、清理临时文件
	fail := func(cause error) (*cache.Entry, error) {
		res.Close() // 幂等：关闭连接、停看门狗、取消超时预算链
		ent.finishFailed(cause)
		return nil, cause
	}

	// 活跃下载进度登记（周期汇总，见 progress_summary.go）：响应头快照就绪（本路
	// commit 点）后即登记 source=prefetch，total 取 Content-Length（不可得则不输出
	// percent）；defer 注销——EOF 写缓存 / 失败 / panic 都不留幽灵条目。初始已下载
	// 字节取条目既有 written（登记时通常为 0），随后每写一块经 addBytes 累加
	prog := s.progressRegister(dockerLogSourcePrefetch, labels.image, labels.digest, req.Method,
		ent.writtenOf(), progressSummaryTotal(res.header))
	defer prog.unregister()

	// 首块先行写入并广播（与客户端流式路径同一节拍：探测阶段已缓冲的首块直接入列）
	if len(res.first) > 0 {
		if err := ent.writeBroadcast(res.first); err != nil {
			return fail(fmt.Errorf("预取流式回源写临时文件失败（%s）: %w", res.url, err))
		}
		prog.addBytes(len(res.first)) // 活跃下载周期汇总累加
	}
	// 上游读经 progressReader 记入看门狗：read-timeout（无新增数据超时）语义与客户端
	// 流式路径一致，慢速但持续传输的大 blob 不会被误砍；res.body 为 nil 表示探测期已到
	// EOF / 无实体响应（与 fetchViaCandidatesStreaming 的保护一致），直接走 EOF 收尾
	if res.body != nil {
		pr := &progressReader{r: res.body, wd: res.wd}
		buf := make([]byte, streamChunkSize)
		for {
			n, rerr := pr.Read(buf)
			if n > 0 {
				if err := ent.writeBroadcast(buf[:n]); err != nil {
					return fail(fmt.Errorf("预取流式回源写临时文件失败（%s）: %w", res.url, err))
				}
				prog.addBytes(n) // 活跃下载周期汇总累加
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					break // 完整读完：磁盘前缀即完整 body，半截数据不存在，可以入库
				}
				// blob 半截保留：读上游失败时把已下载前缀（临时文件内容）保留为半截文件
				//（仅全局开关开启；同目录 rename 零拷贝、进度不回退，见 partial_resume.go）。
				// 须在 fail（finishFailed 清理临时文件）之前改名——改名后旧临时文件名已
				// 不存在，finishFailed 的清理按 ErrNotExist 静默跳过，无副作用
				if s.cfg != nil && s.cfg.GlobalDockerPartialResumeEnabled() {
					s.keepPrefetchPartial(disk, key, tmp.Name(), ent.writtenOf(), labels)
				}
				// 读上游失败：广播 failed（半截数据绝不入库），Error 留痕
				s.log.Error("预取流式回源读上游失败（已广播搭车订阅者终止/降级，半截数据不入库）",
					dockerLogAttrs(dockerLogSourcePrefetch, labels.image, "rule", eff.Rule, "digest", digest,
						"candidate", truncate(res.url, 300),
						"status", res.status, "bytes", ent.writtenOf(), "err", rerr)...)
				return fail(fmt.Errorf("预取流式回源读上游失败（%s）: %w", res.url, rerr))
			}
		}
	}
	res.Close()

	// EOF：读回临时文件构造完整 Entry，复用既有缓存写入语义（判据 / TTL 固化 / 日志 /
	// 预取触发全复用——blob 目标非 manifest，maybePrefetchImageLayers 内部静默返回）；
	// 写缓存日志按本路来源标注 source=prefetch（预取写者，与客户端主路径区分）
	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		return fail(fmt.Errorf("读回预取临时文件失败: %w", err))
	}
	entry := &cache.Entry{
		Status:   res.status,
		Header:   res.header,
		Body:     data,
		StoredAt: time.Now(),
		Source:   res.url,
	}
	s.finalizeFetchEntry(req, key, target, entry, eff, disk, dockerLogSourcePrefetch)

	// 缓存已原子落盘：置 done 并关闭订阅（搭车方收完 channel 缓冲内的最后增量即完整）；
	// 条目滞留 hub 一小段时间供并发 MISS 兜底检出「done → 直接 HIT」，到期自动移除
	ent.finishDone()
	return entry, nil
}

// writtenOf 读取已落盘字节数（日志观测用）。
func (e *prefetchStreamEntry) writtenOf() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.written
}

// subscriberCount 返回当前搭车订阅者数（e.mu 下 len(e.subs)）：续传执行方的宽限终止
// 判定用（见 partial_resume.go）——客户端断开且此刻无订阅者才启动宽限计时，到期仍无
// 订阅者才终止上游读取；宽限期内本数变正（新订阅者 subscribe 加入）即放弃终止。
func (e *prefetchStreamEntry) subscriberCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.subs)
}

// logSourceOf 返回条目所属链路的日志来源：续传条目（resume=true）归 resume，预取条目
// 归 prefetch——写侧 hub 事件（广播摘除订阅者 / 临时文件清理）由写者（预取/续传执行方）
// 触发，按写者来源标注；搭车侧的日志由搭车调用点统一标 ridealong（见 tryServeFromPrefetchHub）。
func (e *prefetchStreamEntry) logSourceOf() string {
	if e.resume {
		return dockerLogSourceResume
	}
	return dockerLogSourcePrefetch
}
