// slowdownload.go —— 慢速升级多源并发续传（slow-download escalation）与时间驱动的
// 单层 blob 多源并发补充（blob-concurrency-interval）共用一套「加入新源 + 有序合并」：
// docker 类（type=docker 规则）GET 路径目标流式回源 commit 后，滑动窗口平均送出速率
// 低于 slow-download-speed 判「过慢」且剩余量足够、候选数>1 时，向其余全部候选并发发起
// `Range: bytes={offset}-{end}` 续传，胜者既有连接不中断并入合并；合并循环按绝对偏移
// 严格有序写给客户端与 accum（缓存语义不变），落后的源按 max-behind 剪枝关闭收敛带宽放大。
//
// 时间驱动补充（线上真实故障的直接对策）：客户端主路径与半截续传执行方每满
// blob-concurrency-interval（默认 5m，0 = 关闭）且该层尚未下载完成，就**只做加法**地
// 向尚未加入合并的候选源补充并发 Range 续传（复用同一 slowMerger；不切换/不中断已在
// 下载的源、客户端无感、下完前一直保持并发）。首轮与之后的每一轮都走同一个
// timeBlobSupplement 判定（日志 reason 枚举 / joined_sources / pending_sources 统一）。
// 动机：1.32GiB 单层 blob 的上游 200 响应没有 Content-Length → 判速升级因「剩余量不可知」
// 被静默跳过，只有一路 53KB/s 硬拉 19 分钟；断流后又走半截续传路径，而续传路径当时不
// 叠加多源升级 → 仍然只有单路。总长不可知的兜底见 blob_size_index.go（manifest size）。
//
// 连续性保证不变：升级只增开续传源，绝不动客户端已见字节（不切状态码/响应头，不回退）。
// 每源读继续走 progressReader+watchdog（无新增数据超时语义不变）；候选并发数上限 =
// 候选列表长度（一般 ≤5），不做额外并发池。
//
// 客户端断开终止（断开即停）：合并循环的 write 回调检测到客户端写失败时，由调用方
// abortAll 逐路关闭全部上游源（含尚未定局的 pending 续传源），合并循环随即自然退出——
// 断开后不再有「继续读源填缓存」，半截保留与运维事件留痕由调用方统一收尾（stream.go
// clientGoneAbort）。
//
// 日志规范：本文件的判定/动作日志按调用方传入的 logSource 标注 source（client / resume，
// 续传执行方路径同一套判定日志；见 log_attrs.go 的统一键序与 partial_resume.go 的续传
// 执行方接入）。

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

// blob 多源并发补充（时间驱动 + 判速触发共用「加入新源」逻辑）的日志枚举：键名固定，
// 便于运维按 reason 聚合检索「为什么没有补充源」。
const (
	// blobAddReasonDisabled 时间驱动补充被关闭：blob-concurrency-interval<=0 且
	// slow-download-speed<=0（两条触发通道都没开）。
	blobAddReasonDisabled = "disabled"
	// blobAddReasonNotDockerRule 命中规则不是 type=docker（slow-download-* 与
	// blob-concurrency-interval 都只在 type=docker 规则的流式回源上生效）。
	blobAddReasonNotDockerRule = "not_docker_rule"
	// blobAddReasonTotalUnknown 总长不可知：上游响应无 Content-Length（也非 206）且
	// manifest 里没有该 digest 的 size 记录 → 无法判定「剩余量」，无法安全并发续传。
	blobAddReasonTotalUnknown = "content_length_unknown"
	// blobAddReasonCandidatesLT2 候选源不足 2 个（只有当前一路，无从并发）。
	blobAddReasonCandidatesLT2 = "candidates_lt_2"
	// blobAddReasonAllJoined 全部候选源都已在合并器内（本次没有可补充的新源）。
	blobAddReasonAllJoined = "all_joined"
	// blobAddReasonClientGone 客户端已断开（下载已按断开即停终止，不再补源）。
	blobAddReasonClientGone = "client_gone"
	// blobAddReasonNoRange 该源不支持 Range（对续传请求回 200 全量）——该源被跳过并留痕。
	blobAddReasonNoRange = "upstream_no_range"
	// blobAddReasonRemainingSmall 剩余字节不足 slow-download-min-remaining（尾巴太小，
	// 并发拉满没有收益）。
	blobAddReasonRemainingSmall = "remaining_too_small"
)

// blob 层下载「已知总长」的来源枚举（日志 total_source 字段）：
const (
	// blobTotalSourceUpstream 总长来自上游响应（Content-Length / Content-Range）。
	blobTotalSourceUpstream = "upstream"
	// blobTotalSourceManifest 总长来自 manifest 记录的 layer size（上游无 Content-Length
	// 时的兜底，见 blob_size_index.go）。
	blobTotalSourceManifest = "manifest"
	// blobTotalSourceUnknown 总长不可知（上游未声明且 manifest 无记录）。
	blobTotalSourceUnknown = "unknown"
)

// slowEscalationTargetWithFallback slowEscalationTarget 的「总长可兜底」版本：
// 上游 200 响应没有 Content-Length 时用 manifest 记录的 layer size 兜底 knownTotal
//（线上故障：1.32GiB 单层上游 200 无 Content-Length → 判速升级被静默跳过，只有一路
// 53KB/s 硬拉 19 分钟）。返回的 totalSource 标注总长来源（upstream / manifest；
// ok=false 时视为 unknown）。206 分支语义不变：起点与区间终点只能来自 Content-Range
//（上游 206 必带可解析 Content-Range 才算合规，见 validateResumeResponse）。
func slowEscalationTargetWithFallback(status int, header http.Header, manifestTotal int64) (winnerStart, knownTotal, end int64, totalSource string, ok bool) {
	winnerStart, knownTotal, end, ok = slowEscalationTarget(status, header)
	if ok {
		return winnerStart, knownTotal, end, blobTotalSourceUpstream, true
	}
	if status == http.StatusOK && manifestTotal > 0 {
		return 0, manifestTotal, manifestTotal - 1, blobTotalSourceManifest, true
	}
	return 0, 0, 0, blobTotalSourceUnknown, false
}

// blobConcurrencyOffReason 时间驱动多源并发补充「未启用」的原因（仅用于单层 blob 下载
// 开始的一次性留痕）：
//   - 非 type=docker 规则 → not_docker_rule（线上踩过的坑：规则缺 type 时全部慢速/
//     时间触发参数静默不生效）；
//   - type=docker 但 blob-concurrency-interval<=0 **且** slow-download-speed<=0 →
//     disabled（两条触发通道都关了）；
//   - 其余（至少一条触发通道生效，或只关时间通道但判速仍在）→ 空串，不输出原因日志。
func blobConcurrencyOffReason(eff config.Options) string {
	if eff.RuleType != config.RuleTypeDocker {
		return blobAddReasonNotDockerRule
	}
	if eff.BlobConcurrencyInterval <= 0 && eff.SlowDownloadSpeed <= 0 {
		return blobAddReasonDisabled
	}
	return ""
}

// blobConcurrencyTrack 时间驱动的多源并发补充判定节拍（每层 blob 下载一个）：
// 首次触发 = 该层下载开始（客户端主路径 commit / 续传执行方 commit）后满 interval，
// 之后每满 interval 再判一次；读循环因上游停滞阻塞较久而错过时间点时只补一次判定
//（不做补偿轮次），下一轮从当前时刻重新计时。
type blobConcurrencyTrack struct {
	// interval 判定间隔（blob-concurrency-interval 生效值）。
	interval time.Duration
	// start 该层下载开始时刻（日志 elapsed 的基线）。
	start time.Time
	// nextAt 下一次判定时刻。
	nextAt time.Time
	// round 已判定的轮次（1 起，日志字段）。
	round int
}

// newBlobConcurrencyTrack 创建判定节拍：enabled=false（非 type=docker / interval<=0）
// 时返回 nil——调用方按 nil 跳过全部判定（零开销，除一次 nil 判断）。
func newBlobConcurrencyTrack(enabled bool, start time.Time, interval time.Duration) *blobConcurrencyTrack {
	if !enabled || interval <= 0 {
		return nil
	}
	return &blobConcurrencyTrack{interval: interval, start: start, nextAt: start.Add(interval)}
}

// due 判定是否到点：到点返回本次轮次（1 起）与 true，并把下一次判定推到 now+interval。
func (t *blobConcurrencyTrack) due(now time.Time) (int, bool) {
	if t == nil || now.Before(t.nextAt) {
		return 0, false
	}
	t.round++
	t.nextAt = now.Add(t.interval)
	return t.round, true
}

// //////////////////  时间驱动补充的判定与动作（单源 / 合并两阶段共用）  //////////////////////

// blobSupplementInput 一次「时间驱动补充」判定的输入（stream.go 客户端主路径与
// partial_resume.go 续传执行方共用，保证两条路径的判定/日志/动作完全一致）：
// merger 为 in/out——nil 表示仍在单源阶段（本函数会创建合并器并回填该字段），
// 非 nil 表示已在有序合并中（本次只做「增量加入尚未加入的源」）。
type blobSupplementInput struct {
	// r 客户端请求（建续传源用：请求头白名单 / token dance / 候选级超时全复用）。
	r *http.Request
	// res 单源阶段的胜者活体结果（创建合并器时并入首位；合并阶段可为 nil）。
	res *probeResult
	// labels docker 日志统一标签（image/digest 等）。
	labels dockerLogInfo
	// logSource 日志 source 值（client / resume）。
	logSource string
	// eff 生效配置（候选级/规则级/全局已解析）。
	eff config.Options
	// candidates 本次请求展开出的全部候选（含当前胜者）。
	candidates []candidate
	// round 本次判定轮次（1 起）。
	round int
	// now 判定时刻。
	now time.Time
	// startedAt 该层下载开始时刻（日志 elapsed 基线）。
	startedAt time.Time
	// bytes 该层已写出的绝对字节数（日志 bytes；进度不回退）。
	bytes int64
	// offset 下一个期望的绝对偏移（单源 = 胜者起点+已消费；合并 = 由合并器游标另行取）。
	offset int64
	// knownTotal 已知对象总长（0 = 不可知）。
	knownTotal int64
	// totalSource 总长来源（upstream / manifest / unknown）。
	totalSource string
	// end 续传区间终点（<0 = 开区间到对象末尾）。
	end int64
	// clientGone 客户端是否已断开（断开即停语义下不再补源）。
	clientGone bool
	// merger in/out：nil = 单源阶段；非 nil = 已在有序合并中。
	merger *slowMerger
}

// timeBlobSupplement 时间驱动补充的判定与动作（每满 blob-concurrency-interval 调用一次）：
//  1. 输出 Info「blob 下载 N 分钟未完成，评估多源并发补充」（含 elapsed/bytes/total/
//     total_source/rate/joined_sources/pending_sources）；
//  2. 按原因枚举判定能否补充——client_gone / candidates_lt_2 / content_length_unknown /
//     all_joined / remaining_too_small → Info「未补充多源并发」并带 reason；
//  3. 可补充时：单源阶段创建合并器（胜者既有连接并入 + 其余全部候选并发 Range 续传，
//     返回 created=true 让调用方转出单源读循环）；已在合并阶段则只做「增量加入尚未
//     加入的候选源」（joinSlowSources，绝不切换/中断已在下载的源）。
//
// 返回值 created：本次调用是否新建了合并器（调用方据此转入有合并循环的路径）。
func (s *Server) timeBlobSupplement(in *blobSupplementInput) (created bool) {
	elapsed := in.now.Sub(in.startedAt)
	// joined_sources / pending_sources：已加入合并的源数（含胜者）与尚未加入的候选数。
	joined := 1
	if in.merger != nil {
		joined = in.merger.sourceCount()
	}
	pending := len(in.candidates) - joined
	if pending < 0 {
		pending = 0
	}
	minutes := int(elapsed.Minutes() + 0.5)
	if minutes < 1 {
		minutes = 1
	}
	s.log.Info(fmt.Sprintf("blob 下载 %d 分钟未完成，评估多源并发补充", minutes),
		in.labels.attrs(in.logSource, "rule", in.eff.Rule,
			"round", in.round,
			"elapsed", elapsed.Round(time.Millisecond).String(),
			"bytes", in.bytes,
			"total", in.knownTotal,
			"total_source", in.totalSource,
			"rate", humanBytesPerSec(in.bytes, elapsed),
			"joined_sources", joined,
			"pending_sources", pending)...)

	// 原因判定（顺序 = 优先级）：客户端没了 > 候选不足 > 总长不可知 > 已全加入 > 剩余太小
	reason := ""
	candidate := sourceURLOf(in.res)
	if in.merger != nil {
		candidate = in.merger.winnerURL()
	}
	remaining := int64(-1)
	switch {
	case in.clientGone:
		reason = blobAddReasonClientGone
	case len(in.candidates) < 2:
		reason = blobAddReasonCandidatesLT2
	case in.knownTotal <= 0:
		reason = blobAddReasonTotalUnknown
	case pending == 0:
		reason = blobAddReasonAllJoined
	default:
		remaining = in.knownTotal - in.offset
		if remaining < in.eff.SlowDownloadMinRemaining {
			reason = blobAddReasonRemainingSmall
		}
	}
	if reason != "" {
		extra := []any{
			"rule", in.eff.Rule,
			"candidate", truncate(candidate, 300),
			"round", in.round,
			"reason", reason,
			"joined_sources", joined,
			"pending_sources", pending,
			"bytes", in.bytes,
			"total", in.knownTotal,
			"total_source", in.totalSource,
		}
		if remaining >= 0 {
			extra = append(extra, "remaining", remaining, "min_remaining", in.eff.SlowDownloadMinRemaining)
		}
		s.log.Info("未补充多源并发", in.labels.attrs(in.logSource, extra...)...)
		return false
	}

	others := escalationCandidates(in.candidates, candidate)
	if in.merger == nil {
		// 单源阶段：胜者既有连接不中断并入合并（天然从当前偏移继续），其余全部候选
		// 并发发起 Range 续传；客户端仍是一条连续的流（不切状态码、不回退已发字节）。
		in.merger = s.escalateSlowSources(in.r, in.res, in.offset, others, in.eff,
			in.knownTotal, in.end, in.labels, in.logSource,
			"total_source", in.totalSource, "elapsed", elapsed.Round(time.Millisecond).String())
		return true
	}
	// 合并阶段：只做加法（增量加入尚未加入的候选源，绝不切换/中断已在下载的源）
	from := in.merger.cursor()
	if from < in.offset {
		from = in.offset
	}
	s.joinSlowSources(in.r, in.merger, others, in.eff, from, in.end, in.labels, in.logSource,
		"total_source", in.totalSource, "elapsed", elapsed.Round(time.Millisecond).String())
	return false
}

// sourceURLOf 取探测结果的候选 URL（res 为 nil 时返回空串，仅用于日志字段）。
func sourceURLOf(res *probeResult) string {
	if res == nil {
		return ""
	}
	return res.url
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
	// finished 是否已收场（run 的成功/失败出口在关闭源前置位）。
	finished bool
	// endedAlive 收尾时刻的存活源数快照（finished=true 后有效；完成日志的
	// alive_sources 字段据此输出，见 finalAlive）。
	endedAlive int
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
		case !src.pending && m.maxBehind > 0 && m.next-src.readPos > m.maxBehind:
			// pending（续传尝试尚未定局）不按落后剪枝：时间驱动的「补充源」是在下载已经
			// 很慢时加入的，其响应头可能比既有源晚到几秒；若按落后剪枝会在它连上之前就把
			// 它剪掉，等于白加一轮。maxBehind<=0（显式关闭剪枝阈值，仅时间驱动可配到）
			// 表示不按落后剪枝，仍保留出错 / EOF 落后两条收敛规则。
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
	total := len(m.sources)
	m.mu.Unlock()
	// 锁外收尾：关闭被剪枝源、唤醒可能在等缓冲空间的读取协程、输出剪枝留痕
	for _, src := range toClose {
		src.res.Close() // 幂等：关闭上游连接、停看门狗、取消超时预算链
	}
	if len(events) > 0 || dropped > 0 {
		m.changed() // 有剪枝或缓冲空间释放：唤醒等通知的合并循环/读取协程（防漏唤醒卡死）
	}
	for _, e := range events {
		// sources = 源总数、alive_sources = 存活（未剪枝）源数：补充源后源数会变，
		// 两个字段一起看才能判断「补充的源是否已经被剪掉了」。
		m.log.Log(context.Background(), e.level, "慢速升级多源续传源已剪枝",
			"candidate", truncate(e.name, 300), "reason", e.reason,
			"next", m.next, "total", m.knownTotal,
			"sources", total, "alive_sources", alive, "live_sources", alive)
	}
	return chunk
}

// aliveCount 返回当前存活（未剪枝）源数。
func (m *slowMerger) aliveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aliveLocked()
}

// aliveLocked 统计存活（未剪枝）源数（m.mu 下调用）。
func (m *slowMerger) aliveLocked() int {
	alive := 0
	for _, src := range m.sources {
		if src.pruned == "" {
			alive++
		}
	}
	return alive
}

// finalAlive 返回「收尾时刻」的存活源数（供完成日志的 alive_sources 字段）：
// run 收场时 closeAll 会把全部源置剪枝，之后的 aliveCount 恒为 0——那不是运维想看的
// 数字（「收尾时还剩几路源在跑」才有意义），因此在收场前记一份快照；run 尚未收场
//（如被中断）时回落到当前存活数。
func (m *slowMerger) finalAlive() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finished {
		return m.endedAlive
	}
	return m.aliveLocked()
}

// markFinished 记录收尾快照（存活源数），run 的每个收场出口在关闭源之前调用一次（幂等）。
func (m *slowMerger) markFinished() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finished {
		return
	}
	m.finished = true
	m.endedAlive = m.aliveLocked()
}

// sourceCount 返回合并器内源总数（含已剪枝的——「已在合并中的源」不再重复加入，
// 日志 sources / joined_sources 字段据此计算）。
func (m *slowMerger) sourceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sources)
}

// cursor 返回当前有序合并游标（下一个期望的绝对偏移）：时间驱动补充源时以此作为
// 新源的 Range 起点（绝不重复拉取已写给客户端的区间）。
func (m *slowMerger) cursor() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}

// winnerURL 返回首位源（胜者）的 URL（日志 candidate 字段用）。
func (m *slowMerger) winnerURL() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sources) == 0 {
		return ""
	}
	return m.sources[0].name
}

// hasSourceLocked 判断某候选 URL 是否已在合并器内（m.mu 下调用；调用方持锁）。
func (m *slowMerger) hasSourceLocked(url string) bool {
	for _, src := range m.sources {
		if src.name == url {
			return true
		}
	}
	return false
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

// abortAll 客户端断开后的终止动作（fetchViaCandidatesStreaming 的 writeOut 检测到写
// 响应体失败时调用一次）：全部未剪枝源一律置剪枝标记并关闭已建立的连接——含尚未定局
// 的 pending 续传源（与 closeAll 不同，closeAll 只处理已有连接的源）：其尝试协程定局时
// 发现剪枝标记即自行释放连接、绝不启动 pump（见 settleSlowResume 的 pruned 自检），
// 已建立连接的源立即 Close 停读停看门狗。飞行中的候选尝试无法就地取消（startUpstream
// 与客户端断开解耦），定局自检即释放，窗口受候选级超时约束。此后合并循环的存活源数为
// 0 自然退出（run 返回失败），由调用方按「客户端断开终止」出口收尾——绝不再读上游。
// 幂等：重复调用或与 closeAll / take 剪枝并发均安全（pruned 全部在 m.mu 下读写）。
func (m *slowMerger) abortAll(reason string) {
	m.mu.Lock()
	var toClose []*slowSource
	for _, src := range m.sources {
		if src.pruned == "" {
			src.pruned = reason
			if src.res != nil {
				toClose = append(toClose, src)
			}
		}
	}
	m.mu.Unlock()
	for _, src := range toClose {
		src.res.Close() // 幂等：关闭上游连接、停看门狗、取消超时预算链
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
			// 正常走既有 EOF 收尾（finalize 写缓存/206 不写缓存、成功返回日志）；
			// 收场前先记存活源快照（完成日志的 alive_sources 才有可读值）
			m.markFinished()
			m.closeAll("completed")
			return next, nil
		}
		live := m.aliveCount()
		if live == 0 {
			// 所有源全部耗尽且未到总长：沿用「commit 后读上游失败」语义
			//（Error 日志、终止响应、不写缓存，由调用方返回错误）
			m.markFinished()
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

// resumeRangeHeaderTail 构造续传 Range 头：end>=0 且有界时 bytes={offset}-{end}，
// 否则 bytes={offset}-（开区间到对象末尾）——总长不可知时也能按「从断点拉到末尾」续传。
func resumeRangeHeaderTail(offset, end int64) string {
	if end < 0 || end < offset {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return resumeRangeHeader(offset, end)
}

// escalateSlowSources 单源阶段的升级动作（判速触发与时间驱动的首轮补充共用）：
//   - 胜者既有连接不中断，作为首个源并入合并（天然从当前已传偏移继续）；
//   - 其余全部候选（排除胜者；此前竞速中硬失败的候选也照常尝试）**并发**发起
//     `Range: bytes={offset}-{end}` 续传——每个候选一个 goroutine（候选并发数上限 =
//     候选列表长度），尝试本体复用 startUpstream 公共底层（请求头白名单、docker token
//     dance、看门狗、read/http-timeout、候选级 insecure），严禁另复制一份 dance 逻辑。
//
// offset 为「下一个期望的绝对偏移」（胜者起点 + 已消费字节数）；end 为续传区间终点
//（<0 = 开区间到对象末尾）。返回的合并器已开始异步填充（胜者与已定局续传源的 pump
// 运行中，未定局源保持 pending）。加入动作的 Info 日志由 joinSlowSources 统一输出。
func (s *Server) escalateSlowSources(r *http.Request, winner *probeResult, offset int64, others []candidate, eff config.Options, knownTotal, end int64, labels dockerLogInfo, logSource string, extra ...any) *slowMerger {
	sources := make([]*slowSource, 0, len(others)+1)
	// 胜者源：既有连接不中断（编号首位，合并时天然优先持有下一期望偏移）
	sources = append(sources, &slowSource{name: winner.url, start: offset, readPos: offset, res: winner})
	m := newSlowMerger(sources, offset, knownTotal, eff.SlowDownloadMaxBehind, s.log)
	if len(others) > 0 {
		s.joinSlowSources(r, m, others, eff, offset, end, labels, logSource, extra...)
	}
	return m
}

// joinSlowSources 向合并器增量加入候选源（时间驱动补充与判速触发共用同一套「加入新源」
// 逻辑，实现「允许多次补充」）：
//   - 去重：已在合并器内的源（胜者 / 此前已加入的候选，含已被剪枝的）直接跳过——绝不
//     重复拉取同一候选，也绝不切换/中断已在下载的源（只做加法）；
//   - 每个新源以 offset 为 Range 起点（= 合并游标，已写给客户端的字节边界，绝不重复
//     拉取已送出区间）、end 为终点（<0 = 开区间到对象末尾）并发发起续传；尝试本体复用
//     startUpstream 公共底层与既有 206/Content-Range 校验（见 settleSlowResume）；
//   - 加入动作输出 Info「补充多源并发下载」（added 列出本次加入的源、sources 计数），
//     extra 追加调用方字段（total_source / elapsed 等）。返回本次真正加入的候选 URL。
func (s *Server) joinSlowSources(r *http.Request, m *slowMerger, cands []candidate, eff config.Options, offset, end int64, labels dockerLogInfo, logSource string, extra ...any) (added []string) {
	rangeHeader := resumeRangeHeaderTail(offset, end)
	var picked []candidate
	var fresh []*slowSource
	m.mu.Lock()
	for _, cand := range cands {
		if m.hasSourceLocked(cand.url) {
			continue // 已在合并中（含已被剪枝的）：不重复加入
		}
		src := &slowSource{name: cand.url, start: offset, readPos: offset, pending: true}
		m.sources = append(m.sources, src)
		picked = append(picked, cand)
		fresh = append(fresh, src)
	}
	joined := len(m.sources)
	m.mu.Unlock()
	if len(fresh) == 0 {
		return nil // 全部候选都已在合并中：本次没有可补充的新源（调用方已留痕 all_joined）
	}
	for i := range picked {
		cand, src := picked[i], fresh[i]
		go func() {
			s.settleSlowResume(r, src, m, cand, eff, rangeHeader, offset, labels, logSource)
			m.changed() // 定局通知（成功/失败皆然），合并循环免去 200ms tick 等待
		}()
	}
	added = make([]string, 0, len(picked))
	for _, c := range picked {
		added = append(added, c.url)
	}
	args := []any{
		"rule", eff.Rule,
		"added", truncate(strings.Join(added, " "), 900),
		"sources", len(added),
		"joined_sources", joined,
		"alive_sources", m.aliveCount(),
		"offset", offset,
		"total", m.knownTotal,
	}
	args = append(args, extra...)
	s.log.Info("补充多源并发下载", labels.attrs(logSource, args...)...)
	return added
}

// settleSlowResume 单个候选的 Range 续传尝试（joinSlowSources 的 goroutine 内执行）：
// 复用 startUpstream 公共底层（与候选探测同一套请求头/token dance/看门狗/超时/insecure）。
// 响应必须 206 且 Content-Range 起始等于请求偏移，否则丢弃该源；尝试失败同样只置 err
// （合并循环剪枝留 debug 痕）——绝不影响胜者流与客户端已见字节。源不支持 Range（回
// 200 全量）时额外输出 Info「未补充多源并发」reason=upstream_no_range（该源被跳过），
// 其余失败保留既有 Debug 留痕。成功且有活体 body 时由本协程启动该源的 pump。
// 全部状态写入在 m.mu 下进行（与合并循环并发安全）。labels/logSource 为 docker 日志
// 统一标签（image/digest + source=client|resume），与判定日志同一套键序。
func (s *Server) settleSlowResume(r *http.Request, src *slowSource, m *slowMerger, cand candidate, eff config.Options, rangeHeader string, offset int64, labels dockerLogInfo, logSource string) {
	// 与客户端断开解耦（WithoutCancel）：docker 已断开也继续填缓存（与胜者流同一语义）
	res, err := s.startUpstream(context.WithoutCancel(r.Context()), r, cand, eff, rangeHeader, nil, nil)
	if err != nil {
		m.mu.Lock()
		src.err = err
		m.mu.Unlock()
		s.log.Debug("慢速升级续传候选请求失败，该源将被剪枝",
			labels.attrs(logSource, "candidate", truncate(cand.url, 300), "range", rangeHeader, "err", err,
				"sources", m.sourceCount(), "alive_sources", m.aliveCount())...)
		return
	}
	if verr := validateResumeResponse(res.status, res.header, offset); verr != nil {
		res.Close()
		m.mu.Lock()
		src.err = verr
		m.mu.Unlock()
		if res.status == http.StatusOK {
			// 该源不支持 Range（对 bytes=N- 回 200 全量）：跳过该源并留痕 reason
			// upstream_no_range——绝不能把全量响应拼到已发字节后面（数据错位）。
			s.log.Info("未补充多源并发",
				labels.attrs(logSource, "rule", eff.Rule, "candidate", truncate(cand.url, 300),
					"reason", blobAddReasonNoRange, "status", res.status, "offset", offset,
					"range", rangeHeader, "sources", m.sourceCount(), "alive_sources", m.aliveCount())...)
		}
		s.log.Debug("慢速升级续传响应不合规，丢弃该源",
			labels.attrs(logSource, "candidate", truncate(cand.url, 300), "status", res.status, "offset", offset,
				"err", verr)...)
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
