// stream.go —— GET 路径目标（docker registry 回源）缓存 MISS / BYPASS 的流式透传：
// 候选竞速（selectCandidate）选定候选后立即向客户端 WriteHeader 并透传响应头
//（commit），把已缓冲首块写给客户端，再边读上游边同时写客户端与内存累积缓冲；
// EOF 后按既有条件（仅 200 且过判据）用 cache.Set 原子写缓存，并把完整 Entry
// 返回给单飞等待方。blob 的流式下载中断/失败时把已下载前缀保留为半截文件供下次
// 无感续传（partial_resume.go，仅注入 partial 上下文的缓存可读主路径启用）。
//
// 客户端断开即停（断开终止）：commit 后写响应体失败（docker 已断开）立即终止上游读取
// 与 slowMerger 多源合并流（逐路关闭全部上游源），blob 缓存可读主路径经 captureStreamPartial
// 保留半截（进度不回退，下次无 Range 完整 GET 从断点无感续传），并返回 errClientDisconnected
// 给调用方——客户端连接已断，绝不再向客户端写任何字节（不 502 不 writeError），仅 Warn/Debug
// 级运维事件留痕。旧版「继续读完上游只填缓存」语义废止：继续读完会长时间占用上游连接与
// 带宽（现场 500MB 层在 19% 断开后仍拖约 1.5h），其缓存价值已被「终止 + 半截保留 + 断点
// 续传」取代。
//
// 慢速升级多源并发续传（slow-download escalation，见 slowdownload.go）：type=docker
// 规则的流式回源在 commit 后的读循环里按 slow-download-window 滑动窗口判速，窗口平均
// 送出速率低于 slow-download-speed 且剩余量足够、候选数>1 时，对其余全部候选并发发起
// Range 续传并与胜者既有连接做有序合并（每流至多一次；升级只增开续传源，绝不动客户端
// 已见字节，连续性保证不变）。
//
// 连续性保证：一旦 commit（写出响应头）就绝不再切换候选或改变状态码——所有候选
// 切换/重试/竞速都发生在 selectCandidate 内部（commit 之前），commit 前的任何失败
// 都不会给客户端输出哪怕一个字节（失败时由调用方 writeError 502）。
//
// 目的：docker 客户端等响应头约 30s 就超时断开（net/http: timeout awaiting response
// headers）。旧版「整包读进内存构造 Entry 再 WriteHeader」让首字节时间 = 候选轮询
// 全部耗时 + 上游完整下载时间，必然超时；流式透传把首字节时间压缩为「选定候选」耗时。
//
// 日志规范：本文件全部日志（客户端流式主路径）统一 source=client，镜像名统一键 image
//（/v2/ 目标按需解析 image/digest/ref，非 docker 请求不加新键，见 log_attrs.go）。

package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// streamChunkSize 流式透传的读写块大小（同时是客户端写与缓存累积的粒度）。
const streamChunkSize = 32 * 1024

// errStreamingCommitted 流式执行方「commit 后失败」的错误标记（errors.Is 判定）：
// 响应头已写出、状态码已固化，调用方绝不能再 writeError（只会污染响应与日志）；
// 底层失败原因已在流式回源内部留痕（Error 级），调用方看到本标记直接终止即可。
var errStreamingCommitted = errors.New("流式回源失败（响应头已写出，终止本次响应）")

// errClientDisconnected 客户端断开（写响应体失败）终止上游读取的标记错误（errors.Is 判定）：
// 客户端连接已断开时不再「继续读完上游只填缓存」，立即终止上游读取与多源合并流，blob
// 缓存可读主路径保留半截（进度不回退）后以本错误收尾。该错误必然发生在 commit 之后
// （响应头已写出，committed=true），调用方绝不能再向客户端写任何字节（连接已断，502
// 或 writeError 只会在对端已关闭的连接上继续报错刷日志），仅按运维事件留痕（Warn/Debug
// 级，非 Error）——客户端将以断点续传（半截已保留时）或缓存重试接续。
var errClientDisconnected = errors.New("客户端已断开，终止上游读取")

// commitStreamingHeader 向客户端写出响应头（commit 点）：
// 沿用 skipResponseHeader 过滤逐跳头与自管头、补 X-Cache / X-Proxy-Upstream 观测头；
// Content-Length 用上游声明值，上游没有就不设（Go 将按 chunked 编码流式传输）。
// 调用后本请求的响应头即固化，绝不允许再切换候选或改变状态码。
func (s *Server) commitStreamingHeader(w http.ResponseWriter, res *probeResult, cacheStatus, source string) {
	h := w.Header()
	for k, vv := range res.header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	h.Set("X-Cache", cacheStatus)
	if source != "" {
		h.Set("X-Proxy-Upstream", source) // 观测本次内容来源（轮询命中的候选 URL）
	}
	if cl := res.header.Get("Content-Length"); cl != "" {
		h.Set("Content-Length", cl)
	}
	w.WriteHeader(res.status)
}

// fetchViaCandidatesStreaming GET 路径目标的流式回源：
//  1. selectCandidate 竞速选定候选（commit 前不向客户端写任何字节）；
//  2. commit：立即 WriteHeader + 透传响应头 + 写出已缓冲首块（续命 docker ~30s 响应头超时）；
//  3. 边读上游边写客户端与内存累积缓冲；客户端写失败（docker 已断开）立即终止上游读取
//     （见 6.，blob 缓存可读主路径保留半截、进度不回退）；
//  4. EOF：按 finalize 回调（既有条件：仅 200 且过判据）用 cache.Set 原子写缓存，
//     并把完整 Entry 返回给单飞等待方；
//  5. commit 后上游读取失败：blob 半截保留（仅 blob + 开关开启 + 无 Range 的缓存可读
//     主路径，进度不回退）、记日志、终止本次响应、不写缓存（半截数据绝不入库），
//     返回错误给单飞等待方（502）；
//  6. 客户端断开终止（写响应体失败，断开即停）：立即终止上游读取（多源合并场景同时
//     逐路关闭全部上游源）、按 5 同款条件保留半截、Warn 级运维事件留痕（含已接收字节
//     数与半截保留状态）、返回 errClientDisconnected——调用方绝不再向客户端写任何字节
//     （连接已断，不 502 不 writeError），客户端将以断点续传或缓存重试接续。
//
// cacheStatus 为 commit 时 X-Cache 观测头的取值（MISS / BYPASS）；finalize 为 EOF 后的
// 缓存写入回调（nil = 不写缓存，BYPASS 场景）。返回值 committed=true 表示已向客户端
// 写出响应头——此时调用方不得再 writeError（响应头已固化，写错误头只会污染日志）。
//
// partial 为可选的 blob 半截保留上下文（partial_resume.go，variadic 注入使既有调用方
// 与既有测试的 7 参调用不受影响）：仅「缓存可读的客户端主路径」注入——注入后，blob
// （/v2/<name>/blobs/<digest>）流式下载中断/失败时把已下载前缀保留为半截文件（进度不
// 回退，见 captureStreamPartial）；BYPASS（Range / no-cache / 关缓存）不注入，维持既有
// 「失败即丢弃」行为完全不变。
func (s *Server) fetchViaCandidatesStreaming(w http.ResponseWriter, r *http.Request, target string, eff config.Options, cacheStatus string, finalize func(*cache.Entry), start time.Time, partial ...partialSink) (entry *cache.Entry, committed bool, err error) {
	// docker 日志统一标签：客户端主路径全部日志统一 source=client，/v2/ 目标按需解析
	// image/digest/ref（纯字符串 split；非 docker 请求 attrs 原样透传既有键，日志零变化）
	labels := parseDockerLogInfo(target)
	sink := partialSinkOf(partial)
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	if len(candidates) == 0 {
		// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，流式路径绝不直连
		return nil, false, errors.New("路径目标请求未配置 url-redirect，无法回源（路径目标必须由命中的域名规则提供 url-redirect）")
	}

	// //////////////////  竞速选定候选（commit 前，可自由切换）  start  /////////////////////////
	res, serr := s.selectCandidate(r, candidates, eff, target)
	if serr != nil {
		// 路径目标没有完整原始 URL 可直连：fallback-direct 不适用，候选全败直接报错
		//（错误语义与旧版 fetchViaCandidates 的路径目标分支一致）
		s.log.Warn("url-redirect 全部候选失败，路径目标无直连回退（fallback-direct 不适用）",
			labels.attrs(dockerLogSourceClient, "target", truncate(target, 300), "last_error", serr)...)
		return nil, false, fmt.Errorf("全部 url-redirect 候选失败，路径目标无直连回退（无完整原始 URL 可直连）: %w", serr)
	}
	// //////////////////  竞速选定候选（commit 前，可自由切换）  end  ///////////////////////////

	source := res.url
	s.commitStreamingHeader(w, res, cacheStatus, source)
	committed = true
	// 进度日志的时间基线自 commit（写出响应头）起算：循环前的 writeFirst 已计入 total，
	// 首条进度日志的速率为自 commit 起的平均速率
	committedAt := time.Now()
	s.log.Info("流式透传开始（已向客户端写出响应头）",
		labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300), "status", res.status,
			"content_length", res.header.Get("Content-Length"), "check", eff.SuccessCheck)...)

	// 流式透传进度日志开关与基线：eff.ProgressLogInterval <= 0 关闭；lastProgressBytes 为
	// 上次进度日志时已传字节数（区间速率的分母基线），初始 0 = 首条按自 commit 起平均速率
	progressInterval := eff.ProgressLogInterval
	lastProgressAt := committedAt
	lastProgressBytes := 0
	// 上游声明的 Content-Length：可解析为正整数时进度日志附带该值与 0-100 百分比，否则不输出
	contentLength := int64(0)
	if v, err := strconv.ParseInt(strings.TrimSpace(res.header.Get("Content-Length")), 10, 64); err == nil && v > 0 {
		contentLength = v
	}

	// 活跃下载进度登记（周期汇总，见 progress_summary.go）：commit（响应头已写出）后
	// 开始写即登记 source=client，defer 注销——任何返回路径（EOF / 断开 / 读上游失败）
	// 都不留幽灵条目；每写一块经 addBytes 累加，total 取上游声明总长
	prog := s.progressRegister(dockerLogSourceClient, labels.image, labels.digest, r.Method, 0, contentLength)
	defer prog.unregister()

	// 慢速升级多源并发续传（slow-download escalation）门控与状态：
	// 仅 type=docker 规则的 GET 路径目标流式回源启用，speed<=0 / window<=0 / max-behind<=0
	// 视为关闭（未配置时 Options 已带默认值，规则显式 0 = 显式关闭）；窗口计时在循环内
	// 进行（不满足条件零开销，除窗口计时本身）。判速触发是「即时触发」，允许多次补充
	//（后续补充由下方的时间驱动判定或合并阶段判定完成）。
	slowEnabled := eff.RuleType == config.RuleTypeDocker &&
		eff.SlowDownloadSpeed > 0 && eff.SlowDownloadWindow > 0 && eff.SlowDownloadMaxBehind > 0
	slowWindowAt := committedAt // 判速滑动窗口起点（自 commit 起算）
	slowWindowBytes := 0        // 窗口起点时已传字节数
	var slowMerger *slowMerger  // 升级后的有序合并器（nil = 仍在单源阶段）

	// 时间驱动的单层 blob 多源并发补充（blob-concurrency-interval，见 slowdownload.go）：
	// 每满间隔且该层尚未完成就「只做加法」地补充尚未加入的候选源（客户端全程无感）。
	// **仅 blob 目标**（/v2/<name>/blobs/<digest>，labels.digest 非空）启用——manifest 等
	// 小目标既没有「单层」语义，也不需要并发补源。
	// 总长兜底：上游 200 无 Content-Length（现场 1.32GiB 单层故障）时用 manifest 记录的
	// layer size（digest → size 进程内索引，见 blob_size_index.go）。
	blobManifestTotal := int64(0)
	if labels.digest != "" {
		if v, ok := s.manifestBlobSize(labels.digest); ok {
			blobManifestTotal = v
		}
	}
	blobTrack := newBlobConcurrencyTrack(
		labels.digest != "" && eff.RuleType == config.RuleTypeDocker && eff.BlobConcurrencyInterval > 0,
		committedAt, eff.BlobConcurrencyInterval)
	// 单源阶段的升级目标区间：胜者响应 + manifest 兜底解析一次即可（响应头不变），
	// 之后每次判定只按「已写字节数」推进 offset。总长不可知（escalatable=false）时
	// knownTotal=0，判定按 reason=content_length_unknown 留痕。
	winnerStartAt, knownTotalAt, rangeEndAt, totalSourceAt, escalatable :=
		slowEscalationTargetWithFallback(res.status, res.header, blobManifestTotal)
	if labels.digest != "" {
		// 单层下载开始（blob 目标才输出）：total 取上游声明值（206 取 Content-Range 的
		// 对象总长），拿不到用 manifest size 兜底，都没有则 unknown（total 不可知时判速/
		// 时间补充都无法评估剩余量）。
		startTotal, startTotalSource := knownTotalAt, totalSourceAt
		if !escalatable {
			startTotal, startTotalSource = blobManifestTotal, blobTotalSourceManifest
			if blobManifestTotal <= 0 {
				startTotalSource = blobTotalSourceUnknown
			}
		}
		s.log.Info("docker blob 单层下载开始（客户端主路径）",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
				"status", res.status, "total", startTotal, "total_source", startTotalSource,
				"candidates", len(candidates), "sources", 1)...)
		if reason := blobConcurrencyOffReason(eff); reason != "" {
			// 时间驱动补充未启用的一次性原因留痕（disabled / not_docker_rule）
			s.log.Info("未补充多源并发",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
					"reason", reason, "candidates", len(candidates))...)
		}
	}
	// blobConc 时间驱动补充判定的输入（单源阶段与合并阶段共用同一份，见 slowdownload.go
	// 的 timeBlobSupplement）：merger 为 in/out，补充后回填合并器。
	var blobConc blobSupplementInput
	if blobTrack != nil {
		blobConc = blobSupplementInput{
			r:           r,
			res:         res,
			labels:      labels,
			logSource:   dockerLogSourceClient,
			eff:         eff,
			candidates:  candidates,
			startedAt:   committedAt,
			knownTotal:  knownTotalAt,
			totalSource: totalSourceAt,
			end:         rangeEndAt,
		}
		if !escalatable {
			blobConc.knownTotal, blobConc.totalSource = 0, blobTotalSourceUnknown
		}
	}

	// //////////////////  边读上游边写客户端与累积缓冲  start  ///////////////////////////////////
	clientOK := true    // 客户端写失败（docker 已断开）后置 false：不再写客户端
	var clientErr error // 客户端断开错误（writeOut/writeFirst 检测到写失败时置值，非 nil = 断开即停）
	accum := bytes.NewBuffer(make([]byte, 0, len(res.first)+streamChunkSize))
	total := 0
	// clientGoneAbort 客户端断开（clientErr 非 nil）的统一终止出口：关闭上游、按既有条件
	// 保留半截（仅 blob + 开关开启 + 无 Range 的缓存可读主路径，进度不回退，见
	// captureStreamPartial）、Warn 级运维事件留痕（含 source=client 与 image/digest 统一
	// 标签、已接收字节数与半截保留状态 partial），返回断开错误给调用方——committed=true
	//（响应头已写出），调用方绝不再向客户端写任何字节，客户端将以断点续传或缓存重试接续
	//（半截保留成功时 savePartialPart 已复用既有「blob 半截前缀已保留」日志）。
	clientGoneAbort := func() error {
		res.Close() // 幂等：多源合并场景的源已由 abortAll 逐路关闭，此处兜底胜者自身
		kept := s.captureStreamPartial(sink, r, target, accum.Bytes())
		s.log.Warn("客户端已断开，终止上游读取（不再继续读完填缓存，客户端将以断点续传或缓存重试）",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
				"status", res.status, "bytes", total, "client", "disconnected", "partial", kept,
				"err", clientErr)...)
		return clientErr
	}
	// writeOut 写一块给客户端与累积缓冲（accum 语义不变）：客户端写失败（docker 已断开）
	// 置 clientOK=false 并标记 clientErr——断开即停，读循环检测到 clientErr 立即终止上游
	// 读取（单源循环与多源合并共用；多源场景同时逐路关闭全部上游源，见 abortAll）
	writeOut := func(p []byte) {
		total += len(p)
		accum.Write(p)
		prog.addBytes(len(p)) // 活跃下载周期汇总累加（与 total 同源，含客户端已断开后仍在写缓存的块）
		if clientOK {
			if _, werr := w.Write(p); werr != nil {
				clientOK = false
				clientErr = fmt.Errorf("%w: 写响应体失败: %v", errClientDisconnected, werr)
				if slowMerger != nil {
					// 多源合并中的断开：逐路关闭全部上游源（含尚未定局的 pending 续传源），
					// 合并循环随后自然退出，绝不再读上游
					slowMerger.abortAll("客户端已断开，终止全部上游源")
				}
				// 与 serveEntry 既有日志文案同源（broken pipe 可辨认）；断开终止事件由
				// clientGoneAbort 统一留痕（含已接收字节数与半截保留状态）
				s.log.Warn("写响应体失败（客户端已断开，终止上游读取）",
					labels.attrs(dockerLogSourceClient, "target", truncate(r.URL.Path, 200), "err", werr)...)
			}
		}
	}
	writeFirst := func() {
		if len(res.first) == 0 {
			return
		}
		total += len(res.first)
		accum.Write(res.first)
		prog.addBytes(len(res.first))
		if clientOK {
			if _, werr := w.Write(res.first); werr != nil {
				clientOK = false
				clientErr = fmt.Errorf("%w: 写响应体失败: %v", errClientDisconnected, werr)
				s.log.Warn("写响应体失败（客户端已断开，终止上游读取）", labels.attrs(dockerLogSourceClient, "target", truncate(r.URL.Path, 200), "err", werr)...)
			}
		}
	}
	writeFirst()
	if clientErr != nil {
		// 首块写出即失败（客户端在 commit 后、读上游前已断开）：断开即停，终止本次回源
		return nil, true, clientGoneAbort()
	}

	// emitProgress 进度日志（单源读循环与多源合并循环同一节拍共用）：extra 追加到字段尾
	//（升级后由合并循环追加 sources=当前存活源数）
	emitProgress := func(now time.Time, extra ...any) {
		clientState := "ok"
		if !clientOK {
			clientState = "disconnected"
		}
		args := labels.attrs(dockerLogSourceClient,
			"rule", eff.Rule, "candidate", truncate(source, 300), "status", res.status,
			"bytes", total,
		)
		if contentLength > 0 {
			// 百分比取 0-100 整数（上游声明值异常偏小时截到 100，不输出越界值）
			pct := int(float64(total) / float64(contentLength) * 100)
			if pct < 0 {
				pct = 0
			} else if pct > 100 {
				pct = 100
			}
			args = append(args, "content_length", contentLength, "percent", pct)
		}
		args = append(args,
			"speed", humanBytesPerSec(int64(total-lastProgressBytes), now.Sub(lastProgressAt)),
			"elapsed", fmt.Sprintf("%.1fs", now.Sub(committedAt).Seconds()),
			"client", clientState,
		)
		args = append(args, extra...)
		s.log.Info("流式透传进度", args...)
		lastProgressAt = now
		lastProgressBytes = total
	}

	if res.body != nil {
		// 上游读经 progressReader 记入看门狗：read-timeout（无新增数据）语义与旧版一致，
		// 慢速但持续传输的大 blob 不会被误砍；客户端写阻塞（接收慢）时读循环随之放缓，
		// 窗口内只要上游仍有新数据到达就不会触发 read-timeout
		pr := &progressReader{r: res.body, wd: res.wd}
		buf := make([]byte, streamChunkSize)
		for {
			n, rerr := pr.Read(buf)
			if n > 0 {
				writeOut(buf[:n])
			}
			// 断开即停：客户端写失败（clientErr 非 nil）立即终止上游读取——进度日志自然
			// 不再输出，半截保留与运维事件留痕统一走 clientGoneAbort
			if clientErr != nil {
				return nil, true, clientGoneAbort()
			}
			// 流式透传进度日志：按 progress-interval 周期留痕回源进度（类似 docker pull 的
			// xxMB/342.7MB 进度感），间隔内整条流已完成则一条都不出（最终日志已有成败留痕）。
			// 用循环内时间检查而非定时器 goroutine 实现：上游停滞无新数据时读循环阻塞、
			// 自然不输出进度（属预期，间隔内的沉默即「无进度」本身）
			if progressInterval > 0 && time.Since(lastProgressAt) >= progressInterval {
				emitProgress(time.Now())
			}
			// 慢速升级触发判定（与进度日志同一节拍；rerr==nil 才判，EOF 同拍到达时走正常收尾）：
			// 窗口满且窗口平均送出速率 < slow-download-speed 判「过慢」，再校验剩余字节
			//（已知总长 - 当前位置）>= slow-download-min-remaining 与候选数 > 1，全部满足
			// 才升级（客户端已断开时 clientErr 非 nil，已在上方断开出口终止本流，到不了这里）。
			// 判速触发是「即时触发」，与时间驱动补充共用同一套「加入新源」逻辑与同一合并器；
			// 升级后转入合并循环，后续补充由合并循环里的时间判定继续做（允许多次补充）。
			if slowEnabled && slowMerger == nil && rerr == nil && time.Since(slowWindowAt) >= eff.SlowDownloadWindow {
				windowSent := int64(total - slowWindowBytes)
				windowElapsed := time.Since(slowWindowAt)
				slowWindowAt = time.Now() // 窗口滚动（触发与否都重新计窗）
				slowWindowBytes = total
				if slowEscalationDue(windowSent, windowElapsed, eff.SlowDownloadSpeed) {
					remaining := slowEscalationRemaining(knownTotalAt, winnerStartAt, int64(total))
					if escalatable && remaining >= eff.SlowDownloadMinRemaining && len(candidates) > 1 {
						offset := winnerStartAt + int64(total)
						others := escalationCandidates(candidates, res.url)
						s.log.Info("下载过慢，升级多源并发续传",
							labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
								"bytes", total, "content_length", knownTotalAt,
								"total_source", totalSourceAt,
								"window", eff.SlowDownloadWindow.String(),
								"window_speed", humanBytesPerSec(windowSent, windowElapsed),
								"sources", len(others)+1)...)
						// 升级动作：胜者既有连接不中断并入合并，其余全部候选并发 Range 续传
						//（每候选一个 goroutine，复用 startUpstream 公共底层）
						slowMerger = s.escalateSlowSources(r, res, offset, others, eff, knownTotalAt, rangeEndAt,
							labels, dockerLogSourceClient,
							"total_source", totalSourceAt, "elapsed", time.Since(committedAt).Round(time.Millisecond).String())
						// 回填时间驱动判定的输入（合并阶段：res 连接已移交合并器，只做增量加源）
						blobConc.merger = slowMerger
						blobConc.knownTotal, blobConc.totalSource, blobConc.end = knownTotalAt, totalSourceAt, rangeEndAt
						break // 转入下方有序合并循环（胜者 body 保持活体移交）
					}
					s.log.Debug("流式回源判慢但暂不满足升级条件",
						labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
							"window_speed", humanBytesPerSec(windowSent, windowElapsed),
							"speed", humanBytesPerSec(eff.SlowDownloadSpeed, time.Second),
							"known_total", knownTotalAt, "remaining", remaining, "total_source", totalSourceAt,
							"min_remaining", eff.SlowDownloadMinRemaining, "candidates", len(candidates))...)
				}
			}
			// 时间驱动的多源并发补充判定（与进度日志同一节拍；每满 blob-concurrency-interval
			// 一次）：单源阶段首次满足即建合并器接手胜者连接（break 转入下方合并循环），
			// 之后每轮继续判定并把尚未加入的候选源增量补进同一合并器（只做加法，不切源）。
			// 这条通道不依赖速率与 Content-Length（总长可用 manifest size 兜底），专治
			// 「上游 200 无 Content-Length → 判速升级被静默跳过」的现场故障。
			if blobTrack != nil && rerr == nil {
				if round, due := blobTrack.due(time.Now()); due {
					blobConc.round = round
					blobConc.now = time.Now()
					blobConc.bytes = int64(total)
					blobConc.offset = winnerStartAt + int64(total)
					blobConc.clientGone = clientErr != nil
					if s.timeBlobSupplement(&blobConc) {
						slowMerger = blobConc.merger
						blobConc.res = nil // 胜者连接已移交合并器：后续判定只做增量加源
						break
					}
				}
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					break // 完整读完：半截数据不存在，可以入库
				}
				// commit 后上游读取失败：blob 半截保留（仅 blob + 开关开启 + 无 Range 的
				// 缓存可读主路径；进度不回退，见 partial_resume.go）、终止本次响应、不写缓存
				res.Close()
				s.captureStreamPartial(sink, r, target, accum.Bytes())
				s.log.Error("流式回源读上游失败，终止本次响应且不写缓存（半截数据绝不入库）",
					labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
						"status", res.status, "bytes", total, "err", rerr)...)
				return nil, true, fmt.Errorf("流式回源读上游失败（%s）: %w", source, rerr)
			}
		}
	}

	// //////////////////  慢速升级后的有序合并循环  start  ///////////////////////////////////////
	if slowMerger != nil {
		// 有序合并：胜者（首位，天然从当前 total 继续）+ 全部续传源按绝对偏移严格有序
		// 写给客户端与 accum；落后源按 max-behind 剪枝收敛带宽放大；游标推进到已知总长
		// 走既有 EOF 收尾，全部源耗尽未到总长按「commit 后读上游失败」语义终止。
		// 时间驱动补充在此继续：每满 blob-concurrency-interval 把尚未加入的候选源增量
		// 补进同一合并器（只做加法，绝不切换/中断已在下载的源）。
		if _, merr := slowMerger.run(writeOut, func(now time.Time, live int) {
			// 与单源读循环同一节拍：仅到 progress-interval 间隔才输出；
			// 升级后追加 sources 字段输出当前存活源数
			if progressInterval > 0 && time.Since(lastProgressAt) >= progressInterval {
				emitProgress(now, "sources", live)
			}
			if blobTrack != nil {
				if round, due := blobTrack.due(now); due {
					blobConc.round = round
					blobConc.now = now
					blobConc.bytes = int64(total)
					blobConc.offset = slowMerger.cursor()
					blobConc.merger = slowMerger
					blobConc.clientGone = clientErr != nil
					s.timeBlobSupplement(&blobConc)
				}
			}
		}); clientErr != nil {
			// 断开即停：多源合并中的客户端断开——writeOut 已调 abortAll 逐路关闭全部上游
			// 源（含尚未定局的 pending 续传源，其尝试协程定局时发现剪枝标记自行释放连接），
			// 缓冲内未写数据一并废弃，统一走断开终止出口（半截保留 + Warn 留痕 + 断开错误）。
			// 即便 abort 恰好发生在游标已达总长的临界（run 返回成功），同样按断开终止：
			// 断开后不再有「继续收尾填缓存」，全路径断开语义一致。
			return nil, true, clientGoneAbort()
		} else if merr != nil {
			// 多源合并失败（非断开，既有语义不变）：blob 半截保留（同上——accum 即按序
			// 合并出的已送前缀）、终止本次响应、不写缓存
			s.captureStreamPartial(sink, r, target, accum.Bytes())
			s.log.Error("流式回源读上游失败，终止本次响应且不写缓存（半截数据绝不入库）",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "candidate", truncate(source, 300),
					"status", res.status, "bytes", total, "err", merr)...)
			return nil, true, fmt.Errorf("流式回源读上游失败（%s）: %w", source, merr)
		}
	}
	// //////////////////  慢速升级后的有序合并循环  end  /////////////////////////////////////////

	res.Close() // 幂等：升级路径的源（含胜者）已由合并器统一关闭
	// //////////////////  边读上游边写客户端与累积缓冲  end  /////////////////////////////////////

	entry = &cache.Entry{
		Status:   res.status,
		Header:   res.header,
		Body:     accum.Bytes(),
		StoredAt: time.Now(),
		Source:   source,
	}
	if finalize != nil {
		finalize(entry) // 既有条件（仅 200 且过判据）内用 cache.Set 原子写缓存并留痕
	}

	// //////////////////  成功返回日志（字段与 serveEntry 兼容）  start  /////////////////////////
	// 多源并发补充过的流：完成日志补充 sources / alive_sources（源总数与收尾时存活源数），
	// 与剪枝日志同一组字段名，便于聚合「一次下载总共用了几路源、收尾时还剩几路」。
	var concAttrs []any
	if slowMerger != nil {
		concAttrs = []any{"sources", slowMerger.sourceCount(), "alive_sources", slowMerger.finalAlive()}
	}
	// okArgs 组装「成功返回内容」类日志的字段（状态 200/206 共用，extra 追加多源字段）
	okArgs := func(extra ...any) []any {
		fields := []any{"rule", eff.Rule, "upstream", source, "status", res.status,
			"bytes", total, "cache", cacheStatus}
		return labels.attrs(dockerLogSourceClient, append(fields, extra...)...)
	}
	switch {
	case res.status == http.StatusPartialContent:
		s.log.Info("成功返回内容（Range 206 流式透传，不写缓存）", okArgs(concAttrs...)...)
	case res.status == http.StatusNotModified:
		s.log.Info("内容未变更（304），客户端缓存副本有效",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "upstream", source, "cache", cacheStatus)...)
	case res.status == http.StatusOK && total > 0:
		s.log.Info("成功返回内容", okArgs(concAttrs...)...)
	}
	s.log.Info("代理请求完成",
		labels.attrs(dockerLogSourceClient,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", res.status,
			"cache", cacheStatus,
			"upstream", source,
			"rule", eff.Rule,
			"bytes", total,
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
	// //////////////////  成功返回日志（字段与 serveEntry 兼容）  end  ///////////////////////////
	return entry, true, nil
}

// //////////////////  进度日志的人类可读格式化  start  /////////////////////////////////////////

// humanBytes 把字节数格式化为人类可读字符串：不足 1MB 原样输出字节数（如 8192B），
// 1MB 起按 1024 进制换算、最多保留两位小数并去掉尾零：如 8.39MB / 342.7MB / 1.21GB，
// 供进度日志的字节数与速率字段复用。
func humanBytes(n int64) string {
	if n < 1024*1024 {
		return strconv.FormatInt(n, 10) + "B"
	}
	// 档位表从 B 起与 v 的当前单位对齐（v 初始即字节数），逐档 /1024 直到不足 1024 或到顶；
	// 上方已把 <1MB 挡在 B 分支，走到这里至少落在 MB 档
	units := [...]string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	v := float64(n)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	// 保留两位小数，再去掉末尾多余的 0 与孤立小数点（342.70 -> 342.7、8.00 -> 8）
	s := strconv.FormatFloat(v, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	return s + units[i]
}

// humanBytesPerSec 把「d 时间内传输 bytes」折算为速率字符串（如 "13.2MB/s"），
// 复用 humanBytes 的格式；d <= 0（时钟回拨等异常）时输出 0B/s 兜底。
func humanBytesPerSec(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "0B/s"
	}
	return humanBytes(int64(float64(bytes)/d.Seconds())) + "/s"
}

// //////////////////  进度日志的人类可读格式化  end  ///////////////////////////////////////////
