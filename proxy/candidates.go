// candidates.go —— url-redirect 候选探测提速管线（流式 GET 与 HEAD 共用）：
//   - probe-timeout：单次候选尝试从发出请求到「响应头可用（含 token dance 的第二次
//     响应）且判据可判定」的探测预算，到期硬失败快切下一候选（见 probe.go）；
//   - 硬失败快切：上游 403 / token dance 后仍 401 / 探测预算耗尽，跳过
//     url-redirect-item-error-retry 原地重试直接切下一候选（原地重试无意义却各烧数秒）；
//   - 阶梯并发竞速（hedge）：候选按 round-robin 顺序起步，首个候选立即发出，之后每隔
//     url-redirect-hedge-delay 放行下一个候选并发探测（前一个不取消）；某候选先失败时
//     立即放行下一候选（不等间隔）；第一个通过成功判据的候选胜出，其余全部取消
//    （context 取消，连接及时释放）。hedge-delay=0 纯串行禁用竞速；
//   - 连续性保证：胜出结果返回前绝不向客户端写出任何字节，所有候选切换/重试/竞速
//     都发生在 commit 之前（commit 由流式调用方在拿到胜出结果后执行）；
//   - url-redirect-all-error-retry 整轮重试语义保持（同一 start 下标）；整轮全败的
//     判定不变：一轮 = 所有候选（含各自 item 重试）都出结果且无胜出；
//   - 扫描总预算：从首个候选发出到全部候选出结果的整体上限（candidateScanBudget），
//     到期取消全部在途尝试并按失败返回，调用方尽早 502（docker 客户端会自行重试该层）；
//     docker blob 目标用放大的预算（candidateScanBudgetFor：60s 起，按候选数×hedge 与
//     探测预算动态放大），保证 blob 的 30s 探测预算不被总预算腰斩，manifest 等小目标
//     维持 20s 快速失败节奏；
//   - 上游明确 4xx 载荷（见 upstream_status.go）：候选判据不过且状态码为 4xx（401 除外）
//     时把「状态码+响应头+body（探测量级）」装进该候选失败错误；全部候选失败后随最终
//     错误上浮，由客户端出口透传给 docker（网络/超时类失败与扫描预算耗尽不带载荷，仍 502）。

package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"proxy-cache/config"
)

// candidateScanBudget 候选扫描总预算（非 blob 目标）：selectCandidate 从首个候选发出到
// 全部候选出结果的整体上限，到期取消全部在途尝试并按失败返回（调用方 502，docker 自行
// 重试该层）。取值依据：docker 客户端等响应头约 30s 就超时断开，留出 commit 与首块写出
// 的余量后取 20s。默认参数（probe 5s / hedge 2s / item 0 / round 0，5 候选）下单轮最坏 ≈
// (5-1)*2s + 5s = 13s，不会被它触发；只有 item/round 重试叠加的极坏情况（理论串行
// 36s+）才会触顶提前失败。
// docker blob 目标不用本值（见 candidateScanBudgetFor / candidateScanBudgetBlob）：
// blob 的探测预算缺省是 30s，20s 总预算会先把扫描掐死，等于 blob 探测预算白加。
const candidateScanBudget = 20 * time.Second

// candidateScanBudgetBlob docker blob 目标的候选扫描总预算下限：60s。
// 为什么不沿用 20s：blob 的候选探测预算缺省是 30s（config.DefaultBlobURLRedirectProbeTimeout），
// 而 20s 的总预算会在探测预算用完之前就把整轮扫描掐死——现场日志正是「1ms.run 已经拿到
// 206（token dance 花 2.9s），随后仍在 probe-timeout 处被判硬失败」+「候选扫描总预算
// 耗尽 budget=20s → 502」，配了 url-redirect-all-error-retry: 1 也因预算耗尽形同虚设。
// 单轮最坏 ≈ (候选数-1)×hedge 放行间隔 + 单个候选探测预算（默认 7 候选 × 2s + 30s = 42s），
// 再留整轮重试与收尾余量，故取 60s 下限；候选更多 / 候选级探测预算更大时按
// blobCandidateScanBudget 动态放大。
//
// 与「commit 早于 docker 约 30s 响应头超时」的关系（别误读）：扫描总预算只是**放弃阈值**，
// 不参与 commit 时机——hedge 竞速下「首个通过判据的候选」立即胜出并 commit，commit 时刻
// 与预算大小无关；放大预算只影响「全都慢时多久放弃」，绝不会把已经就绪的候选拖在预算里。
const candidateScanBudgetBlob = 60 * time.Second

// blobScanSlack blob 扫描总预算里给「整轮重试 + 收尾」留的余量：单轮最坏之外再给 20s，
// 让 url-redirect-all-error-retry 在预算内还有机会真正跑一轮（现场配了 1 轮却因 20s
// 总预算耗尽而形同虚设），同时不至于让失败收场拖得过久。
const blobScanSlack = 20 * time.Second

// candidateScanBudgetFor 返回本次候选扫描的总预算（见 selectCandidate 的 scanTimer）：
//   - 非 blob 目标：沿用 candidateScanBudget（20s），manifest / web 等小响应保持既有快速
//     失败节奏（行为与旧版完全一致）；
//   - docker blob 目标：max(candidateScanBudgetBlob, 单轮最坏 + blobScanSlack)，其中
//     「单轮最坏 = (候选数-1)×hedge 放行间隔 + 候选级探测预算的最大值」——保证 blob 的
//     30s（或候选级显式配置的更大值）探测预算真的能在一次扫描里用完，而不是被总预算腰斩。
//
// target 为 ExtractTarget 的目标原文（blob 判定复用 parseDockerBlobTarget）；eff 提供
// 候选级/规则级/全局解析后的探测预算与 hedge 间隔（与真实扫描用同一套取值）。
func candidateScanBudgetFor(target string, n int, eff config.Options) time.Duration {
	if _, ok := parseDockerBlobTarget(target); !ok {
		return candidateScanBudget
	}
	return blobCandidateScanBudget(n, eff)
}

// blobCandidateScanBudget docker blob 目标的扫描总预算（candidateScanBudgetFor 的 blob
// 分支；续传扫描 scanResumeCandidate 只服务 blob，同样复用本函数）。
func blobCandidateScanBudget(n int, eff config.Options) time.Duration {
	maxProbe := time.Duration(0)
	for _, item := range eff.URLRedirect {
		d := eff.CandidateProbeTimeoutFor(item, true)
		if d <= 0 {
			// 显式 0 = 不限制：估算时按 blob 缺省代替，否则预算会被算成无穷大（永不放弃）
			d = config.DefaultBlobURLRedirectProbeTimeout
		}
		if d > maxProbe {
			maxProbe = d
		}
	}
	if maxProbe <= 0 {
		// 防御：候选列表为空（理论上不会走到扫描）时按 blob 缺省估算
		maxProbe = config.DefaultBlobURLRedirectProbeTimeout
	}
	worstRound := time.Duration(max0(n-1))*eff.URLRedirectHedgeDelay + maxProbe + blobScanSlack
	if worstRound < candidateScanBudgetBlob {
		return candidateScanBudgetBlob
	}
	return worstRound
}

// candidateOutcome 是一个候选（含其全部 item 尝试）的最终结果。
type candidateOutcome struct {
	// res 胜出的探测结果（won=true 时非 nil，资源所有权移交调用方）。
	res *probeResult
	// err 失败原因（won=false 时为该候选的最后一个错误；可被整体取消置 nil）。
	err error
	// won 是否胜出（首个通过成功判据）。
	won bool
	// slot 该候选在本轮内的放行序号（用于胜出时豁免其自身的取消）。
	slot int
}

// selectCandidate 在候选列表上执行「探测提速管线」，返回首个通过成功判据的候选探测
// 结果（活体 body 由调用方负责 Close）。候选全败返回最后一个错误（供调用方拼装
// fallback-direct / 路径目标无直连回退的既有错误语义）。
//
// 轮询起始下标沿用全局 rr 计数器（round-robin 语义不变，每个请求 +1）；
// 请求方法沿用入站方法（GET/HEAD），Range 透传与否由成功判据（probePassed）处理。
//
// target 为本次回源的目标原文：只用于按目标类型取候选扫描总预算（docker blob 放大，
// 见 candidateScanBudgetFor）；非 blob 目标的总预算与旧版完全一致。
func (s *Server) selectCandidate(r *http.Request, candidates []candidate, eff config.Options, target string) (*probeResult, error) {
	n := len(candidates)
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  start  ////////
	start := int(s.rr.Add(1)-1) % n
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  end  ////////
	hasRange := r.Header.Get("Range") != ""
	// 路径目标（registry API 语义）标志：探测判据与硬失败判定用——路径目标下上游回
	// text/html 必为反爬/错误页，直接判失败并硬失败切下一候选（见 probePassed /
	// probeHardFailReason / htmlPageContentType 的现场动机注释）
	pathTarget := isPathTarget(target)

	// //////////////////  轮询展开明细日志  start  ////////////////////////////////////////////////
	// 与旧版一致：按「本轮实际尝试顺序」输出每个候选的模板与展开后 URL，并给出本轮
	// round-robin 起始下标（0 基，指向候选对象列表下标）。Info 级保证默认日志级别下可见。
	details := make([]string, 0, n)
	for i := range candidates {
		idx := (start + i) % n // 该候选在候选对象列表中的原始下标
		details = append(details, fmt.Sprintf("模板[%d] %s -> %s", idx, eff.URLRedirect[idx].Template, candidates[idx].url))
	}
	s.log.Info("url-redirect 轮询展开", "start_index", start,
		"count", n, "candidates", details)
	// //////////////////  轮询展开明细日志  end  ////////////////////////////////////////////////

	itemTotalAttempts := 1 + max0(eff.URLRedirectItemErrorRetry)
	totalRounds := 1 + max0(eff.URLRedirectAllErrorRetry)

	// 基础 ctx：与客户端断开解耦（WithoutCancel）——客户端断开不中断候选扫描，
	// 保证「缓存填充完成」语义与旧版一致。cancelBase 仅由扫描总预算定时器触发；
	// 胜出即停表，并把 cancelBase 挂到胜出结果的资源链上（流结束后统一释放）。
	// 总预算按目标类型取：非 blob 20s（旧版一致）、docker blob 放大（见 candidateScanBudgetFor）
	scanBudget := candidateScanBudgetFor(target, n, eff)
	base, cancelBase := context.WithCancel(context.WithoutCancel(r.Context()))
	scanTimer := time.AfterFunc(scanBudget, cancelBase)

	var (
		schedMu    sync.Mutex
		round      int         // 当前轮次（1 基；轮边界在 schedMu 下自增）
		released   int         // 本轮已放行候选数（schedMu 下读写）
		hedgeTimer *time.Timer // 已武装的 hedge 放行定时器（nil = 未武装；schedMu 下读写）
		cancels       = make([]context.CancelFunc, n)
		lastErr       error // 最近一次候选失败（全败后作为最终错误返回，后写覆盖前写）
		lastStatusErr error // 最近一次「上游明确 4xx」候选失败（全败出口的网络类最终失败兜底，见下方出口注释）
	)
	done := make(chan candidateOutcome, n)

	// runCandidate 运行一个候选的全部 item 尝试（硬失败提前收手），最终结果投递到 done。
	// ctx/cancel 由放行点（releaseNext）同步创建并注册，胜出分支因此必定能取消任何
	// 已放行的候选（包括 goroutine 尚未起跑的）；胜出时该候选 ctx 的取消权移交给
	// 探测结果（流结束后释放），失败/被取消时自行回收。
	// roundNo 为轮次（1 基，值传递避免与主循环共享变量产生数据竞争）。
	runCandidate := func(slot, roundNo int, ctx context.Context, cancel context.CancelFunc) {
		idx := (start + slot) % n // 该候选在候选对象列表中的原始下标
		cand := candidates[idx]   // 含该候选生效的 insecure-skip-verify
		tpl := eff.URLRedirect[idx].Template
		handOff := false // 胜出后取消权已移交给 probeResult
		defer func() {
			if !handOff {
				cancel()
			}
		}()
		// goroutine 边界无法把 panic 交还上层单飞的 recover（不处理会让整个进程崩溃、
		// 所有在途请求陪葬）：这里按该候选失败收场并留 Error 日志，把影响隔离到单个请求
		defer func() {
			if rec := recover(); rec != nil && !handOff {
				s.log.Error("url-redirect 候选尝试 panic，已按该候选失败处理",
					"round", roundNo, "round_total", totalRounds,
					"index", idx, "template", tpl, "candidate", cand.url, "panic", rec)
				done <- candidateOutcome{err: fmt.Errorf("候选尝试 panic: %v", rec), slot: slot}
			}
		}()
		candLast := error(nil)
		for attempt := 1; attempt <= itemTotalAttempts; attempt++ {
			if ctx.Err() != nil {
				break // 已被胜者/扫描总预算取消：本候选提前收场
			}
			s.log.Debug("尝试 url-redirect 候选",
				"round", roundNo, "round_total", totalRounds,
				"attempt", attempt, "attempt_total", itemTotalAttempts,
				"index", idx, "template", tpl, "candidate", cand.url)
			candStart := time.Now()
			res, err, hard, hardReason := s.probeCandidateAttempt(ctx, r, cand, eff)
			cost := time.Since(candStart).Round(time.Millisecond)
			if err != nil {
				if ctx.Err() != nil {
					// 被取消导致的失败（其他候选胜出/预算到期）：降为 Debug，避免噪音
					s.log.Debug("url-redirect 候选已取消（其他候选胜出或扫描预算到期）",
						"index", idx, "template", tpl, "candidate", cand.url)
					break
				}
				candLast = err
				s.log.Warn("url-redirect 候选请求失败",
					"round", roundNo, "round_total", totalRounds,
					"attempt", attempt, "attempt_total", itemTotalAttempts,
					"index", idx, "template", tpl, "candidate", cand.url,
					"cost", cost.String(), "err", err)
				if hard {
					s.log.Warn("url-redirect 候选硬失败，跳过原地重试直接下一候选",
						"round", roundNo, "round_total", totalRounds,
						"attempt", attempt, "attempt_total", itemTotalAttempts,
						"index", idx, "template", tpl, "candidate", cand.url,
						"hard_fail", hardReason)
					break
				}
				continue // 软失败：按 url-redirect-item-error-retry 原地立即重试
			}
			if !probePassed(res, eff, r.Method, hasRange, pathTarget) {
				if hr := probeHardFailReason(res, pathTarget); hr != "" {
					hard, hardReason = true, hr
				}
				candLast = fmt.Errorf("候选 %s 未通过 success-check=%s（状态码 %d，body %d 字节）",
					cand.url, eff.SuccessCheck, res.status, len(res.first))
				if hard {
					candLast = fmt.Errorf("%w（hard-fail: %s）", candLast, hardReason)
				}
				// 上游明确 4xx（401 除外——由既有匿名 token dance 语义处理）：把「状态码 +
				// 可透传响应头 + body（探测量级）」装进 candLast（upstreamStatusError 包住
				// 原始失败描述，Error() 文案与日志格式不变）。候选全败后它随最终错误上浮，
				// 由客户端出口判定透传（如镜像站对 referrers 端点回 404：docker 客户端对该
				// 语义自带 fallback，透传 404 即可正常完成 pull）；网络/超时类失败不产生
				// 载荷，维持既有 502。换下一候选重试的既有语义不变（载荷只在全败后上浮）。
				if use := newUpstreamStatusError(res, cand.url, candLast); use != nil {
					candLast = use
				}
				s.log.Warn("url-redirect 候选未通过成功判据",
					"round", roundNo, "round_total", totalRounds,
					"attempt", attempt, "attempt_total", itemTotalAttempts,
					"index", idx, "template", tpl, "candidate", cand.url,
					"check", eff.SuccessCheck, "status", res.status, "body_bytes", len(res.first))
				res.Close()
				if hard {
					s.log.Warn("url-redirect 候选硬失败，跳过原地重试直接下一候选",
						"round", roundNo, "round_total", totalRounds,
						"attempt", attempt, "attempt_total", itemTotalAttempts,
						"index", idx, "template", tpl, "candidate", cand.url,
						"hard_fail", hardReason)
					break
				}
				continue
			}
			// 胜出：首个通过成功判据的候选，结果移交调用方（commit 前未向客户端写任何字节）
			s.log.Info("url-redirect 候选命中并作为最终返回",
				"round", roundNo, "round_total", totalRounds,
				"attempt", attempt, "attempt_total", itemTotalAttempts,
				"index", idx, "template", tpl, "candidate", cand.url, "status", res.status,
				"bytes", len(res.first), "cost", cost.String())
			res.onClose(cancel) // 该候选 ctx 的回收随流结束/放弃执行
			handOff = true
			done <- candidateOutcome{res: res, won: true, slot: slot}
			return
		}
		done <- candidateOutcome{err: candLast, slot: slot}
	}

	// releaseNext 放行本轮下一个候选（失败快切与 hedge 定时器共用入口，
	// 以 released 计数保证幂等）；放行后按 hedge-delay 重新武装下一个的定时器。
	//（先声明再赋值：hedge 定时器回调需要引用 releaseNext 自身；round/released/hedgeTimer
	// 均在 schedMu 下读写，定时器回调与接收循环并发安全）
	var releaseNext func()
	releaseNext = func() {
		schedMu.Lock()
		if released >= n {
			schedMu.Unlock()
			return
		}
		slot := released
		released++
		roundNo := round
		// ctx 在放行点同步创建并注册（而非在 goroutine 内）：保证胜出分支取消其余
		// 候选时，任何已放行候选（含 goroutine 尚未起跑的）都有 cancel 可取消
		ctx, cancel := context.WithCancel(base)
		cancels[slot] = cancel
		schedMu.Unlock()
		s.log.Debug("url-redirect 候选竞速放行", "round", roundNo,
			"slot", slot+1, "slot_total", n,
			"hedge_delay", eff.URLRedirectHedgeDelay.String())
		go runCandidate(slot, roundNo, ctx, cancel)
		schedMu.Lock()
		if eff.URLRedirectHedgeDelay > 0 && released < n {
			if hedgeTimer != nil {
				hedgeTimer.Stop()
			}
			hedgeTimer = time.AfterFunc(eff.URLRedirectHedgeDelay, releaseNext)
		}
		schedMu.Unlock()
	}

	// //////////////////  逐轮竞速（含整轮重试）  start  ////////////////////////////////////////
	// round 为当前轮次（1 基）；轮边界处没有在途放行（上一轮全部候选已出结果），
	// round 的自增在 schedMu 下进行，与定时器回调互斥。
	var winner *candidateOutcome
	received := 0 // 本轮已消费的结果数（胜出分支据此排干剩余结果）
	for winner == nil {
		schedMu.Lock()
		if round >= totalRounds {
			schedMu.Unlock()
			break
		}
		round++
		released = 0
		received = 0
		if hedgeTimer != nil {
			hedgeTimer.Stop()
			hedgeTimer = nil
		}
		schedMu.Unlock()

		releaseNext() // 本轮首个候选立即发出

		for received < n && winner == nil {
			oc := <-done
			received++
			if oc.won {
				w := oc
				winner = &w
				break
			}
			if oc.err != nil {
				lastErr = oc.err
				// 记下扫描期间出现过的「上游明确 4xx」载荷失败（404/403/429 等，401 被
				// newUpstreamStatusError 排除不会到这里）：全败出口在最终失败为网络/超时类
				// 时用它兜底透传。现场动机（2026-11-07 opencode-to-openai 拉取失败）：referrers
				// 目标下 1panel.live 明确回了 404（docker 客户端本可容忍自行 fallback），但其余
				// 候选全是 DNS 失败 / dance 后 401 / probe-timeout，最终 lastErr 落在网络类的
				// probe-timeout 上，404 载荷被丢弃 → docker 收 502 判整次 pull 失败（层其实
				// 已全部拉完），必须整次重试。
				if _, ok := asUpstreamStatusError(oc.err); ok {
					lastStatusErr = oc.err
				}
			}
			// 失败快切：下一候选尚未被 hedge 定时器放行时立即放行（不等间隔）
			schedMu.Lock()
			pending := released < n
			var t *time.Timer
			if pending {
				t = hedgeTimer
				hedgeTimer = nil
			}
			schedMu.Unlock()
			if pending {
				if t != nil {
					t.Stop()
				}
				releaseNext()
			}
		}
		if winner == nil && round < totalRounds {
			// 整轮全败且还有重试轮：留痕后整体重来（沿用同一候选顺序/同一 start 下标）
			s.log.Warn("url-redirect 整轮候选全部失败，按 url-redirect-all-error-retry 重试下一轮",
				"round", round, "round_total", totalRounds, "last_error", lastErr)
		}
	}
	// //////////////////  逐轮竞速（含整轮重试）  end  //////////////////////////////////////////

	if winner != nil {
		// 胜出：先停扫描预算与 hedge 定时器（预算到期取消会连坐胜者在途 body 读取，
		// hedge 定时器再放行只会白打上游），再取消其余在途尝试（context 取消，连接及时
		// 释放）；胜者自身的取消权已随结果移交（slot 豁免），流结束后统一回收。
		scanTimer.Stop()
		schedMu.Lock()
		if hedgeTimer != nil {
			hedgeTimer.Stop()
			hedgeTimer = nil
		}
		for slot, c := range cancels {
			if c != nil && slot != winner.slot {
				c()
			}
		}
		// 排干剩余结果：每个已放行候选恰好发送一次结果；其余已取消的候选会很快失败上报，
		// 其中「竞速并列同时胜出」的极端并发下落在本通道里的胜出探测结果（活体 body +
		// 看门狗/超时预算）必须在这里统一释放，否则连接与计时器泄漏
		remain := released - received
		schedMu.Unlock()
		for i := 0; i < remain; i++ {
			oc := <-done
			if oc.won && oc.res != nil {
				oc.res.Close()
			}
		}
		winner.res.onClose(cancelBase)
		return winner.res, nil
	}

	// //////////////////  全部轮次失败：回收并返回最后一个错误  start  //////////////////////////
	budgetExhausted := base.Err() != nil // 仅扫描总预算定时器会取消 base
	scanTimer.Stop()
	schedMu.Lock()
	if hedgeTimer != nil {
		hedgeTimer.Stop()
		hedgeTimer = nil
	}
	schedMu.Unlock()
	cancelBase()
	if budgetExhausted {
		// 扫描总预算耗尽按超时语义：剥离可能存在的 4xx 透传载荷（载荷只允许在
		// 「拿到了明确上游响应」的失败上浮；预算到期属超时类失败，维持 502）
		lastErr = stripUpstreamStatus(lastErr)
		s.log.Error("url-redirect 候选扫描总预算耗尽，提前终止候选扫描",
			"budget", scanBudget.String(), "round", round,
			"round_total", totalRounds, "last_error", lastErr)
		return nil, fmt.Errorf("候选扫描总预算（%s）耗尽: %w", scanBudget, lastErr)
	}
	if lastErr == nil {
		lastErr = errors.New("全部 url-redirect 候选尝试均未产生结果")
	}
	// 自然跑完全部轮次后的兜底透传：最终失败为网络/超时类（无 4xx 载荷）而扫描期间出现过
	// 「上游明确 4xx」时，改用最近一次明确 4xx 载荷上浮。它是对该目标的真实上游响应，docker
	// 对 referrers 404 / 限流 429 等语义自带容错，透传后 pull 可正常完成；返回裸 502 则
	// docker 判整次 pull 失败（现场动机见上方 lastStatusErr 记录处注释）。仅本出口兜底：
	// 预算耗尽出口维持 502 语义不变——对 blob 而言把「上游还没同步 / 还在赶路」透传成
	// 4xx 有把可重试失败误判成「对象不存在」的风险（见 stripUpstreamStatus 注释）。
	if _, ok := asUpstreamStatusError(lastErr); !ok {
		if fallback, ok2 := asUpstreamStatusError(lastStatusErr); ok2 && fallback != nil {
			s.log.Warn("url-redirect 全部候选失败：最终失败为网络/超时类，改用扫描期间最近的上游明确 4xx 响应透传",
				"last_error", lastErr.Error(),
				"status", fallback.status,
				"candidate", fallback.url)
			return nil, lastStatusErr
		}
	}
	return nil, lastErr
	// //////////////////  全部轮次失败：回收并返回最后一个错误  end  ////////////////////////////
}
