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
//     到期取消全部在途尝试并按失败返回，调用方尽早 502（docker 客户端会自行重试该层）。

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

// candidateScanBudget 候选扫描总预算：selectCandidate 从首个候选发出到全部候选出结果
// 的整体上限，到期取消全部在途尝试并按失败返回（调用方 502，docker 自行重试该层）。
// 取值依据：docker 客户端等响应头约 30s 就超时断开，留出 commit 与首块写出的余量后取
// 20s。默认参数（probe 5s / hedge 2s / item 0 / round 0，5 候选）下单轮最坏 ≈
// (5-1)*2s + 5s = 13s，不会被它触发；只有 item/round 重试叠加的极坏情况（理论串行
// 36s+）才会触顶提前失败。
const candidateScanBudget = 20 * time.Second

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
func (s *Server) selectCandidate(r *http.Request, candidates []string, eff config.Options) (*probeResult, error) {
	n := len(candidates)
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  start  ////////
	start := int(s.rr.Add(1)-1) % n
	// //////////////////  轮询起始下标：第 1 次请求从 0 开始，之后逐次 +1  end  ////////
	hasRange := r.Header.Get("Range") != ""

	// //////////////////  轮询展开明细日志  start  ////////////////////////////////////////////////
	// 与旧版一致：按「本轮实际尝试顺序」输出每个候选的模板与展开后 URL，并给出本轮
	// round-robin 起始下标（0 基，指向模板列表下标）。Info 级保证默认日志级别下可见。
	details := make([]string, 0, n)
	for i := range candidates {
		idx := (start + i) % n // 该候选在模板列表中的原始下标
		details = append(details, fmt.Sprintf("模板[%d] %s -> %s", idx, eff.URLRedirect[idx], candidates[idx]))
	}
	s.log.Info("url-redirect 轮询展开", "start_index", start,
		"count", n, "candidates", details)
	// //////////////////  轮询展开明细日志  end  ////////////////////////////////////////////////

	itemTotalAttempts := 1 + max0(eff.URLRedirectItemErrorRetry)
	totalRounds := 1 + max0(eff.URLRedirectAllErrorRetry)

	// 基础 ctx：与客户端断开解耦（WithoutCancel）——客户端断开不中断候选扫描，
	// 保证「缓存填充完成」语义与旧版一致。cancelBase 仅由扫描总预算定时器触发；
	// 胜出即停表，并把 cancelBase 挂到胜出结果的资源链上（流结束后统一释放）。
	base, cancelBase := context.WithCancel(context.WithoutCancel(r.Context()))
	scanTimer := time.AfterFunc(candidateScanBudget, cancelBase)

	var (
		schedMu    sync.Mutex
		round      int         // 当前轮次（1 基；轮边界在 schedMu 下自增）
		released   int         // 本轮已放行候选数（schedMu 下读写）
		hedgeTimer *time.Timer // 已武装的 hedge 放行定时器（nil = 未武装；schedMu 下读写）
		cancels    = make([]context.CancelFunc, n)
		lastErr    error
	)
	done := make(chan candidateOutcome, n)

	// runCandidate 运行一个候选的全部 item 尝试（硬失败提前收手），最终结果投递到 done。
	// ctx/cancel 由放行点（releaseNext）同步创建并注册，胜出分支因此必定能取消任何
	// 已放行的候选（包括 goroutine 尚未起跑的）；胜出时该候选 ctx 的取消权移交给
	// 探测结果（流结束后释放），失败/被取消时自行回收。
	// roundNo 为轮次（1 基，值传递避免与主循环共享变量产生数据竞争）。
	runCandidate := func(slot, roundNo int, ctx context.Context, cancel context.CancelFunc) {
		idx := (start + slot) % n // 该候选在模板列表中的原始下标
		cand := candidates[idx]
		tpl := eff.URLRedirect[idx]
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
					"index", idx, "template", tpl, "candidate", cand, "panic", rec)
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
				"index", idx, "template", tpl, "candidate", cand)
			candStart := time.Now()
			res, err, hard, hardReason := s.probeCandidateAttempt(ctx, r, cand, eff)
			cost := time.Since(candStart).Round(time.Millisecond)
			if err != nil {
				if ctx.Err() != nil {
					// 被取消导致的失败（其他候选胜出/预算到期）：降为 Debug，避免噪音
					s.log.Debug("url-redirect 候选已取消（其他候选胜出或扫描预算到期）",
						"index", idx, "template", tpl, "candidate", cand)
					break
				}
				candLast = err
				s.log.Warn("url-redirect 候选请求失败",
					"round", roundNo, "round_total", totalRounds,
					"attempt", attempt, "attempt_total", itemTotalAttempts,
					"index", idx, "template", tpl, "candidate", cand,
					"cost", cost.String(), "err", err)
				if hard {
					s.log.Warn("url-redirect 候选硬失败，跳过原地重试直接下一候选",
						"round", roundNo, "round_total", totalRounds,
						"attempt", attempt, "attempt_total", itemTotalAttempts,
						"index", idx, "template", tpl, "candidate", cand,
						"hard_fail", hardReason)
					break
				}
				continue // 软失败：按 url-redirect-item-error-retry 原地立即重试
			}
			if !probePassed(res, eff, r.Method, hasRange) {
				if hr := probeHardFailReason(res); hr != "" {
					hard, hardReason = true, hr
				}
				candLast = fmt.Errorf("候选 %s 未通过 success-check=%s（状态码 %d，body %d 字节）",
					cand, eff.SuccessCheck, res.status, len(res.first))
				if hard {
					candLast = fmt.Errorf("%w（hard-fail: %s）", candLast, hardReason)
				}
				s.log.Warn("url-redirect 候选未通过成功判据",
					"round", roundNo, "round_total", totalRounds,
					"attempt", attempt, "attempt_total", itemTotalAttempts,
					"index", idx, "template", tpl, "candidate", cand,
					"check", eff.SuccessCheck, "status", res.status, "body_bytes", len(res.first))
				res.Close()
				if hard {
					s.log.Warn("url-redirect 候选硬失败，跳过原地重试直接下一候选",
						"round", roundNo, "round_total", totalRounds,
						"attempt", attempt, "attempt_total", itemTotalAttempts,
						"index", idx, "template", tpl, "candidate", cand,
						"hard_fail", hardReason)
					break
				}
				continue
			}
			// 胜出：首个通过成功判据的候选，结果移交调用方（commit 前未向客户端写任何字节）
			s.log.Info("url-redirect 候选命中并作为最终返回",
				"round", roundNo, "round_total", totalRounds,
				"attempt", attempt, "attempt_total", itemTotalAttempts,
				"index", idx, "template", tpl, "candidate", cand, "status", res.status,
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
		s.log.Error("url-redirect 候选扫描总预算耗尽，提前终止候选扫描",
			"budget", candidateScanBudget.String(), "round", round,
			"round_total", totalRounds, "last_error", lastErr)
		return nil, fmt.Errorf("候选扫描总预算（%s）耗尽: %w", candidateScanBudget, lastErr)
	}
	if lastErr == nil {
		lastErr = errors.New("全部 url-redirect 候选尝试均未产生结果")
	}
	return nil, lastErr
	// //////////////////  全部轮次失败：回收并返回最后一个错误  end  ////////////////////////////
}
