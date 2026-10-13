// probe.go —— url-redirect 候选管线的「探测式」单次上游尝试：
// 响应头可用（含 401 token dance 的第二次响应）且成功判据可判定（首块读出或 EOF）
// 即返回，body 保持活体，由调用方决定流式透传（GET 路径目标缓存 MISS）还是
// 排干成完整 Entry（HEAD / 完整 URL 缓冲回放）。
//
// 探测预算（probe-timeout）只覆盖「发出请求 → 响应头可用 → 首块/EOF 判定完成」的
// 探测阶段（含 token dance 全程），到期取消该次尝试并按硬失败快切下一候选；
// 探测通过后预算即解除，后续读 body 不受它约束（仍受 http-timeout / read-timeout 约束，
// 建连本身由 connect-timeout 负责，见 probeCandidateAttempt 注释）。

package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// probeFirstChunkLimit 探测阶段首块读取上限：success-check=content 只需判定
// 「body 非空」，读到首块即停，剩余 body 留给 commit 后的流式透传/缓冲排干阶段。
const probeFirstChunkLimit = 64 << 10

// probeResult 是一次上游尝试探测阶段的结果（资源所有权随返回值移交调用方：
// 活体 body、read-timeout 看门狗、http-timeout/探测预算的 ctx 取消链都挂在
// onClose 链上，由 Close 在流结束/放弃时统一释放）。
type probeResult struct {
	// status 上游响应状态码。
	status int
	// header 深拷贝后的上游响应头（透传给客户端 / 写入缓存条目）。
	header http.Header
	// first 探测阶段读到的首块（≤probeFirstChunkLimit；HEAD/304 等无 body 时为空）。
	first []byte
	// body 活体响应体（首块之后的部分）；nil = 已无数据（HEAD / 304 / 探测期已到 EOF）。
	body io.ReadCloser
	// eof 探测阶段是否已读到 EOF。
	eof bool
	// url 本次尝试的候选 URL（观测与日志）。
	url string
	// danced 是否完成过 401 匿名 token dance（dance 后仍 401 → 硬失败依据）。
	danced bool
	// wd 本次尝试的 read-timeout 看门狗（活体 body 读取仍需经它记进度）。
	wd *progressWatchdog

	onCloseFns []func()
	closeOnce  sync.Once
}

// onClose 追加资源释放回调（Close 时按注册的逆序执行，幂等）。
func (p *probeResult) onClose(fn func()) { p.onCloseFns = append(p.onCloseFns, fn) }

// Close 释放探测结果的全部资源：关闭活体 body、停看门狗、取消 ctx 预算链（幂等）。
func (p *probeResult) Close() {
	p.closeOnce.Do(func() {
		if p.body != nil {
			_ = p.body.Close()
			p.body = nil
		}
		for i := len(p.onCloseFns) - 1; i >= 0; i-- {
			p.onCloseFns[i]()
		}
	})
}

// newProbeResultFromResponse 从已收到响应头的 http.Response 构建探测结果：
// 304 / 204 等无实体语义的状态排干并关闭 body（保证连接可复用），其余保持活体。
func newProbeResultFromResponse(resp *http.Response, wd *progressWatchdog, urlStr string) *probeResult {
	p := &probeResult{status: resp.StatusCode, url: urlStr, wd: wd}
	p.header = make(http.Header, len(resp.Header))
	for k, vv := range resp.Header {
		p.header[k] = append([]string(nil), vv...)
	}
	p.body = resp.Body
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotModified:
		_, _ = io.Copy(io.Discard, resp.Body) // 无实体：排干（通常 0 字节）后关闭
		_ = resp.Body.Close()
		p.body = nil
		p.eof = true
	}
	return p
}

// readProbeFirst 读取探测首块（有限缓冲 probeFirstChunkLimit）：
// 循环读至拿到非空数据、EOF 或错误（0,nil 的空读继续等）；返回 (首块, 是否EOF, 错误)。
// 每次读到数据都经 progressReader 记入看门狗（read-timeout 进度）。
func readProbeFirst(body io.Reader, wd *progressWatchdog) ([]byte, bool, error) {
	if body == nil {
		return nil, true, nil
	}
	pr := &progressReader{r: body, wd: wd}
	buf := make([]byte, probeFirstChunkLimit)
	total := 0
	for {
		n, err := pr.Read(buf[total:])
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				return buf[:total], true, nil
			}
			return buf[:total], false, err
		}
		if total > 0 {
			return buf[:total], false, nil
		}
	}
}

// probePassed 判定探测结果是否已满足成功判据（可选定该候选并立即 commit）：
//   - 请求带 Range 且上游回 206：视为成功（docker 断点续传必需；旧判据会把 206
//     全部拒掉导致 Range 拉取全挂。缓存写入条件不变——206 仍不写缓存）；
//   - 其余沿用 checkSuccess 既有语义：304 视为成功；content 判据对已缓冲首块判非空
//     （首块为空必然伴随 EOF/错误，等价于旧版「完整 body 为空」的失败判定）；
//     HEAD 响应无 body，自动按 status 语义。
func probePassed(res *probeResult, eff config.Options, method string, hasRange bool) bool {
	if hasRange && res.status == http.StatusPartialContent {
		return true
	}
	return checkSuccess(&cache.Entry{Status: res.status, Body: res.first}, eff.SuccessCheck, method)
}

// probeHardFailReason 返回探测结果的硬失败原因（空串 = 非硬失败）：
//   - 上游 403：镜像站限流/拒绝，原地重试同样 403；
//   - token dance 后仍 401：该镜像站对本代理不可用，dance 已带对 token 仍被拒，
//     原地重试无意义却各烧数秒。
//
// 硬失败候选跳过 url-redirect-item-error-retry 原地重试，直接切换下一候选。
func probeHardFailReason(res *probeResult) string {
	switch {
	case res.status == http.StatusForbidden:
		return "upstream-403"
	case res.status == http.StatusUnauthorized && res.danced:
		return "dance-still-401"
	}
	return ""
}

// startUpstream 单次上游尝试的公共底层（候选探测管线与慢速升级多源并发续传共用）：
// 覆盖「建请求头（白名单透传 + 可选 Range 覆盖）→ per-candidate 客户端 →
// 响应头 → 探测首块 → 401 匿名 token dance」整段，探测预算（probe-timeout）不在其中
// （仅探测管线需要，probeCandidateAttempt 在外层经 onWatchdog 联动自行包装）。
//
// 尝试级超时与客户端均取候选级生效值（cand 内字段，ExpandCandidates 已按
// 「候选显式 ?? 规则 ?? 全局」逐候选解析）：http-timeout 整次尝试总预算、
// read-timeout 无新增数据看门狗、(httpProxy, connectTimeout, insecureSkipVerify)
// 决定复用的上游 Client；单个候选可单独走指定代理/放宽预算，互不影响其他候选。
//
// 资源与超时所有权：
//   - parent 为尝试父 ctx（调用方负责与客户端断开解耦，WithoutCancel）；
//   - http-timeout（整次尝试总超时，0 = 不限制）在本函数内从 parent 派生；
//   - read-timeout 看门狗（progressWatchdog）在本函数内创建，onWatchdog 非 nil 时在
//     「请求发出前」回调（探测管线借此把预算定时器联动到 wd.cancel——到期取消在途请求，
//     且不连坐 http-timeout 分支，成功路径拆除联动后活体 body 不受影响）；
//   - 成功时 cancelTimeout 与 wd.Stop 注册进返回结果的 onClose 链（流结束/放弃时统一
//     释放）；失败时立即回收，防计时器泄漏。
//
// 返回 (结果, 错误)：res 非 nil 时资源所有权移交调用方。eff 为该请求「规则 ?? 全局」
// 合并生效配置，保留在签名中维持调用方语义（尝试级取值已全部随 cand 传入）。
func (s *Server) startUpstream(parent context.Context, r *http.Request, cand candidate, eff config.Options, rangeHeader string, onWatchdog func(*progressWatchdog)) (res *probeResult, err error) {
	urlStr := cand.url // 候选最终 URL（占位符展开后）
	// //////////////////  http-timeout（整次尝试总预算）  start  //////////////////////////////
	// 与客户端连接解耦由上层 ctx（WithoutCancel）保证；这里的总预算不能 defer cancel——
	// 活体 body 在本函数返回后仍需存活，预算取消链注册进 probeResult 由 Close 统一触发。
	// 预算取候选级生效值 cand.httpTimeout（候选显式 ?? 规则 ?? 全局，ExpandCandidates
	// 解析），单个候选可放宽/收紧总预算而不影响其他候选。
	attemptParent := parent
	var cancelTimeout context.CancelFunc
	if cand.httpTimeout > 0 {
		attemptParent, cancelTimeout = context.WithTimeout(parent, cand.httpTimeout)
	}
	wd := newProgressWatchdog(attemptParent, cand.readTimeout)
	if onWatchdog != nil {
		onWatchdog(wd)
	}
	defer func() {
		if res != nil {
			// 尝试成功：资源所有权移交 probeResult（流结束/放弃时释放）
			if cancelTimeout != nil {
				res.onClose(cancelTimeout)
			}
			res.onClose(wd.Stop)
		} else {
			// 尝试失败：立即释放，防计时器泄漏
			if cancelTimeout != nil {
				cancelTimeout()
			}
			wd.Stop()
		}
	}()
	// //////////////////  http-timeout（整次尝试总预算）  end  //////////////////////////////////

	// //////////////////  构造并发起上游请求  start  ////////////////////////////////////////////
	req, err := http.NewRequestWithContext(wd.ctx, r.Method, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("构造上游请求失败（%s）: %w", urlStr, err)
	}
	applyForwardRequestHeaders(req, r)
	// rangeHeader 非空时覆盖客户端透传的 Range（慢速升级续传按当前进度精确请求剩余区间）
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}

	// 按该候选生效的 (代理, connect-timeout, insecure) 三元组选择客户端：
	// 三维均为候选级生效值（候选显式 ?? 规则 ?? 全局，ExpandCandidates 解析）——
	// 候选显式 http-proxy（含显式空串 = 强制直连）与 connect-timeout 使单个候选可走
	// 指定代理并单独放宽建连预算；true 的 insecure = 跳过上游证书校验。
	// 同一 (代理, connect-timeout, insecure) 组合复用同一 Client，连接复用不受影响；
	// 30x 重定向跟随与 token dance 的第二次请求沿用本 cl，天然继承候选级三维配置
	//（fetchViaCandidates 的直连/fallback 路径不受影响，仍按 eff 走全局/规则值）
	cl := s.client(cand.httpProxy, cand.connectTimeout, cand.insecureSkipVerify)
	resp, err := cl.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求上游失败（%s）: %w", urlStr, wd.wrapErr(err))
	}
	wd.reset() // 响应头到达 = 一次进度（read-timeout 计时器重置）
	// //////////////////  构造并发起上游请求  end  //////////////////////////////////////////////

	// //////////////////  探测首块（判据可判定即返回）  start  ////////////////////////////////////
	res = newProbeResultFromResponse(resp, wd, urlStr)
	if res.body != nil {
		first, eof, ferr := readProbeFirst(res.body, wd)
		if ferr != nil {
			res.Close()
			res = nil // 已放弃该结果：置 nil 让出口的释放链立即回收看门狗/超时预算
			return nil, fmt.Errorf("读取上游响应失败（%s）: %w", urlStr, wd.wrapErr(ferr))
		}
		res.first, res.eof = first, eof
		if eof { // 探测期已到 EOF：body 无剩余数据，立即关闭释放连接
			_ = res.body.Close()
			res.body = nil
		}
	}
	// //////////////////  探测首块（判据可判定即返回）  end  //////////////////////////////////////

	// //////////////////  401 匿名 token dance（探测式）  start  /////////////////////////////////
	if res.status == http.StatusUnauthorized && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		resp2, consumed, ok := s.dockerAuthDanceRetry(wd, cl, req, resp, urlStr)
		if ok {
			res.Close() // 首 401 响应已被 dance 排干并关闭，释放其资源引用后改用重试响应
			res = newProbeResultFromResponse(resp2, wd, urlStr)
			res.danced = true
			if res.body != nil {
				first, eof, ferr := readProbeFirst(res.body, wd)
				if ferr != nil {
					res.Close()
					res = nil // 已放弃该结果：置 nil 让出口的释放链立即回收看门狗/超时预算
					return nil, fmt.Errorf("读取上游响应失败（%s）: %w", urlStr, wd.wrapErr(ferr))
				}
				res.first, res.eof = first, eof
				if eof {
					_ = res.body.Close()
					res.body = nil
				}
			}
		} else if consumed {
			// dance 已启动但未拿到重试响应（取 token 失败/重试请求失败）：
			// 首 401 响应已被排干关闭，按原始 401 探测结果继续（判据必拒、软失败）
			res.body = nil
			res.eof = true
		}
		// consumed=false：401 无可解析 Bearer 挑战，未 dance，探测结果原样保留
	}
	// //////////////////  401 匿名 token dance（探测式）  end  ///////////////////////////////////
	return res, nil
}

// probeCandidateAttempt 向 urlStr 发起一次探测式上游尝试（候选管线共用，方法沿用入站
// GET/HEAD）。返回 (结果, 错误, 是否硬失败, 硬失败原因)。硬失败（hard=true）：
// probe-timeout 探测预算耗尽 / 上游 403 / token dance 后仍 401——候选层应跳过
// url-redirect-item-error-retry 原地重试直接切换下一候选；其余失败（网络错误、
// 超时、判据不过）为软失败，按既有 item 重试语义原地重试。
//
// 尝试本体（请求头/token dance/看门狗/read-timeout/http-timeout/候选级 (代理,
// connect-timeout, insecure) 客户端）全部复用 startUpstream 公共底层；本函数只负责
// 探测预算（probe-timeout）的包装，预算同样取候选级生效值 cand.probeTimeout
// （候选显式 ?? 规则 ?? 全局，ExpandCandidates 解析——走慢代理的候选可单独放宽
// 探测预算，其他候选保持快速失败节奏）：
//   - probe-timeout：从发出请求到「响应头可用（含 token dance 的第二次响应）且成功
//     判据可判定（首块读出或 EOF）」的时限，覆盖建连后的等待与 token dance 全程；
//     到期经 AfterFunc 联动取消看门狗 ctx 使在途请求立即失败，硬失败快切下一候选；
//     探测结束（成败皆然）即拆链停表——探测通过后预算解除，绝不影响后续 body 读取；
//     0 = 不限制。预算 ctx 与 http-timeout 分支平级（联动只取消看门狗），
//     成功路径拆除联动并回收预算 ctx 后，活体 body 的读取不受任何影响。
func (s *Server) probeCandidateAttempt(parent context.Context, r *http.Request, cand candidate, eff config.Options) (res *probeResult, err error, hard bool, hardReason string) {
	// //////////////////  probe-timeout（探测预算，到期硬失败快切）  start  /////////////////////
	var (
		probeCtx    context.Context
		cancelProbe context.CancelFunc
		probeTimer  *time.Timer
		stopLink    func() bool
		probeFired  atomic.Bool // 定时器是否已触发（区分探测预算到期与 http-timeout 到期）
	)
	if cand.probeTimeout > 0 {
		probeCtx, cancelProbe = context.WithCancel(parent)
		probeTimer = time.AfterFunc(cand.probeTimeout, func() {
			probeFired.Store(true)
			cancelProbe()
		})
		// 探测阶段结束（成败皆然）即拆链停表：成功时不再约束后续 body 读取，
		// 失败时也不留悬挂定时器
		defer func() {
			if probeTimer.Stop() && stopLink != nil {
				stopLink() // 定时器未触发：解除与看门狗的联动后再回收探测 ctx
			}
			cancelProbe()
		}()
	}
	probeExpired := func() bool { return probeFired.Load() }
	// onWatchdog 在 startUpstream 创建看门狗后（请求发出前）被回调：
	// 预算到期 → 取消看门狗 ctx（本次尝试的所有在途读写随之失败）
	onWatchdog := func(wd *progressWatchdog) {
		if cand.probeTimeout > 0 {
			stopLink = context.AfterFunc(probeCtx, wd.cancel)
		}
	}
	// //////////////////  probe-timeout（探测预算，到期硬失败快切）  end  ///////////////////////

	res, err = s.startUpstream(parent, r, cand, eff, "", onWatchdog)
	if err != nil {
		if probeExpired() {
			// 探测预算到期：硬失败快切下一候选（跳过 item 原地重试）
			return nil, fmt.Errorf("probe-timeout（候选探测预算 %s 内响应头/首块未就绪）: %w",
				cand.probeTimeout, err), true, "probe-timeout"
		}
		return nil, err, false, ""
	}
	if probeExpired() {
		// 探测预算在探测阶段结束前到期（竞态窗口内触发）：本次尝试按硬失败快切处理，
		// 绝不把「ctx 已被预算取消」的活体 body 交给调用方 commit（否则 commit 后
		// 首次读 body 就会失败，产生半截响应）
		res.Close()
		res = nil // 已放弃该结果：置 nil 让出口的释放链立即回收看门狗/超时预算
		return nil, fmt.Errorf("probe-timeout（候选探测预算 %s 内响应头/判据未就绪）",
			cand.probeTimeout), true, "probe-timeout"
	}
	return res, nil, false, ""
}

// drainProbe 把探测结果排干为完整缓存条目（HEAD / 完整 URL 缓冲回放路径使用；
// 语义与旧版 doRequest 的「整包读进内存」一致，仅探测阶段前置）。返回前后都会
// Close 探测结果（缓冲路径不再需要活体 body）。读 body 阶段触发 read-timeout 时
// 错误经 wd.wrapErr 标注（含 "read-timeout" 字样，便于日志辨认）并留 Warn 日志。
func (s *Server) drainProbe(res *probeResult) (*cache.Entry, error) {
	defer res.Close()
	var rest []byte
	if res.body != nil {
		b, err := io.ReadAll(&progressReader{r: res.body, wd: res.wd})
		if err != nil {
			err = fmt.Errorf("读取上游响应失败（%s）: %w", res.url, res.wd.wrapErr(err))
			s.log.Warn("选定候选后读取响应体失败", "candidate", truncate(res.url, 300),
				"status", res.status, "err", err)
			return nil, err
		}
		rest = b
	}
	body := make([]byte, 0, len(res.first)+len(rest))
	body = append(body, res.first...)
	body = append(body, rest...)
	return &cache.Entry{
		Status:   res.status,
		Header:   res.header,
		Body:     body,
		StoredAt: time.Now(),
	}, nil
}
