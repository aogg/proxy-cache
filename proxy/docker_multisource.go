// docker_multisource.go —— docker 可变目标（tag manifest / referrers / tags list）的
// 多源并发回源与合并（开关 docker-multi-source-enabled，默认开）：
//
// 背景：可变目标不缓存、每次回源（docker_mutable.go），此前走单候选顺序轮询
//（url-redirect 多轮多尝试）：先命中的源直接把响应透传给 docker。镜像站之间同步有
// 快慢，先命中的源可能 tags/list 还没刷新、manifest 还是旧 digest——docker 拿不到
// 最新 tag / 拉到旧镜像。本文件把可变目标的 BYPASS 回源改为「并发请求全部候选源 +
// 按端点类型合并/共识」：
//   - tags/list：合并各源 tags（并集去重、按字典序排序）后返回，慢源漏 tag 不再可能；
//   - referrers/<digest>：合并各源 descriptors 数组（按 mediaType+digest 去重）；
//   - manifests/<tag>（GET 与 HEAD）：收集各成功源的 Docker-Content-Digest，取多数
//     共识摘要（平票取最快成功者），用该源的 body+响应头应答。HEAD 与 GET 同策略，
//     保证 docker pull 流程内 HEAD/GET 判定一致。
//
// 一致性推理（为什么 manifest 只做摘要共识、无需再做源 pin）：tags 与 manifest 由
// 同一 push 事务产生，多数源给出的 Docker-Content-Digest 即最新内容寻址结果；blob
// 按 digest 寻址、内容不可变——共识摘要确定后，任意源取同 digest 的 manifest 与
// blob 内容都一致，不存在「后续请求漂移到旧源」的问题，故无需把共识源固定下来
//（README「docker 可变目标多源并发回源与合并」同款说明）。
//
// 合并/共识之外的边界（全部刻意收窄，避免语义漂移）：
//   - 仅 bypassReason=="docker-mutable" 的 GET/HEAD 启用多源；其余 BYPASS
//    （request-range / request-no-cache / cache-disabled / negative-ttl 等）维持
//     现有顺序管线不变；
//   - tags/list 带分页参数（?n= / ?last=）时不做多源合并：跨源页码语义不一致
//    （各源 tag 集合不同，同一 (n,last) 指向不同窗口），合并会产生错页/丢页，
//     退回现有顺序管线；
//   - 候选数 <2（单候选）无合并/共识意义，维持顺序管线；
//   - 成功判据复用现有判据（probePassed）：HTML 反爬页等未过判据的源被拒之于合并
//     之外（html-error-page 同款判定）；tags/referrers 合并还要求源响应为 200 且
//     body 可解析为目标端点 JSON，解析失败视同该源失败；
//   - 所有源都失败 → 退回现有顺序管线（继承其 4xx 透传 / 502 语义，如 referrers
//     404 透传）；仅 1 源成功 → 直接用该源（等价旧结果但并发下更快）；
//   - 客户端断开：r.Context() 取消全部在途上游（断开即停，可变目标不写缓存、没有
//     「继续读完填缓存」的价值可挽救，与顺序管线的 WithoutCancel 语义刻意不同），
//     错误按既有 errClientDisconnected 语义低调处理（不 502 不刷日志）；
//   - 并发上限 dockerMultiSourceMaxConcurrency（8），超出的候选排队放行；每候选
//     超时复用现有候选级配置（probe-timeout / http-timeout / read-timeout /
//     connect-timeout，经 probeCandidateAttempt 全套生效）；
//   - GET 可变目标请求带 Range 时（可变判定在 bypass 分流中先于 Range）同样进入
//     多源路径：tags/referrers 合并要求 200，206 全部判为不可合并、退回顺序管线
//     维持既有 206 透传；manifest 共识语义不受 Range 影响。
//
// 应答形态：合并/共识必须收齐各源完整响应，无法边收边发，故本路径为缓冲应答
//（经 serveEntry 回放，X-Cache: BYPASS；GET/HEAD 同一入口，HEAD 只回放响应头）。
// 可变目标（manifest JSON / tags 列表 / referrers 列表）响应体量级小，可接受。
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// dockerMultiSourceMaxConcurrency docker 可变目标多源并发回源的并发上限：
// 候选全并发但超出的排队放行，防止候选列表过长时瞬间打满上游连接与内存
//（每个在途源缓冲一份完整响应）。
const dockerMultiSourceMaxConcurrency = 8

// dockerMutableKind docker 可变目标的多源处理类型（dockerMultiSourceKind 的解析结果）。
type dockerMutableKind string

const (
	// dockerMutableKindTags tags/list：多源 tags 并集去重、字典序排序。
	dockerMutableKindTags dockerMutableKind = "tags"
	// dockerMutableKindReferrers referrers/<digest>：descriptors 按 mediaType+digest 去重合并。
	dockerMutableKindReferrers dockerMutableKind = "referrers"
	// dockerMutableKindManifest manifests/<tag>：Docker-Content-Digest 多数共识。
	dockerMutableKindManifest dockerMutableKind = "manifest"
)

// dockerMultiSourceResult 一个候选源在多源并发回源中的最终结果。
type dockerMultiSourceResult struct {
	// url 候选最终 URL（观测与日志）。
	url string
	// entry 成功源完整响应（判据通过且可参与合并/共识；nil = 失败源）。
	entry *cache.Entry
	// err 失败原因（entry == nil 时非 nil；供 Debug 留痕与全败诊断）。
	err error
	// seq 完成序号（全局递增）：成功源按它升序排列，共识平票时「最快成功者」即 seq 最小者。
	seq int
}

// dockerMultiSourceSuccess 进入合并/共识的有效源（判据通过；合并类端点还要求
// 200 且 body 可解析，见各 merge 函数）。
type dockerMultiSourceSuccess struct {
	// url 候选最终 URL（响应头 X-Proxy-Upstream 与汇总日志的提供源）。
	url string
	// entry 该源完整响应（GET 含完整 body；HEAD 天然无 body）。
	entry *cache.Entry
	// seq 完成序号（最快成功者 = seq 最小）。
	seq int
}

// dockerMultiSourceKind 解析路径目标的多源处理类型（仅可变目标；返回空串 = 不适用多源）：
//   - v2/<name>/tags/list 不带分页参数（?n= / ?last=）→ tags；
//   - v2/<name>/referrers/<digest> → referrers；
//   - v2/<name>/manifests/<tag> → manifest（digest 形态不可变、可缓存，到不了这里，
//     由 bypassReason=="docker-mutable" 闸门先挡）；
//   - 其余（带分页的 tags/list、非可变目标）→ 空串。
func dockerMultiSourceKind(target string) dockerMutableKind {
	// //////////////////  校验参数：拆路径与查询串  start  ///////////////////////////////////////
	path := target
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimPrefix(path, "/") // 兜底：容忍前导 /（与 dockerMutableTarget 同款宽松）
	segs := strings.Split(path, "/")
	if len(segs) < 4 || segs[0] != "v2" {
		return ""
	}
	if !dockerMutableTarget(target) {
		return "" // 仅可变目标启用多源（digest manifest / blob 等不可变目标走缓存管线）
	}
	// //////////////////  校验参数：拆路径与查询串  end  /////////////////////////////////////////

	// //////////////////  核心逻辑：按端点分类（分页 tags/list 不合并）  start  ///////////////////
	switch second := segs[len(segs)-2]; {
	case second == "tags" && segs[len(segs)-1] == "list":
		if dockerTagsPaged(target) {
			return "" // 分页不合并（跨源页码语义不一致），退回顺序管线
		}
		return dockerMutableKindTags
	case second == "referrers":
		return dockerMutableKindReferrers
	case second == "manifests":
		return dockerMutableKindManifest
	}
	return ""
	// //////////////////  核心逻辑：按端点分类（分页 tags/list 不合并）  end  /////////////////////
}

// dockerTagsPaged 判断 tags/list 目标是否带分页参数（?n= / ?last=）：
// 带分页时不做多源合并——各源 tag 集合不同，同一 (n,last) 在不同源指向不同窗口，
// 合并会产生错页/丢页，必须退回顺序管线（单源分页语义自洽）。
func dockerTagsPaged(target string) bool {
	// //////////////////  校验参数：拆查询串  start  /////////////////////////////////////////////
	i := strings.IndexByte(target, '?')
	if i < 0 {
		return false
	}
	q, err := url.ParseQuery(target[i+1:])
	if err != nil {
		return false // 查询串解析失败按无分页处理（合并仍按并集语义安全）
	}
	// //////////////////  校验参数：拆查询串  end  ///////////////////////////////////////////////
	return q.Has("n") || q.Has("last")
}

// serveDockerMultiSource docker 可变目标多源并发回源的总入口（GET BYPASS 分支与
// handleHead 可变分支各一个调用点，proxy.go 最小接入）：
//   - mutableBypass 本请求是否为 docker-mutable BYPASS（GET 传 bypassReason 判定结果，
//     HEAD 传可变目标分支标志）——false 直接放行（其余 BYPASS / MISS 维持顺序管线）；
//   - 返回 true 表示已接管本次响应（含错误出口：客户端断开低调收尾、成功应答）；
//     返回 false 表示未接管（条件不满足或全部源失败），调用方退回既有顺序候选管线
//    （继承其 4xx 透传 / 502 语义）。
func (s *Server) serveDockerMultiSource(w http.ResponseWriter, r *http.Request, target string, eff config.Options, mutableBypass bool, start time.Time) bool {
	// //////////////////  校验参数（接管条件闸门）  start  ////////////////////////////////////////
	if !mutableBypass {
		return false // 仅 docker-mutable BYPASS 启用；其余 BYPASS 维持现状
	}
	if s.cfg == nil || !s.cfg.GlobalDockerMultiSourceEnabled() {
		return false // 开关关闭：行为与旧版（顺序管线）完全一致
	}
	kind := dockerMultiSourceKind(target)
	if kind == "" {
		// 分页 tags/list 等不适用多源的目标：留 Debug 痕迹后退回顺序管线
		if dockerTagsPaged(target) {
			s.log.Debug("tags/list 带分页参数，跳过多源合并（跨源页码语义不一致，退回顺序候选管线）",
				parseDockerLogInfo(target).attrs(dockerLogSourceClient,
					"rule", eff.Rule, "reason", "paged-tags", "target", truncate(target, 300))...)
		}
		return false
	}
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	if len(candidates) < 2 {
		return false // 单候选无合并/共识意义，维持顺序管线
	}
	// //////////////////  校验参数（接管条件闸门）  end  //////////////////////////////////////////

	labels := parseDockerLogInfo(target)
	pathTarget := isPathTarget(target)
	hasRange := r.Header.Get("Range") != ""

	// //////////////////  核心逻辑：并发回源全部候选  start  //////////////////////////////////////
	sources := s.fetchMultiSourceConcurrent(r, candidates, eff, pathTarget, hasRange)
	// 客户端断开（断开即停）：r.Context() 已随源失败取消全部在途上游，低调收尾——
	// 连接已断，绝不再 502 / writeError（对端已关闭，写只会报错刷日志）；客户端将以
	// 缓存重试或重新 pull 接续
	if r.Context().Err() != nil {
		s.log.Debug("客户端已断开，docker 可变目标多源并发回源终止（断开即停，不 502）",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "target", truncate(target, 300))...)
		return true
	}
	successes := dockerMultiSourceSuccesses(sources)
	if len(successes) == 0 {
		// 全部源失败：退回既有顺序候选管线（继承其 4xx 透传 / 502 语义，如 referrers
		// 404 透传；顺序管线自己会重新探测并留 Warn/Error 级失败日志，此处仅 Debug 简记）
		s.log.Debug("docker 可变目标多源并发回源全部源失败，退回既有顺序候选管线（4xx 透传 / 502 语义不变）",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "sources", len(candidates),
				"target", truncate(target, 300))...)
		return false
	}
	// //////////////////  核心逻辑：并发回源全部候选  end  ////////////////////////////////////////

	// //////////////////  按端点类型合并/共识并应答  start  //////////////////////////////////////
	var (
		entry    *cache.Entry // 应答条目（合并重组体或共识胜出源原文）
		merged   int          // 合并条数（tags 数 / descriptors 数；manifest 共识恒 0）
		agree    int          // 共识票数（合并类端点恒 0）
		disagree int          // 分歧源数（合并类端点恒 0）
		via      string       // 实际提供内容的源 URL（观测 X-Proxy-Upstream 与日志）
		summary  string       // 汇总日志消息（按端点类型区分，便于按消息 grep）
	)
	switch kind {
	case dockerMutableKindTags:
		entry, merged, via, summary = s.mergeMultiSourceTags(successes, eff, labels)
	case dockerMutableKindReferrers:
		entry, merged, via, summary = s.mergeMultiSourceReferrers(successes, eff, labels)
	case dockerMutableKindManifest:
		entry, agree, disagree, via, summary = s.pickMultiSourceManifestConsensus(successes, eff, labels)
	}
	if entry == nil {
		// 合并类端点全部成功源 body 不可解析（判据放过了非 JSON 内容）：视同全部源失败，
		// 退回顺序管线（绝不把合并不出来的数据发给客户端）
		s.log.Debug("docker 可变目标多源合并无有效源（全部成功源 body 不可解析），退回顺序候选管线",
			labels.attrs(dockerLogSourceClient, "rule", eff.Rule, "sources", len(candidates),
				"success", len(successes), "target", truncate(target, 300))...)
		return false
	}
	entry.Source = via // serveEntry 回放时输出 X-Proxy-Upstream 观测头
	// 汇总 Info：候选数 / 成功源数 / 合并条数或共识票数与摘要，可按消息 grep 本特性全部命中
	//（merged/agree/disagree 三键在三类端点间统一输出，不适用者恒 0，便于聚合检索）
	summaryArgs := labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
		"sources", len(candidates), "success", len(successes),
		"merged", merged, "agree", agree, "disagree", disagree,
		"upstream", truncate(via, 300),
		"cost", time.Since(start).Round(time.Millisecond).String())
	if dgst := entry.Header.Get("Docker-Content-Digest"); dgst != "" {
		summaryArgs = append(summaryArgs, "digest", dgst)
	}
	s.log.Info(summary, summaryArgs...)
	// 缓冲回放（X-Cache: BYPASS）：serveEntry 兼容 GET 全量与 HEAD 仅响应头两种形态，
	// 合并/共识路径复用同一出口，响应头与观测头语义与既有 BYPASS 完全一致
	s.serveEntry(w, r, entry, "BYPASS", eff, start)
	return true
	// //////////////////  按端点类型合并/共识并应答  end  ////////////////////////////////////////
}

// fetchMultiSourceConcurrent 并发请求全部候选源（上限 dockerMultiSourceMaxConcurrency，
// 超出排队放行），返回每个候选的最终结果（下标与候选列表对齐）：
//   - 单源管线复用探测式尝试 probeCandidateAttempt（候选级 probe-timeout / http-timeout /
//     read-timeout / connect-timeout / (代理, insecure) 客户端三元组全套生效）+
//     drainProbe 排干完整响应——成功判据与顺序管线完全相同（probePassed，HTML 反爬页
//     等未过判据的源在此被拒之于合并之外）；
//   - 每源 ctx 挂在 r.Context() 下：客户端断开立即取消全部在途上游（断开即停）；
//   - 单源失败只 Debug 留痕（全败/部分失败的宏观状态由汇总 Info / 调用方顺序管线的
//     既有日志承载，避免每次回源刷一屏 Warn）。
func (s *Server) fetchMultiSourceConcurrent(r *http.Request, candidates []candidate, eff config.Options, pathTarget, hasRange bool) []dockerMultiSourceResult {
	// //////////////////  准备数据：ctx / 信号量 / 结果槽  start  /////////////////////////////////
	// 与客户端断开联动（可变目标不写缓存，没有「继续读完填缓存」的价值可挽救——
	// 与顺序管线的 WithoutCancel 语义刻意不同）
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	sem := make(chan struct{}, dockerMultiSourceMaxConcurrency)
	results := make([]dockerMultiSourceResult, len(candidates))
	var (
		seqMu sync.Mutex
		seq   int // 完成序号发生器（共识平票取最快成功者用）
	)
	var wg sync.WaitGroup
	// //////////////////  准备数据：ctx / 信号量 / 结果槽  end  ///////////////////////////////////

	// //////////////////  核心逻辑：逐候选并发探测+排干  start  ///////////////////////////////////
	for i := range candidates {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			cand := candidates[slot]
			// goroutine 边界无法把 panic 交还上层单飞的 recover（不处理会让整个进程
			// 崩溃）：按该源失败收场并留 Error 日志，把影响隔离到单个请求
			res, err, hard, hardReason := func() (res *probeResult, err error, hard bool, hardReason string) {
				defer func() {
					if rec := recover(); rec != nil {
						s.log.Error("docker 多源并发回源候选尝试 panic，已按该源失败处理",
							"candidate", truncate(cand.url, 300), "panic", rec)
						res, err, hard, hardReason = nil, fmt.Errorf("候选尝试 panic: %v", rec), true, "panic"
					}
				}()
				return s.probeCandidateAttempt(ctx, r, cand, eff)
			}()
			if err != nil {
				if ctx.Err() != nil {
					// 客户端断开导致的取消：降为 Debug，避免刷日志
					s.log.Debug("docker 多源并发回源候选已取消（客户端断开）",
						"candidate", truncate(cand.url, 300))
					results[slot] = dockerMultiSourceResult{url: cand.url, err: ctx.Err()}
					return
				}
				s.log.Debug("docker 多源并发回源候选源失败",
					"candidate", truncate(cand.url, 300),
					"hard_fail", hardFailAttr(hard, hardReason), "err", err)
				results[slot] = dockerMultiSourceResult{url: cand.url, err: err}
				return
			}
			// 成功判据复用现有判定（probePassed）：HTML 反爬页等未过判据的源在此被拒
			if !probePassed(res, eff, r.Method, hasRange, pathTarget) {
				hr := probeHardFailReason(res, pathTarget)
				res.Close()
				s.log.Debug("docker 多源并发回源候选未通过成功判据（拒之于合并之外）",
					"candidate", truncate(cand.url, 300), "check", eff.SuccessCheck,
					"status", res.status, "hard_fail", hardFailAttr(hr != "", hr))
				results[slot] = dockerMultiSourceResult{url: cand.url,
					err: fmt.Errorf("候选 %s 未通过 success-check=%s（状态码 %d）", cand.url, eff.SuccessCheck, res.status)}
				return
			}
			// 排干为完整 Entry（tags/referrers/manifest 响应体量级小，缓冲可接受；
			// 合并/共识语义必须收齐完整响应，无法流式）
			e, derr := s.drainProbe(res)
			if derr != nil {
				s.log.Debug("docker 多源并发回源候选读取响应体失败",
					"candidate", truncate(cand.url, 300), "status", res.status, "err", derr)
				results[slot] = dockerMultiSourceResult{url: cand.url, err: derr}
				return
			}
			seqMu.Lock()
			seq++
			mySeq := seq
			seqMu.Unlock()
			results[slot] = dockerMultiSourceResult{url: cand.url, entry: e, seq: mySeq}
		}(i)
	}
	wg.Wait()
	// //////////////////  核心逻辑：逐候选并发探测+排干  end  /////////////////////////////////////
	return results
}

// hardFailAttr 硬失败原因的日志取值（非硬失败输出空串，保持键存在便于统一 grep）。
func hardFailAttr(hard bool, reason string) string {
	if !hard {
		return ""
	}
	return reason
}

// dockerMultiSourceSuccesses 提取成功源并按完成顺序（seq 升序）排列：
// 完成顺序即「最快优先」，共识平票时直接取首个满足最高票数的源。
func dockerMultiSourceSuccesses(results []dockerMultiSourceResult) []dockerMultiSourceSuccess {
	var out []dockerMultiSourceSuccess
	for _, res := range results {
		if res.entry == nil {
			continue
		}
		out = append(out, dockerMultiSourceSuccess{url: res.url, entry: res.entry, seq: res.seq})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// mergeMultiSourceTags tags/list 多源合并：各成功源（200 且 body 可解析为含 tags 数组的
// JSON 对象）的 tags 取并集、去重、按字典序排序；顶层其余字段（name 等）取最快成功源
// 原文，仅替换 tags；响应头取最快成功源（Content-Length 剔除，交由 serveEntry 按重组后
// body 重设）。返回 (合并条目, 合并 tags 条数, 提供源 URL, 汇总日志消息)；entry 为 nil =
// 无有效源（调用方退回顺序管线）。
func (s *Server) mergeMultiSourceTags(successes []dockerMultiSourceSuccess, eff config.Options, labels dockerLogInfo) (*cache.Entry, int, string, string) {
	// //////////////////  准备数据：筛出可合并源与并集  start  ///////////////////////////////////
	base := (*dockerMultiSourceSuccess)(nil)
	var envelope map[string]json.RawMessage
	union := map[string]struct{}{}
	for i := range successes {
		suc := &successes[i]
		if suc.entry.Status != http.StatusOK {
			continue // 合并要求 200（206/304 等无法安全并集）
		}
		var env map[string]json.RawMessage
		if err := json.Unmarshal(suc.entry.Body, &env); err != nil || env == nil {
			s.log.Debug("docker 多源合并：tags/list body 非 JSON 对象，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300), "body_bytes", len(suc.entry.Body))...)
			continue
		}
		rawTags, ok := env["tags"]
		if !ok {
			s.log.Debug("docker 多源合并：tags/list body 无 tags 字段，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300))...)
			continue
		}
		var tags []string
		if err := json.Unmarshal(rawTags, &tags); err != nil {
			s.log.Debug("docker 多源合并：tags/list tags 字段非字符串数组，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300))...)
			continue
		}
		if base == nil {
			base = suc // 最快成功源：envelope（name 等）与响应头基线
			envelope = env
		}
		for _, t := range tags {
			union[t] = struct{}{} // 并集去重
		}
	}
	if base == nil {
		return nil, 0, "", ""
	}
	// //////////////////  准备数据：筛出可合并源与并集  end  /////////////////////////////////////

	// //////////////////  核心逻辑：并集排序 + 重组 body  start  //////////////////////////////////
	merged := make([]string, 0, len(union))
	for t := range union {
		merged = append(merged, t)
	}
	sort.Strings(merged) // 字典序排序（跨源集合稳定输出）
	tagsRaw, err := json.Marshal(merged)
	if err != nil {
		return nil, 0, "", "" // 理论不可达（[]string 恒可序列化）；防御同全败
	}
	envelope["tags"] = tagsRaw
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, 0, "", ""
	}
	// //////////////////  核心逻辑：并集排序 + 重组 body  end  ////////////////////////////////////

	// //////////////////  返回整理：响应头取最快成功源  start  ////////////////////////////////////
	entry := &cache.Entry{
		Status:   http.StatusOK,
		Header:   clonedHeaderWithoutContentLength(base.entry.Header),
		Body:     body,
		StoredAt: time.Now(),
	}
	return entry, len(merged), base.url, "docker 可变目标多源并发回源完成（tags/list 多源合并返回）"
	// //////////////////  返回整理：响应头取最快成功源  end  //////////////////////////////////////
}

// mergeMultiSourceReferrers referrers/<digest> 多源合并：各成功源（200 且 body 可解析为
// 含 manifests 数组的 JSON 对象）的 descriptors 按 mediaType+digest 去重合并（保留首次
// 出现顺序与描述符原文未知字段）；顶层其余字段（schemaVersion / mediaType / annotations
// 等）取最快成功源原文，仅替换 manifests。返回值语义同 mergeMultiSourceTags。
func (s *Server) mergeMultiSourceReferrers(successes []dockerMultiSourceSuccess, eff config.Options, labels dockerLogInfo) (*cache.Entry, int, string, string) {
	// //////////////////  准备数据：筛出可合并源与描述符去重表  start  ///////////////////////////
	base := (*dockerMultiSourceSuccess)(nil)
	var envelope map[string]json.RawMessage
	seen := map[string]struct{}{}
	var mergedRaw []json.RawMessage
	for i := range successes {
		suc := &successes[i]
		if suc.entry.Status != http.StatusOK {
			continue // 合并要求 200
		}
		var env map[string]json.RawMessage
		if err := json.Unmarshal(suc.entry.Body, &env); err != nil || env == nil {
			s.log.Debug("docker 多源合并：referrers body 非 JSON 对象，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300), "body_bytes", len(suc.entry.Body))...)
			continue
		}
		rawManifests, ok := env["manifests"]
		if !ok {
			s.log.Debug("docker 多源合并：referrers body 无 manifests 字段，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300))...)
			continue
		}
		var descs []json.RawMessage
		if err := json.Unmarshal(rawManifests, &descs); err != nil {
			s.log.Debug("docker 多源合并：referrers manifests 字段非对象数组，该源拒之于合并之外",
				labels.attrs(dockerLogSourceClient, "rule", eff.Rule,
					"upstream", truncate(suc.url, 300))...)
			continue
		}
		if base == nil {
			base = suc // 最快成功源：envelope（schemaVersion/mediaType 等）与响应头基线
			envelope = env
		}
		for _, raw := range descs {
			var desc struct {
				MediaType string `json:"mediaType"`
				Digest    string `json:"digest"`
			}
			if err := json.Unmarshal(raw, &desc); err != nil || desc.Digest == "" {
				continue // 无法按 mediaType+digest 识别的描述符不参与合并
			}
			key := desc.MediaType + "\x00" + desc.Digest
			if _, dup := seen[key]; dup {
				continue // 按 mediaType+digest 去重
			}
			seen[key] = struct{}{}
			mergedRaw = append(mergedRaw, raw) // 保留首次出现顺序与描述符原文
		}
	}
	if base == nil {
		return nil, 0, "", ""
	}
	// //////////////////  准备数据：筛出可合并源与描述符去重表  end  /////////////////////////////

	// //////////////////  核心逻辑：替换 manifests 并重组 body  start  ///////////////////////////
	if mergedRaw == nil {
		mergedRaw = []json.RawMessage{}
	}
	manifestsRaw, err := json.Marshal(mergedRaw)
	if err != nil {
		return nil, 0, "", "" // 理论不可达；防御同全败
	}
	envelope["manifests"] = manifestsRaw
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, 0, "", ""
	}
	// //////////////////  核心逻辑：替换 manifests 并重组 body  end  /////////////////////////////

	// //////////////////  返回整理：响应头取最快成功源  start  ////////////////////////////////////
	entry := &cache.Entry{
		Status:   http.StatusOK,
		Header:   clonedHeaderWithoutContentLength(base.entry.Header),
		Body:     body,
		StoredAt: time.Now(),
	}
	return entry, len(mergedRaw), base.url, "docker 可变目标多源并发回源完成（referrers 多源合并返回）"
	// //////////////////  返回整理：响应头取最快成功源  end  //////////////////////////////////////
}

// pickMultiSourceManifestConsensus manifests/<tag> 多源摘要共识：收集各成功源响应头的
// Docker-Content-Digest 按票数取多数；平票取最快成功者（成功源按完成顺序排列，首个
// 达到最高票数的摘要胜出）；全部成功源都无摘要头时退化为直接用最快成功源（等价旧
// 结果）。应答整体使用胜出源的 body+响应头（ETag / Content-Type / Docker-Content-Digest
// 与 body 同源一致，HEAD/GET 判定一致）。
// 一致性推理：blob 按 digest 寻址、内容不可变，共识摘要确定后任意源的同 digest 内容
// 一致，无需再做源 pin（本文件头与 README 同款说明）。
// 返回 (胜出条目, 共识票数, 分歧源数, 胜出源 URL, 汇总日志消息)。
func (s *Server) pickMultiSourceManifestConsensus(successes []dockerMultiSourceSuccess, eff config.Options, labels dockerLogInfo) (*cache.Entry, int, int, string, string) {
	// //////////////////  核心逻辑：按完成顺序计票，平票取最快  start  ///////////////////////////
	votes := map[string]int{}
	bestDigest := ""
	bestCount := 0
	for i := range successes { // 成功源已按完成顺序（最快优先）排列
		dgst := successes[i].entry.Header.Get("Docker-Content-Digest")
		if dgst == "" {
			continue // 无摘要头（非 registry 语义响应）不参与投票
		}
		votes[dgst]++
		if votes[dgst] > bestCount {
			bestDigest, bestCount = dgst, votes[dgst]
		}
	}
	// //////////////////  核心逻辑：按完成顺序计票，平票取最快  end  /////////////////////////////

	// //////////////////  返回整理：胜出源与分歧统计  start  /////////////////////////////////////
	chosen := &successes[0] // 全部无摘要：退化为最快成功源（等价旧结果）
	if bestCount > 0 {
		for i := range successes {
			if successes[i].entry.Header.Get("Docker-Content-Digest") == bestDigest {
				chosen = &successes[i] // 首个持有共识摘要的源（完成顺序，即最快胜出者）
				break
			}
		}
	}
	disagree := 0
	for i := range successes {
		if dgst := successes[i].entry.Header.Get("Docker-Content-Digest"); dgst != "" && dgst != bestDigest {
			disagree++ // 持不同摘要的成功源数（无摘要头不计入）
		}
	}
	// //////////////////  返回整理：胜出源与分歧统计  end  ///////////////////////////////////////
	return chosen.entry, bestCount, disagree, chosen.url,
		"docker 可变目标多源并发回源完成（manifest 摘要共识返回）"
}

// clonedHeaderWithoutContentLength 深拷贝响应头并剔除 Content-Length（合并重组类条目
// 专用）：body 已重组，旧声明长度必然失配，交由 serveEntry 按新 body 重设。
func clonedHeaderWithoutContentLength(src http.Header) http.Header {
	hdr := make(http.Header, len(src))
	for k, vv := range src {
		hdr[k] = append([]string(nil), vv...)
	}
	hdr.Del("Content-Length")
	return hdr
}
