// stream.go —— GET 路径目标（docker registry 回源）缓存 MISS / BYPASS 的流式透传：
// 候选竞速（selectCandidate）选定候选后立即向客户端 WriteHeader 并透传响应头
//（commit），把已缓冲首块写给客户端，再边读上游边同时写客户端与内存累积缓冲；
// EOF 后按既有条件（仅 200 且过判据）用 cache.Set 原子写缓存，并把完整 Entry
// 返回给单飞等待方。
//
// 连续性保证：一旦 commit（写出响应头）就绝不再切换候选或改变状态码——所有候选
// 切换/重试/竞速都发生在 selectCandidate 内部（commit 之前），commit 前的任何失败
// 都不会给客户端输出哪怕一个字节（失败时由调用方 writeError 502）。
//
// 目的：docker 客户端等响应头约 30s 就超时断开（net/http: timeout awaiting response
// headers）。旧版「整包读进内存构造 Entry 再 WriteHeader」让首字节时间 = 候选轮询
// 全部耗时 + 上游完整下载时间，必然超时；流式透传把首字节时间压缩为「选定候选」耗时。

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
//  3. 边读上游边写客户端与内存累积缓冲；客户端写失败（docker 已断开）保持既有行为——
//     继续读完上游只填缓存（broken pipe 后仍「写缓存成功」，docker 重试该层时可 HIT）；
//  4. EOF：按 finalize 回调（既有条件：仅 200 且过判据）用 cache.Set 原子写缓存，
//     并把完整 Entry 返回给单飞等待方；
//  5. commit 后上游读取失败：记日志、终止本次响应、不写缓存（半截数据绝不入库），
//     返回错误给单飞等待方（502）。
//
// cacheStatus 为 commit 时 X-Cache 观测头的取值（MISS / BYPASS）；finalize 为 EOF 后的
// 缓存写入回调（nil = 不写缓存，BYPASS 场景）。返回值 committed=true 表示已向客户端
// 写出响应头——此时调用方不得再 writeError（响应头已固化，写错误头只会污染日志）。
func (s *Server) fetchViaCandidatesStreaming(w http.ResponseWriter, r *http.Request, target string, eff config.Options, cacheStatus string, finalize func(*cache.Entry), start time.Time) (entry *cache.Entry, committed bool, err error) {
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target))
	if len(candidates) == 0 {
		// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，流式路径绝不直连
		return nil, false, errors.New("路径目标请求未配置 url-redirect，无法回源（路径目标必须由命中的域名规则提供 url-redirect）")
	}

	// //////////////////  竞速选定候选（commit 前，可自由切换）  start  /////////////////////////
	res, serr := s.selectCandidate(r, candidates, eff)
	if serr != nil {
		// 路径目标没有完整原始 URL 可直连：fallback-direct 不适用，候选全败直接报错
		//（错误语义与旧版 fetchViaCandidates 的路径目标分支一致）
		s.log.Warn("url-redirect 全部候选失败，路径目标无直连回退（fallback-direct 不适用）",
			"target", truncate(target, 300), "last_error", serr)
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
		"rule", eff.Rule, "candidate", truncate(source, 300), "status", res.status,
		"content_length", res.header.Get("Content-Length"), "check", eff.SuccessCheck)

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

	// //////////////////  边读上游边写客户端与累积缓冲  start  ///////////////////////////////////
	clientOK := true // 客户端写失败（docker 已断开）后置 false：继续读完上游只填缓存
	accum := bytes.NewBuffer(make([]byte, 0, len(res.first)+streamChunkSize))
	total := 0
	writeFirst := func() {
		if len(res.first) == 0 {
			return
		}
		total += len(res.first)
		accum.Write(res.first)
		if clientOK {
			if _, werr := w.Write(res.first); werr != nil {
				clientOK = false
				s.log.Warn("写响应体失败", "target", truncate(r.URL.Path, 200), "err", werr)
			}
		}
	}
	writeFirst()

	if res.body != nil {
		// 上游读经 progressReader 记入看门狗：read-timeout（无新增数据）语义与旧版一致，
		// 慢速但持续传输的大 blob 不会被误砍；客户端写阻塞（接收慢）时读循环随之放缓，
		// 窗口内只要上游仍有新数据到达就不会触发 read-timeout
		pr := &progressReader{r: res.body, wd: res.wd}
		buf := make([]byte, streamChunkSize)
		for {
			n, rerr := pr.Read(buf)
			if n > 0 {
				total += n
				accum.Write(buf[:n])
				if clientOK {
					if _, werr := w.Write(buf[:n]); werr != nil {
						clientOK = false
						// 与 serveEntry 既有日志文案一致（broken pipe 可辨认）；
						// 保持既有行为：继续读完上游只填缓存
						s.log.Warn("写响应体失败（客户端已断开，继续读上游仅填缓存）",
							"target", truncate(r.URL.Path, 200), "err", werr)
					}
				}
			}
			// 流式透传进度日志：按 progress-interval 周期留痕回源进度（类似 docker pull 的
			// xxMB/342.7MB 进度感），间隔内整条流已完成则一条都不出（最终日志已有成败留痕）。
			// 用循环内时间检查而非定时器 goroutine 实现：上游停滞无新数据时读循环阻塞、
			// 自然不输出进度（属预期，间隔内的沉默即「无进度」本身）
			if progressInterval > 0 && time.Since(lastProgressAt) >= progressInterval {
				now := time.Now()
				clientState := "ok"
				if !clientOK {
					clientState = "disconnected"
				}
				args := []any{
					"rule", eff.Rule, "candidate", truncate(source, 300), "status", res.status,
					"bytes", total,
				}
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
				s.log.Info("流式透传进度", args...)
				lastProgressAt = now
				lastProgressBytes = total
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					break // 完整读完：半截数据不存在，可以入库
				}
				// commit 后上游读取失败：终止本次响应、不写缓存（半截数据绝不入库）
				res.Close()
				s.log.Error("流式回源读上游失败，终止本次响应且不写缓存（半截数据绝不入库）",
					"rule", eff.Rule, "candidate", truncate(source, 300),
					"status", res.status, "bytes", total, "err", rerr)
				return nil, true, fmt.Errorf("流式回源读上游失败（%s）: %w", source, rerr)
			}
		}
	}
	res.Close()
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
	switch {
	case res.status == http.StatusPartialContent:
		s.log.Info("成功返回内容（Range 206 流式透传，不写缓存）",
			"rule", eff.Rule, "upstream", source, "status", res.status,
			"bytes", total, "cache", cacheStatus)
	case res.status == http.StatusNotModified:
		s.log.Info("内容未变更（304），客户端缓存副本有效",
			"rule", eff.Rule, "upstream", source, "cache", cacheStatus)
	case res.status == http.StatusOK && total > 0:
		s.log.Info("成功返回内容",
			"rule", eff.Rule, "upstream", source, "status", res.status,
			"bytes", total, "cache", cacheStatus)
	}
	s.log.Info("代理请求完成",
		"method", r.Method,
		"target", truncate(r.URL.Path, 300),
		"status", res.status,
		"cache", cacheStatus,
		"upstream", source,
		"rule", eff.Rule,
		"bytes", total,
		"cost", time.Since(start).Round(time.Millisecond).String(),
	)
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
