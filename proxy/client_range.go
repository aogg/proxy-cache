// client_range.go —— docker blob 的客户端 Range 语义（起点 0 = 完整 GET；任意起点 = 本地切片 / 半截对齐）：
//
// 现场故障（已确证）：docker/containerd 拉 blob 时**无论从哪个偏移开始都带 Range**
// （httpReadSeeker）：首次是 `Range: bytes=0-`，断点续传重试则是 `Range: bytes=<已有偏移>-`
// ——现场 content store 里该 layer 已落 311MB，重试请求对客户端回的是
// `206 content_length=273013970`（blob 总长 355851474）⇒ 客户端请求的起点 ≈ 82.8MB。
// 旧版把所有带 Range 的请求一律判 BYPASS（见 handleGet 的 request-range 分支），于是
// docker 的每一次尝试都绕过缓存、绕过 blob 半截续传、绕开预取搭车，纯透传给上游；而
// 「客户端 → 本服务 → 上游」这条链路会在 21~45 秒后中途断（实测 2.7MB/s 与 874KB/s
// 两种速率下都断），302MB 的单层必然下不完——断了只能从 0 重来，重试又带 Range 再
// BYPASS，最终以 502 收场。起点 >0 的重试更糟：它本来就是「从已有断点继续」，却因为
// 本地半截前缀（本服务已经下到的部分，现场 311MB 中的 65.7MB）不被复用，每次都要重新
// 向上游要一遍（现场上游 13KB/s）、慢而不死，且每轮重试都要再烧一遍候选探测。
//
// 本文件只做两件事（都仅对「能解析成 /v2/<name>/blobs/<digest> 的 docker blob 目标」生效）：
//
//  1. 解析层（parseBlobRange / parseZeroRangeBlob）：把客户端单区间 bytes Range 解析成
//     blobRangeSpec{start, hasEnd, end}。起点 0 即「等价完整 GET」，起点 >0 即「真正的
//     断点续传」，两种都由本服务接管（安全前提：blob 按 digest 寻址、内容不可变）。
//
//  2. 命中层：
//     - **缓存 HIT + 任意 Range** → serveEntryBlobRange 直接从缓存条目 body 切片回 206
//     （Content-Range / Accept-Ranges / Content-Length 按 RFC 9110 算，起点越界回 416 +
//     `Content-Range: bytes */<total>`），**完全不发上游**——这是「一次落盘、后续任意
//     起点全秒回」的落点，也是 docker 反复重试同一 layer 时不再打镜像站的保证；
//     - **缓存 MISS + 起点 0** → 请求侧在 handleGet 入口剥离 Range 后按完整 GET 处理
//     （缓存 HIT / 半截续传 / 预取搭车 / 单飞合并全部照常；Range 绝不透传给上游，否则
//     上游回 206，成功判据、缓存写入、半截续传会全部失配），响应侧由 zeroRangeWriter
//     把内部 200 就地改写成对客户端的 206 + Content-Range；
//     - **缓存 MISS + 起点 >0** → 半截前缀对齐交给既有续传管线（partial_resume.go 的
//     tryServeBlobResume / resumeBlobStreaming，**不新造下载器**）：起点 == 半截长度时
//     直接接上游剩余区间；起点 < 半截长度时从半截的起点位置回放 + 继续向上游续传拼接。
//     两种情况内部都按完整 blob 回放/续传并照常写缓存，对客户端只发区间内字节（206）。
//     半截不存在、或起点 > 半截长度（中间缺一段，不能凭空补）时维持旧版 206 透传，
//     后者另打一条 Warn 说明缺口大小（运维定位用）。
//
// 刻意不接管的边界（保持旧版行为，避免任何语义风险）：
//   - 非 docker blob 目标（manifest / 其它路径目标 / 完整 URL（web）目标）：带 Range 仍按
//     旧版 BYPASS 透传（manifest 内容会变，缓存复用与半截拼接都不适用）；
//   - 后缀区间（`bytes=-N`）、多区间（含多个 Range 头）、非 bytes 单位、语法不合法、
//     终点小于起点：真正的局部读取/无效语义，交给上游决定如何回应（旧版 206 透传）；
//   - 客户端 Cache-Control: no-cache（显式要求强制回源）：不拿本地缓存/半截满足它。
//
// 总长未知（上游没声明 Content-Length、走 chunked）时的降级：无法构造合法的
// Content-Range——起点 0 的开区间退回 200 全量响应（`bytes=0-` 与完整 GET 语义等价，
// offset=0 的客户端按 200 处理完全正确），起点 0 的带终点区间按 RFC 7233 用总长未知写法
// `bytes 0-<end>/*`；起点 >0 的续传路径在 commit 前发现总长未知则不 commit，由调用方
// 走旧版 206 透传（绝不把「半截前缀长度」当总长谎报给客户端）。
package proxy

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// blobRangeSpec 客户端单区间 bytes Range 的解析结果（仅 docker blob 目标会被本服务接管）。
type blobRangeSpec struct {
	// start 区间起点（>=0）：0 = 等价完整 GET；>0 = 真正的断点续传（从该字节开始要）。
	start int64
	// hasEnd 是否带区间终点（`bytes=<start>-<end>` 形态）；false = 开区间 `bytes=<start>-`
	//（docker/containerd 实际发的都是开区间）。
	hasEnd bool
	// end 区间终点（含），仅 hasEnd=true 时有效；可能大于总长-1，由回放侧按 RFC 收窄。
	end int64
}

// blobRangeLast 按对象总长收窄区间终点（RFC 9110：last-byte-pos 不小于当前长度时按
// 总长-1 解释）。调用方须先确认 spec.start < total（起点越界是 416 语义，不是收窄）。
func blobRangeLast(spec blobRangeSpec, total int64) int64 {
	last := total - 1
	if spec.hasEnd && spec.end < last {
		last = spec.end
	}
	return last
}

// parseBlobRange 解析「docker blob 目标 + 单区间 bytes Range」，返回 (解析结果, 是否接管)。
// target 为 ExtractTarget 的路径目标原文（不带 scheme 与前导 /，可能带查询串，解析复用
// parseDockerBlobTarget）。以下情况一律返回 false（保持旧版 BYPASS 透传，语义零变化）：
//   - 没有 Range 头 / 有多个 Range 头（多区间语义无法用单区间（缓存条目/半截前缀）表达）；
//   - 单位不是 bytes、不是单区间（含逗号）、起点无法解析（后缀区间 `bytes=-N` 的起点段为空）、
//     起点为负、终点不合法（解析失败 / 终点小于起点）；
//   - 目标是完整 URL / manifest / 任何解析不出 `/v2/<name>/blobs/<digest>` 的路径。
func parseBlobRange(r *http.Request, target string) (blobRangeSpec, bool) {
	var spec blobRangeSpec
	if r == nil {
		return spec, false
	}
	// 多个 Range 头 = 多区间请求（RFC 9110 允许重复头），既有网关语义不接管
	if vals := r.Header.Values("Range"); len(vals) != 1 {
		return blobRangeSpec{}, false
	}
	v := strings.TrimSpace(r.Header.Get("Range"))
	// 单位只认 bytes（大小写不敏感，与 RFC 一致），其它单位维持透传
	if len(v) < len("bytes=") || !strings.EqualFold(v[:len("bytes=")], "bytes=") {
		return blobRangeSpec{}, false
	}
	v = strings.TrimSpace(v[len("bytes="):])
	if strings.ContainsRune(v, ',') {
		return blobRangeSpec{}, false // 多区间（bytes=0-99,200-299）：维持透传
	}
	dash := strings.IndexByte(v, '-')
	if dash < 0 {
		return blobRangeSpec{}, false // 语法不合法的 Range：维持透传（由上游决定如何回应）
	}
	// 起点必须显式写数字：后缀区间（`-500`）的起点段为空，ParseInt 失败 → 不接管
	start, err := strconv.ParseInt(strings.TrimSpace(v[:dash]), 10, 64)
	if err != nil || start < 0 {
		return blobRangeSpec{}, false
	}
	spec.start = start
	if endStr := strings.TrimSpace(v[dash+1:]); endStr != "" {
		end, perr := strconv.ParseInt(endStr, 10, 64)
		if perr != nil || end < start {
			return blobRangeSpec{}, false // 终点不合法 / 终点小于起点：维持透传（RFC 属无效区间）
		}
		spec.hasEnd, spec.end = true, end
	}
	// 目标必须是 docker blob：本地切片/半截拼接/缓存复用只对 digest 寻址的不可变内容成立
	if _, ok := parseDockerBlobTarget(target); !ok {
		return blobRangeSpec{}, false
	}
	return spec, true
}

// parseZeroRangeBlob 保留「起点 0」这一窄语义的既有入口（captureStreamPartial 等既有
// 调用方按它判定「是否等价完整 GET」）：非 blob / 起点 >0 / 后缀区间 / 多区间一律 false。
// 泛化解析见 parseBlobRange（本函数是它的零起点子集，签名与既有调用方保持不变）。
func parseZeroRangeBlob(r *http.Request, target string) (blobRangeSpec, bool) {
	spec, ok := parseBlobRange(r, target)
	if !ok || spec.start != 0 {
		return blobRangeSpec{}, false
	}
	return spec, true
}

// zeroRangeWriter 把内部「完整 GET」写出的 200 响应改写成 Range 语义的 206 响应
// （只包装 handleGet 的 w，且仅在「blob + 起点 0」命中时包装）。
//
// 改写规则（全部在 WriteHeader 里一次算清，此后响应头固化）：
//   - 状态码 200 且总长可得（Content-Length > 0）：206 + `Content-Range: bytes 0-<last>/<total>`，
//     并补 `Accept-Ranges: bytes`；last = min(区间终点, total-1)，带区间终点时把
//     Content-Length 收紧为 last+1，并截断写给客户端的多余字节（内部仍按完整 GET 消费
//     上游、照常写缓存——缓存条目与客户端响应互不影响）；
//   - 状态码 200 但总长未知：带终点时按 RFC 7233 的总长未知写法 `bytes 0-<end>/*`（206，
//     不设 Content-Length，Go 走 chunked）；开区间则保持 200（`bytes=0-` 与完整 GET 等价，
//     构造不出合法 Content-Range 时回 200 比回一个坏 Content-Range 更安全），仍补
//     Accept-Ranges: bytes；
//   - 非 200（502 错误页 / 上游 4xx 透传 / 304 等）：原样透传，绝不改写——错误语义必须
//     让 docker 客户端看到真实情况。
type zeroRangeWriter struct {
	http.ResponseWriter
	// spec 客户端请求的区间语义（parseZeroRangeBlob 的结果，start 恒为 0）。
	spec blobRangeSpec
	// wroteHeader WriteHeader 只允许生效一次（与 net/http 语义一致，防重复写头）。
	wroteHeader bool
	// written 已写给客户端的 body 字节数（带区间终点时用于截断判断）。
	written int64
	// limit 允许写给客户端的 body 上限（带区间终点时 = last+1）；capped=false 时不生效。
	limit int64
	// capped 是否需要把 body 截断到 limit（仅「带区间终点且总长可得」时为 true）。
	capped bool
}

// newZeroRangeWriter 用客户端的区间语义包装 w（在 handleGet 剥离 Range 之前调用一次）。
func newZeroRangeWriter(w http.ResponseWriter, spec blobRangeSpec) http.ResponseWriter {
	return &zeroRangeWriter{ResponseWriter: w, spec: spec}
}

// WriteHeader 实现 http.ResponseWriter：200 → 206 的改写点（其余状态码原样透传）。
func (z *zeroRangeWriter) WriteHeader(status int) {
	if z.wroteHeader {
		return
	}
	z.wroteHeader = true
	if status != http.StatusOK {
		z.ResponseWriter.WriteHeader(status)
		return
	}
	h := z.Header()
	// 总长取内部自己写出的 Content-Length：完整 GET 的各条出口都会在 commit 前设好它
	//（缓存条目长度 / 上游声明的总长），它正是 Content-Range 需要的 `<total>`。
	total := int64(-1)
	if cl := strings.TrimSpace(h.Get("Content-Length")); cl != "" {
		if v, err := strconv.ParseInt(cl, 10, 64); err == nil && v > 0 {
			total = v
		}
	}
	h.Set("Accept-Ranges", "bytes") // 明确告知客户端本资源支持按区间重取
	switch {
	case total > 0:
		last := blobRangeLast(z.spec, total)
		h.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", last, total))
		h.Set("Content-Length", strconv.FormatInt(last+1, 10))
		z.limit = last + 1
		z.capped = true
		status = http.StatusPartialContent
	case z.spec.hasEnd:
		// 总长未知 + 客户端给了终点：RFC 7233 允许总长写 *；不设 Content-Length
		//（body 实际长度 <= end+1 时也不会与声明冲突，Go 走 chunked 编码）
		h.Set("Content-Range", fmt.Sprintf("bytes 0-%d/*", z.spec.end))
		h.Del("Content-Length")
		z.limit = z.spec.end + 1
		z.capped = true
		status = http.StatusPartialContent
	default:
		// 总长未知 + 开区间：保持 200（等价完整 GET），仅补上 Accept-Ranges
	}
	z.ResponseWriter.WriteHeader(status)
}

// Write 实现 http.ResponseWriter：带区间终点时把超出区间的字节丢弃（不写客户端），
// 返回「已消费」的长度让内部完整 GET 链路继续跑完——缓存照常写全，客户端只收区间内字节。
func (z *zeroRangeWriter) Write(p []byte) (int, error) {
	if !z.wroteHeader {
		z.WriteHeader(http.StatusOK) // 隐式写头：与 net/http 语义一致
	}
	orig := len(p)
	if z.capped {
		remain := z.limit - z.written
		if remain <= 0 {
			return orig, nil // 区间已写满：多余字节丢弃（内部仍继续消费上游/写缓存）
		}
		if int64(orig) > remain {
			p = p[:remain]
		}
	}
	n, err := z.ResponseWriter.Write(p)
	z.written += int64(n)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, nil // 底层短写：如实返回（调用方按短写处理）
	}
	return orig, nil // 被截断的尾部按「已消费」返回，避免调用方把截断当成写失败
}

// serveEntryBlobRange 把「完整的本地内容」按客户端区间切片回 206（缓存 HIT 的任意 Range、
// 以及续传执行方刚把半截补成完整 blob 的收尾回放共用；两种情况 body 都是完整 blob 原文）：
//
//   - 起点越界（start >= total）：按 RFC 9110 回 416 + `Content-Range: bytes */<total>`，
//     让客户端知道真实总长（而不是拿一个空 200 去猜）；
//   - 终点越界按 RFC 收窄到 total-1；开区间到 total-1；
//   - 响应头：沿用条目响应头（剔除逐跳头与自管头），补 X-Cache / X-Proxy-Upstream /
//     Accept-Ranges: bytes / Content-Range / Content-Length（区间长度），状态码 206；
//   - **绝不发上游**：这是「一次落盘、后续任意起点全秒回」的落点，也是 docker 反复重试
//     同一 layer 时不再打镜像站的保证。
func (s *Server) serveEntryBlobRange(w http.ResponseWriter, r *http.Request, e *cache.Entry, spec blobRangeSpec, cacheStatus string, eff config.Options, start time.Time) {
	labels := parseDockerLogInfo(r.URL.Path)
	// source 按回放链路派生（与 serveEntry 一致）：X-Cache: RESUME 的区间回放属续传链路，
	// 其余 HIT / MISS / BYPASS 区间回放属客户端主路径（日志规范四值枚举）
	logSource := dockerLogSourceClient
	if cacheStatus == resumeCacheStatus {
		logSource = dockerLogSourceResume
	}
	total := int64(len(e.Body))
	h := w.Header()
	for k, vv := range e.Header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	h.Set("X-Cache", cacheStatus)
	if e.Source != "" {
		h.Set("X-Proxy-Upstream", e.Source) // 观测内容原始来源（缓存条目里固化的上游）
	}

	// //////////////////  起点越界：416（按 RFC 报真实总长）  start  ////////////////////////
	if spec.start >= total {
		h.Del("Content-Length")
		h.Set("Accept-Ranges", "bytes")
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		s.log.Warn("blob 区间请求起点越界，回 416（本地内容总长已知，不发上游）",
			labels.attrs(logSource, "rule", eff.Rule, "cache", cacheStatus,
				"range_start", spec.start, "total", total, "target", truncate(r.URL.Path, 300))...)
		s.log.Info("代理请求完成",
			labels.attrs(logSource,
				"method", r.Method, "target", truncate(r.URL.Path, 300),
				"status", http.StatusRequestedRangeNotSatisfiable, "cache", cacheStatus,
				"upstream", e.Source, "rule", eff.Rule, "bytes", 0,
				"cost", time.Since(start).Round(time.Millisecond).String())...)
		return
	}
	// //////////////////  起点越界：416（按 RFC 报真实总长）  end  //////////////////////////

	// //////////////////  按区间切片回放  start  ////////////////////////////////////////////
	last := blobRangeLast(spec, total)
	n := last - spec.start + 1
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", spec.start, last, total))
	h.Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusPartialContent)
	// 活跃下载进度登记（周期汇总，见 progress_summary.go）：区间回放同样是客户端下载
	//（source 与上方日志派生一致），分块写逐块 addBytes；total 取本次区间长度（percent
	// 描述这次要发的区间而不是完整对象），不输出超区间语义
	prog := s.progressRegister(logSource, labels.image, labels.digest, r.Method, 0, n)
	defer prog.unregister()
	if err := writeBodyWithProgress(w, e.Body[int(spec.start):int(last)+1], prog); err != nil {
		s.log.Warn("写响应体失败（客户端可能已断开）",
			labels.attrs(logSource, "cache", cacheStatus,
				"target", truncate(r.URL.Path, 200), "err", err)...)
	}
	s.log.Info("成功返回内容（本地完整内容按客户端区间切片回 206，不发上游）",
		labels.attrs(logSource, "rule", eff.Rule, "cache", cacheStatus,
			"range_start", spec.start, "range_last", last, "total", total, "bytes", n,
			"upstream", e.Source)...)
	s.log.Info("代理请求完成",
		labels.attrs(logSource,
			"method", r.Method, "target", truncate(r.URL.Path, 300),
			"status", http.StatusPartialContent, "cache", cacheStatus,
			"upstream", e.Source, "rule", eff.Rule, "bytes", n,
			"cost", time.Since(start).Round(time.Millisecond).String())...)
	// //////////////////  按区间切片回放  end  //////////////////////////////////////////////
}
