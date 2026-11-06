// partial_resume.go —— docker blob 半截保留与无感续传：
// 用户场景：镜像层 digest（唯一值）不变，之前下载到一半（如 248.9MB/773.3MB）中断，
// docker 客户端跨 pull 重试是**无 Range 的完整 GET**（从 0 开始），但 proxy 这边已经
// 下载过的一部分应该保留——客户端再请求该层时把已下载部分直接给客户端，proxy 同时从
// 断掉的地方继续向远端下载。
//
// 机制：
//   - 半截保留：blob（/v2/<name>/blobs/<digest>）的流式下载（客户端主路径与预取路径）
//     中断/失败时，把已写前缀保留为缓存目录内的半截文件 `.part-<缓存key>`（文件大小即
//     已下载字节数，内容为 blob 原文前缀、无缓存文件头）；manifest 会变，仅 blob 启用。
//     进度只增不减：任何保留动作仅在「新前缀比现有半截更长」时落盘，绝不回退；
//   - 无感续传：客户端无 Range 完整 GET 撞上「正式缓存 MISS + 半截文件存在」时走
//     「续传执行方」——按既有候选轮询发 `Range: bytes=<已存字节>-`（既有候选级超时 /
//     http-proxy / insecure TLS / 401 匿名 token dance 全复用），从上游 206 响应的
//     Content-Range 取总长，对客户端回**一次完整 200**：Content-Length=总长（可得时），
//     body = 本地半截前缀回放 + 上游剩余区间实时拼接——docker 客户端无需支持 Range、
//     完全无感知；拼接期间上游剩余数据边转发边追加写半截文件，EOF 后半截内容 = 完整
//     blob，按既有 cache.Set 语义原子转为正式缓存条目并删除半截文件；
//   - 并发合并：续传执行方登记与预取同一套在途流共享注册表（prefetchStreamHub 的
//     resume 条目），其余客户端请求/等待超限的重试请求经既有搭车机制（tryServeFromPrefetchHub）
//     订阅「半截前缀 + 实时增量」，X-Cache: PREFETCH；同 blob 的预取任务经既有
//     flightGroup（同缓存键）合并到同一路续传流——上游对该 blob 只有一路 Range 续传下载；
//   - 失败语义：续传中途失败时保留更新后的半截文件（已追加到最新前缀），客户端本次响应
//     被截断——docker 会重试，下次从更长前缀接着续传（进度不回退）。
//
// 安全依据与边界：
//   - blob 按 digest 寻址、内容不可变：任意候选源的同 digest 内容一致，续传可从任意
//     候选发 `Range: bytes=<n>-` 拼到本地前缀后面（这是半截可跨候选拼接的安全前提）；
//   - 客户端/上游侧 Docker-Content-Digest 不做本地全量校验（对 GB 级 blob 成本过高）：
//     信任 digest 寻址语义（前缀 + 断点剩余拼出的即完整 blob），与「206 透传不校验」
//     的既有取舍一致；
//   - commit 前必须先确认上游是 206 且 Content-Range 起点匹配半截长度：确认前绝不向
//     客户端写任何字节——若上游对 Range 回 200 全量响应，回放本地前缀再拼全量会数据
//     错位，因此非 206 一律安全降级（丢弃半截走既有全量回源路径，绝不给客户端错误数据）；
//   - 半截为 0 字节直接丢弃；上游 416（起点 ≥ 总长，而半截是合法前缀）证明半截已完整，
//     直接读回写缓存回放，不再全量下载；
//   - 客户端带 Range 的请求维持现状（206 透传，不做本地拼接，不写半截）——docker 重试
//     时既有 206 机制已兜底；no-cache / 关缓存等 BYPASS 请求同样维持既有行为；
//   - 续传路径不叠加 slowdownload 慢速升级多源并发续传（与预取一路同一取舍：上游速率
//     由续传那一路决定，过慢靠截断 + docker Range 重试兜底）。
//
// 生命周期：
//   - 产生：客户端主路径流式回源失败（stream.go「commit 后读上游失败」与「客户端断开
//     终止」出口，后者由断开即停语义取代旧版「继续读完填缓存」——上游读取立即终止、
//     半截保留，进度不回退）/ 预取读上游失败（prefetch.go，临时文件改名零拷贝保留）；
//   - 消费：无 Range 完整 GET 的续传执行方（本文件）回放前缀并续传拼接；
//   - 删除：对应 blob 正式缓存条目落盘后立即删除（finalizeFetchEntry 钩子）；续传扫描
//     判定上游明确不支持按断点续传（非 206）时丢弃；cache.Clean 扩展清理空半截与超过
//     partMaxAge（24h，量级同缓存条目过期 TTL）的孤儿残留（崩溃残留兜底）。
//
// 日志规范：续传执行方（scanResumeCandidate / resumeBlobStreaming / serveResumeComplete）
// 全部日志统一 source=resume（客户端请求触发的执行方）；半截保留日志按产生路径标注
// 来源——客户端主路径流式失败（savePartialPart）= client、预取流式失败
// （keepPrefetchPartial）= prefetch。镜像名统一键 image（savePartialPart /
// keepPrefetchPartial 经 variadic dockerLogInfo 注入，既有定参调用方与既有测试不受影响，
// 与 fetchViaCandidatesStreaming 的 partial ...partialSink 同款风格）。键名与顺序由
// log_attrs.go 统一拼装。
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// resumeCacheStatus 续传执行方路径 X-Cache 观测头的取值：与 MISS / BYPASS / HIT /
// PREFETCH 风格一致的新值，标示本次内容来自「半截前缀回放 + 断点续传实时拼接」。
const resumeCacheStatus = "RESUME"

// resumeScanBudget 续传候选扫描的整体预算（全部候选轮询完的上限，到期按超时类失败
// 收场并降级既有全量回源；半截保留不受影响）。取值沿用既有 candidateScanBudget
// （20s，与 docker 客户端约 30s 响应头超时的匹配依据一致，见 candidates.go）。
const resumeScanBudget = candidateScanBudget

// parseDockerBlobTarget 判断路径目标是否为 docker blob 请求并拆出 digest：形如
// v2/<name>/blobs/<digest>（target 为 ExtractTarget 输出的路径目标原文：不带 scheme
// 与前导 /，可能带查询串；容忍前导 /）。digest 经 validBlobDigest 宽松校验（路径安全，
// 与预取同款）。半截保留与续传仅对 blob 启用——blob 按 digest 寻址、内容不可变；
// manifest 会变，绝不适用。保持既有 (digest, ok) 签名不变（既有调用方与既有测试的
// 表驱动用例不受影响）；需要镜像名（日志统一标签 image）时用 parseDockerBlobNameDigest。
func parseDockerBlobTarget(target string) (digest string, ok bool) {
	_, d, ok := parseDockerBlobNameDigest(target)
	return d, ok
}

// parseDockerBlobNameDigest parseDockerBlobTarget 的完整版：同时拆出仓库名与 digest
// （v2/<name>/blobs/<digest>，name 支持多级仓库）。拆分逻辑与 parseDockerBlobTarget
// 同源同语义（digest 校验同款），name 供日志统一标签 image 使用（log_attrs.go）。
func parseDockerBlobNameDigest(target string) (name, digest string, ok bool) {
	t := target
	if i := strings.IndexByte(t, '?'); i >= 0 {
		t = t[:i] // 去掉查询串
	}
	t = strings.TrimPrefix(t, "/") // 兜底：容忍前导 /（ExtractTarget 输出本就没有）
	segs := strings.Split(t, "/")
	// v2 / <name 至少一段> / blobs / <digest>：至少 4 段，倒数第 2 段必须是 blobs
	if len(segs) < 4 || segs[0] != "v2" || segs[len(segs)-2] != "blobs" {
		return "", "", false
	}
	name = strings.Join(segs[1:len(segs)-2], "/")
	d := segs[len(segs)-1]
	if !validBlobDigest(d) {
		return "", "", false
	}
	return name, d, true
}

// //////////////////  半截文件的写侧互斥与保留  ////////////////////////////////////////////////

// partialSink 流式回源失败时的 blob 半截保留上下文：fetchViaCandidatesStreaming 以可选
// variadic 注入（既有调用方与既有测试的 7 参调用完全不受影响），仅「缓存可读的客户端
// 主路径」注入——BYPASS（Range / no-cache / 关缓存）不注入，维持既有行为。
type partialSink struct {
	// key blob 缓存键（半截文件定位）。
	key string
	// disk 生效缓存实例（半截文件所在目录；nil = 不保留）。
	disk *cache.Cache
}

// partialSinkOf 取出可选 variadic 中的首个 sink（无则 nil）。
func partialSinkOf(all []partialSink) *partialSink {
	if len(all) == 0 {
		return nil
	}
	return &all[0]
}

// partWriter 返回 key 对应的半截文件写锁（无则创建）：串行化同一 blob 半截文件的全部
// 写侧操作（续传执行方逐块追加 / 失败前缀整体保存 / 预取半截改名），杜绝并发写交错
// 产生「非前缀」内容——半截文件必须是 blob 原文的前缀，这是续传拼接正确性的前提。
// 键为 blob 缓存键，仅在出现过半截活动的 blob 上产生条目（单条目 = 一个 sync.Mutex，
// 内存量级可忽略），随进程生命周期驻留。
func (s *Server) partWriter(key string) *sync.Mutex {
	s.partialMu.Lock()
	defer s.partialMu.Unlock()
	if s.partialWriters == nil { // 防御：绕过 New 构造的 Server 也能安全写入
		s.partialWriters = make(map[string]*sync.Mutex)
	}
	m, ok := s.partialWriters[key]
	if !ok {
		m = &sync.Mutex{}
		s.partialWriters[key] = m
	}
	return m
}

// savePartialPart 把已下载前缀整体保存为半截文件（流式回源失败后的半截保留入口，写锁内）：
// 仅当待存前缀比现有半截更长时覆盖重写——半截进度只增不减，绝不回退。整体重写用
// os.WriteFile（truncate + 顺序写）：任意时点中断的残留仍是合法前缀（顺序写的性质），
// 进程崩溃残留由 cache.Clean 的空文件/孤儿清理规则兜底。data 为空不落盘（0 字节半截
// 无续传价值，见入口侧的空半截丢弃）。本函数仅在客户端主路径流式失败时被调用
// （captureStreamPartial），日志统一 source=client；labels 为可选的 docker 日志统一标签
// （variadic 注入，既有定参调用方与既有测试不受影响，见 dockerLogInfoOf）。
// 返回值 kept：true = 半截保留生效（本次落盘成功，或已有不短于待存前缀的半截——进度
// 已保住）；false = 不适用或落盘失败（disk 为 nil / 空数据 / 写文件失败，后者已有 Warn
// 留痕）。既有语句式调用方忽略返回值不受影响。
func (s *Server) savePartialPart(disk *cache.Cache, key string, data []byte, labels ...dockerLogInfo) (kept bool) {
	if disk == nil || len(data) == 0 {
		return false
	}
	lab := dockerLogInfoOf(labels)
	mu := s.partWriter(key)
	mu.Lock()
	defer mu.Unlock()
	path := disk.PartPath(key)
	if st, err := os.Stat(path); err == nil && st.Size() >= int64(len(data)) {
		return true // 已有相同或更长前缀：保持不动（进度不回退，视为已保留）
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		s.log.Warn("保留 blob 半截前缀失败", lab.attrs(dockerLogSourceClient, "file", path, "bytes", len(data), "err", err)...)
		return false
	}
	s.log.Info("blob 半截前缀已保留（下载中断，下次无 Range 请求将从断点无感续传）",
		lab.attrs(dockerLogSourceClient, "key", key, "bytes", len(data), "file", path)...)
	return true
}

// keepPrefetchPartial 预取路径的半截保留：预取流式下载读上游失败时，把已下载前缀的
// 临时文件改名为半截文件（同目录 rename，原子零拷贝）。仅当临时文件比现有半截更长时
// 改名（进度不回退）；改名后 hub 条目的 tmpPath 仍指向旧临时文件名（已被移走），
// finishFailed 的临时文件清理对不存在文件按 ErrNotExist 静默跳过，无副作用。写锁内
// 执行（与续传执行方的半截写侧互斥）。本函数仅在预取流式失败时被调用
// （prefetchBlobStreaming），日志统一 source=prefetch；labels 为可选的 docker 日志统一
// 标签（variadic 注入，既有定参调用方与既有测试不受影响，见 dockerLogInfoOf）。
func (s *Server) keepPrefetchPartial(disk *cache.Cache, key, tmpPath string, size int64, labels ...dockerLogInfo) {
	if disk == nil || tmpPath == "" || size <= 0 {
		return
	}
	lab := dockerLogInfoOf(labels)
	mu := s.partWriter(key)
	mu.Lock()
	defer mu.Unlock()
	partPath := disk.PartPath(key)
	if st, err := os.Stat(partPath); err == nil && st.Size() >= size {
		s.log.Debug("blob 半截已保留更长前缀，预取半截丢弃（进度不回退）",
			lab.attrs(dockerLogSourcePrefetch, "key", key, "kept", st.Size(), "dropped", size)...)
		return
	}
	if err := os.Rename(tmpPath, partPath); err != nil {
		s.log.Warn("保留预取 blob 半截前缀失败（改名失败，残留由缓存后台清理兜底）",
			lab.attrs(dockerLogSourcePrefetch, "from", tmpPath, "to", partPath, "err", err)...)
		return
	}
	s.log.Info("blob 半截前缀已保留（预取下载中断，下次无 Range 请求将从断点无感续传）",
		lab.attrs(dockerLogSourcePrefetch, "key", key, "bytes", size, "file", partPath)...)
}

// appendPartChunk 续传执行方逐块追加半截文件（写锁内）：追加前校验文件当前大小 == 期望
// 已写字节数——不匹配说明半截被并发保存方整体重写过（内容已换成更长前缀，进度已超前），
// 本流立即失败收尾（半截保留更长前缀、进度不回退），绝不产生错位拼接的损坏内容。
// expect 为追加前的期望字节数（追加后由调用方推进游标）。
func (s *Server) appendPartChunk(disk *cache.Cache, key string, f *os.File, expect int64, p []byte) error {
	mu := s.partWriter(key)
	mu.Lock()
	defer mu.Unlock()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("校验半截文件大小失败: %w", err)
	}
	if st.Size() != expect {
		return fmt.Errorf("半截文件已被并发更新（当前 %d 字节 ≠ 期望 %d 字节），终止本流续传（更长前缀已保留，进度不回退）",
			st.Size(), expect)
	}
	if _, err := f.Write(p); err != nil {
		return fmt.Errorf("追加写半截文件失败: %w", err)
	}
	return nil
}

// captureStreamPartial 流式回源失败后的 blob 半截保留入口（fetchViaCandidatesStreaming
// 的「commit 后读上游失败」与「客户端断开终止」出口调用）：仅 blob 目标 + 全局开关开启 +
// 无客户端 Range 时保留前缀；manifest 会变不适用（仅 blob 启用）。写入按进度不回退规则
// （savePartialPart）。返回值 kept：true = 半截保留生效，false = 不适用或落盘失败——
// 供断开终止出口的运维事件日志字段（partial）区分「保留成功 / 不适用」。
func (s *Server) captureStreamPartial(sink *partialSink, r *http.Request, target string, data []byte) (kept bool) {
	if sink == nil || sink.disk == nil || len(data) == 0 {
		return false
	}
	if s.cfg == nil || !s.cfg.GlobalDockerPartialResumeEnabled() {
		return false // 全局开关关闭：维持既有「失败即丢弃」行为
	}
	if r.Header.Get("Range") != "" {
		return false // 防御：带 Range 的请求维持 206 透传不做本地半截（入口分流本不会带 sink）
	}
	// 客户端主路径半截保留：解析镜像名与 digest 作为日志统一标签（source=client）
	name, digest, ok := parseDockerBlobNameDigest(target)
	if !ok {
		return false // 非 blob（manifest 会变等）：不适用半截保留
	}
	return s.savePartialPart(sink.disk, sink.key, data, dockerLogInfo{image: name, digest: digest})
}

// //////////////////  续传候选扫描  ////////////////////////////////////////////////////////////

// scanResumeCandidate 续传候选轮询扫描：按 round-robin 起始下标（共用 s.rr 计数器，
// 与既有候选管线同一负载轮询语义）逐个候选发 `Range: bytes=<partSize>-`（复用
// startUpstream 公共底层：候选级超时覆盖 / http-proxy / insecure TLS / 401 匿名
// token dance 全复用），整体受 resumeScanBudget 兜底。逐候选失败不中断轮询（换下一
// 候选重试语义与既有管线一致）。返回值：
//   - res：首个通过校验的 206 探测结果（活体 body 所有权移交调用方；complete=true 时为 nil）
//   - total：对象总长（206 Content-Range 的 total；不可知为 -1，调用方流式不设 Content-Length）
//   - complete：上游回 416 证实半截已完整（Range 起点 ≥ 总长而半截是合法前缀 → 只可能已完整）
//   - invalid：是否存在「上游明确拒绝按断点续传」的失败（非 206 / Content-Range 起点不匹配）——
//     供调用方按安全降级规则丢弃半截；纯网络/超时失败（含扫描预算耗尽）不算
//   - err：全部候选失败时的最后一个错误（complete 时为 nil）
//
// 校验复用 validateResumeResponse（206 且 Content-Range 起点必须等于请求偏移）：上游
// 200 全量回属「不支持 Range」，绝不能把本地前缀回放与全量响应拼在一起给客户端——
// 在确认 206 且起点匹配之前，绝不向客户端写任何字节（安全降级的前提）。
func (s *Server) scanResumeCandidate(r *http.Request, candidates []candidate, eff config.Options, partSize int64) (res *probeResult, total int64, complete, invalid bool, err error) {
	if len(candidates) == 0 {
		// 防御分支：handleProxy 已拦截「路径目标未配置 url-redirect」，续传路径同样绝不直连
		return nil, -1, false, false, errors.New("路径目标请求未配置 url-redirect，无法续传回源")
	}
	n := len(candidates)
	start := int(s.rr.Add(1)-1) % n
	rangeHeader := fmt.Sprintf("bytes=%d-", partSize) // 开区间：从断点到对象末尾
	// docker 日志统一标签：续传请求必然是 /v2/ 路径目标，按需解析 image/digest
	//（本函数无 target 参数，从入站请求路径解析——纯字符串 split，与 ExtractTarget 输出同构）
	labels := parseDockerLogInfo(r.URL.Path)

	// 整体扫描预算：与客户端断开解耦（WithoutCancel，与既有候选管线一致），
	// 到期按超时类失败收场（半截保留、降级全量回源）。cancel 不走 defer：defer
	// 会在本函数返回时立即取消 ctx，杀掉命中候选的在途 body 读取——206 响应是
	// 活体流，续传收到首块即 context canceled。取消权随结果移交（与 selectCandidate
	// 的 onClose(cancelBase) 同款）：命中 206 的出口把 cancel 挂 upRes.onClose，
	// 随调用方流结束/放弃的 Close 统一释放；416 / 扫描全败 / 预算耗尽等不返回
	// 活体 body 的出口显式 cancel()，绝不泄漏。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), resumeScanBudget)

	var lastErr error
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break // 扫描预算耗尽：按超时类失败收场
		}
		idx := (start + i) % n
		cand := candidates[idx]
		upRes, uerr := s.startUpstream(ctx, r, cand, eff, rangeHeader, nil)
		if uerr != nil {
			lastErr = uerr
			s.log.Debug("blob 半截续传候选请求失败，轮询下一候选",
				labels.attrs(dockerLogSourceResume, "index", idx, "candidate", truncate(cand.url, 300), "range", rangeHeader, "err", uerr)...)
			continue // 网络/超时类失败：半截保留
		}
		if upRes.status == http.StatusRequestedRangeNotSatisfiable {
			// 416：Range 起点不小于对象总长，而半截是同 digest 内容的合法前缀
			//（起点 ≤ 总长）→ 只可能是「半截已完整」。从 416 的 Content-Range:
			// bytes */<total> 解析总长留痕（解析不出按 -1）
			_, _, t416, pok := parseContentRange(upRes.header.Get("Content-Range"))
			if !pok {
				t416 = -1
			}
			upRes.Close()
			cancel() // 416 出口不返回活体 body：ctx 就地回收（重复取消幂等无害）
			s.log.Info("blob 半截续传发现半截已完整（上游 416 证实起点已达总长）",
				labels.attrs(dockerLogSourceResume,
					"candidate", truncate(cand.url, 300), "part_bytes", partSize, "total", t416)...)
			return nil, t416, true, false, nil
		}
		if verr := validateResumeResponse(upRes.status, upRes.header, partSize); verr != nil {
			upRes.Close()
			invalid = true
			lastErr = verr
			s.log.Debug("blob 半截续传响应不合规（上游明确拒绝按断点续传），轮询下一候选",
				labels.attrs(dockerLogSourceResume, "index", idx, "candidate", truncate(cand.url, 300),
					"status", upRes.status, "part_bytes", partSize, "err", verr)...)
			continue
		}
		// 206 且起点匹配：解析对象总长（Content-Range 的 total；拿不到按 -1 →
		// 流式不设 Content-Length，Go 按 chunked 编码）
		_, _, t, pok := parseContentRange(upRes.header.Get("Content-Range"))
		if !pok || t <= 0 {
			t = -1
		}
		s.log.Info("blob 半截续传候选命中",
			labels.attrs(dockerLogSourceResume, "index", idx, "candidate", truncate(cand.url, 300), "status", upRes.status,
				"part_bytes", partSize, "total", t)...)
		upRes.onClose(cancel) // 命中出口返回活体 body：扫描 ctx 取消权随流移交调用方（Close 时释放）
		return upRes, t, false, false, nil
	}
	if lastErr == nil {
		lastErr = errors.New("全部续传候选均未产生结果")
	}
	cancel() // 扫描收场（全败/预算耗尽/不合规轮完）：本出口无活体 body，ctx 就地回收
	return nil, -1, false, invalid, lastErr
}

// //////////////////  续传执行方  //////////////////////////////////////////////////////////////

// resumeBlobStreaming blob 半截续传执行方（tryServeBlobResume 经既有 flightGroup 编排，
// 同 key 只有一个执行方）：
//  1. 重查半截前缀（前置检查后可能已被并发流程消费，竞态按既有全量回源兜底）；
//  2. 登记「续传在途条目」（prefetchStreamHub 的 resume 条目，written 预置为半截前缀
//     长度）——此后其余客户端请求即可经既有搭车机制订阅前缀+增量（上游仅此一路下载）；
//  3. 候选轮询 Range 续传扫描（scanResumeCandidate）：全部失败按失败类别降级——上游
//     明确拒绝按断点续传（非 206）丢弃半截、网络/超时保留半截，均走既有全量回源兜底
//     （commit 前未向客户端写过任何字节，行为与旧版一致）；
//  4. 扫描发现半截已完整（上游 416）：读回半截写缓存并回放，不再全量下载；
//  5. 206 且起点匹配（commit 前置校验）：合成完整 200 响应头（Content-Length=总长，
//     剔除上游 206 的 Content-Range），对客户端 commit 一次完整 200；
//  6. 回放半截前缀（写锁内整读、锁外写客户端——慢客户端不拖住写锁），随后流式读取
//     上游剩余区间：逐块「追加写半截文件 → 广播增量给搭车订阅者 → 写客户端与累积缓冲」
//     （先磁盘后推进 written，是搭车订阅者读前缀的既有契约）；
//  7. EOF 且累积长度与总长一致：半截内容 = 完整 blob，复用 finalize（既有 cache.Set
//     语义）原子写缓存（blob 落盘成功即删半截，见 finalizeFetchEntry 钩子），置 done
//     关闭订阅（搭车方收完缓冲内增量即完整）；
//  8. 任何失败：保留更新后的半截文件、广播 failed 并移除条目、本次响应被截断（docker
//     以 Range 断点续传重试，下次从更长前缀接着续传——进度不回退）。
//
// 返回值语义与 fetchViaCandidatesStreaming 一致：committed=true 表示已向客户端写出
// 响应头（调用方不得再 writeError）。不叠加 slowdownload 慢速升级多源并发续传
// （取舍见文件头边界说明）。
func (s *Server) resumeBlobStreaming(w http.ResponseWriter, r *http.Request, key, target, digest string, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry)) (*cache.Entry, bool, error) {
	// docker 日志统一标签：续传请求必然是 /v2/ blob 目标，按需解析 image/digest
	//（本函数全部执行方日志统一 source=resume，见 log_attrs.go）
	labels := parseDockerLogInfo(target)
	// 重查半截前缀：前置检查与成为执行方之间可能被并发流程消费（前一个执行方完成
	// 写缓存并删半截等），竞态按既有全量回源兜底（行为与旧版一致）
	n, ok := disk.PartSize(key)
	if !ok || n <= 0 {
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}

	// 登记续传在途条目（搭车窗口自候选扫描前开始，覆盖整个续传过程；同 key 既有
	// 预取/续传条目被覆盖属极小竞态窗口，两路内容同源、写缓存原子替换，仅浪费一路
	// 带宽不产生错误数据）；tmpPath 指向半截文件本体（搭车订阅者读前缀用，无写句柄）
	hub := s.prefetchHubOrInit()
	ent := hub.registerResume(key, eff.Rule, labels.image, digest, n, s.log)
	ent.attachFile(nil, disk.PartPath(key))
	// panic 防护（与 prefetchBlobStreaming 同款理由）：flight 对 fn panic 有自身 recover
	//（清理在途记录后 re-panic），这里必须先把 hub 条目置 failed——否则 running 条目
	// 滞留 hub 会泄漏，后续请求会搭上一条永不推进的死流
	defer func() {
		if rec := recover(); rec != nil {
			ent.finishFailed(fmt.Errorf("blob 半截续传 panic: %v", rec))
			panic(rec)
		}
	}()

	// 候选轮询 Range 续传扫描（commit 前，可自由换候选；失败不向客户端写任何字节）
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	upRes, total, complete, invalid, serr := s.scanResumeCandidate(r, candidates, eff, n)
	if serr != nil {
		// 广播搭车订阅者降级（未 commit 的改走各自既有回源路径）
		ent.finishFailed(serr)
		if invalid {
			// 上游明确拒绝按断点续传（非 206 / Content-Range 起点不匹配）：丢弃半截
			// 走既有全量回源（安全降级——确认 206 且起点匹配前未向客户端写过任何字节，
			// 全量回源从 0 开始，绝不产生数据错位）
			disk.RemovePart(key)
		}
		s.log.Warn("blob 半截续传候选扫描失败，降级既有全量回源",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "part_bytes", n,
				"invalid", invalid, "err", serr)...)
		// 网络/超时类失败保留半截：下方全量回源若再失败，其失败捕获会把更长前缀存回
		// 半截（进度不回退）；全量成功则写缓存并删半截，两条路都正确
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}
	if complete {
		// 半截已完整（上游 416 证实）：读回半截写缓存并回放
		return s.serveResumeComplete(w, r, key, target, digest, ent, eff, disk, start, finalize)
	}

	// 合成完整 200 响应头（对客户端绝不透传 206 语义）：沿用上游响应头（Content-Type /
	// Docker-Content-Digest / ETag 等），剔除 Content-Range、Content-Length 重置为总长
	//（拿不到总长则不设，Go 按 chunked 编码流式传输）。hdr 为本次探测独立深拷贝，
	// 构建后只读（直接共享给缓存条目与搭车快照，与既有 probeResult.header 用法一致）
	hdr := upRes.header.Clone()
	hdr.Del("Content-Range")
	if total > 0 {
		hdr.Set("Content-Length", strconv.FormatInt(total, 10))
	} else {
		hdr.Del("Content-Length")
	}
	ent.setSnapshot(http.StatusOK, hdr, upRes.url) // 搭车订阅者的响应头快照（预取侧 commit 点同语义）

	// 失败收尾：关闭上游与半截写句柄、广播 failed 并移除条目（半截文件已随流追加到
	// 最新前缀，进度不回退；本次响应被截断，docker 重试从更长前缀续传）
	var paf *os.File // 半截文件追加写句柄（O_APPEND）
	fail := func(cause error) (*cache.Entry, bool, error) {
		upRes.Close() // 幂等：关闭连接、停看门狗、取消超时预算链
		if paf != nil {
			_ = paf.Close()
		}
		ent.finishFailed(cause)
		return nil, true, cause
	}

	// commit 点：对客户端回一次完整 200（本地前缀回放 + 上游剩余实时拼接，docker 无需
	// 支持 Range）。此后响应头固化，任何失败只能截断本次响应（docker 以 Range 重试，
	// 下次从更长前缀续传——进度不回退）
	s.commitResumeHeader(w, hdr, upRes.url, total)
	s.log.Info("blob 半截续传开始（回放半截前缀+实时拼接剩余区间，上游仅此一路 Range 续传）",
		labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "part_bytes", n, "total", total,
			"candidate", truncate(upRes.url, 300), "check", eff.SuccessCheck)...)

	// 回放半截前缀：写锁内一次性整读进累积缓冲（与并发保存方的整体重写互斥，保证读到
	// 的每个字节来自同一份合法前缀；磁盘顺序读，锁持有时间 = 磁盘读耗时），锁外再写给
	// 客户端（慢客户端绝不拖住写锁）。前缀字节即同 digest 内容前缀（digest 寻址不可变）
	mu := s.partWriter(key)
	mu.Lock()
	prefix, rerr := os.ReadFile(disk.PartPath(key))
	mu.Unlock()
	if rerr != nil {
		return fail(fmt.Errorf("读半截文件失败: %w", rerr))
	}
	if int64(len(prefix)) < n {
		// 半截比注册快照短（并发重写竞态窗口，理论不可达）：绝不能给客户端「短前缀 +
		// 从 n 起的上游剩余」的错位拼接，按失败截断（半截保留，docker 重试再续）
		return fail(fmt.Errorf("半截文件短于注册快照（实读 %d / 期望 %d 字节），终止本次续传", len(prefix), n))
	}
	accum := bytes.NewBuffer(make([]byte, 0, int(n)+streamChunkSize))
	clientOK := true // 客户端写失败（docker 已断开）后置 false：继续读完上游只填缓存（与既有语义一致）
	accum.Write(prefix[:n])
	if n > 0 {
		if _, werr := w.Write(prefix[:n]); werr != nil {
			clientOK = false
			s.log.Warn("写响应体失败（客户端已断开，续传继续只填缓存）",
				labels.attrs(dockerLogSourceResume, "target", truncate(r.URL.Path, 200), "err", werr)...)
		}
	}

	// 追加写句柄：上游剩余区间逐块追加到半截文件（O_APPEND 顺序追加 + 每块大小一致性
	// 校验，见 appendPartChunk）
	paf, aerr := os.OpenFile(disk.PartPath(key), os.O_WRONLY|os.O_APPEND, 0o644)
	if aerr != nil {
		return fail(fmt.Errorf("打开半截文件追加写失败: %w", aerr))
	}

	// 进度日志开关与基线（与客户端流式路径同一节拍；区间速率按自上次输出起算）
	progressInterval := eff.ProgressLogInterval
	lastProgressAt := time.Now()
	lastProgressBytes := int(n) // int64→int 对齐 accum.Len()（int）的差值/赋值运算；语义不变——速率按前缀回放之后的新增量计算
	emitProgress := func(now time.Time) {
		args := labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "bytes", accum.Len())
		if total > 0 {
			pct := int(float64(accum.Len()) / float64(total) * 100)
			if pct < 0 {
				pct = 0
			} else if pct > 100 {
				pct = 100
			}
			args = append(args, "total", total, "percent", pct)
		}
		clientState := "ok"
		if !clientOK {
			clientState = "disconnected"
		}
		args = append(args,
			"speed", humanBytesPerSec(int64(accum.Len()-lastProgressBytes), now.Sub(lastProgressAt)),
			"client", clientState)
		s.log.Info("blob 半截续传进度", args...)
		lastProgressAt = now
		lastProgressBytes = accum.Len()
	}

	// 首块先行（探测阶段已缓冲的剩余区间首块）：与客户端流式路径同一节拍，逐块
	//「追加半截 → 广播增量 → 写客户端与累积缓冲」
	written := n // 追加游标 = 半截文件当前字节数（appendPartChunk 以此做一致性校验）
	writeRemainChunk := func(p []byte) error {
		if aerr := s.appendPartChunk(disk, key, paf, written, p); aerr != nil {
			return aerr
		}
		written += int64(len(p))
		if berr := ent.writeBroadcast(p); berr != nil {
			return fmt.Errorf("blob 半截续传广播失败: %w", berr)
		}
		accum.Write(p)
		if clientOK {
			if _, werr := w.Write(p); werr != nil {
				clientOK = false
				s.log.Warn("写响应体失败（客户端已断开，续传继续只填缓存）",
					labels.attrs(dockerLogSourceResume, "target", truncate(r.URL.Path, 200), "err", werr)...)
			}
		}
		return nil
	}
	if len(upRes.first) > 0 {
		if aerr := writeRemainChunk(upRes.first); aerr != nil {
			return fail(fmt.Errorf("blob 半截续传写半截文件失败（%s）: %w", upRes.url, aerr))
		}
	}

	// 上游剩余区间流式读取：经 progressReader 记入看门狗（read-timeout「无新增数据」
	// 语义与客户端流式路径一致）；客户端写失败（已断开）保持既有行为——继续读完上游
	// 只填缓存（半截与广播照常推进，等待中的搭车订阅者不受影响）
	if upRes.body != nil {
		pr := &progressReader{r: upRes.body, wd: upRes.wd}
		buf := make([]byte, streamChunkSize)
		for {
			rd, rdErr := pr.Read(buf)
			if rd > 0 {
				if aerr := writeRemainChunk(buf[:rd]); aerr != nil {
					return fail(fmt.Errorf("blob 半截续传写半截文件失败（%s）: %w", upRes.url, aerr))
				}
			}
			if progressInterval > 0 && time.Since(lastProgressAt) >= progressInterval {
				emitProgress(time.Now())
			}
			if rdErr != nil {
				if errors.Is(rdErr, io.EOF) {
					break // 剩余区间读完：半截文件 = 完整 blob，前缀+增量恰好拼出完整 body
				}
				// commit 后读上游失败：保留更新后的半截文件（已追加到最新前缀，进度
				// 不回退），广播 failed 并截断本次响应（docker 重试接着更长前缀续传）
				s.log.Error("blob 半截续传读上游失败，终止本次响应（半截已保留，进度不回退）",
					labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest,
						"candidate", truncate(upRes.url, 300),
						"bytes", accum.Len(), "part_bytes", written, "err", rdErr)...)
				return fail(fmt.Errorf("blob 半截续传读上游失败（%s）: %w", upRes.url, rdErr))
			}
		}
	}
	_ = paf.Close() // 追加完毕；磁盘写为直写（无用户态缓冲），无需 Sync（断电丢失由 Clean 兜底）
	upRes.Close()   // 幂等：关闭连接、停看门狗、取消超时预算链

	// 完整性校验（总长已知时）：累积长度必须等于总长——不足说明上游提前 EOF（绝不把
	// 短 body 写缓存），超出说明上游 Content-Range 声明与实际不符（绝不给客户端错误
	// 数据）；两者均按失败截断（半截保留已收到的内容，进度不回退）
	if total > 0 && int64(accum.Len()) != total {
		s.log.Error("blob 半截续传长度不符，终止本次响应且不写缓存（半截已保留，进度不回退）",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest,
				"candidate", truncate(upRes.url, 300),
				"bytes", accum.Len(), "total", total)...)
		return fail(fmt.Errorf("blob 半截续传长度不符（累计 %d ≠ 总长 %d）", accum.Len(), total))
	}

	// EOF：半截前缀 + 剩余区间拼出完整 blob，复用既有缓存写入语义（判据 / TTL 固化 /
	// 日志 / 预取触发全复用——blob 目标在 finalizeFetchEntry 内写盘成功即删半截）；
	// 续传执行方写缓存直接调 finalizeFetchEntry 并标注 source=resume（不经注入的
	// finalize 回调——该回调按客户端主路径标注 source=client，此处写者实为续传执行方）
	entry := &cache.Entry{
		Status:   http.StatusOK,
		Header:   hdr,
		Body:     accum.Bytes(),
		StoredAt: time.Now(),
		Source:   upRes.url,
	}
	s.finalizeFetchEntry(r, key, target, entry, eff, disk, dockerLogSourceResume)
	// 置 done 并关闭订阅（搭车方收完 channel 缓冲内的最后增量即完整）；条目滞留 hub
	// 一小段时间供并发 MISS 兜底检出「done → 直接 HIT」，到期自动移除（不碰半截文件）
	ent.finishDone()

	s.log.Info("blob 半截续传完成（半截前缀+剩余区间拼出完整 blob 并已写缓存）",
		labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "bytes", accum.Len(),
			"part_bytes", n, "upstream", upRes.url,
			"cost", time.Since(start).Round(time.Millisecond).String())...)
	s.log.Info("代理请求完成",
		labels.attrs(dockerLogSourceResume,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", http.StatusOK,
			"cache", resumeCacheStatus,
			"upstream", upRes.url,
			"rule", eff.Rule,
			"bytes", accum.Len(),
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
	return entry, true, nil
}

// serveResumeComplete 半截已完整的收尾（续传扫描发现上游对 bytes=<n>- 回 416）：416
// 语义 = 请求起点不小于对象总长，而半截是同 digest 内容的合法前缀（起点 ≤ 总长），
// 只能 n == total——半截即完整 blob。直接读回半截写缓存并回放（X-Cache: RESUME），
// 不再全量下载。读回失败降级既有全量回源兜底（半截保留）。
func (s *Server) serveResumeComplete(w http.ResponseWriter, r *http.Request, key, target, digest string, ent *prefetchStreamEntry, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry)) (*cache.Entry, bool, error) {
	labels := parseDockerLogInfo(target) // 日志统一标签（执行方日志统一 source=resume）
	data, rerr := os.ReadFile(disk.PartPath(key))
	if rerr != nil {
		// 广播搭车订阅者降级后走既有全量回源兜底（半截保留；commit 前未向客户端写过字节）
		cerr := fmt.Errorf("读回完整半截失败: %w", rerr)
		ent.finishFailed(cerr)
		s.log.Warn("blob 半截已完整但读回失败，降级既有全量回源",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "err", rerr)...)
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}
	entry := &cache.Entry{
		Status:   http.StatusOK,
		Header:   http.Header{},
		Body:     data,
		StoredAt: time.Now(),
		Source:   "part-resume", // 观测：内容来自半截文件整体回放（digest 寻址语义可信）
	}
	// 续传执行方写缓存：直接调 finalizeFetchEntry 并标注 source=resume（不经注入的
	// finalize 回调——该回调按客户端主路径标注 source=client，此处写者实为续传执行方；
	// blob 落盘成功即删半截见 finalizeFetchEntry 钩子）
	s.finalizeFetchEntry(r, key, target, entry, eff, disk, dockerLogSourceResume)
	ent.finishDone()
	s.log.Info("blob 半截续传完成（半截已完整，整体回放写缓存）",
		labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "bytes", len(data),
			"cost", time.Since(start).Round(time.Millisecond).String())...)
	s.log.Info("代理请求完成",
		labels.attrs(dockerLogSourceResume,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", http.StatusOK,
			"cache", resumeCacheStatus,
			"upstream", entry.Source,
			"rule", eff.Rule,
			"bytes", len(data),
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
	s.serveEntry(w, r, entry, resumeCacheStatus, eff, start)
	return entry, true, nil
}

// commitResumeHeader 续传执行方对客户端的 commit 点：对客户端回一次完整 200（客户端
// 无需支持 Range）——沿用 skipResponseHeader 过滤逐跳头与自管头（上游 206 的
// Content-Range 已由调用方在 hdr 中剔除；Content-Length 在过滤名单内由下方按总长补回），
// X-Cache 用 RESUME 观测值、X-Proxy-Upstream 观测来源候选。调用后响应头固化，后续失败
// 只能截断本次响应（docker 以 Range 重试，从更长前缀续传，进度不回退）。
func (s *Server) commitResumeHeader(w http.ResponseWriter, hdr http.Header, source string, total int64) {
	h := w.Header()
	for k, vv := range hdr {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	h.Set("X-Cache", resumeCacheStatus)
	if source != "" {
		h.Set("X-Proxy-Upstream", source) // 观测续传命中的候选 URL
	}
	if total > 0 {
		h.Set("Content-Length", strconv.FormatInt(total, 10)) // 一次完整 200 的总长
	}
	w.WriteHeader(http.StatusOK)
}

// //////////////////  客户端请求入口  //////////////////////////////////////////////////////////

// tryServeBlobResume 客户端 blob GET 缓存 MISS 后的「半截续传」入口（handleGet 在在途
// 预取搭车检查之后调用；调用位置保证：路径目标无 Range 完整 GET、缓存可读、正式缓存
// MISS）。返回 handled=true 表示本次请求已由续传路径处理（含内部降级后的既有全量
// 回源），调用方直接返回；false = 续传不适用（非 blob / 开关关闭 / 无半截文件 / 半截
// 为空），行为与旧版完全一致。
//
// 时序（全部编排在同 key 既有 flightGroup 上——谁先到谁执行、其余等待共享结果，与
// 预取/客户端回源的合并语义一致，上游对该 blob 只有一路下载）：
//  1. 前置检查：blob 语义、全局开关、半截文件存在且非空（空半截立即丢弃）；
//  2. flight.DoLimit：首个调用方成为续传执行方（resumeBlobStreaming），其余同 key
//     请求成为等待方（等待上限 flightWaitLimit，与既有流式回源一致）；
//  3. 等待方超限：先查续传 hub 条目搭车（tryServeFromPrefetchHub，前缀+增量拼接，
//     X-Cache: PREFETCH——续传条目在执行方扫描候选前已登记，搭车窗口覆盖整个续传
//     过程）；搭不上（执行方已失败移除）再走既有全量回源兜底（失败捕获会把更长前缀
//     存回半截，进度不回退）。
func (s *Server) tryServeBlobResume(w http.ResponseWriter, r *http.Request, key, target string, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry), streamingError func(error, bool)) bool {
	if disk == nil {
		return false // 防御：缓存不可读时不适用（handleGet 已分流）
	}
	if s.cfg == nil || !s.cfg.GlobalDockerPartialResumeEnabled() {
		return false // 全局开关关闭：行为与旧版完全一致
	}
	digest, ok := parseDockerBlobTarget(target)
	if !ok {
		return false // 非 blob（manifest 会变等）：半截保留与续传仅对 blob 启用
	}
	if n, pok := disk.PartSize(key); !pok || n <= 0 {
		if pok {
			// 空半截（0 字节）：立即丢弃（无续传价值），走既有全量回源
			disk.RemovePart(key)
		}
		return false
	}

	e, executed, err := s.flight.DoLimit(key, flightWaitLimit, func() (*cache.Entry, error) {
		ent, committed, ferr := s.resumeBlobStreaming(w, r, key, target, digest, eff, disk, start, finalize)
		if ferr != nil && committed {
			// 已 commit 后失败：响应头已固化，标记与既有流式执行方同款（不得再 writeError）
			return nil, fmt.Errorf("%w: %v", errStreamingCommitted, ferr)
		}
		return ent, ferr
	})
	switch {
	case errors.Is(err, errFlightWaitTimeout):
		// 等待续传执行方超限：先查续传 hub 条目搭车（条目在执行方扫描候选前已登记，
		// 覆盖整个续传窗口）；搭不上（执行方已失败移除）走既有全量回源兜底
		if s.tryServeFromPrefetchHub(w, r, key, target, eff, disk, start) {
			return true
		}
		s.log.Warn("等待同 blob 半截续传超限且续传已结束，自行走既有全量回源",
			// 本请求自行回源：按客户端请求标注 source=client（非续传执行方）
			parseDockerLogInfo(target).attrs(dockerLogSourceClient,
				"rule", eff.Rule, "key", key, "wait", flightWaitLimit.String(), "target", truncate(target, 300))...)
		_, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
		streamingError(ferr, committed)
	case errors.Is(err, errStreamingCommitted):
		// 本调用是续传执行方且 commit 后失败：底层原因已留痕（读上游失败=Error、客户端
		// 断开=Warn 运维事件），响应头已写出（本次响应被截断，docker 会以 Range 断点续传
		// 重试，下次从更长前缀续传——进度不回退）
	case err != nil:
		// 未 commit 失败：走 streamingError 一致出口（上游明确 4xx 透传 / 其余 502）
		streamingError(err, false)
	case executed:
		// 本调用是续传执行方：响应已直接写给客户端（commit 200 + 拼接回放），无需再回放
	default:
		// 等待方共享到执行方的完整结果：缓存此刻已就绪，按 RESUME 语义回放
		if e != nil {
			s.serveEntry(w, r, e, resumeCacheStatus, eff, start)
			return true
		}
		return false // 防御：无结果时走既有路径
	}
	return true
}
