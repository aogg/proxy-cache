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
//   - 立即响应（首字节时序优化，2026-11 现场「docker pull 长时间零进度」的根因修复）：
//     响应头不再等待候选扫描命中（旧时序里扫描窗口可达 60s+，docker 客户端等响应头约
//     30s 就超时断开）——登记续传在途条目后立即合成响应头 commit 并回放本地半截前缀
//     （总长取 manifest 索引 blob_size_index.go，未知则 chunked），docker 毫秒级看到
//     响应头与字节进度；上游增量仍必须等既有安全校验（206 且 Content-Range 起点匹配）
//     通过后才拼接。扫描失败（含上游明确拒绝 Range）时按「安全中断」截断本次响应——
//     已发出的前缀字节是合法内容前缀、绝不错位，docker 以 Range/重试接续（无半截走
//     干净全量回源、有半截从更长前缀再续），数据永远正确；搭车订阅者同样立即响应
//     （快照未就绪即合成头 commit，见 prefetch_hub.go 的 tryServeFromPrefetchHub）；
//   - 并发合并：续传执行方登记与预取同一套在途流共享注册表（prefetchStreamHub 的
//     resume 条目），其余客户端请求/等待超限的重试请求经既有搭车机制（tryServeFromPrefetchHub）
//     订阅「半截前缀 + 实时增量」，X-Cache: PREFETCH；同 blob 的预取任务经既有
//     flightGroup（同缓存键）合并到同一路续传流——上游对该 blob 只有一路 Range 续传下载；
//   - 失败语义：续传中途失败时保留更新后的半截文件（已追加到最新前缀），客户端本次响应
//     被截断——docker 会重试，下次从更长前缀接着续传（进度不回退）；
//   - 宽限终止（仅续传执行方）：续传中客户端断开（写响应体失败）且 hub 条目无搭车订阅者
//     时，按 docker-resume-client-gone-grace（默认 2s，<=0 关闭恢复旧语义）宽限计时——
//     到期仍无订阅者则终止上游读取（errResumeGraceAborted：从计时器 goroutine 关闭上游
//     body 让阻塞的 Read 返回）、保留半截（进度不回退）、finishFailed 移除在途条目，
//     不再「继续读完只填缓存」长时间占用上游带宽（现场 261MB blob 断开后旧语义又拉了
//     3m44s 才写完）；宽限期内有新订阅者搭车（subscribe）则放弃终止维持现状继续拉，
//     断开时已有订阅者直接不宽限。语义分工必须遵守：docker 客户端驱动的链路
//     （source=client / ridealong / resume）客户端没了就该停——主路径是断开即停
//     （b251149，立即终止）、续传执行方是宽限终止（给搭车订阅者留上车窗口）；搭车读侧
//     断开仅停读取订阅；异步预取（source=prefetch）没有客户端概念，保持「继续填缓存」
//     语义不变，绝不改。
//
// 安全依据与边界：
//   - blob 按 digest 寻址、内容不可变：任意候选源的同 digest 内容一致，续传可从任意
//     候选发 `Range: bytes=<n>-` 拼到本地前缀后面（这是半截可跨候选拼接的安全前提）；
//   - 客户端/上游侧流式传输不做逐块摘要校验（对 GB 级 blob 成本过高）：信任 digest
//     寻址语义（前缀 + 断点剩余拼出的即完整 blob），与「206 透传不校验」的既有取舍一致；
//     但写缓存前由 finalizeFetchEntry 统一做 sha256 强校验（防毒缓存兜底）——2026-11-07
//     现场：毒半截（前缀混入异常镜像源的错误字节）被固化为正式缓存后客户端每次 pull 都
//     确定性失败，故校验失败拒绝入库并丢弃毒半截（下次全量回源自愈）；
//   - 上游数据写客户端的前置校验绝不放宽：206 且 Content-Range 起点匹配半截长度确认前，
//     绝不把上游字节写给客户端——若上游对 Range 回 200 全量响应，把本地前缀与全量响应
//     拼在一起会数据错位，非 206 一律安全降级。立即响应只把**本地半截前缀**（同 digest
//     内容的合法前缀，内容永远可信）先发给客户端；扫描失败/非 206 时响应已 commit 的
//     按「安全中断」截断（docker 重试接续，绝不在已回放前缀的流上拼全量），未 commit
//     的（旧时序）维持既有降级全量回源不变。2026-11 现场 767MB blob 拼接后 sha256 不符
//     的对策再加两道内容级校验（只加严不放宽）：续传响应 Content-Length 与 Content-Range
//     区间长度一致性核对（validateResumeResponse）；多源补充源的重复区间字节与已确认
//     输出同偏移对账（slowMerger.verify → verifyStaleAgainstConfirmed），不符即剪枝——
//     谎报起点的源的错位字节进不了半截文件、广播与客户端流；
//   - 半截为 0 字节直接丢弃；上游 416（起点 ≥ 总长，而半截是合法前缀）证明半截已完整，
//     直接读回写缓存回放，不再全量下载；
//   - 客户端带 Range 的请求按起点与半截长度的关系分流（见 client_range.go）：
//     起点 0（`bytes=0-`）等价于完整 GET——handleGet 入口剥离 Range 后按完整 GET 处理，
//     半截照常保留、照常无感续传（docker/containerd 首次拉 blob 就带它）；
//     起点 >0 且 起点 <= 半截长度（无缺口）——复用本文件的续传执行方：从半截的起点
//     位置回放 + 上游剩余区间续传拼接，对客户端回 206（内部仍按完整 blob 拼接并写缓存）；
//     起点 >0 但 起点 > 半截长度（中间缺一段，不能凭空补）——维持旧版 206 透传，只打
//     Warn 说明缺口大小；
//     no-cache / 关缓存等 BYPASS 请求维持既有行为；
//   - 续传路径**接入**时间驱动的多源并发补充（与客户端主路径同一套
//     判定/合并器，见 slowdownload.go 的 timeBlobSupplement）：续传执行方每满
//     blob-concurrency-interval 且该层未完成，就向尚未加入的候选源补充并发 Range 续传
//     （只做加法、客户端无感、不切状态码）；判速触发（slow-download-speed）在续传路径
//     同样生效。此前本路径「不叠加 slowdownload 升级」，现场故障（1.32GiB 单层断流后走
//     续传）因此仍然只有单路下载。
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

// errBlobRangeUnavailable 客户端区间请求（起点 >0 的 blob Range）无法由本地半截续传满足：
// 典型是上游 206 没带可解析的 Content-Range（对象总长未知）——此时给不出合法的
// Content-Range / Content-Length，绝不 commit（半截前缀长度 ≠ 对象总长）。此刻未向客户端
// 写出任何字节，调用方按旧版 206 透传兜底（语义与旧版完全一致）。
var errBlobRangeUnavailable = errors.New("本地半截无法满足客户端区间请求")

// errResumeGraceAborted 续传宽限终止哨兵（errors.Is 判定）：客户端断开（写响应体失败）
// 且宽限期内无搭车订阅者，宽限到期主动终止上游读取——正常运维事件而非故障（见
// proxy.go streamingError 的低调出口：绝不 502 / 不再向客户端写任何字节，不产生
// Error 级日志）；半截保留（进度不回退）、hub 条目 finishFailed 移除，docker 重试
// 将从当前断点再续。
var errResumeGraceAborted = errors.New("blob 半截续传宽限终止（客户端断开且宽限期内无搭车订阅者）")

// resumeScanBudgetFor 续传候选扫描的整体预算（全部候选轮询完的上限，到期按超时类失败
// 收场并降级既有全量回源；半截保留不受影响）。预算只约束「选源」（探测/校验候选直到
// 首个合规 206 / 416），命中后的活体续传流不受该预算约束——见 scanResumeCandidate 的
// AfterFunc 门控实现。续传只服务 docker blob，故按 blob 的
// 放大预算算（candidates.go 的 blobCandidateScanBudget：60s 起，随候选数×hedge 与候选
// 探测预算动态放大）——旧的固定 20s 会在「7 个候选 × 慢上游」的现场场景里，于某个慢
// 候选的 206 到手之前就退出扫描（随后降级全量回源 = 又从头下一遍，等于半截白留）。
func resumeScanBudgetFor(n int, eff config.Options) time.Duration {
	return blobCandidateScanBudget(n, eff)
}

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
// 的「commit 后读上游失败」与「客户端断开终止」出口调用）：仅 blob 目标 + 全局开关开启时
// 保留前缀；manifest 会变不适用（仅 blob 启用）。带 Range 的请求原则上不保留（它的语义是
// 「我要某一段」，与「整 blob 前缀」不同）——唯一例外是「docker blob 目标 + Range 起点 0」：
// 它等价于完整 GET（见 client_range.go），断在半路的字节就是合法的整 blob 前缀，必须保留，
// 否则下次重试又从 0 开始（现场 302MB 层白扔 30MB 的根因之一）。写入按进度不回退规则
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
		// 防御：带 Range 的请求默认不保留半截（入口分流本不会给它们注入 sink）。
		// 例外 = blob 目标 + 起点 0（等价完整 GET）：保留半截，下次重试从断点无感续传。
		// 注：正常链路里 handleGet 已在入口把这种 Range 从请求头剥离（内部按完整 GET 回源），
		// 此分支是不依赖剥离顺序的兜底（请求对象被直接构造 / 未来新增调用方时同样正确）。
		if _, ok := parseZeroRangeBlob(r, target); !ok {
			return false
		}
	}
	// 客户端主路径半截保留：解析镜像名与 digest 作为日志统一标签（source=client）
	name, digest, ok := parseDockerBlobNameDigest(target)
	if !ok {
		return false // 非 blob（manifest 会变等）：不适用半截保留
	}
	return s.savePartialPart(sink.disk, sink.key, data, dockerLogInfo{image: name, digest: digest})
}

// partIdleSince 半截文件自上次写盘（= 上次下载中断时刻）至今的间隔（「隔离时长」）：
// 断开续传提醒与续传开始日志用它回答「客户端隔了多久才回来续传」。半截文件只在追加写
// 时更新 mtime，下载中断后不再变动，因此跨进程重启依然准确；搭车/预取路径留下的半截
// 同样适用（语义泛化为「上次下载中断至今的间隔」）。stat 失败（半截已被并发流程消费/
// 删除）返回 0，调用方按「无法观测」省略字段，绝不影响主流程。
func partIdleSince(disk *cache.Cache, key string) time.Duration {
	st, err := os.Stat(disk.PartPath(key))
	if err != nil {
		return 0
	}
	return time.Since(st.ModTime())
}

// parseContentRangeStarTotal 解析 416 响应的 `Content-Range: bytes */<total>`
// （RFC 7233：对不满足的单区间请求，416 必带此头，total 即对象总长）。常规区间头
// （`bytes <start>-<end>/<total>`）不走这里——parseContentRange 已覆盖；416 的区间
// 段恒为 `*`，通用解析器对它返回 ok=false，故需要本专用解析。格式不符、total 为
// "*" 或非法（≤0）时 ok=false。
func parseContentRangeStarTotal(v string) (int64, bool) {
	v = strings.TrimSpace(v)
	// 去掉 "bytes" scheme 前缀（scheme 名不参与判定）：容忍 "bytes *" 与 "bytes*" 两种写法
	if i := strings.IndexByte(v, ' '); i >= 0 {
		v = strings.TrimSpace(v[i+1:])
	} else if len(v) >= 5 && strings.EqualFold(v[:5], "bytes") {
		v = strings.TrimSpace(v[5:])
	}
	rangePart, totalPart, found := strings.Cut(v, "/")
	if !found || strings.TrimSpace(rangePart) != "*" {
		return 0, false
	}
	t, err := strconv.ParseInt(strings.TrimSpace(totalPart), 10, 64)
	if err != nil || t <= 0 {
		return 0, false
	}
	return t, true
}

// //////////////////  续传候选扫描  ////////////////////////////////////////////////////////////

// scanResumeCandidate 续传候选轮询扫描：按 round-robin 起始下标（共用 s.rr 计数器，
// 与既有候选管线同一负载轮询语义）逐个候选发 `Range: bytes=<partSize>-`（复用
// startUpstream 公共底层：候选级超时覆盖 / http-proxy / insecure TLS / 401 匿名
// token dance 全复用），整体受 resumeScanBudgetFor（blob 放大预算）兜底——预算只
// 约束「选源」阶段，命中后的活体续传流不受该预算约束（AfterFunc 门控，见实现内
// 注释）。逐候选失败不中断轮询（换下一
// 候选重试语义与既有管线一致）。返回值：
//   - res：首个通过校验的 206 探测结果（活体 body 所有权移交调用方；complete=true 时为 nil）
//   - total：对象总长（206 Content-Range 的 total；不可知为 -1，调用方流式不设 Content-Length）
//   - complete：上游回「合规 416」证实半截已完整——416 必须携带可解析的
//     `Content-Range: bytes */<total>` 且 total == 半截长度（RFC 7233 语义；不符即
//     毒缓存风险，按 invalid 处理轮询下一候选，绝不把半截当完整 blob 写缓存）
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
	// 到期按超时类失败收场（半截保留、降级全量回源）。预算只约束「选源」阶段
	//（找到首个合规 206 / 416 为止）——此前用 WithTimeout 把 deadline 直接挂进
	// ctx，命中候选的活体 body 读取也被同一 deadline 约束，现场观测到「206 命中
	// 后正常流式约 2 分钟被预算到期杀掉（context deadline exceeded），半截停更、
	// 客户端响应截断」。故改用 AfterFunc 门控的 cancel：到期且未命中才 cancel
	//（预算耗尽语义不变）；命中出口先 Stop 再核验 ctx.Err()，命中后的活体流
	// 只有「调用方放弃 / 流结束」这一个取消来源。cancel 不走 defer：defer 会在
	// 本函数返回时立即取消 ctx，杀掉命中候选的在途 body 读取——206 响应是活体
	// 流，续传收到首块即 context canceled。取消权随结果移交（与 selectCandidate
	// 的 onClose(cancelBase) 同款）：命中 206 的出口把 cancel 挂 upRes.onClose，
	// 随调用方流结束/放弃的 Close 统一释放；416 / 扫描全败 / 预算耗尽等不返回
	// 活体 body 的出口显式 Stop + cancel()，绝不泄漏。
	scanBudget := resumeScanBudgetFor(n, eff)
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	scanTimer := time.AfterFunc(scanBudget, func() { cancel() }) // 预算到期：未命中才生效（命中出口已 Stop）

	var lastErr error
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break // 扫描预算耗尽：按超时类失败收场
		}
		idx := (start + i) % n
		cand := candidates[idx]
		upRes, uerr := s.startUpstream(ctx, r, cand, eff, rangeHeader, nil, nil)
		if uerr != nil {
			lastErr = uerr
			s.log.Debug("blob 半截续传候选请求失败，轮询下一候选",
				labels.attrs(dockerLogSourceResume, "index", idx, "candidate", truncate(cand.url, 300), "range", rangeHeader, "err", uerr)...)
			continue // 网络/超时类失败：半截保留
		}
		if upRes.status == http.StatusRequestedRangeNotSatisfiable {
			// 416 的可信性校验（防毒缓存）：合规服务端对「起点已达总长」的单区间请求
			// 回 416，且 RFC 7233 要求必带 `Content-Range: bytes */<total>`。半截是同
			// digest 内容的合法前缀（起点 ≤ 总长），只有「总长可解析且 == 半截长度」
			// 才证实「半截已完整」。总长解析不出或 total ≠ 半截长度，都说明该候选的
			// 416 不符合语义（实测 dockerpull.cn 对任意 Range 一律回 416）——绝不能
			// 据此把半截当完整 blob 写缓存：现场曾因此把 12875538 字节的半截固化为
			// 133611458 字节 blob 的「正式缓存」，此后每次 pull 都命中毒缓存报 short
			// read。按「明确拒绝按断点续传」处理：invalid 置位轮询下一候选；全部候选
			// 如此时走既有 invalid 降级（丢弃半截全量回源，进度回零但内容正确）。
			// 注意 416 的 Content-Range 区间段恒为 `*`（parseContentRange 只认
			// start-end 形式、对此返回 ok=false），必须用专用解析器拿总长。
			t416, pok := parseContentRangeStarTotal(upRes.header.Get("Content-Range"))
			if !pok || t416 != partSize {
				upRes.Close()
				invalid = true
				if !pok {
					t416 = -1
				}
				lastErr = fmt.Errorf("候选 416 不可信（Content-Range 总长 %d 缺失或 ≠ 半截长度 %d）", t416, partSize)
				s.log.Debug("blob 半截续传 416 缺合规 Content-Range，不可证实半截完整，轮询下一候选",
					labels.attrs(dockerLogSourceResume, "index", idx,
						"candidate", truncate(cand.url, 300), "part_bytes", partSize,
						"total", t416, "err", lastErr)...)
				continue
			}
			upRes.Close()
			scanTimer.Stop() // 命中出口（完整收场）：停掉预算 timer，绝不遗留到期触发
			cancel()         // 416 出口不返回活体 body：ctx 就地回收（重复取消幂等无害）
			s.log.Info("blob 半截续传发现半截已完整（上游 416 且 Content-Range 总长与半截一致）",
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
		// 命中出口：先停预算 timer（命中后的活体流不再受扫描预算约束），再核验
		// ctx 是否已在停表前被预算到期击穿（并发窗口：206 头到手瞬间 timer 恰好
		// 触发）——被击穿则本候选按超时类失败收场（活体 body 一读即挂，绝不能移交
		// 调用方），循环顶的 ctx.Err() 检查随后按预算耗尽收场
		scanTimer.Stop()
		if ctx.Err() != nil {
			upRes.Close()
			lastErr = errors.New("续传扫描预算到期（206 命中窗口内被预算终止）")
			continue
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
	scanTimer.Stop() // 收场出口（全败/预算耗尽/不合规轮完）：停掉预算 timer（幂等）
	cancel()         // 扫描收场（全败/预算耗尽/不合规轮完）：本出口无活体 body，ctx 就地回收
	return nil, -1, false, invalid, lastErr
}

// //////////////////  续传执行方  //////////////////////////////////////////////////////////////

// resumeBlobStreaming blob 半截续传执行方（tryServeBlobResume 经既有 flightGroup 编排，
// 同 key 只有一个执行方）：
//  1. 重查半截前缀（前置检查后可能已被并发流程消费，竞态按既有全量回源兜底）；
//  2. 登记「续传在途条目」（prefetchStreamHub 的 resume 条目，written 预置为半截前缀
//     长度）——此后其余客户端请求即可经既有搭车机制订阅前缀+增量（上游仅此一路下载）；
//  3. 立即响应（首字节时序优化）：登记条目后立即合成响应头并 commit——不再等待上游
//     206 确认（旧时序里 commit 被候选扫描窗口拖住数十秒，docker 客户端等响应头约 30s
//     就超时断开，pull 输出长时间零进度的根因）。总长已知（manifest 索引，见
//     blob_size_index.go）时 commit 200 + Content-Length=总长（cr 非空为 206 + 区间头），
//     未知则不设（chunked）；随后立即回放本地半截前缀——本地前缀内容永远可信，先发给
//     客户端是安全的，docker 毫秒级看到响应头与字节进度；
//  4. 候选轮询 Range 续传扫描（scanResumeCandidate）：全部失败时——立即响应路径响应头
//     已 commit，按「安全中断」收场（半截保留/丢弃规则不变，docker 以重试接续；绝不在
//     已回放前缀的流上降级全量回源，从 0 的全量与已发前缀拼接必然错位）；旧时序路径
//     （区间请求且总长未知，未 commit）维持既有降级全量回源不变；
//  5. 扫描发现半截已完整（上游合规 416）：立即响应路径直接缓存收尾（前缀已回放完毕）；
//     旧时序路径读回半截写缓存并回放（区间请求按区间切片回 206），不再全量下载；
//  6. 206 且起点匹配（上游增量拼接的强制前置校验，绝不放宽）：旧时序路径此时才合成
//     响应头（沿用上游 206 的 Content-Type / Docker-Content-Digest 等，剔除
//     Content-Range、Content-Length 重置为总长）并 commit + 回放前缀；两路时序随后一致
//     ——流式读取上游剩余区间：逐块「追加写半截文件 → 广播增量给搭车订阅者 → 写客户端
//     与累积缓冲」（先磁盘后推进 written，是搭车订阅者读前缀的既有契约）；
//  7. EOF 且累积长度与总长一致：半截内容 = 完整 blob，复用 finalize（既有 cache.Set
//     语义）原子写缓存（blob 落盘成功即删半截，见 finalizeFetchEntry 钩子），置 done
//     关闭订阅（搭车方收完缓冲内增量即完整）；
//  8. 任何失败：保留更新后的半截文件、广播 failed 并移除条目、本次响应被截断（docker
//     以 Range 断点续传重试，下次从更长前缀接着续传——进度不回退）。其中「客户端断开
//     且无搭车订阅者」在宽限配置 >0 时经宽限计时后主动终止（宽限终止，errResumeGraceAborted，
//     Info 级运维事件；<=0 或宽限期内订阅者加入则维持「继续读完填缓存」）——见文件头
//     「宽限终止」与本函数内的宽限状态块；
//
// 可选参数 cr（variadic，既有调用方不受影响）：客户端区间请求（起点 >0 的 blob Range，
// 见 client_range.go）。非空时对客户端回 206 且只发区间内字节，内部仍按完整 blob 拼接
// 并照常写缓存（缓存条目必须是完整 blob，后续任意起点才能直接本地切片秒回）；上游 206
// 拿不到对象总长时绝不 commit（半截前缀长度不等于总长），返回 errBlobRangeUnavailable
// 让调用方走旧版 206 透传。
//
// 返回值语义与 fetchViaCandidatesStreaming 一致：committed=true 表示已向客户端写出
// 响应头（调用方不得再 writeError）。多源并发补充：时间驱动（blob-concurrency-interval）
// 每满间隔向尚未加入的候选源增量补充并发 Range 续传（复用同一 slowMerger，客户端仍是
// 一条连续的流）；判速触发（slow-download-speed）同样生效——两者共用「加入新源」逻辑。
func (s *Server) resumeBlobStreaming(w http.ResponseWriter, r *http.Request, key, target, digest string, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry), cr ...blobRangeSpec) (*cache.Entry, bool, error) {
	// docker 日志统一标签：续传请求必然是 /v2/ blob 目标，按需解析 image/digest
	//（本函数全部执行方日志统一 source=resume，见 log_attrs.go）
	labels := parseDockerLogInfo(target)
	// 客户端区间窗口（缺省 = 旧版完整 200 语义）：clientStart 之前的字节跳过、clientLast
	// 之后的字节不再写给客户端；两者都不影响内部累积（accum / 半截文件始终是完整 blob）
	hasRange := len(cr) > 0
	var spec blobRangeSpec
	clientStart, clientLast := int64(0), int64(-1)
	if hasRange {
		spec = cr[0]
		clientStart = spec.start
		if spec.hasEnd {
			clientLast = spec.end
		}
	}
	// 重查半截前缀：前置检查与成为执行方之间可能被并发流程消费（前一个执行方完成
	// 写缓存并删半截等），竞态按既有全量回源兜底（行为与旧版一致）
	n, ok := disk.PartSize(key)
	if !ok || n <= 0 {
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}

	// 断开续传提醒（观测需求：客户端断开后再来续传时，日志要一眼可辨「这是断点续传」，
	// 且写明「隔离了多久」——上次中断到本次重试的间隔）。半截存在才会走到这里，故本
	// 提醒即「续传方回场」事件本身；间隔取半截文件 mtime（见 partIdleSince），≥1s 才
	// 输出（同秒内的并发重试没有提醒价值）。
	isolatedGap := partIdleSince(disk, key)
	if isolatedGap >= time.Second {
		s.log.Info("blob 断点续传提醒：上次下载中断后客户端重新请求（半截保留中，本次将从断点无感续传）",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
				"part_bytes", n, "isolated_gap", isolatedGap.Round(time.Millisecond).String())...)
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

	// 宽限终止状态（graceMu 保护，语义分工见文件头；自原 commit 后位置上移——立即响应
	// 路径在候选扫描前就要写客户端，writeClient 的断开检测/宽限计时必须先于扫描就绪）：
	// 客户端写失败（docker 已断开）且 hub 条目无搭车订阅者时启动宽限计时器——到期仍无
	// 订阅者则置位 graceAborted 并从计时器 goroutine 关闭上游 body（让阻塞在 Read 上的
	// 读循环立即返回错误终止读取，读循环查 graceAborted 按宽限终止收场）；宽限期内有
	// 订阅者加入则放弃终止维持现状。与 EOF 的竞态以先到者为准：EOF 先到则 break 出循环
	// 经 stopGrace 停表正常写缓存（计时器已停；恰已触发的回调最多做一次幂等 upRes.Close，
	// 无害），宽限先到则读循环按 errResumeGraceAborted 收场——两条路的半截（只增不减）
	// 与缓存（仅完整 EOF 后写入）数据都一致。
	graceLimit := config.DefaultDockerResumeClientGoneGrace
	if s.cfg != nil {
		graceLimit = s.cfg.GlobalDockerResumeClientGoneGrace()
	}
	var (
		graceMu      sync.Mutex
		graceTimer   *time.Timer
		graceAborted bool
		// graceMerger 宽限到期时要一并终止的多源合并器（时间驱动补充启用后非 nil）：
		// 由续传读循环在升级时登记（graceMu 保护），宽限到期回调据此逐路关闭全部上游源
		//（客户端已断开又无人搭车时，多源并发的每一路都不该继续占带宽）。
		graceMerger *slowMerger

		upRes    *probeResult // 扫描命中后的活体上游（onGraceExpired 引用，先声明后赋值）
		total    int64        // 上游确认的对象总长（206 Content-Range；不可知为 -1）
		complete bool         // 扫描发现半截已完整（上游合规 416）
		invalid  bool         // 存在「上游明确拒绝按断点续传」的候选
		serr     error        // 扫描失败（全部候选出结果且无命中）
	)
	// stopGrace 停掉宽限计时器（幂等）：读循环与 fail 的每个收场出口调用，防止计时器
	// 在收场后误触发，timer goroutine 不泄漏。
	stopGrace := func() {
		graceMu.Lock()
		defer graceMu.Unlock()
		if graceTimer != nil {
			graceTimer.Stop()
			graceTimer = nil
		}
	}
	// onGraceExpired 宽限到期回调（计时器 goroutine）：到期复查订阅者数——仍无订阅者
	// 才终止（宽限期内 subscribe() 加入即放弃终止，等价取消计时，订阅者还要收增量）；
	// 置位后关闭上游 body，阻塞中的上游 Read 立即返回。upRes.Close 幂等，与本函数、
	// 读循环收场出口的 Close 竞态安全；绝不在此向客户端写任何字节（客户端已断开）。
	onGraceExpired := func() {
		graceMu.Lock()
		if graceAborted {
			graceMu.Unlock()
			return // 防御：Stop 与触发的竞态窗口，已终止过则幂等返回
		}
		if ent.subscriberCount() > 0 {
			graceMu.Unlock()
			return // 宽限期内来了订阅者：放弃终止，继续拉（订阅者还要收增量）
		}
		graceAborted = true
		merger := graceMerger // 锁内取：续传读循环升级时在同一把锁下登记
		graceMu.Unlock()
		// 多源并发补充过：宽限到期必须逐路关闭全部上游源（含尚未定局的 pending 续传源）
		if merger != nil {
			merger.abortAll("续传客户端断开且宽限期内无搭车订阅者，终止全部上游源")
		}
		upRes.Close()
	}

	// 累积缓冲与客户端写窗口（自原 prefix 回放处上移，供立即响应路径在扫描前回放前缀）：
	// accum 始终累积完整 blob（缓存条目必须完整），writeClient 按客户端区间窗口裁剪
	//（起点之前的字节跳过、终点之后的字节不再写；完整 GET 时窗口 = [0, +∞) 等价于
	// 原来的 w.Write(p)）。
	accum := bytes.NewBuffer(make([]byte, 0, int(n)+streamChunkSize))
	clientOK := true // 客户端写失败（docker 已断开）后置 false：按宽限语义处置（见写失败分支）
	writeClient := func(off int64, p []byte) {
		if !clientOK || len(p) == 0 {
			return
		}
		lo, hi := off, off+int64(len(p)) // 该块在完整 blob 中的半开区间 [lo, hi)
		if lo < clientStart {
			lo = clientStart
		}
		if clientLast >= 0 && hi > clientLast+1 {
			hi = clientLast + 1
		}
		if hi <= lo {
			return // 整块都在客户端区间之外：只进缓存/半截，不写客户端
		}
		if _, werr := w.Write(p[lo-off : hi-off]); werr != nil {
			clientOK = false
			// 客户端断开（docker 已断）的宽限终止判定（本闭包此后不再进入，仅此一次）：
			//   - hub 条目已有搭车订阅者：不宽限，维持现状继续拉（订阅者还要收增量）；
			//   - 宽限配置 <=0（graceLimit<=0）：关闭宽限终止，维持旧版「继续读完只填
			//     缓存」语义不变；
			//   - 无订阅者且宽限 >0：启动宽限计时器——到期仍无订阅者则终止上游读取
			//    （errResumeGraceAborted，见读循环的宽限终止出口；预取路径与搭车读侧
			//     不受影响），宽限期内有订阅者加入则到点自动放弃（见 onGraceExpired）。
			subs := ent.subscriberCount()
			if graceLimit > 0 && subs == 0 {
				graceMu.Lock()
				if graceTimer == nil { // 防御：仅启动一次（clientOK 已拦住重复进入）
					graceTimer = time.AfterFunc(graceLimit, onGraceExpired)
				}
				graceMu.Unlock()
			}
			s.log.Warn("写响应体失败（客户端已断开，续传按宽限语义处置）",
				labels.attrs(dockerLogSourceResume, "target", truncate(r.URL.Path, 200),
					"grace", graceLimit.String(), "subscribers", subs, "err", werr)...)
		}
	}
	// replayPrefix 读回半截前缀并写入累积缓冲与客户端（写锁内整读、锁外写客户端——慢
	// 客户端不拖住写锁；磁盘顺序读，锁持有时间 = 磁盘读耗时）。前缀字节即同 digest 内容
	// 前缀（digest 寻址不可变），先发给客户端是安全的；与并发保存方的整体重写互斥，
	// 保证读到的每个字节来自同一份合法前缀。短于注册快照按失败终止（绝不给客户端
	// 「短前缀 + 从 n 起的上游剩余」的错位拼接）。
	replayPrefix := func() error {
		mu := s.partWriter(key)
		mu.Lock()
		prefix, rerr := os.ReadFile(disk.PartPath(key))
		mu.Unlock()
		if rerr != nil {
			return fmt.Errorf("读半截文件失败: %w", rerr)
		}
		if int64(len(prefix)) < n {
			return fmt.Errorf("半截文件短于注册快照（实读 %d / 期望 %d 字节），终止本次续传", len(prefix), n)
		}
		accum.Write(prefix[:n])
		writeClient(0, prefix[:n])
		return nil
	}

	// //////////////////  立即响应（首字节时序优化）  /////////////////////////////////////////
	// 2026-11 现场：docker pull 长时间零进度的根因——旧时序里客户端响应头被推迟到候选
	// 扫描命中之后（scanResumeCandidate 逐候选探测 + 401 token dance，预算 60s 起步），
	// 而 docker 客户端等响应头约 30s 就超时断开，于是「反复重试 → 每次都等扫描 → 永远
	// 见不到进度」。现在登记条目后立即合成响应头并 commit（早于上游确认）：总长已知
	//（manifest 索引，blob_size_index.go）时设 Content-Length=总长（cr 非空为 206 +
	// 区间头），未知则不设（chunked）；commit 后立即回放本地半截前缀——本地前缀内容永远
	// 可信，先发给客户端是安全的，docker 毫秒级看到响应头与字节进度。上游增量仍必须等
	// 既有安全校验（206 且 Content-Range 起点匹配）通过后才拼接，该语义绝不放宽。
	// cr 非空（客户端区间请求）必须有总长才能给出合法的 206 头：总长未知时维持旧时序
	//（先扫描后 commit，扫描后仍无总长按 errBlobRangeUnavailable 走旧版 206 透传）。
	earlyTotal := int64(0)
	if v, ok := s.manifestBlobSize(digest); ok && v > n {
		earlyTotal = v
	}
	earlyCommit := !hasRange || earlyTotal > 0
	var hdrEarly http.Header
	// prog 活跃下载进度登记（周期汇总，见 progress_summary.go）：commit 后开始写即登记
	// source=resume，初始已下载字节 = 半截前缀 n（进度不回退，percent 直接对总长），defer
	// 注销——失败/成功/panic 返回都不留幽灵条目；每追加一块经 addBytes 累加
	var prog *progressSummaryEntry
	if earlyCommit {
		hdrEarly = resumeEarlyHeader(digest, earlyTotal)
		// 搭车订阅者立即拿到同一份响应头快照（不再等扫描完成才 setSnapshot）
		ent.setSnapshot(http.StatusOK, hdrEarly, "")
		s.commitResumeHeader(w, hdrEarly, "", earlyTotal, cr...)
		prog = s.progressRegister(dockerLogSourceResume, labels.image, labels.digest, r.Method, n, earlyTotal)
		defer prog.unregister()
		earlySource := blobTotalSourceManifest
		if earlyTotal <= 0 {
			earlySource = blobTotalSourceUnknown // 不设 Content-Length，Go 按 chunked 传输
		}
		s.log.Info("blob 半截续传立即响应（先回放半截前缀，响应头不再等待上游 206 确认）",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n,
				"total", earlyTotal, "total_source", earlySource, "has_range", hasRange)...)
		if rerr := replayPrefix(); rerr != nil {
			stopGrace()
			ent.finishFailed(rerr)
			s.log.Warn("blob 半截续传立即响应读半截失败，终止本次响应（半截保留，docker 重试再续）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n, "err", rerr)...)
			return nil, true, rerr
		}
		// 显式冲刷：响应头 + 已回放前缀立即落网（前缀可能小于服务端缓冲，不冲刷会在
		// 候选扫描期间滞留，客户端拿不到首字节）
		flushClient(w)
		if !clientOK && ent.subscriberCount() == 0 {
			// 客户端在回放前缀时已断开且无搭车订阅者：尚未开始任何上游读取，宽限终止
			// 的意义（给搭车订阅者留上车窗口 + 终止在途上游读取）均不成立——立即按失败
			// 收场移除条目（半截保留，进度不回退），docker 重试时重新成为执行方
			stopGrace()
			ferr := fmt.Errorf("%w: 立即响应回放前缀失败（客户端已断开）", errClientDisconnected)
			ent.finishFailed(ferr)
			s.log.Info("blob 半截续传立即响应阶段客户端已断开且无搭车订阅者，终止（半截保留，docker 重试再续）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n)...)
			return nil, true, ferr
		}
	}

	// 候选轮询 Range 续传扫描：立即响应路径的 commit 与前缀回放已在上方完成（扫描只
	// 决定「上游增量从哪一路、哪个偏移拼接」）；旧时序路径（区间请求且总长未知）保持
	// commit 前扫描，失败不向客户端写任何字节
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	upRes, total, complete, invalid, serr = s.scanResumeCandidate(r, candidates, eff, n)
	if serr != nil {
		// 广播搭车订阅者收场
		ent.finishFailed(serr)
		if invalid {
			// 上游明确拒绝按断点续传（非 206 / Content-Range 起点不匹配）：丢弃半截
			//（半截对续传已无价值；重试按干净全量回源，进度回零但内容正确）
			disk.RemovePart(key)
		}
		if earlyCommit {
			// 立即响应路径：响应头已 commit、本地前缀已回放——绝不能在已回放前缀的流上
			// 「降级全量回源」（从 0 开始的全量与已发前缀拼接必然错位），按既有「commit
			// 后失败只能截断本次响应」安全中断。已发出的前缀字节是合法内容前缀（绝不
			// 错位），docker 以 Range/重试接续：无半截（invalid 丢弃）走干净全量回源，
			// 有半截（网络/超时保留）从更长前缀再次续传，两条路数据都正确。
			stopGrace()
			s.log.Warn("blob 半截续传候选扫描失败（响应头已先行写出，安全中断本次响应，docker 将重试接续）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n,
					"invalid", invalid, "err", serr)...)
			return nil, true, fmt.Errorf("blob 半截续传候选扫描失败（已 commit，安全中断，docker 将重试）: %w", serr)
		}
		// 旧时序路径（commit 前）：维持既有降级——走既有全量回源兜底
		s.log.Warn("blob 半截续传候选扫描失败，降级既有全量回源",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n,
				"invalid", invalid, "err", serr)...)
		// 网络/超时类失败保留半截：下方全量回源若再失败，其失败捕获会把更长前缀存回
		// 半截（进度不回退）；全量成功则写缓存并删半截，两条路都正确
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}
	if complete {
		// 半截已完整（上游合规 416 证实：Content-Range 总长 == 半截长度）
		if earlyCommit {
			// 立即响应路径：半截前缀已在 commit 后整段回放给客户端，416 只证明「前缀即
			// 完整 blob」——直接走缓存收尾，不再读上游、不再重复回放：
			//   - 总长一致（或未知-chunked）：复用 finalizeFetchEntry 写缓存（blob 落盘
			//     成功即删半截），置 done 关闭订阅，正常收尾；
			//   - 总长不一致（manifest 索引与 416 证实值冲突，极小概率）：缓存仍按摘要
			//     校验结论入库/丢弃，但本次客户端响应的 Content-Length 已按错误总长固化
			//     ——Warn 留痕后按截断收场（docker 重试命中刚写入的缓存自愈）。
			if earlyTotal > 0 && earlyTotal != n {
				s.log.Warn("blob 半截续传 commit 总长与上游 416 证实值不符（响应头已固化，按截断收场，docker 重试走缓存自愈）",
					labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n,
						"commit_total", earlyTotal)...)
			}
			stopGrace()
			entry := &cache.Entry{
				Status:   http.StatusOK,
				Header:   hdrEarly,
				Body:     accum.Bytes(),
				StoredAt: time.Now(),
				Source:   "part-resume", // 观测：内容来自半截文件整体回放（写缓存前由 finalize 强校验摘要）
			}
			// 续传执行方写缓存：直接调 finalizeFetchEntry 并标注 source=resume（不经注入的
			// finalize 回调——该回调按客户端主路径标注 source=client，此处写者实为续传执行方）
			if cached := s.finalizeFetchEntry(r, key, target, entry, eff, disk, dockerLogSourceResume); !cached {
				// 半截整体回放被拒入库（防毒缓存守卫 / 写盘失败）：前缀字节已发给客户端
				//（docker 将因摘要不符重试；守卫拒绝时毒半截已随 finalize 丢弃，重试走
				// 干净全量回源自愈），hub 条目按 failed 收场——绝不广播 done
				ferr := fmt.Errorf("blob 半截整体回放未写入缓存（digest %s，摘要校验/守卫拒绝或写盘失败）", digest)
				s.log.Warn("blob 半截已完整但内容未写缓存（防毒缓存守卫拒绝或写盘失败；毒半截已丢弃，docker 重试全量回源）",
					labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "bytes", accum.Len())...)
				ent.finishFailed(ferr)
				return nil, true, ferr
			}
			ent.finishDone()
			// 客户端实际收到的状态码与字节数：区间请求是 206 + 区间长度，完整 GET 是 200 + 总长
			respStatus, sentBytes := http.StatusOK, int64(accum.Len())
			if hasRange {
				respStatus = http.StatusPartialContent
				sentBytes = blobRangeLast(spec, int64(accum.Len())) - clientStart + 1
			}
			s.log.Info("blob 半截续传完成（半截已完整，整体回放写缓存）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "bytes", accum.Len(),
					"cost", time.Since(start).Round(time.Millisecond).String())...)
			s.log.Info("代理请求完成",
				labels.attrs(dockerLogSourceResume,
					"method", r.Method,
					"target", truncate(r.URL.Path, 300),
					"status", respStatus,
					"cache", resumeCacheStatus,
					"upstream", entry.Source,
					"rule", eff.Rule,
					"bytes", sentBytes,
					"cost", time.Since(start).Round(time.Millisecond).String(),
				)...)
			return entry, true, nil
		}
		// 旧时序路径（区间请求且总长未知，未 commit）：读回半截写缓存并回放
		//（区间请求按区间切片回 206）
		return s.serveResumeComplete(w, r, key, target, digest, ent, eff, disk, start, finalize, cr...)
	}

	if !earlyCommit && hasRange && total <= 0 {
		// 客户端区间请求必须有总长才能给出合法的 Content-Range / Content-Length：上游 206
		// 没带可解析的 Content-Range 时绝不 commit（半截前缀长度 ≠ 对象总长，谎报会让客户端
		// 从错误偏移继续）。此刻未向客户端写任何字节 → 按「本地半截无法满足」收场，调用方
		// 改走旧版 206 透传（client_range.go 的 errBlobRangeUnavailable 分支）
		upRes.Close()
		ent.finishFailed(errBlobRangeUnavailable)
		s.log.Warn("blob 半截续传拿不到对象总长，无法满足客户端区间请求，改走旧版 206 透传",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
				"part_bytes", n, "range_start", spec.start)...)
		return nil, false, errBlobRangeUnavailable
	}

	// 合成（以完整 blob 为基准的）响应头（仅旧时序路径需要重建：区间请求且总长未知——
	// 立即响应路径的响应头已在扫描前合成并 commit，此处直接沿用 hdrEarly）：沿用上游
	// 响应头（Content-Type / Docker-Content-Digest / ETag 等），剔除上游 206 的
	// Content-Range、Content-Length 重置为总长（拿不到总长则不设，Go 按 chunked 编码
	// 流式传输）。完整 GET 时它直接作为客户端的 200 响应头；区间请求（cr 非空）时
	// commitResumeHeader 会把它改写成 206 + 区间 Content-Range/Content-Length（上游 206
	// 语义绝不透传给客户端）。hdr 构建后只读（直接共享给缓存条目与搭车快照）。
	hdr := hdrEarly
	if !earlyCommit {
		hdr = upRes.header.Clone()
		hdr.Del("Content-Range")
		if total > 0 {
			hdr.Set("Content-Length", strconv.FormatInt(total, 10))
		} else {
			hdr.Del("Content-Length")
		}
		ent.setSnapshot(http.StatusOK, hdr, upRes.url) // 搭车订阅者的响应头快照（预取侧 commit 点同语义）
	}

	// 失败收尾：关闭上游与半截写句柄、广播 failed 并移除条目（半截文件已随流追加到
	// 最新前缀，进度不回退；本次响应被截断，docker 重试从更长前缀续传）
	var paf *os.File // 半截文件追加写句柄（O_APPEND）
	fail := func(cause error) (*cache.Entry, bool, error) {
		stopGrace()   // 宽限计时器随任一失败收场停掉（幂等；goroutine 不泄漏）
		upRes.Close() // 幂等：关闭连接、停看门狗、取消超时预算链
		if paf != nil {
			_ = paf.Close()
		}
		ent.finishFailed(cause)
		return nil, true, cause
	}

	if !earlyCommit {
		// commit 点（仅旧时序路径）：缺省回一次完整 200（本地前缀回放 + 上游剩余实时
		// 拼接，docker 无需支持 Range）；cr 非空（客户端区间请求）回 206 + Content-Range，
		// body 只含区间内字节。此后响应头固化，任何失败只能截断本次响应（docker 以
		// Range 重试，下次从更长前缀续传——进度不回退）。立即响应路径的 commit 已在
		// 扫描前完成（合成响应头，上游命中与否不影响客户端已拿到的头）。
		s.commitResumeHeader(w, hdr, upRes.url, total, cr...)
	}

	// 扫描命中：进度汇总的 total 按上游 206 Content-Range 校正（立即响应路径 commit 时
	// 可能仅有 manifest 索引值或未知-chunked，percent 以此从「不可知」变为精确）
	if prog != nil && total > 0 {
		prog.setTotal(total)
	}

	// 时间驱动的单层 blob 多源并发补充（续传执行方，日志 source=resume）：
	// 与客户端主路径同一套判定/合并器（slowdownload.go 的 timeBlobSupplement）。此前本
	// 路径明确「不叠加 slowdownload 慢速升级」，现场故障里断流后走续传仍然只有单路——现
	// 接入：每满 blob-concurrency-interval 且该层未完成，就向尚未加入的候选源补充并发
	// Range 续传（只做加法、客户端无感、不切状态码）。总长优先取上游 206 的
	// Content-Range，拿不到用 manifest 记录的 layer size 兜底（与客户端主路径一致）。
	resumeKnownTotal, resumeTotalSource := total, blobTotalSourceUpstream
	if resumeKnownTotal <= 0 {
		if v, ok := s.manifestBlobSize(digest); ok && v > 0 {
			resumeKnownTotal, resumeTotalSource = v, blobTotalSourceManifest
		} else {
			resumeTotalSource = blobTotalSourceUnknown
		}
	}
	resumeEnd := int64(-1) // 续传区间终点（<0 = 开区间到对象末尾）
	if resumeKnownTotal > n {
		resumeEnd = resumeKnownTotal - 1
	}
	resumeStartedAt := time.Now()
	blobTrack := newBlobConcurrencyTrack(
		eff.RuleType == config.RuleTypeDocker && eff.BlobConcurrencyInterval > 0,
		resumeStartedAt, eff.BlobConcurrencyInterval)
	var blobConc blobSupplementInput
	if blobTrack != nil {
		blobConc = blobSupplementInput{
			r:           r,
			res:         upRes, // 胜者活体（升级时并入合并器首位）
			labels:      labels,
			logSource:   dockerLogSourceResume,
			eff:         eff,
			candidates:  candidates,
			startedAt:   resumeStartedAt,
			knownTotal:  resumeKnownTotal,
			totalSource: resumeTotalSource,
			end:         resumeEnd,
			// 重复区间内容对账（slowMerger.verify，见 slowdownload.go 的
			// verifyStaleAgainstConfirmed）：补充源的过期缓冲字节与已确认输出（accum =
			// [0..已写游标) 的权威字节：半截前缀 + 胜者已写增量）同偏移逐字节比对，不符
			// 即剪枝该源——上游谎报 206 起点 / 混入非本 blob 内容的错位字节从此进不了
			// 半截文件、广播与客户端流。闭包在合并循环 goroutine 内调用（与
			// writeRemainChunk 同 goroutine 串行），读 accum 无数据竞争。
			verify: func(off int64, stale []byte) bool {
				return verifyStaleAgainstConfirmed(accum.Bytes(), off, stale)
			},
		}
	}

	startArgs := labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "part_bytes", n, "total", total,
		"total_source", resumeTotalSource, "candidate", truncate(upRes.url, 300),
		"candidates", len(candidates), "sources", 1, "check", eff.SuccessCheck)
	if isolatedGap > 0 {
		startArgs = append(startArgs, "isolated_gap", isolatedGap.Round(time.Millisecond).String())
	}
	if hasRange {
		startArgs = append(startArgs, "range_start", clientStart, "range_end", clientLast)
	}
	s.log.Info("blob 半截续传开始（回放半截前缀+实时拼接剩余区间，按候选轮询选定一路 Range 续传，可按间隔补充多源）", startArgs...)
	if reason := blobConcurrencyOffReason(eff); reason != "" {
		// 时间驱动补充未启用的一次性原因留痕（disabled / not_docker_rule）
		s.log.Info("未补充多源并发",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "candidate", truncate(upRes.url, 300),
				"reason", reason, "candidates", len(candidates))...)
	}

	// 活跃下载进度登记（周期汇总，见 progress_summary.go）：立即响应路径已在扫描前登记
	//（初始字节 = 半截前缀 n、total = commit 总长）；旧时序路径在 commit 后登记，初始
	// 已下载字节同样 = 半截前缀 n（进度不回退，percent 直接对总长），defer 注销——失败/
	// 成功/panic 返回都不留幽灵条目；每追加一块经 addBytes 累加
	if !earlyCommit {
		prog = s.progressRegister(dockerLogSourceResume, labels.image, labels.digest, r.Method, n, total)
		defer prog.unregister()
	}

	// 回放半截前缀（仅旧时序路径；立即响应路径已在扫描前经 replayPrefix 回放完毕）：
	// 写锁内一次性整读进累积缓冲（与并发保存方的整体重写互斥，保证读到的每个字节来自
	// 同一份合法前缀；磁盘顺序读，锁持有时间 = 磁盘读耗时），锁外再按客户端区间窗口
	// 写给客户端（慢客户端绝不拖住写锁）。前缀字节即同 digest 内容前缀（digest 寻址
	// 不可变）
	if !earlyCommit {
		if rerr := replayPrefix(); rerr != nil {
			return fail(rerr)
		}
		flushClient(w) // 冲刷响应头 + 前缀（与立即响应路径同款，首字节及时落网）
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
		args := labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "bytes", accum.Len())
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
		off := written // 该块在完整 blob 中的起始偏移（= 追加前的半截长度）
		if aerr := s.appendPartChunk(disk, key, paf, written, p); aerr != nil {
			return aerr
		}
		written += int64(len(p))
		prog.addBytes(len(p)) // 活跃下载周期汇总累加（剩余区间增量；半截前缀已计入初始值）
		if berr := ent.writeBroadcast(p); berr != nil {
			return fmt.Errorf("blob 半截续传广播失败: %w", berr)
		}
		accum.Write(p)
		writeClient(off, p)
		return nil
	}
	if len(upRes.first) > 0 {
		if aerr := writeRemainChunk(upRes.first); aerr != nil {
			return fail(fmt.Errorf("blob 半截续传写半截文件失败（%s）: %w", upRes.url, aerr))
		}
	}

	// 上游剩余区间流式读取：经 progressReader 记入看门狗（read-timeout「无新增数据」
	// 语义与客户端流式路径一致）；客户端写失败（已断开）按宽限语义处置——宽限到期仍无
	// 搭车订阅者则终止上游读取（下方宽限终止出口），其余情况维持「继续读完只填缓存」
	//（半截与广播照常推进，等待中的搭车订阅者不受影响）。
	// 时间驱动的多源并发补充在此判定：首轮满足条件即建合并器接手胜者连接（break 转入
	// 下方有序合并循环），合并阶段再按同一节拍继续增量加源（只做加法，绝不切源）。
	var resMerger *slowMerger
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
			if blobTrack != nil && rdErr == nil {
				if round, due := blobTrack.due(time.Now()); due {
					blobConc.round = round
					blobConc.now = time.Now()
					blobConc.bytes = int64(accum.Len())
					blobConc.offset = written
					blobConc.clientGone = !clientOK
					if s.timeBlobSupplement(&blobConc) {
						// 首轮补充：胜者当前连接并入合并器（天然从 written 继续），其余候选
						// 并发 Range 续传；客户端仍是同一条连续流（状态码/响应头不变）。
						resMerger = blobConc.merger
						blobConc.res = nil // 胜者连接已移交合并器：后续判定只做增量加源
						graceMu.Lock()
						graceMerger = resMerger // 宽限到期回调据此逐路终止全部上游源
						graceMu.Unlock()
						break
					}
				}
			}
			if rdErr != nil {
				if errors.Is(rdErr, io.EOF) {
					break // 剩余区间读完：半截文件 = 完整 blob，前缀+增量恰好拼出完整 body
				}
				// 宽限终止判定（先查宽限标志再按普通读失败处理）：宽限到期回调已置位
				// ——客户端断开且无搭车订阅者的正常运维事件而非故障，Info 级留痕
				//（绝不 Error），半截保留（进度不回退）、hub 条目经下方 fail 的
				// finishFailed 移除；上游已由回调关闭（fail 的 Close 幂等兜底）。
				// 响应已 commit 且客户端已断，绝不再向客户端写任何字节。
				graceMu.Lock()
				aborted := graceAborted
				graceMu.Unlock()
				if aborted {
					stopGrace() // 计时器已触发：Stop 幂等收表（timer goroutine 不泄漏）
					s.log.Info("blob 半截续传客户端断开且无搭车订阅者，宽限终止续传（半截保留，下次从断点再续）",
						labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
							"bytes", accum.Len(), "part_bytes", written,
							"grace", graceLimit.String(), "subscribers", ent.subscriberCount())...)
					return fail(errResumeGraceAborted)
				}
				// commit 后读上游失败：保留更新后的半截文件（已追加到最新前缀，进度
				// 不回退），广播 failed 并截断本次响应（docker 重试接着更长前缀续传）
				s.log.Error("blob 半截续传读上游失败，终止本次响应（半截已保留，进度不回退）",
					labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
						"candidate", truncate(upRes.url, 300),
						"bytes", accum.Len(), "part_bytes", written, "err", rdErr)...)
				return fail(fmt.Errorf("blob 半截续传读上游失败（%s）: %w", upRes.url, rdErr))
			}
		}
	}

	// //////////////////  时间驱动补充后的有序合并循环  start  ///////////////////////////////////
	if resMerger != nil {
		// 有序合并：胜者（首位，天然从 written 继续）+ 补充的续传源按绝对偏移严格有序交给
		// writeRemainChunk（追加半截 → 广播增量 → 写客户端与累积缓冲，顺序与单源路径完全
		// 一致）；合并阶段每满 blob-concurrency-interval 继续增量加源（只做加法）。
		var chunkErr error
		_, merr := resMerger.run(func(p []byte) {
			if chunkErr != nil {
				return // 写半截已失败：后续块一律丢弃（合并循环随 abortAll 退出）
			}
			if aerr := writeRemainChunk(p); aerr != nil {
				chunkErr = aerr
				resMerger.abortAll("半截续传写半截文件失败，终止全部上游源")
			}
		}, func(now time.Time, live int) {
			if progressInterval > 0 && time.Since(lastProgressAt) >= progressInterval {
				emitProgress(now)
			}
			if blobTrack != nil {
				if round, due := blobTrack.due(now); due {
					blobConc.round = round
					blobConc.now = now
					blobConc.bytes = int64(accum.Len())
					blobConc.offset = resMerger.cursor()
					blobConc.merger = resMerger
					blobConc.clientGone = !clientOK
					s.timeBlobSupplement(&blobConc)
				}
			}
		})
		if chunkErr != nil {
			return fail(fmt.Errorf("blob 半截续传写半截文件失败（多源合并中）: %w", chunkErr))
		}
		// 宽限终止：客户端断开且宽限到期 → onGraceExpired 已 abortAll，合并循环随之退出
		graceMu.Lock()
		aborted := graceAborted
		graceMu.Unlock()
		if aborted {
			stopGrace()
			s.log.Info("blob 半截续传客户端断开且无搭车订阅者，宽限终止续传（半截保留，下次从断点再续）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
					"bytes", accum.Len(), "part_bytes", written,
					"grace", graceLimit.String(), "subscribers", ent.subscriberCount())...)
			return fail(errResumeGraceAborted)
		}
		if merr != nil {
			s.log.Error("blob 半截续传读上游失败，终止本次响应（半截已保留，进度不回退）",
				labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
					"candidate", truncate(upRes.url, 300),
					"bytes", accum.Len(), "part_bytes", written, "err", merr)...)
			return fail(fmt.Errorf("blob 半截续传读上游失败（多源合并）: %w", merr))
		}
	}
	// //////////////////  时间驱动补充后的有序合并循环  end  /////////////////////////////////////

	stopGrace()     // 正常 EOF 收场：停掉宽限计时器（幂等；宽限终止与 EOF 竞态以先到者为准）
	_ = paf.Close() // 追加完毕；磁盘写为直写（无用户态缓冲），无需 Sync（断电丢失由 Clean 兜底）
	upRes.Close()   // 幂等：关闭连接、停看门狗、取消超时预算链

	// 完整性校验（总长已知时）：累积长度必须等于总长——不足说明上游提前 EOF（绝不把
	// 短 body 写缓存），超出说明上游 Content-Range 声明与实际不符（绝不给客户端错误
	// 数据）；两者均按失败截断（半截保留已收到的内容，进度不回退）
	if total > 0 && int64(accum.Len()) != total {
		s.log.Error("blob 半截续传长度不符，终止本次响应且不写缓存（半截已保留，进度不回退）",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule,
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
	if cached := s.finalizeFetchEntry(r, key, target, entry, eff, disk, dockerLogSourceResume); !cached {
		// 拼接完成但拒绝入库（防毒缓存守卫 / 写盘失败）：本路响应已 commit（客户端将因
		// 摘要不符自行重试；守卫拒绝时毒半截已随 finalize 丢弃，下次全量回源从干净状态
		// 开始），hub 条目按 failed 收场——绝不广播 done（搭车订阅者的增量拼不出可入库
		// 的完整内容，广播 done 会让并发 MISS 直接 HIT 到不存在/毒性的条目）
		ferr := fmt.Errorf("blob 拼接内容未写入缓存（digest %s，摘要校验/守卫拒绝或写盘失败）", digest)
		s.log.Error("blob 半截续传完成但内容未写缓存（防毒缓存守卫拒绝或写盘失败；守卫拒绝时毒半截已丢弃，下次全量回源）",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest,
				"bytes", accum.Len(), "candidate", truncate(upRes.url, 300))...)
		ent.finishFailed(ferr)
		return nil, true, ferr
	}
	// 置 done 并关闭订阅（搭车方收完 channel 缓冲内的最后增量即完整）；条目滞留 hub
	// 一小段时间供并发 MISS 兜底检出「done → 直接 HIT」，到期自动移除（不碰半截文件）
	ent.finishDone()

	// 客户端实际收到的状态码与字节数：区间请求是 206 + 区间长度，完整 GET 是 200 + 总长
	respStatus, sentBytes := http.StatusOK, int64(accum.Len())
	if hasRange {
		respStatus = http.StatusPartialContent
		sentBytes = blobRangeLast(spec, int64(accum.Len())) - clientStart + 1
	}
	doneArgs := labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "bytes", accum.Len(),
		"part_bytes", n, "upstream", upRes.url,
		"cost", time.Since(start).Round(time.Millisecond).String())
	if hasRange {
		doneArgs = append(doneArgs, "range_start", clientStart, "sent", sentBytes)
	}
	if resMerger != nil {
		// 多源并发补充过的流：完成日志补充 sources / alive_sources（与剪枝日志同一组字段）
		doneArgs = append(doneArgs, "sources", resMerger.sourceCount(), "alive_sources", resMerger.finalAlive())
	}
	s.log.Info("blob 半截续传完成（半截前缀+剩余区间拼出完整 blob 并已写缓存）", doneArgs...)
	s.log.Info("代理请求完成",
		labels.attrs(dockerLogSourceResume,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", respStatus,
			"cache", resumeCacheStatus,
			"upstream", upRes.url,
			"rule", eff.Rule,
			"bytes", sentBytes,
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
	return entry, true, nil
}

// serveResumeComplete 半截已完整的收尾（续传扫描发现上游对 bytes=<n>- 回 416）：416
// 语义 = 请求起点不小于对象总长，而半截是同 digest 内容的合法前缀（起点 ≤ 总长），
// 只能 n == total——半截即完整 blob。直接读回半截写缓存并回放（X-Cache: RESUME），
// 不再全量下载。读回失败降级既有全量回源兜底（半截保留）。
// 可选 cr（客户端区间请求，起点 >0）：写缓存后按区间切片回 206（serveEntryBlobRange），
// 而不是回完整 200——语义与兼容两条路径一致（内容都是完整 blob 原文）。
func (s *Server) serveResumeComplete(w http.ResponseWriter, r *http.Request, key, target, digest string, ent *prefetchStreamEntry, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry), cr ...blobRangeSpec) (*cache.Entry, bool, error) {
	labels := parseDockerLogInfo(target) // 日志统一标签（执行方日志统一 source=resume）
	data, rerr := os.ReadFile(disk.PartPath(key))
	if rerr != nil {
		// 广播搭车订阅者降级后走既有全量回源兜底（半截保留；commit 前未向客户端写过字节）
		cerr := fmt.Errorf("读回完整半截失败: %w", rerr)
		ent.finishFailed(cerr)
		s.log.Warn("blob 半截已完整但读回失败，降级既有全量回源",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "err", rerr)...)
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}
	entry := &cache.Entry{
		Status:   http.StatusOK,
		Header:   http.Header{},
		Body:     data,
		StoredAt: time.Now(),
		Source:   "part-resume", // 观测：内容来自半截文件整体回放（写缓存前由 finalize 强校验摘要）
	}
	// 续传执行方写缓存：直接调 finalizeFetchEntry 并标注 source=resume（不经注入的
	// finalize 回调——该回调按客户端主路径标注 source=client，此处写者实为续传执行方；
	// blob 落盘成功即删半截见 finalizeFetchEntry 钩子）
	if cached := s.finalizeFetchEntry(r, key, target, entry, eff, disk, dockerLogSourceResume); !cached {
		// 半截整体回放被拒入库（防毒缓存守卫 / 写盘失败）：客户端尚未收到任何字节
		//（commit 在写缓存之后），此刻降级既有全量回源仍能给出正确内容；守卫拒绝时
		// 毒半截已随 finalize 丢弃（logSource=resume），降级路径失败时其 partial sink
		// 从零保留新前缀，绝不复用毒半截
		ferr := fmt.Errorf("blob 半截整体回放未写入缓存（digest %s，摘要校验/守卫拒绝或写盘失败）", digest)
		s.log.Warn("blob 半截已完整但内容未写缓存（防毒缓存守卫拒绝或写盘失败），丢弃半截降级全量回源",
			labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "digest", digest, "bytes", len(data))...)
		ent.finishFailed(ferr)
		return s.fetchViaCandidatesStreaming(w, r, target, eff, "MISS", finalize, start, partialSink{key: key, disk: disk})
	}
	ent.finishDone()
	// 回放方式按请求语义定：区间请求（起点 >0）切片回 206，其余保持完整 200 回放；
	// 起点越界按 RFC 回 416（serveEntryBlobRange 内部同样按 416 处理），日志字节数为 0
	respStatus, sentBytes := http.StatusOK, int64(len(data))
	if len(cr) > 0 && int64(len(data)) > 0 {
		spec := cr[0]
		switch {
		case spec.start >= int64(len(data)):
			respStatus, sentBytes = http.StatusRequestedRangeNotSatisfiable, 0
		default:
			respStatus = http.StatusPartialContent
			sentBytes = blobRangeLast(spec, int64(len(data))) - spec.start + 1
		}
	}
	doneArgs := labels.attrs(dockerLogSourceResume, "rule", eff.Rule, "bytes", len(data),
		"cost", time.Since(start).Round(time.Millisecond).String())
	if len(cr) > 0 {
		doneArgs = append(doneArgs, "range_start", cr[0].start, "sent", sentBytes)
	}
	s.log.Info("blob 半截续传完成（半截已完整，整体回放写缓存）", doneArgs...)
	s.log.Info("代理请求完成",
		labels.attrs(dockerLogSourceResume,
			"method", r.Method,
			"target", truncate(r.URL.Path, 300),
			"status", respStatus,
			"cache", resumeCacheStatus,
			"upstream", entry.Source,
			"rule", eff.Rule,
			"bytes", sentBytes,
			"cost", time.Since(start).Round(time.Millisecond).String(),
		)...)
	if len(cr) > 0 {
		// 区间请求：缓存条目已就绪，直接本地切片回 206（绝不发上游）
		s.serveEntryBlobRange(w, r, entry, cr[0], resumeCacheStatus, eff, start)
		return entry, true, nil
	}
	s.serveEntry(w, r, entry, resumeCacheStatus, eff, start)
	return entry, true, nil
}

// resumeEarlyHeader 续传/搭车「立即响应」的合成响应头（commit 先于上游确认，本地半截
// 前缀回放先行）：blob 按 digest 寻址、内容不可变，Content-Type 恒 application/octet-stream、
// Docker-Content-Digest 即目标 digest；total>0（manifest 索引，见 blob_size_index.go）时设
// Content-Length=total，否则不设（Go 按 chunked 编码流式传输——docker 对 chunked blob
// 响应完全兼容，客户端至少立即看到字节进度而不是长时间零反馈）。构建后只读，直接共享
// 给客户端 commit、搭车快照与缓存条目。
func resumeEarlyHeader(digest string, total int64) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/octet-stream")
	if digest != "" {
		h.Set("Docker-Content-Digest", digest)
	}
	if total > 0 {
		h.Set("Content-Length", strconv.FormatInt(total, 10))
	}
	return h
}

// flushClient 显式冲刷响应（commit 头 + 已回放的本地前缀立即到达客户端）：net/http
// 服务端对响应有约 2-4KB 的用户态缓冲，半截前缀小于缓冲时不显式冲刷的话，响应头与
// 前缀会滞留到下一个大块写出才落网——立即响应的全部意义就是首字节及时到网。
func flushClient(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// commitResumeHeader 续传执行方对客户端的 commit 点——沿用 skipResponseHeader 过滤逐跳头
// 与自管头（上游 206 的 Content-Range 已由调用方在 hdr 中剔除），X-Cache 用 RESUME 观测值、
// X-Proxy-Upstream 观测来源候选。两种语义：
//   - 缺省（无 cr，客户端发的是完整 GET）：对客户端回一次完整 200，Content-Length = 总长
//     （客户端无需支持 Range）；
//   - cr 非空（客户端区间请求，起点 >0）：回 206 + `Content-Range: bytes <start>-<last>/<total>`，
//     并补 `Accept-Ranges: bytes`、Content-Length 收窄为区间长度——调用方保证 total > 0
//     （拿不到总长时不会走到 commit，见 resumeBlobStreaming 的 errBlobRangeUnavailable 分支）。
//
// 调用后响应头固化，后续失败只能截断本次响应（docker 以 Range 重试，从更长前缀续传，
// 进度不回退）。
func (s *Server) commitResumeHeader(w http.ResponseWriter, hdr http.Header, source string, total int64, cr ...blobRangeSpec) {
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
	if len(cr) == 0 {
		if total > 0 {
			h.Set("Content-Length", strconv.FormatInt(total, 10)) // 一次完整 200 的总长
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	spec := cr[0]
	last := blobRangeLast(spec, total)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", spec.start, last, total))
	h.Set("Content-Length", strconv.FormatInt(last-spec.start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
}

// //////////////////  客户端请求入口  //////////////////////////////////////////////////////////

// tryServeBlobResume 客户端 blob GET 缓存 MISS 后的「半截续传」入口（handleGet 在在途
// 预取搭车检查之后调用；调用位置保证：路径目标 GET、缓存可读、正式缓存 MISS）。
// 返回 handled=true 表示本次请求已由续传路径处理（含内部降级后的既有全量回源 / 区间透传），
// 调用方直接返回；false = 续传不适用（非 blob / 开关关闭 / 无半截文件 / 半截为空 /
// 区间请求起点超出半截长度），行为与旧版完全一致。
//
// 可选参数 cr（variadic，既有调用方不受影响）：客户端区间请求（起点 >0 的 blob Range，
// 见 client_range.go）。非空时本路径对客户端回 206 且只发区间内字节：
//   - 起点 <= 半截长度（无缺口）：复用续传执行方（本地半截从起点回放 + 上游剩余区间续传
//     拼接），执行方内部仍按完整 blob 拼接并写缓存；
//   - 起点 > 半截长度（中间缺一段，不能凭空补）：打一条 Warn 说明缺口大小后返回 false，
//     调用方维持旧版 206 透传（不写缓存、不保留半截）。
//
// 时序（全部编排在同 key 既有 flightGroup 上——谁先到谁执行、其余等待共享结果，与
// 预取/客户端回源的合并语义一致，上游对该 blob 只有一路下载）：
//  1. 前置检查：blob 语义、全局开关、半截文件存在且非空（空半截立即丢弃）、区间请求的
//     起点与半截长度的对齐（缺口即不接管）；
//  2. flight.DoLimit：首个调用方成为续传执行方（resumeBlobStreaming），其余同 key
//     请求成为等待方（等待上限 flightWaitLimit，与既有流式回源一致）；
//  3. 等待方超限：完整 GET 先查续传 hub 条目搭车（tryServeFromPrefetchHub，前缀+增量
//     拼接，X-Cache: PREFETCH——续传条目在执行方扫描候选前已登记，搭车窗口覆盖整个续传
//     过程），搭不上（执行方已失败移除）再走既有全量回源兜底（失败捕获会把更长前缀
//     存回半截，进度不回退）；区间请求不搭车（搭车只提供从 0 起的完整内容），直接走
//     旧版 206 透传兜底（rangePassthrough）。
func (s *Server) tryServeBlobResume(w http.ResponseWriter, r *http.Request, key, target string, eff config.Options, disk *cache.Cache, start time.Time, finalize func(*cache.Entry), streamingError func(error, bool), cr ...blobRangeSpec) bool {
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
	// 客户端区间请求（起点 >0）：本路径要么完整满足（206 拼接），要么交回调用方旧版透传
	hasRange := len(cr) > 0
	var spec blobRangeSpec
	if hasRange {
		spec = cr[0]
	}
	n, pok := disk.PartSize(key)
	if !pok || n <= 0 {
		if pok {
			// 空半截（0 字节）：立即丢弃（无续传价值），走既有全量回源
			disk.RemovePart(key)
		}
		return false
	}
	if hasRange && spec.start > n {
		// 起点超出了本地半截：中间缺的这段没有任何本地来源，不能凭空补（拼上去就是损坏
		// 内容）。维持旧版 206 透传（Range 原样发上游），这里留一条 Warn 说明缺口大小——
		// 运维据此判断「半截为什么没帮上忙」（通常是上次中断在更早的位置）。
		s.log.Warn("blob 区间请求起点超出本地半截长度（中间缺一段，无法凭空补齐），本次维持旧版 206 透传",
			parseDockerLogInfo(target).attrs(dockerLogSourceClient, "rule", eff.Rule,
				"range_start", spec.start, "part_bytes", n, "gap_bytes", spec.start-n,
				"target", truncate(target, 300))...)
		return false
	}

	// rangePassthrough 区间请求的兜底：恢复旧版「Range 原样透传上游 206」行为（不写缓存、
	// 不保留半截），语义与 handleGet 的 request-range BYPASS 分支一致。只在尚未向客户端
	// 写出任何字节时调用（commit 之后的失败绝不能走这里，见各分支）。
	rangePassthrough := func() {
		_, committed, ferr := s.fetchViaCandidatesStreaming(w, r, target, eff, "BYPASS", nil, start)
		streamingError(ferr, committed)
	}

	e, executed, err := s.flight.DoLimit(key, flightWaitLimit, func() (*cache.Entry, error) {
		ent, committed, ferr := s.resumeBlobStreaming(w, r, key, target, digest, eff, disk, start, finalize, cr...)
		if ferr != nil && committed {
			// 已 commit 后失败：响应头已固化，标记与既有流式执行方同款（不得再 writeError）
			return nil, fmt.Errorf("%w: %v", errStreamingCommitted, ferr)
		}
		return ent, ferr
	})
	switch {
	case errors.Is(err, errFlightWaitTimeout):
		if hasRange {
			// 区间请求不搭车（搭车提供的是从 0 起的完整内容）：直接按旧版 206 透传兜底
			s.log.Warn("等待同 blob 半截续传超限（区间请求不搭车），改走旧版 206 透传",
				parseDockerLogInfo(target).attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
					"wait", flightWaitLimit.String(), "range_start", spec.start, "target", truncate(target, 300))...)
			rangePassthrough()
			return true
		}
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
	case errors.Is(err, errBlobRangeUnavailable):
		// 区间请求无法由本地半截满足（上游 206 未带可解析总长等）：未 commit，按旧版 206
		// 透传兜底（语义与旧版完全一致；绝不 commit 一个总长错误的 206）
		s.log.Warn("blob 半截续传无法满足客户端区间请求，改走旧版 206 透传",
			parseDockerLogInfo(target).attrs(dockerLogSourceClient, "rule", eff.Rule, "key", key,
				"range_start", spec.start, "part_bytes", n, "target", truncate(target, 300))...)
		rangePassthrough()
		return true
	case errors.Is(err, errStreamingCommitted):
		// 本调用是续传执行方且 commit 后失败：底层原因已留痕（读上游失败=Error、客户端
		// 断开=Warn 运维事件），响应头已写出（本次响应被截断，docker 会以 Range 断点续传
		// 重试，下次从更长前缀续传——进度不回退）
	case err != nil:
		// 未 commit 失败：走 streamingError 一致出口（上游明确 4xx 透传 / 其余 502）
		streamingError(err, false)
	case executed:
		// 本调用是续传执行方：响应已直接写给客户端（commit + 拼接回放），无需再回放
	default:
		// 等待方共享到执行方的完整结果：缓存/内容此刻已就绪，按 RESUME 语义回放
		if e != nil {
			if hasRange {
				// 共享到的结果是完整 blob（执行方内部按完整 blob 拼接），本地切片回 206
				s.serveEntryBlobRange(w, r, e, spec, resumeCacheStatus, eff, start)
				return true
			}
			s.serveEntry(w, r, e, resumeCacheStatus, eff, start)
			return true
		}
		if hasRange {
			rangePassthrough() // 防御：无结果时按旧版 206 透传
			return true
		}
		return false // 防御：无结果时走既有路径
	}
	return true
}
