// blob_size_index.go —— docker layer blob 的 manifest size 进程内索引（knownTotal 兜底）：
// 线上真实故障：docker pull 一个 1.32GiB 的单层 blob，上游镜像站 200 响应**没有
// Content-Length**，而慢速升级多源并发续传的触发判定要求「已知总长」（见
// slowdownload.go 的 slowEscalationTarget）——总长不可知 → 判速升级被静默跳过，
// 客户端只有一路连接 ~53KB/s 慢慢下，19 分钟只下了 57.6MiB 后被上游
// PROTOCOL_ERROR 掐断。
//
// 兜底依据：单平台 image manifest 的每个 layer（含 config blob）都带 size 字段，且
// blob 按 digest 寻址、内容不可变——同 digest 的 size 是稳定事实。所以在解析 manifest
// 时把 digest → size 记进本索引，blob 流式回源响应没有 Content-Length 时用 manifest
// size 兜底 knownTotal（日志 total_source=manifest），判速/时间驱动的多源并发补充即可
// 正常工作。注意：本索引只用于「推进 / 收尾的进度判断」，写缓存前仍由既有 sha256 全量
// 校验（finalizeFetchEntry 的防毒缓存守卫）做最终把关——manifest 里的 size 若被上游
// 伪造（与真实内容不符），拼出的内容摘要必然对不上、绝不会被固化成正式缓存条目。
//
// 并发与容量：Server 级进程内 map，读写全部经互斥锁；容量上限
// manifestBlobSizeCapacity（FIFO 淘汰最旧插入项）——镜像层数有限、单条记录量级几十
// 字节，上限只为防止长期运行被海量不同 digest 撑爆内存。进程重启丢失可接受：manifest
// 与 layer 通常在同一次 pull 内先后回源，进程内缓存即可，无需跨进程持久化。

package proxy

import "sync"

// manifestBlobSizeCapacity manifest blob size 索引的最大条目数（FIFO 淘汰最旧插入项）。
// 一个镜像一般 10-200 层，8192 条可覆盖几十个并发镜像的 manifest，量级足够且内存可控
//（8192 × 约百字节 ≈ 1MB 以内）。
const manifestBlobSizeCapacity = 8192

// manifestBlobSizeIndex digest → 对象字节数 的进程内有界索引（并发安全）。
type manifestBlobSizeIndex struct {
	// mu 保护 sizes 与 order 的并发读写（manifest 解析 goroutine 与 blob 流式下载并发）。
	mu sync.Mutex
	// sizes digest → size（字节）；只收录 size>0 的合法项。
	sizes map[string]int64
	// order 插入顺序（FIFO 淘汰依据；重复 put 已在 sizes 里时不再追加）。
	order []string
}

// newManifestBlobSizeIndex 创建空索引。
func newManifestBlobSizeIndex() *manifestBlobSizeIndex {
	return &manifestBlobSizeIndex{sizes: make(map[string]int64, 64)}
}

// put 记录一个 digest 的对象字节数（size<=0 或 digest 为空时静默忽略）：
// 同 digest 重复记录只覆盖 size（内容不可变，正常不会变），不重复占用淘汰队列；
// 容量满时按 FIFO 淘汰最旧插入项。
func (i *manifestBlobSizeIndex) put(digest string, size int64) {
	if i == nil || digest == "" || size <= 0 {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.sizes == nil { // 防御：零值索引也能安全写入
		i.sizes = make(map[string]int64, 64)
	}
	if _, ok := i.sizes[digest]; ok {
		i.sizes[digest] = size
		return
	}
	for len(i.sizes) >= manifestBlobSizeCapacity && len(i.order) > 0 {
		old := i.order[0]
		i.order = i.order[1:]
		delete(i.sizes, old)
	}
	i.sizes[digest] = size
	i.order = append(i.order, digest)
}

// get 查询 digest 的对象字节数；未记录返回 ok=false。
func (i *manifestBlobSizeIndex) get(digest string) (int64, bool) {
	if i == nil || digest == "" {
		return 0, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	size, ok := i.sizes[digest]
	return size, ok
}

// len 返回当前条目数（仅供测试/诊断观察）。
func (i *manifestBlobSizeIndex) len() int {
	if i == nil {
		return 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.sizes)
}

// //////////////////  Server 入口  ////////////////////////////////////////////////////////////

// blobSizeIndex 返回 Server 级的 manifest blob size 索引：New 已预构建；绕过 New 构造
// 的 Server 惰性补建（防御风格与 dockerTokens / prefetchSlots 一致）。
func (s *Server) blobSizeIndex() *manifestBlobSizeIndex {
	s.blobSizeMu.Lock()
	defer s.blobSizeMu.Unlock()
	if s.blobSizes == nil {
		s.blobSizes = newManifestBlobSizeIndex()
	}
	return s.blobSizes
}

// recordManifestBlobSizes 把单平台 image manifest 里全部 blob（config + layers）的
// digest → size 记进进程内索引（由 prefetch.go 的 manifest 解析处调用，**与预取开关
// 无关**：即使 docker-prefetch-enabled=false，blob 流式下载的 knownTotal 兜底仍需要
// 这份 size）。size<=0 / digest 不合法的单项静默跳过。
func (s *Server) recordManifestBlobSizes(body imageManifestBody) {
	if s == nil {
		return
	}
	idx := s.blobSizeIndex()
	if validBlobDigest(body.Config.Digest) {
		idx.put(body.Config.Digest, body.Config.Size)
	}
	for _, l := range body.Layers {
		if validBlobDigest(l.Digest) {
			idx.put(l.Digest, l.Size)
		}
	}
}

// manifestBlobSize 查询索引里记录的 blob 字节数（blob 流式回源无 Content-Length 时的
// knownTotal 兜底，见 slowdownload.go 的 slowEscalationTargetWithFallback）。
func (s *Server) manifestBlobSize(digest string) (int64, bool) {
	if s == nil {
		return 0, false
	}
	return s.blobSizeIndex().get(digest)
}
