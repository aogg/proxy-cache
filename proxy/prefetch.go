// prefetch.go —— docker 镜像 layer 异步预取（manifest 驱动）：
// docker 客户端并发下载 layer 受限（daemon 默认 max-concurrent-downloads=3），manifest
// 拿到后逐层请求导致拉镜像慢。代理一旦识别出某次请求是 docker manifest 请求且回源成功
//（HTTP 200 已写入缓存），就异步预取该镜像其余 layer blob 写入缓存——客户端后续 blob
// 请求直接命中，不用等客户端逐个请求才下载。
//
// 触发条件（全部满足才预取，见 maybePrefetchImageLayers；不满足静默返回）：
//   - 路径目标 GET 回源成功：HTTP 200 且已过成功判据写入缓存（206/Range 断点续传、
//     no-cache、关缓存等 BYPASS 请求到不了「写入缓存」这一步，天然不触发；
//     HEAD 不走该路径且无 body 不写缓存，不触发）；
//   - 请求 URL 匹配 /v2/<name>/manifests/<ref>（blob 请求与完整 URL 目标不匹配）；
//   - 响应 Content-Type 为单平台 image manifest（docker schema2 / OCI image manifest，
//     容忍 ;charset=... 等参数后缀）。manifest list / OCI index（多平台）不触发：
//     客户端拿到 index 后会立刻再请求具体平台的 child manifest，届时自然触发，
//     代理无需在本层解析 index 替客户端挑平台。
//
// 去重与合并（singleflight 语义见 flight.go）：
//   - 已有未过期缓存的 blob 直接跳过（不重复下载）；
//   - 预取在途集合（blob 缓存键）防重复排队：多个客户端同时拉同一镜像 / 同一 manifest
//     的预取被重复触发时，同一 blob 只排队一次；
//   - 预取下载复用现有 flightGroup（缓存键为 key）：与客户端并发请求同一 blob 时合并
//    为同一次回源（在途则不另起第二路下载），谁先到谁执行、其余等待共享结果；
//   - Server 级有界并发：全部预取共享一个信号量（docker-prefetch-concurrency，默认 4），
//     预取绝不阻塞 manifest 响应（异步 goroutine）。
//
// 回源路径复用：预取 blob 的下载与客户端请求走完全相同的回源链路——候选轮询
//（round-robin 起始下标共用 s.rr）、候选级超时覆盖、http-proxy、insecure TLS、
// docker 401 匿名 token dance、成功判据与缓存写入（finalizeFetchEntry）全复用；
// 只写缓存、不产生客户端响应。
//
// 失败降级：预取失败只记日志（Warn/Debug），不重试（候选管线自身已有 item/round
// 重试语义），绝不影响客户端主请求；客户端后续请求照走原路径——flight 等待
// flightWaitLimit（10s）上限后自行回源的既有语义不变。
//
// 刷新语义：blob 按 digest 寻址、内容不可变，不存在刷新问题；过期由现有 cache.ttl /
// clean-interval 与 Janitor 管理。预取不改变 manifest 的 TTL/ETag/缓存行为；tag（如
// :latest）更新后新 manifest 引用相同 digest 的 layer 直接命中缓存，digest 变了才回源
// 下载；manifest 自身的刷新与客户端 ETag 协商完全不受预取影响。
//
// 生命周期：预取 goroutine 不使用请求的 context（请求结束会被取消），任务级用独立
// context + 超时兜底（prefetchTaskTimeout，默认 5 分钟）；进程退出不等待预取任务
//（goroutine 不持有客户端资源，随进程消亡即可，绝不悬挂退出；prefetchWG 仅供
// 测试/诊断观察在途任务）。

package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// prefetchTaskTimeout 单个预取任务的独立超时兜底：预取 goroutine 不使用请求的 context
// （请求结束会被取消），用独立 context 计时——任务整体（含排队等待并发额度与逐个
// blob 回源）超过该时长即放弃剩余 layer，已写入缓存的 layer 保留。
const prefetchTaskTimeout = 5 * time.Minute

// dockerManifestMediaTypes 单平台 image manifest 的 Content-Type 基名集合（小写）。
// 只收录「单平台」两类：docker schema2 与 OCI image manifest；manifest list /
// OCI index（多平台）不收录——客户端拿到 index 后会立刻再请求具体平台的 child
// manifest，届时自然触发预取（见本文件头注释）。
var dockerManifestMediaTypes = map[string]struct{}{
	"application/vnd.docker.distribution.manifest.v2+json": {},
	"application/vnd.oci.image.manifest.v1+json":           {},
}

// imageManifestBody 单平台 image manifest 中预取需要的字段（其余字段一律忽略）。
type imageManifestBody struct {
	// Config 镜像 config blob（容器文件系统元数据，按 digest 寻址），客户端拉镜像必取。
	Config struct {
		Digest string `json:"digest"`
	} `json:"config"`
	// Layers 镜像 layer blob 列表（digest 寻址、内容不可变）。
	Layers []struct {
		Digest string `json:"digest"`
	} `json:"layers"`
}

// maybePrefetchImageLayers 在「路径目标 GET manifest 回源成功且已写入缓存」后按触发
// 条件异步预取该镜像其余 layer blob 写入缓存（由 finalizeFetchEntry 在写盘成功后调用，
// 仅 200 且过判据的响应能到达这里）。逐项判定触发条件，不满足静默返回：
//   - docker-prefetch-enabled 关闭 / 缓存实例不可用（disk==nil）/ 条目为空（防御）；
//   - 非 GET（HEAD 不触发；finalize 只在 GET 路径被调用，这里再兜底一次）；
//   - 状态码非 200（206/Range 本就绕过缓存到不了这里，兜底防御）；
//   - 目标不是 /v2/<name>/manifests/<ref>（blob 请求 / web 完整 URL 目标等）；
//   - Content-Type 非单平台 image manifest（多平台不触发的原因见本文件头注释）；
//   - manifest JSON 解析失败 / 无可预取的 blob（body 已在缓存、客户端响应早已完成，
//     仅 Debug 留痕跳过）。
//
// 触发后异步执行（prefetchWG 登记），绝不阻塞 manifest 响应；预取的任何失败只留痕，
// 绝不影响客户端主请求。
func (s *Server) maybePrefetchImageLayers(r *http.Request, target string, ent *cache.Entry, eff config.Options, disk *cache.Cache) {
	// //////////////////  触发条件逐项判定  start  ////////////////////////////////////////////
	if s.cfg == nil || disk == nil || ent == nil {
		return // 防御：缓存未写盘（disk==nil）预取无从落地；其余为绕过 New 构造的兜底
	}
	if !s.cfg.GlobalDockerPrefetchEnabled() {
		return // 全局开关关闭
	}
	if r == nil || r.Method != http.MethodGet {
		return // HEAD 不触发（HEAD 探测无 body 不写缓存，本到不了这里，兜底防御）
	}
	if ent.Status != http.StatusOK {
		return // 仅 200 触发（206/Range 绕过缓存本到不了这里，兜底防御）
	}
	name, ref, ok := parseDockerManifestTarget(target)
	if !ok {
		return // 非 /v2/<name>/manifests/<ref>（blob 请求 / web 完整 URL 目标等）
	}
	if !isSinglePlatformImageManifest(ent.Header.Get("Content-Type")) {
		return // 多平台 manifest list / OCI index / 未知类型不触发（原因见文件头注释）
	}
	var body imageManifestBody
	if err := json.Unmarshal(ent.Body, &body); err != nil {
		// body 已在缓存、客户端响应早已完成：解析失败只降级留痕（Debug）
		s.log.Debug("docker manifest 解析失败，跳过 layer 预取",
			"rule", eff.Rule, "name", name, "ref", truncate(ref, 128), "err", err)
		return
	}
	// config blob 在前（体积小、客户端必取），随后 layers；digest 不合法的单项剔除
	digests := make([]string, 0, len(body.Layers)+1)
	if validBlobDigest(body.Config.Digest) {
		digests = append(digests, body.Config.Digest)
	}
	for _, l := range body.Layers {
		if validBlobDigest(l.Digest) {
			digests = append(digests, l.Digest)
		}
	}
	if len(digests) == 0 {
		return // 无可预取的 blob（如 attestation 类空 manifest）
	}
	// //////////////////  触发条件逐项判定  end  //////////////////////////////////////////////

	s.log.Info("识别到 docker manifest，异步预取镜像 layer",
		"rule", eff.Rule, "name", name, "ref", truncate(ref, 128),
		"blobs", len(digests), "concurrency", s.cfg.GlobalDockerPrefetchConcurrency())

	// 异步执行：绝不阻塞 manifest 响应（响应此时早已写完，这里只是顺手触发的后台任务）
	s.prefetchWG.Add(1)
	go func() {
		defer s.prefetchWG.Done()
		// goroutine 边界的 panic 防护（与 candidates.go runCandidate 的同类防护同款理由）：
		// 后台 goroutine 的 panic 没有请求处理链兜底，不处理会让整个进程崩溃、
		// 所有在途请求陪葬——预取属旁路任务，留 Error 日志把影响隔离到本任务
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("docker layer 预取任务 panic，已按任务失败处理（不影响客户端请求）",
					"rule", eff.Rule, "name", name, "panic", rec)
			}
		}()
		s.prefetchImageLayers(r, name, ref, digests, eff, disk)
	}()
}

// prefetchImageLayers 逐个预取 manifest 引用的 blob（config + layers）：
// 顺序排队 + Server 级信号量限并发；任务级超时兜底（独立 context，绝不使用请求的
// context），到期放弃剩余 layer（已写入缓存的 layer 保留，客户端请求照常命中）。
func (s *Server) prefetchImageLayers(r *http.Request, name, ref string, digests []string, eff config.Options, disk *cache.Cache) {
	ctx, cancel := context.WithTimeout(context.Background(), prefetchTaskTimeout)
	defer cancel()
	for i, digest := range digests {
		// 任务级超时兜底：到期放弃剩余 layer（已写入缓存的 layer 保留）
		if ctx.Err() != nil {
			s.log.Debug("docker layer 预取任务超时，放弃剩余 blob",
				"rule", eff.Rule, "name", name, "ref", truncate(ref, 128),
				"done", i, "total", len(digests), "err", ctx.Err())
			return
		}
		s.prefetchOneBlob(ctx, r, name, digest, eff, disk)
	}
	s.log.Debug("docker layer 预取任务结束",
		"rule", eff.Rule, "name", name, "ref", truncate(ref, 128), "blobs", len(digests))
}

// prefetchOneBlob 预取单个 blob（/v2/<name>/blobs/<digest>）：
//  1. 已有未过期缓存命中 → 跳过；
//  2. 该 blob 已在预取在途集合 → 跳过（防止重复排队）；
//  3. 占 Server 级并发信号量（等待受任务 ctx 约束）后回源下载：与客户端请求同一回源
//     路径（候选轮询、超时覆盖、http-proxy、insecure TLS、docker 401 token dance、
//     缓存写入全复用），只写缓存、不产生客户端响应；与客户端并发请求同一 blob 时经
//     现有 flightGroup 合并（在途则不另起第二路下载，见 flight.go）。
//
// 失败降级：仅留痕（Warn/Debug），不重试，绝不影响客户端主请求。
func (s *Server) prefetchOneBlob(ctx context.Context, r *http.Request, name, digest string, eff config.Options, disk *cache.Cache) {
	blobTarget := "v2/" + name + "/blobs/" + digest
	// 缓存键与客户端 blob 请求同构（cacheKeyInput 对路径目标的输入：http://<Host>/<目标>），
	// 保证 flight 合并与缓存命中都对得上客户端的真实 blob 请求
	blobKey := cache.Key(config.BuildMatchStr(r.Host, blobTarget))

	// 已有未过期缓存命中 → 跳过（Get 内含过期惰性删除语义，不重复下载）
	if _, ok := disk.Get(blobKey); ok {
		s.log.Debug("预取跳过：blob 缓存已命中", "rule", eff.Rule, "name", name, "digest", digest)
		return
	}
	// 该 blob 已在预取在途集合 → 跳过（防止重复排队）
	if !s.prefetchInflightAdd(blobKey) {
		s.log.Debug("预取跳过：同 blob 已在预取在途", "rule", eff.Rule, "name", name, "digest", digest)
		return
	}
	defer s.prefetchInflightRemove(blobKey)

	// Server 级有界并发：信号量等待受任务 ctx 约束（超时放弃，绝不悬挂）
	slots := s.prefetchSlotsChan()
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		s.log.Debug("预取放弃：等待并发额度超时",
			"rule", eff.Rule, "name", name, "digest", digest, "err", ctx.Err())
		return
	}

	// 任务剩余预算折算为本 blob 的回源约束：flight 等待上限与单次尝试总超时（http-timeout）
	// 都取剩余预算——独立 ctx 的取消无法穿透回源链路的 WithoutCancel 解耦，
	// 借既有配置字段传递预算是该模型内最直接的兜底方式（候选级显式 http-timeout
	// 优先级更高，保持既有三级覆盖语义不变，仅未显式配置的候选被预算兜底）。
	effTry := eff
	waitLimit := time.Duration(0)
	if d, ok := ctx.Deadline(); ok {
		if rem := time.Until(d); rem > 0 {
			waitLimit = rem
			effTry.HTTPTimeout = rem
		} else {
			return // 预算已耗尽：下轮循环的任务级检查会统一退出
		}
	}

	// 回源下载（经现有 flightGroup）：本调用为执行方时走「与客户端 blob 请求完全相同的
	// 缓冲回源路径（候选竞速 + 排干为完整 Entry）+ 既有缓存写入语义」；为等待方时
	// 共享客户端在途回源的结果（缓存由客户端执行方写入，本调用不再重复写）。
	blobReq := prefetchBlobRequest(r, blobTarget)
	e, executed, err := s.flight.DoLimit(blobKey, waitLimit, func() (*cache.Entry, error) {
		ent, ferr := s.fetchViaCandidates(blobReq, blobTarget, effTry)
		if ferr != nil {
			return nil, ferr
		}
		// 既有缓存写入语义复用（仅 200 且过判据写缓存；成功时留「写入缓存成功」日志）
		s.finalizeFetchEntry(blobReq, blobKey, blobTarget, ent, effTry, disk)
		return ent, nil
	})
	switch {
	case errors.Is(err, errFlightWaitTimeout):
		// 等待客户端在途回源超限：客户端执行方会自行写缓存，预取让路即可
		s.log.Debug("预取等待同 blob 单飞回源超限，交由客户端既有回源填缓存",
			"rule", eff.Rule, "name", name, "digest", digest)
	case err != nil:
		// 失败降级：仅留痕，绝不影响客户端主请求（客户端后续请求照走原路径）
		s.log.Warn("预取 blob 回源失败（不影响客户端，客户端请求照走原路径）",
			"rule", eff.Rule, "name", name, "digest", digest,
			"target", truncate(blobTarget, 300), "err", err)
	case !executed:
		// 等待方共享到客户端在途回源的结果：缓存已由客户端执行方写入
		s.log.Debug("预取合并客户端在途回源完成（缓存由执行方写入）",
			"rule", eff.Rule, "name", name, "digest", digest)
	default:
		// 本调用为执行方且回源成功：留痕实际落盘文件（bytes 为 0 的防御分支仅兜 nil）
		bytes := 0
		if e != nil {
			bytes = len(e.Body)
		}
		s.log.Info("预取 blob 写入缓存完成", "rule", eff.Rule, "name", name,
			"digest", digest, "bytes", bytes, "file", disk.FilePath(blobKey))
	}
}

// prefetchBlobRequest 构造 blob 预取用的请求对象：以触发预取的 manifest 请求为模板克隆
// （复用其请求头与入站 Host——候选模板变量展开与上游请求头白名单都基于它），仅把 URL
// 路径改写为 blob 路径并清掉查询串——保证模板里 $1 与 $http.server.header.full_url /
// full_url_no_server 都按 blob 请求展开（与客户端真实 blob 请求同构），而非残留在
// manifest 路径上。Accept 等请求头沿用 manifest 请求（registry 对 blob 无内容协商语义）。
// 克隆自带请求 context 仅作值传递底版，回源链路全部经 WithoutCancel 与请求断开解耦，
// 请求结束后预取不受影响。
func prefetchBlobRequest(r *http.Request, blobTarget string) *http.Request {
	br := r.Clone(r.Context()) // Clone 深拷贝 Header/URL/TLS 等，互不共享底层数据
	u := *r.URL
	u.Path = "/" + blobTarget
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	br.URL = &u
	return br
}

// parseDockerManifestTarget 判断路径目标是否为 docker manifest 请求并拆出仓库名与引用：
// 形如 v2/<name>/manifests/<ref>（target 为 ExtractTarget 输出的路径目标原文：不带
// scheme 与前导 /，可能带查询串）。<name> 支持多级仓库（如 library/nvidia/cuda）；
// <ref> 为 tag 或 digest（如 latest / sha256:...），digest 形式的 manifest 请求同样
// 触发（客户端校验阶段会按 digest 拉 manifest）。非 manifest 请求返回 ok=false。
func parseDockerManifestTarget(target string) (name, ref string, ok bool) {
	t := target
	if i := strings.IndexByte(t, '?'); i >= 0 {
		t = t[:i] // 去掉查询串（registry 的 manifest 请求一般无查询串，兜底剥离）
	}
	t = strings.TrimPrefix(t, "/") // 兜底：容忍前导 /（ExtractTarget 输出本就没有）
	segs := strings.Split(t, "/")
	// v2 / <name 至少一段> / manifests / <ref>：至少 4 段，倒数第 2 段必须是 manifests
	if len(segs) < 4 || segs[0] != "v2" || segs[len(segs)-2] != "manifests" {
		return "", "", false
	}
	name = strings.Join(segs[1:len(segs)-2], "/")
	ref = segs[len(segs)-1]
	if name == "" || ref == "" {
		return "", "", false
	}
	return name, ref, true
}

// isSinglePlatformImageManifest 判断响应 Content-Type 是否为单平台 image manifest：
// 取分号前的基名（容忍 ;charset=... 等参数后缀）trim + 小写后比对已知集合；
// manifest list / OCI index（多平台）与未知类型返回 false（多平台不触发的原因见
// 本文件头注释）。
func isSinglePlatformImageManifest(contentType string) bool {
	base := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	_, ok := dockerManifestMediaTypes[base]
	return ok
}

// validBlobDigest 校验 manifest 里的 digest 可安全作为 URL 单段路径拼接：
// 非空且不含 / ? # 空白与控制字符（registry digest 形如 sha256:<hex>，这里做宽松的
// 路径安全校验、不锁算法与长度——只为防御异常上游构造出越界路径，缓存键是整串的
// sha256、不落文件名，无路径穿越风险）。
func validBlobDigest(digest string) bool {
	if digest == "" {
		return false
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if c <= ' ' || c == 0x7f || c == '/' || c == '?' || c == '#' {
			return false
		}
	}
	return true
}

// prefetchInflightAdd 登记预取在途：同 key 已在途时返回 false（调用方跳过，防重复
// 排队）。key 为 blob 缓存键（含入站 Host 与 name/digest），同一 blob 的预取只排队一次。
func (s *Server) prefetchInflightAdd(key string) bool {
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	if s.prefetchInflight == nil { // 防御：绕过 New 构造的 Server 也能安全写入
		s.prefetchInflight = make(map[string]struct{})
	}
	if _, ok := s.prefetchInflight[key]; ok {
		return false
	}
	s.prefetchInflight[key] = struct{}{}
	return true
}

// prefetchInflightRemove 解除预取在途登记（幂等）。
func (s *Server) prefetchInflightRemove(key string) {
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	delete(s.prefetchInflight, key)
}

// prefetchSlotsChan 返回 Server 级预取并发信号量：New 已按配置预构建；绕过 New 构造的
// Server 惰性补建（防御风格与 dockerTokens 一致）。
func (s *Server) prefetchSlotsChan() chan struct{} {
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	if s.prefetchSlots == nil {
		s.prefetchSlots = make(chan struct{}, s.cfg.GlobalDockerPrefetchConcurrency())
	}
	return s.prefetchSlots
}

// waitPrefetchIdle 等待全部在途预取任务结束（仅供测试/诊断观察；进程退出不调用——
// 预取 goroutine 不持有客户端资源且任务自带超时兜底，随进程消亡即可，绝不悬挂退出）。
func (s *Server) waitPrefetchIdle() {
	s.prefetchWG.Wait()
}
