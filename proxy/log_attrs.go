// log_attrs.go —— docker 链路日志统一附加键（source 来源标签 + image 镜像标签）：
// docker 加速链路分散在多处（客户端流式主路径 / 搭车订阅 / blob 半截续传 / 后台预取），
// 此前各处日志字段不统一——预取日志用 name、搭车与续传日志只有 key/digest、客户端主
// 路径只有 target/path——既无法区分「异步下载」与「客户端请求下载」，也无法按镜像 grep。
// 本文件统一两件事，键名与附加顺序全仓库一致（统一经 dockerLogAttrs / dockerLogInfo.attrs
// 拼装，各日志点禁止散写）：
//   - source：本次日志行为的来源，四值枚举（见下方常量注释）——按 source 一眼区分
//     「异步下载」（prefetch）与「客户端请求下载」（client / ridealong / resume）；
//   - image：镜像仓库名（/v2/<name>/... 的 name，多级原样保留，如 foxhui/webai-2api），
//     作为统一标签输出，方便按镜像 grep；附属 digest（blob digest）与 ref（manifest 的
//     tag 或 digest 引用）适用才带。
//
// 键序约定：source 恒在首位、image 次之、digest/ref 再次，既有键（rule/candidate/key/
// target/err 等）原样追加在尾部。兜底约定：镜像名解析不出（非 /v2/ 请求、异常路径）时
// 省略 source/image/digest/ref 全部新键（不填空串），日志行为与旧版零变化；仅路径目标
// 为 /v2/ 的请求才附加这些键。
package proxy

// docker 链路日志 source 键的四值枚举。
const (
	// dockerLogSourceClient 客户端请求触发的回源：客户端流式主路径与 BYPASS
	//（含 HIT / MISS 回放与单飞等待超限后自行回源）。
	dockerLogSourceClient = "client"
	// dockerLogSourceRidealong 搭车订阅在途流（预取/续传条目）的客户端请求
	//（X-Cache: PREFETCH 回放，上游仅预取/续传那一路下载）。
	dockerLogSourceRidealong = "ridealong"
	// dockerLogSourceResume blob 半截续传执行方（客户端请求触发，X-Cache: RESUME）。
	dockerLogSourceResume = "resume"
	// dockerLogSourcePrefetch 后台异步预取（manifest 驱动的 layer 异步下载）。
	dockerLogSourcePrefetch = "prefetch"
)

// dockerLogAttrs docker 链路日志统一附加键（调用点已持有镜像名时用）：
// source 恒在首位、image 次之（空串省略——异常路径解析不出镜像名时不填空串），
// extra 为既有附加键（rule/digest/ref/key/target/err 等，原样追加在尾部）。
// 包级函数而非 Server 方法：hub 条目侧日志（writeBroadcast / cleanupFile，持有
// e.log 而非 Server）同样需要统一拼装。
func dockerLogAttrs(source, image string, extra ...any) []any {
	if image == "" {
		return extra // 镜像名解析不出：省略全部新键，仅保留既有键（行为与旧版一致）
	}
	attrs := make([]any, 0, len(extra)+4)
	attrs = append(attrs, "source", source, "image", image)
	return append(attrs, extra...)
}

// dockerLogInfo 路径目标的 docker 日志统一标签（parseDockerLogInfo 的解析结果）：
// image 为空表示非 docker（/v2/）请求——attrs 对其原样透传既有键，日志零变化。
type dockerLogInfo struct {
	// image 镜像仓库名（/v2/<name>/... 的 name，多级原样保留）。
	image string
	// digest blob digest（仅 blob 目标非空）。
	digest string
	// ref manifest 的 tag 或 digest 引用（仅 manifest 目标非空）。
	ref string
}

// parseDockerLogInfo 解析路径目标的 docker 日志统一标签（纯字符串 split，供日志点
// 按需调用，不改热路径函数签名传参链）：
//   - blob 目标 v2/<name>/blobs/<digest>：image = 仓库名、digest = blob digest；
//   - manifest 目标 v2/<name>/manifests/<ref>：image = 仓库名、ref = tag 或 digest 引用；
//   - 其余目标（web 完整 URL / 非 /v2/ 路径）：零值（image 为空）——日志不加新键。
func parseDockerLogInfo(target string) dockerLogInfo {
	if name, digest, ok := parseDockerBlobNameDigest(target); ok {
		return dockerLogInfo{image: name, digest: digest}
	}
	if name, ref, ok := parseDockerManifestTarget(target); ok {
		return dockerLogInfo{image: name, ref: ref}
	}
	return dockerLogInfo{}
}

// attrs 输出统一附加键：source 恒首位、image/digest/ref 次之（digest/ref 适用才带，
// 空值省略），extra 原样追加在尾部；非 docker 请求（image 为空）原样返回 extra——
// 日志行为与旧版零变化。
func (i dockerLogInfo) attrs(source string, extra ...any) []any {
	if i.image == "" {
		return extra
	}
	head := make([]any, 0, len(extra)+8)
	head = append(head, "source", source, "image", i.image)
	if i.digest != "" {
		head = append(head, "digest", i.digest)
	}
	if i.ref != "" {
		head = append(head, "ref", i.ref)
	}
	return append(head, extra...)
}

// dockerLogInfoOf 取出可选 variadic 中的首个标签（无则零值；与 partialSinkOf 同款风格，
// 供既有定参调用方与既有测试不受影响的标签注入，见 savePartialPart / keepPrefetchPartial）。
func dockerLogInfoOf(all []dockerLogInfo) dockerLogInfo {
	if len(all) == 0 {
		return dockerLogInfo{}
	}
	return all[0]
}
