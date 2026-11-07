// docker_mutable.go —— docker registry 路径目标的「可变性」判定（读写缓存是否放行）：
// 背景（2026-11-07 现场）：docker 规则 cache_ttl=666h 对「内容寻址」目标（blob、
// digest manifest）是正确的——内容不可变，长缓存正是代理缓存的价值；但 GET/HEAD
// /v2/<name>/manifests/<tag>（如 latest）此前被一视同仁写进缓存长驻，后续 HEAD
// manifests/latest 复用旧条目回放过期 Docker-Content-Digest，docker 客户端直接判
// 「Image is up to date」——上游 hub 已推送新镜像，代理却永远回旧 digest，新版本
// 拉不到也无法触发下载（现场：adockero/proxy-cache:latest 卡在旧 digest 988e507a）。
//
// registry v2 语义：内容寻址 = 不可变，名字寻址 = 可变：
//   - 不可变（可缓存）：v2/<name>/blobs/<digest>、v2/<name>/manifests/<algo:hex>；
//   - 可变（不得读写缓存）：
//     v2/<name>/manifests/<tag>（tag 指向内容随每次 push 变化）；
//     v2/<name>/referrers/<digest>（列表 append-only：后续 cosign 签名 / buildx
//     attestation 会向同一 digest 的 referrers 追加，读旧 = 静默丢新签名）；
//     v2/<name>/tags/list（push / delete tag 都会变）。
//
// 可变目标读旧缓存比不缓存更糟（静默给出过期数据），故 GET 与 HEAD 一律 BYPASS：
// 既不读缓存（绝不回放旧条目）也不写缓存（不在磁盘长驻必过期数据）。
package proxy

import "strings"

// dockerMutableTarget 判断路径目标是否为 docker「可变目标」（不得读写缓存）：
// target 为 ExtractTarget 输出的路径目标原文（不带 scheme 与前导 /，可能带查询串；
// 容忍前导 /，与 parseDockerBlobNameDigest 同款宽松）。完整 URL（web）目标恒 false，
// 行为不变。digest 形态判定用「ref 含冒号」宽松实现：registry tag 语法禁止冒号，
// 含冒号只可能是 algo:hex 的 digest（不校验 hex 长度——非法 digest 上游会 404，
// 不影响可变性判定）。
func dockerMutableTarget(target string) bool {
	t := target
	if i := strings.IndexByte(t, '?'); i >= 0 {
		t = t[:i] // 去掉查询串（referrers 支持 ?artifactType= 过滤）
	}
	t = strings.TrimPrefix(t, "/") // 兜底：容忍前导 /（ExtractTarget 输出本就没有）
	segs := strings.Split(t, "/")
	// v2 / <name 至少一段> / <manifests|referrers|tags> / <尾段>：至少 4 段
	if len(segs) < 4 || segs[0] != "v2" {
		return false
	}
	switch second := segs[len(segs)-2]; {
	case second == "manifests":
		// manifests/<ref>：digest 形态（含冒号）不可变可缓存；tag 形态可变
		return !strings.Contains(segs[len(segs)-1], ":")
	case second == "referrers":
		// referrers/<digest>：列表 append-only，同一 digest 的响应会随签名/attestation 追加
		return true
	case second == "tags" && segs[len(segs)-1] == "list":
		// tags/list：tag 集合随 push / delete 变化
		return true
	}
	return false
}
