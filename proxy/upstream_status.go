// upstream_status.go —— 候选全败时的「上游明确 4xx 响应」透传载荷：
// 背景：部分镜像站对某些 registry 端点（如 OCI referrers 端点 /v2/<name>/referrers/<digest>）
// 一律回 404，而 docker 客户端对这类端点语义自带容错（referrers 404 会自行 fallback，
// 不影响 pull）。旧版候选全败后一律 writeError 502，docker 会判「整层拉取失败」导致
// pull 中断；正确语义是把上游明确的 4xx 状态码与响应原样透传给客户端。
//
// 机制：候选探测拿到明确上游 4xx 响应（判据必拒）时，把「状态码 + 可透传响应头 + body
//（探测量级，≤probeFirstChunkLimit）」装进本错误（包住原始失败描述，Error() 文案不变，
// 日志与 errors.Is/As 兼容）；候选全败后它随最终错误上浮，由客户端出口（proxy.go 的
// streamingError / flight 执行方错误 / handleHead）判定透传。
//
// 透传判定条件（见 newUpstreamStatusError 与各出口）：
//   - 状态码 ∈ [400, 500) 且 ≠ 401——401 由既有匿名 token dance 语义处理（dance 后仍
//     401 属硬失败，维持既有 502 行为，不透传）；
//   - 仅「拿到了明确上游响应」的失败产生载荷：网络错误 / 超时（含候选扫描总预算耗尽，
//     见 candidates.go 的预算出口剥离载荷）不产生载荷，维持 502；
//   - 适用场景：referrers 404、未知端点 405、限流 429 等客户端能理解的 registry 语义
//     错误；换下一候选重试的既有语义完全不变（载荷只在全部候选失败后上浮）。
//
// 安全性：header 为该次探测独立深拷贝（newProbeResultFromResponse 构建，构建后只读），
// body 为探测首块（readProbeFirst 的独立缓冲切片），随错误值只读共享，无数据竞争。

package proxy

import (
	"errors"
	"net/http"
)

// upstreamStatusError 携带「上游明确 4xx 响应」的候选失败错误：err 为原始失败描述
//（判据不过 / 硬失败包装），status/header/body 为可透传给客户端的上游响应三元组。
type upstreamStatusError struct {
	// status 上游响应状态码（∈ [400,500) 且 ≠ 401）。
	status int
	// header 上游响应头深拷贝（透传时经 skipResponseHeader 过滤逐跳头与自管头）。
	header http.Header
	// body 上游响应 body（探测量级：探测首块，≤probeFirstChunkLimit）。
	body []byte
	// url 产生该响应的候选 URL（透传时输出 X-Proxy-Upstream 观测头与日志留痕）。
	url string
	// err 原始候选失败错误（Unwrap 目标，日志文案保持既有格式）。
	err error
}

// Error 返回原始失败描述（与未加载荷时的日志文案完全一致）。
func (e *upstreamStatusError) Error() string { return e.err.Error() }

// Unwrap 暴露原始失败错误（errors.Is/As 穿透；载荷判定用 errors.As 找 *upstreamStatusError）。
func (e *upstreamStatusError) Unwrap() error { return e.err }

// newUpstreamStatusError 从探测结果构建 4xx 透传载荷（非 4xx / 401 返回 nil，不产生载荷）：
// header/body 直接共享 probeResult 的深拷贝字段（构建后只读，见文件头安全性说明）。
func newUpstreamStatusError(res *probeResult, url string, cause error) *upstreamStatusError {
	if res == nil || cause == nil {
		return nil
	}
	if res.status < 400 || res.status >= 500 || res.status == http.StatusUnauthorized {
		return nil
	}
	return &upstreamStatusError{status: res.status, header: res.header, body: res.first, url: url, err: cause}
}

// asUpstreamStatusError 从错误链中提取 4xx 透传载荷（errors.As 穿透多层 %w 包装）。
func asUpstreamStatusError(err error) (*upstreamStatusError, bool) {
	var use *upstreamStatusError
	if errors.As(err, &use) && use != nil {
		return use, true
	}
	return nil, false
}

// stripUpstreamStatus 剥离错误链中的 4xx 透传载荷，返回裸错误（供「预算耗尽按超时
// 语义」等需要维持 502 的出口使用——载荷只允许在「拿到明确上游响应」的失败上浮）。
func stripUpstreamStatus(err error) error {
	if use, ok := asUpstreamStatusError(err); ok {
		return use.err
	}
	return err
}
