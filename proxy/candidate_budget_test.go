package proxy

// candidate_budget_test.go —— 候选扫描总预算按目标类型取值的单测（candidates.go）：
// 本轮给 docker blob 目标放大了扫描总预算（candidateScanBudgetBlob = 60s 下限，按
// 「(候选数-1)×hedge + 候选级探测预算最大值 + 20s 收尾余量」动态放大），保证 blob 的
// 30s 探测预算（config.DefaultBlobURLRedirectProbeTimeout）不会被 20s 总预算提前腰斩；
// manifest / web 等非 blob 目标维持既有 20s 快速失败节奏（行为与旧版完全一致）。
//
// 表驱动全部为纯函数断言（不启动上游、不发请求），显式 0 = 不限制时估算按 30s 处理
// （否则预算会被算成无穷大、永不放弃）。

import (
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestCandidateScanBudgetFor 覆盖 candidateScanBudgetFor 的目标类型分流与 blob 放大公式：
//   - 非 blob：固定 20s（candidateScanBudget）；
//   - blob：max(60s, (n-1)×hedge + maxProbe + 20s)；
//   - blob + 显式 probe-timeout 0（不限制）：估算按 30s 缺省，避免预算无穷大；
//   - blob + 候选级显式 90s：按 90s 估算（候选级优先于 blob 缺省）。
func TestCandidateScanBudgetFor(t *testing.T) {
	blobTarget := "v2/demo/app/blobs/sha256:" + strings.Repeat("ab", 32)
	const manifestTarget = "v2/demo/app/manifests/latest"
	baseEff := func() config.Options {
		return config.Options{
			URLRedirect:             []config.URLRedirectItem{{Template: "https://mirror.example.com/$1"}},
			URLRedirectHedgeDelay:   2 * time.Second,
			URLRedirectProbeTimeout: config.DefaultURLRedirectProbeTimeout,
		}
	}
	tests := []struct {
		name   string
		target string
		n      int
		eff    func() config.Options
		want   time.Duration
	}{
		{
			name:   "非blob目标固定20s",
			target: manifestTarget,
			n:      7,
			eff:    baseEff,
			want:   candidateScanBudget,
		},
		{
			// 5 候选：单轮最坏 = 4×2s + 30s + 20s = 58s < 60s 下限 → 取 60s
			name:   "blob5候选_按60s下限",
			target: blobTarget,
			n:      5,
			eff:    baseEff,
			want:   candidateScanBudgetBlob,
		},
		{
			// 10 候选：单轮最坏 = 9×2s + 30s + 20s = 68s > 60s → 动态放大到 68s
			name:   "blob10候选_动态放大到68s",
			target: blobTarget,
			n:      10,
			eff:    baseEff,
			want:   68 * time.Second,
		},
		{
			// 全局显式 0 = 不限制：估算按 blob 缺省 30s，否则预算无穷大（永不放弃）
			name:   "blob显式0不限制_估算按30s",
			target: blobTarget,
			n:      5,
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirectProbeTimeout = 0
				o.URLRedirectProbeTimeoutExplicit = true
				return o
			},
			want: candidateScanBudgetBlob,
		},
		{
			// 候选级显式 90s：单轮最坏 = 4×2s + 90s + 20s = 118s
			name:   "blob候选级90s_按118s",
			target: blobTarget,
			n:      5,
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirect = []config.URLRedirectItem{{
					Template:     "https://mirror.example.com/$1",
					ProbeTimeout: durPtr(90 * time.Second),
				}}
				return o
			},
			want: 118 * time.Second,
		},
		{
			// 候选列表为空（理论不会走到扫描）：maxProbe 按 blob 缺省 30s 估算
			// 2 候选：1×2s + 30s + 20s = 52s < 60s → 取 60s
			name:   "blob候选列表为空_按缺省估算",
			target: blobTarget,
			n:      2,
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirect = nil
				return o
			},
			want: candidateScanBudgetBlob,
		},
		{
			// 非 blob 目标即便候选级探测预算很大，也维持 20s 快速失败节奏
			name:   "非blob候选级90s仍20s",
			target: manifestTarget,
			n:      10,
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirect = []config.URLRedirectItem{{
					Template:     "https://mirror.example.com/$1",
					ProbeTimeout: durPtr(90 * time.Second),
				}}
				return o
			},
			want: candidateScanBudget,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := tt.eff()
			if got := candidateScanBudgetFor(tt.target, tt.n, o); got != tt.want {
				t.Errorf("candidateScanBudgetFor(%q, %d) = %s, want %s", tt.target, tt.n, got, tt.want)
			}
			// 续传扫描只服务 blob：预算恒等于 blobCandidateScanBudget（与本函数的 blob 分支同源）
			if got, want := resumeScanBudgetFor(tt.n, o), blobCandidateScanBudget(tt.n, o); got != want {
				t.Errorf("resumeScanBudgetFor(%d) = %s, want %s（应与 blob 扫描预算同源）", tt.n, got, want)
			}
		})
	}
}
