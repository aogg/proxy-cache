package proxy

// probe_budget_test.go —— 探测预算（probe-timeout）计时口径的单测（probe.go）：
// 本轮把预算终点从「响应头可用且首块/EOF 可判定」提前到「**最终响应头到达**」（onHeaders
// 回调里就地停表）。现场证据：1ms.run 已在 5s 内回 206（token dance 花 2.9s），却因为还在
// 等首块被 probe-timeout 判硬失败丢弃；配了 url-redirect-all-error-retry 也因扫描预算耗尽
// 形同虚设。响应头到手即解除预算，后续首块/body 读取由 read-timeout（无新增数据）与
// http-timeout（整次尝试）兜底。
//
// 全部使用 httptest 假上游（不启动本服务、不连真实 registry），通过极短 probe-timeout +
// 延迟首块精确验证「预算停在响应头」这一口径，并保留「响应头都等不到仍按预算硬失败」的
// 反向用例（防止把预算无意中整体关掉）。

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// probeBudgetTarget 是本组用例使用的 docker blob 目标（仅作日志/解析用途）。
var probeBudgetTarget = "v2/probe-demo/app/blobs/sha256:" + strings.Repeat("ab", 32)

// newProbeBudgetServer 启动一个「先写响应头、再延迟发首块」的假上游，并构建最小 Server
// 与候选（尝试级超时全部取候选显式值，probe-timeout 由用例指定）。
//   - headerDelay：写出响应头前的延迟（模拟「响应头都等不到」）；
//   - firstBlockDelay：写出响应头之后、发出首块之前的延迟（模拟「头到了、首块一直不来」）。
func newProbeBudgetServer(t *testing.T, headerDelay, firstBlockDelay time.Duration, probeTimeout time.Duration) (*Server, candidate) {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if headerDelay > 0 {
			time.Sleep(headerDelay)
		}
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(http.StatusOK)
		hubFlush(w) // 立即把响应头送到代理（首块继续延迟，复现现场「头已到、首块未到」）
		if firstBlockDelay > 0 {
			time.Sleep(firstBlockDelay)
		}
		_, _ = w.Write([]byte("hello"))
	}))
	t.Cleanup(up.Close)
	return newPartialTestServer(t), candidate{
		url:            up.URL + "/" + probeBudgetTarget,
		connectTimeout: 5 * time.Second,
		httpTimeout:    5 * time.Second,
		readTimeout:    5 * time.Second,
		probeTimeout:   probeTimeout,
	}
}

// probeBudgetAttempt 向候选发起一次探测式尝试（eff 取本 Server 的生效配置，目标取 blob
// 形态以便与线上同形；尝试级取值全部在 cand 内，eff 仅维持调用签名）。
func probeBudgetAttempt(t *testing.T, s *Server, cand candidate) (*probeResult, error, bool, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+probeBudgetTarget, nil)
	eff := s.cfg.ResolveOptions(r.Host, probeBudgetTarget, false)
	return s.probeCandidateAttempt(context.Background(), r, cand, eff)
}

// TestProbeCandidateAttemptBudgetStopsAtHeaders 覆盖点 5：
//   - 响应头立即到、首块 200ms 后才到，probe-timeout 只给 50ms：响应头到达即停表，
//     本次尝试必须成功（不再被判 probe-timeout）——若预算仍把首块算在内，50ms 处就会
//     取消在途请求并按硬失败丢弃；
//   - 响应头 400ms 后才到，probe-timeout 50ms：仍按 probe-timeout 硬失败——预算的原始
//     目的（别让 docker 等不到响应头）保持不变。
func TestProbeCandidateAttemptBudgetStopsAtHeaders(t *testing.T) {
	const probeBudget = 50 * time.Millisecond

	t.Run("响应头到达即停表_首块迟到仍成功", func(t *testing.T) {
		// 首块延迟（200ms）是探测预算（50ms）的 4 倍：只有预算真的停在响应头，
		// 这次尝试才可能成功。
		s, cand := newProbeBudgetServer(t, 0, 200*time.Millisecond, probeBudget)
		res, err, hard, reason := probeBudgetAttempt(t, s, cand)
		if err != nil {
			t.Fatalf("响应头已在探测预算内到达，不应判失败: %v（hard=%v reason=%q）", err, hard, reason)
		}
		if hard || reason != "" {
			t.Errorf("不应按硬失败处理: hard=%v reason=%q", hard, reason)
		}
		if res == nil {
			t.Fatal("探测成功时应返回结果")
		}
		defer res.Close()
		if string(res.first) != "hello" {
			t.Errorf("首块 = %q, want %q（预算解除后应继续等首块）", res.first, "hello")
		}
	})

	t.Run("响应头等不到_仍按探测预算硬失败", func(t *testing.T) {
		// 响应头 400ms 才到（探测预算 8 倍）：预算必须在 50ms 处触发并硬失败，
		// 保证「上游连响应头都不给」时仍能快速切下一候选。
		s, cand := newProbeBudgetServer(t, 400*time.Millisecond, 0, probeBudget)
		start := time.Now()
		res, err, hard, reason := probeBudgetAttempt(t, s, cand)
		if res != nil {
			res.Close()
			t.Fatal("响应头未在预算内到达，不应返回结果")
		}
		if err == nil {
			t.Fatal("响应头未在预算内到达，应返回错误")
		}
		if !hard || reason != "probe-timeout" {
			t.Errorf("应为 probe-timeout 硬失败: hard=%v reason=%q err=%v", hard, reason, err)
		}
		if !strings.Contains(err.Error(), "probe-timeout") {
			t.Errorf("错误信息应含 probe-timeout，实际: %v", err)
		}
		// 粗校验「预算到期即时收场」：没有拖到远超预算（1s）。真正区分「预算是否仍然生效」
		// 的是上面的 err/hard/reason 断言——若预算被整体关掉，400ms 响应头到达后本次尝试
		// 会成功（err == nil），用例同样会失败；这里只留一条宽松的耗时分寸检查，避免在
		// 高负载环境下因调度抖动偶发误判。
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("probe-timeout 应在 %s 预算附近收场，实际耗时 %s", probeBudget, elapsed)
		}
	})
}

// TestExpandCandidatesProbeTimeoutByTarget 覆盖 CandidateProbeTimeoutFor 在候选展开处的接线：
// docker blob 目标在「候选级/规则级/全局都未显式配置」时用 30s 缺省，manifest 等目标维持
// 5s 快速失败节奏；任一层显式配置（含显式 0 = 不限制）都优先于 blob 缺省。
func TestExpandCandidatesProbeTimeoutByTarget(t *testing.T) {
	blobTarget := "v2/demo/app/blobs/sha256:" + strings.Repeat("ab", 32)
	manifestTarget := "v2/demo/app/manifests/latest"
	item := func() []config.URLRedirectItem {
		return []config.URLRedirectItem{{Template: "https://mirror.example.com/$1"}}
	}
	baseEff := func() config.Options {
		return config.Options{
			URLRedirect:             item(),
			URLRedirectHedgeDelay:   2 * time.Second,
			URLRedirectProbeTimeout: config.DefaultURLRedirectProbeTimeout,
		}
	}
	tests := []struct {
		name   string
		target string
		items  []config.URLRedirectItem
		eff    func() config.Options
		want   time.Duration
	}{
		{
			name:   "blob未显式_用30s缺省",
			target: blobTarget,
			items:  item(),
			eff:    baseEff,
			want:   config.DefaultBlobURLRedirectProbeTimeout,
		},
		{
			name:   "manifest未显式_维持5s",
			target: manifestTarget,
			items:  item(),
			eff:    baseEff,
			want:   config.DefaultURLRedirectProbeTimeout,
		},
		{
			name:   "blob全局显式7s_覆盖缺省",
			target: blobTarget,
			items:  item(),
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirectProbeTimeout = 7 * time.Second
				o.URLRedirectProbeTimeoutExplicit = true
				return o
			},
			want: 7 * time.Second,
		},
		{
			name:   "blob候选级显式9s_覆盖全局",
			target: blobTarget,
			items:  []config.URLRedirectItem{{Template: "https://mirror.example.com/$1", ProbeTimeout: durPtr(9 * time.Second)}},
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirectProbeTimeout = 7 * time.Second
				o.URLRedirectProbeTimeoutExplicit = true
				return o
			},
			want: 9 * time.Second,
		},
		{
			name:   "blob全局显式0不限制_保持0",
			target: blobTarget,
			items:  item(),
			eff: func() config.Options {
				o := baseEff()
				o.URLRedirectProbeTimeout = 0
				o.URLRedirectProbeTimeoutExplicit = true
				return o
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cands := ExpandCandidates(tt.items, RequestVars{Target: tt.target}, tt.eff())
			if len(cands) != 1 {
				t.Fatalf("候选数 = %d, want 1", len(cands))
			}
			if got := cands[0].probeTimeout; got != tt.want {
				t.Errorf("probeTimeout = %s, want %s（blob 目标未显式配置时套用 30s 缺省）", got, tt.want)
			}
		})
	}
}
