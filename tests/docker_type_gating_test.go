// docker_type_gating_test.go —— 规则缺 `type: docker` 时 docker 专属特性「静默失效」的
// 负向断言族（缺口#8）。经公开 API 逐一实证后的覆盖边界（与任务书前提的差异见各用例
// 注释与最终报告「跳过项」）：
//
//   - blob-concurrency（时间驱动多源补充）：**确实受 type 门控**——stream.go /
//     partial_resume.go 的启用条件为 RuleType==docker && interval>0，且类型不符时输出
//     专门留痕「未补充多源并发 reason=not_docker_rule」。本文件给出负向（type 留空 →
//     第二候选绝不被请求）+ 正向对照（type: docker 同配置 → 第二候选被补充）成对断言。
//   - slowdownload 判速升级：同门控（stream.go RuleType==docker），proxy 包已有
//     TestSlowDownloadRuleTypeWebNoEscalation 负向用例，此处不重复。
//   - partial-resume / 宽限终止 / prefetch：**实证不受规则 type 门控或触发链路不可达**
//     （详见报告跳过项）：type 留空时半截续传完整接管（立即响应/候选命中/宽限终止日志
//     齐全）；prefetch 的触发链路（manifest GET 按 docker-mutable BYPASS，不经
//     finalizeFetchEntry）无法经公开 API 达到可断言形态。为它们写「静默关闭」负向断言
//     会与现实现相悖（红测）。
//
// 确定性设计：胜者候选按固定节奏慢速供数（时间驱动补充的间隔必然在流中途到期），
// 第二候选命中计数为状态信号；无 sleep 赌时序。
package tests

import (
	"net/http"
	"testing"
	"time"

	"proxy-cache/config"
)

// gatingData 门控族测试 blob：512KB。
var gatingData = blobPattern(512 * 1024)

// gatingDigest / gatingTarget / gatingBlobPath 门控族测试 blob 标识。
func gatingDigest() string   { return digestOf(gatingData) }
func gatingTarget() string   { return blobTarget("app/gating", gatingDigest()) }
func gatingBlobPath() string { return "/" + gatingTarget() }

// gatingUpstreams 构造双候选假上游：慢速胜者（32KB/150ms，512KB ≈2.4s 供完，保证
// 250ms 的补充间隔必然在流中途到期）+ Range 续传补充源。返回两者 URL 与补充源计数。
func gatingUpstreams(t *testing.T) (winnerURL, suppURL string, suppHits *hitsCounter) {
	t.Helper()
	winner := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", itoa(len(gatingData)))
		w.WriteHeader(http.StatusOK)
		for off := 0; off < len(gatingData); off += chunk32K {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk32K
			if end > len(gatingData) {
				end = len(gatingData)
			}
			if _, err := w.Write(gatingData[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(150 * time.Millisecond)
		}
	})
	supp := upstreamRangeCapable(t, gatingData)
	return winner.URL, supp.srv.URL, supp.hits
}

// gatingEnv 组装门控测试环境：ruleType 定门控行为，interval 定补充节奏。
func gatingEnv(t *testing.T, ruleType string, interval time.Duration) (*testEnv, *hitsCounter) {
	t.Helper()
	winnerURL, suppURL, suppHits := gatingUpstreams(t)
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.Type = ruleType
		rule.URLRedirect = &[]config.URLRedirectItem{
			{Template: winnerURL + "/$1"},
			{Template: suppURL + "/$1"},
		}
		rule.BlobConcurrencyInterval = ptrDuration(interval)
		// 时间驱动补充的剩余量门槛：缺省 64MB 会把 512KB 的测试 blob 判成
		// remaining_too_small 直接跳过——显式收紧到 1KB 让「间隔到期即评估、评估即补充」。
		rule.SlowDownloadMinRemaining = ptrBytes(1024)
	})
	return env, suppHits
}

// TestBlobConcurrencyGatedByRuleType 缺口#8 成对断言：时间驱动多源补充
//   - 正向对照（type: docker）：interval 到期后补充第二候选（Range 请求到达 + 留痕）；
//   - 负向（type 留空）：整段下载仅胜者一路，第二候选 0 请求，且留「未补充多源并发
//     reason=not_docker_rule」专门日志——静默失效可观测。
func TestBlobConcurrencyGatedByRuleType(t *testing.T) {
	t.Run("type_docker_时间驱动补充生效", func(t *testing.T) {
		env, suppHits := gatingEnv(t, config.RuleTypeDocker, 250*time.Millisecond)
		done := env.serveWithWriter(http.MethodGet, gatingBlobPath(), nil, &countingWriter{ResponseWriter: httpRecorder()})
		waitFor(t, 5*time.Second, "补充源被请求", func() bool { return suppHits.Load() >= 1 })
		if n := countLines(env.logText(), "补充多源并发下载"); n < 1 {
			t.Errorf("type:docker 正向对照应留「补充多源并发下载」日志，实际 %d 行", n)
		}
		if n := countLines(env.logText(), "reason=not_docker_rule"); n != 0 {
			t.Errorf("type:docker 不应出现 not_docker_rule 留痕，实际 %d 行", n)
		}
		<-done
		if n := suppHits.Load(); n < 1 {
			t.Errorf("补充源应至少收到 1 次 Range 续传请求，实际 %d", n)
		}
	})

	t.Run("缺type补充静默关闭", func(t *testing.T) {
		env, suppHits := gatingEnv(t, "", 250*time.Millisecond)
		cw := &countingWriter{ResponseWriter: httpRecorder()}
		env.getWithWriter(gatingBlobPath(), nil, cw)
		if got := cw.Bytes(); got != int64(len(gatingData)) {
			t.Fatalf("客户端仍应收到完整 blob %d 字节（特性失效不影响基本功能），实际 %d", len(gatingData), got)
		}
		if !bytesEqual(cw.body(), gatingData) {
			t.Fatalf("胜者单路供数内容应与原文逐字节一致")
		}
		if n := suppHits.Load(); n != 0 {
			t.Errorf("缺 type:docker 时第二候选绝不应被补充请求，实际 %d 次", n)
		}
		log := env.logText()
		if n := countLines(log, "未补充多源并发"); n < 1 {
			t.Errorf("应留「未补充多源并发」专门日志，实际 %d 行:\n%v", n, logLines(log, "未补充多源并发"))
		}
		if n := countLines(log, "reason=not_docker_rule"); n < 1 {
			t.Errorf("未补充原因应为 not_docker_rule，实际 %d 行", n)
		}
		if n := countLines(log, "补充多源并发下载"); n != 0 {
			t.Errorf("缺 type:docker 不应出现补充留痕，实际 %d 行", n)
		}
	})
}
