// merger_pump_race_test.go —— slowMerger pump 并发时序回归压力（缺口#10，session-7b5126f5
// -race 复现的历史竞态：pump 协程启动即无锁读 src.res.body，而 take()/closeAll() 锁外调
// res.Close() 写 body=nil；修复 = 源上自有字段在「定 res 之后、go pump 之前」一次性捕获）。
// 黑盒回归：多轮判速升级压力（正确/错位补充源交替），在 -race 下锁定该约束——若回归为
// 「go pump 后才捕获/直接引用 res.body」，竞速窗口会在多轮高频升级中被检测器命中；
// 非 -race 运行时退化为功能正确性回归（客户端字节逐轮完整且正确）。
//
// 确定性设计：与 reconcile_crosscheck 同构的判速升级（时间窗判慢 + 补充源 Range 续传），
// 胜者节拍缩短（100ms/块、窗口 700ms）压缩单轮时长；补充源首块必与胜者缓冲重叠
// （错位轮被 cross-check 剪枝、正确轮全部存活），断言逐轮确定。
package tests

import (
	"net/http"
	"testing"
	"time"

	"proxy-cache/config"
)

// pumpRaceData 压力轮 blob：512KB = 16 × 32KB。
var pumpRaceData = blobPattern(512 * 1024)

// pumpRaceDigest / pumpRaceTarget / pumpRaceBlobPath 压力轮 blob 标识。
func pumpRaceDigest() string   { return digestOf(pumpRaceData) }
func pumpRaceTarget() string   { return blobTarget("app/pumprace", pumpRaceDigest()) }
func pumpRaceBlobPath() string { return "/" + pumpRaceTarget() }

// pumpRaceEnv 快节拍判速升级环境：胜者 100ms/块（320KB/s），判速窗口 700ms → 升级
// 游标确定 ≈ 7 块（224KB）；补充源在升级瞬间并入。
func pumpRaceEnv(t *testing.T, winnerURL, supplementURL string) *testEnv {
	return newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{
			{Template: winnerURL + "/$1"},
			{Template: supplementURL + "/$1"},
		}
		rule.SlowDownloadSpeed = ptrBytes(1 << 20)
		rule.SlowDownloadWindow = ptrDuration(700 * time.Millisecond)
		rule.SlowDownloadMinRemaining = ptrBytes(1024)
		rule.SlowDownloadMaxBehind = ptrBytes(1 << 20)
		rule.BlobConcurrencyInterval = ptrDuration(0)
		cfg.URLRedirectHedgeDelay = ptrDuration(0)
	})
}

// TestMergerPumpRaceStress 多轮升级压力：正确/错位补充源交替 6 轮，逐轮断言客户端
// 完整且内容正确、补充源请求行为确定、无对账冲突（补充源要么被剪要么全对）。
func TestMergerPumpRaceStress(t *testing.T) {
	for round := 0; round < 6; round++ {
		misaligned := round%2 == 1
		t.Run("round_"+itoa(round)+"_"+map[bool]string{true: "misaligned", false: "correct"}[misaligned], func(t *testing.T) {
			// 胜者：32KB/100ms 慢速供数（判速窗口 700ms 必然中途判慢触发升级）
			var winnerHits hitsCounter
			winner := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				winnerHits.Add(1)
				w.Header().Set("Content-Length", itoa(len(pumpRaceData)))
				w.WriteHeader(http.StatusOK)
				for off := 0; off < len(pumpRaceData); off += chunk32K {
					if r.Context().Err() != nil {
						return
					}
					end := off + chunk32K
					if end > len(pumpRaceData) {
						end = len(pumpRaceData)
					}
					if _, err := w.Write(pumpRaceData[off:end]); err != nil {
						return
					}
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
					time.Sleep(100 * time.Millisecond)
				}
			})
			// 补充源：按 Range 起点回 206，载荷按轮次取正确原文 / 错位反码
			payload := pumpRaceData
			if misaligned {
				payload = wrongPattern(pumpRaceData)
			}
			supp := upstreamRangeCapable(t, payload)
			env := pumpRaceEnv(t, winner.URL, supp.srv.URL)

			cw := &countingWriter{ResponseWriter: httpRecorder()}
			env.getWithWriter(pumpRaceBlobPath(), nil, cw)

			if got := cw.Bytes(); got != int64(len(pumpRaceData)) {
				t.Fatalf("第 %d 轮客户端应收到完整 %d 字节，实际 %d", round, len(pumpRaceData), got)
			}
			if !bytesEqual(cw.body(), pumpRaceData) {
				t.Fatalf("第 %d 轮客户端内容与原文不一致（pump 并发时序回归/错位字节入流）", round)
			}
			log := env.logText()
			if n := countLines(log, "对账冲突"); n != 0 {
				t.Errorf("第 %d 轮不应产生对账冲突（胜者全程正确），实际 %d 行", round, n)
			}
			if n := winnerHits.Load(); n != 1 {
				t.Errorf("第 %d 轮胜者应恰 1 次请求，实际 %d", round, n)
			}
			// 错位轮：补充源首块与胜者缓冲重叠 → 必被 cross-check 剪枝（恰好 1 次请求）；
			// 正确轮：补充源全对存活，同样恰好 1 次请求
			if n := supp.hits.Load(); n != 1 {
				t.Errorf("第 %d 轮补充源应恰 1 次请求，实际 %d", round, n)
			}
			pruned := len(logLines(log, "已剪枝"))
			if misaligned && pruned == 0 {
				t.Errorf("错位轮补充源应被对账剪枝（日志缺剪枝行）")
			}
			if !misaligned && pruned != 0 {
				t.Errorf("正确轮不应有剪枝，实际 %d 行: %v", pruned, logLines(log, "已剪枝"))
			}
		})
	}
}
