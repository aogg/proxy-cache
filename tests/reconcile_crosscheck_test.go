// reconcile_crosscheck_test.go —— 多源拼接三道内容级对账（commit 8fab96b，线上最严重
// 事故的最终防线）的黑盒回归：经公开 API（blob 下载 + slow-download 判速升级）构造
// 多源合并场景，断言坏字节绝不进客户端流与缓存。四个子主题：
//  1. 错位源（206 起点声明合法、载荷错位）在其首块被对账剪枝（verify-mismatch /
//     cross-check-mismatch），客户端字节与原始数据逐字节一致；
//  2. 正确的非胜者补充源不被误剪（全程无剪枝日志、完成时 alive_sources=2）；
//  3. 胜者重复区间与被污染的已确认输出不符时只 Warn 一次（对账冲突），绝不反向剪掉
//     胜者；污染流被写缓存前的 sha256 强校验拒绝入库；
//  4. selfRef 自引用候选（路径目标 template: $1 展开指向本代理）不发起上游请求、
//     不按 item 重试原地空转。
//
// 确定性设计：胜者 200 + Content-Length，每 32KB 块歇 200ms（读循环保持活跃，判速
// 窗口 1s 到点时窗口速率 160KB/s < 1MB/s 阈值必然触发升级，游标确定在 160KB）；
// 补充源以「客户端已收字节数」为状态信号决定应答时机（轮询计数而非 sleep 猜时序）。
package tests

import (
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// blobData 单层 blob 测试数据：512KB = 16 × 32KB 块（读块大小 = streamChunkSize）。
var blobData = blobPattern(512 * 1024)

// blobDigest 数据对应的合法 blob digest（写缓存 finalize 强校验用）。
func blobDigest() string { return digestOf(blobData) }

// blobKeyTarget blob 路径目标（不含前导 /）。
func blobKeyTarget() string { return blobTarget("app/library", blobDigest()) }

// blobPath blob 请求路径。
func blobPath() string { return "/" + blobKeyTarget() }

// chunk32K 阅读块大小（与 proxy 包 streamChunkSize 同值，便于按块推理节拍）。
const chunk32K = 32 * 1024

// 判速升级节拍常量：胜者每 32KB 歇 200ms；窗口 1.9s 在块 11（2.0s）后的首次判定触发，
// 升级游标确定 = 11 × 32KB = 352KB（见 slowEscalationEnv 注释）。
const (
	winnerChunkDelay = 200 * time.Millisecond
	escalateWindow   = 1900 * time.Millisecond
	escalateCursor   = 11 * chunk32K
)

// slowEscalationEnv 构造判速升级测试共用环境：
//   - type=docker 规则、候选 [胜者, 补充源]（rr 首请求 start=0 → 胜者先探先胜）；
//   - 判速阈值 1MB/s + 窗口 1.9s：胜者 200ms/块（块 k 在 (k-1)×200ms 到手）→ 窗口在
//     块 11（2.0s ≥ 1.9s；块 10 的 1.8s+ε < 1.9s 不提前，容差 ±100ms）后的首次判定
//     必然判慢触发升级，游标确定 = 11 × 32KB = 352KB；
//   - min-remaining 1024B（剩余量门槛必然满足）；blob-concurrency 关闭（0）只走判速通道。
func slowEscalationEnv(t *testing.T, winnerURL, supplementURL string) *testEnv {
	return newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{
			{Template: winnerURL + "/$1"},
			{Template: supplementURL + "/$1"},
		}
		rule.SlowDownloadSpeed = ptrBytes(1 << 20)
		rule.SlowDownloadWindow = ptrDuration(escalateWindow)
		rule.SlowDownloadMinRemaining = ptrBytes(1024)
		rule.SlowDownloadMaxBehind = ptrBytes(1 << 20)
		rule.BlobConcurrencyInterval = ptrDuration(0)
		cfg.URLRedirectHedgeDelay = ptrDuration(0) // 串行探测：候选 0 必然先胜
	})
}

// pacedWinner 启动限速吐完整 200 的假胜者：响应头立即到达（探测停表即停），body 每
// 32KB 块歇 delay 慢速流出（1s 判速窗口到点时仍在传输，读循环保持活跃）。
func pacedWinner(t *testing.T, data []byte, delay time.Duration) (string, *hitsCounter) {
	t.Helper()
	var hits hitsCounter
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		for off := 0; off < len(data); off += chunk32K {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk32K
			if end > len(data) {
				end = len(data)
			}
			if _, err := w.Write(data[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(delay)
		}
	})
	return srv.URL, &hits
}

// supplementServer 构造补充源假上游：按请求 Range 起点回 206（头声明合法），payload
// 由调用方给定（正确原文 / 错位载荷）；waitOn 非 nil 时以客户端已收字节数为状态信号，
// 达到阈值才应答（状态驱动，替代 sleep 猜时序）。
func supplementServer(t *testing.T, data []byte, payload []byte, waitOn func() int64, threshold int64) (string, *hitsCounter) {
	t.Helper()
	var hits hitsCounter
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		start, _ := parseTestRange(r.Header.Get("Range"), len(data))
		if start < 0 {
			start = 0
		}
		if waitOn != nil {
			deadline := time.Now().Add(8 * time.Second)
			for waitOn() < threshold && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
		}
		body := payload[start:]
		w.Header().Set("Content-Range", "bytes "+itoa(start)+"-"+itoa(len(data)-1)+"/"+itoa(len(data)))
		w.Header().Set("Content-Length", itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	})
	return srv.URL, &hits
}

// TestMisalignedSupplementPrunedBeforeFirstByte 缺口#1-a：错位源首块被对账剪枝。
// 补充源等客户端收满 480KB（游标必然越过其 join 时的 Range 起点 320KB）才应答——
// 206 头合法但载荷整体错位。断言：错位载荷 0 字节进客户端流、补充源被对账剪枝
// （alive_sources=1）、胜者供出全部正确字节、缓存写入且内容正确、补充源仅 1 次请求。
func TestMisalignedSupplementPrunedBeforeFirstByte(t *testing.T) {
	winnerURL, winnerHits := pacedWinner(t, blobData, winnerChunkDelay)
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	suppURL, suppHits := supplementServer(t, blobData, wrongPattern(blobData), cw.Bytes, 480*1024)
	env := slowEscalationEnv(t, winnerURL, suppURL)

	env.getWithWriter(blobPath(), nil, cw)

	if got := cw.Bytes(); got != int64(len(blobData)) {
		t.Fatalf("客户端应收到完整 %d 字节，实际 %d", len(blobData), got)
	}
	if !bytesEqual(cw.body(), blobData) {
		t.Fatalf("客户端流混入错位字节：载荷与原始数据不一致（错位源字节进入了流）")
	}

	log := env.logText()
	pruned := false
	for _, ln := range logLines(log, "已剪枝") {
		if strings.Contains(ln, "verify-mismatch") || strings.Contains(ln, "cross-check-mismatch") {
			pruned = true
			if !strings.Contains(ln, suppURL) {
				t.Errorf("对账剪枝应指向补充源 %s，实际行: %s", suppURL, ln)
			}
			if !strings.Contains(ln, "level=WARN") {
				t.Errorf("对账剪枝应 Warn 级留痕，实际行: %s", ln)
			}
		}
	}
	if !pruned {
		t.Errorf("错位补充源应被对账剪枝（日志缺 verify-mismatch/cross-check-mismatch 剪枝行）")
	}
	if n := countLines(log, "对账冲突"); n != 0 {
		t.Errorf("正确胜者不应产生对账冲突 Warn，实际 %d 行", n)
	}
	assertCompletionCounts(t, log, 2, 1)

	if n := suppHits.Load(); n != 1 {
		t.Errorf("补充源应恰好收到 1 次续传请求（剪枝后不重试不重加），实际 %d", n)
	}
	if n := winnerHits.Load(); n != 1 {
		t.Errorf("胜者应恰好收到 1 次请求，实际 %d", n)
	}
	assertBlobCached(t, env, blobData)
}

// TestCorrectSupplementNotPruned 缺口#1-b：正确的非胜者源不被误剪。
// 同场景但补充源载荷与原文一致：重复区间对账全部相符，全程无剪枝，完成时两路源都
// 存活（alive_sources=2），客户端字节正确，缓存写入。
func TestCorrectSupplementNotPruned(t *testing.T) {
	winnerURL, winnerHits := pacedWinner(t, blobData, winnerChunkDelay)
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	suppURL, suppHits := supplementServer(t, blobData, blobData, cw.Bytes, 480*1024)
	env := slowEscalationEnv(t, winnerURL, suppURL)

	env.getWithWriter(blobPath(), nil, cw)

	if got := cw.Bytes(); got != int64(len(blobData)) {
		t.Fatalf("客户端应收到完整 %d 字节，实际 %d", len(blobData), got)
	}
	if !bytesEqual(cw.body(), blobData) {
		t.Fatalf("正确补充源场景客户端流应与原文逐字节一致")
	}
	if pruned := logLines(env.logText(), "已剪枝"); len(pruned) != 0 {
		t.Errorf("正确补充源不应被剪枝，实际剪枝日志: %v", pruned)
	}
	assertCompletionCounts(t, env.logText(), 2, 2)
	assertBlobCached(t, env, blobData)

	if n := suppHits.Load(); n != 1 {
		t.Errorf("补充源应恰好收到 1 次续传请求，实际 %d", n)
	}
	if n := winnerHits.Load(); n != 1 {
		t.Errorf("胜者应恰好收到 1 次请求，实际 %d", n)
	}
}

// TestWinnerConflictWarnsOnceNotReversePruned 缺口#1-c：胜者对账不符只 Warn 不反向剪。
// 用「静默空窗 + 前缀正确毒源」精确构造已确认输出被抢先出数污染的确定性场景
// （合并器约束「非胜者源不得抢在胜者首次出数之前出数」，故纯错位载荷在胜者首块前
// 必然被挡住或按过期区间对账剪枝——能真实触发胜者对账冲突的形态是：毒源与已确认
// 输出一致的前缀过期丢弃、其后首个新鲜块在胜者缓冲空窗携带错位字节进入输出）：
//   - 胜者 200ms/块送满块 11（游标恰为升级窗口触发的 352KB）后进入 channel 门控的
//     静默空窗；补充源（前缀 384KB 与原文一致、其后错位）此刻并入，等待胜者先出块；
//   - 胜者块 12 [352K,384K) 到手出数（胜者首次出数，游标 384KB）后再次静默；
//     补充源的前缀块 [352K,384K) 过期丢弃前对账相符（不冤枉），其首个新鲜块
//     [384K,416K) 携带错位字节、胜者缓冲空窗无对账样本 → 放行进入已确认输出（污染）；
//   - 客户端收满 416KB（污染成立，状态轮询）才放行胜者块 13 [384K,416K)——其重复
//     区间与已确认输出不符 → 对账冲突 Warn 恰好一次，胜者不被剪枝（存活到收尾，
//     alive_sources=2）；补充源后续块以慢节拍续供，保证冲突先于流完成；
//   - 被污染的累积内容被写缓存前的 sha256 强校验拒绝入库（毒内容绝不入库）。
func TestWinnerConflictWarnsOnceNotReversePruned(t *testing.T) {
	gapStarted := make(chan struct{}) // 胜者块 11 写毕：静默空窗开始（等待补充源并入）
	arrived := make(chan struct{})    // 补充源请求已到（胜者可安全送出块 12）
	proceed := make(chan struct{})    // 污染成立（客户端收满 416KB）：放行胜者块 13
	winnerURL, winnerHits := gatedWinner(t, blobData, winnerChunkDelay, gapStarted, arrived, proceed)
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	poison := make([]byte, 0, len(blobData))
	poison = append(poison, blobData[:12*chunk32K]...)               // 前缀 [0,384K) 与原文一致
	poison = append(poison, wrongPattern(blobData[12*chunk32K:])...) // [384K,末尾) 错位
	suppURL, suppHits := gatedPoisonSupplement(t, blobData, poison, cw.Bytes, gapStarted, arrived, proceed)
	env := slowEscalationEnv(t, winnerURL, suppURL)

	env.getWithWriter(blobPath(), nil, cw)

	if got := cw.Bytes(); got != int64(len(blobData)) {
		t.Fatalf("客户端应收到完整流（长度 %d, want %d）", got, len(blobData))
	}
	body := cw.body()
	// 游标 384KB 前全部正确（块 1..12 由胜者供出；补充源前缀段过期丢弃不写客户端）
	if !bytesEqual(body[:12*chunk32K], blobData[:12*chunk32K]) {
		t.Fatalf("游标 384KB 前应全部正确（胜者供数 + 补充源前缀过期丢弃）")
	}
	// 设计上补充源首个错位块在胜者缓冲空窗抢先出数，其后字节已被污染——若仍与原文
	// 一致说明测试构造失效（污染没有发生，断言前提不成立）
	if bytesEqual(body[12*chunk32K:], blobData[12*chunk32K:]) {
		t.Fatalf("该场景补充源应在胜者缓冲空窗污染已确认输出（测试构造失效）")
	}

	log := env.logText()
	if n := countLines(log, "对账冲突"); n != 1 {
		t.Errorf("胜者对账冲突 Warn 应恰好输出一次（防逐块刷屏），实际 %d 行:\n%v", n, logLines(log, "对账冲突"))
	}
	for _, ln := range logLines(log, "已剪枝") {
		if strings.Contains(ln, winnerURL) {
			t.Errorf("胜者绝不因对账冲突被反向剪枝，实际剪枝行: %s", ln)
		}
	}
	assertCompletionCounts(t, log, 2, 2)

	// 毒流被写缓存闸口拒绝：无正式缓存条目、无半截文件（client 来源不保留半截）
	if _, err := os.Stat(env.entryPath(blobKeyTarget())); err == nil {
		t.Errorf("被污染的流必须被 sha256 强校验拒绝入库，但缓存文件已落盘")
	}
	if _, err := os.Stat(env.partPath(blobKeyTarget())); err == nil {
		t.Errorf("client 来源不应留下半截文件")
	}
	if n := countLines(log, "拒绝写缓存"); n < 1 {
		t.Errorf("应留痕「blob 回源内容摘要不符，拒绝写缓存」，日志无该行")
	}
	if n := suppHits.Load(); n != 1 {
		t.Errorf("补充源应恰好收到 1 次请求，实际 %d", n)
	}
	if n := winnerHits.Load(); n != 1 {
		t.Errorf("胜者应恰好收到 1 次请求，实际 %d", n)
	}
}

// gatedWinner 带两道确定性门控的假胜者（缺口#1-c 专用）：块 1..11 按 delay 节拍供数；
// 写完块 11（覆盖到偏移 = 升级游标 352KB）后关闭 gapStarted（通知外界静默空窗开始，
// 升级此刻触发、补充源并入）并阻塞等 arrived（补充源请求已到）；随后送出块 12
// [352K,384K)（胜者首次出数，游标推进到 384KB，补充源前缀块由此变为过期区间）
// 并再次阻塞等 proceed（污染成立）；proceed 关闭后才按 delay 节拍续供块 13..。
// 全部同步点由 channel 与状态轮询构造，不靠 sleep 赌时序。
func gatedWinner(t *testing.T, data []byte, delay time.Duration, gapStarted, arrived, proceed chan struct{}) (string, *hitsCounter) {
	t.Helper()
	var hits hitsCounter
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		for off := 0; off < len(data); off += chunk32K {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk32K
			if end > len(data) {
				end = len(data)
			}
			if _, err := w.Write(data[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			switch int64(end) {
			case escalateCursor: // 块 11 写毕：静默空窗，等补充源并入
				close(gapStarted)
				select {
				case <-arrived:
				case <-r.Context().Done():
					return
				}
			case 12 * chunk32K: // 块 12 写毕：胜者已首次出数，等污染成立
				select {
				case <-proceed:
				case <-r.Context().Done():
					return
				}
			}
			time.Sleep(delay)
		}
	})
	return srv.URL, &hits
}

// gatedPoisonSupplement 缺口#1-c 配套的「前缀正确毒源」假上游：等 gapStarted（静默
// 空窗开始）后按 Range 起点（升级游标 352KB）回 206；载荷前 32KB 与原文一致（过期
// 丢弃前对账相符、不冤枉），等胜者块 12 出数（客户端收满 384KB，状态轮询）后送出
// 首个错位块 32KB（胜者缓冲空窗 → 无对账样本 → 放行污染）；客户端收满 416KB（污染
// 成立）才关闭 proceed 放行胜者块 13；其后载荷按 8KB/300ms 慢速续供，保证对账冲突
// 先于流完成被触发。全部同步点状态驱动。
func gatedPoisonSupplement(t *testing.T, data, payload []byte, clientBytes func() int64, gapStarted, arrived, proceed chan struct{}) (string, *hitsCounter) {
	t.Helper()
	var hits hitsCounter
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		close(arrived) // 通知胜者：补充源请求已到（可送出块 12）
		select {
		case <-gapStarted:
		case <-r.Context().Done():
			return
		}
		start, _ := parseTestRange(r.Header.Get("Range"), len(data))
		if start < 0 {
			start = 0
		}
		body := payload[start:]
		w.Header().Set("Content-Range", "bytes "+itoa(start)+"-"+itoa(len(data)-1)+"/"+itoa(len(data)))
		w.Header().Set("Content-Length", itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		// 阶段 1：前缀块（与原文一致，胜者块 12 出数后过期丢弃）；等客户端收满 384KB
		//（胜者块 12 已出数）再送首个错位块——保证它落在胜者缓冲空窗
		prefix := int64(13*chunk32K - start) // [start, 384K) 与原文一致段
		if prefix > int64(len(body)) {
			prefix = int64(len(body))
		}
		if prefix > 0 {
			if _, err := w.Write(body[:prefix]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		deadline := time.Now().Add(8 * time.Second)
		for clientBytes() < int64(12*chunk32K) && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		// 阶段 2：首个错位块 [384K,416K) 一次送出；客户端收满 416KB（污染成立）后放行胜者
		if prefix < int64(len(body)) {
			mis := prefix + chunk32K
			if mis > int64(len(body)) {
				mis = int64(len(body))
			}
			if _, err := w.Write(body[prefix:mis]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			deadline = time.Now().Add(8 * time.Second)
			for clientBytes() < int64(13*chunk32K) && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
		}
		close(proceed)
		// 阶段 3：其余载荷 8KB/300ms 慢速续供，保证胜者块 13 的对账冲突先于流完成
		//（offset 以 body 切片为基准：body = payload[start:]，起点即 Range 起点）
		for off := int(prefix) + chunk32K; off < len(body); off += 8 * 1024 {
			end := off + 8*1024
			if end > len(body) {
				end = len(body)
			}
			if _, err := w.Write(body[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(300 * time.Millisecond)
		}
	})
	return srv.URL, &hits
}

// TestSelfRefCandidateSkippedNoUpstreamRequest 缺口#1-d：selfRef 自引用候选拒发不重试。
// 候选模板 template: $1 在路径目标下展开为「http://<入站Host>/<路径目标>」= 本代理自身
// （入站 Host 指向 canary 假上游监听，若误发请求必被计数）。断言：
//   - 仅 selfRef 候选：502、canary 0 命中、跳过留痕恰好 1 次（不按 item 重试空转）；
//   - selfRef + 真实候选混合：真实候选照常服务，canary 仍 0 命中。
func TestSelfRefCandidateSkippedNoUpstreamRequest(t *testing.T) {
	data := blobPattern(64 * 1024)
	digest := digestOf(data)
	path := "/" + blobTarget("app/selfref", digest)

	// canary 假上游同时充当「入站 Host 解析目标」：selfRef 误发请求会立即被计数发现
	var canaryHits hitsCounter
	canary := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		canaryHits.Add(1)
		w.Header().Set("Content-Length", itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	hostPort := strings.TrimPrefix(canary.URL, "http://")
	matchRe := `http://` + regexp.QuoteMeta(hostPort) + `/`

	t.Run("仅自引用候选立即失败且不发上游", func(t *testing.T) {
		env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
			rule.Match = matchRe
			rule.URLRedirect = &[]config.URLRedirectItem{
				{Template: "$1"}, // 展开即本代理自身 → selfRef
			}
			cfg.URLRedirectItemErrorRetry = ptrInt(3) // 若误按 item 重试会留下 4 次跳过痕迹（断言恰好 1 次）
		})
		rec := runServe(env.srv, requestTo("http://"+hostPort+path))
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("Code = %d, want 502（全部候选为自引用时失败收场）, body: %s", rec.Code, rec.Body.String())
		}
		if n := canaryHits.Load(); n != 0 {
			t.Errorf("自引用候选绝不能发起上游请求（否则递归成环），canary 命中 %d 次", n)
		}
		if n := countLines(env.logText(), "自引用候选跳过"); n != 1 {
			t.Errorf("自引用跳过应恰好留痕 1 次（不按 url-redirect-item-error-retry=3 原地重试空转），实际 %d 行:\n%v", n, logLines(env.logText(), "自引用候选跳过"))
		}
	})

	t.Run("自引用候选跳过后真实候选照常服务", func(t *testing.T) {
		var realHits hitsCounter
		real := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			realHits.Add(1)
			w.Header().Set("Content-Length", itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		})
		env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
			rule.Match = matchRe
			rule.URLRedirect = &[]config.URLRedirectItem{
				{Template: "$1"},
				{Template: real.URL + "/$http.server.header.full_url_no_server"},
			}
		})
		rec := runServe(env.srv, requestTo("http://"+hostPort+path))
		if rec.Code != http.StatusOK {
			t.Fatalf("Code = %d, want 200（真实候选应服务）, body: %s", rec.Code, rec.Body.String())
		}
		if len(rec.Body.String()) != len(data) {
			t.Fatalf("body 长度 = %d, want %d", len(rec.Body.String()), len(data))
		}
		if n := canaryHits.Load(); n != 0 {
			t.Errorf("自引用候选不应发出请求，canary 命中 %d 次", n)
		}
		if n := realHits.Load(); n != 1 {
			t.Errorf("真实候选应恰好命中 1 次，实际 %d", n)
		}
	})
}

// //////////////////  小工具  //////////////////////////////////////////////////////////////////////////////////

// assertCompletionCounts 断言「成功返回内容」完成日志的 sources / alive_sources 字段。
func assertCompletionCounts(t *testing.T, log string, wantSources, wantAlive int) {
	t.Helper()
	for _, ln := range logLines(log, "成功返回内容") {
		if !strings.Contains(ln, "sources="+itoa(wantSources)) {
			t.Errorf("完成日志 sources 应为 %d，实际行: %s", wantSources, ln)
		}
		if !strings.Contains(ln, "alive_sources="+itoa(wantAlive)) {
			t.Errorf("完成日志 alive_sources 应为 %d，实际行: %s", wantAlive, ln)
		}
		return
	}
	t.Errorf("日志缺「成功返回内容」完成行")
}

// assertBlobCached 断言 blob 正式缓存条目已落盘且内容与 data 逐字节一致。
func assertBlobCached(t *testing.T, env *testEnv, data []byte) {
	t.Helper()
	raw, err := os.ReadFile(env.entryPath(blobKeyTarget()))
	if err != nil {
		t.Fatalf("缓存条目应已落盘: %v", err)
	}
	// 缓存文件 = 8B 头 + JSON 元信息 + body 原文；body 为文件尾段，直接比对后缀
	if len(raw) < len(data) || !bytesEqual(raw[len(raw)-len(data):], data) {
		t.Fatalf("缓存条目 body 与原文不一致（长度 raw=%d want=%d）", len(raw), len(data))
	}
}

// bytesEqual 逐字节比较。
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
