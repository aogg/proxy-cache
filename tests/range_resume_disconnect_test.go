// range_resume_disconnect_test.go —— 起点>0 的 Range 请求客户端断开 → 半截保留的黑盒
// 回归（缺口#5，session-24db0992 现场「302MB 层已下 30MB 白扔」的根源场景）。现覆盖只有
// 起点 0（proxy 包 TestBlobRangeZeroStartDisconnectKeepsPartial）；本用例补齐：
//
//	docker 断点重试形态 `Range: bytes=N-`（0<N≤半截长度，无缺口）→ 续传执行方接管，
//	对客户端 commit 206（区间 Content-Range），本地前缀从 N 切片回放 + 上游从「半截
//	长度」续传剩余；客户端中途断开（broken pipe）→ 宽限终止（无搭车订阅者）→ 上游
//	读取终止，但已下载增量必须保留在半截文件里（进度不回退），且半截绝不入库。
//
// 断开时序确定性：failAfterWriter 精确限定客户端可收字节数（前缀切片 + 整数个增量块后
// 必然 broken pipe）；宽限窗口（300ms）内上游以 200ms/块慢速续供，半截大小落在一个
// 确定的小区间（断开块已先落盘、宽限期内至多再落 1-2 块），内容按前缀逐字节校验。
package tests

import (
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/config"
)

// rangeDiscData 起点>0 断开测试 blob：512KB。
var rangeDiscData = blobPattern(512 * 1024)

// rangeDiscDigest / rangeDiscTarget / rangeDiscBlobPath 测试 blob 标识。
func rangeDiscDigest() string   { return digestOf(rangeDiscData) }
func rangeDiscTarget() string   { return blobTarget("app/rangedisc", rangeDiscDigest()) }
func rangeDiscBlobPath() string { return "/" + rangeDiscTarget() }

// 断开节拍常量：半截 256KB、客户端起点 128KB、上游 16KB/200ms、宽限 300ms、
// 客户端可收 = 前缀切片 128KB + 2 个增量块 32KB = 160KB（第 3 个增量块必然 broken pipe）。
const (
	rangeDiscPart      = 256 * 1024
	rangeDiscClientSta = 128 * 1024
	rangeDiscChunk     = 16 * 1024
	rangeDiscDelay     = 200 * time.Millisecond
	rangeDiscBudget    = 160 * 1024 // failAfterWriter 额度
)

// TestRangeStartPositiveDisconnectKeepsPartial 缺口#5：起点 128KB（≤半截 256KB）Range
// 请求在增量接收 2 块后客户端断开 → 206 已 commit 的截断响应、上游按半截长度（256KB）
// 而非客户端起点续传、半截保留到「断开块已落盘 + 宽限窗口内收到的块」、内容为原文前缀、
// 绝不入库；goroutine 收敛（宽限计时器不泄漏）。
func TestRangeStartPositiveDisconnectKeepsPartial(t *testing.T) {
	var lastRange atomic.Value // 上游最后一次收到的 Range 头原文
	srv, hits := upstreamCustom(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := parseTestRange(r.Header.Get("Range"), len(rangeDiscData))
		if start < 0 {
			http.Error(w, "want range", http.StatusBadRequest)
			return
		}
		lastRange.Store(r.Header.Get("Range"))
		body := rangeDiscData[start:]
		w.Header().Set("Content-Range", "bytes "+itoa(start)+"-"+itoa(len(rangeDiscData)-1)+"/"+itoa(len(rangeDiscData)))
		w.Header().Set("Content-Length", itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		for off := 0; off < len(body); off += rangeDiscChunk {
			if r.Context().Err() != nil {
				return
			}
			end := off + rangeDiscChunk
			if end > len(body) {
				end = len(body)
			}
			if _, err := w.Write(body[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(rangeDiscDelay)
		}
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
	})
	env.seedPart(rangeDiscTarget(), rangeDiscData, rangeDiscPart)

	baseline := runtime.NumGoroutine()
	ra := httpRecorder()
	cw := &countingWriter{ResponseWriter: ra}
	fw := &failAfterWriter{ResponseWriter: cw, remaining: rangeDiscBudget}
	env.getWithWriter(rangeDiscBlobPath(), map[string][]string{"Range": {"bytes=" + itoa(rangeDiscClientSta) + "-"}}, fw)

	// 已 commit 的 206 截断响应：区间头按完整 blob 语义（起点 = 客户端请求起点）
	if code := ra.code(); code != http.StatusPartialContent {
		t.Fatalf("Code = %d, want %d（续传接管的 206）", code, http.StatusPartialContent)
	}
	if cr := cw.Header().Get("Content-Range"); cr != "bytes "+itoa(rangeDiscClientSta)+"-"+itoa(len(rangeDiscData)-1)+"/"+itoa(len(rangeDiscData)) {
		t.Errorf("Content-Range = %q, want 区间 206 头", cr)
	}
	if got := fw.failed.Load(); !got {
		t.Errorf("断开构造失效：客户端 Writer 未触发 broken pipe")
	}
	// 客户端应收满 160KB 额度：前缀切片 data[128K:256K) + 2 个增量块
	if got := cw.Bytes(); got != int64(rangeDiscBudget) {
		t.Fatalf("断开前客户端应收满 %d 字节（前缀切片+2 增量块），实际 %d", int64(rangeDiscBudget), got)
	}
	if !bytesEqual(cw.body(), rangeDiscData[rangeDiscClientSta:rangeDiscClientSta+rangeDiscBudget]) {
		t.Fatalf("已发字节应为原文 data[%d:%d]（本地前缀切片+上游增量拼接）", rangeDiscClientSta, rangeDiscClientSta+rangeDiscBudget)
	}

	// 上游只按「半截长度」续传（绝不按客户端起点重拉已保留区间）
	if got, _ := lastRange.Load().(string); got != "bytes="+itoa(rangeDiscPart)+"-" {
		t.Errorf("上游应收到的续传 Range = bytes=%d-, 实际 %q（应从半截长度续传，而非客户端起点）", rangeDiscPart, got)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("上游应恰收到 1 次续传请求，实际 %d", n)
	}

	// 半截保留：断开块已先落盘（256K+3×16K=304K），宽限 300ms 内至多再落 1-2 块
	//（200ms/块）→ 确定区间 [304K, 336K]；内容为原文前缀
	deadline := time.Now().Add(3 * time.Second)
	var size int64
	var exists bool
	for time.Now().Before(deadline) {
		size, exists = env.partSize(rangeDiscTarget())
		if exists && size >= int64(rangeDiscPart+3*rangeDiscChunk) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !exists || size < int64(rangeDiscPart+3*rangeDiscChunk) || size > int64(rangeDiscPart+5*rangeDiscChunk) {
		t.Fatalf("半截应保留在 [%d,%d] 区间（进度不回退），实际 exists=%v size=%d",
			rangeDiscPart+3*rangeDiscChunk, rangeDiscPart+5*rangeDiscChunk, exists, size)
	}
	stored, err := os.ReadFile(env.partPath(rangeDiscTarget()))
	if err != nil {
		t.Fatalf("读半截文件失败: %v", err)
	}
	if !bytesEqual(stored, rangeDiscData[:size]) {
		t.Fatalf("半截内容应为原文前缀（got %d 字节）", len(stored))
	}
	// 半截绝不入库
	if _, err := os.Stat(env.entryPath(rangeDiscTarget())); err == nil {
		t.Errorf("中断的下载不应写正式缓存（半截数据绝不入库）")
	}
	log := env.logText()
	if n := countLines(log, "宽限终止"); n == 0 {
		t.Errorf("断开且无搭车订阅者应走宽限终止留痕，日志:\n%v", logLines(log, "宽限"))
	}
	assertGoroutinesSettle(t, baseline, 8, 5*time.Second)
}
