// flight_wait_limit_test.go —— flightWaitLimit（10s 单飞等待上限）超限语义的黑盒回归
//（缺口#3，session-124a15cf 用户需求起点 / prefetch_hub.go 防重复下载语义核心边界）：
//  1. 超限路径本体：同 key 并发回源，执行方流式下载超过 10s 等待上限时，等待方放弃
//     flight 等待、自行走候选回源（第二路下载）——两路各自完整收敛、缓存原子替换不互
//     污染（cache.Set 临时文件 + rename），这是「无在途流可搭车时」的文档化兜底语义；
//  2. 防重复下载边界：blob 半截续传执行方在途时，并发客户端 GET 命中续传在途条目直接
//     搭车（X-Cache: PREFETCH，磁盘前缀 + 实时增量拼接）——不进 flight 等待、更不会在
//     超限后发起第二路完整下载，上游对该 blob 只有执行方那一路请求。语义上不得两路各
//     下完整一遍。
//
// 确定性设计：执行方上游按块慢速流出（节奏 > flightWaitLimit），等待方在「上游已被
// 执行方请求命中」（= flight 已注册且执行流已开启）后启动；状态轮询同步，无 sleep 赌
// 时序。flightWaitLimit 是包级常量 10s，超限用例整体耗时 ≈ 12s。
package tests

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"
	"time"

	"proxy-cache/config"
)

// flightData flight 测试 blob：512KB。
var flightData = blobPattern(512 * 1024)

// flightDigest / flightTarget / flightBlobPath flight 测试 blob 标识。
func flightDigest() string   { return digestOf(flightData) }
func flightTarget() string   { return blobTarget("app/flight", flightDigest()) }
func flightBlobPath() string { return "/" + flightTarget() }

// flightUpstream 状态分流的假上游：首次请求慢速流出（chunk × delay，保证执行方流式
// 窗口覆盖 flightWaitLimit），后续请求（等待方超限后的第二路）立即吐完整 200——模拟
// 「镜像站第二路很快」的现实形态，也让总耗时可控。
func flightUpstream(t *testing.T, data []byte, chunk int, delay time.Duration) (*httptest.Server, *hitsCounter) {
	t.Helper()
	var hits hitsCounter
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if n > 1 { // 第二路：快速完整响应
			_, _ = w.Write(data)
			return
		}
		for off := 0; off < len(data); off += chunk {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk
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
	return srv, &hits
}

// TestFlightWaitLimitTimeoutWaiterRefetches 缺口#3-a：等待方 10s 超限放弃 → 自行走候选
// 回源（第二路下载）→ 两路各自完整、互不污染、上游恰被请求 2 次（执行方一路 + 等待方
// 一路），完成日志与缓存落盘正确。
func TestFlightWaitLimitTimeoutWaiterRefetches(t *testing.T) {
	srv, hits := flightUpstream(t, flightData, chunk32K, 800*time.Millisecond) // 首路 16×800ms=12.8s > 10s
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
	})

	baseline := runtime.NumGoroutine()
	cw1 := &countingWriter{ResponseWriter: httpRecorder()} // 执行方
	done1 := env.serveWithWriter(http.MethodGet, flightBlobPath(), nil, cw1)
	waitFor(t, 5*time.Second, "执行方请求到达上游", func() bool { return hits.Load() >= 1 })

	cw2 := &countingWriter{ResponseWriter: httpRecorder()} // 等待方（10s 后超限）
	done2 := env.serveWithWriter(http.MethodGet, flightBlobPath(), nil, cw2)
	<-done2
	if got := cw2.Bytes(); got != int64(len(flightData)) {
		t.Fatalf("等待方超限后第二路回源应完整送出 %d 字节，实际 %d", len(flightData), got)
	}
	if !bytesEqual(cw2.body(), flightData) {
		t.Fatalf("等待方第二路内容应与原文逐字节一致")
	}
	if n := countLines(env.logText(), "等待同 key 单飞回源超限，自行走候选回源"); n != 1 {
		t.Errorf("应恰有 1 条等待超限自行回源留痕，实际 %d:\n%v", n, logLines(env.logText(), "单飞回源超限"))
	}
	if got := cw2.Header().Get("X-Cache"); got == "PREFETCH" {
		t.Errorf("无在途流场景不应出现搭车响应头")
	}
	<-done1
	if got := cw1.Bytes(); got != int64(len(flightData)) {
		t.Fatalf("执行方应继续完整送出 %d 字节（不受等待方超限影响），实际 %d", len(flightData), got)
	}
	if !bytesEqual(cw1.body(), flightData) {
		t.Fatalf("执行方内容应与原文逐字节一致")
	}
	if n := hits.Load(); n != 2 {
		t.Errorf("上游应恰被请求 2 次（执行方一路 + 等待方超限后第二路），实际 %d", n)
	}
	assertEntryBody(t, env, flightTarget(), flightData)
	if _, exists := env.partSize(flightTarget()); exists {
		t.Errorf("完整下载不应留下半截文件")
	}
	// slack 放宽到 8：两路客户端 × 上游 keep-alive 空闲连接（transport readLoop/writeLoop
	// 与服务端 conn）会驻留到 IdleConnTimeout，属连接池常态而非代理内部泄漏（看门狗/
	// AfterFunc/pump 泄漏会让计数远超此值）。
	assertGoroutinesSettle(t, baseline, 8, 5*time.Second)
}

// TestFlightWaitLimitRidealongPreventsSecondDownload 缺口#3-b：blob 半截续传在途时并发
// GET 搭车——上游只有执行方那一路 Range 请求（hits==1、带 Range 头），等待方经
// X-Cache: PREFETCH 拼接完整内容，不进 flight 等待、不产生第二路下载（「不得两路各下
// 完整一遍」的正面断言）。
func TestFlightWaitLimitRidealongPreventsSecondDownload(t *testing.T) {
	var lastRange hitsCounter // 收到的非 Range 请求计数（>0 即出现了第二路全量下载）
	srv, hits := upstreamCustom(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			lastRange.Add(1) // 完整 GET = 第二路下载：契约破戒，计数留证
		}
		start, _ := parseTestRange(r.Header.Get("Range"), len(flightData))
		if start < 0 {
			start = 0
		}
		body := flightData[start:]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(flightData)-1, len(flightData)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		for off := 0; off < len(body); off += chunk32K {
			if r.Context().Err() != nil {
				return
			}
			end := off + chunk32K
			if end > len(body) {
				end = len(body)
			}
			if _, err := w.Write(body[off:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(400 * time.Millisecond) // 8 块 ≈3.2s，搭车窗口稳定
		}
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
	})
	env.seedPart(flightTarget(), flightData, 64*1024)

	baseline := runtime.NumGoroutine()
	cw1 := &countingWriter{ResponseWriter: httpRecorder()} // 续传执行方
	done1 := env.serveWithWriter(http.MethodGet, flightBlobPath(), nil, cw1)
	waitFor(t, 5*time.Second, "执行方 Range 请求到达上游", func() bool { return hits.Load() >= 1 })

	cw2 := &countingWriter{ResponseWriter: httpRecorder()} // 并发搭车方
	done2 := env.serveWithWriter(http.MethodGet, flightBlobPath(), nil, cw2)
	<-done2
	if code := cw2.Header().Get("X-Cache"); code != "PREFETCH" {
		t.Fatalf("并发 GET 应搭车在途续传流（X-Cache: PREFETCH），实际 X-Cache=%q", code)
	}
	if got := cw2.Bytes(); got != int64(len(flightData)) {
		t.Fatalf("搭车方应拼接出完整 %d 字节，实际 %d", len(flightData), got)
	}
	if !bytesEqual(cw2.body(), flightData) {
		t.Fatalf("搭车方内容应与原文逐字节一致（磁盘前缀 + 实时增量）")
	}
	if n := countLines(env.logText(), "搭车"); n == 0 {
		t.Errorf("应留搭车日志（拼接磁盘前缀+实时增量），日志:\n%v", logLines(env.logText(), "source=ridealong"))
	}
	if n := countLines(env.logText(), "单飞回源超限"); n != 0 {
		t.Errorf("在途流存在时不应走到 flight 超限，日志出现超限留痕 %d 行", n)
	}
	<-done1
	if got := cw1.Bytes(); got != int64(len(flightData)) {
		t.Fatalf("执行方应完整送出 %d 字节，实际 %d", len(flightData), got)
	}
	if !bytesEqual(cw1.body(), flightData) {
		t.Fatalf("执行方内容应与原文逐字节一致")
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("上游应只收到执行方那 1 路请求（防重复下载），实际 %d", n)
	}
	if n := lastRange.Load(); n != 0 {
		t.Errorf("不得出现第二路完整下载（无 Range 完整 GET 请求 %d 次）", n)
	}
	assertEntryBody(t, env, flightTarget(), flightData)
	if _, exists := env.partSize(flightTarget()); exists {
		t.Errorf("续传完成后半截文件应已删除")
	}
	assertGoroutinesSettle(t, baseline, 3, 5*time.Second)
}
