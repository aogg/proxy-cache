// resume_scan_test.go —— 续传候选扫描「取消权移交」契约（commit e84d860 AfterFunc 门控）
// 与 416 判完整矩阵的黑盒回归。四个子主题：
//  1. 扫描命中 206 后的活体续传流不受扫描预算 cancel 约束：历史 bug 是把预算 deadline
//     直接挂进 ctx（defer cancel / WithTimeout），命中候选的 body 一读即 context
//     canceled，现场「206 命中后正常流式约 2 分钟被预算杀掉、客户端响应截断」——现实现
//     把 cancel 挂 upRes.onClose 随流移交。本用例以慢速分块上游构造「扫描毫秒级返回、
//     活体流再跑 5 秒以上」的窗口：若回归为返回即取消，命中后的第一次 body 读必然失败、
//     客户端流截断在半截前缀处；完成全量 + 无 canceled 痕迹即锁死该契约。
//  2. 扫描全败出口：网络级失败（上游连接秒断）保留半截 + 安全中断已 commit 响应，
//     goroutine 收敛（cancel 不泄漏）；
//  3. 416 判完整正向：Content-Range 总长 == 半截长度才证实「半截已完整」→ 整体回放写
//     缓存、半截删除、客户端拿全量；
//  4. 416 判完整负向矩阵：总长多/少一字节、缺 Content-Range 一律不可信（防
//     dockerpull.cn 对任意 Range 一律回 416 的毒缓存事故）——丢弃半截 + 安全中断，
//     绝不据 416 把半截固化为正式缓存。
//
// 确定性设计：全部状态驱动（命中计数 / 轮询），无 sleep 赌时序；分块节奏只用于把
// 「活体流窗口」拉长到可观察量级。
package tests

import (
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// resumeData 续传测试 blob：512KB。
var resumeData = blobPattern(512 * 1024)

// resumeDigest / resumeTarget / resumePath 续传测试 blob 的 digest、路径目标与请求路径。
func resumeDigest() string   { return digestOf(resumeData) }
func resumeTarget() string   { return blobTarget("app/resume", resumeDigest()) }
func resumeBlobPath() string { return "/" + resumeTarget() }

// TestResumeScanBudgetDoesNotKillLiveStream 缺口#2-a：命中 206 后活体流不被扫描预算杀。
// 半截 64KB；上游对 Range bytes=65536- 回 206 并按 32KB/400ms 慢速流出剩余 448KB
//（≈5.6s 活体窗口，远超扫描函数毫秒级返回）。断言：客户端收到全量且逐字节正确、
// 日志有「候选命中」且全程无 context canceled、缓存写入 + 半截删除、goroutine 收敛。
func TestResumeScanBudgetDoesNotKillLiveStream(t *testing.T) {
	up := upstreamPacedRange(t, resumeData, chunk32K, 400*time.Millisecond)
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: up.srv.URL + "/$1"}}
	})
	env.seedPart(resumeTarget(), resumeData, 64*1024)

	baseline := runtime.NumGoroutine()
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	env.getWithWriter(resumeBlobPath(), nil, cw)

	if got := cw.Bytes(); got != int64(len(resumeData)) {
		t.Fatalf("活体续传流应完整送出 %d 字节（若被扫描预算 cancel 杀会截断在 64KB 前缀附近），实际 %d", len(resumeData), got)
	}
	if !bytesEqual(cw.body(), resumeData) {
		t.Fatalf("续传流内容应与原文逐字节一致（半截前缀 + 上游剩余拼接）")
	}
	log := env.logText()
	if n := countLines(log, "blob 半截续传候选命中"); n != 1 {
		t.Errorf("应恰有 1 条续传候选命中留痕，实际 %d:\n%v", n, logLines(log, "blob 半截续传候选命中"))
	}
	if n := countLines(log, "blob 半截续传立即响应"); n != 1 {
		t.Errorf("应恰有 1 条立即响应留痕（响应头先于候选扫描 commit），实际 %d", n)
	}
	for _, bad := range []string{"context canceled", "context deadline exceeded"} {
		if n := countLines(log, bad); n != 0 {
			t.Errorf("命中后的活体流不受扫描预算约束，日志不应出现 %q，实际 %d 行:\n%v", bad, n, logLines(log, bad))
		}
	}
	if n := up.rangeHits(); n != 1 {
		t.Errorf("上游应恰收到 1 次 Range 续传请求，实际 %d", n)
	}
	// 半截已随写缓存删除、正式条目内容正确
	if _, exists := env.partSize(resumeTarget()); exists {
		t.Errorf("续传完成后半截文件应已删除")
	}
	assertEntryBody(t, env, resumeTarget(), resumeData)
	assertGoroutinesSettle(t, baseline, 3, 5*time.Second)
}

// TestResumeScanAllFailSafeAbortKeepsPart 缺口#2-b：扫描全败（网络级失败）出口——
// 上游连接秒断（hijack 后立即关闭，网络/超时类失败而非 4xx 明确拒绝）→ 半截保留、
// 已 commit 的响应按「安全中断」收场（绝不降级全量回源造成错位拼接）、goroutine 收敛。
func TestResumeScanAllFailSafeAbortKeepsPart(t *testing.T) {
	srv, hits := upstreamCustom(t, func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, _ := hj.Hijack()
		_ = conn.Close() // 连接秒断：代理侧网络级失败
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
	})
	env.seedPart(resumeTarget(), resumeData, 64*1024)

	baseline := runtime.NumGoroutine()
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	env.getWithWriter(resumeBlobPath(), nil, cw)

	if n := hits.Load(); n != 1 {
		t.Errorf("上游应恰被尝试 1 次（单候选），实际 %d", n)
	}
	// 已 commit 的立即响应：客户端收到回放的半截前缀后流被安全中断（无降级全量）
	if got := cw.Bytes(); got != int64(64*1024) {
		t.Fatalf("安全中断后客户端应只收到半截前缀 %d 字节，实际 %d", int64(64*1024), got)
	}
	if !bytesEqual(cw.body(), resumeData[:64*1024]) {
		t.Fatalf("已发出的前缀字节应是合法内容前缀")
	}
	log := env.logText()
	if n := countLines(log, "候选扫描失败"); n == 0 {
		t.Errorf("应留痕「blob 半截续传候选扫描失败」，日志无该行:\n%v", logLines(log, "候选扫描失败"))
	}
	if !strings.Contains(log, "安全中断") {
		t.Errorf("已 commit 的响应应按安全中断收场，日志缺「安全中断」")
	}
	if size, exists := env.partSize(resumeTarget()); !exists || size != int64(64*1024) {
		t.Errorf("网络级扫描失败应保留半截 64KB（进度不回退），实际 exists=%v size=%d", exists, size)
	}
	if _, err := os.Stat(env.entryPath(resumeTarget())); err == nil {
		t.Errorf("未完成的续传不应写缓存")
	}
	assertGoroutinesSettle(t, baseline, 3, 5*time.Second)
}

// TestResumeScan416CompleteWritesCache 缺口#3-正向：上游 416 且 Content-Range 总长与
// 半截长度一致 → 证实半截即完整 blob：整体回放写缓存、半截删除、客户端拿全量 200。
func TestResumeScan416CompleteWritesCache(t *testing.T) {
	srv, hits := upstreamCustom(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes */"+itoa(len(resumeData)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
	})
	env.seedPart(resumeTarget(), resumeData, len(resumeData)) // 半截 = 完整内容

	baseline := runtime.NumGoroutine()
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	env.getWithWriter(resumeBlobPath(), nil, cw)

	if got := cw.Bytes(); got != int64(len(resumeData)) {
		t.Fatalf("客户端应收到完整 blob %d 字节，实际 %d", len(resumeData), got)
	}
	if !bytesEqual(cw.body(), resumeData) {
		t.Fatalf("416 证实完整后客户端内容应与原文一致")
	}
	log := env.logText()
	if n := countLines(log, "发现半截已完整"); n != 1 {
		t.Errorf("应恰有 1 条「发现半截已完整」留痕，实际 %d:\n%v", n, logLines(log, "发现半截已完整"))
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("上游应恰被请求 1 次（416 收场不再读上游），实际 %d", n)
	}
	if _, exists := env.partSize(resumeTarget()); exists {
		t.Errorf("半截已整体写缓存，半截文件应删除")
	}
	assertEntryBody(t, env, resumeTarget(), resumeData)
	assertGoroutinesSettle(t, baseline, 3, 5*time.Second)
}

// TestResumeScan416UntrustedTotalMatrix 缺口#3-负向矩阵：416 的 Content-Range 总长
// 与半截长度不一致（±1 字节）或缺失时不可信——绝不能把半截固化为正式缓存（现场毒缓存
// 事故），按「明确拒绝按断点续传」处理：丢弃半截 + 已 commit 响应安全中断，无缓存写入。
func TestResumeScan416UntrustedTotalMatrix(t *testing.T) {
	part := resumeData[:64*1024]
	cases := []struct {
		name          string
		contentRange  string // 空串 = 不带 Content-Range
		wantTotalLine string // 期望日志中的不可信总长值（缺失时记 -1）
	}{
		{"总长多一字节不可信", "bytes */" + itoa(64*1024+1), itoa(64*1024+1)},
		{"总长少一字节不可信", "bytes */" + itoa(64*1024-1), itoa(64*1024-1)},
		{"缺Content-Range不可信", "", "-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, hits := upstreamCustom(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			})
			env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
				rule.URLRedirect = &[]config.URLRedirectItem{{Template: srv.URL + "/$1"}}
			})
			env.seedPart(resumeTarget(), resumeData, len(part))

			baseline := runtime.NumGoroutine()
			cw := &countingWriter{ResponseWriter: httpRecorder()}
			env.getWithWriter(resumeBlobPath(), nil, cw)

			if n := hits.Load(); n != 1 {
				t.Errorf("上游应恰被请求 1 次，实际 %d", n)
			}
			// 已 commit 的立即响应安全中断在半截前缀处
			if got := cw.Bytes(); got != int64(len(part)) {
				t.Fatalf("客户端应只收到半截前缀 %d 字节（安全中断），实际 %d", len(part), got)
			}
			if !bytesEqual(cw.body(), part) {
				t.Fatalf("已发出的前缀字节应是合法内容前缀")
			}
			log := env.logText()
			if n := countLines(log, "发现半截已完整"); n != 0 {
				t.Errorf("不可信 416 绝不能证实半截完整，日志出现「发现半截已完整」%d 行", n)
			}
			if !strings.Contains(log, "轮询下一候选") || !strings.Contains(log, "不可证实半截完整") {
				t.Errorf("应留痕「416 缺合规 Content-Range，不可证实半截完整」，日志:\n%v", logLines(log, "416"))
			}
			if n := countLines(log, "缺失或 ≠ 半截长度"); n < 1 {
				t.Errorf("不可信 reason 应留痕「Content-Range 总长 %s 缺失或 ≠ 半截长度」，日志:\n%v", tc.wantTotalLine, logLines(log, "416"))
			}
			if _, exists := env.partSize(resumeTarget()); exists {
				t.Errorf("不可信 416 应丢弃半截（对续传已无价值，重试走干净全量回源）")
			}
			if _, err := os.Stat(env.entryPath(resumeTarget())); err == nil {
				t.Errorf("毒缓存防线：不可信 416 绝不能触发写缓存")
			}
			assertGoroutinesSettle(t, baseline, 3, 5*time.Second)
		})
	}
}

// assertEntryBody 断言指定路径目标的正式缓存条目已落盘且 body 与 data 逐字节一致
//（缓存文件 = 8B 头 + JSON 元信息 + body 尾段）。
func assertEntryBody(t *testing.T, env *testEnv, target string, data []byte) {
	t.Helper()
	raw, err := os.ReadFile(env.entryPath(target))
	if err != nil {
		t.Fatalf("正式缓存条目应已落盘: %v", err)
	}
	if len(raw) < len(data) || !bytesEqual(raw[len(raw)-len(data):], data) {
		t.Fatalf("缓存条目 body 与原文不一致（长度 raw=%d want=%d）", len(raw), len(data))
	}
}
