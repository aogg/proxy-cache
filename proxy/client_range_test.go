package proxy

// client_range_test.go —— 客户端 Range 的「起点 0 = 完整 GET；任意起点 = 本地切片 /
// 半截对齐」（client_range.go）语义单测：全部使用 httptest 假 registry 上游（不启动本
// 服务、不发真实外网请求），缓存目录一律 t.TempDir()，测试只写临时目录，不含任何 e2e。
// 复用 stream_disconnect_test.go 的 newDisconnectFixture（带日志缓冲区，便于断言 Warn /
// BYPASS 决策日志）与 partial_resume_test.go 的 savePartialPart / partBytes / rangesEqual。
//
// 现场故障背景（容器里的实现注释已详述）：docker/containerd 的 httpReadSeeker 无论偏移
// 是否为 0 都带 Range——首次 `bytes=0-`，断点续传重试 `bytes=<已有偏移>-`。旧版把所有
// Range 一律判 BYPASS（既不读缓存也不保留半截），302MB 单层每轮重试都从 0 重来，最终 502。
//
// 覆盖点：
//   1. 起点 0（首次请求的真实形态）：
//      - 缓存 HIT → 206 + 完整 Content-Range/Content-Length，body = 缓存原文，上游零请求；
//      - 带区间终点 bytes=0-99 且缓存 MISS → 客户端只收区间内 100 字节（206），内部仍按
//        完整 GET 消费上游并写完整缓存，Range 绝不透传给上游；
//   2. 起点 >0 + 缓存 HIT → 本地切片回 206：后缀、终点越界按总长收窄、起点越界 416
//      + `Content-Range: bytes */<total>`，全部不发上游；
//   3. 起点 >0 + 无正式缓存但有半截 → 半截对齐续传回 206：半截长度 == 起点（直接续）、
//      半截长度 > 起点（从半截里回放后缀）；半截长度 < 起点（缺口）→ 维持旧版 206 透传
//      + 缺口 Warn（gap_bytes），半截不被消费；
//   4. 起点 0 + 下载中途上游断连 → 入口剥离 Range 后照常注入半截 sink，已收前缀落盘，
//      下次重试可无感续传（不再从 0 开始）。

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"proxy-cache/cache"
)

// seedBlobCache 预写一份完整 blob 的正式缓存条目（模拟「上次已完整落盘」）；
// 内容与上游原文逐字节一致，HIT 切片用例据此断言 body。
func seedBlobCache(t *testing.T, disk *cache.Cache, key string, content []byte) {
	t.Helper()
	if err := disk.Set(key, &cache.Entry{
		Status:   http.StatusOK,
		Header:   http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:     content,
		StoredAt: time.Now(),
	}); err != nil {
		t.Fatalf("预写缓存失败: %v", err)
	}
}

// //////////////////  1：起点 0（等价完整 GET）  ////////////////////////////////////////////////

// TestBlobRangeZeroStartCacheHit 覆盖点 1 的 HIT 分支：docker blob + `Range: bytes=0-`
// 等价完整 GET——缓存 HIT 时对客户端回 206 + 完整 Content-Range/Content-Length，body 就是
// 缓存条目原文，且**上游零请求**（旧版把 Range 一律 BYPASS，这条路径根本走不到缓存）。
func TestBlobRangeZeroStartCacheHit(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xc1, 4096)
	key := prefetchBlobKey(name, digest)
	seedBlobCache(t, fx.disk, key, content)

	r := prefetchBlobTestRequest(name, digest)
	r.Header.Set("Range", "bytes=0-")
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)

	if w.Code != http.StatusPartialContent {
		t.Fatalf("Code = %d, want %d（起点 0 内部按完整 GET，对客户端仍是 206）, body: %s",
			w.Code, http.StatusPartialContent, w.Body.String())
	}
	wantCR := fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content))
	if got := w.Header().Get("Content-Range"); got != wantCR {
		t.Errorf("Content-Range = %q, want %q", got, wantCR)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Errorf("Content-Length = %q, want %q", got, strconv.Itoa(len(content)))
	}
	if got := w.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q, want bytes", got)
	}
	if got := w.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("X-Cache = %q, want HIT（起点 0 必须能命中缓存，不再 BYPASS）", got)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("body 与缓存原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}
	// 上游零请求：命中层直接用本地完整内容切片
	if n := fx.reg.rec.calls(digest); n != 0 {
		t.Errorf("缓存 HIT 的上游回源 %d 次, want 0（本地切片不回源）", n)
	}
	// 不输出 BYPASS 决策日志（起点 0 已按完整 GET 接管）；应输出「按完整 GET 处理」日志
	if lines := logLinesOf(fx.logBuf.String(), "GET 请求绕过缓存（BYPASS）"); len(lines) != 0 {
		t.Errorf("起点 0 不应再走 BYPASS，实际日志: %s", lines[0])
	}
	if lines := logLinesOf(fx.logBuf.String(), "按完整 GET 处理"); len(lines) == 0 {
		t.Fatalf("应输出「起点 0 按完整 GET 处理」日志，实际:\n%s", fx.logBuf.String())
	}
}

// TestBlobRangeZeroStartBoundedCacheMiss 覆盖点 1 的「起点 0 + 带区间终点」分支（缓存 MISS）：
// `bytes=0-99` 只要求前 100 字节——客户端收 206 + 收窄后的 Content-Range/Content-Length 与
// 前 100 字节；内部仍按完整 blob 消费上游并**照常写完整缓存**（缓存条目必须完整，后续任意
// 起点才能本地切片秒回）；Range 绝不透传给上游（上游必须收到无 Range 的完整 GET）。
func TestBlobRangeZeroStartBoundedCacheMiss(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xc2, 4096)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(4096, 0)

	r := prefetchBlobTestRequest(name, digest)
	r.Header.Set("Range", "bytes=0-99")
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)

	if w.Code != http.StatusPartialContent {
		t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
	}
	if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 0-99/%d", len(content)) {
		t.Errorf("Content-Range = %q, want bytes 0-99/%d（终点按客户端区间收窄、总长仍是完整 blob）",
			got, len(content))
	}
	if got := w.Header().Get("Content-Length"); got != "100" {
		t.Errorf("Content-Length = %q, want 100（只发区间内字节）", got)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content[:100]) {
		t.Errorf("body 应为 content[:100]（got %d 字节）", len(got))
	}
	// 上游收到的是无 Range 的完整 GET（Range 已在入口剥离，绝不能透传——否则上游回 206，
	// 成功判据 / 写缓存 / 半截续传会全部失配）
	if got := fx.reg.rangesOf(digest); !rangesEqual(got, []string{""}) {
		t.Errorf("上游收到的 Range 头 = %v, want [\"\"]（起点 0 绝不透传 Range）", got)
	}
	// 内部按完整 blob 写缓存（客户端只收 100 字节，缓存必须完整）
	e, ok := fx.disk.Get(prefetchBlobKey(name, digest))
	if !ok {
		t.Fatal("完整 GET 收尾后应写缓存")
	}
	if !bytes.Equal(e.Body, content) {
		t.Errorf("缓存 body 应为完整 blob（got %d 字节, want %d 字节）", len(e.Body), len(content))
	}
}

// //////////////////  2：起点 >0 + 缓存 HIT（本地切片）  ////////////////////////////////////////

// TestBlobRangePositiveStartCacheHit 覆盖点 2：起点 >0 且正式缓存 HIT——直接从缓存条目
// body 本地切片回 206（「一次落盘、后续任意起点秒回」），**不发上游**：
//   - 开区间 bytes=100-：后缀到结尾，Content-Range 终点 = total-1；
//   - 终点越界 bytes=100-999999：按 RFC 收窄到 total-1；
//   - 起点越界 bytes=4096-（= 总长）：416 + `Content-Range: bytes */total`。
func TestBlobRangePositiveStartCacheHit(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xc3, 4096)
	key := prefetchBlobKey(name, digest)
	seedBlobCache(t, fx.disk, key, content)

	tests := []struct {
		name       string
		rangeHdr   string
		wantStatus int
		wantCR     string
		wantCL     string
		wantBody   []byte
	}{
		{
			name:       "起点100开区间回后缀",
			rangeHdr:   "bytes=100-",
			wantStatus: http.StatusPartialContent,
			wantCR:     fmt.Sprintf("bytes 100-%d/%d", len(content)-1, len(content)),
			wantCL:     strconv.Itoa(len(content) - 100),
			wantBody:   content[100:],
		},
		{
			name:       "终点越界按总长收窄",
			rangeHdr:   "bytes=100-999999",
			wantStatus: http.StatusPartialContent,
			wantCR:     fmt.Sprintf("bytes 100-%d/%d", len(content)-1, len(content)),
			wantCL:     strconv.Itoa(len(content) - 100),
			wantBody:   content[100:],
		},
		{
			name:       "起点等于总长回416",
			rangeHdr:   fmt.Sprintf("bytes=%d-", len(content)),
			wantStatus: http.StatusRequestedRangeNotSatisfiable,
			wantCR:     fmt.Sprintf("bytes */%d", len(content)),
			wantCL:     "",
			wantBody:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := prefetchBlobTestRequest(name, digest)
			r.Header.Set("Range", tt.rangeHdr)
			w := httptest.NewRecorder()
			fx.s.ServeHTTP(w, r)

			if w.Code != tt.wantStatus {
				t.Fatalf("Code = %d, want %d（%s）, body: %s", w.Code, tt.wantStatus, tt.rangeHdr, w.Body.String())
			}
			if got := w.Header().Get("Content-Range"); got != tt.wantCR {
				t.Errorf("Content-Range = %q, want %q", got, tt.wantCR)
			}
			if got := w.Header().Get("Content-Length"); got != tt.wantCL {
				t.Errorf("Content-Length = %q, want %q", got, tt.wantCL)
			}
			if got := w.Header().Get("X-Cache"); got != "HIT" {
				t.Errorf("X-Cache = %q, want HIT", got)
			}
			if tt.wantBody == nil {
				if w.Body.Len() != 0 {
					t.Errorf("416 不应有 body, got %d 字节", w.Body.Len())
				}
			} else if got := w.Body.Bytes(); !bytes.Equal(got, tt.wantBody) {
				t.Errorf("body 应为缓存原文的对应后缀（got %d 字节, want %d 字节）", len(got), len(tt.wantBody))
			}
		})
	}
	// 三个用例全部走本地切片：上游零请求
	if n := fx.reg.rec.calls(digest); n != 0 {
		t.Errorf("缓存 HIT 的区间请求上游回源 %d 次, want 0（任意起点都本地切片）", n)
	}
}

// //////////////////  3：起点 >0 + 半截对齐（续传 / 缺口透传）  ////////////////////////////////

// TestBlobRangePositiveStartResume 覆盖点 3：起点 >0 且无正式缓存、但有半截前缀——
// 复用既有续传执行方（不新造下载器），内部仍按完整 blob 拼接并写缓存，对客户端只发
// 区间内字节（206 + Content-Range）：
//   - 半截长度 == 起点：直接从上游剩余区间续传，客户端收半截之后的部分；
//   - 半截长度 > 起点：先回放半截里从起点开始的后缀，再接上游剩余区间；
//   - 半截长度 < 起点（缺口）：不能凭空补，维持旧版 206 透传 + 缺口 Warn（gap_bytes），
//     半截不被消费。
func TestBlobRangePositiveStartResume(t *testing.T) {
	t.Run("半截长度等于起点_续传回206", func(t *testing.T) {
		fx := newDisconnectFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xc4, 4096)
		half := 1000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-", half))
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)

		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		wantCR := fmt.Sprintf("bytes %d-%d/%d", half, len(content)-1, len(content))
		if got := w.Header().Get("Content-Range"); got != wantCR {
			t.Errorf("Content-Range = %q, want %q（总长取上游 206 的 total）", got, wantCR)
		}
		if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)-half) {
			t.Errorf("Content-Length = %q, want %q（区间长度）", got, strconv.Itoa(len(content)-half))
		}
		if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
			t.Errorf("X-Cache = %q, want %q（走续传执行方）", got, resumeCacheStatus)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content[half:]) {
			t.Errorf("body 应为 content[%d:]（got %d 字节, want %d 字节）", half, len(got), len(content)-half)
		}
		// 上游只收到续传自己的一路 Range（起点 = 半截长度），客户端的区间不参与上游请求
		want := []string{fmt.Sprintf("bytes=%d-", half)}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v", got, want)
		}
		// 执行方内部按完整 blob 写缓存（缓存条目必须完整，后续任意起点才能本地切片）
		e, ok := fx.disk.Get(key)
		if !ok || !bytes.Equal(e.Body, content) {
			t.Error("续传完成后缓存应命中且内容为完整 blob")
		}
		if _, ok := fx.disk.PartSize(key); ok {
			t.Error("正式缓存落盘后半截应已删除")
		}
	})

	t.Run("半截长度大于起点_回放后缀再续传", func(t *testing.T) {
		fx := newDisconnectFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xc6, 4096)
		start, half := 100, 1000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)

		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		wantCR := fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content))
		if got := w.Header().Get("Content-Range"); got != wantCR {
			t.Errorf("Content-Range = %q, want %q", got, wantCR)
		}
		if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)-start) {
			t.Errorf("Content-Length = %q, want %q", got, strconv.Itoa(len(content)-start))
		}
		if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
			t.Errorf("X-Cache = %q, want %q", got, resumeCacheStatus)
		}
		// 起点落在半截内部：客户端收到「半截里的 content[100:1000] + 上游剩余」= content[100:]
		if got := w.Body.Bytes(); !bytes.Equal(got, content[start:]) {
			t.Errorf("body 应为 content[%d:]（got %d 字节, want %d 字节）", start, len(got), len(content)-start)
		}
		// 上游仍只按半截长度续传（起点 = 半截长度），不重复下载已知前缀
		want := []string{fmt.Sprintf("bytes=%d-", half)}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v", got, want)
		}
		e, ok := fx.disk.Get(key)
		if !ok || !bytes.Equal(e.Body, content) {
			t.Error("续传完成后缓存应为完整 blob")
		}
	})

	t.Run("半截长度小于起点_缺口维持206透传加Warn", func(t *testing.T) {
		fx := newDisconnectFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xc7, 4096)
		half, start := 1000, 2000 // 缺口 1000 字节：本地没有 content[1000:2000]，不能凭空补
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)

		// 旧版 206 透传：客户端 Range 原样发上游，body 为上游原文后缀
		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d（缺口时维持旧版透传）, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got != "BYPASS" {
			t.Errorf("X-Cache = %q, want BYPASS（缺口无法补齐，不得标 RESUME）", got)
		}
		if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)) {
			t.Errorf("Content-Range = %q, want 上游 206 的 bytes %d-%d/%d", got, start, len(content)-1, len(content))
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content[start:]) {
			t.Errorf("body 应为 content[%d:]（got %d 字节）", start, len(got))
		}
		want := []string{fmt.Sprintf("bytes=%d-", start)}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v（客户端 Range 原样透传）", got, want)
		}
		// 缺口 Warn（运维定位用）：含起点、半截长度与缺口字节数
		warns := logLinesOf(fx.logBuf.String(), "起点超出本地半截长度")
		if len(warns) == 0 {
			t.Fatalf("应输出缺口 Warn，实际:\n%s", fx.logBuf.String())
		}
		wantLineField(t, warns[0], fmt.Sprintf("range_start=%d", start),
			fmt.Sprintf("part_bytes=%d", half), fmt.Sprintf("gap_bytes=%d", start-half))
		// 半截不被消费、不被修改（本次走透传，不写缓存）
		data := partBytes(t, fx.disk.PartPath(key))
		if !bytes.Equal(data, content[:half]) {
			t.Errorf("缺口透传不应触碰半截（got %d 字节, want %d 字节）", len(data), half)
		}
		if _, ok := fx.disk.Get(key); ok {
			t.Error("缺口透传不应写缓存")
		}
	})
}

// //////////////////  4：起点 0 + 断开 → 半截落盘  //////////////////////////////////////////////

// TestBlobRangeZeroStartDisconnectKeepsPartial 覆盖点 4：客户端（docker）带 `bytes=0-`
// 下载中途上游断连——入口把 Range 剥离后按完整 GET 处理，半截 sink 照常注入，
// 已收前缀必须落盘（否则下次重试又从 0 开始，正是现场 302MB 层白扔 30MB 的根因之一）；
// 客户端收到的是已 commit 的 206 截断响应（docker 会以 Range 重试）。
func TestBlobRangeZeroStartDisconnectKeepsPartial(t *testing.T) {
	fx := newDisconnectFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xc5, 8192)
	half := 4096
	fx.reg.setContent(digest, content)
	fx.reg.setAbort(digest, half) // 200 全量路径写出前 half 字节后断连

	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	r.Header.Set("Range", "bytes=0-")
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)

	// 已 commit：zeroRangeWriter 把内部 200 改写成 206，body 为已收前缀（断连后截断）
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Code = %d, want %d（响应头已 commit，截断的 206）, body: %s",
			w.Code, http.StatusPartialContent, w.Body.String())
	}
	if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content)) {
		t.Errorf("Content-Range = %q, want 完整 blob 的 bytes 0-%d/%d", got, len(content)-1, len(content))
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content[:half]) {
		t.Errorf("客户端已收 body 应为上游原文前缀 content[:%d]（got %d 字节）", half, len(got))
	}
	// 半截已落盘且字节数正确（Range 被剥离后 captureStreamPartial 照常保留）
	n, ok := fx.disk.PartSize(key)
	if !ok || n != int64(half) {
		t.Fatalf("半截大小 = (%d, %v), want (%d, true)（起点 0 断开必须保留前缀）", n, ok, half)
	}
	if data := partBytes(t, fx.disk.PartPath(key)); !bytes.Equal(data, content[:half]) {
		t.Errorf("半截内容与上游原文前缀不一致（got %d 字节, want %d 字节）", len(data), half)
	}
	// 半截数据绝不入库
	if _, ok := fx.disk.Get(key); ok {
		t.Error("下载中断不应写正式缓存（半截数据绝不入库）")
	}
}

// //////////////////  解析边界（刻意不接管的形态）  //////////////////////////////////////////

// TestParseBlobRange 表驱动验证 parseBlobRange 的「接管边界」（client_range.go）：
// 只接管「docker blob 目标 + 单个 bytes 单位、起点显式、终点不早于起点的区间」，其余
// 一律返回 false（保持旧版 BYPASS 透传，语义零变化）——
//   - 后缀区间（bytes=-500）、多区间（逗号）、非 bytes 单位、起点非数字、终点小于起点；
//   - 多个 Range 头（RFC 允许重复头，等价多区间）；
//   - 非 blob 目标（manifest / 完整 URL）。
//
// parseZeroRangeBlob 是它的零起点子集（既有调用方按「是否等价完整 GET」判定，签名不变），
// 本用例同时验证两个入口的一致性。
func TestParseBlobRange(t *testing.T) {
	blobTarget := "v2/demo/app/blobs/sha256:" + strings.Repeat("ab", 32)
	const manifestTarget = "v2/demo/app/manifests/latest"
	tests := []struct {
		name   string
		target string
		ranges []string
		want   blobRangeSpec
		wantOK bool
	}{
		{"起点0开区间_接管", blobTarget, []string{"bytes=0-"}, blobRangeSpec{start: 0}, true},
		{"起点100开区间_接管", blobTarget, []string{"bytes=100-"}, blobRangeSpec{start: 100}, true},
		{"带终点_接管", blobTarget, []string{"bytes=100-199"}, blobRangeSpec{start: 100, hasEnd: true, end: 199}, true},
		{"空格容忍_接管", blobTarget, []string{"bytes= 0 - "}, blobRangeSpec{start: 0}, true},
		{"单位大小写不敏感_接管", blobTarget, []string{"BYTES=7-"}, blobRangeSpec{start: 7}, true},
		{"后缀区间不接管", blobTarget, []string{"bytes=-500"}, blobRangeSpec{}, false},
		{"多区间不接管", blobTarget, []string{"bytes=0-99,200-299"}, blobRangeSpec{}, false},
		{"多个Range头不接管", blobTarget, []string{"bytes=0-99", "bytes=200-299"}, blobRangeSpec{}, false},
		{"非bytes单位不接管", blobTarget, []string{"items=0-99"}, blobRangeSpec{}, false},
		{"起点非数字不接管", blobTarget, []string{"bytes=abc-"}, blobRangeSpec{}, false},
		{"终点小于起点不接管", blobTarget, []string{"bytes=200-100"}, blobRangeSpec{}, false},
		{"终点非数字不接管", blobTarget, []string{"bytes=0-xyz"}, blobRangeSpec{}, false},
		{"manifest目标不接管", manifestTarget, []string{"bytes=0-"}, blobRangeSpec{}, false},
		{"完整URL目标不接管", "https://mirror.example.com/v2/app/blobs/sha256:" + strings.Repeat("ab", 32), []string{"bytes=0-"}, blobRangeSpec{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://example.com/x", nil)
			for _, v := range tt.ranges {
				r.Header.Add("Range", v)
			}
			got, ok := parseBlobRange(r, tt.target)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("parseBlobRange(%q, %v) = (%+v, %v), want (%+v, %v)",
					tt.target, tt.ranges, got, ok, tt.want, tt.wantOK)
			}
			// parseZeroRangeBlob = 零起点子集：只有「接管且起点 0」才为 true
			wantZero := tt.wantOK && tt.want.start == 0
			if zgot, zok := parseZeroRangeBlob(r, tt.target); zok != wantZero || (zok && zgot != tt.want) {
				t.Errorf("parseZeroRangeBlob(%q, %v) = (%+v, %v), want (%+v, %v)",
					tt.target, tt.ranges, zgot, zok, tt.want, wantZero)
			}
		})
	}
}
