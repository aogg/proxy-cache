package proxy

// cache_stream_test.go —— 缓存 HIT 流式回放（内存安全）回归单测：
//
// 2026-11-20 线上事故：registry-proxy-cache 在拉取 767MB 的 layer 时，HIT 侧用
// io.ReadAll 把整个缓存体读进内存（约 1GB 峰值），ENOMEM 后旧版又把读取错误当
// 「文件损坏」删掉缓存，客户端被迫回源慢速镜像站。本文件锁定修复后的语义：
//   - 32MB 级缓存条目的 HIT 回放全程按区间从磁盘流式读（内存占用与 body 大小无关）；
//   - 回放字节与缓存原文完全一致；
//   - 任意 Range 切片继续正确（起点 0 / 起点 > 0 / 起点越界 416）；
//   - 缓存文件在 HIT 后仍然存在（不被误删）。
//
// 全部使用进程内 httptest 假上游与 t.TempDir() 缓存目录，无 e2e、无真实外网请求。

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"proxy-cache/cache"
)

// discardRecorder 是只统计字节数的 ResponseWriter（不缓存 body）：
// 用于「HIT 回放不把 body 读进内存」的内存度量——用 httptest.ResponseRecorder 会把
// 32MB body 原样存进内存缓冲，反而污染度量。
type discardRecorder struct {
	header  http.Header
	status  int
	written int64
}

// Header 实现 http.ResponseWriter。
func (d *discardRecorder) Header() http.Header {
	if d.header == nil {
		d.header = make(http.Header)
	}
	return d.header
}

// Write 实现 http.ResponseWriter：只累计字节数，不保存内容。
func (d *discardRecorder) Write(p []byte) (int, error) {
	d.written += int64(len(p))
	return len(p), nil
}

// WriteHeader 实现 http.ResponseWriter。
func (d *discardRecorder) WriteHeader(code int) { d.status = code }

// Flush 实现 http.Flusher（空实现）。
func (d *discardRecorder) Flush() {}

// sha256Digest 返回内容的真实 blob digest（"sha256:<hex>"）。
func sha256Digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// seedStreamedBlobCache 用流式写缓存（cache.Put）预写一份完整 blob 条目：
// 内容与 digest 一致（生产默认摘要校验可过），写入过程本身也不整份物化 body。
func seedStreamedBlobCache(t *testing.T, disk *cache.Cache, key, digest string, content []byte) {
	t.Helper()
	if _, err := disk.Put(key, &cache.Entry{
		Status:   http.StatusOK,
		Header:   http.Header{"Content-Type": []string{"application/octet-stream"}, "Docker-Content-Digest": []string{digest}},
		StoredAt: time.Now(),
		TTL:      0, // 永不过期
	}, bytes.NewReader(content)); err != nil {
		t.Fatalf("流式预写缓存失败: %v", err)
	}
}

// TestCacheHitLargeBlobStreamingReplay 32MB 缓存条目的 HIT 回放（事故核心场景）：
//   - 完整 GET：200 + 全量字节一致 + 上游零请求；
//   - `Range: bytes=0-`：206 + 完整 Content-Range/Content-Length + 全量字节一致；
//   - `Range: bytes=<中点>-`：206 + 区间切片字节一致；
//   - `Range: bytes=<总长>-`（起点越界）：416 + `Content-Range: bytes */<total>`；
//   - 全程 HIT（X-Cache: HIT），缓存文件在回放后仍存在。
func TestCacheHitLargeBlobStreamingReplay(t *testing.T) {
	const size = 32 << 20 // 32MB
	content := hubBlobContent(0xd7, size)
	digest := sha256Digest(content)
	fx := newDisconnectFixture(t, nil)
	// 生产默认摘要校验（真实 sha256）：HIT 侧复核必须走流式路径（不整份读进内存）
	fx.s.blobDigestVerify = nil
	name := prefetchName
	key := prefetchBlobKey(name, digest)
	seedStreamedBlobCache(t, fx.disk, key, digest, content)
	if _, err := fx.disk.Open(key); err != nil {
		t.Fatalf("预写条目应可流式打开: %v", err)
	}

	t.Run("完整GET", func(t *testing.T) {
		r := prefetchBlobTestRequest(name, digest)
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got != "HIT" {
			t.Errorf("X-Cache = %q, want HIT（32MB 条目应命中流式缓存）", got)
		}
		if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
			t.Errorf("Content-Length = %q, want %d", got, len(content))
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content) {
			t.Errorf("HIT 回放字节与缓存原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
		}
		if n := fx.reg.rec.calls(digest); n != 0 {
			t.Errorf("HIT 回放的上游请求 %d 次, want 0", n)
		}
	})

	t.Run("起点0区间", func(t *testing.T) {
		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", "bytes=0-")
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)
		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want 206", w.Code)
		}
		wantCR := fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content))
		if got := w.Header().Get("Content-Range"); got != wantCR {
			t.Errorf("Content-Range = %q, want %q", got, wantCR)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content) {
			t.Errorf("起点 0 区间回放字节不一致（got %d 字节, want %d 字节）", len(got), len(content))
		}
	})

	t.Run("起点大于0区间", func(t *testing.T) {
		start := int64(size/2 + 12345)
		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)
		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want 206, body: %s", w.Code, w.Body.String())
		}
		wantCR := fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content))
		if got := w.Header().Get("Content-Range"); got != wantCR {
			t.Errorf("Content-Range = %q, want %q", got, wantCR)
		}
		if got := w.Header().Get("Content-Length"); got != strconv.FormatInt(int64(len(content))-start, 10) {
			t.Errorf("Content-Length = %q, want %d", got, int64(len(content))-start)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content[start:]) {
			t.Errorf("起点>0 区间切片不一致（got %d 字节, want %d 字节）", len(got), int64(len(content))-start)
		}
		if n := fx.reg.rec.calls(digest); n != 0 {
			t.Errorf("本地切片的上游请求 %d 次, want 0", n)
		}
	})

	t.Run("起点越界416", func(t *testing.T) {
		r := prefetchBlobTestRequest(name, digest)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-", len(content)))
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, r)
		if w.Code != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Code = %d, want 416, body: %s", w.Code, w.Body.String())
		}
		wantCR := fmt.Sprintf("bytes */%d", len(content))
		if got := w.Header().Get("Content-Range"); got != wantCR {
			t.Errorf("Content-Range = %q, want %q", got, wantCR)
		}
		if n := fx.reg.rec.calls(digest); n != 0 {
			t.Errorf("416 不应回源，实际请求 %d 次", n)
		}
	})

	// HIT 回放后缓存文件必须仍在（旧版把临时读取错误当损坏删文件，下一次请求就 MISS）
	if _, err := fx.disk.Open(key); err != nil {
		t.Errorf("HIT 回放后缓存条目必须保留: %v", err)
	}
}

// TestCacheHitLargeBlobDoesNotBufferBody 32MB 缓存条目 HIT 回放不把 body 读进内存：
// 用丢弃型 ResponseWriter 度量回放前后的进程累计分配字节数（TotalAlloc 差值）——
// 流式实现只分配块缓冲（量级 < 8MB），旧版 io.ReadAll 必然 ≥ body 大小（32MB）。
func TestCacheHitLargeBlobDoesNotBufferBody(t *testing.T) {
	const size = 32 << 20
	content := hubBlobContent(0xe3, size)
	digest := sha256Digest(content)
	fx := newDisconnectFixture(t, nil)
	fx.s.blobDigestVerify = nil // 生产默认摘要校验（流式 sha256）
	name := prefetchName
	key := prefetchBlobKey(name, digest)
	seedStreamedBlobCache(t, fx.disk, key, digest, content)

	r := prefetchBlobTestRequest(name, digest)
	w := &discardRecorder{}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fx.s.ServeHTTP(w, r)
	runtime.ReadMemStats(&after)
	if w.status != http.StatusOK {
		t.Fatalf("Code = %d, want 200", w.status)
	}
	if w.written != int64(len(content)) {
		t.Errorf("写给客户端的字节数 = %d, want %d", w.written, len(content))
	}
	const limit = 8 << 20 // 流式实现的分配量级上限（远小于 32MB body）
	if delta := after.TotalAlloc - before.TotalAlloc; delta > limit {
		t.Errorf("HIT 回放累计分配 %d 字节 > %d 字节：body 被整份读进内存了（必须流式回放）",
			delta, limit)
	}
}

// assertNoCacheTempFiles 断言缓存目录里没有 .tmp- 残留（流式写缓存与落盘累积缓冲
// 用完必须清理：Commit/Abort 或缓存写入成功后删除临时来源）。
func assertNoCacheTempFiles(t *testing.T, dir string) {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取缓存目录失败: %v", err)
	}
	for _, de := range des {
		if strings.HasPrefix(de.Name(), ".tmp-") {
			t.Errorf("缓存目录不应残留临时文件: %s", de.Name())
		}
	}
}

// TestMissLargeBlobStreamsToCacheAndSpills 超过落盘阈值（8MB）的 MISS 回源：
// 响应体边收边写（内存只有块缓冲，超过阈值后整体落盘临时文件），EOF 后按既有防毒
// 语义做 sha256 全量比对，再以「同目录临时文件 + 原子 rename」写缓存；随后 HIT 直接
// 命中同一份内容。这是 2026-11-20 事故里 767MB 层 MISS 主路径（旧版 bytes.Buffer 累积）
// 的内存安全回归。
func TestMissLargeBlobStreamsToCacheAndSpills(t *testing.T) {
	const size = 9 << 20 // 9MB > bodySpillThreshold(8MB)：强制走落盘累积
	content := hubBlobContent(0xf1, size)
	digest := sha256Digest(content)
	fx := newDisconnectFixture(t, nil)
	fx.s.blobDigestVerify = nil // 生产默认 sha256 强校验（流式摘要）
	name := prefetchName
	key := prefetchBlobKey(name, digest)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(64<<10, 0)

	// 第一次：MISS 回源并写缓存
	r := prefetchBlobTestRequest(name, digest)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("MISS 回源 Code = %d, want 200, body: %s", w.Code, w.Body.String())
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Fatalf("MISS 回源写给客户端的字节不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游请求 %d 次, want 1", n)
	}

	// 缓存内容正确（流式写入 + 摘要校验通过），且没有临时文件残留
	e, err := fx.disk.Open(key)
	if err != nil {
		t.Fatalf("MISS 完成后应写入缓存: %v", err)
	}
	if e.BodySize() != int64(len(content)) {
		t.Errorf("缓存条目长度 = %d, want %d", e.BodySize(), len(content))
	}
	actual, serr := e.SHA256()
	_ = e.Close()
	if serr != nil || actual != strings.TrimPrefix(digest, "sha256:") {
		t.Errorf("缓存条目摘要 = %s, err=%v, want %s", actual, serr, strings.TrimPrefix(digest, "sha256:"))
	}
	assertNoCacheTempFiles(t, fx.disk.Dir())

	// 第二次：HIT（本地流式回放，上游零新增请求）
	r2 := prefetchBlobTestRequest(name, digest)
	w2 := httptest.NewRecorder()
	fx.s.ServeHTTP(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("HIT Code = %d, want 200", w2.Code)
	}
	if got := w2.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("X-Cache = %q, want HIT", got)
	}
	if got := w2.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("HIT 回放字节不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("HIT 不应再回源，累计上游请求 %d 次, want 1", n)
	}
}
