// resume_early_response_test.go —— blob 半截续传/搭车「立即响应（首字节时序）」单测：
// 现场背景（2026-11，registry-proxy-cache.linkease.net:5480）：docker pull 输出停在
// 「Pulling from ...」长时间零进度，而代理侧 source=resume 的节拍日志显示半截续传在
// 正常推进。根因：旧时序里客户端响应头被推迟到候选扫描命中之后（逐候选探测 + 401
// 匿名 token dance，blob 扫描预算 60s 起步），docker 客户端等响应头约 30s 就超时断开，
// 于是「反复重试 → 每次都等扫描 → 永远见不到进度」；搭车路径等响应头快照最长 30s 的
// 窗口同理。修复后：执行方登记在途条目后立即合成响应头 commit 并回放本地半截前缀
// （总长取 manifest 索引，未知则 chunked）；搭车订阅者对快照未就绪的在途条目同样立即
// 合成头 commit。本文件用真实 HTTP 客户端（httptest.Server 承载 Server）+「响应头写出
// 前阻塞」的慢上游闸门，断言：
//  1. 执行方立即响应：上游候选被闸门挂死期间，客户端已拿到响应头（Content-Length=总长）
//     与完整本地前缀字节——首字节不再依赖上游命中；放行闸门后剩余区间拼接出完整 body；
//  2. 总长未知：不设 Content-Length（chunked），响应头同样立即到达（docker 至少有
//     字节进度），放行后 body 完整；
//  3. 区间请求（起点 >0、总长已知）：立即 commit 206 + Content-Range + 区间长度
//     Content-Length，body 为区间内字节；
//  4. 搭车立即响应：白盒登记快照未就绪的续传条目，客户端请求立即拿到 X-Cache: PREFETCH
//     响应头与本地前缀，随后随广播增量/完成收尾拼出完整 body。
//
// 全部 httptest 假上游、缓存目录 t.TempDir()，不含任何 e2e。
package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// earlyServeFixture 在 resumeFixture 之上把 Server 挂到真实 httptest.Server（Host 头
// 覆写为既有 docker registry 测试主机以命中规则），供首字节时序断言用。
type earlyServeFixture struct {
	*resumeFixture
	ts *httptest.Server
}

func newEarlyServeFixture(t *testing.T) *earlyServeFixture {
	t.Helper()
	fx := newResumeFixture(t, nil)
	ts := httptest.NewServer(fx.s)
	t.Cleanup(ts.Close)
	// 失败时转储服务端日志（时序问题定位用）
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("server logs:\n%s", fx.logBuf.String())
		}
	})
	return &earlyServeFixture{resumeFixture: fx, ts: ts}
}

// earlyBlobGet 构造打向真实测试服务器的 blob GET 请求（Host 头覆写命中 docker 规则）。
func (fx *earlyServeFixture) earlyBlobGet(name, digest string) *http.Request {
	t := "v2/" + name + "/blobs/" + digest
	req, err := http.NewRequest(http.MethodGet, fx.ts.URL+"/"+t, nil)
	if err != nil {
		panic(err)
	}
	req.Host = hostDockerRegistry
	return req
}

// TestBlobResumeEarlyResponseBeforeUpstream 覆盖点 1+2：执行方立即响应——上游候选被
// 闸门挂死期间客户端已拿到响应头与完整本地前缀；放行后拼接完整 body。子表：
//   - CL已知（manifest 索引登记总长）：Content-Length=总长；
//   - 总长未知（无 manifest 记录）：不设 Content-Length（chunked），docker 仍有字节进度。
func TestBlobResumeEarlyResponseBeforeUpstream(t *testing.T) {
	t.Run("总长已知_CL等于总长", func(t *testing.T) {
		fx := newEarlyServeFixture(t)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xe1, 8192)
		half := 3000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)
		fx.setManifestSize(digest, int64(len(content)))
		gate := fx.reg.armHoldHeaders(digest) // 上游候选挂死：扫描阶段拿不到响应头
		release := fx.reg.holdHeadersClose(gate)
		defer release()

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		resp, err := http.DefaultClient.Do(fx.earlyBlobGet(name, digest))
		if err != nil {
			t.Fatalf("客户端 Do 失败: %v", err)
		}
		defer resp.Body.Close()
		// 此刻上游闸门仍关闭（扫描不可能命中）：响应头已到达 = 立即响应成立
		if st := fx.reg.headerGateOpen(digest); st {
			t.Fatal("测试自检失败：上游闸门应仍处于关闭（挂死）状态")
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Code = %d, want %d（立即响应一次完整 200）", resp.StatusCode, http.StatusOK)
		}
		if got := resp.Header.Get("X-Cache"); got != resumeCacheStatus {
			t.Errorf("X-Cache = %q, want %q", got, resumeCacheStatus)
		}
		if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(content)) {
			t.Errorf("Content-Length = %q, want 总长 %q（总长已知时立即 commit 即携带）", got, strconv.Itoa(len(content)))
		}
		if got := resp.Header.Get("Docker-Content-Digest"); got != digest {
			t.Errorf("Docker-Content-Digest = %q, want %q", got, digest)
		}
		// 本地前缀立即回放：在闸门关闭期间读满 half 字节（客户端首字节进度）
		prefix := make([]byte, half)
		if _, err := io.ReadFull(resp.Body, prefix); err != nil {
			t.Fatalf("闸门关闭期间未读到完整本地前缀（立即回放失败）: %v", err)
		}
		if !bytes.Equal(prefix, content[:half]) {
			t.Error("回放前缀与本地半截不一致")
		}

		// 放行上游闸门：剩余区间拼接，body 恰好拼出完整 blob
		release()
		rest, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("读剩余 body 失败: %v", err)
		}
		if !bytes.Equal(append(prefix, rest...), content) {
			t.Errorf("最终 body 与上游完整原文不一致（got %d 字节, want %d 字节）", half+len(rest), len(content))
		}
		// 上游视角：恰好一路 Range 续传探测，起点 = 半截长度
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游 blob 回源 %d 次, want 1", n)
		}
		if rs := fx.reg.rangesOf(digest); len(rs) != 1 || rs[0] != fmt.Sprintf("bytes=%d-", half) {
			t.Errorf("上游收到的 Range 头 = %v, want [bytes=%d-]", rs, half)
		}
		// 生命周期：写缓存成功后半截删除、缓存命中。注意 CL 已知的响应在客户端侧按
		// 字节计数即「读完」（早于服务端 handler 收尾/写缓存），这里必须轮询等待落盘
		resumeWaitUntil(t, 2*time.Second, func() bool {
			_, ok := fx.disk.PartSize(key)
			return !ok
		}, "等待续传完成写缓存（半截删除）超时")
		if e, ok := fx.disk.Get(key); !ok || !bytes.Equal(e.Body, content) {
			t.Error("续传完成后缓存应命中且内容一致")
		}
	})

	t.Run("总长未知_chunked立即响应", func(t *testing.T) {
		fx := newEarlyServeFixture(t)
		name, digest := prefetchName, prefetchDigestLayer2
		content := hubBlobContent(0xe2, 8192)
		half := 2000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)
		// 不登记 manifest size：总长未知 → chunked
		gate := fx.reg.armHoldHeaders(digest)
		release := fx.reg.holdHeadersClose(gate)
		defer release()

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		resp, err := http.DefaultClient.Do(fx.earlyBlobGet(name, digest))
		if err != nil {
			t.Fatalf("客户端 Do 失败: %v", err)
		}
		defer resp.Body.Close()
		if st := fx.reg.headerGateOpen(digest); st {
			t.Fatal("测试自检失败：上游闸门应仍处于关闭（挂死）状态")
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Code = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		if got := resp.Header.Get("Content-Length"); got != "" {
			t.Errorf("Content-Length = %q, want 空（总长未知时不谎报，Go 按 chunked 传输）", got)
		}
		prefix := make([]byte, half)
		if _, err := io.ReadFull(resp.Body, prefix); err != nil {
			t.Fatalf("闸门关闭期间未读到完整本地前缀: %v", err)
		}
		release()
		rest, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("读剩余 body 失败: %v", err)
		}
		if !bytes.Equal(append(prefix, rest...), content) {
			t.Errorf("最终 body 与上游完整原文不一致（got %d 字节, want %d 字节）", half+len(rest), len(content))
		}
	})
}

// TestBlobResumeEarlyResponseClientRange206 覆盖点 3：区间请求（起点 >0 且 <= 半截长度，
// 总长已知）立即 commit 206 + Content-Range + 区间长度 Content-Length，body 只含区间内
// 字节且与原文逐字节一致（内部仍按完整 blob 拼接并写缓存）。
func TestBlobResumeEarlyResponseClientRange206(t *testing.T) {
	fx := newEarlyServeFixture(t)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xe3, 8192)
	half := 3000
	start := int64(500)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(4096, 0)
	fx.setManifestSize(digest, int64(len(content)))
	gate := fx.reg.armHoldHeaders(digest)
	defer func() { fx.reg.holdHeadersClose(gate) }()

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	req := fx.earlyBlobGet(name, digest)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("客户端 Do 失败: %v", err)
	}
	defer resp.Body.Close()
	// 闸门关闭期间响应头已到达：206 + Content-Range（区间窗口）
	if st := fx.reg.headerGateOpen(digest); st {
		t.Fatal("测试自检失败：上游闸门应仍处于关闭（挂死）状态")
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("Code = %d, want %d（区间请求立即 commit 206）", resp.StatusCode, http.StatusPartialContent)
	}
	if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)) {
		t.Errorf("Content-Range = %q, want bytes %d-%d/%d", got, start, len(content)-1, len(content))
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(content)-int(start)) {
		t.Errorf("Content-Length = %q, want 区间长度 %d", got, len(content)-int(start))
	}
	// 先放行上游闸门（服务端正在扫描等待上游命中，不先放行 body 不会推进）再读 body
	release := fx.reg.holdHeadersClose(gate)
	release()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读 body 失败: %v", err)
	}
	if !bytes.Equal(body, content[start:]) {
		t.Errorf("body 应为区间内字节 content[%d:]（got %d 字节, want %d 字节）", start, len(body), len(content)-int(start))
	}
	// 区间请求内部仍按完整 blob 写缓存（CL 已知响应客户端侧按字节计数即读完，早于
	// 服务端收尾，轮询等待落盘）
	resumeWaitUntil(t, 2*time.Second, func() bool {
		_, ok := fx.disk.Get(key)
		return ok
	}, "等待区间请求写缓存超时")
	if e, ok := fx.disk.Get(key); !ok || !bytes.Equal(e.Body, content) {
		t.Error("区间请求内部仍应按完整 blob 写缓存")
	}
}

// TestRidealongEarlyCommitBeforeSnapshot 覆盖点 4：搭车立即响应——白盒登记快照未就绪
// 的续传在途条目（模拟执行方仍在候选扫描），客户端请求立即拿到 X-Cache: PREFETCH 合成
// 响应头与本地前缀字节，随后随广播增量与 done 收尾拼出完整 body。
func TestRidealongEarlyCommitBeforeSnapshot(t *testing.T) {
	fx := newEarlyServeFixture(t)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xe4, 8192)
	half := 1500
	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	// 白盒登记快照未就绪的续传条目（不 setSnapshot、不触发扫描——模拟执行方在候选扫描中）
	ent := fx.s.prefetchHubOrInit().registerResume(key, "early-ridealong", name, digest, int64(half), quietLogger())
	ent.attachFile(nil, fx.disk.PartPath(key))
	if ent.headerReadyOf() {
		t.Fatal("测试自检失败：条目快照应处于未就绪状态")
	}

	done := make(chan struct{})
	respCh := make(chan *http.Response, 1)
	go func() {
		defer close(done)
		r, err := http.DefaultClient.Do(fx.earlyBlobGet(name, digest))
		if err != nil {
			t.Errorf("客户端 Do 失败: %v", err)
			respCh <- nil
			return
		}
		respCh <- r // channel 发布建立 happens-before：主协程此后读 resp 字段无竞争
	}()
	// 等响应头就绪 + 前缀字节可读（立即响应：不依赖任何快照/广播）
	waitResp := func() *http.Response {
		t.Helper()
		select {
		case r := <-respCh:
			if r == nil {
				t.Fatal("客户端 Do 失败（见上方 Errorf）")
			}
			return r
		case <-time.After(2 * time.Second):
			t.Fatal("等待搭车立即响应超时（响应头未及时到达）")
			return nil
		}
	}
	resp := waitResp()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Code = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("X-Cache"); got != prefetchCacheStatus {
		t.Errorf("X-Cache = %q, want %q", got, prefetchCacheStatus)
	}
	if got := resp.Header.Get("Content-Length"); got != "" {
		t.Errorf("Content-Length = %q, want 空（快照未就绪时总长未知 → chunked，不谎报）", got)
	}
	prefix := make([]byte, half)
	if _, err := io.ReadFull(resp.Body, prefix); err != nil {
		t.Fatalf("未读到完整本地前缀（立即回放失败）: %v", err)
	}
	if !bytes.Equal(prefix, content[:half]) {
		t.Error("回放前缀与本地半截不一致")
	}

	// 执行方广播一段增量 → 客户端随即收到；置 done → 客户端收完缓冲即完整 body
	if err := ent.writeBroadcast(content[half : half+1024]); err != nil {
		t.Fatalf("writeBroadcast 意外失败: %v", err)
	}
	chunk := make([]byte, 1024)
	if _, err := io.ReadFull(resp.Body, chunk); err != nil {
		t.Fatalf("未读到广播增量: %v", err)
	}
	if !bytes.Equal(chunk, content[half:half+1024]) {
		t.Error("广播增量与原文不一致")
	}
	ent.finishDone()
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读收尾 body 失败: %v", err)
	}
	// done 收尾 = 写侧宣告「到此为止」：搭车 body 恰为已回放前缀 + 已广播增量（本测试
	// 的假执行方只广播了 1024 字节增量，剩余区间从未广播——绝不出现编造/多余字节）
	if len(rest) != 0 {
		t.Errorf("done 后不应再有收尾字节（got %d 字节）", len(rest))
	}
	if !bytes.Equal(append(prefix, chunk...), content[:half+1024]) {
		t.Errorf("搭车 body 应为前缀+增量（got %d 字节, want %d 字节）", len(prefix)+len(chunk), half+1024)
	}
	<-done
}
