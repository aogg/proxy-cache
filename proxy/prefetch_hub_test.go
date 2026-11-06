package proxy

// prefetch_hub_test.go —— docker 预取 blob 在途流共享（prefetch_hub.go，提交 1c5f29f）的单测：
// 全部使用 httptest 假 registry 上游（不启动本服务、不发真实外网请求），缓存目录一律
// t.TempDir()，测试只写临时目录，不含任何 e2e。与 prefetch_hub.go 同包（白盒），可直接
// 构造 prefetchStreamEntry / 调用 subscribe、waitHeaderReady、writeBroadcast、finishFailed
// 等未导出方法；复用 prefetch_test.go 的假 registry 辅助（fakeManifestRegistry /
// prefetchUpstreamRec / prefetchBlobKey 等）与 quietLogger。覆盖：
//   - 主场景：预取先行（上游慢速分块发 blob）时客户端 GET 撞上 hub 直接搭车——
//     X-Cache: PREFETCH、body 与上游原文逐字节一致、上游对 blob 仅 1 路请求、
//     Content-Length / Docker-Content-Digest 照抄响应头快照；
//   - 多客户端并发搭车：≥3 个并发 GET 同一 blob 全部 200 且 body 一致、上游仍仅 1 路；
//   - 预取失败广播：未 commit 订阅者收到 failed 广播后 tryServeFromPrefetchHub 返回
//     false（走原回源路径）、条目从 hub 移除、临时文件已删；失败移除后客户端 GET
//     降级原回源拿到完整 body（上游第二路成功）；已 commit 订阅者收到 failed 广播
//     终止本次响应（已回放内容为完整内容的截断前缀，半截数据不入库）；
//   - 白盒：writeBroadcast 逐订阅者独立 copy（写者复用缓冲不覆写订阅者内容）、
//     缓冲满摘除慢订阅者、close-once 终态幂等（finishDone/finishFailed 重复调用与
//     交错安全）、linger 滞留期内 lookup 命中且 done 条目兜底走 HIT、waitHeaderReady
//     快照唤醒/超时/终态语义、unsubscribe 幂等；
//   - 边界：带 Range 的 GET / HEAD 不搭车（走原回源路径，X-Cache 非 PREFETCH）、
//     docker-prefetch-enabled 关闭时 hub 无条目不搭车；
//   - 客户端中途断开：搭车 reader 写失败（请求 ctx 已取消）后订阅注销，预取那一路
//     继续完成并写缓存成功（上游仍仅 1 路）。

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// hubBlobContent 构造确定性伪随机 blob 内容（同参数必得同内容，供逐字节比对）。
func hubBlobContent(seed byte, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed ^ byte(i*7%251)
	}
	return b
}

// hubUpstream 慢速分块 blob 假上游（实现 http.Handler）：
//   - manifest 路径复用嵌入的 fakeManifestRegistry（本文件多数用例直接调 prefetchOneBlob，
//     manifest 分支仅为防御兜底）；
//   - blob 路径按 digest 返回 content[digest]，按 chunkSize 分块写出、块间 sleep delay
//     （拉长流式下载窗口，供搭车时序观察），并附 Docker-Content-Digest / Content-Length /
//     Content-Type 响应头（搭车响应头快照断言用）；
//   - hold1[digest]：首路在响应头前阻塞，放行后写半截 body 即断连（模拟上游中途断连，
//     预取失败广播场景——先失败移除、客户端再降级原回源的确定性时序）；
//   - hold2[digest]：首路写出首块后阻塞，放行后立即断连（快照已就绪、订阅者已 commit
//     的失败广播场景——终止本次搭车响应的确定性时序）。
//   - 每次进入 blob 处理即经 rec 计数（与 fakeManifestRegistry 同一记录器，断言口径一致）。
type hubUpstream struct {
	*fakeManifestRegistry
	mu        sync.Mutex
	content   map[string][]byte        // digest -> blob 完整内容
	chunkSize int                      // 分块大小
	delay     time.Duration            // 块间隔（首块不延迟：探测即取走）
	hold1     map[string]chan struct{} // digest -> 首路「响应头前阻塞」闸门
	hold2     map[string]chan struct{} // digest -> 首路「首块后阻塞」闸门
}

// newHubUpstream 构建慢速分块假上游。
func newHubUpstream() *hubUpstream {
	return &hubUpstream{
		fakeManifestRegistry: &fakeManifestRegistry{rec: newPrefetchUpstreamRec()},
		content:              make(map[string][]byte),
		hold1:                make(map[string]chan struct{}),
		hold2:                make(map[string]chan struct{}),
	}
}

// setContent 设置某 digest 的 blob 完整内容。
func (u *hubUpstream) setContent(digest string, body []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.content[digest] = body
}

// setChunkConfig 设置分块大小与块间隔。
func (u *hubUpstream) setChunkConfig(chunk int, delay time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.chunkSize, u.delay = chunk, delay
}

// armHold1 布防首路「响应头前阻塞」闸门并返回通道（测试 close 放行）。
func (u *hubUpstream) armHold1(digest string) chan struct{} {
	c := make(chan struct{})
	u.mu.Lock()
	defer u.mu.Unlock()
	u.hold1[digest] = c
	return c
}

// armHold2 布防首路「首块后阻塞」闸门并返回通道（测试 close 放行）。
func (u *hubUpstream) armHold2(digest string) chan struct{} {
	c := make(chan struct{})
	u.mu.Lock()
	defer u.mu.Unlock()
	u.hold2[digest] = c
	return c
}

// ServeHTTP 按 manifest / blob 分流。
func (u *hubUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/blobs/") {
		u.serveBlob(w, r)
		return
	}
	u.fakeManifestRegistry.ServeHTTP(w, r)
}

// hubFlush 立即冲刷响应（存在 Flusher 时），让分块逐块到达客户端。
func hubFlush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// serveBlob 处理 blob 请求（计数、闸门与分块写出，见类型注释）。
func (u *hubUpstream) serveBlob(w http.ResponseWriter, r *http.Request) {
	digest := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	u.rec.enter(digest)
	defer u.rec.leave()
	nth := u.rec.calls(digest) // enter 已计数：1 = 本次为该 digest 的首路请求

	u.mu.Lock()
	body := u.content[digest]
	hold1, hold2 := u.hold1[digest], u.hold2[digest]
	chunk, delay := u.chunkSize, u.delay
	u.mu.Unlock()
	if len(body) == 0 {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK) // HEAD 无 body：只回响应头
		return
	}

	w.WriteHeader(http.StatusOK)
	if nth == 1 && hold1 != nil {
		<-hold1 // 首路阻塞在响应头阶段（此时快照未就绪，搭车者未 commit）
		_, _ = w.Write(body[:len(body)/2])
		hubFlush(w)
		panic(http.ErrAbortHandler) // 放行后写半截即断连：预取流式读失败 → finishFailed
	}
	if nth == 1 && hold2 != nil {
		_, _ = w.Write(body[:chunk]) // 首块先行（探测取走即快照就绪，订阅者可 commit）
		hubFlush(w)
		<-hold2
		panic(http.ErrAbortHandler) // 放行后断连：已 commit 订阅者收到 failed 广播
	}
	for off := 0; off < len(body); off += chunk {
		end := off + chunk
		if end > len(body) {
			end = len(body)
		}
		_, _ = w.Write(body[off:end])
		hubFlush(w)
		time.Sleep(delay)
	}
}

// hubFixture 聚合一个在途搭车测试环境：慢速分块假上游 + 带 docker 规则的 Server +
// 独立缓存实例（与 Server 同目录，可断言落盘结果）。结构与 newPrefetchFixture 同构。
type hubFixture struct {
	upstream *httptest.Server
	reg      *hubUpstream
	s        *Server
	disk     *cache.Cache
}

// newHubFixture 构建搭车测试环境：域名规则与 url-redirect 模板同 prefetchFixture
// （enabled 传 nil 沿用全局默认开）。
func newHubFixture(t *testing.T, enabled *bool) *hubFixture {
	t.Helper()
	reg := newHubUpstream()
	upstream := httptest.NewServer(reg)
	t.Cleanup(upstream.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	if enabled != nil {
		cfg.DockerPrefetchEnabled = enabled
	}
	templates := []config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "prefetch-hub",
		Match:       `http://registry-proxy-cache\.linkease\.net:5480/`,
		URLRedirect: &templates,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, cache.NewManager(context.Background()), quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	disk, err := cache.New(cacheDir)
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}
	return &hubFixture{upstream: upstream, reg: reg, s: s, disk: disk}
}

// effFor 返回某路径目标在本环境的生效配置。
func (fx *hubFixture) effFor(r *http.Request, target string) config.Options {
	return fx.s.cfg.ResolveOptions(r.Host, target, false)
}

// hubSubCount 白盒读取条目当前订阅者数。
func hubSubCount(ent *prefetchStreamEntry) int {
	ent.mu.Lock()
	defer ent.mu.Unlock()
	return len(ent.subs)
}

// hubWaitEntry 轮询等待 hub 出现 key 的在途条目并返回（预取开始下载即登记）。
func hubWaitEntry(t *testing.T, s *Server, key string, timeout time.Duration) *prefetchStreamEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if ent := s.prefetchHubOrInit().lookup(key); ent != nil {
			return ent
		}
		if time.Now().After(deadline) {
			t.Fatal("等待 hub 在途条目超时（预取未登记或已提前终态移除）")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// hubWaitTmpPath 轮询等待条目登记预取临时文件路径并返回（CreateTemp 后 attachFile）。
func hubWaitTmpPath(t *testing.T, ent *prefetchStreamEntry, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if p := ent.tmpPathOf(); p != "" {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatal("等待预取临时文件登记超时")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// hubWaitSubCount 轮询等待条目订阅者数达到 want（搭车者已 subscribe、尚未 commit）。
func hubWaitSubCount(t *testing.T, ent *prefetchStreamEntry, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for hubSubCount(ent) < want {
		if time.Now().After(deadline) {
			t.Fatalf("等待订阅者数 ≥ %d 超时（当前 %d）", want, hubSubCount(ent))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// hubCommitRecorder 搭车 commit 信号 recorder：首次 WriteHeader（commit 点）时发信号，
// 供测试观测「已 commit」时刻——轮询共享 recorder 字段会与响应写 goroutine 产生数据竞争，
// 经 channel 同步无竞争。body 与 Code 的读取一律等 ServeHTTP 所在 goroutine 结束后进行。
type hubCommitRecorder struct {
	rec       *httptest.ResponseRecorder
	committed chan struct{}
	once      sync.Once
}

func newHubCommitRecorder() *hubCommitRecorder {
	return &hubCommitRecorder{rec: httptest.NewRecorder(), committed: make(chan struct{})}
}

func (w *hubCommitRecorder) Header() http.Header         { return w.rec.Header() }
func (w *hubCommitRecorder) Write(p []byte) (int, error) { return w.rec.Write(p) }
func (w *hubCommitRecorder) WriteHeader(code int) {
	w.rec.WriteHeader(code)
	w.once.Do(func() { close(w.committed) })
}

// waitCommitted 等待 commit 信号（超时 fatal）。
func (w *hubCommitRecorder) waitCommitted(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-w.committed:
	case <-time.After(timeout):
		t.Fatal("等待搭车 commit（WriteHeader）超时")
	}
}

// hubFailingWriter 模拟客户端断开的 ResponseWriter：Write 一律报错。http 服务端在客户端
// 连接断开后 w.Write 即返回错误，搭车实现据此停止读取订阅（与真实断开同一感知机制）。
type hubFailingWriter struct{ rec *httptest.ResponseRecorder }

func (w *hubFailingWriter) Header() http.Header         { return w.rec.Header() }
func (w *hubFailingWriter) WriteHeader(code int)        { w.rec.WriteHeader(code) }
func (w *hubFailingWriter) Write(p []byte) (int, error) { return 0, errors.New("客户端已断开") }

// TestPrefetchHubRideAlongMainStream 主场景：预取先行（上游慢速分块发 blob），客户端 GET
// 撞上 hub 直接搭车——响应 X-Cache: PREFETCH、body 与上游原文逐字节一致、上游对 blob
// 仅 1 路请求、Content-Length / Docker-Content-Digest 照抄预取响应头快照；搭车完成后
// 缓存命中（预取那一路已原子落盘）。
func TestPrefetchHubRideAlongMainStream(t *testing.T) {
	fx := newHubFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0x5a, 8192)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 30*time.Millisecond) // 8 块 × 30ms：拉长在途窗口供搭车

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	// 预取先行：后台执行 prefetchOneBlob（内部登记 hub 条目并流式下载）
	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()

	// 等预取已登记且响应头快照就绪（此时仍在流式下载中，可搭车）
	ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
	if !ent.waitHeaderReady(2 * time.Second) {
		t.Fatal("等待预取响应头快照就绪超时")
	}
	if st := ent.stateOf(); st != prefetchStreamRunning {
		t.Fatalf("快照就绪后条目状态 = %v, want running（应仍在流式下载）", st)
	}

	// 客户端 blob GET（缓存 MISS）撞上在途预取：直接搭车（阻塞至搭车完成）
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	if w.Code != http.StatusOK {
		t.Fatalf("搭车响应 Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != prefetchCacheStatus {
		t.Errorf("搭车响应 X-Cache = %q, want %q", got, prefetchCacheStatus)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Errorf("搭车响应 Content-Length = %q, want 照抄快照 %q", got, strconv.Itoa(len(content)))
	}
	if got := w.Header().Get("Docker-Content-Digest"); got != digest {
		t.Errorf("搭车响应 Docker-Content-Digest = %q, want 照抄快照 %q", got, digest)
	}
	if w.Body.String() != string(content) {
		t.Errorf("搭车 body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游对 blob 回源 %d 次, want 仅预取 1 路（客户端应搭车而非自行回源）", n)
	}
	if n := fx.reg.rec.total(); n != 1 {
		t.Errorf("上游 blob 回源总数 %d, want 1", n)
	}

	// 预取那一路完成：缓存写入成功且内容一致（搭车 body 与最终缓存条目逐字节一致）
	<-prefetchDone
	e, ok := fx.disk.Get(key)
	if !ok {
		t.Fatal("预取完成后缓存应命中，disk.Get 未命中")
	}
	if !bytes.Equal(e.Body, content) {
		t.Errorf("缓存 body 与上游原文不一致（got %d 字节, want %d 字节）", len(e.Body), len(content))
	}
}

// TestPrefetchHubMultiClientRideAlong 多客户端并发搭车：4 个并发 GET 同一 blob 全部 200、
// body 逐字节一致、X-Cache 均为 PREFETCH，上游对该 blob 仍仅预取 1 路请求。
func TestPrefetchHubMultiClientRideAlong(t *testing.T) {
	fx := newHubFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer2
	content := hubBlobContent(0x3c, 8192)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 30*time.Millisecond)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()

	ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
	if !ent.waitHeaderReady(2 * time.Second) {
		t.Fatal("等待预取响应头快照就绪超时")
	}

	// 4 个客户端并发搭车（各自独立订阅，互不阻塞互不干扰）
	const clients = 4
	var wg sync.WaitGroup
	recs := make([]*httptest.ResponseRecorder, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := httptest.NewRecorder()
			fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
			recs[i] = w
		}(i)
	}
	wg.Wait()

	for i, w := range recs {
		if w == nil {
			t.Fatalf("客户端 %d 未收到响应", i)
		}
		if w.Code != http.StatusOK {
			t.Errorf("客户端 %d Code = %d, want %d", i, w.Code, http.StatusOK)
		}
		if got := w.Header().Get("X-Cache"); got != prefetchCacheStatus {
			t.Errorf("客户端 %d X-Cache = %q, want %q", i, got, prefetchCacheStatus)
		}
		if w.Body.String() != string(content) {
			t.Errorf("客户端 %d body 与上游原文不一致（got %d 字节）", i, w.Body.Len())
		}
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游对 blob 回源 %d 次, want 仅预取 1 路（多客户端并发搭车不应各自回源）", n)
	}

	<-prefetchDone
	if _, ok := fx.disk.Get(key); !ok {
		t.Error("预取完成后缓存应命中，disk.Get 未命中")
	}
}

// TestPrefetchHubFailureBroadcast 预取失败广播的三种确定性时序：
//  1. 未 commit 订阅者（等待响应头快照阶段）收到 failed 广播 → tryServeFromPrefetchHub
//     返回 false 走原回源路径，条目从 hub 移除、临时文件已删；
//  2. 预取失败（首路中途断连）移除后客户端 GET → hub 无条目降级原回源，拿到完整 body
//     （上游第二路成功）；
//  3. 已 commit 订阅者收到 failed 广播 → 终止本次搭车响应（已回放内容为完整内容的截断
//     前缀），上游仍仅 1 路，半截数据不入库。
func TestPrefetchHubFailureBroadcast(t *testing.T) {
	t.Run("未commit订阅者失败广播返回false并清理", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		key := prefetchBlobKey(prefetchName, prefetchDigestLayer1)
		hub := fx.s.prefetchHubOrInit()
		ent := hub.register(key, "prefetch-hub", prefetchName, prefetchDigestLayer1, quietLogger())

		// 真实临时文件（模拟预取已落盘部分字节），验证 failed 收尾的清理
		tmp, err := os.CreateTemp(fx.disk.Dir(), prefetchTmpPattern)
		if err != nil {
			t.Fatalf("创建临时文件失败: %v", err)
		}
		if _, err := tmp.WriteString("half"); err != nil {
			t.Fatalf("写临时文件失败: %v", err)
		}
		ent.attachFile(tmp, tmp.Name())

		// 白盒直接调用搭车入口：条目未 setSnapshot（未 commit 语义 = 卡在等快照阶段）
		target := "v2/" + prefetchName + "/blobs/" + prefetchDigestLayer1
		r := prefetchBlobTestRequest(prefetchName, prefetchDigestLayer1)
		handled := make(chan bool, 1)
		go func() {
			w := httptest.NewRecorder()
			handled <- fx.s.tryServeFromPrefetchHub(w, r, key, target, fx.effFor(r, target), fx.disk, time.Now())
		}()
		hubWaitSubCount(t, ent, 1, 2*time.Second) // 已订阅、阻塞在等待快照

		ent.finishFailed(errors.New("上游中途断连")) // 失败广播：close changed + 移除 + 清理

		if got := <-handled; got {
			t.Fatal("未 commit 的失败广播应返回 false（降级走原回源路径），got true")
		}
		if got := hub.lookup(key); got != nil {
			t.Error("failed 条目应已从 hub 移除（后续请求查不到 → 走原回源）")
		}
		if n := hubSubCount(ent); n != 0 {
			t.Errorf("搭车放弃后订阅应注销, got %d", n)
		}
		if _, err := os.Stat(ent.tmpPathOf()); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("failed 收尾应删除临时文件 %s, stat err = %v", ent.tmpPathOf(), err)
		}
	})

	t.Run("预取失败移除后客户端降级原回源拿完整body", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		name, digest := prefetchName, prefetchDigestConfig
		content := hubBlobContent(0x11, 4096)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(1024, 10*time.Millisecond)
		hold := fx.reg.armHold1(digest) // 首路：响应头前阻塞，放行后写半截即断连

		target := "v2/" + name + "/blobs/" + digest
		key := prefetchBlobKey(name, digest)
		r := prefetchBlobTestRequest(name, digest)
		eff := fx.effFor(r, target)

		prefetchDone := make(chan struct{})
		go func() {
			defer close(prefetchDone)
			fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
		}()
		ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
		tmpPath := hubWaitTmpPath(t, ent, 2*time.Second)

		close(hold) // 放行首路 → 上游写半截断连 → 预取失败收尾
		<-prefetchDone

		if got := fx.s.prefetchHubOrInit().lookup(key); got != nil {
			t.Error("预取失败后条目应已从 hub 移除")
		}
		if _, err := os.Stat(tmpPath); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("预取失败后临时文件应已删除, stat err = %v", err)
		}
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Fatalf("失败阶段上游应仅首路 1 次回源, got %d", n)
		}

		// 客户端 GET：hub 无条目 → 降级走原回源（第二路）→ 拿到完整 body
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
		if w.Code != http.StatusOK {
			t.Fatalf("降级回源 Code = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("X-Cache"); got == prefetchCacheStatus {
			t.Errorf("降级路径不应标记 X-Cache: %q", got)
		}
		if w.Body.String() != string(content) {
			t.Errorf("降级 body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
		}
		// 3 路 = 首路预取失败（半截已保留）+ 客户端 GET 先按断点续传尝试（本假上游不支持
		// Range、对 bytes=<n>- 回 200 → 判定明确拒绝续传、丢弃半截）+ 降级既有全量回源。
		// （docker-partial-resume-enabled 默认开启后，预取失败保留半截使客户端 GET 多出
		// 一路续传探测；客户端拿到的 body / 缓存语义与旧版一致，见 partial_resume.go）
		if n := fx.reg.rec.calls(digest); n != 3 {
			t.Errorf("上游应共 3 路回源（首路预取失败 + 续传探测被 200 拒绝 + 降级全量成功）, got %d", n)
		}
		if _, ok := fx.disk.Get(key); !ok {
			t.Error("客户端降级回源成功后应写入缓存")
		}
	})

	t.Run("已commit订阅者收到失败广播终止响应", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		name, digest := prefetchName, prefetchDigestOther
		content := hubBlobContent(0x2b, 4096)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(1024, 10*time.Millisecond)
		hold := fx.reg.armHold2(digest) // 首路：首块后阻塞，放行后断连

		target := "v2/" + name + "/blobs/" + digest
		key := prefetchBlobKey(name, digest)
		r := prefetchBlobTestRequest(name, digest)
		eff := fx.effFor(r, target)

		prefetchDone := make(chan struct{})
		go func() {
			defer close(prefetchDone)
			fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
		}()
		ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
		if !ent.waitHeaderReady(2 * time.Second) {
			t.Fatal("等待预取响应头快照就绪超时")
		}

		// 客户端搭车（后台 goroutine），等 commit 信号（响应头已固化）
		cw := newHubCommitRecorder()
		clientDone := make(chan struct{})
		go func() {
			defer close(clientDone)
			fx.s.ServeHTTP(cw, prefetchBlobTestRequest(name, digest))
		}()
		cw.waitCommitted(t, 2*time.Second)

		close(hold) // 放行 → 上游断连 → 预取失败广播 → 已 commit 的搭车响应终止
		<-clientDone

		if cw.rec.Code != http.StatusOK {
			t.Errorf("已 commit 的响应 Code = %d, want %d（commit 后头已固化）", cw.rec.Code, http.StatusOK)
		}
		if got := cw.rec.Header().Get("X-Cache"); got != prefetchCacheStatus {
			t.Errorf("已 commit 的响应 X-Cache = %q, want %q", got, prefetchCacheStatus)
		}
		// 已回放内容必须是完整内容的截断前缀（前缀 + 失败前的增量，绝不出现错字节）
		if body := cw.rec.Body.String(); !strings.HasPrefix(string(content), body) {
			t.Errorf("终止响应的已回放内容不是完整内容的前缀（got %d 字节）", cw.rec.Body.Len())
		}

		<-prefetchDone
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游应仅预取 1 路回源, got %d", n)
		}
		if got := fx.s.prefetchHubOrInit().lookup(key); got != nil {
			t.Error("预取失败后条目应已从 hub 移除")
		}
		if _, ok := fx.disk.Get(key); ok {
			t.Error("预取失败半截数据不入库，缓存不应命中")
		}
	})
}

// TestPrefetchHubWhiteboxEntry 白盒验证条目内部机制：
// writeBroadcast 逐订阅者独立 copy、缓冲满摘除慢订阅者、close-once 终态幂等、
// linger 滞留期内 lookup 命中且 done 条目兜底走 HIT、waitHeaderReady 三态语义、
// unsubscribe 幂等。
func TestPrefetchHubWhiteboxEntry(t *testing.T) {
	t.Run("writeBroadcast逐订阅者独立copy", func(t *testing.T) {
		hub := newPrefetchStreamHub()
		ent := hub.register("k1", "prefetch-hub", prefetchName, prefetchDigestLayer1, quietLogger())
		sub1, _, ok1 := ent.subscribe()
		sub2, _, ok2 := ent.subscribe()
		if !ok1 || !ok2 {
			t.Fatalf("running 条目订阅应成功, got (%v, %v)", ok1, ok2)
		}

		// 写者用复用缓冲连续广播两块，中途覆写缓冲内容：订阅者此前拿到的块不得被覆写
		buf := []byte("chunk-AAAA")
		if err := ent.writeBroadcast(buf); err != nil {
			t.Fatalf("writeBroadcast 意外报错: %v", err)
		}
		copy(buf, "chunk-BBBB") // 模拟读循环复用 buf 写下一块
		if err := ent.writeBroadcast(buf); err != nil {
			t.Fatalf("writeBroadcast 意外报错: %v", err)
		}
		if got := ent.writtenOf(); got != 20 {
			t.Errorf("written = %d, want 20", got)
		}

		ent.finishDone() // 关闭订阅：缓冲内增量仍可收完

		for i, sub := range []*prefetchStreamSub{sub1, sub2} {
			got1 := <-sub.ch
			got2 := <-sub.ch
			if string(got1) != "chunk-AAAA" || string(got2) != "chunk-BBBB" {
				t.Errorf("订阅者 %d 收到 (%q, %q), want (chunk-AAAA, chunk-BBBB)——写者复用缓冲不得覆写订阅者内容", i, got1, got2)
			}
			if &got1[0] == &buf[0] {
				t.Errorf("订阅者 %d 的块与写者缓冲共享底层数组（应逐订阅者独立 copy）", i)
			}
		}

		// unsubscribe 幂等：重复注销不 panic；逐个注销后订阅集合清空
		ent.unsubscribe(sub1)
		ent.unsubscribe(sub1)
		if n := hubSubCount(ent); n != 1 {
			t.Errorf("注销 sub1 后订阅数 = %d, want 1（sub2 仍在）", n)
		}
		ent.unsubscribe(sub2)
		if n := hubSubCount(ent); n != 0 {
			t.Errorf("全部注销后订阅数 = %d, want 0", n)
		}
	})

	t.Run("缓冲满摘除慢订阅者", func(t *testing.T) {
		hub := newPrefetchStreamHub()
		ent := hub.register("k2", "prefetch-hub", prefetchName, prefetchDigestLayer2, quietLogger())
		sub, _, ok := ent.subscribe()
		if !ok {
			t.Fatal("running 条目订阅应成功")
		}

		// 不消费地连续广播：prefetchHubSubBuffer 次填满缓冲，再 +1 次即缓冲满摘除
		chunk := []byte("x")
		for i := 0; i < prefetchHubSubBuffer; i++ {
			if err := ent.writeBroadcast(chunk); err != nil {
				t.Fatalf("第 %d 次 writeBroadcast 意外报错: %v", i+1, err)
			}
		}
		if ent.subOverflow(sub) {
			t.Fatal("缓冲未满不应摘除订阅者")
		}
		if err := ent.writeBroadcast(chunk); err != nil {
			t.Fatalf("摘除广播的 writeBroadcast 意外报错: %v", err)
		}
		if !ent.subOverflow(sub) {
			t.Error("缓冲满后应标记订阅者 overflow（消费过慢摘除）")
		}
		if n := hubSubCount(ent); n != 0 {
			t.Errorf("慢订阅者应被摘除, 订阅数 = %d, want 0", n)
		}
		// 摘除 = close channel：缓冲内数据收完后收到 closed
		for i := 0; i < prefetchHubSubBuffer; i++ {
			<-sub.ch
		}
		if _, ok := <-sub.ch; ok {
			t.Error("摘除后订阅 channel 应关闭")
		}

		// 摘除不影响写者与其余订阅者：新订阅者正常收块
		sub2, _, ok := ent.subscribe()
		if !ok {
			t.Fatal("摘除后新订阅应成功")
		}
		if err := ent.writeBroadcast([]byte("after")); err != nil {
			t.Fatalf("writeBroadcast 意外报错: %v", err)
		}
		if got := <-sub2.ch; string(got) != "after" {
			t.Errorf("新订阅者收到 %q, want %q", got, "after")
		}
	})

	t.Run("close-once终态幂等", func(t *testing.T) {
		hub := newPrefetchStreamHub()

		// finishDone 幂等；done 后迟到的 finishFailed 不得覆盖终态
		ent := hub.register("k3", "prefetch-hub", prefetchName, prefetchDigestConfig, quietLogger())
		sub, _, _ := ent.subscribe()
		ent.finishDone()
		ent.finishDone() // 二次调用应无动作（仅 running 态生效），重复 close 会 panic
		ent.finishFailed(errors.New("迟到失败"))
		if st, terr := ent.terminal(); st != prefetchStreamDone || terr != nil {
			t.Errorf("终态 = (%v, %v), want (done, nil)——迟到失败不得覆盖 done", st, terr)
		}
		// 无缓冲增量：收尾关闭后应立即收到 closed
		if _, ok := <-sub.ch; ok {
			t.Error("done 收尾应关闭订阅 channel")
		}

		// finishFailed 幂等：err 以首次为准、重复 close 订阅 channel 不 panic
		ent2 := hub.register("k4", "prefetch-hub", prefetchName, prefetchDigestOther, quietLogger())
		sub2, _, _ := ent2.subscribe()
		ent2.finishFailed(errors.New("boom"))
		ent2.finishFailed(errors.New("boom2"))
		if st, terr := ent2.terminal(); st != prefetchStreamFailed || terr == nil || terr.Error() != "boom" {
			t.Errorf("终态 = (%v, %v), want (failed, boom)——重复失败不得覆盖首次 err", st, terr)
		}
		if _, ok := <-sub2.ch; ok {
			t.Error("failed 收尾应关闭订阅 channel")
		}
		if got := hub.lookup("k4"); got != nil {
			t.Error("failed 条目应已移除")
		}
		ent2.cleanupFile() // 无临时文件时清理幂等（不 panic）
	})

	t.Run("linger滞留lookup命中与done兜底HIT", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		key := prefetchBlobKey(prefetchName, prefetchDigestLayer1)
		cached := &cache.Entry{Status: http.StatusOK, Header: http.Header{}, Body: []byte("done-hit-body"), StoredAt: time.Now()}
		if err := fx.disk.Set(key, cached); err != nil {
			t.Fatalf("预置缓存失败: %v", err)
		}

		hub := fx.s.prefetchHubOrInit()
		ent := hub.register(key, "prefetch-hub", prefetchName, prefetchDigestLayer1, quietLogger())
		ent.finishDone() // 置 done 并登记 linger（滞留 1 分钟才到期移除）

		// 滞留期内 lookup 仍命中，且 done 条目取出即移除（后续查询走 disk.Get 既有 HIT）
		if got := hub.lookup(key); got != ent {
			t.Fatal("done 滞留期内 lookup 应命中该条目")
		}
		if got := hub.lookup(key); got != nil {
			t.Error("done 条目取出后应移除")
		}

		// done 兜底出口：tryServeFromPrefetchHub 对 done 条目直接走既有 HIT 回放
		ent2 := hub.register(key, "prefetch-hub", prefetchName, prefetchDigestLayer1, quietLogger())
		ent2.finishDone()
		r := prefetchBlobTestRequest(prefetchName, prefetchDigestLayer1)
		w := httptest.NewRecorder()
		handled := fx.s.tryServeFromPrefetchHub(w, r, key, "v2/"+prefetchName+"/blobs/"+prefetchDigestLayer1,
			fx.effFor(r, "v2/"+prefetchName+"/blobs/"+prefetchDigestLayer1), fx.disk, time.Now())
		if !handled {
			t.Fatal("done 条目应由兜底 HIT 出口接管本次响应")
		}
		if got := w.Header().Get("X-Cache"); got != "HIT" {
			t.Errorf("done 兜底 X-Cache = %q, want HIT", got)
		}
		if w.Body.String() != "done-hit-body" {
			t.Errorf("done 兜底 body = %q, want 缓存内容 done-hit-body", w.Body.String())
		}
	})

	t.Run("waitHeaderReady快照唤醒超时与终态", func(t *testing.T) {
		hub := newPrefetchStreamHub()
		ent := hub.register("k5", "prefetch-hub", prefetchName, prefetchDigestLayer2, quietLogger())

		// 未就绪且未终态：超时返回 false
		if ent.waitHeaderReady(30 * time.Millisecond) {
			t.Error("未就绪且未终态的 waitHeaderReady 应超时返回 false")
		}

		// setSnapshot 唤醒等待者并写入快照
		done := make(chan bool, 1)
		go func() { done <- ent.waitHeaderReady(5 * time.Second) }()
		time.Sleep(20 * time.Millisecond)
		h := http.Header{}
		h.Set("Content-Length", "3")
		ent.setSnapshot(http.StatusOK, h, "http://upstream/example")
		if ok := <-done; !ok {
			t.Error("快照就绪应唤醒 waitHeaderReady 返回 true")
		}
		st, header, source := ent.snapshot()
		if st != http.StatusOK || header.Get("Content-Length") != "3" || source != "http://upstream/example" {
			t.Errorf("快照 = (%d, %v, %s), want (200, Content-Length:3, http://upstream/example)", st, header, source)
		}

		// 终态后无需等待立即返回 true
		ent.finishFailed(errors.New("boom"))
		if !ent.waitHeaderReady(30 * time.Millisecond) {
			t.Error("终态条目的 waitHeaderReady 应立即返回 true")
		}
	})
}

// TestPrefetchHubBoundaries 边界：带 Range 的 GET 与 HEAD 不搭车（走原回源路径），
// docker-prefetch-enabled 关闭时 hub 无条目不搭车。
func TestPrefetchHubBoundaries(t *testing.T) {
	t.Run("带Range的GET不搭车走BYPASS", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0x4d, 4096)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(1024, 20*time.Millisecond)

		target := "v2/" + name + "/blobs/" + digest
		key := prefetchBlobKey(name, digest)
		r := prefetchBlobTestRequest(name, digest)
		eff := fx.effFor(r, target)

		prefetchDone := make(chan struct{})
		go func() {
			defer close(prefetchDone)
			fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
		}()
		ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
		if !ent.waitHeaderReady(2 * time.Second) {
			t.Fatal("等待预取响应头快照就绪超时")
		}

		// 带 Range 的 GET 在 handleGet 前段已分流 BYPASS（Range 请求绕过缓存），不搭车：
		// 自行走原回源（第二路），X-Cache 非 PREFETCH，body 为上游完整回放
		rr := prefetchBlobTestRequest(name, digest)
		rr.Header.Set("Range", "bytes=2048-")
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, rr)

		if got := w.Header().Get("X-Cache"); got == prefetchCacheStatus {
			t.Errorf("带 Range 的 GET 不应搭车, X-Cache = %q", got)
		}
		if w.Body.String() != string(content) {
			t.Errorf("Range 请求 body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
		}
		if n := fx.reg.rec.calls(digest); n != 2 {
			t.Errorf("上游应 2 路回源（预取 + Range 请求自身）, got %d", n)
		}

		<-prefetchDone
		if _, ok := fx.disk.Get(key); !ok {
			t.Error("预取那一路不受 Range 请求影响，完成后缓存应命中")
		}
	})

	t.Run("HEAD不搭车", func(t *testing.T) {
		fx := newHubFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer2
		content := hubBlobContent(0x6e, 4096)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(1024, 20*time.Millisecond)

		target := "v2/" + name + "/blobs/" + digest
		key := prefetchBlobKey(name, digest)
		r := prefetchBlobTestRequest(name, digest)
		eff := fx.effFor(r, target)

		prefetchDone := make(chan struct{})
		go func() {
			defer close(prefetchDone)
			fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
		}()
		ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
		if !ent.waitHeaderReady(2 * time.Second) {
			t.Fatal("等待预取响应头快照就绪超时")
		}
		if st := ent.stateOf(); st != prefetchStreamRunning {
			t.Fatalf("条目状态 = %v, want running", st)
		}

		// HEAD 走 handleHead 原路径（查缓存 MISS → 候选回源），不触碰 hub
		hr := httptest.NewRequest(http.MethodHead, "http://"+hostDockerRegistry+"/"+target, nil)
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, hr)

		if got := w.Header().Get("X-Cache"); got == prefetchCacheStatus {
			t.Errorf("HEAD 不应搭车, X-Cache = %q", got)
		}
		if n := fx.reg.rec.calls(digest); n != 2 {
			t.Errorf("上游应 2 路请求（预取 GET + HEAD 自身）, got %d", n)
		}

		<-prefetchDone
		if _, ok := fx.disk.Get(key); !ok {
			t.Error("预取完成后缓存应命中")
		}
	})

	t.Run("prefetch关闭无条目不搭车", func(t *testing.T) {
		fx := newHubFixture(t, boolPtr(false)) // docker-prefetch-enabled=false
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0x7f, 2048)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(512, 5*time.Millisecond)

		// 即使显式触发预取入口，开关关闭也绝不登记 hub 条目、绝不起预取任务
		manifestTarget := "v2/" + name + "/manifests/latest"
		mr := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+manifestTarget, nil)
		fx.s.maybePrefetchImageLayers(mr, manifestTarget,
			prefetchManifestEntry(prefetchCTSchema2, prefetchManifestJSON(prefetchDigestConfig, digest)),
			fx.effFor(mr, manifestTarget), fx.disk)
		fx.s.waitPrefetchIdle()

		key := prefetchBlobKey(name, digest)
		if got := fx.s.prefetchHubOrInit().lookup(key); got != nil {
			t.Error("开关关闭不应有 hub 条目")
		}
		if n := fx.reg.rec.total(); n != 0 {
			t.Errorf("开关关闭不应预取，上游 blob 回源 %d 次, want 0", n)
		}

		// 客户端 GET 走既有原回源路径（MISS），不搭车
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
		if w.Code != http.StatusOK {
			t.Fatalf("GET Code = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("X-Cache"); got == prefetchCacheStatus {
			t.Errorf("开关关闭不应搭车, X-Cache = %q", got)
		}
		if w.Body.String() != string(content) {
			t.Errorf("body 与上游原文不一致（got %d 字节, want %d 字节）", w.Body.Len(), len(content))
		}
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游应仅客户端自身 1 路回源, got %d", n)
		}
	})
}

// TestPrefetchHubClientDisconnectPrefetchContinues 客户端中途断开：搭车 reader 写响应体
// 失败（请求 ctx 已取消）后仅注销订阅，预取那一路不受影响继续完成并写缓存成功——
// 上游对该 blob 仍仅 1 路请求，缓存命中且内容与上游原文一致。
func TestPrefetchHubClientDisconnectPrefetchContinues(t *testing.T) {
	fx := newHubFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0x77, 8192)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 30*time.Millisecond)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()
	ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
	if !ent.waitHeaderReady(2 * time.Second) {
		t.Fatal("等待预取响应头快照就绪超时")
	}

	// 白盒直接调搭车入口：请求 ctx 已取消 + Write 一律报错（模拟客户端连接断开——
	// 实现经 w.Write 错误感知断开并停止读取订阅，预取继续填缓存）
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fw := &hubFailingWriter{rec: httptest.NewRecorder()}
	handled := fx.s.tryServeFromPrefetchHub(fw, r.WithContext(ctx), key, target, eff, fx.disk, time.Now())
	if !handled {
		t.Fatal("搭车应接管本次响应（断开感知后仍返回 handled=true）")
	}
	if n := hubSubCount(ent); n != 0 {
		t.Errorf("客户端断开后订阅应注销, got %d", n)
	}

	// 预取不受影响：继续完成并写缓存成功（上游仍仅 1 路）
	<-prefetchDone
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游应仅预取 1 路回源, got %d", n)
	}
	e, ok := fx.disk.Get(key)
	if !ok {
		t.Fatal("客户端断开后预取仍应写缓存成功，disk.Get 未命中")
	}
	if !bytes.Equal(e.Body, content) {
		t.Errorf("缓存 body 与上游原文不一致（got %d 字节, want %d 字节）", len(e.Body), len(content))
	}
}
