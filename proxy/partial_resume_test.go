package proxy

// partial_resume_test.go —— blob 半截保留与无感续传（partial_resume.go，提交 0650253）的单测：
// 全部使用 httptest 假 registry 上游（不启动本服务、不发真实外网请求），缓存目录一律
// t.TempDir()，测试只写临时目录，不含任何 e2e。与 partial_resume.go 同包（白盒），可直接
// 调用 savePartialPart / keepPrefetchPartial / captureStreamPartial / appendPartChunk 等
// 未导出方法；复用 prefetch_test.go / prefetch_hub_test.go 的既有辅助（prefetchName /
// prefetchDigest* / prefetchBlobKey / prefetchBlobTestRequest / hubBlobContent /
// hubWaitEntry / hubCommitRecorder / quietLogger）。覆盖点（B 系列）：
//   1. 半截产生：客户端流式下载中上游发一半断连 → .part-<key> 存在且内容为已收前缀
//     （对照上游原文逐字节一致）；
//   2. 预取失败保留：预取中途失败 → 临时文件零拷贝改名保留半截（进度不回退）；
//   3. 续传主场景：半截存在 + 客户端完整 GET → 200、X-Cache: RESUME、Content-Length=total、
//      body 与上游完整原文逐字节一致、上游恰好收到一个 Range: bytes=<前缀长>- 请求、
//      完成后半截删除且缓存命中（再 GET 走 HIT）；
//   4. 非 206 降级：上游对 Range 回 200 全量 → 丢弃半截走既有全量回源，客户端拿到完整
//      正确 body（绝无错位）；
//   5. 416：半截已完整直接回放写缓存（不再下载）；
//   6. EOF 长度不符：截断响应、不写缓存、半截保留且更长；
//   7. 并发合并：续传进行中第二客户端搭车（X-Cache: PREFETCH、body 一致、上游对 blob
//      仅一路 Range 请求）；同 blob 预取任务经 flightGroup 与续传合并；
//   8. 边界：起点 >0 的 Range 在「无半截 / 起点超出半截（有缺口）」时维持 206 透传、
//      起点 0 的 Range 等价完整 GET（走续传、对客户端 206 + Content-Range，绝不 BYPASS）、
//      开关关闭不续传不产生半截、manifest 不产生半截；
//   9. 白盒：parseDockerBlobTarget 表驱动、savePartialPart / keepPrefetchPartial 进度
//      不回退、captureStreamPartial 门控（开关 / Range 起点 / 非 blob）、appendPartChunk
//      错位校验终止；
//  10. 防毒缓存：上游对 Range 回「裸 416（无 Content-Range）/ 总长不符 416」→ 绝不把
//      半截当完整 blob 写缓存（invalid 降级全量回源，缓存内容 = 完整原文）；
//  11. 断开续传提醒：半截存在且隔了 ≥1s 的重试 → 提醒日志 + isolated_gap（隔离时长），
//      续传开始日志同样携带。

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

// //////////////////  假上游：支持 Range 的 blob registry  ////////////////////////////////////

// resumeUpstream 支持 Range 续传语义的假 blob registry（实现 http.Handler）：
//   - blob 路径按 digest 返回 content[digest]；带 Range 且未显式忽略时按 RFC 7233 回
//     206（Content-Range 起点与请求一致）或 416（起点 ≥ 总长）；每次进入处理即经 rec
//     计数，并把收到的 Range 头逐次记入 ranges（无 Range 记空串，供断言续传请求形状）；
//   - short[digest]：206 声明完整总长但只发剩余区间前 N 字节后干净 EOF（不设
//     Content-Length，Go 按 chunked 编码正常收尾）——模拟「上游提前断流但连接干净关闭」，
//     命中续传的 EOF 长度不符分支；
//   - abort[digest]：200 全量路径写出前 N 字节后断连（http.ErrAbortHandler）——模拟
//     「客户端流式下载 / 预取下载中途上游断连」，驱动半截产生；
//   - ignore[digest]：忽略 Range 一律回 200 全量——模拟不支持断点续传的上游；
//   - hold[digest]：首路写出 body 首块后阻塞在闸门上（测试 close 放行）——精确控制
//     续传进行中的时序窗口，供搭车 / 预取合并在确定状态下观察；
//   - manifest 路径回 200 + manifestBody（Content-Type 为单平台 image manifest）；
//     manifestBr=true 时写一半断连（验证 manifest 绝不产生半截）。
type resumeUpstream struct {
	mu           sync.Mutex
	rec          *prefetchUpstreamRec
	content      map[string][]byte        // digest -> blob 完整内容
	ranges       map[string][]string      // digest -> 逐次收到的 Range 头（无 Range 记 ""）
	ignore       map[string]bool          // digest -> 忽略 Range 一律 200 全量
	short        map[string]int64         // digest -> 206 只发剩余区间前 N 字节后干净 EOF
	abort        map[string]int           // digest -> 200 全量写出前 N 字节后断连
	hold         map[string]chan struct{} // digest -> 首路「首块后阻塞」闸门
	bad416       map[string]bool          // digest -> Range 探测回 416 且不带 Content-Range（模拟不合规镜像源，防毒缓存回归用）
	fake416Total map[string]int64         // digest -> Range 探测回 416 且 Content-Range 总长伪造为该值（模拟总长与半截不符）
	poisonTail   map[string][]byte        // digest -> Range 剩余区间回毒内容（长度与原文尾段一致；模拟同一 digest 内容错误的异常镜像源，防毒缓存回归用）
	manifest     []byte                   // manifest 响应 body
	manifestBr   bool                     // manifest 写一半后断连
	chunkSize    int                      // 分块大小（默认整段一次写出）
	delay        time.Duration            // 块间隔（拉长续传流窗口）
}

// newResumeUpstream 构建空假上游。
func newResumeUpstream() *resumeUpstream {
	return &resumeUpstream{
		rec:          newPrefetchUpstreamRec(),
		content:      make(map[string][]byte),
		ranges:       make(map[string][]string),
		ignore:       make(map[string]bool),
		short:        make(map[string]int64),
		abort:        make(map[string]int),
		hold:         make(map[string]chan struct{}),
		bad416:       make(map[string]bool),
		fake416Total: make(map[string]int64),
		poisonTail:   make(map[string][]byte),
	}
}

func (u *resumeUpstream) setContent(digest string, body []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.content[digest] = body
}

func (u *resumeUpstream) setChunkConfig(chunk int, delay time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.chunkSize, u.delay = chunk, delay
}

func (u *resumeUpstream) setIgnore(digest string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ignore[digest] = true
}

func (u *resumeUpstream) setShort(digest string, n int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.short[digest] = n
}

func (u *resumeUpstream) setAbort(digest string, n int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.abort[digest] = n
}

// setBad416 标记某 digest 的 Range 探测一律回不带 Content-Range 的 416（模拟不合规镜像源）。
func (u *resumeUpstream) setBad416(digest string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bad416[digest] = true
}

// setFake416Total 标记某 digest 的 Range 探测回 416 且 Content-Range 总长伪造为 total。
func (u *resumeUpstream) setFake416Total(digest string, total int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.fake416Total[digest] = total
}

// setPoisonTail 标记某 digest 的 Range 剩余区间回毒内容（长度须与原文尾段一致；模拟
// 「同一 digest 但内容错误」的异常镜像源——200 全量路径仍回正确原文，防毒缓存回归用）。
func (u *resumeUpstream) setPoisonTail(digest string, tail []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.poisonTail[digest] = tail
}

func (u *resumeUpstream) setManifest(body []byte, broken bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.manifest, u.manifestBr = body, broken
}

// armHold 布防首路「首块后阻塞」闸门并返回通道（测试 close 放行）。
func (u *resumeUpstream) armHold(digest string) chan struct{} {
	c := make(chan struct{})
	u.mu.Lock()
	defer u.mu.Unlock()
	u.hold[digest] = c
	return c
}

// rangesOf 返回某 digest 逐次收到的 Range 头快照（并发安全拷贝）。
func (u *resumeUpstream) rangesOf(digest string) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.ranges[digest]...)
}

// ServeHTTP 按 manifest / blob 分流（其余路径 404）。
func (u *resumeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.Contains(r.URL.Path, "/manifests/"):
		u.serveManifest(w, r)
	case strings.Contains(r.URL.Path, "/blobs/"):
		u.serveBlob(w, r)
	default:
		http.NotFound(w, r)
	}
}

// serveManifest 处理 manifest 请求（manifestBr=true 时写一半断连）。
func (u *resumeUpstream) serveManifest(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	body, broken := u.manifest, u.manifestBr
	u.mu.Unlock()
	if len(body) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", prefetchCTSchema2)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if broken {
		_, _ = w.Write(body[:len(body)/2]) // 写一半即断连：流式回源失败（半截绝不入库）
		hubFlush(w)
		panic(http.ErrAbortHandler)
	}
	_, _ = w.Write(body)
}

// serveBlob 处理 blob 请求（计数、Range 记录与 200/206/416 分流，见类型注释）。
func (u *resumeUpstream) serveBlob(w http.ResponseWriter, r *http.Request) {
	digest := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	u.rec.enter(digest)
	defer u.rec.leave()
	nth := u.rec.calls(digest) // enter 已计数：1 = 本次为该 digest 的首路请求

	u.mu.Lock()
	body := u.content[digest]
	ig, short, abort := u.ignore[digest], u.short[digest], u.abort[digest]
	hold, chunk, delay := u.hold[digest], u.chunkSize, u.delay
	u.mu.Unlock()
	u.addRange(digest, r.Header.Get("Range")) // 先记录再分流：断言续传请求形状用
	if len(body) == 0 {
		http.NotFound(w, r)
		return
	}

	total := int64(len(body))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)

	// //////////////////  Range 请求：206 / 416（或按 ignore 降级 200 全量）  //////////////
	if rng := r.Header.Get("Range"); rng != "" && !ig {
		// bad416 / fake416Total：模拟不合规镜像源（如对任意 Range 一律回 416 且不带
		// Content-Range，或伪造总长）——供防毒缓存回归测试使用
		u.mu.Lock()
		bad, fake := u.bad416[digest], u.fake416Total[digest]
		u.mu.Unlock()
		if bad || fake > 0 {
			if fake > 0 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", fake))
			}
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		n := resumeParseRangeStart(rng)
		if n >= total {
			// 起点 ≥ 总长：上游 416（Content-Range 仅含总长），证明半截已完整
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		remain := body[n:]
		u.mu.Lock()
		if pt := u.poisonTail[digest]; pt != nil {
			// 防毒缓存回归：Range 剩余区间回毒内容（长度与原文尾段一致；200 全量路径
			// 仍回正确原文）——拼接出的「完整长度」内容摘要必不符
			remain = pt
		}
		u.mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", n, total-1, total))
		w.WriteHeader(http.StatusPartialContent)
		if short > 0 && int64(len(remain)) > short {
			// 声明完整总长但提前干净 EOF：不设 Content-Length（chunked 正常收尾），
			// 命中续传的「EOF 长度不符」分支（而非连接异常断开）
			resumeWriteChunks(w, remain[:short], nil, chunk, 0)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(remain)))
		resumeWriteChunks(w, remain, holdAt(hold, nth), chunk, delay)
		return
	}

	// //////////////////  200 全量（无 Range / ignore / HEAD）  /////////////////////////////
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	if abort > 0 && nth == 1 {
		_, _ = w.Write(body[:abort]) // 写前缀即断连：客户端主路径 / 预取的半截产生
		hubFlush(w)
		panic(http.ErrAbortHandler)
	}
	resumeWriteChunks(w, body, holdAt(hold, nth), chunk, delay)
}

func (u *resumeUpstream) addRange(digest, rng string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ranges[digest] = append(u.ranges[digest], rng)
}

// holdAt 返回「首路」请求应使用的闸门（非首路不阻塞，避免多路请求互相卡死）。
func holdAt(hold chan struct{}, nth int) chan struct{} {
	if nth == 1 {
		return hold
	}
	return nil
}

// resumeWriteChunks 分块写出 data：首块写出后若 hold 非 nil 则阻塞在闸门上（测试放行
// 后继续）；其余逐块 hubFlush + delay，拉长流式窗口供时序观察。
func resumeWriteChunks(w http.ResponseWriter, data []byte, hold chan struct{}, chunk int, delay time.Duration) {
	first := chunk
	if first <= 0 || first > len(data) {
		first = len(data)
	}
	if _, err := w.Write(data[:first]); err != nil {
		return
	}
	hubFlush(w)
	if hold != nil {
		<-hold
	}
	for off := first; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		if _, err := w.Write(data[off:end]); err != nil {
			return
		}
		hubFlush(w)
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

// resumeParseRangeStart 解析 "bytes=N-" 形态的 Range 起点（非法按 0，仅测试辅助）。
func resumeParseRangeStart(rng string) int64 {
	v := strings.TrimPrefix(strings.TrimSpace(rng), "bytes=")
	if i := strings.IndexByte(v, '-'); i >= 0 {
		v = v[:i]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// //////////////////  测试环境  ////////////////////////////////////////////////////////////////

// resumeFixture 聚合一个半截续传测试环境：支持 Range 的假 registry + 带 docker 规则的
// Server + 独立缓存实例（与 Server 同目录，可断言半截文件与落盘结果）。docker-prefetch
// 显式关闭（续传测试自带预取注入，避免 manifest 触发的旁路预取干扰断言）。
type resumeFixture struct {
	upstream *httptest.Server
	reg      *resumeUpstream
	s        *Server
	disk     *cache.Cache
	logBuf   bytes.Buffer // 服务日志捕获（断言提醒/观测日志用）
}

// newResumeFixture 构建半截续传测试环境（resumeEnabled 传 nil 沿用全局默认开）。
func newResumeFixture(t *testing.T, resumeEnabled *bool) *resumeFixture {
	t.Helper()
	reg := newResumeUpstream()
	upstream := httptest.NewServer(reg)
	t.Cleanup(upstream.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	if resumeEnabled != nil {
		cfg.DockerPartialResumeEnabled = resumeEnabled
	}
	prefetchOff := false
	cfg.DockerPrefetchEnabled = &prefetchOff
	templates := []config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "partial-resume",
		Match:       `http://registry-proxy-cache\.linkease\.net:5480/`,
		URLRedirect: &templates,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	fx := &resumeFixture{upstream: upstream, reg: reg}
	s, err := New(cfg, cache.NewManager(context.Background()), captureLogger(&fx.logBuf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	// 本夹具的 digest 常量与内容无关（sha256:bbbb... 等伪造值）：关闭写缓存侧的 blob
	// 摘要强校验，使既有断言维持原语义——防毒缓存校验由真实摘要回归测试覆盖
	s.blobDigestVerify = func([]byte, string) (string, bool) { return "", true }
	fx.s = s
	disk, err := cache.New(cacheDir)
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}
	fx.disk = disk
	return fx
}

// effFor 返回某路径目标在本环境的生效配置。
func (fx *resumeFixture) effFor(r *http.Request, target string) config.Options {
	return fx.s.cfg.ResolveOptions(r.Host, target, false)
}

// waitUntil 轮询等待 cond 成立（超时 fatal），供续传时序窗口观察。
func resumeWaitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// partCount 统计缓存目录内半截文件（.part- 前缀）数量。
func partCount(t *testing.T, dir string) int {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读缓存目录失败: %v", err)
	}
	n := 0
	for _, de := range des {
		if strings.HasPrefix(de.Name(), ".part-") {
			n++
		}
	}
	return n
}

// partBytes 读回半截文件内容（不存在即 fatal）。
func partBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读半截文件 %s 失败: %v", path, err)
	}
	return data
}

// newPartialTestServer 构建仅带默认配置的 Server（白盒辅助测试用，缓存写入走独立实例）。
func newPartialTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// //////////////////  B1/B2：半截产生  //////////////////////////////////////////////////////////

// TestPartialCaptureOnClientStreamAbort 覆盖点 1：客户端流式下载中上游发一半断连 →
// .part-<key> 存在且内容为已收前缀（对照上游原文逐字节一致）；半截数据绝不入库；
// 上游仅 1 路请求；客户端响应为已 commit 的截断 200（docker 会以 Range 重试）。
func TestPartialCaptureOnClientStreamAbort(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xa1, 8192)
	half := 4096
	fx.reg.setContent(digest, content)
	fx.reg.setAbort(digest, half) // 200 全量路径写出前 half 字节后断连

	key := prefetchBlobKey(name, digest)

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	// commit 后断连：响应头已固化为 200，body 为截断的已收前缀
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（响应头已 commit，截断响应）", w.Code, http.StatusOK)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content[:half]) {
		t.Errorf("客户端已收 body 与上游原文前缀不一致（got %d 字节, want %d 字节）", len(got), half)
	}

	// 半截文件存在且内容 = 上游原文前缀（逐字节一致）
	data := partBytes(t, fx.disk.PartPath(key))
	if !bytes.Equal(data, content[:half]) {
		t.Errorf("半截文件内容与上游原文前缀不一致（got %d 字节, want %d 字节）", len(data), half)
	}

	// 半截数据绝不入库；上游仅 1 路
	if _, ok := fx.disk.Get(key); ok {
		t.Error("流式回源失败不应写缓存（半截数据绝不入库）")
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1", n)
	}
}

// TestPartialKeptOnPrefetchFailure 覆盖点 2：预取中途失败 → 半截保留（临时文件同目录
// rename 零拷贝改名，进度不回退）；hub 条目移除、临时文件随改名消失、缓存不写。
func TestPartialKeptOnPrefetchFailure(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer2
	content := hubBlobContent(0xa2, 4096)
	half := 2048
	fx.reg.setContent(digest, content)
	fx.reg.setAbort(digest, half)

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)

	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()
	<-prefetchDone

	// 半截保留：临时文件改名零拷贝保留，内容为已收前缀（进度不回退）
	data := partBytes(t, fx.disk.PartPath(key))
	if !bytes.Equal(data, content[:half]) {
		t.Errorf("预取失败保留的半截与上游原文前缀不一致（got %d 字节, want %d 字节）", len(data), half)
	}
	// 失败条目已从 hub 移除；缓存目录无 .tmp- 残留（临时文件已被改名走）
	if got := fx.s.prefetchHubOrInit().lookup(key); got != nil {
		t.Error("预取失败后 hub 条目应已移除")
	}
	des, err := os.ReadDir(fx.disk.Dir())
	if err != nil {
		t.Fatalf("读缓存目录失败: %v", err)
	}
	for _, de := range des {
		if strings.HasPrefix(de.Name(), ".tmp-") {
			t.Errorf("预取失败后不应残留临时文件 %s（应已改名走）", de.Name())
		}
	}
	if n := partCount(t, fx.disk.Dir()); n != 1 {
		t.Errorf("缓存目录半截文件数 = %d, want 1", n)
	}
	// 半截数据不入库；上游仅 1 路
	if _, ok := fx.disk.Get(key); ok {
		t.Error("预取失败不应写缓存（半截数据绝不入库）")
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1", n)
	}
}

// //////////////////  B3：续传主场景  ////////////////////////////////////////////////////////////

// TestBlobResumeMainScenario 覆盖点 3：半截存在 + 客户端完整 GET → 续传执行方回一次完整
// 200（X-Cache: RESUME、Content-Length=总长），body 与上游完整原文逐字节一致；上游恰好
// 收到一个 Range: bytes=<前缀长>- 请求；完成后半截删除、缓存命中（再 GET 走 HIT）——
// 即 finalizeFetchEntry 钩子「正式缓存落盘后半截被删」的生命周期验证。
func TestBlobResumeMainScenario(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xa3, 4096)
	half := 1000
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(3096, 0) // 剩余区间整段一次写出

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)

	// 白盒预置半截（模拟上次下载中断保留的前缀）
	fx.s.savePartialPart(fx.disk, key, content[:half])

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	// 客户端视角：一次完整 200，docker 无需支持 Range、完全无感知
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（续传对客户端回一次完整 200）, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("X-Cache = %q, want %q", got, resumeCacheStatus)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Errorf("Content-Length = %q, want 总长 %q", got, strconv.Itoa(len(content)))
	}
	wantUpstream := fx.upstream.URL + "/" + target
	if got := w.Header().Get("X-Proxy-Upstream"); got != wantUpstream {
		t.Errorf("X-Proxy-Upstream = %q, want 续传命中的候选 %q", got, wantUpstream)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("body 与上游完整原文不一致（got %d 字节, want %d 字节）——本地前缀+剩余区间应恰好拼出完整 blob",
			len(got), len(content))
	}

	// 上游视角：恰好一个 Range 续传请求，起点 = 半截长度
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1（仅续传那一路 Range 下载）", n)
	}
	rs := fx.reg.rangesOf(digest)
	if len(rs) != 1 || rs[0] != fmt.Sprintf("bytes=%d-", half) {
		t.Errorf("上游收到的 Range 头 = %v, want [%q]", rs, fmt.Sprintf("bytes=%d-", half))
	}

	// 生命周期：正式缓存落盘后半截被删（finalizeFetchEntry 钩子），缓存命中且内容一致
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("续传完成后半截应已删除, stat err = %v", err)
	}
	e, ok := fx.disk.Get(key)
	if !ok {
		t.Fatal("续传完成后缓存应命中，disk.Get 未命中")
	}
	if !bytes.Equal(e.Body, content) {
		t.Errorf("缓存 body 与上游原文不一致（got %d 字节, want %d 字节）", len(e.Body), len(content))
	}

	// 再次 GET：正式缓存命中（X-Cache: HIT），上游零请求
	w2 := httptest.NewRecorder()
	fx.s.ServeHTTP(w2, prefetchBlobTestRequest(name, digest))
	if got := w2.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("二次 GET X-Cache = %q, want HIT", got)
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("二次 GET 后上游回源仍应 1 次, got %d", n)
	}
}

// //////////////////  B4/B5/B6：失败降级与边界语义  ////////////////////////////////////////////

// TestBlobResumeFallbackOnNon206 覆盖点 4：上游对 Range 回 200 全量（明确不支持按断点
// 续传）→ 丢弃半截走既有全量回源，客户端拿到完整正确 body（前缀回放与全量绝不拼接错位）。
func TestBlobResumeFallbackOnNon206(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer2
	content := hubBlobContent(0xa4, 4096)
	half := 1000
	fx.reg.setContent(digest, content)
	fx.reg.setIgnore(digest) // 对 Range 也回 200 全量：明确不支持断点续传
	fx.reg.setChunkConfig(1024, 0)

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	// 客户端拿到完整正确 body（降级全量回源，绝无错位）
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got == resumeCacheStatus {
		t.Errorf("X-Cache = %q, 降级全量回源不应标 RESUME", got)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("降级 body 与上游原文不一致（got %d 字节, want %d 字节）——绝不产生错位拼接", len(got), len(content))
	}

	// 上游两路：第一路 Range 尝试被 200 拒绝（载荷判无效），第二路全量回源
	if n := fx.reg.rec.calls(digest); n != 2 {
		t.Errorf("上游 blob 回源 %d 次, want 2（Range 尝试 + 降级全量）", n)
	}
	want := []string{fmt.Sprintf("bytes=%d-", half), ""}
	if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
		t.Errorf("上游收到的 Range 头 = %v, want %v（首路续传、次路全量）", got, want)
	}

	// 半截已丢弃；全量成功后写缓存
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("上游明确拒绝续传后半截应被丢弃, stat err = %v", err)
	}
	e, ok := fx.disk.Get(key)
	if !ok || !bytes.Equal(e.Body, content) {
		t.Error("降级回源成功后应写缓存且内容一致")
	}
}

// TestBlobResumeCompleteOn416 覆盖点 5：半截已完整（上游对 bytes=<n>- 回 416 证实起点
// 已达总长）→ 直接读回半截写缓存回放（X-Cache: RESUME），不再发起任何下载。
func TestBlobResumeCompleteOn416(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestOther
	content := hubBlobContent(0xa5, 2048)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 0)

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content) // 半截 = 完整内容（上次下载只差最后确认）

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("X-Cache = %q, want %q（半截整体回放）", got, resumeCacheStatus)
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("body 与上游原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}

	// 上游仅 1 路（416 探测），Range 起点 = 半截长度 = 总长；绝不再发起全量下载
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1（416 后不再下载）", n)
	}
	want := []string{fmt.Sprintf("bytes=%d-", len(content))}
	if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
		t.Errorf("上游收到的 Range 头 = %v, want %v", got, want)
	}

	// 半截转正式缓存后删除，缓存命中且内容一致
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("416 回放写缓存后半截应已删除, stat err = %v", err)
	}
	e, ok := fx.disk.Get(key)
	if !ok || !bytes.Equal(e.Body, content) {
		t.Error("416 回放后缓存应命中且内容一致")
	}
}

// TestBlobResumeDistrustsUnverifiable416 防毒缓存回归（现场事故：dockerpull.cn 对任意
// Range 一律回不带合规 Content-Range 的 416，旧逻辑据此把 12875538 字节半截固化为
// 133611458 字节 blob 的「正式缓存」，此后每次 pull 都命中毒缓存报 short read）：
// 416 缺合规 `Content-Range: bytes */<total>` 或总长 ≠ 半截长度 → 绝不证实「半截已完整」，
// 按 invalid 降级全量回源——半截丢弃、缓存内容 = 完整原文、客户端拿到完整 body。
func TestBlobResumeDistrustsUnverifiable416(t *testing.T) {
	name, digest := prefetchName, prefetchDigestOther
	content := hubBlobContent(0xb1, 4096)

	assertFullRefetch := func(t *testing.T, fx *resumeFixture, key string, w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got == resumeCacheStatus {
			t.Errorf("X-Cache = %q，不可信 416 绝不能走「半截已完整」整体回放", got)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content) {
			t.Errorf("客户端应拿到完整 body（got %d 字节, want %d 字节）", len(got), len(content))
		}
		if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
			t.Error("invalid 降级后半截应被丢弃")
		}
		e, ok := fx.disk.Get(key)
		if !ok || !bytes.Equal(e.Body, content) {
			t.Errorf("降级全量回源后缓存应为完整原文 %d 字节（绝不把半截固化为正式缓存）", len(content))
		}
	}

	t.Run("裸416_无ContentRange", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		fx.reg.setContent(digest, content)
		fx.reg.setBad416(digest) // 对任意 Range 回 416 且不带 Content-Range

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:1000])
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
		assertFullRefetch(t, fx, key, w)
	})

	t.Run("416总长与半截不符", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		fx.reg.setContent(digest, content)
		fx.reg.setFake416Total(digest, int64(len(content))+1000) // 416 总长伪造为大于半截

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:1000])
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
		assertFullRefetch(t, fx, key, w)
	})
}

// TestParseContentRangeStarTotal 416 Content-Range 专用解析（`bytes */<total>`）表驱动：
// 合规头解析出总长；`*` 总长 / 常规区间形式 / 非法值一律 ok=false（绝不误判半截已完整）。
func TestParseContentRangeStarTotal(t *testing.T) {
	cases := []struct {
		in    string
		total int64
		ok    bool
	}{
		{"bytes */133611458", 133611458, true},
		{"bytes*/133611458", 133611458, true}, // 无空白容忍
		{"  bytes */ 42  ", 42, true},         // 空白容忍
		{"bytes */*", 0, false},               // 总长不可知
		{"bytes 0-9/100", 0, false},           // 常规区间形式不走本解析
		{"bytes */0", 0, false},               // 非法总长
		{"bytes */-3", 0, false},
		{"bytes */abc", 0, false},
		{"100", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, ok := parseContentRangeStarTotal(c.in)
			if ok != c.ok || (ok && got != c.total) {
				t.Fatalf("parseContentRangeStarTotal(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.total, c.ok)
			}
		})
	}
}

// TestBlobResumeReminderLogsIsolatedGap 断开续传提醒：半截存在且「隔离时长」≥1s 的重试
// → 输出提醒日志（一眼可辨「断点续传」并写明隔了多久），续传开始日志同样携带 isolated_gap。
func TestBlobResumeReminderLogsIsolatedGap(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestConfig
	content := hubBlobContent(0xb2, 4096)
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(2048, 0)

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:1000])
	// 半截 mtime 回拨 90s：模拟「客户端断开 90 秒后才回来重试」（半截 mtime = 上次中断时刻）
	partPath := fx.disk.PartPath(key)
	past := time.Now().Add(-90 * time.Second)
	if err := os.Chtimes(partPath, past, past); err != nil {
		t.Fatalf("回拨半截 mtime 失败: %v", err)
	}

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("body 与上游原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}

	logs := fx.logBuf.String()
	if lines := logLinesOf(logs, "blob 断点续传提醒"); len(lines) == 0 {
		t.Fatalf("应输出断开续传提醒日志，实际:\n%s", logs)
	}
	// captureLogger 无双语键改写（生产由 logging.ReplaceAttr 输出 isolated_gap(隔离时长)），
	// 这里按原始英文键断言取值正确
	if !strings.Contains(logs, "isolated_gap=1m30s") {
		t.Errorf("提醒日志应携带隔离时长 isolated_gap=1m30s，实际:\n%s", logs)
	}
	if lines := logLinesOf(logs, "blob 半截续传开始"); len(lines) == 0 || !strings.Contains(lines[0], "isolated_gap=1m30s") {
		t.Errorf("续传开始日志应携带 isolated_gap=1m30s，实际:\n%s", logs)
	}
}

// TestBlobResumeTruncateOnShortEOF 覆盖点 6：上游 206 声明总长但提前干净 EOF（长度不符）
// → 截断本次响应（commit 后头已固化）、不写缓存、半截保留且比注册前缀更长（进度不回退）。
func TestBlobResumeTruncateOnShortEOF(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestConfig
	content := hubBlobContent(0xa6, 4096)
	half, short := 1000, 500
	fx.reg.setContent(digest, content)
	fx.reg.setShort(digest, int64(short)) // 只发剩余区间前 short 字节后干净 EOF

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

	// commit 后失败：响应头已固化为 200，body 截断为「前缀 + 已收剩余」
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d（commit 后失败只能截断响应）", w.Code, http.StatusOK)
	}
	gotBody := w.Body.Bytes()
	if !bytes.Equal(gotBody, content[:half+short]) {
		t.Errorf("截断响应应为 content[:%d]（got %d 字节）——前缀+已收剩余，绝无错位字节", half+short, len(gotBody))
	}

	// 不写缓存；半截保留且更长（1000 → 1500 字节，进度不回退）
	if _, ok := fx.disk.Get(key); ok {
		t.Error("EOF 长度不符不应写缓存（短 body 绝不入库）")
	}
	data := partBytes(t, fx.disk.PartPath(key))
	if !bytes.Equal(data, content[:half+short]) {
		t.Errorf("半截应保留为更长前缀 content[:%d]（got %d 字节）", half+short, len(data))
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1", n)
	}
}

// //////////////////  B7：并发合并  //////////////////////////////////////////////////////////////

// TestBlobResumeConcurrentMerge 覆盖点 7：续传进行中的并发合并。
//   - 第二客户端完整 GET 撞上续传在途条目 → 搭车（X-Cache: PREFETCH、body 与上游原文
//     逐字节一致）；
//   - 同 blob 预取任务经既有 flightGroup 与续传合并（共享执行方结果，不再起第二路下载）；
//   - 上游对 blob 仅一路 Range 请求；完成后半截删除、缓存命中。
func TestBlobResumeConcurrentMerge(t *testing.T) {
	fx := newResumeFixture(t, nil)
	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xa7, 8192)
	half := 2048
	fx.reg.setContent(digest, content)
	fx.reg.setChunkConfig(1024, 30*time.Millisecond) // 剩余区间 6 块 × 30ms：拉长续传窗口
	hold := fx.reg.armHold(digest)                   // 首路（续传那一路）首块后阻塞
	// 闸门释放统一走 sync.Once + defer：任何路径退出（含 t.Fatal）都先放行上游 handler，
	// 再跑 t.Cleanup 的 upstream.Close——否则卡死的 handler 连接会让 Close 永久阻塞，
	// 把单个用例的失败放大成整个测试二进制超时
	var holdOnce sync.Once
	releaseHold := func() { holdOnce.Do(func() { close(hold) }) }
	defer releaseHold()

	target := "v2/" + name + "/blobs/" + digest
	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:half])

	// 执行方：首个客户端完整 GET，成为续传执行方（后台运行）
	execDone := make(chan struct{})
	w1 := httptest.NewRecorder()
	go func() {
		defer close(execDone)
		fx.s.ServeHTTP(w1, prefetchBlobTestRequest(name, digest))
	}()

	// 等续传条目登记、快照就绪、首块剩余区间已追加（进度前进 = 已 commit 且在拼接中）
	ent := hubWaitEntry(t, fx.s, key, 2*time.Second)
	if !ent.waitHeaderReady(2 * time.Second) {
		t.Fatal("等待续传响应头快照就绪超时")
	}
	resumeWaitUntil(t, 2*time.Second, func() bool {
		return ent.writtenOf() > int64(half)
	}, "等待续传执行方追加首块超时（进度未前进）")

	// 第二客户端完整 GET：撞上续传在途条目 → 搭车（等 commit 信号确认走上搭车路径）
	rideDone := make(chan struct{})
	rw := newHubCommitRecorder()
	go func() {
		defer close(rideDone)
		fx.s.ServeHTTP(rw, prefetchBlobTestRequest(name, digest))
	}()
	rw.waitCommitted(t, 2*time.Second)

	// 同 blob 预取任务：经既有 flightGroup 与续传合并（等待方共享执行方结果）
	r := prefetchBlobTestRequest(name, digest)
	eff := fx.effFor(r, target)
	prefetchDone := make(chan struct{})
	go func() {
		defer close(prefetchDone)
		fx.s.prefetchOneBlob(context.Background(), r, name, digest, eff, fx.disk)
	}()

	releaseHold() // 放行上游：剩余区间继续分块发出直至 EOF
	<-execDone
	<-rideDone
	<-prefetchDone

	// 执行方：完整 200 + RESUME + body 与上游原文一致
	if w1.Code != http.StatusOK {
		t.Fatalf("执行方 Code = %d, want %d", w1.Code, http.StatusOK)
	}
	if got := w1.Header().Get("X-Cache"); got != resumeCacheStatus {
		t.Errorf("执行方 X-Cache = %q, want %q", got, resumeCacheStatus)
	}
	if got := w1.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("执行方 body 与上游原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}

	// 搭车方：PREFETCH + body 逐字节一致
	if rw.rec.Code != http.StatusOK {
		t.Errorf("搭车方 Code = %d, want %d", rw.rec.Code, http.StatusOK)
	}
	if got := rw.rec.Header().Get("X-Cache"); got != prefetchCacheStatus {
		t.Errorf("搭车方 X-Cache = %q, want %q", got, prefetchCacheStatus)
	}
	if got := rw.rec.Body.Bytes(); !bytes.Equal(got, content) {
		t.Errorf("搭车方 body 与上游原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
	}

	// 上游对 blob 仅一路 Range 请求（预取合并 + 搭车都不产生第二路下载）
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("上游 blob 回源 %d 次, want 1（续传/搭车/预取合并应为同一路）", n)
	}
	want := []string{fmt.Sprintf("bytes=%d-", half)}
	if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
		t.Errorf("上游收到的 Range 头 = %v, want %v", got, want)
	}

	// 完成后：半截删除、缓存命中且内容一致（预取共享到执行方结果，不重复写缓存）
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("续传完成后半截应已删除, stat err = %v", err)
	}
	e, ok := fx.disk.Get(key)
	if !ok || !bytes.Equal(e.Body, content) {
		t.Error("续传完成后缓存应命中且内容一致")
	}
}

// rangesEqual 比较两个 Range 头记录切片（测试辅助）。
func rangesEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// //////////////////  B8：边界  //////////////////////////////////////////////////////////////////

// TestBlobResumeBoundaries 覆盖点 8：起点 >0 的 Range 在「无半截 / 起点超出半截（缺口）」
// 时维持旧版 206 透传、起点 0 的 Range 等价完整 GET（走续传并对客户端回 206，不再 BYPASS）、
// 开关关闭不续传且不产生半截、manifest 不产生半截。
func TestBlobResumeBoundaries(t *testing.T) {
	t.Run("起点100超出半截_有缺口走206透传", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xb1, 4096)
		half := 40 // 半截只有 40 字节 < 客户端起点 100：中间缺 60 字节，不能凭空补
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		// 起点 100 > 半截 40：续传不接管（缺口无法补齐），维持旧版 206 透传——客户端
		// Range 原样发上游、不写缓存、不消费半截，只留一条缺口 Warn 供运维定位。
		rr := prefetchBlobTestRequest(name, digest)
		rr.Header.Set("Range", "bytes=100-")
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, rr)

		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d（206 透传现状）, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got != "BYPASS" {
			t.Errorf("X-Cache = %q, want BYPASS（缺口无法补齐时维持旧版透传）", got)
		}
		if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 100-%d/%d", len(content)-1, len(content)) {
			t.Errorf("Content-Range = %q, want 上游 206 的 bytes 100-%d/%d", got, len(content)-1, len(content))
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content[100:]) {
			t.Errorf("206 透传 body 应为上游原文 content[100:]（got %d 字节, want %d 字节）", len(got), len(content)-100)
		}
		// 上游收到的是客户端自己的 Range（bytes=100-），不是续传 Range（bytes=40-）
		want := []string{"bytes=100-"}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v（客户端 Range 原样透传）", got, want)
		}
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游 blob 回源 %d 次, want 1", n)
		}
		// 半截不受影响（进度不回退、不被消费）
		data := partBytes(t, fx.disk.PartPath(key))
		if !bytes.Equal(data, content[:half]) {
			t.Errorf("带 Range 的 GET 不应触碰半截（got %d 字节, want %d 字节）", len(data), half)
		}
	})

	t.Run("无半截起点100维持206透传", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer2
		content := hubBlobContent(0xb5, 4096)
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		// 无半截：续传不适用（tryServeBlobResume 直接返回 false），旧版 206 透传
		rr := prefetchBlobTestRequest(name, digest)
		rr.Header.Set("Range", "bytes=100-")
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, rr)

		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d（206 透传现状）, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got != "BYPASS" {
			t.Errorf("X-Cache = %q, want BYPASS（无半截时不接管，维持旧版透传）", got)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content[100:]) {
			t.Errorf("206 透传 body 应为上游原文 content[100:]（got %d 字节, want %d 字节）", len(got), len(content)-100)
		}
		want := []string{"bytes=100-"}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v（客户端 Range 原样透传）", got, want)
		}
		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游 blob 回源 %d 次, want 1", n)
		}
		if n := partCount(t, fx.disk.Dir()); n != 0 {
			t.Errorf("旧版 206 透传不应产生半截文件, got %d 个", n)
		}
	})

	t.Run("起点0等价完整GET_回206不BYPASS", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xb4, 4096)
		half := 1000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		// 起点 0 = 等价完整 GET（docker/containerd 首次请求的真实形态）：入口剥离 Range
		// 后走既有半截续传（X-Cache: RESUME），响应侧改写成 206 + 完整 Content-Range；
		// Range 绝不再透传上游（上游只收到续传自己的 Range），也不再进 BYPASS。
		rr := prefetchBlobTestRequest(name, digest)
		rr.Header.Set("Range", "bytes=0-")
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, rr)

		if w.Code != http.StatusPartialContent {
			t.Fatalf("Code = %d, want %d（起点 0 对客户端仍是 206）, body: %s", w.Code, http.StatusPartialContent, w.Body.String())
		}
		if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content)) {
			t.Errorf("Content-Range = %q, want %q（完整 blob 总长）", got, fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content)))
		}
		if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
			t.Errorf("Content-Length = %q, want %q（区间长度 = 总长）", got, strconv.Itoa(len(content)))
		}
		if got := w.Header().Get("X-Cache"); got != resumeCacheStatus {
			t.Errorf("X-Cache = %q, want %q（起点 0 走续传，不是 BYPASS）", got, resumeCacheStatus)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content) {
			t.Errorf("body 与上游完整原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
		}
		// 上游只收到续传 Range（起点 = 半截长度），客户端的 bytes=0- 绝不透传
		want := []string{fmt.Sprintf("bytes=%d-", half)}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v", got, want)
		}
		// 续传完成后写完整缓存、半截删除
		if _, ok := fx.disk.PartSize(key); ok {
			t.Error("续传完成后半截应已删除")
		}
		e, ok := fx.disk.Get(key)
		if !ok || !bytes.Equal(e.Body, content) {
			t.Error("续传完成后缓存应命中且内容为完整 blob")
		}
	})

	t.Run("开关关闭不续传", func(t *testing.T) {
		fx := newResumeFixture(t, boolPtr(false)) // docker-partial-resume-enabled=false
		name, digest := prefetchName, prefetchDigestLayer2
		content := hubBlobContent(0xb2, 4096)
		half := 1000
		fx.reg.setContent(digest, content)
		fx.reg.setChunkConfig(4096, 0)

		key := prefetchBlobKey(name, digest)
		fx.s.savePartialPart(fx.disk, key, content[:half])

		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Header().Get("X-Cache"); got != "MISS" {
			t.Errorf("X-Cache = %q, want MISS（开关关闭走既有全量回源，不标 RESUME）", got)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, content) {
			t.Errorf("body 与上游原文不一致（got %d 字节, want %d 字节）", len(got), len(content))
		}
		// 不续传：上游收到的请求不带 Range（无 bytes=1000- 续传请求）
		want := []string{""}
		if got := fx.reg.rangesOf(digest); !rangesEqual(got, want) {
			t.Errorf("上游收到的 Range 头 = %v, want %v（开关关闭不得发续传 Range）", got, want)
		}
		// 全量成功落盘后半截被删（finalize 钩子与开关无关：正式缓存已含完整内容）
		if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("开关关闭全量落盘后半截应删除, stat err = %v", err)
		}
		if _, ok := fx.disk.Get(key); !ok {
			t.Error("全量回源成功后应写缓存")
		}
	})

	t.Run("开关关闭不产生半截", func(t *testing.T) {
		fx := newResumeFixture(t, boolPtr(false))
		name, digest := prefetchName, prefetchDigestLayer1
		content := hubBlobContent(0xb3, 8192)
		half := 4096
		fx.reg.setContent(digest, content)
		fx.reg.setAbort(digest, half) // 下载中断：开关关闭时维持「失败即丢弃」旧行为

		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest))

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（响应头已 commit，截断响应）", w.Code, http.StatusOK)
		}
		if n := partCount(t, fx.disk.Dir()); n != 0 {
			t.Errorf("开关关闭不应产生半截文件, got %d 个", n)
		}
		key := prefetchBlobKey(name, digest)
		if _, ok := fx.disk.Get(key); ok {
			t.Error("开关关闭且下载中断不应写缓存")
		}
	})

	t.Run("manifest不产生半截", func(t *testing.T) {
		fx := newResumeFixture(t, nil)
		manifest := prefetchManifestJSON(prefetchDigestConfig, prefetchDigestLayer1)
		fx.reg.setManifest(manifest, true) // manifest 写一半断连

		manifestTarget := "v2/" + prefetchName + "/manifests/latest"
		mr := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+manifestTarget, nil)
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, mr)

		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want %d（manifest 响应头已 commit，截断响应）", w.Code, http.StatusOK)
		}
		// manifest 会变：半截保留与续传仅对 blob 启用，绝不产生半截文件
		if n := partCount(t, fx.disk.Dir()); n != 0 {
			t.Errorf("manifest 下载中断不应产生半截文件, got %d 个", n)
		}
		key := cache.Key(config.BuildMatchStr(hostDockerRegistry, manifestTarget))
		if _, ok := fx.disk.Get(key); ok {
			t.Error("manifest 下载中断不应写缓存")
		}
	})
}

// //////////////////  B 白盒：辅助函数语义  ////////////////////////////////////////////////////////

// TestParseDockerBlobTarget 表驱动验证 blob 目标判定（半截保留与续传仅对 blob 启用）：
// blob 路径（含多级仓库 / 查询串 / 前导 /）拆出 digest；manifest / referrers / 段数不足 /
// 首段非 v2 / 非法 digest 一律不适用。
func TestParseDockerBlobTarget(t *testing.T) {
	digest := prefetchDigestLayer1
	tests := []struct {
		name       string
		target     string
		wantDigest string
		wantOK     bool
	}{
		{"单级仓库名blob", "v2/app/blobs/" + digest, digest, true},
		{"多级仓库名blob", "v2/library/nvidia/cuda/blobs/" + digest, digest, true},
		{"带查询串剥离", "v2/app/blobs/" + digest + "?ns=example.com", digest, true},
		{"容忍前导斜杠", "/v2/app/blobs/" + digest, digest, true},
		{"manifest不适用", "v2/app/manifests/latest", "", false},
		{"referrers不适用", "v2/app/referrers/" + digest, "", false},
		{"段数不足", "v2/blobs/" + digest, "", false},
		{"首段非v2", "x2/app/blobs/" + digest, "", false},
		{"倒数第二段非blobs", "v2/app/blobs/sub/" + digest, "", false},
		{"digest含空白拒绝", "v2/app/blobs/sha256: aa", "", false},
		{"digest为空拒绝", "v2/app/blobs/", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDigest, ok := parseDockerBlobTarget(tt.target)
			if ok != tt.wantOK || gotDigest != tt.wantDigest {
				t.Errorf("parseDockerBlobTarget(%q) = (%q, %v), want (%q, %v)",
					tt.target, gotDigest, ok, tt.wantDigest, tt.wantOK)
			}
		})
	}
}

// TestPartialProgressNeverRegresses 白盒验证「半截进度只增不减」与写侧门控：
//   - savePartialPart：更短前缀不覆盖、空数据不落盘、更长前缀覆盖重写；
//   - keepPrefetchPartial：已有更长半截时预取临时文件不被改名（保留原文件）；
//   - captureStreamPartial：开关关闭 / 客户端 Range 起点>0 / 非 blob 目标一律不落盘，
//     而「blob + Range 起点 0」等价完整 GET，照常落盘；
//   - appendPartChunk：追加前大小校验——不匹配即报错终止（并发交错防护）。
func TestPartialProgressNeverRegresses(t *testing.T) {
	t.Run("savePartialPart进度不回退", func(t *testing.T) {
		s := newPartialTestServer(t)
		disk, err := cache.New(t.TempDir())
		if err != nil {
			t.Fatalf("cache.New() 意外报错: %v", err)
		}
		key := "k-save"
		s.savePartialPart(disk, key, []byte("0123456789")) // 首次保留 10 字节
		if n, ok := disk.PartSize(key); !ok || n != 10 {
			t.Fatalf("首次保留后半截大小 = (%d, %v), want (10, true)", n, ok)
		}
		s.savePartialPart(disk, key, []byte("01234")) // 更短前缀：不得覆盖
		if n, _ := disk.PartSize(key); n != 10 {
			t.Errorf("更短前缀不得回退进度, 半截大小 = %d, want 10", n)
		}
		s.savePartialPart(disk, key, nil) // 空数据：不落盘
		if n, _ := disk.PartSize(key); n != 10 {
			t.Errorf("空数据不得触碰半截, 半截大小 = %d, want 10", n)
		}
		s.savePartialPart(disk, key, []byte("0123456789ABCDEF")) // 更长前缀：覆盖重写
		if n, _ := disk.PartSize(key); n != 16 {
			t.Errorf("更长前缀应覆盖重写, 半截大小 = %d, want 16", n)
		}
	})

	t.Run("keepPrefetchPartial进度不回退", func(t *testing.T) {
		s := newPartialTestServer(t)
		disk, err := cache.New(t.TempDir())
		if err != nil {
			t.Fatalf("cache.New() 意外报错: %v", err)
		}
		key := "k-keep"
		tmp1, err := os.CreateTemp(disk.Dir(), prefetchTmpPattern)
		if err != nil {
			t.Fatalf("创建临时文件失败: %v", err)
		}
		if _, err := tmp1.WriteString("abcdef"); err != nil {
			t.Fatalf("写临时文件失败: %v", err)
		}
		tmp1.Close()
		s.keepPrefetchPartial(disk, key, tmp1.Name(), 6) // 改名保留 6 字节
		if data := partBytes(t, disk.PartPath(key)); string(data) != "abcdef" {
			t.Errorf("半截内容 = %q, want %q（零拷贝改名）", data, "abcdef")
		}
		if _, err := os.Stat(tmp1.Name()); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("临时文件应已改名为半截, stat err = %v", err)
		}

		tmp2, err := os.CreateTemp(disk.Dir(), prefetchTmpPattern)
		if err != nil {
			t.Fatalf("创建临时文件失败: %v", err)
		}
		if _, err := tmp2.WriteString("xyz"); err != nil {
			t.Fatalf("写临时文件失败: %v", err)
		}
		tmp2.Close()
		s.keepPrefetchPartial(disk, key, tmp2.Name(), 3) // 更短：不得覆盖半截
		if data := partBytes(t, disk.PartPath(key)); string(data) != "abcdef" {
			t.Errorf("更短前缀不得回退进度, 半截内容 = %q, want %q", data, "abcdef")
		}
		if _, err := os.Stat(tmp2.Name()); err != nil {
			t.Errorf("未改名的临时文件应保留原处, stat err = %v", err)
		}
	})

	t.Run("captureStreamPartial门控", func(t *testing.T) {
		digest := prefetchDigestLayer2
		blobTarget := "v2/" + prefetchName + "/blobs/" + digest

		mustAbsent := func(t *testing.T, disk *cache.Cache, key string) {
			t.Helper()
			if _, ok := disk.PartSize(key); ok {
				t.Error("该门控分支不应产生半截文件")
			}
		}

		t.Run("开关关闭不落盘", func(t *testing.T) {
			s := newResumeFixture(t, boolPtr(false)).s
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-off"
			s.captureStreamPartial(&partialSink{key: key, disk: disk},
				prefetchBlobTestRequest(prefetchName, digest), blobTarget, []byte("data"))
			mustAbsent(t, disk, key)
		})

		t.Run("sink为nil不落盘", func(t *testing.T) {
			s := newPartialTestServer(t)
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-nil"
			s.captureStreamPartial(nil, prefetchBlobTestRequest(prefetchName, digest), blobTarget, []byte("data"))
			mustAbsent(t, disk, key)
		})

		// 起点 0 的 blob Range 等价完整 GET（docker/containerd 首次请求的真实形态）：
		// 断在半路的字节就是合法的整 blob 前缀，必须落盘——否则下次重试又从 0 开始。
		// 注：正常链路里 handleGet 已在入口剥离 Range，此分支是不依赖剥离顺序的兜底。
		t.Run("客户端带Range起点0落盘", func(t *testing.T) {
			s := newPartialTestServer(t)
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-range-zero"
			r := prefetchBlobTestRequest(prefetchName, digest)
			r.Header.Set("Range", "bytes=0-")
			s.captureStreamPartial(&partialSink{key: key, disk: disk}, r, blobTarget, []byte("data"))
			if n, ok := disk.PartSize(key); !ok || n != 4 {
				t.Errorf("半截大小 = (%d, %v), want (4, true)（起点 0 等价完整 GET，应保留前缀）", n, ok)
			}
		})

		// 起点 >0 的 Range 语义是「我要某一段」，与「整 blob 前缀」不同：维持旧版不落盘。
		t.Run("客户端带Range起点100不落盘", func(t *testing.T) {
			s := newPartialTestServer(t)
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-range"
			r := prefetchBlobTestRequest(prefetchName, digest)
			r.Header.Set("Range", "bytes=100-")
			s.captureStreamPartial(&partialSink{key: key, disk: disk}, r, blobTarget, []byte("data"))
			mustAbsent(t, disk, key)
		})

		t.Run("非blob目标不落盘", func(t *testing.T) {
			s := newPartialTestServer(t)
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-manifest"
			s.captureStreamPartial(&partialSink{key: key, disk: disk},
				prefetchBlobTestRequest(prefetchName, digest), "v2/"+prefetchName+"/manifests/latest", []byte("data"))
			mustAbsent(t, disk, key)
		})

		t.Run("blob且开关开启落盘", func(t *testing.T) {
			s := newPartialTestServer(t)
			disk, _ := cache.New(t.TempDir())
			key := "k-gate-hit"
			s.captureStreamPartial(&partialSink{key: key, disk: disk},
				prefetchBlobTestRequest(prefetchName, digest), blobTarget, []byte("data"))
			if n, ok := disk.PartSize(key); !ok || n != 4 {
				t.Errorf("半截大小 = (%d, %v), want (4, true)", n, ok)
			}
		})
	})

	t.Run("appendPartChunk错位校验终止", func(t *testing.T) {
		s := newPartialTestServer(t)
		disk, err := cache.New(t.TempDir())
		if err != nil {
			t.Fatalf("cache.New() 意外报错: %v", err)
		}
		key := "k-append"
		if err := os.WriteFile(disk.PartPath(key), []byte("12345"), 0o644); err != nil {
			t.Fatalf("预置半截失败: %v", err)
		}
		f, err := os.OpenFile(disk.PartPath(key), os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("打开半截失败: %v", err)
		}
		defer f.Close()

		if err := s.appendPartChunk(disk, key, f, 5, []byte("hello")); err != nil {
			t.Fatalf("期望大小匹配的追加意外报错: %v", err)
		}
		if n, _ := disk.PartSize(key); n != 10 {
			t.Errorf("追加后半截大小 = %d, want 10", n)
		}
		// 大小不匹配（并发保存方整体重写过）：必须报错终止本流，绝不产生错位拼接
		if err := s.appendPartChunk(disk, key, f, 5, []byte("x")); err == nil {
			t.Error("期望字节数与文件实际大小不符时应报错终止")
		}
		if n, _ := disk.PartSize(key); n != 10 {
			t.Errorf("校验失败的追加不得写入, 半截大小 = %d, want 10", n)
		}
	})
}

// TestBlobResumeStallWatchdogCutsDeadUpstream 覆盖点 12：续传活体流的 read-timeout
// 看门狗（候选级「无新增数据超时」，与客户端流式路径同一机制）能砍掉「响应头合规但
// body 停滞」的死上游——上游发出剩余区间首块后不再吐字节，执行方应在 read-timeout
// 窗口内按既有失败语义收场（半截保留、进度不回退、客户端响应截断待重试），而不是
// 干等到客户端自身超时。现场背景：docker.1ms.run 曾长期停滞，续传无此保护时每次
// 重试都要白等客户端读超时。
func TestBlobResumeStallWatchdogCutsDeadUpstream(t *testing.T) {
	fx := newResumeFixture(t, nil)
	rt := config.Duration(300 * time.Millisecond) // 窗口取短值：测试只验证「会砍、砍得快」
	fx.s.cfg.DomainRules[0].ReadTimeout = &rt

	name, digest := prefetchName, prefetchDigestLayer1
	content := hubBlobContent(0xc1, 4096)
	fx.reg.setContent(digest, content)
	hold := fx.reg.armHold(digest) // 剩余区间首块写出后阻塞在闸门上：模拟上游停滞
	var once sync.Once
	releaseHold := func() { once.Do(func() { close(hold) }) }
	defer releaseHold() // 任何退出路径先放行上游 handler，避免卡死 t.Cleanup 的 Close

	key := prefetchBlobKey(name, digest)
	fx.s.savePartialPart(fx.disk, key, content[:1000])

	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() { defer close(done); fx.s.ServeHTTP(w, prefetchBlobTestRequest(name, digest)) }()

	start := time.Now()
	select {
	case <-done:
		if el := time.Since(start); el > 3*time.Second {
			t.Errorf("执行方 %v 才结束，read-timeout=300ms 应在窗口内砍掉停滞上游", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("执行方 5s 内未结束：read-timeout 看门狗没有砍掉停滞上游")
	}
	if _, err := os.Stat(fx.disk.PartPath(key)); err != nil {
		t.Errorf("失败收场后半截文件应保留: %v", err)
	}
	if lines := logLinesOf(fx.logBuf.String(), "blob 半截续传读上游失败"); len(lines) == 0 {
		t.Errorf("应有读上游失败留痕:\n%s", fx.logBuf.String())
	}
}

// //////////////////  防毒缓存：写缓存侧 blob 摘要强校验  //////////////////////////////////////

// realDigestOf 计算内容的真实 blob 摘要（"sha256:<hex>"）——防毒缓存回归测试用：
// 生产写缓存侧按 digest 强校验内容寻址一致性，测试数据必须使用真实摘要才能入库。
func realDigestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// TestFinalizeRejectsBlobDigestMismatch 白盒：finalizeFetchEntry 对 blob 目标做 sha256
// 强校验——摘要不符拒绝写缓存并留痕（actual_digest），摘要一致正常入库。
// 夹具默认注入的校验 stub 在本测试还原为 nil（走生产默认真校验）。
func TestFinalizeRejectsBlobDigestMismatch(t *testing.T) {
	fx := newResumeFixture(t, nil)
	fx.s.blobDigestVerify = nil // 还原生产默认校验（夹具 stub 为伪造 digest 常量服务）
	content := hubBlobContent(0xf1, 4096)
	digest := realDigestOf(content)
	target := "v2/" + prefetchName + "/blobs/" + digest
	key := prefetchBlobKey(prefetchName, digest)
	r := prefetchBlobTestRequest(prefetchName, digest)
	eff := fx.effFor(r, target)

	mkEntry := func(body []byte) *cache.Entry {
		h := http.Header{}
		h.Set("Content-Type", "application/octet-stream")
		return &cache.Entry{Status: http.StatusOK, Header: h, Body: body, StoredAt: time.Now()}
	}

	// 摘要不符（毒内容）：拒绝入库并留痕
	if cached := fx.s.finalizeFetchEntry(r, key, target, mkEntry(hubBlobContent(0xf2, 4096)), eff, fx.disk, dockerLogSourceClient); cached {
		t.Fatal("摘要不符的内容不应写缓存")
	}
	if _, ok := fx.disk.Get(key); ok {
		t.Error("毒内容不应入库")
	}
	if !strings.Contains(fx.logBuf.String(), "摘要不符") {
		t.Error("应留痕「blob 回源内容摘要不符」日志")
	}

	// 摘要一致：正常入库
	if cached := fx.s.finalizeFetchEntry(r, key, target, mkEntry(content), eff, fx.disk, dockerLogSourceClient); !cached {
		t.Fatal("摘要一致的正常内容应写缓存")
	}
	e, ok := fx.disk.Get(key)
	if !ok || !bytes.Equal(e.Body, content) {
		t.Errorf("缓存内容应与原文一致（ok=%v）", ok)
	}
}

// TestBlobResumeCompletePoisonPartDiscardedAndRefetched 端到端回归（2026-11-07 现场
// 2f1083db 事故形态）：毒半截（长度==总长、内容错误）经「416 证实完整」路径回放——
// finalize 摘要强校验拒绝入库并丢弃毒半截，随后安全降级全量回源：客户端最终拿到
// 正确原文、缓存=正确原文、无半截残留。
func TestBlobResumeCompletePoisonPartDiscardedAndRefetched(t *testing.T) {
	fx := newResumeFixture(t, nil)
	fx.s.blobDigestVerify = nil
	content := hubBlobContent(0xf3, 8192)
	digest := realDigestOf(content)
	poison := hubBlobContent(0xf4, 8192) // 同长度毒内容
	fx.reg.setContent(digest, content)
	key := prefetchBlobKey(prefetchName, digest)
	// 预置毒半截：长度 == 真实总长（416 的 Content-Range 总长与半截一致、可「证实完整」）
	if err := os.WriteFile(fx.disk.PartPath(key), poison, 0o644); err != nil {
		t.Fatalf("预置毒半截失败: %v", err)
	}

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(prefetchName, digest))

	// 客户端最终拿到正确原文（拒绝入库后降级全量回源）
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d", w.Code, http.StatusOK)
	}
	if !bytes.Equal(w.Body.Bytes(), content) {
		t.Errorf("客户端应拿到正确原文（got %d 字节, want %d 字节）", len(w.Body.Bytes()), len(content))
	}
	// 缓存 = 正确原文（毒内容绝不入库）
	e, ok := fx.disk.Get(key)
	if !ok || !bytes.Equal(e.Body, content) {
		t.Error("缓存应为完整正确原文（毒内容不得入库）")
	}
	// 毒半截已丢弃
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Error("毒半截应已丢弃")
	}
	// 留痕：摘要不符拒绝入库
	if !strings.Contains(fx.logBuf.String(), "摘要不符") {
		t.Error("应留痕「blob 回源内容摘要不符」日志")
	}
	// 上游恰 2 路：1 路 Range 探测（416 证实完整）+ 1 路降级全量回源
	if n := fx.reg.rec.calls(digest); n != 2 {
		t.Errorf("上游 blob 回源 %d 次, want 2（Range 探测 + 降级全量）", n)
	}
}

// TestBlobResumeStreamingPoisonTailNotCached 端到端回归（另一半事故形态）：合法半截
// 前缀 + 上游剩余区间回毒内容，拼接长度与总长一致但摘要不符 → 拒绝入库、毒半截丢弃、
// hub 按 failed 收场（绝不广播 done）、本次响应已 commit（客户端将因摘要不符自行重试，
// 重试时无半截走全量回源拿到正确内容并入库）。
func TestBlobResumeStreamingPoisonTailNotCached(t *testing.T) {
	fx := newResumeFixture(t, nil)
	fx.s.blobDigestVerify = nil
	content := hubBlobContent(0xf5, 8192)
	digest := realDigestOf(content)
	prefix := content[:4096]
	poisonTail := hubBlobContent(0xf6, 4096) // 与原文尾段同长度的毒内容
	fx.reg.setContent(digest, content)
	fx.reg.setPoisonTail(digest, poisonTail)
	key := prefetchBlobKey(prefetchName, digest)
	// 预置合法前缀半截
	if err := os.WriteFile(fx.disk.PartPath(key), prefix, 0o644); err != nil {
		t.Fatalf("预置半截前缀失败: %v", err)
	}

	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, prefetchBlobTestRequest(prefetchName, digest))

	// 已 commit 一次完整 200，长度 = 总长（拼出了「完整长度的毒内容」）
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.Len() != len(content) {
		t.Fatalf("body = %d 字节, want %d（长度与总长一致）", w.Body.Len(), len(content))
	}
	if bytes.Equal(w.Body.Bytes(), content) {
		t.Fatal("毒尾巴场景客户端收到的应是拼接毒内容（测试自检）")
	}
	// 拒绝入库；毒半截已丢弃
	if _, ok := fx.disk.Get(key); ok {
		t.Error("摘要不符的拼接内容不应写缓存")
	}
	if _, err := os.Stat(fx.disk.PartPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Error("毒半截应已丢弃")
	}
	if !strings.Contains(fx.logBuf.String(), "未写缓存") {
		t.Error("应留痕「内容未写缓存」日志")
	}

	// 客户端重试（docker 语义）：无半截 → 全量回源拿到正确内容并入库（自愈闭环）
	w2 := httptest.NewRecorder()
	fx.s.ServeHTTP(w2, prefetchBlobTestRequest(prefetchName, digest))
	if !bytes.Equal(w2.Body.Bytes(), content) {
		t.Error("重试后客户端应拿到正确原文")
	}
	if e, ok := fx.disk.Get(key); !ok || !bytes.Equal(e.Body, content) {
		t.Error("重试后缓存应为完整正确原文")
	}
}
