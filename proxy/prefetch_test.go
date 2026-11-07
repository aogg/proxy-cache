package proxy

// prefetch_test.go —— docker 镜像 layer 异步预取（prefetch.go，提交 6f61108）的单测：
// 全部使用 httptest 假 registry 上游（不启动本服务、不发真实外网请求），缓存目录一律
// t.TempDir()，测试只写临时目录，不含任何 e2e。覆盖：
//   - parseDockerManifestTarget：tag/digest 引用、多级仓库名、查询串剥离、前导 / 兜底，
//     与各类非 manifest 目标（blob、段数不足、首段非 v2、倒数第二段非 manifests、空 name/ref）；
//   - isSinglePlatformImageManifest：两类合法基名（大小写混合、;charset 参数后缀）、
//     manifest list / OCI index / 未知类型 / 空串；
//   - validBlobDigest：合法 sha256:hex 放行，含 / ? # 空白、控制字符与空串拒绝；
//   - maybePrefetchImageLayers 触发判定：开关关 / 非 GET / 非 200 / URL 非 manifest /
//     多平台 Content-Type / JSON 解析失败 / digest 全非法均不触发（上游零 blob 回源）；
//     正常触发后 config + layers 各 blob 异步写入缓存（waitPrefetchIdle 等待后
//     disk.Get 命中、上游每个 digest 恰好一次回源）；
//   - prefetchOneBlob 三级去重与失败降级：缓存命中跳过（上游零请求）、预取在途集合
//     同 key 跳过、上游 500 不写缓存且不 panic、回源成功写缓存；
//   - flight 合并：预取与客户端并发请求同一 blob 时上游只回源一路；
//   - Server 级信号量限并发：docker-prefetch-concurrency=1 时多任务 blob 逐个回源
//    （上游并发活跃峰值恰为 1）；
//   - prefetchBlobRequest：路径改写为 blob 路径、查询串清空、方法/Host/请求头保留、
//     原请求对象不被污染。

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// prefetchCTSchema2 单平台 docker schema2 manifest 的 Content-Type（多数用例共用）。
const prefetchCTSchema2 = "application/vnd.docker.distribution.manifest.v2+json"

// prefetchName 测试用多级仓库名（触发判定走 parseDockerManifestTarget 的多级拆分路径）。
const prefetchName = "prefetch-demo/app"

// prefetch 测试用的 blob digest（形状真实的 sha256 hex，内容本身无关紧要）。
var (
	prefetchDigestConfig = "sha256:" + strings.Repeat("aa", 32)
	prefetchDigestLayer1 = "sha256:" + strings.Repeat("bb", 32)
	prefetchDigestLayer2 = "sha256:" + strings.Repeat("cc", 32)
	prefetchDigestOther  = "sha256:" + strings.Repeat("ee", 32)
)

// prefetchUpstreamRec 记录假 registry 收到的 blob 回源（并发安全）：
// 逐 digest 计数、总数与并发活跃峰值（flight 合并与信号量限并发断言用）。
type prefetchUpstreamRec struct {
	mu     sync.Mutex
	blobs  map[string]int // digest -> 回源次数（进入处理即计数，含失败响应）
	active int            // 当前正在处理的 blob 请求数
	peak   int            // 并发活跃峰值
}

// newPrefetchUpstreamRec 构建空记录器。
func newPrefetchUpstreamRec() *prefetchUpstreamRec {
	return &prefetchUpstreamRec{blobs: make(map[string]int)}
}

// enter 记录一次 blob 回源开始（digest 计数 +1、活跃数 +1 并刷新峰值）。
func (rec *prefetchUpstreamRec) enter(digest string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.blobs[digest]++
	rec.active++
	if rec.active > rec.peak {
		rec.peak = rec.active
	}
}

// leave 记录一次 blob 回源结束（活跃数 -1）。
func (rec *prefetchUpstreamRec) leave() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.active--
}

// calls 返回某 digest 的回源次数。
func (rec *prefetchUpstreamRec) calls(digest string) int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.blobs[digest]
}

// total 返回 blob 回源总次数。
func (rec *prefetchUpstreamRec) total() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	n := 0
	for _, c := range rec.blobs {
		n += c
	}
	return n
}

// peakConcurrency 返回 blob 回源的并发活跃峰值。
func (rec *prefetchUpstreamRec) peakConcurrency() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.peak
}

// fakeManifestRegistry 模拟 docker registry 的假上游（实现 http.Handler）：
//   - 路径含 /manifests/：按 status/ct/body 返回 manifest 响应（status 0 视为 200）；
//   - 路径含 /blobs/：按 digest 返回 "blob-<digest>" 文本（blobStatus 0 视为 200，
//     非 200 模拟回源失败；blobDelay 拉长回源窗口供合并/限并发测试观察），
//     每次进入处理即经 rec 计数；
//   - 其他路径：404。
type fakeManifestRegistry struct {
	mu         sync.Mutex
	rec        *prefetchUpstreamRec // blob 回源记录器（manifest 请求不进入）
	status     int                  // manifest 响应状态码
	ct         string               // manifest 响应 Content-Type（空 = 不设置）
	body       []byte               // manifest 响应 body
	blobStatus int                  // blob 响应状态码
	blobDelay  time.Duration        // 每次 blob 回源的延时
	manifests  int                  // 收到的 manifest 请求次数
}

// manifestCalls 返回收到的 manifest 请求次数。
func (f *fakeManifestRegistry) manifestCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.manifests
}

// ServeHTTP 按 manifest / blob 分流（计数与响应字段读取均受 mu 保护，可并发调用）。
func (f *fakeManifestRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.Contains(r.URL.Path, "/manifests/"):
		f.mu.Lock()
		f.manifests++
		st, ct, body := f.status, f.ct, f.body
		f.mu.Unlock()
		if st == 0 {
			st = http.StatusOK
		}
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(st)
		_, _ = w.Write(body)
	case strings.Contains(r.URL.Path, "/blobs/"):
		digest := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		f.rec.enter(digest)
		defer f.rec.leave()
		f.mu.Lock()
		st := f.blobStatus
		f.mu.Unlock()
		if f.blobDelay > 0 {
			time.Sleep(f.blobDelay)
		}
		if st == 0 {
			st = http.StatusOK
		}
		w.WriteHeader(st)
		_, _ = io.WriteString(w, "blob-"+digest)
	default:
		http.NotFound(w, r)
	}
}

// prefetchFixture 聚合一个预取测试环境：httptest 假 registry + 带 docker 规则的 Server
// + 独立缓存实例（t.TempDir 目录，与 Server 走同一目录从而可断言落盘结果）。
type prefetchFixture struct {
	upstream *httptest.Server      // 假 registry
	reg      *fakeManifestRegistry // 假 registry 状态与计数
	s        *Server               // 被测 Server（规则命中 hostDockerRegistry）
	disk     *cache.Cache          // 断言用的缓存实例（与 Server 生效目录相同）
}

// newPrefetchFixture 构建预取测试环境（参考 registry_test.go 的 newRegistryServer）：
// 域名规则 match 命中 hostDockerRegistry，url-redirect 用 full_url_no_server 指向假上游；
// enabled/concurrency 传 nil 时沿用全局默认（开 / 4）。缓存目录固定 t.TempDir()。
func newPrefetchFixture(t *testing.T, enabled *bool, concurrency *int) *prefetchFixture {
	t.Helper()
	reg := &fakeManifestRegistry{rec: newPrefetchUpstreamRec()}
	upstream := httptest.NewServer(reg)
	t.Cleanup(upstream.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	if enabled != nil {
		cfg.DockerPrefetchEnabled = enabled
	}
	if concurrency != nil {
		cfg.DockerPrefetchConcurrency = concurrency
	}
	templates := []config.URLRedirectItem{{Template: upstream.URL + "/$http.server.header.full_url_no_server"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "docker-prefetch",
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
	return &prefetchFixture{upstream: upstream, reg: reg, s: s, disk: disk}
}

// effFor 返回某路径目标在本环境的生效配置（路径目标 fullURL=false，按入站 Host 命中规则）。
func (fx *prefetchFixture) effFor(r *http.Request, target string) config.Options {
	return fx.s.cfg.ResolveOptions(r.Host, target, false)
}

// prefetchBlobKey 返回 blob 的缓存键（与 prefetchOneBlob 同构：http://<Host>/<blob 目标>）。
func prefetchBlobKey(name, digest string) string {
	return cache.Key(config.BuildMatchStr(hostDockerRegistry, "v2/"+name+"/blobs/"+digest))
}

// prefetchBlobTestRequest 构造 blob 路径目标的入站 GET 请求（Host 与既有 registry 测试一致）。
func prefetchBlobTestRequest(name, digest string) *http.Request {
	return httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+"/v2/"+name+"/blobs/"+digest, nil)
}

// prefetchManifestJSON 构造单平台 image manifest 的 body（config + layers 的 digest 列表）。
func prefetchManifestJSON(configDigest string, layerDigests ...string) []byte {
	type blob struct {
		Digest string `json:"digest"`
	}
	m := struct {
		SchemaVersion int    `json:"schemaVersion"`
		MediaType     string `json:"mediaType"`
		Config        blob   `json:"config"`
		Layers        []blob `json:"layers"`
	}{
		SchemaVersion: 2,
		MediaType:     prefetchCTSchema2,
		Config:        blob{Digest: configDigest},
	}
	for _, d := range layerDigests {
		m.Layers = append(m.Layers, blob{Digest: d})
	}
	b, err := json.Marshal(&m)
	if err != nil {
		panic(err) // 固定结构体序列化不可能失败
	}
	return b
}

// prefetchManifestEntry 构造「已写入缓存」形态的 manifest 条目（200 + 指定 Content-Type + body）。
func prefetchManifestEntry(ct string, body []byte) *cache.Entry {
	h := http.Header{}
	if ct != "" {
		h.Set("Content-Type", ct)
	}
	return &cache.Entry{Status: http.StatusOK, Header: h, Body: body, StoredAt: time.Now()}
}

// TestParseDockerManifestTarget 表驱动验证 manifest 目标解析：
// tag/digest 引用、多级仓库名、查询串剥离、前导 / 兜底均正确拆出 name/ref；
// blob 请求、段数不足、首段非 v2、倒数第二段非 manifests、空 name / 空 ref 判为非 manifest。
func TestParseDockerManifestTarget(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantName string
		wantRef  string
		wantOK   bool
	}{
		{"tag引用", "v2/library/nginx/manifests/latest", "library/nginx", "latest", true},
		{"digest引用", "v2/adockero/proxy-cache/manifests/sha256:abc123", "adockero/proxy-cache", "sha256:abc123", true},
		{"多级仓库名", "v2/library/nvidia/cuda/manifests/12.4.1", "library/nvidia/cuda", "12.4.1", true},
		{"单级仓库名", "v2/nginx/manifests/latest", "nginx", "latest", true},
		{"带查询串剥离", "v2/foo/bar/manifests/latest?ns=docker.io", "foo/bar", "latest", true},
		{"前导斜杠兜底", "/v2/foo/manifests/latest", "foo", "latest", true},
		{"blob请求非manifest", "v2/foo/blobs/" + prefetchDigestConfig, "", "", false},
		{"段数不足", "v2/foo/manifests", "", "", false},
		{"首段非v2", "v3/foo/manifests/latest", "", "", false},
		{"倒数第二段非manifests", "v2/foo/layers/latest", "", "", false},
		{"空name", "v2//manifests/latest", "", "", false},
		{"空ref", "v2/foo/manifests/", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ref, ok := parseDockerManifestTarget(tt.target)
			if ok != tt.wantOK {
				t.Fatalf("parseDockerManifestTarget(%q) ok = %v, want %v", tt.target, ok, tt.wantOK)
			}
			if name != tt.wantName || ref != tt.wantRef {
				t.Errorf("parseDockerManifestTarget(%q) = (%q, %q), want (%q, %q)",
					tt.target, name, ref, tt.wantName, tt.wantRef)
			}
		})
	}
}

// TestIsSinglePlatformImageManifest 表驱动验证 Content-Type 判定：
// 两类单平台基名放行（大小写归一化、;charset 参数后缀容忍），
// manifest list / OCI index（多平台）与未知类型、空串拒绝。
func TestIsSinglePlatformImageManifest(t *testing.T) {
	tests := []struct {
		name string
		ct   string
		want bool
	}{
		{"docker-schema2放行", "application/vnd.docker.distribution.manifest.v2+json", true},
		{"oci-image-manifest放行", "application/vnd.oci.image.manifest.v1+json", true},
		{"大小写混合归一化", "Application/VND.Docker.Distribution.Manifest.V2+JSON", true},
		{"charset参数后缀容忍", "application/vnd.docker.distribution.manifest.v2+json; charset=utf-8", true},
		{"docker-manifest-list多平台拒绝", "application/vnd.docker.distribution.manifest.list.v2+json", false},
		{"oci-index多平台拒绝", "application/vnd.oci.image.index.v1+json", false},
		{"未知类型拒绝", "application/json", false},
		{"空串拒绝", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSinglePlatformImageManifest(tt.ct); got != tt.want {
				t.Errorf("isSinglePlatformImageManifest(%q) = %v, want %v", tt.ct, got, tt.want)
			}
		})
	}
}

// TestValidBlobDigest 表驱动验证 digest 的路径安全校验：
// 合法 sha256:hex 放行（宽松校验不锁算法/长度）；含 / ? # 空白、控制字符与空串拒绝。
func TestValidBlobDigest(t *testing.T) {
	tests := []struct {
		name   string
		digest string
		want   bool
	}{
		{"合法sha256放行", prefetchDigestConfig, true},
		{"不锁算法长度宽松放行", "sha256:ABCDEF", true},
		{"含斜杠拒绝", "sha256:abc/../etc", false},
		{"含问号拒绝", "sha256:abc?x=1", false},
		{"含井号拒绝", "sha256:abc#frag", false},
		{"含空格拒绝", "sha256:abc def", false},
		{"含制表符拒绝", "sha256:abc\tdef", false},
		{"含控制字符拒绝", "sha256:abc\x00def", false},
		{"含DEL拒绝", "sha256:abc\x7fdef", false},
		{"空串拒绝", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validBlobDigest(tt.digest); got != tt.want {
				t.Errorf("validBlobDigest(%q) = %v, want %v", tt.digest, got, tt.want)
			}
		})
	}
}

// TestMaybePrefetchImageLayersNotTriggered 表驱动验证触发判定的否决分支：
// 开关关闭 / 非 GET / 上游非 200 / URL 非 manifest 目标 / 多平台 Content-Type /
// manifest JSON 解析失败 / digest 全部非法——均不触发预取（除入站请求自身外上游零 blob 回源）。
func TestMaybePrefetchImageLayersNotTriggered(t *testing.T) {
	manifestBody := prefetchManifestJSON(prefetchDigestConfig, prefetchDigestLayer1, prefetchDigestLayer2)
	tests := []struct {
		name             string
		enabled          *bool  // nil = 默认开
		method           string // 入站方法
		upStatus         int    // manifest 上游状态码（0 = 200）
		ct               string // manifest Content-Type（空 = schema2）
		body             []byte // manifest body（nil = 合法 manifestBody）
		inboundPath      string // 入站路径（空 = manifest 路径）
		wantManifestReqs int    // 期望上游收到的 manifest 请求次数
		wantBlobReqs     int    // 期望上游收到的 blob 请求次数（入站请求自身计入）
	}{
		{"开关关闭不触发", boolPtr(false), http.MethodGet, 0, "", nil, "", 1, 0},
		{"HEAD请求不触发", nil, http.MethodHead, 0, "", nil, "", 1, 0},
		{"上游非200不触发", nil, http.MethodGet, http.StatusNotFound, "", nil, "", 1, 0},
		{"URL非manifest目标不触发", nil, http.MethodGet, 0, "", nil,
			"/v2/" + prefetchName + "/blobs/" + prefetchDigestOther, 0, 1},
		{"manifest-list多平台不触发", nil, http.MethodGet, 0,
			"application/vnd.docker.distribution.manifest.list.v2+json", nil, "", 1, 0},
		{"oci-index多平台不触发", nil, http.MethodGet, 0,
			"application/vnd.oci.image.index.v1+json", nil, "", 1, 0},
		{"manifest解析失败不触发", nil, http.MethodGet, 0, "", []byte("not-json"), "", 1, 0},
		{"digest全非法不触发", nil, http.MethodGet, 0, "",
			prefetchManifestJSON("bad/digest", "sha256:abc def", ""), "", 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newPrefetchFixture(t, tt.enabled, nil)
			fx.reg.status = tt.upStatus
			if tt.ct == "" {
				fx.reg.ct = prefetchCTSchema2
			} else {
				fx.reg.ct = tt.ct
			}
			if tt.body == nil {
				fx.reg.body = manifestBody
			} else {
				fx.reg.body = tt.body
			}
			path := tt.inboundPath
			if path == "" {
				path = "/v2/" + prefetchName + "/manifests/latest"
			}

			r := httptest.NewRequest(tt.method, "http://"+hostDockerRegistry+path, nil)
			w := httptest.NewRecorder()
			fx.s.ServeHTTP(w, r)
			fx.s.waitPrefetchIdle()

			if n := fx.reg.manifestCalls(); n != tt.wantManifestReqs {
				t.Errorf("上游收到 manifest 请求 %d 次, want %d", n, tt.wantManifestReqs)
			}
			if n := fx.reg.rec.total(); n != tt.wantBlobReqs {
				t.Errorf("上游收到 blob 请求 %d 次, want %d（否决分支不应预取）", n, tt.wantBlobReqs)
			}
		})
	}
}

// TestMaybePrefetchImageLayersTriggersAndCaches 验证正常触发主链路：
// GET 单平台 manifest 回源 200 已写缓存后，config + layers 各 blob 被异步预取——
// waitPrefetchIdle 等待后上游每个 digest 恰好回源一次、缓存可直接命中（客户端后续 HIT），
// 且 manifest 自身已写缓存。
func TestMaybePrefetchImageLayersTriggersAndCaches(t *testing.T) {
	fx := newPrefetchFixture(t, nil, nil)
	fx.reg.ct = prefetchCTSchema2
	fx.reg.body = prefetchManifestJSON(prefetchDigestConfig, prefetchDigestLayer1, prefetchDigestLayer2)

	// 入站用 digest manifest（真实 docker 流程：HEAD latest 拿 digest 后按 digest GET；
	// tag latest 属可变目标一律 BYPASS 不写缓存，见 docker_mutable.go，不能作预取触发入口）
	manifestDigest := "sha256:pf" + strings.Repeat("ab", 16)
	manifestTarget := "v2/" + prefetchName + "/manifests/" + manifestDigest
	r := httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+"/"+manifestTarget, nil)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("manifest 请求 Code = %d, want %d（body: %s）", w.Code, http.StatusOK, w.Body.String())
	}
	fx.s.waitPrefetchIdle()

	// manifest 自身已写缓存（预取是写盘成功后的顺带触发，前置条件即已落盘）
	manifestKey := cache.Key(config.BuildMatchStr(hostDockerRegistry, manifestTarget))
	e, ok := fx.disk.Get(manifestKey)
	if !ok {
		t.Fatal("manifest 应已写入缓存，disk.Get 未命中")
	}
	if string(e.Body) != string(fx.reg.body) {
		t.Errorf("缓存 manifest body = %q, want %q", e.Body, fx.reg.body)
	}

	// 每个 blob 恰好回源一次且已写入缓存（客户端后续请求即可 HIT）
	for _, d := range []string{prefetchDigestConfig, prefetchDigestLayer1, prefetchDigestLayer2} {
		if n := fx.reg.rec.calls(d); n != 1 {
			t.Errorf("blob %s 回源 %d 次, want 恰好 1 次", d, n)
		}
		be, ok := fx.disk.Get(prefetchBlobKey(prefetchName, d))
		if !ok {
			t.Errorf("blob %s 应已预取写入缓存，disk.Get 未命中", d)
			continue
		}
		if want := "blob-" + d; string(be.Body) != want {
			t.Errorf("blob %s 缓存 body = %q, want %q", d, be.Body, want)
		}
	}
}

// TestPrefetchOneBlobDedupAndFallback 验证 prefetchOneBlob 的去重与失败降级：
//   - 缓存已命中 → 直接跳过，上游零请求，已有条目不被覆盖；
//   - 预取在途集合已登记同 key → 跳过，上游零请求；
//   - 上游 500 → 实际回源但不写缓存、不 panic（留痕降级，不影响客户端主请求路径）；
//   - 回源成功 → 写入缓存。
func TestPrefetchOneBlobDedupAndFallback(t *testing.T) {
	t.Run("缓存已命中跳过上游零请求", func(t *testing.T) {
		fx := newPrefetchFixture(t, nil, nil)
		digest := prefetchDigestLayer1
		blobKey := prefetchBlobKey(prefetchName, digest)
		cached := &cache.Entry{Status: http.StatusOK, Header: http.Header{}, Body: []byte("already-cached"), StoredAt: time.Now()}
		if err := fx.disk.Set(blobKey, cached); err != nil {
			t.Fatalf("预置缓存失败: %v", err)
		}

		r := prefetchBlobTestRequest(prefetchName, digest)
		fx.s.prefetchOneBlob(context.Background(), r, prefetchName, digest, fx.effFor(r, "v2/"+prefetchName+"/blobs/"+digest), fx.disk)

		if n := fx.reg.rec.calls(digest); n != 0 {
			t.Errorf("缓存命中应跳过预取，上游却收到 %d 次 blob 回源", n)
		}
		if e, ok := fx.disk.Get(blobKey); !ok || string(e.Body) != "already-cached" {
			t.Errorf("已有缓存条目不应被覆盖，Get = (%v, %v)", e, ok)
		}
	})

	t.Run("预取在途集合已登记跳过", func(t *testing.T) {
		fx := newPrefetchFixture(t, nil, nil)
		digest := prefetchDigestLayer2
		blobKey := prefetchBlobKey(prefetchName, digest)
		if !fx.s.prefetchInflightAdd(blobKey) {
			t.Fatal("prefetchInflightAdd 首次登记应成功")
		}
		defer fx.s.prefetchInflightRemove(blobKey)

		r := prefetchBlobTestRequest(prefetchName, digest)
		fx.s.prefetchOneBlob(context.Background(), r, prefetchName, digest, fx.effFor(r, "v2/"+prefetchName+"/blobs/"+digest), fx.disk)

		if n := fx.reg.rec.calls(digest); n != 0 {
			t.Errorf("同 key 已在预取在途应跳过，上游却收到 %d 次 blob 回源", n)
		}
	})

	t.Run("上游500不写缓存不panic", func(t *testing.T) {
		fx := newPrefetchFixture(t, nil, nil)
		fx.reg.blobStatus = http.StatusInternalServerError
		digest := prefetchDigestConfig

		r := prefetchBlobTestRequest(prefetchName, digest)
		fx.s.prefetchOneBlob(context.Background(), r, prefetchName, digest, fx.effFor(r, "v2/"+prefetchName+"/blobs/"+digest), fx.disk)

		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游应被实际回源 1 次, got %d", n)
		}
		if _, ok := fx.disk.Get(prefetchBlobKey(prefetchName, digest)); ok {
			t.Error("回源 500 未过成功判据，不应写缓存")
		}
	})

	t.Run("回源成功写缓存", func(t *testing.T) {
		fx := newPrefetchFixture(t, nil, nil)
		digest := prefetchDigestLayer1

		r := prefetchBlobTestRequest(prefetchName, digest)
		fx.s.prefetchOneBlob(context.Background(), r, prefetchName, digest, fx.effFor(r, "v2/"+prefetchName+"/blobs/"+digest), fx.disk)

		if n := fx.reg.rec.calls(digest); n != 1 {
			t.Errorf("上游应被实际回源 1 次, got %d", n)
		}
		e, ok := fx.disk.Get(prefetchBlobKey(prefetchName, digest))
		if !ok {
			t.Fatal("回源成功应写入缓存，disk.Get 未命中")
		}
		if want := "blob-" + digest; string(e.Body) != want {
			t.Errorf("缓存 body = %q, want %q", e.Body, want)
		}
	})
}

// TestPrefetchMergeWithClientInFlight 验证 flight 合并：预取与客户端并发请求同一 blob 时
// 共享同一次回源（谁先到谁执行、其余等待）——上游对该 blob 只回源一路，
// 且合并完成后缓存命中（由客户端执行方写缓存）。
func TestPrefetchMergeWithClientInFlight(t *testing.T) {
	fx := newPrefetchFixture(t, nil, nil)
	fx.reg.blobDelay = 300 * time.Millisecond // 拉长回源窗口，保证预取发起时客户端仍在途
	digest := prefetchDigestLayer1

	// 客户端先发起 blob GET（ServeHTTP 全链路，流式回源在途）
	clientDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		fx.s.ServeHTTP(w, prefetchBlobTestRequest(prefetchName, digest))
		clientDone <- w
	}()

	// 等客户端请求已打到上游（flight 在途调用此时必然已登记：先登记后回源）
	deadline := time.Now().Add(2 * time.Second)
	for fx.reg.rec.calls(digest) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("等待客户端 blob 请求到达上游超时")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 预取同一 blob：应合并进客户端在途回源（等待方），不发起第二路回源
	r := prefetchBlobTestRequest(prefetchName, digest)
	fx.s.prefetchOneBlob(context.Background(), r, prefetchName, digest, fx.effFor(r, "v2/"+prefetchName+"/blobs/"+digest), fx.disk)

	w := <-clientDone
	if w.Code != http.StatusOK {
		t.Errorf("客户端 Code = %d, want %d", w.Code, http.StatusOK)
	}
	if n := fx.reg.rec.calls(digest); n != 1 {
		t.Errorf("预取与客户端并发请求同一 blob，上游却回源 %d 次, want 1（flight 应合并）", n)
	}
	e, ok := fx.disk.Get(prefetchBlobKey(prefetchName, digest))
	if !ok {
		t.Error("合并回源完成后缓存应命中（由执行方写缓存）")
	} else if want := "blob-" + digest; string(e.Body) != want {
		t.Errorf("缓存 body = %q, want %q", e.Body, want)
	}
}

// TestPrefetchConcurrencySemaphore 验证 Server 级信号量限并发：
// docker-prefetch-concurrency=1 时，两个并发预取任务（不同镜像、digest 互不相关）
// 的 blob 回源逐个进行——上游并发活跃峰值恰为 1，且每个 blob 恰好回源一次并写入缓存。
func TestPrefetchConcurrencySemaphore(t *testing.T) {
	conc := 1
	fx := newPrefetchFixture(t, nil, &conc)
	fx.reg.blobDelay = 80 * time.Millisecond // 拉长单次回源窗口，让并发重叠可被观测

	disk, err := cache.New(t.TempDir())
	if err != nil {
		t.Fatalf("cache.New() 意外报错: %v", err)
	}

	name1, name2 := "prefetch/img-one", "prefetch/img-two"
	d1 := "sha256:" + strings.Repeat("11", 32)
	d2 := "sha256:" + strings.Repeat("22", 32)
	d3 := "sha256:" + strings.Repeat("33", 32)
	d4 := "sha256:" + strings.Repeat("44", 32)

	// 触发两个并发预取任务（maybePrefetchImageLayers 内部各自起 goroutine）
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name1, prefetchManifestJSON(d1, d2)},
		{name2, prefetchManifestJSON(d3, d4)},
	} {
		target := "v2/" + tc.name + "/manifests/latest"
		r := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+target, nil)
		fx.s.maybePrefetchImageLayers(r, target, prefetchManifestEntry(prefetchCTSchema2, tc.body), fx.effFor(r, target), disk)
	}
	fx.s.waitPrefetchIdle()

	if peak := fx.reg.rec.peakConcurrency(); peak != 1 {
		t.Errorf("并发额度=1 时 blob 回源并发活跃峰值 = %d, want 1（信号量应逐个放行）", peak)
	}
	for _, tc := range []struct{ name, digest string }{
		{name1, d1}, {name1, d2}, {name2, d3}, {name2, d4},
	} {
		if n := fx.reg.rec.calls(tc.digest); n != 1 {
			t.Errorf("blob %s 回源 %d 次, want 1", tc.digest, n)
		}
		if _, ok := disk.Get(prefetchBlobKey(tc.name, tc.digest)); !ok {
			t.Errorf("blob %s 应已预取写入缓存，disk.Get 未命中", tc.digest)
		}
	}
}

// TestPrefetchBlobRequestRewrite 验证 prefetchBlobRequest 的克隆改写语义：
// URL 路径改写为 blob 路径、查询串与 fragment 清空、方法/Host/请求头保留，
// 且原请求对象不被污染（Clone 深拷贝互不共享底层数据）。
func TestPrefetchBlobRequestRewrite(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+"/v2/foo/bar/manifests/latest?ns=docker.io", nil)
	r.Header.Set("Accept", prefetchCTSchema2)
	r.Header.Set("User-Agent", "docker/24.0 test")

	br := prefetchBlobRequest(r, "v2/foo/bar/blobs/"+prefetchDigestConfig)

	if got := br.URL.Path; got != "/v2/foo/bar/blobs/"+prefetchDigestConfig {
		t.Errorf("br.URL.Path = %q, want blob 路径 %q", got, "/v2/foo/bar/blobs/"+prefetchDigestConfig)
	}
	if br.URL.RawPath != "" {
		t.Errorf("br.URL.RawPath = %q, want 清空", br.URL.RawPath)
	}
	if br.URL.RawQuery != "" {
		t.Errorf("br.URL.RawQuery = %q, want 清空", br.URL.RawQuery)
	}
	if br.URL.Fragment != "" {
		t.Errorf("br.URL.Fragment = %q, want 清空", br.URL.Fragment)
	}
	if br.Method != http.MethodGet {
		t.Errorf("br.Method = %q, want 保留 %q", br.Method, http.MethodGet)
	}
	if br.Host != hostDockerRegistry {
		t.Errorf("br.Host = %q, want 保留入站 Host %q", br.Host, hostDockerRegistry)
	}
	if got := br.Header.Get("Accept"); got != prefetchCTSchema2 {
		t.Errorf("br.Header Accept = %q, want 保留 %q", got, prefetchCTSchema2)
	}
	if got := br.Header.Get("User-Agent"); got != "docker/24.0 test" {
		t.Errorf("br.Header User-Agent = %q, want 保留原值", got)
	}

	// 原请求不被污染
	if r.URL.Path != "/v2/foo/bar/manifests/latest" || r.URL.RawQuery != "ns=docker.io" {
		t.Errorf("原请求 URL 被污染: path=%q query=%q", r.URL.Path, r.URL.RawQuery)
	}
	if r.Header.Get("Accept") != prefetchCTSchema2 || r.Header.Get("User-Agent") != "docker/24.0 test" {
		t.Error("原请求头被污染")
	}
}
