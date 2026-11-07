package proxy

// docker_mutable_test.go —— docker「可变目标」读写缓存跳过（docker_mutable.go）的单测：
// 全部使用 httptest 假上游（不发真实外网请求），经 Server.ServeHTTP 走完整链路（缓存真实
// 启用，与 partial_resume_test.go 的 newResumeFixture 同款构造）。覆盖点：
//   1. dockerMutableTarget 白盒表驱动：blob / digest manifest 不可变（false），
//      tag manifest / referrers / tags/list 可变（true），完整 URL 与 v2/ ping false；
//   2. 集成：GET manifests/latest 两次都 BYPASS 回源、上游收到 2 次、不落缓存条目
//    （现场动机：latest 被缓存 666h 后 HEAD 回放过期 digest，docker 永远拉不到新推送）；
//   3. 集成：GET manifests/sha256:<digest> 第一次 MISS 第二次 HIT（不可变目标缓存不受影响）；
//   4. 集成：HEAD manifests/latest 不复用缓存条目（两次都打到上游），
//      HEAD manifests/sha256:<digest> 复用 GET 写下的条目（HIT 不打上游）；
//   5. 集成：GET referrers/<digest> / GET tags/list 同样 BYPASS 且不落缓存条目。

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// TestDockerMutableTarget 覆盖点 1：可变性判定的表驱动用例。
func TestDockerMutableTarget(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{"blob不可变", "v2/library/alpine/blobs/" + digest, false},
		{"blob带查询串不可变", "v2/library/alpine/blobs/" + digest + "?ns=docker.io", false},
		{"digest manifest不可变", "v2/library/alpine/manifests/" + digest, false},
		{"tag latest可变", "v2/library/alpine/manifests/latest", true},
		{"tag版本号可变", "v2/library/alpine/manifests/v3.19", true},
		{"tag带查询串可变", "v2/library/alpine/manifests/latest?x=1", true},
		{"多级仓库tag可变", "v2/a/b/c/manifests/latest", true},
		{"referrers可变", "v2/library/alpine/referrers/" + digest, true},
		{"referrers带artifactType可变", "v2/library/alpine/referrers/" + digest + "?artifactType=application/vnd.example", true},
		{"tags list可变", "v2/library/alpine/tags/list", true},
		{"v2 ping不是可变目标", "v2/", false},
		{"单段不足4段", "v2/manifests/latest", false},
		{"完整URL目标恒false", "https://example.com/v2/a/manifests/latest", false},
		{"容忍前导斜杠", "/v2/a/manifests/latest", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dockerMutableTarget(tt.target); got != tt.want {
				t.Fatalf("dockerMutableTarget(%q) = %v, want %v", tt.target, got, tt.want)
			}
		})
	}
}

// countedManifestUpstream 按路径返回不同响应的假上游（带请求计数，供「是否打到上游」断言）。
type countedManifestUpstream struct {
	mu   sync.Mutex
	hits int
}

func (u *countedManifestUpstream) hitCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

func (u *countedManifestUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.hits++
	u.mu.Unlock()
	switch {
	case strings.Contains(r.URL.Path, "/manifests/"):
		w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("cd", 32))
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[]}`)
	case strings.Contains(r.URL.Path, "/referrers/"):
		w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`)
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"name":"adockero/proxy-cache","tags":["latest","v1"]}`)
	default:
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}
}

// mutableFixture 可变目标测试环境：缓存真实启用的服务 + 假上游 + 独立磁盘句柄（断言落盘）。
type mutableFixture struct {
	s        *Server
	upstream *countedManifestUpstream
	disk     *cache.Cache
}

// newMutableFixture 构建缓存启用 + 单候选假上游的测试环境（预取关闭避免 manifest 写缓存
// 触发异步下载干扰计数）。
func newMutableFixture(t *testing.T) *mutableFixture {
	t.Helper()
	upstream := &countedManifestUpstream{}
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)

	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	prefetchOff := false
	cfg.DockerPrefetchEnabled = &prefetchOff
	templates := []config.URLRedirectItem{{Template: srv.URL + "/$http.server.header.full_url_no_server"}}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "docker-mutable",
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
	return &mutableFixture{s: s, upstream: upstream, disk: disk}
}

// get / head 发入站请求到被测服务。
func (fx *mutableFixture) get(target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+target, nil)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, req)
	return w
}

func (fx *mutableFixture) head(target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodHead, "http://"+hostDockerRegistry+"/"+target, nil)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, req)
	return w
}

// cachedKey 返回目标在缓存实例里的 key（与 cacheKeyInput 同构：入站 Host 参与输入）。
func (fx *mutableFixture) cachedKey(target string) string {
	return cache.Key(config.BuildMatchStr(hostDockerRegistry, target))
}

// TestTagManifestNeverCached 覆盖点 2+3：tag manifest 两次 GET 都 BYPASS（上游 2 次、
// 不落缓存条目），digest manifest 第一次 MISS 第二次 HIT（上游仅 1 次，条目落盘）。
func TestTagManifestNeverCached(t *testing.T) {
	fx := newMutableFixture(t)
	digest := "sha256:" + strings.Repeat("ab", 32)
	latestTarget := "v2/adockero/proxy-cache/manifests/latest"
	digestTarget := "v2/adockero/proxy-cache/manifests/" + digest

	// tag manifest：两次 GET 都必须打到上游（不读缓存），且不写缓存
	for i := 1; i <= 2; i++ {
		w := fx.get(latestTarget)
		if w.Code != http.StatusOK {
			t.Fatalf("第 %d 次 GET latest: Code = %d, want 200", i, w.Code)
		}
		if got := w.Header().Get("X-Cache"); got == "HIT" {
			t.Errorf("第 %d 次 GET latest: X-Cache = HIT, tag manifest 绝不允许回放缓存", i)
		}
	}
	if n := fx.upstream.hitCount(); n != 2 {
		t.Errorf("上游收到 %d 次 GET latest 请求, want 2（tag manifest 两次都回源）", n)
	}
	if _, ok := fx.disk.Get(fx.cachedKey(latestTarget)); ok {
		t.Error("tag manifest 不应写缓存条目（latest 会随 push 变化）")
	}

	// digest manifest：第一次 MISS 第二次 HIT（不可变目标缓存语义不变）
	if w := fx.get(digestTarget); w.Header().Get("X-Cache") == "HIT" {
		t.Errorf("首次 GET digest manifest: X-Cache = HIT, 首次必然 MISS")
	}
	if _, ok := fx.disk.Get(fx.cachedKey(digestTarget)); !ok {
		t.Fatal("digest manifest 应回盘缓存条目（不可变目标，作为对照证明缓存写入链路正常）")
	}
	if w := fx.get(digestTarget); w.Header().Get("X-Cache") != "HIT" {
		t.Errorf("第二次 GET digest manifest: X-Cache = %q, want HIT（digest 不可变可缓存）", w.Header().Get("X-Cache"))
	}
	if n := fx.upstream.hitCount(); n != 3 {
		t.Errorf("上游共收到 %d 次请求, want 3（latest×2 + digest×1，第二次 digest 命中缓存）", n)
	}
}

// TestHeadMutableSkipsCache 覆盖点 4：HEAD manifests/latest 不复用缓存（两次都打上游，
// 现场：HEAD 复用旧条目回放过期 Docker-Content-Digest → docker 判 up to date）；
// HEAD manifests/sha256:<digest> 在 GET 写入后复用条目（HIT，不打上游）。
func TestHeadMutableSkipsCache(t *testing.T) {
	fx := newMutableFixture(t)
	digest := "sha256:" + strings.Repeat("ab", 32)
	latestTarget := "v2/adockero/proxy-cache/manifests/latest"
	digestTarget := "v2/adockero/proxy-cache/manifests/" + digest

	// HEAD latest 两次：都不许 HIT（必须打到上游拿新鲜 digest）
	for i := 1; i <= 2; i++ {
		if w := fx.head(latestTarget); w.Header().Get("X-Cache") == "HIT" {
			t.Errorf("第 %d 次 HEAD latest: X-Cache = HIT, 可变目标不允许回放缓存", i)
		}
	}
	if n := fx.upstream.hitCount(); n != 2 {
		t.Errorf("上游收到 %d 次 HEAD latest 请求, want 2（HEAD latest 两次都回源）", n)
	}

	// GET digest manifest 写缓存后，HEAD digest manifest 复用条目（HIT，上游不新增请求）
	fx.get(digestTarget)
	before := fx.upstream.hitCount()
	if w := fx.head(digestTarget); w.Header().Get("X-Cache") != "HIT" {
		t.Errorf("HEAD digest manifest: X-Cache = %q, want HIT（不可变目标 HEAD 复用 GET 条目）", w.Header().Get("X-Cache"))
	}
	if n := fx.upstream.hitCount(); n != before {
		t.Errorf("HEAD digest manifest 后上游请求数 %d → %d, want 不变（缓存命中不打上游）", before, n)
	}
}

// TestReferrersAndTagsListNeverCached 覆盖点 5：referrers 与 tags/list 同样 BYPASS 且不落缓存。
func TestReferrersAndTagsListNeverCached(t *testing.T) {
	fx := newMutableFixture(t)
	digest := "sha256:" + strings.Repeat("ab", 32)
	targets := []string{
		"v2/adockero/proxy-cache/referrers/" + digest,
		"v2/adockero/proxy-cache/tags/list",
	}

	for _, target := range targets {
		for i := 1; i <= 2; i++ {
			w := fx.get(target)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s 第 %d 次: Code = %d, want 200", target, i, w.Code)
			}
			if got := w.Header().Get("X-Cache"); got == "HIT" {
				t.Errorf("GET %s 第 %d 次: X-Cache = HIT, 可变目标不允许回放缓存", target, i)
			}
		}
		if _, ok := fx.disk.Get(fx.cachedKey(target)); ok {
			t.Errorf("%s 不应写缓存条目（可变目标）", target)
		}
	}
	// referrers ×2 + tags/list ×2 全部回源，恰好 4 次
	if n := fx.upstream.hitCount(); n != 4 {
		t.Errorf("上游收到 %d 次请求, want 4（referrers 与 tags/list 各两次都回源）", n)
	}
}
