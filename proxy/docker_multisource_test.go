package proxy

// docker_multisource_test.go —— docker 可变目标多源并发回源与合并（docker_multisource.go）
// 的单测：全部使用 httptest 假上游（不发真实外网请求），经 Server.ServeHTTP 走完整链路
//（缓存真实启用，与 docker_mutable_test.go / partial_resume_test.go 同款构造），三个
// 候选源各为一个独立假上游（候选顺序 A→B→C 与模板顺序一致）。日志经 captureLogger
//（无 ReplaceAttr 的原始英文键）落 buf，供汇总 Info 键值断言（success=3 / merged=5 等）。
// 覆盖点：
//   1. 三源 tags 集合不同 → 并集去重、字典序排序返回（慢源漏 tag 不再可能）；
//      慢源（响应超过候选级探测预算）不拖过每源超时——预算到期即放弃该源，合并不等它；
//   2. tags/list 带分页参数（?n= / ?last=）→ 不做多源合并，退回既有顺序管线
//    （首发候选原文透传、乱序不合并，其余候选零请求）；
//   3. 混入 HTML 反爬毒源 → 现有成功判据拒判（html-error-page 同款），合并只用合法源；
//   4. manifests 多源摘要 2:1 → 多数共识胜出、响应头（Docker-Content-Digest /
//      Content-Length）正确，GET 与 HEAD 同策略；
//   5. referrers descriptors 按 mediaType+digest 复合键去重合并（同 digest 不同
//      mediaType 保留、描述符未知字段保留）；
//   6. 全部源失败 → 退回既有顺序管线（referrers 404 透传语义不变，每源多源 1 次 +
//      顺序回退探测 1 次）；
//   7. 开关关闭（docker-multi-source-enabled: false）→ 行为与旧版顺序管线完全一致
//    （首发候选原文、其余候选零请求）；
//   8. 客户端断开 → r.Context() 取消全部在途上游（断开即停，慢源在途请求被取消）。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// multiSourceTagsBody tags/list 假上游响应体（与真实 registry 同构的最小形态）。
type multiSourceTagsBody struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// multiSourceReferrersBody referrers 假上游响应体（OCI image index 最小形态）。
type multiSourceReferrersBody struct {
	SchemaVersion int              `json:"schemaVersion"`
	MediaType     string           `json:"mediaType"`
	Manifests     []map[string]any `json:"manifests"`
}

// multiSourceFake 单个候选源的假上游（A/B/C 各一个实例，行为按字段开关）：
//   - tags：tags/list 响应的 tags 数组；
//   - refs：referrers 响应的 descriptors（原始对象，保留未知字段断言）；
//   - manifestDigest / manifestBody：manifests/<tag> 响应头与 body；
//   - notFound：所有端点一律回 404（JSON 错误体，模拟镜像站对 referrers 端点 404）；
//   - poison：所有端点一律回 200 + text/html 反爬页（现有判据应拒判）；
//   - delay：响应前延迟（模拟慢源；延迟中被请求 ctx 取消时记 cancelled=true）。
type multiSourceFake struct {
	mu             sync.Mutex
	hits           int
	cancelled      bool
	tags           []string
	refs           []map[string]any
	manifestDigest string
	manifestBody   string
	notFound       bool
	poison         bool
	delay          time.Duration
}

// hitCount 返回收到的请求次数。
func (f *multiSourceFake) hitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

// wasCancelled 返回在途请求是否被客户端断开取消。
func (f *multiSourceFake) wasCancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// ServeHTTP 按字段开关应答（tags/list、referrers、manifests 三类端点；延迟可被取消）。
func (f *multiSourceFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.hits++
	f.mu.Unlock()
	// 延迟模拟慢源：延迟窗口内请求被取消（客户端断开联动取消在途上游）即记 cancelled
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-r.Context().Done():
			f.mu.Lock()
			f.cancelled = true
			f.mu.Unlock()
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case f.notFound:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[{"code":"NAME_UNKNOWN","message":"repository name not known to registry"}]}`))
	case f.poison:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<!DOCTYPE html><html lang=\"zh-CN\"><body>加速页</body></html>"))
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		body, _ := json.Marshal(multiSourceTagsBody{Name: "adockero/proxy-cache", Tags: f.tags})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case strings.Contains(r.URL.Path, "/referrers/"):
		body, _ := json.Marshal(multiSourceReferrersBody{
			SchemaVersion: 2,
			MediaType:     "application/vnd.oci.image.index.v1+json",
			Manifests:     f.refs,
		})
		w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case strings.Contains(r.URL.Path, "/manifests/"):
		w.Header().Set("Docker-Content-Digest", f.manifestDigest)
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(f.manifestBody))
	default:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// multiSourceFixture 多源并发回源测试环境：缓存真实启用 + 三个候选假上游（A→B→C 顺序
// 与模板顺序一致）+ captureLogger 日志缓冲（原始英文键，供汇总 Info 键值断言）。
type multiSourceFixture struct {
	s      *Server
	fakes  []*multiSourceFake
	logBuf bytes.Buffer
}

// newMultiSourceFixture 构建三候选测试环境（预取关闭避免 manifest 触发异步下载干扰
// 计数）；enabled 为 nil = 用默认配置（开关默认开）；tweak 非 nil 时在 Validate 前
// 微调配置（如给单个候选设 probe-timeout）。
func newMultiSourceFixture(t *testing.T, enabled *bool, tweak func(*config.Config)) *multiSourceFixture {
	t.Helper()
	fx := &multiSourceFixture{}
	var servers []*httptest.Server
	for i := 0; i < 3; i++ {
		fake := &multiSourceFake{}
		srv := httptest.NewServer(fake)
		t.Cleanup(srv.Close)
		fx.fakes = append(fx.fakes, fake)
		servers = append(servers, srv)
	}
	cacheDir := t.TempDir()
	cfg := config.Default()
	cfg.Cache.Path = cacheDir
	prefetchOff := false
	cfg.DockerPrefetchEnabled = &prefetchOff
	if enabled != nil {
		cfg.DockerMultiSourceEnabled = enabled
	}
	templates := make([]config.URLRedirectItem, 0, len(servers))
	for _, srv := range servers {
		templates = append(templates, config.URLRedirectItem{Template: srv.URL + "/$http.server.header.full_url_no_server"})
	}
	cfg.DomainRules = []config.DomainRule{{
		Name:        "docker-multi-source",
		Match:       `http://registry-proxy-cache\.linkease\.net:5480/`,
		URLRedirect: &templates,
	}}
	if tweak != nil {
		tweak(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, cache.NewManager(context.Background()), captureLogger(&fx.logBuf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	fx.s = s
	return fx
}

// get / head 发入站请求到被测服务（Host 与既有 registry 测试一致）。
func (fx *multiSourceFixture) get(target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://"+hostDockerRegistry+"/"+target, nil)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, req)
	return w
}

func (fx *multiSourceFixture) head(target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodHead, "http://"+hostDockerRegistry+"/"+target, nil)
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, req)
	return w
}

// multiSourceDigest 构造可读的伪 digest（前缀重复 hex 对，仅供测试辨识，不参与内容校验）。
func multiSourceDigest(pair string) string {
	return "sha256:" + strings.Repeat(pair, 32)
}

// TestMultiSourceTagsMerge 覆盖点 1：三源 tags 并集去重排序；慢源不拖过每源超时。
func TestMultiSourceTagsMerge(t *testing.T) {
	t.Run("三源并集去重排序", func(t *testing.T) {
		fx := newMultiSourceFixture(t, nil, nil)
		fx.fakes[0].tags = []string{"v1", "latest"}          // A
		fx.fakes[1].tags = []string{"v2", "latest", "v3"}    // B（含与 A 重复的 latest）
		fx.fakes[2].tags = []string{"v0", "v1"}              // C（v1 与 A 重复）

		w := fx.get("v2/adockero/proxy-cache/tags/list")
		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
		}
		if w.Header().Get("X-Cache") != "BYPASS" {
			t.Errorf("X-Cache = %q, want BYPASS（可变目标不缓存）", w.Header().Get("X-Cache"))
		}
		var got multiSourceTagsBody
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("响应 body 非 tags/list JSON: %v（body: %s）", err, w.Body.String())
		}
		wantTags := []string{"latest", "v0", "v1", "v2", "v3"} // 并集去重 + 字典序
		if !reflect.DeepEqual(got.Tags, wantTags) {
			t.Errorf("合并 tags = %v, want 并集去重排序 %v（慢源漏 tag 不再可能）", got.Tags, wantTags)
		}
		if got.Name != "adockero/proxy-cache" {
			t.Errorf("name = %q, want 保留最快成功源原文 %q", got.Name, "adockero/proxy-cache")
		}
		for i, fake := range fx.fakes {
			if n := fake.hitCount(); n != 1 {
				t.Errorf("源 %d 收到 %d 次请求, want 1（并发各打一次）", i, n)
			}
		}
		// 汇总 Info 日志（captureLogger 为原始英文键）：候选数 / 成功源数 / 合并条数
		logs := fx.logBuf.String()
		for _, want := range []string{"tags/list 多源合并返回", "sources=3", "success=3", "merged=5"} {
			if !strings.Contains(logs, want) {
				t.Errorf("汇总日志缺少 %q，实际:\n%s", want, logs)
			}
		}
	})

	t.Run("慢源不拖过每源超时", func(t *testing.T) {
		// C 候选探测预算 300ms、响应延迟 2s：多源合并必须在 ~300ms 处放弃 C，
		// 绝不等慢源把整次合并拖到 2s（每源超时复用现有候选级配置）
		pt := config.Duration(300 * time.Millisecond)
		fx := newMultiSourceFixture(t, nil, func(cfg *config.Config) {
			(*cfg.DomainRules[0].URLRedirect)[2].ProbeTimeout = &pt
		})
		fx.fakes[0].tags = []string{"latest", "v1"}
		fx.fakes[1].tags = []string{"latest", "v2"}
		fx.fakes[2].tags = []string{"v9"}
		fx.fakes[2].delay = 2 * time.Second

		begin := time.Now()
		w := fx.get("v2/adockero/proxy-cache/tags/list")
		elapsed := time.Since(begin)
		if elapsed > 1500*time.Millisecond {
			t.Fatalf("合并耗时 %v，慢源拖住了响应（应在每源探测预算 300ms 处放弃）", elapsed)
		}
		var got multiSourceTagsBody
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("响应 body 非 tags/list JSON: %v（body: %s）", err, w.Body.String())
		}
		wantTags := []string{"latest", "v1", "v2"} // 慢源未过探测预算：v9 不进合并
		if !reflect.DeepEqual(got.Tags, wantTags) {
			t.Errorf("合并 tags = %v, want %v（未过每源超时的源拒之于合并之外）", got.Tags, wantTags)
		}
		if n := fx.fakes[2].hitCount(); n != 1 {
			t.Errorf("慢源收到 %d 次请求, want 1（被探测、超时放弃）", n)
		}
	})
}

// TestMultiSourcePagedTagsSkipsMerge 覆盖点 2：tags/list 带分页参数不做多源合并
//（跨源页码语义不一致），退回既有顺序管线——首发候选原文透传（乱序保留），其余候选零请求。
func TestMultiSourcePagedTagsSkipsMerge(t *testing.T) {
	fx := newMultiSourceFixture(t, nil, nil)
	fx.fakes[0].tags = []string{"v2", "latest"} // 故意乱序：合并路径会排序，顺序管线保原文
	fx.fakes[1].tags = []string{"v0"}
	fx.fakes[2].tags = []string{"v9"}

	w := fx.get("v2/adockero/proxy-cache/tags/list?n=10&last=v1")
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
	}
	wantBody, _ := json.Marshal(multiSourceTagsBody{Name: "adockero/proxy-cache", Tags: []string{"v2", "latest"}})
	if w.Body.String() != string(wantBody) {
		t.Errorf("分页 body = %q, want 顺序管线首发候选原文 %q（不做并集排序合并）", w.Body.String(), string(wantBody))
	}
	if n := fx.fakes[0].hitCount(); n != 1 {
		t.Errorf("首发候选收到 %d 次请求, want 1（顺序管线首个通过判据即胜出）", n)
	}
	if n := fx.fakes[1].hitCount() + fx.fakes[2].hitCount(); n != 0 {
		t.Errorf("其余候选共收到 %d 次请求, want 0（分页不启用多源并发）", n)
	}
}

// TestMultiSourcePoisonSourceRejected 覆盖点 3：HTML 反爬毒源被现有成功判据拒判
//（html-error-page 同款），合并只使用合法源。
func TestMultiSourcePoisonSourceRejected(t *testing.T) {
	fx := newMultiSourceFixture(t, nil, nil)
	fx.fakes[0].tags = []string{"v1"}
	fx.fakes[1].tags = []string{"v2"}
	fx.fakes[2].poison = true // 毒源：200 + text/html

	w := fx.get("v2/adockero/proxy-cache/tags/list")
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Errorf("Content-Type = %q, 毒源的 HTML 反爬页绝不能透传给客户端", w.Header().Get("Content-Type"))
	}
	var got multiSourceTagsBody
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应 body 非 tags/list JSON: %v（body: %s）", err, w.Body.String())
	}
	wantTags := []string{"v1", "v2"} // 仅两个合法源参与合并
	if !reflect.DeepEqual(got.Tags, wantTags) {
		t.Errorf("合并 tags = %v, want %v（HTML 毒源拒之于合并之外）", got.Tags, wantTags)
	}
	if n := fx.fakes[2].hitCount(); n != 1 {
		t.Errorf("毒源收到 %d 次请求, want 1（被并发探测、判据拒绝）", n)
	}
	logs := fx.logBuf.String()
	if !strings.Contains(logs, "success=2") {
		t.Errorf("汇总日志缺少 success=2（毒源不计入成功源），实际:\n%s", logs)
	}
}

// TestMultiSourceManifestConsensusMajority 覆盖点 4：manifests 多源摘要 2:1 → 多数共识
// 胜出，响应头正确；HEAD 与 GET 同策略（pull 流程内判定一致）。
func TestMultiSourceManifestConsensusMajority(t *testing.T) {
	d1 := multiSourceDigest("11")
	d2 := multiSourceDigest("22")
	// 多数派（A/B）同摘要同 body；分歧源（C）不同摘要不同 body
	bodyMajority := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{},"layers":[]}`
	bodyDissent := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"diverged":true},"layers":[]}`

	build := func(t *testing.T) *multiSourceFixture {
		fx := newMultiSourceFixture(t, nil, nil)
		fx.fakes[0].manifestDigest, fx.fakes[0].manifestBody = d1, bodyMajority
		fx.fakes[1].manifestDigest, fx.fakes[1].manifestBody = d1, bodyMajority
		fx.fakes[2].manifestDigest, fx.fakes[2].manifestBody = d2, bodyDissent
		return fx
	}

	t.Run("GET多数共识胜出", func(t *testing.T) {
		fx := build(t)
		w := fx.get("v2/adockero/proxy-cache/manifests/latest")
		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
		}
		if got := w.Header().Get("Docker-Content-Digest"); got != d1 {
			t.Errorf("Docker-Content-Digest = %q, want 多数共识摘要 %q（2:1 多数胜出）", got, d1)
		}
		if w.Body.String() != bodyMajority {
			t.Errorf("body = %q, want 共识源原文 %q（body+响应头同源一致）", w.Body.String(), bodyMajority)
		}
		if w.Header().Get("X-Cache") != "BYPASS" {
			t.Errorf("X-Cache = %q, want BYPASS（可变目标不缓存）", w.Header().Get("X-Cache"))
		}
		logs := fx.logBuf.String()
		for _, want := range []string{"manifest 摘要共识返回", "sources=3", "success=3", "agree=2", "disagree=1", "digest=" + d1} {
			if !strings.Contains(logs, want) {
				t.Errorf("汇总日志缺少 %q，实际:\n%s", want, logs)
			}
		}
	})

	t.Run("HEAD同策略", func(t *testing.T) {
		fx := build(t)
		w := fx.head("v2/adockero/proxy-cache/manifests/latest")
		if w.Code != http.StatusOK {
			t.Fatalf("Code = %d, want 200", w.Code)
		}
		if got := w.Header().Get("Docker-Content-Digest"); got != d1 {
			t.Errorf("HEAD Docker-Content-Digest = %q, want 多数共识摘要 %q（HEAD 与 GET 同策略）", got, d1)
		}
		if got := w.Header().Get("Content-Length"); got != fmt.Sprint(len(bodyMajority)) {
			t.Errorf("HEAD Content-Length = %q, want %d（共识源声明长度）", got, len(bodyMajority))
		}
		if w.Body.Len() != 0 {
			t.Errorf("HEAD 响应不应有 body, 实际 %d 字节", w.Body.Len())
		}
	})
}

// TestMultiSourceReferrersMergeDedup 覆盖点 5：referrers descriptors 按 mediaType+digest
// 复合键去重合并（同 digest 不同 mediaType 保留、重复对只留一份、描述符未知字段保留）。
func TestMultiSourceReferrersMergeDedup(t *testing.T) {
	fx := newMultiSourceFixture(t, nil, nil)
	d1, d2, d3 := multiSourceDigest("aa"), multiSourceDigest("bb"), multiSourceDigest("cc")
	mtManifest := "application/vnd.oci.image.manifest.v1+json"
	mtIndex := "application/vnd.oci.image.index.v1+json"
	desc := func(mt, dg string, annotations map[string]string) map[string]any {
		m := map[string]any{"mediaType": mt, "digest": dg, "size": 1024}
		if annotations != nil {
			m["annotations"] = annotations // 描述符未知字段：合并后必须原样保留
		}
		return m
	}
	fx.fakes[0].refs = []map[string]any{desc(mtManifest, d1, nil), desc(mtIndex, d2, nil)}
	fx.fakes[1].refs = []map[string]any{desc(mtIndex, d2, nil), desc(mtManifest, d3, nil)}
	// 同 digest(d2) 不同 mediaType：复合键不去重，必须保留；带 annotations 的描述符验证字段保真
	fx.fakes[2].refs = []map[string]any{desc(mtManifest, d2, map[string]string{"org.opencontainers.artifact.created": "2026-11-07T00:00:00Z"})}

	w := fx.get("v2/adockero/proxy-cache/referrers/" + d1)
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
	}
	var got multiSourceReferrersBody
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应 body 非 referrers JSON: %v（body: %s）", err, w.Body.String())
	}
	if got.SchemaVersion != 2 || got.MediaType != "application/vnd.oci.image.index.v1+json" {
		t.Errorf("顶层字段丢失：schemaVersion=%d mediaType=%q, want 保留最快成功源原文(2, oci.image.index)",
			got.SchemaVersion, got.MediaType)
	}
	if len(got.Manifests) != 4 {
		t.Fatalf("合并 descriptors = %d 条, want 4（重复对去重、同 digest 异 mediaType 保留）, body: %s",
			len(got.Manifests), w.Body.String())
	}
	// 用「mediaType|digest」复合键排序后比对（合并保留首次出现顺序、完成顺序并发下不固定，
	// 故按集合语义断言）
	type composite struct {
		mt, dg string
	}
	var keys []string
	keyOf := func(mt, dg string) string { return mt + "|" + dg }
	annotationsSeen := false
	for _, m := range got.Manifests {
		mt, _ := m["mediaType"].(string)
		dg, _ := m["digest"].(string)
		keys = append(keys, keyOf(mt, dg))
		if _, ok := m["annotations"].(map[string]any); ok {
			annotationsSeen = true
		}
	}
	sort.Strings(keys)
	wantKeys := []string{
		keyOf(mtIndex, d2),
		keyOf(mtManifest, d1),
		keyOf(mtManifest, d2), // 同 digest 异 mediaType：复合键保留
		keyOf(mtManifest, d3),
	}
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("合并 descriptors 复合键 = %v, want %v", keys, wantKeys)
	}
	if !annotationsSeen {
		t.Error("描述符 annotations 字段合并后丢失（未知字段必须原样保留）")
	}
	logs := fx.logBuf.String()
	if !strings.Contains(logs, "merged=4") || !strings.Contains(logs, "referrers 多源合并返回") {
		t.Errorf("汇总日志缺少 referrers 合并条数留痕，实际:\n%s", logs)
	}
}

// TestMultiSourceAllFailFallsBackToSequential 覆盖点 6：全部源失败 → 退回既有顺序管线，
// 继承其 4xx 透传语义（referrers 404 透传，docker 自带容错），每源共 2 次请求
//（多源并发 1 次 + 顺序管线回退探测 1 次）。
func TestMultiSourceAllFailFallsBackToSequential(t *testing.T) {
	fx := newMultiSourceFixture(t, nil, nil)
	for _, fake := range fx.fakes {
		fake.notFound = true // 三个源全部 404
	}
	w := fx.get("v2/adockero/proxy-cache/referrers/" + multiSourceDigest("ab"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("Code = %d, want 透传上游 404（顺序管线 4xx 透传语义不变）, body: %s",
			w.Code, w.Body.String())
	}
	wantBody := `{"errors":[{"code":"NAME_UNKNOWN","message":"repository name not known to registry"}]}`
	if w.Body.String() != wantBody {
		t.Errorf("body = %q, want 原样透传上游 404 body %q（而非 JSON 502 错误体）", w.Body.String(), wantBody)
	}
	for i, fake := range fx.fakes {
		if n := fake.hitCount(); n != 2 {
			t.Errorf("源 %d 收到 %d 次请求, want 2（多源并发 1 次 + 顺序管线回退探测 1 次）", i, n)
		}
	}
}

// TestMultiSourceDisabledSequentialBehavior 覆盖点 7：开关关闭 → 行为与旧版顺序管线
// 完全一致（首发候选原文透传、不做合并、其余候选零请求）。
func TestMultiSourceDisabledSequentialBehavior(t *testing.T) {
	off := false
	fx := newMultiSourceFixture(t, &off, nil)
	fx.fakes[0].tags = []string{"v2", "latest"} // 乱序：合并路径会排序，关闭开关必须保原文
	fx.fakes[1].tags = []string{"v0"}
	fx.fakes[2].tags = []string{"v9"}

	w := fx.get("v2/adockero/proxy-cache/tags/list")
	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", w.Code, w.Body.String())
	}
	wantBody, _ := json.Marshal(multiSourceTagsBody{Name: "adockero/proxy-cache", Tags: []string{"v2", "latest"}})
	if w.Body.String() != string(wantBody) {
		t.Errorf("开关关闭 body = %q, want 顺序管线首发候选原文 %q（行为与旧版一致）", w.Body.String(), string(wantBody))
	}
	if n := fx.fakes[0].hitCount(); n != 1 {
		t.Errorf("首发候选收到 %d 次请求, want 1", n)
	}
	if n := fx.fakes[1].hitCount() + fx.fakes[2].hitCount(); n != 0 {
		t.Errorf("其余候选共收到 %d 次请求, want 0（开关关闭不并发打全部候选）", n)
	}
}

// TestMultiSourceClientDisconnectCancelsUpstream 覆盖点 8：客户端断开 → r.Context()
// 取消全部在途上游（断开即停），慢源在途请求被取消、整次回源不再等它。
func TestMultiSourceClientDisconnectCancelsUpstream(t *testing.T) {
	// 慢源候选显式 probe-timeout 0（不限制）：证明取消来自客户端断开联动，
	// 而非探测预算到期
	zero := config.Duration(0)
	fx := newMultiSourceFixture(t, nil, func(cfg *config.Config) {
		(*cfg.DomainRules[0].URLRedirect)[2].ProbeTimeout = &zero
	})
	fx.fakes[0].tags = []string{"v1"}
	fx.fakes[1].tags = []string{"v2"}
	fx.fakes[2].tags = []string{"v9"}
	fx.fakes[2].delay = 10 * time.Second // 远超下方 200ms 断开时机

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet,
		"http://"+hostDockerRegistry+"/v2/adockero/proxy-cache/tags/list", nil).WithContext(ctx)
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel() // 模拟 docker 客户端断开
	}()

	begin := time.Now()
	w := httptest.NewRecorder()
	fx.s.ServeHTTP(w, r)
	elapsed := time.Since(begin)

	if !fx.fakes[2].wasCancelled() {
		t.Error("慢源的在途上游请求未被客户端断开取消（断开即停失效）")
	}
	if elapsed > 3*time.Second {
		t.Errorf("断开后整次回源耗时 %v，不应继续等慢源（10s 延迟被取消打断）", elapsed)
	}
	if n := fx.fakes[0].hitCount(); n != 1 {
		t.Errorf("快速源收到 %d 次请求, want 1", n)
	}
}
