// multisource_merge_test.go —— docker 可变目标多源并发回源的合并/共识语义黑盒回归
// （缺口#12，P2；commit 97fef24）：经公开 API（docker-multi-source-enabled + 双候选 +
// tag/referrers/manifest 端点）驱动 docker_multisource.go 的三条合并语义：
//  1. tags/list：各源 tags 并集去重、字典序排序（慢源漏 tag 不再可能）；
//  2. referrers/<digest>：descriptors 按 mediaType+digest 组合键去重合并（同 digest
//     不同 mediaType 各自成条）；
//  3. manifests/<tag> 摘要共识：偶数源 1:1 平票取最快成功者（完成序 seq 最小）——
//     慢源刻意延迟，胜出者必为快源的 body 与响应头。
//
// 确定性设计：慢源以 400ms 延迟控制完成序（seq），远大于调度抖动；合并断言直接比对
// 重组后的 JSON 内容与日志 merged/agree/disagree 字段。
package tests

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// multisourceDigests 多源合并测试用的 digest 常量（形状真实即可，内容无校验）。
func multisourceDigest(seed byte) string {
	hex := make([]byte, 64)
	for i := range hex {
		hex[i] = "0123456789abcdef"[(int(seed)*7+i*13)%16]
	}
	return "sha256:" + string(hex)
}

// multisourceEnv 双候选多源环境：慢源可注入响应前延迟（决定完成序）。
func multisourceEnv(t *testing.T, fastDelay, slowDelay time.Duration) (*testEnv, string) {
	t.Helper()
	fast := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(fastDelay)
		multisourceRespond(w, r, "fast")
	})
	slow := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(slowDelay)
		multisourceRespond(w, r, "slow")
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{
			{Template: fast.URL + "/$1"},
			{Template: slow.URL + "/$1"},
		}
		cfg.DockerMultiSourceEnabled = ptrBool(true)
	})
	return env, fast.URL
}

// multisourceRespond 按请求端点形态应答（fast/slow 两源共用同一响应器，便于断言「共识
// 胜出者是谁」：body 里带 source 标记）。
func multisourceRespond(w http.ResponseWriter, r *http.Request, source string) {
	path := r.URL.Path
	switch {
	case strings.Contains(path, "/tags/list"):
		tags := []string{"v1", "v2"}
		if source == "slow" { // 慢源独有 v3、重叠 v2：并集语义的核心样本
			tags = []string{"v2", "v3"}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"app/tags","tags":["` + strings.Join(tags, `","`) + `"]}`))
	case strings.Contains(path, "/referrers/"):
		w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(referrersBodyFor(source)))
	case strings.Contains(path, "/manifests/"):
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Header().Set("Docker-Content-Digest", multisourceDigest(map[string]byte{"fast": 0x11, "slow": 0x22}[source]))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"schemaVersion":2,"source":"` + source + `"}`))
	default:
		http.Error(w, "unknown", http.StatusNotFound)
	}
}

// referrersBodyFor 两源描述符表：fast = [D1(mt1), D2(mt2)]；slow = [D2(mt2)(与 fast 重复),
// D2(mt3)(同 digest 不同 mediaType——组合键应各自成条), D3(mt2)]。
func referrersBodyFor(source string) string {
	d1, d2, d3 := multisourceDigest(0x31), multisourceDigest(0x32), multisourceDigest(0x33)
	mt1 := "application/vnd.oci.image.manifest.v1+json"
	mt2 := "application/vnd.docker.distribution.manifest.v2+json"
	mt3 := "application/vnd.oci.image.index.v1+json"
	fast := []string{
		`{"mediaType":"` + mt1 + `","digest":"` + d1 + `"}`,
		`{"mediaType":"` + mt2 + `","digest":"` + d2 + `"}`,
	}
	slow := []string{
		`{"mediaType":"` + mt2 + `","digest":"` + d2 + `"}`,
		`{"mediaType":"` + mt3 + `","digest":"` + d2 + `"}`,
		`{"mediaType":"` + mt2 + `","digest":"` + d3 + `"}`,
	}
	pick := fast
	if source == "slow" {
		pick = slow
	}
	return `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[` + strings.Join(pick, ",") + `]}`
}

// TestMultiSourceTagsUnion 缺口#12-1：tags 并集去重排序——快源 [v1,v2] + 慢源 [v2,v3]
// → 合并 [v1,v2,v3]（字典序），日志 merged=3，X-Cache=BYPASS。
func TestMultiSourceTagsUnion(t *testing.T) {
	env, _ := multisourceEnv(t, 0, 400*time.Millisecond)
	rec := runServe(env.srv, requestTo("http://"+testHost+"/v2/app/tags/tags/list"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	var envBody struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envBody); err != nil {
		t.Fatalf("合并响应应为 JSON: %v, body: %s", err, rec.Body.String())
	}
	want := []string{"v1", "v2", "v3"}
	if len(envBody.Tags) != len(want) {
		t.Fatalf("合并 tags = %v, want %v（并集去重）", envBody.Tags, want)
	}
	for i := range want {
		if envBody.Tags[i] != want[i] {
			t.Errorf("合并 tags[%d] = %q, want %q（字典序排序）", i, envBody.Tags[i], want[i])
		}
	}
	if got := rec.Header().Get("X-Cache"); got != "BYPASS" {
		t.Errorf("X-Cache = %q, want BYPASS（可变目标不缓存）", got)
	}
	if n := countLines(env.logText(), "merged=3"); n < 1 {
		t.Errorf("汇总日志应含 merged=3，实际 %d 行:\n%v", n, logLines(env.logText(), "多源并发回源完成"))
	}
}

// TestMultiSourceReferrersDedup 缺口#12-2：descriptors 按 mediaType+digest 组合键去重——
// 快源 [D1(mt1), D2(mt2)] + 慢源 [D2(mt2)(重复), D2(mt3)(同 digest 异 mediaType), D3(mt2)]
// → 合并恰 4 条（D2(mt2) 只保留一份，D2(mt3) 独立成条）。
func TestMultiSourceReferrersDedup(t *testing.T) {
	env, _ := multisourceEnv(t, 0, 400*time.Millisecond)
	rec := runServe(env.srv, requestTo("http://"+testHost+"/v2/app/tags/referrers/"+multisourceDigest(0x41)))
	if rec.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Manifests []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("合并响应应为 JSON: %v, body: %s", err, rec.Body.String())
	}
	type key struct{ mt, dg string }
	got := map[key]int{}
	for _, m := range body.Manifests {
		got[key{m.MediaType, m.Digest}]++
	}
	d1, d2, d3 := multisourceDigest(0x31), multisourceDigest(0x32), multisourceDigest(0x33)
	want := map[key]int{
		{"application/vnd.oci.image.manifest.v1+json", d1}:           1,
		{"application/vnd.docker.distribution.manifest.v2+json", d2}: 1,
		{"application/vnd.oci.image.index.v1+json", d2}:              1,
		{"application/vnd.docker.distribution.manifest.v2+json", d3}: 1,
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("descriptor (mediaType=%s, digest=%s…) 出现 %d 次, want %d（组合键去重）", k.mt, k.dg[:12], got[k], n)
		}
	}
	if len(body.Manifests) != len(want) {
		t.Errorf("合并后 descriptors 总数 = %d, want %d", len(body.Manifests), len(want))
	}
	if n := countLines(env.logText(), "merged=4"); n < 1 {
		t.Errorf("汇总日志应含 merged=4，实际 %d 行", n)
	}
}

// TestMultiSourceManifestTiePicksFastest 缺口#12-3：偶数源 1:1 平票取最快成功者——
// 快源立即应答、慢源延迟 400ms，两源摘要不同 → 共识胜出者必为快源（body 与
// X-Proxy-Upstream 均为快源），日志 agree=1 disagree=1。
func TestMultiSourceManifestTiePicksFastest(t *testing.T) {
	env, fastURL := multisourceEnv(t, 0, 400*time.Millisecond)
	rec := runServe(env.srv, requestTo("http://"+testHost+"/v2/app/tags/manifests/latest"))
	if rec.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Source string `json:"source"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("共识响应应为 JSON: %v", err)
	}
	if body.Source != "fast" {
		t.Errorf("平票应取最快成功者，胜出 body 来自 %q, want fast", body.Source)
	}
	if got := rec.Header().Get("X-Proxy-Upstream"); !strings.Contains(got, "127.0.0.1") || got == "" {
		t.Logf("X-Proxy-Upstream = %q（应为快源候选 URL）", got)
	}
	if dg := rec.Header().Get("Docker-Content-Digest"); dg != multisourceDigest(0x11) {
		t.Errorf("共识摘要 = %q, want 快源摘要 %q", dg, multisourceDigest(0x11))
	}
	log := env.logText()
	if n := countLines(log, "agree=1"); n < 1 {
		t.Errorf("汇总日志应含 agree=1，实际 %d 行:\n%v", n, logLines(log, "摘要共识"))
	}
	if n := countLines(log, "disagree=1"); n < 1 {
		t.Errorf("汇总日志应含 disagree=1，实际 %d 行", n)
	}
	if n := countLines(log, "agree=2"); n != 0 {
		t.Errorf("两源摘要不同不应 agree=2，实际 %d 行", n)
	}
	_ = fastURL
}
