package proxy

// docker_auth_test.go —— docker 候选上游凭据（URLRedirectItem.Auth / 规则级 auth）的单测：
// 候选 401 token dance 用 Basic 账号密码换 Bearer token（替代匿名 dance）。全部使用
// httptest 假上游（复用 dockerauth_test.go 的 danceRec / newTokenEndpoint / newDanceMirror）。
// 覆盖点：
//   1. ExpandCandidates 凭据解析：候选显式 auth > 规则级 auth（无规则级继承为 nil = 匿名）；
//   2. 带凭据 dance 全链路：镜像 401 → token 端点收到 Basic base64(user:pass) + account
//      参数 → 带 Bearer 重试成功，客户端拿到 200 内容；
//   3. token 缓存按凭据身份隔离：同 realm/service/scope 下不同账号各取各的 token
//     （匿名/账号互不串用，密码不进缓存 key 原文）；
//   4. 二级路径镜像模板：占位符前带路径前缀（如 https://mirror.houlang.cloud/dh/$...）
//      展开为 <prefix>/v2/...（斜杠接缝去重不吞前缀）。

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestCandidateAuthExpandResolution 覆盖点 1：候选展开的凭据两级解析。
func TestCandidateAuthExpandResolution(t *testing.T) {
	ruleAuth := &config.CandidateAuth{Username: "rule-user", Password: "rule-pass"}
	itemAuth := &config.CandidateAuth{Username: "item-user", Password: "item-pass"}
	eff := config.Options{
		Auth:        ruleAuth,
		HTTPTimeout: 10 * time.Second,
	}
	items := []config.URLRedirectItem{
		{Template: "https://a.example/$http.server.header.full_url_no_server"},                 // 继承规则级
		{Template: "https://b.example/$http.server.header.full_url_no_server", Auth: itemAuth}, // 候选显式覆盖
		{Template: "https://c.example/$http.server.header.full_url_no_server"},                 // 继承规则级
	}
	noAuth := config.Options{HTTPTimeout: 10 * time.Second}
	anonymous := []config.URLRedirectItem{
		{Template: "https://a.example/$http.server.header.full_url_no_server"}, // 无任何 auth 来源 = 匿名
	}

	req := httptest.NewRequest(http.MethodGet, "http://registry.example.com/v2/x/manifests/latest", nil)
	vars := NewRequestVars(req, "v2/x/manifests/latest")

	cands := ExpandCandidates(items, vars, eff)
	if len(cands) != 3 {
		t.Fatalf("候选数 = %d, want 3", len(cands))
	}
	for i, wantUser := range []string{"rule-user", "item-user", "rule-user"} {
		if cands[i].auth == nil || cands[i].auth.Username != wantUser {
			t.Errorf("候选 %d auth = %v, want username=%s（候选显式 ?? 规则级）", i, cands[i].auth, wantUser)
		}
	}
	for _, c := range ExpandCandidates(anonymous, vars, noAuth) {
		if c.auth != nil {
			t.Errorf("无 auth 配置时候选应为匿名（nil），got %v", c.auth)
		}
	}
}

// TestDanceWithCredentials 覆盖点 2：带凭据 dance 全链路。镜像站对匿名访问一律 401
// （模拟 HLmirror 等带鉴权镜像站），候选配置 auth 后代理用 Basic 换 token 并成功回源。
func TestDanceWithCredentials(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false) // 带 Bearer token 即放行

	s := newRegistryServer(t,
		`http://registry-proxy-cache\.linkease\.net:5480/`,
		"", // 缓存关闭：聚焦 dance 行为
		[]config.URLRedirectItem{{
			Template: mirror.URL + "/$http.server.header.full_url_no_server",
			Auth:     &config.CandidateAuth{Username: "demo@gmail.com", Password: "tok-123"},
		}},
		nil)

	req := httptest.NewRequest(http.MethodGet,
		"http://registry-proxy-cache.linkease.net:5480/"+referrersTarget(), nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Code = %d, want 200（凭据 dance 后应成功）, body: %s", w.Code, w.Body.String())
	}
	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点收到 %d 次请求, want 1", n)
	}
	tok := rec.lastToken()
	wantBasic := "Basic " + base64.StdEncoding.EncodeToString([]byte("demo@gmail.com:tok-123"))
	if tok.authz != wantBasic {
		t.Errorf("token 请求 Authorization = %q, want Basic 凭据 %q", tok.authz, wantBasic)
	}
	if got := tok.query.Get("account"); got != "demo@gmail.com" {
		t.Errorf("token 请求 account 参数 = %q, want 账号 %q", got, "demo@gmail.com")
	}
	// 镜像站收到 401 探测 + 带 token 重试共 2 次，重试带的是签发的 Bearer token
	if n := rec.mirrorCalls(); n != 2 {
		t.Errorf("镜像站收到 %d 次请求, want 2（401 探测 + Bearer 重试）", n)
	}
	if got := rec.mirrorAt(1).authz; got != "Bearer "+danceToken {
		t.Errorf("重试请求 Authorization = %q, want Bearer 签发 token", got)
	}
}

// TestTokenCacheSeparatesCredentials 覆盖点 3：token 缓存按凭据身份隔离。
// 同一 realm/service/scope 下 user1 / user2 / 匿名 三种身份各取各的 token；同身份
// 第二次取走进程内缓存（token 端点不再新增请求）。
func TestTokenCacheSeparatesCredentials(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	s := newRegistryServer(t, `http://registry-proxy-cache\.linkease\.net:5480/`, "", nil, nil)

	wd := newProgressWatchdog(context.Background(), 5*time.Second)
	defer wd.Stop()
	ch := &bearerChallenge{
		realm:   tokenSrv.URL + "/token",
		service: "registry.docker.io",
		scope:   "repository:library/alpine:pull",
	}
	user1 := &config.CandidateAuth{Username: "user1", Password: "pw1"}
	user2 := &config.CandidateAuth{Username: "user2", Password: "pw2"}

	// 三种身份各取一次：token 端点应收到 3 次请求（身份不同绝不复用）
	for _, auth := range []*config.CandidateAuth{user1, user2, nil} {
		if _, err := s.dockerToken(wd, s.client("", 0, false), ch, auth); err != nil {
			t.Fatalf("dockerToken(%v) 失败: %v", auth, err)
		}
	}
	if n := rec.tokenCalls(); n != 3 {
		t.Fatalf("token 端点收到 %d 次请求, want 3（匿名/账号各取各的）", n)
	}
	basicAt := func(i int) string { return rec.tokenReqs[i].authz }
	if basicAt(0) != "Basic "+base64.StdEncoding.EncodeToString([]byte("user1:pw1")) {
		t.Errorf("第 1 次 token 请求 Authorization = %q, want user1 凭据", basicAt(0))
	}
	if basicAt(1) != "Basic "+base64.StdEncoding.EncodeToString([]byte("user2:pw2")) {
		t.Errorf("第 2 次 token 请求 Authorization = %q, want user2 凭据", basicAt(1))
	}
	if basicAt(2) != "" {
		t.Errorf("第 3 次（匿名）token 请求 Authorization = %q, want 空（匿名不带凭据）", basicAt(2))
	}

	// 同身份再取：进程内缓存命中，token 端点不再新增请求
	for _, auth := range []*config.CandidateAuth{user1, user2, nil} {
		if _, err := s.dockerToken(wd, s.client("", 0, false), ch, auth); err != nil {
			t.Fatalf("dockerToken(%v) 二次失败: %v", auth, err)
		}
	}
	if n := rec.tokenCalls(); n != 3 {
		t.Errorf("二次取 token 后 token 端点共收到 %d 次请求, want 仍为 3（同身份命中缓存）", n)
	}
}

// TestCandidatesTemplateWithPrefixPath 覆盖点 4：二级路径镜像模板（HLmirror 风格
// /dh/、/ghcr/ 前缀）展开为 <prefix>/v2/...——占位符前的路径前缀保留，斜杠接缝
// 去重不吞前缀（现场动机：mirror.houlang.cloud 用 /dh/ 后缀区分 Docker Hub 上游）。
func TestCandidatesTemplateWithPrefixPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://registry.example.com/v2/library/alpine/manifests/latest", nil)
	vars := NewRequestVars(req, "v2/library/alpine/manifests/latest")
	items := []config.URLRedirectItem{
		{Template: "https://mirror.houlang.cloud/dh/$http.server.header.full_url_no_server"},
	}
	cands := ExpandCandidates(items, vars, config.Options{})
	if len(cands) != 1 {
		t.Fatalf("候选数 = %d, want 1", len(cands))
	}
	want := "https://mirror.houlang.cloud/dh/v2/library/alpine/manifests/latest"
	if cands[0].url != want {
		t.Errorf("展开 URL = %q, want 前缀保留的 %q", cands[0].url, want)
	}
	if !strings.HasPrefix(cands[0].url, "https://mirror.houlang.cloud/dh/") {
		t.Errorf("展开 URL %q 丢失 /dh/ 二级路径前缀", cands[0].url)
	}
}
