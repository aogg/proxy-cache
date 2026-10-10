package proxy

// dockerauth_test.go —— docker registry 上游 401 匿名 Bearer token dance 单测：
// 全部使用 httptest 假镜像站（候选上游）与假 token 端点（不启动本服务、不发任何真实
// 外网请求），经 fetchViaCandidates / parseBearerChallenge 验证：
//   - 候选 GET 回源遇 401+WWW-Authenticate: Bearer 挑战 → 去 realm 匿名换 token →
//     带 Authorization: Bearer 重试一次的成功路径：token 端点收到正确 service/scope
//     查询参数、Accept: application/json、绝不携带客户端 Authorization；重试请求
//     Authorization 已替换为 Bearer <token>；Entry.Source 命中候选且 200+body 过判据；
//   - token 响应仅回 access_token 字段时的兜底；
//   - token 进程内缓存：同 realm|service|scope 连续两次候选请求只打一次 token 端点，
//     两次 blob 内容不同仍各自正确返回；
//   - 失败兜底一：token 端点 500 → 候选按 401 拒绝（报错或回退直连），不 panic；
//   - 失败兜底二：401 无 WWW-Authenticate（或非 Bearer 挑战如 Basic）→ 不 dance
//     （token 端点计数为 0），候选照旧被拒；
//   - HEAD 探测：dance 后重试 HEAD 成功（200），不因 HEAD 无 body 报错；
//   - parseBearerChallenge / splitChallengeParams 纯函数：daocloud 风格挑战（双引号、
//     scope 含冒号）、参数名大小写混合、多参数（含 error=xxx）、非 Bearer scheme、
//     缺 realm、引号内逗号不拆分等；
//   - 重试仍 401：只 dance 一次不循环（上游共 2 次请求），最终按 401 处理。

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"proxy-cache/cache"
)

// danceClientAuthz / danceRetryAuthz 是 dance 测试里客户端透传与 token 端点签发的
// Authorization 值（假 token 正文，仅指向本地 httptest 端点）。
const (
	danceClientAuthz = "Bearer client-cred" // 客户端自带的 Authorization（透传给上游首请求）
	danceToken       = "abc123"             // 假 token 端点签发的 token 正文（token 字段）
	danceTokenAlt    = "xyz789"             // access_token 字段兜底用例签发的 token 正文
)

// mirrorReq 是假镜像站收到的一次请求快照（拷贝所需字段，不保留原 *http.Request，
// 避免连接复用导致底层数据被后续请求改写）。
type mirrorReq struct {
	method string // 请求方法（GET/HEAD）
	path   string // 请求路径（含查询串）
	authz  string // Authorization 头原文（未携带则为空串）
	accept string // Accept 头原文（验证白名单头透传）
}

// tokenReq 是假 token 端点收到的一次请求快照。
type tokenReq struct {
	query  url.Values // 查询参数（service/scope 应来自挑战参数）
	accept string     // Accept 头（应为 application/json）
	authz  string     // Authorization 头（应为空：绝不携带客户端凭证）
	ua     string     // User-Agent 头（应为 proxy-cache/<Version>）
}

// danceRec 是 dance 测试的记账器：记录假镜像站与假 token 端点各自收到的请求快照
// 与次数（并发安全；测试内请求串行到达，锁仅为防御性保留）。
type danceRec struct {
	mu         sync.Mutex
	mirrorHits int         // 镜像站收到的请求总数
	mirrorReqs []mirrorReq // 逐次快照
	okHits     int         // 镜像站放行（200）次数：用于生成逐次不同的 blob 内容
	tokenHits  int         // token 端点收到的请求总数
	tokenReqs  []tokenReq  // 逐次快照
}

// addMirror 记录一次镜像站收到的请求。
func (rec *danceRec) addMirror(r *http.Request) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.mirrorHits++
	rec.mirrorReqs = append(rec.mirrorReqs, mirrorReq{
		method: r.Method,
		path:   r.URL.RequestURI(),
		authz:  r.Header.Get("Authorization"),
		accept: r.Header.Get("Accept"),
	})
}

// mirrorCalls 返回镜像站收到的请求总次数。
func (rec *danceRec) mirrorCalls() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.mirrorHits
}

// mirrorAt 返回镜像站第 i 次请求快照（0 基）。
func (rec *danceRec) mirrorAt(i int) mirrorReq {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.mirrorReqs[i]
}

// nextOK 返回放行序号（1 基）并计数：用于让每次成功响应的 blob 内容互不相同。
func (rec *danceRec) nextOK() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.okHits++
	return rec.okHits
}

// addToken 记录一次 token 端点收到的请求。
func (rec *danceRec) addToken(r *http.Request) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.tokenHits++
	rec.tokenReqs = append(rec.tokenReqs, tokenReq{
		query:  r.URL.Query(),
		accept: r.Header.Get("Accept"),
		authz:  r.Header.Get("Authorization"),
		ua:     r.Header.Get("User-Agent"),
	})
}

// tokenCalls 返回 token 端点收到的请求总次数。
func (rec *danceRec) tokenCalls() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.tokenHits
}

// lastToken 返回 token 端点最近一次请求快照。
func (rec *danceRec) lastToken() tokenReq {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.tokenReqs[len(rec.tokenReqs)-1]
}

// newTokenEndpoint 启动假 token 端点：以 status/body 应答任何请求，并把请求快照
// 记入 rec（测试按需返回 token JSON、access_token JSON 或 500）。
func newTokenEndpoint(t *testing.T, rec *danceRec, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.addToken(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newDanceMirror 启动假镜像站（候选上游），模拟 docker registry 匿名访问行为：
//   - 未带 Bearer <token> 的请求一律回 401 + 标准 Bearer 挑战（realm 指向 tokenSrv
//     的 /token、service/scope 为 daocloud 风格，值带双引号、scope 含冒号）；
//   - 带 Bearer <token> 的请求回 200 + 逐次不同的 blob 内容（blob-1、blob-2…），
//     证明缓存 token 的两次请求各自拿到各自的正确内容；
//   - alwaysUnauthorized 为 true 时即使带对 token 也回 401（验证不无限重试）。
func newDanceMirror(t *testing.T, rec *danceRec, tokenSrv *httptest.Server, token string, alwaysUnauthorized bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.addMirror(r)
		if alwaysUnauthorized || r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(
				`Bearer realm=%q,service="registry.docker.io",scope="repository:library/alpine:pull"`,
				tokenSrv.URL+"/token"))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`)
			return
		}
		w.Header().Set("Docker-Content-Digest", "sha256:abc123")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "blob-%d", rec.nextOK()) // 每次放行内容不同：验证各自正确返回
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fetchViaMethod 用 fetchViaCandidates 以指定方法（GET/HEAD）回源 target，
// 返回条目与命中来源（Entry.Source）。
func fetchViaMethod(t *testing.T, s *Server, method, target string) (*cache.Entry, string, error) {
	t.Helper()
	eff := s.cfg.ResolveOptions("", target)
	r := httptest.NewRequest(method, "/"+target, nil)
	e, err := s.fetchViaCandidates(r, target, eff)
	var src string
	if e != nil {
		src = e.Source
	}
	return e, src, err
}

// TestDockerAuthDanceCandidateSuccess 验证候选 401+Bearer 挑战 → dance 成功的全链路：
// 镜像站首次回 401+挑战，代理去 realm 匿名换 token（token 端点收到正确 service/scope
// 查询参数、Accept: application/json、不带客户端 Authorization），随后带
// Authorization: Bearer <token> 重试一次拿到 200+body；fetchViaCandidates 命中该候选
// （Entry.Source 为候选 URL），200+非空 body 过 content 判据（可写缓存语义不破坏）。
func TestDockerAuthDanceCandidateSuccess(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false)

	s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)

	// 客户端自带 Authorization 与 Accept：前者应透传给上游首请求、但绝不带到 token 端点，
	// 且 dance 重试时被替换为 Bearer <token>；后者作为白名单头全程透传
	r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
	r.Header.Set("Authorization", danceClientAuthz)
	r.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")
	eff := s.cfg.ResolveOptions("", candidatesTargetURL)
	e, err := s.fetchViaCandidates(r, candidatesTargetURL, eff)
	if err != nil {
		t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
	}

	// 结果语义：200 + body，命中候选 URL（401+重试 200 过 content 判据，缓存语义不破坏）
	if e.Status != http.StatusOK {
		t.Errorf("Status = %d, want %d（dance 重试应成功）", e.Status, http.StatusOK)
	}
	if got := string(e.Body); got != "blob-1" {
		t.Errorf("Body = %q, want 重试响应内容 %q", got, "blob-1")
	}
	if want := mirror.URL + "/" + candidatesTargetURL; e.Source != want {
		t.Errorf("Source = %q, want 命中候选 %q", e.Source, want)
	}

	// token 端点请求形态：service/scope 查询参数、Accept、无 Authorization、固定 UA
	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点被打 %d 次, want 1", n)
	}
	tr := rec.lastToken()
	if got := tr.query.Get("service"); got != "registry.docker.io" {
		t.Errorf("token 端点 service 参数 = %q, want %q", got, "registry.docker.io")
	}
	if got := tr.query.Get("scope"); got != "repository:library/alpine:pull" {
		t.Errorf("token 端点 scope 参数 = %q, want %q", got, "repository:library/alpine:pull")
	}
	if got := tr.accept; got != "application/json" {
		t.Errorf("token 端点 Accept = %q, want %q", got, "application/json")
	}
	if got := tr.authz; got != "" {
		t.Errorf("token 端点收到 Authorization = %q, want 不携带（客户端凭证绝不外泄给 realm）", got)
	}
	if got := tr.ua; !strings.HasPrefix(got, "proxy-cache/") {
		t.Errorf("token 端点 User-Agent = %q, want proxy-cache/ 前缀", got)
	}

	// 镜像站两次请求：首次带客户端 Authorization，重试已替换为 Bearer <token>，
	// 且只 dance 一次（镜像站共 2 次请求、token 端点 1 次）
	if n := rec.mirrorCalls(); n != 2 {
		t.Errorf("镜像站被打 %d 次, want 2（首次 401 + 带 token 重试一次）", n)
	}
	if got := rec.mirrorAt(0).authz; got != danceClientAuthz {
		t.Errorf("镜像站首次请求 Authorization = %q, want 客户端透传 %q", got, danceClientAuthz)
	}
	retry := rec.mirrorAt(1)
	if got := retry.authz; got != "Bearer "+danceToken {
		t.Errorf("重试请求 Authorization = %q, want %q", got, "Bearer "+danceToken)
	}
	if got := retry.accept; got != "application/vnd.oci.image.manifest.v1+json" {
		t.Errorf("重试请求 Accept = %q, want 白名单头原样透传", got)
	}
}

// TestDockerAuthDanceAccessTokenFallback 验证 token 响应 access_token 字段兜底：
// token 端点只回 {"access_token":...}（无 token 字段）也能换取成功并完成重试。
func TestDockerAuthDanceAccessTokenFallback(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"access_token":"`+danceTokenAlt+`"}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceTokenAlt, false)

	s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)
	e, src, err := fetchVia(t, s, candidatesTargetURL)
	if err != nil {
		t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
	}
	if e.Status != http.StatusOK {
		t.Errorf("Status = %d, want %d（access_token 兜底应成功）", e.Status, http.StatusOK)
	}
	if got := string(e.Body); got != "blob-1" {
		t.Errorf("Body = %q, want %q", got, "blob-1")
	}
	if want := mirror.URL + "/" + candidatesTargetURL; src != want {
		t.Errorf("Source = %q, want 命中候选 %q", src, want)
	}
	if got := rec.mirrorAt(1).authz; got != "Bearer "+danceTokenAlt {
		t.Errorf("重试请求 Authorization = %q, want %q（取自 access_token 字段）", got, "Bearer "+danceTokenAlt)
	}
}

// TestDockerAuthDanceTokenCachedAcrossRequests 验证 token 进程内缓存：
// 同一 Server 上连续两次候选请求（realm|service|scope 相同），token 端点只被打一次；
// 两次 blob 内容不同也能各自正确返回（第二次 401 后用缓存 token 重试成功）。
func TestDockerAuthDanceTokenCachedAcrossRequests(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false)

	s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)

	for i, wantBody := range []string{"blob-1", "blob-2"} {
		e, src, err := fetchVia(t, s, candidatesTargetURL)
		if err != nil {
			t.Fatalf("第 %d 次 fetchViaCandidates() 意外报错: %v", i+1, err)
		}
		if e.Status != http.StatusOK {
			t.Errorf("第 %d 次 Status = %d, want %d", i+1, e.Status, http.StatusOK)
		}
		if got := string(e.Body); got != wantBody {
			t.Errorf("第 %d 次 Body = %q, want %q（两次请求内容不同仍各自正确）", i+1, got, wantBody)
		}
		if want := mirror.URL + "/" + candidatesTargetURL; src != want {
			t.Errorf("第 %d 次 Source = %q, want %q", i+1, src, want)
		}
	}

	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点被打 %d 次, want 1（同 realm|service|scope 第二次应命中进程内缓存）", n)
	}
	if n := rec.mirrorCalls(); n != 4 {
		t.Errorf("镜像站被打 %d 次, want 4（每次请求各自 401+重试）", n)
	}
}

// TestDockerAuthDanceTokenEndpointFailure 验证失败兜底一：token 端点回 500 时
// dance 放弃、按原始 401 处理（候选判据不过），全程不 panic：
//   - 关闭 fallback-direct：fetchViaCandidates 返回错误（候选按 401 拒绝）；
//   - 开启 fallback-direct：候选 401 拒绝后回退直连本地成功上游（Source=direct）。
func TestDockerAuthDanceTokenEndpointFailure(t *testing.T) {
	t.Run("关闭fallback时候选按401拒绝返回错误", func(t *testing.T) {
		rec := &danceRec{}
		tokenSrv := newTokenEndpoint(t, rec, http.StatusInternalServerError, "boom")
		mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false)

		s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)
		_, _, err := fetchVia(t, s, candidatesTargetURL)
		if err == nil {
			t.Fatal("fetchViaCandidates() 期望报错（token 换取失败 → 原始 401 不过判据），实际为 nil")
		}
		if !strings.Contains(err.Error(), "success-check") {
			t.Errorf("错误信息 %q 应包含判据拒绝语义 %q", err.Error(), "success-check")
		}
		if n := rec.tokenCalls(); n != 1 {
			t.Errorf("token 端点被打 %d 次, want 1", n)
		}
		if n := rec.mirrorCalls(); n != 1 {
			t.Errorf("镜像站被打 %d 次, want 1（token 失败后不应发起重试）", n)
		}
	})

	t.Run("开启fallback时回退直连本地成功上游", func(t *testing.T) {
		rec := &danceRec{}
		tokenSrv := newTokenEndpoint(t, rec, http.StatusInternalServerError, "boom")
		mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false)
		good := contentUpstream(t)
		defer good.Close()

		s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, true)
		// 目标 URL 指向本地必成功假上游：候选 401 拒绝后回退直连该上游（不碰真实外网）
		target := good.URL + "/file.yaml"
		e, src, err := fetchVia(t, s, target)
		if err != nil {
			t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
		}
		if src != "direct" {
			t.Errorf("Source = %q, want %q（候选 401 后应回退直连）", src, "direct")
		}
		if e.Status != http.StatusOK || string(e.Body) != "content" {
			t.Errorf("Status/Body = %d/%q, want 200/content", e.Status, string(e.Body))
		}
		if n := rec.mirrorCalls(); n != 1 {
			t.Errorf("镜像站被打 %d 次, want 1（token 失败后不应发起重试）", n)
		}
	})
}

// TestDockerAuthDanceSkippedWithoutBearerChallenge 验证失败兜底二：
// 401 但无可解析的 Bearer 挑战（无 WWW-Authenticate 头，或挑战为 Basic）时不 dance
// （token 端点计数为 0、上游只收到首请求无重试），候选照旧按 401 被拒。
func TestDockerAuthDanceSkippedWithoutBearerChallenge(t *testing.T) {
	tests := []struct {
		name      string
		challenge string // 401 响应携带的 WWW-Authenticate 头（空串 = 不携带）
	}{
		{"401无WWW-Authenticate头", ""},
		{"401挑战为非Bearer的Basic", `Basic realm="example"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &danceRec{}
			// token 端点仅用于断言「不 dance 时零请求」，返回值无需引用
			_ = newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
			mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rec.addMirror(r)
				if tt.challenge != "" {
					w.Header().Set("WWW-Authenticate", tt.challenge)
				}
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, "unauthorized")
			}))
			defer mirror.Close()

			s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)
			_, _, err := fetchVia(t, s, candidatesTargetURL)
			if err == nil {
				t.Fatal("fetchViaCandidates() 期望报错（401 不 dance 应照旧被拒），实际为 nil")
			}
			if n := rec.tokenCalls(); n != 0 {
				t.Errorf("token 端点被打 %d 次, want 0（无 Bearer 挑战不应 dance）", n)
			}
			if n := rec.mirrorCalls(); n != 1 {
				t.Errorf("镜像站被打 %d 次, want 1（不 dance 即不应重试）", n)
			}
		})
	}
}

// TestDockerAuthDanceHeadProbeSuccess 验证 HEAD 探测的 dance：
// HEAD 候选 401+挑战 → 换 token 重试 HEAD 成功（200），响应头照常进入 Entry，
// 不因 HEAD 无 body 报错（Entry.Body 为空，content 判据对 HEAD 按 status 语义放行）。
func TestDockerAuthDanceHeadProbeSuccess(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, false)

	s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)
	e, src, err := fetchViaMethod(t, s, http.MethodHead, candidatesTargetURL)
	if err != nil {
		t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
	}
	if e.Status != http.StatusOK {
		t.Errorf("Status = %d, want %d（HEAD dance 重试应成功）", e.Status, http.StatusOK)
	}
	if len(e.Body) != 0 {
		t.Errorf("HEAD Entry.Body = %d 字节, want 0（HEAD 无 body 语义）", len(e.Body))
	}
	if got := e.Header.Get("Docker-Content-Digest"); got != "sha256:abc123" {
		t.Errorf("Docker-Content-Digest = %q, want 重试响应头透传 %q", got, "sha256:abc123")
	}
	if want := mirror.URL + "/" + candidatesTargetURL; src != want {
		t.Errorf("Source = %q, want 命中候选 %q", src, want)
	}
	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点被打 %d 次, want 1", n)
	}
	if n := rec.mirrorCalls(); n != 2 {
		t.Errorf("镜像站被打 %d 次, want 2（HEAD 401 + 带 token 重试一次）", n)
	}
	if got := rec.mirrorAt(1); got.method != http.MethodHead || got.authz != "Bearer "+danceToken {
		t.Errorf("重试请求 = %s Authorization %q, want HEAD / Bearer %q", got.method, got.authz, danceToken)
	}
}

// TestParseBearerChallenge 表驱动验证 parseBearerChallenge 纯函数：
// 标准 daocloud 风格挑战（双引号、scope 含冒号）、scheme/参数名大小写混合、多参数
// （含 error=xxx）、值不带引号、引号内逗号不拆分均可解析；非 Bearer scheme、缺 realm、
// 空串返回 nil（不 dance）。
func TestParseBearerChallenge(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantRealm   string
		wantService string
		wantScope   string
		wantNil     bool
	}{
		{
			name:        "daocloud风格标准挑战",
			header:      `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull"`,
			wantRealm:   "https://auth.docker.io/token",
			wantService: "registry.docker.io",
			wantScope:   "repository:library/alpine:pull",
		},
		{
			name:        "scheme与参数名大小写混合",
			header:      `bearer REALM="https://t/token",SeRvIcE="svc",SCope="repository:a/b:pull"`,
			wantRealm:   "https://t/token",
			wantService: "svc",
			wantScope:   "repository:a/b:pull",
		},
		{
			name:        "多参数含error",
			header:      `Bearer realm="https://t/token",service="svc",scope="repository:a/b:pull",error="invalid_token"`,
			wantRealm:   "https://t/token",
			wantService: "svc",
			wantScope:   "repository:a/b:pull",
		},
		{
			name:        "值不带双引号",
			header:      `Bearer realm=https://t/token,service=svc`,
			wantRealm:   "https://t/token",
			wantService: "svc",
			wantScope:   "",
		},
		{
			name:      "scope双引号内逗号不参与分隔",
			header:    `Bearer realm="https://t/token",scope="repository:a,b:pull"`,
			wantRealm: "https://t/token",
			wantScope: "repository:a,b:pull",
		},
		{name: "非Bearer scheme", header: `Basic realm="https://t/token"`, wantNil: true},
		{name: "缺realm", header: `Bearer service="registry.docker.io",scope="repository:library/alpine:pull"`, wantNil: true},
		{name: "空串", header: "", wantNil: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := parseBearerChallenge(tt.header)
			if tt.wantNil {
				if ch != nil {
					t.Fatalf("parseBearerChallenge(%q) = %+v, want nil", tt.header, ch)
				}
				return
			}
			if ch == nil {
				t.Fatalf("parseBearerChallenge(%q) = nil, want 可解析", tt.header)
			}
			if ch.realm != tt.wantRealm {
				t.Errorf("realm = %q, want %q", ch.realm, tt.wantRealm)
			}
			if ch.service != tt.wantService {
				t.Errorf("service = %q, want %q", ch.service, tt.wantService)
			}
			if ch.scope != tt.wantScope {
				t.Errorf("scope = %q, want %q", ch.scope, tt.wantScope)
			}
		})
	}

	t.Run("splitChallengeParams按逗号拆分且保留引号原文", func(t *testing.T) {
		tests := []struct {
			name  string
			in    string
			wantN int
			want0 string
			want1 string
		}{
			{"引号内逗号不拆分", `a="x,y",b=z`, 2, `a="x,y"`, `b=z`},
			{"普通逗号逐个拆分", `realm="r",service="s",scope="p"`, 3, `realm="r"`, `service="s"`},
			{"空串无参数", "", 0, "", ""},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := splitChallengeParams(tt.in)
				if len(got) != tt.wantN {
					t.Fatalf("splitChallengeParams(%q) 拆出 %d 段 %v, want %d 段", tt.in, len(got), got, tt.wantN)
				}
				if tt.wantN > 0 && got[0] != tt.want0 {
					t.Errorf("got[0] = %q, want %q", got[0], tt.want0)
				}
				if tt.wantN > 1 && got[1] != tt.want1 {
					t.Errorf("got[1] = %q, want %q", got[1], tt.want1)
				}
			})
		}
	})
}

// TestDockerAuthDanceRetryStillUnauthorized 验证重试仍 401 的兜底：
// token 有效但镜像站对 Bearer 请求依旧回 401 时，只 dance 一次不无限循环
// （镜像站共 2 次请求：首请求 + 带对 token 的重试；token 端点 1 次），
// 重试的 401 未过判据 → 候选照旧被拒（fetchViaCandidates 报错）。
func TestDockerAuthDanceRetryStillUnauthorized(t *testing.T) {
	rec := &danceRec{}
	tokenSrv := newTokenEndpoint(t, rec, http.StatusOK, `{"token":"`+danceToken+`","expires_in":300}`)
	mirror := newDanceMirror(t, rec, tokenSrv, danceToken, true) // 带 token 也回 401

	s := newCandidatesServer(t, []string{mirror.URL + "/$1"}, false)
	_, _, err := fetchVia(t, s, candidatesTargetURL)
	if err == nil {
		t.Fatal("fetchViaCandidates() 期望报错（重试仍 401 应按候选拒绝处理），实际为 nil")
	}
	if !strings.Contains(err.Error(), "success-check") {
		t.Errorf("错误信息 %q 应包含判据拒绝语义 %q", err.Error(), "success-check")
	}
	if n := rec.mirrorCalls(); n != 2 {
		t.Errorf("镜像站被打 %d 次, want 2（只重试一次，不无限循环）", n)
	}
	if n := rec.tokenCalls(); n != 1 {
		t.Errorf("token 端点被打 %d 次, want 1", n)
	}
	if got := rec.mirrorAt(0).authz; got != "" {
		t.Errorf("镜像站首次请求 Authorization = %q, want 空（客户端未携带）", got)
	}
	if got := rec.mirrorAt(1).authz; got != "Bearer "+danceToken {
		t.Errorf("重试请求 Authorization = %q, want %q（确已带 token 重试）", got, "Bearer "+danceToken)
	}
}
