// token_realm_isolation_test.go —— docker token dance 按 realm|service|scope 缓存隔离的
// 黑盒回归（缺口#6，session-c3461af1 现场疑点「hub.rat.dev 用了 1ms.run 签发的 token」）。
// 两个候选镜像站各自回 401 + 指向不同 realm 的 Bearer 挑战，交错请求断言：
//  1. 各 realm 各取各 token（两个 token 端点各恰被请求 1 次，service/scope 查询参数
//     与各自挑战一致）；
//  2. token 进程内缓存的 key 含 realm|service|scope——同 realm 第二次请求命中缓存
//     （token 端点不再被请求），另一 realm 绝不复用别家的 token；
//  3. 每个镜像站的带 token 重试拿到的都是自己 realm 签发的 token（A 拿 tok-a、
//     B 拿 tok-b；若缓存 key 塌缩，B 会拿 A 的 token 被困在 401）。
//
// 确定性设计：候选轮询共享 s.rr 计数器（串行探测 start_index 依次 0,1,0），请求序列
// A、B、A 天然交错三 realm 场景；每个 blob 用独立 digest 避免缓存 HIT 干扰。
package tests

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"proxy-cache/config"
)

// realmFixture 一套「镜像站 + token 端点」假上游：镜像站对无 token/错 token 的请求回
// 401 + 指向自己 token 端点的挑战；token 端点记录 service/scope 查询参数并签发固定
// token。全部命中计数供隔离断言。
type realmFixture struct {
	mirrorSrv    string
	mirrorHits   sync.Map // retry Authorization 原文列表（[]string）
	tokenSrv     string
	tokenCalls   int
	tokenSvc     string // 最近一次收到的 service 查询参数
	tokenScope   string // 最近一次收到的 scope 查询参数
	blobs        map[string][]byte
	challengeTag string
}

// newRealmFixture 启动一套假镜像站 + 假 token 端点。
func newRealmFixture(t *testing.T, token, service, scope string) *realmFixture {
	t.Helper()
	fx := &realmFixture{blobs: map[string][]byte{}, challengeTag: service}
	var mu sync.Mutex
	tokenSrv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fx.tokenCalls++
		fx.tokenSvc = r.URL.Query().Get("service")
		fx.tokenScope = r.URL.Query().Get("scope")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"token": token, "expires_in": 300})
	})
	fx.tokenSrv = tokenSrv.URL
	mirrorSrv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("WWW-Authenticate",
				`Bearer realm="`+tokenSrv.URL+`",service="`+service+`",scope="`+scope+`"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		digest := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		mu.Lock()
		fx.mirrorHits.Store(digest, append(authzOf(fx, digest), r.Header.Get("Authorization")))
		mu.Unlock()
		body, ok := fx.blobs[digest] // 路径末段已含 sha256: 前缀
		if !ok {
			http.Error(w, "unknown blob", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	fx.mirrorSrv = mirrorSrv.URL
	return fx
}

// authzOf 取该 blob 路径已记录的 retry Authorization 列表。
func authzOf(fx *realmFixture, path string) []string {
	v, _ := fx.mirrorHits.Load(path)
	s, _ := v.([]string)
	return s
}

// authzList 断言期读取（镜像站 handler 已结束，无并发写）。
func (fx *realmFixture) authzFor(digestPath string) []string {
	s, _ := fx.mirrorHits.Load(digestPath)
	l, _ := s.([]string)
	return l
}

// registerBlob 登记一个 blob 内容（两个镜像站共享同一份内容表：内容正确性与「哪个
// 镜像站供数」解耦）。
func (fx *realmFixture) registerBlob(digest string, body []byte) {
	fx.blobs[digest] = body
}

// TestTokenDanceRealmIsolation 两候选不同 realm 交错请求：各取各 token、同 realm 缓存
// 复用、跨 realm 绝不串用。
func TestTokenDanceRealmIsolation(t *testing.T) {
	const (
		svcA = "svc-a"
		svcB = "svc-b"
		scA  = "repository:app/auth:pull"
		scB  = "repository:app/auth:pull" // scope 与 A 相同：隔离必须靠 realm（与 svc）
		tokA = "tok-a-realm-a-only"
		tokB = "tok-b-realm-b-only"
	)
	fxA := newRealmFixture(t, tokA, svcA, scA)
	fxB := newRealmFixture(t, tokB, svcB, scB)

	blob1 := blobPattern(16 * 1024)
	blob2 := blobPattern(16 * 1024)
	for i := range blob2 {
		blob2[i] = ^blob2[i]
	}
	blob3 := blobPattern(16 * 1024)
	for i := range blob3 {
		blob3[i] = byte(0x5a ^ blob3[i])
	}
	fxA.registerBlob(digestOf(blob1), blob1)
	fxA.registerBlob(digestOf(blob3), blob3)
	fxB.registerBlob(digestOf(blob2), blob2)

	hostPort := strings.TrimPrefix(fxA.mirrorSrv, "http://") // 入站 Host 无关紧要，仅要求 match 命中
	_ = hostPort
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.Match = `http://registry\.proxycache\.test/`
		rule.URLRedirect = &[]config.URLRedirectItem{
			{Template: fxA.mirrorSrv + "/$1"},
			{Template: fxB.mirrorSrv + "/$1"},
		}
		cfg.URLRedirectHedgeDelay = ptrDuration(0)
	})
	_ = hostPort

	// 请求 1（候选 A）、请求 2（候选 B）、请求 3（候选 A 复用缓存）：独立 digest 防 HIT
	reqs := []struct {
		body  []byte
		fx    *realmFixture
		token string
	}{
		{blob1, fxA, tokA},
		{blob2, fxB, tokB},
		{blob3, fxA, tokA},
	}
	for i, req := range reqs {
		digest := digestOf(req.body)
		rec := env.get(blobTarget("app/auth", digest), nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("请求 %d Code = %d, want 200, body: %s", i+1, rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); len(got) != len(req.body) {
			t.Fatalf("请求 %d body 长度 = %d, want %d（经 token dance 后内容完整）", i+1, len(got), len(req.body))
		}
	}

	// token 端点各恰 1 次（同 realm 第二次走缓存；跨 realm 不复用）
	if fxA.tokenCalls != 1 {
		t.Errorf("realm A token 端点应恰被请求 1 次（请求 3 命中缓存），实际 %d", fxA.tokenCalls)
	}
	if fxB.tokenCalls != 1 {
		t.Errorf("realm B token 端点应恰被请求 1 次（绝不复用 realm A 的缓存 token），实际 %d", fxB.tokenCalls)
	}
	// service/scope 查询参数与各自挑战一致
	if fxA.tokenSvc != svcA || fxA.tokenScope != scA {
		t.Errorf("realm A token 请求参数 service=%q scope=%q, want %q/%q", fxA.tokenSvc, fxA.tokenScope, svcA, scA)
	}
	if fxB.tokenSvc != svcB || fxB.tokenScope != scB {
		t.Errorf("realm B token 请求参数 service=%q scope=%q, want %q/%q", fxB.tokenSvc, fxB.tokenScope, svcB, scB)
	}
	// 每个镜像站：一次 401 挑战 + 一次带「自己 realm 的 token」重试成功
	for _, fx := range []*realmFixture{fxA, fxB} {
		for digestKey := range fx.blobs {
			authz := fx.authzFor(digestKey)
			want := "Bearer " + map[*realmFixture]string{fxA: tokA, fxB: tokB}[fx]
			if len(authz) != 1 {
				t.Errorf("%s blob %s 重试 Authorization 记录 = %v, want 恰 1 条", fx.challengeTag, digestKey, authz)
				continue
			}
			if authz[0] != want {
				t.Errorf("%s 拿到的重试 token = %q, want %q（token 缓存必须按 realm|service|scope 隔离）", fx.challengeTag, authz[0], want)
			}
		}
	}
	// 日志：token 缓存命中恰 1 次（请求 3，realm A）
	if n := countLines(env.logText(), "token 缓存命中"); n != 1 {
		t.Errorf("应恰有 1 条 token 缓存命中留痕（请求 3），实际 %d", n)
	}
}
