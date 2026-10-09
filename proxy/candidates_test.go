package proxy

// candidates_test.go —— fetchViaCandidates 的 url-redirect 轮询回源行为单测：
// 全部使用 httptest 假上游（不启动本服务、不发真实外网请求），直接调用未导出方法验证：
//   - 首个候选通过 success-check=content 即返回（Entry.Source 为该候选 URL）；
//   - 首个候选失败（判据不过）时轮换到下一个候选命中；
//   - 起始下标随请求次数 round-robin 轮换（第 1 次从 0 开始，之后逐次 +1）；
//   - 候选全部失败时按 fallback-direct 回退直连原始 URL，或关闭回退时报错。

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// candidatesTargetURL 是测试目标 URL（形状真实，仅指向 httptest 假上游）。
const candidatesTargetURL = "https://raw.githubusercontent.com/foo/bar/main/x.yaml"

// quietLogger 返回丢弃输出的 slog.Logger，避免测试日志噪音。
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newCandidatesServer 构建已校验配置（全局 url-redirect=templates、指定 fallback-direct）
// 且禁用缓存的 Server。
func newCandidatesServer(t *testing.T, templates []string, fallbackDirect bool) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.URLRedirect = templates
	cfg.FallbackDirect = &fallbackDirect
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// fetchVia 用 fetchViaCandidates 回源 target，返回条目与命中来源（Entry.Source）。
func fetchVia(t *testing.T, s *Server, target string) (*cache.Entry, string, error) {
	t.Helper()
	eff := s.cfg.ResolveOptions("", target)
	r := httptest.NewRequest(http.MethodGet, "/"+target, nil)
	e, err := s.fetchViaCandidates(r, target, eff)
	var src string
	if e != nil {
		src = e.Source
	}
	return e, src, err
}

// contentUpstream 启动一个恒返回 200 + 非空 body 的假上游（content 判据必过）。
func contentUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "content")
	}))
}

// brokenUpstream 启动一个恒返回 500 的假上游（任何判据都不过，必失败）。
func brokenUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
}

// TestFetchViaCandidatesFirstHit 验证首个候选通过 success-check=content 即返回：
// Entry 内容来自假上游，Source 为首个候选 URL。
func TestFetchViaCandidatesFirstHit(t *testing.T) {
	upstream := contentUpstream(t)
	defer upstream.Close()

	s := newCandidatesServer(t, []string{upstream.URL + "/$1"}, true)
	e, src, err := fetchVia(t, s, candidatesTargetURL)
	if err != nil {
		t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
	}
	if e.Status != http.StatusOK {
		t.Errorf("Status = %d, want %d", e.Status, http.StatusOK)
	}
	if string(e.Body) != "content" {
		t.Errorf("Body = %q, want %q", string(e.Body), "content")
	}
	if want := upstream.URL + "/" + candidatesTargetURL; src != want {
		t.Errorf("Source = %q, want 首个候选 %q", src, want)
	}
}

// TestFetchViaCandidatesRotatesToNext 验证首个候选失败时轮换到下一个候选：
// 候选 [必失败, 必成功]，全新 Server 首次请求起始下标为 0，
// 第一个候选 500 不过判据后应轮换到第二个候选并命中。
func TestFetchViaCandidatesRotatesToNext(t *testing.T) {
	bad := brokenUpstream(t)
	defer bad.Close()
	good := contentUpstream(t)
	defer good.Close()

	s := newCandidatesServer(t, []string{bad.URL + "/$1", good.URL + "/$1"}, true)
	e, src, err := fetchVia(t, s, candidatesTargetURL)
	if err != nil {
		t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
	}
	if want := good.URL + "/" + candidatesTargetURL; src != want {
		t.Errorf("Source = %q, want 轮换后的第二个候选 %q", src, want)
	}
	if string(e.Body) != "content" {
		t.Errorf("Body = %q, want %q（应来自第二个候选）", string(e.Body), "content")
	}
}

// TestFetchViaCandidatesRoundRobinStartIndex 验证起始下标 round-robin 轮换：
// 两个必成功候选下，同一 Server 第 1 次请求命中候选 0，第 2 次请求命中候选 1。
func TestFetchViaCandidatesRoundRobinStartIndex(t *testing.T) {
	up1 := contentUpstream(t)
	defer up1.Close()
	up2 := contentUpstream(t)
	defer up2.Close()

	s := newCandidatesServer(t, []string{up1.URL + "/$1", up2.URL + "/$1"}, true)

	for i, want := range []*httptest.Server{up1, up2} {
		_, src, err := fetchVia(t, s, candidatesTargetURL)
		if err != nil {
			t.Fatalf("第 %d 次 fetchViaCandidates() 意外报错: %v", i+1, err)
		}
		if want := want.URL + "/" + candidatesTargetURL; src != want {
			t.Errorf("第 %d 次 Source = %q, want 轮换命中 %q", i+1, src, want)
		}
	}
}

// TestFetchViaCandidatesFallbackDirect 验证候选全部失败后的 fallback-direct 语义：
// 开启时回退直连原始目标（Source=direct）；关闭时返回错误且不再直连。
func TestFetchViaCandidatesFallbackDirect(t *testing.T) {
	bad := brokenUpstream(t)
	defer bad.Close()
	good := contentUpstream(t)
	defer good.Close()

	t.Run("开启时回退直连原始URL", func(t *testing.T) {
		s := newCandidatesServer(t, []string{bad.URL + "/$1"}, true)
		// 目标 URL 直接指向必成功的假上游：候选必失败，直连必成功，结果可断言
		target := good.URL + "/file.yaml"
		e, src, err := fetchVia(t, s, target)
		if err != nil {
			t.Fatalf("fetchViaCandidates() 意外报错: %v", err)
		}
		if src != "direct" {
			t.Errorf("Source = %q, want %q（全部候选失败后应回退直连）", src, "direct")
		}
		if string(e.Body) != "content" {
			t.Errorf("Body = %q, want %q", string(e.Body), "content")
		}
	})

	t.Run("关闭时全部失败返回错误", func(t *testing.T) {
		s := newCandidatesServer(t, []string{bad.URL + "/$1"}, false)
		_, _, err := fetchVia(t, s, candidatesTargetURL)
		if err == nil {
			t.Fatal("fetchViaCandidates() 期望报错（全部候选失败且关闭回退直连），实际为 nil")
		}
		if !strings.Contains(err.Error(), "fallback-direct") {
			t.Errorf("错误信息 %q 应包含 %q", err.Error(), "fallback-direct")
		}
	})
}
