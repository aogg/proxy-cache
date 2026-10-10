package proxy

// serve_test.go —— serveEntry 响应回放行为的单测（httptest.ResponseRecorder，不起真实服务）：
//   - 最终给客户端 304（上游 304 透传 / HIT ETag 命中降级）不设置 Content-Length
//     （304 不应携带实体头），并输出「内容未变更」成功语义日志；
//   - 200 且 body 非空（非 HEAD）输出「成功返回内容」Info 日志（含 rule/upstream/status/bytes/cache）；
//   - 其余状态码行为不变（如 200 仍设置 Content-Length）。

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
)

// captureLogger 返回把日志写入 buf 的 slog.Logger，用于断言日志输出内容。
func captureLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

// newServeServer 构建用于 serveEntry 测试的 Server：
// 默认配置（无 url-redirect）、禁用缓存、日志写入 buf 便于断言。
func newServeServer(t *testing.T, buf *bytes.Buffer) *Server {
	t.Helper()
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, captureLogger(buf))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// TestServeEntryUpstream304NoContentLength 验证上游 304 原样透传：
// 最终状态码 304、不设置 Content-Length（304 不应带实体头），
// 并输出「内容未变更」Info 成功日志而非失败。
func TestServeEntryUpstream304NoContentLength(t *testing.T) {
	var buf bytes.Buffer
	s := newServeServer(t, &buf)

	eff := s.cfg.ResolveOptions("", candidatesTargetURL)
	r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
	rec := httptest.NewRecorder()
	// 条件头已不透传，上游对无条件 GET 仍回 304 属异常/缓存副本有效兜底场景
	e := &cache.Entry{Status: http.StatusNotModified, Header: http.Header{}, Source: "direct"}

	s.serveEntry(rec, r, e, "BYPASS", eff, time.Now())

	if rec.Code != http.StatusNotModified {
		t.Errorf("Code = %d, want %d", rec.Code, http.StatusNotModified)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "" {
		t.Errorf("304 响应不应设置 Content-Length，实际 %q", cl)
	}
	if !strings.Contains(buf.String(), "内容未变更") {
		t.Errorf("日志应包含「内容未变更」成功语义，实际: %s", buf.String())
	}
}

// TestServeEntryHitEtag304NoContentLength 验证缓存 HIT 且 If-None-Match 命中条目 ETag：
// 回 304 且不设置 Content-Length，输出「内容未变更」成功日志（既有 304 行为 + 新日志语义）。
func TestServeEntryHitEtag304NoContentLength(t *testing.T) {
	var buf bytes.Buffer
	s := newServeServer(t, &buf)

	eff := s.cfg.ResolveOptions("", candidatesTargetURL)
	r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
	r.Header.Set("If-None-Match", `"abc"`)
	rec := httptest.NewRecorder()
	h := http.Header{}
	h.Set("ETag", `"abc"`)
	e := &cache.Entry{Status: http.StatusOK, Header: h, Body: []byte("content"), Source: "direct"}

	s.serveEntry(rec, r, e, "HIT", eff, time.Now())

	if rec.Code != http.StatusNotModified {
		t.Errorf("Code = %d, want %d", rec.Code, http.StatusNotModified)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "" {
		t.Errorf("304 响应不应设置 Content-Length，实际 %q", cl)
	}
	if !strings.Contains(buf.String(), "内容未变更") {
		t.Errorf("日志应包含「内容未变更」成功语义，实际: %s", buf.String())
	}
}

// TestServeEntry200SuccessLog 验证 200 且 body 非空（非 HEAD）时
// 输出「成功返回内容」Info 日志，且 200 的 Content-Length 行为不变。
func TestServeEntry200SuccessLog(t *testing.T) {
	var buf bytes.Buffer
	s := newServeServer(t, &buf)

	eff := s.cfg.ResolveOptions("", candidatesTargetURL)
	r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
	rec := httptest.NewRecorder()
	e := &cache.Entry{Status: http.StatusOK, Header: http.Header{}, Body: []byte("content"), Source: "direct"}

	s.serveEntry(rec, r, e, "MISS", eff, time.Now())

	if rec.Code != http.StatusOK {
		t.Errorf("Code = %d, want %d", rec.Code, http.StatusOK)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "7" {
		t.Errorf("200 响应 Content-Length = %q, want %q（行为不变）", cl, "7")
	}
	for _, want := range []string{"成功返回内容", "upstream=direct",
		"status=200", "bytes=7", "cache=MISS"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("日志应包含 %q，实际: %s", want, buf.String())
		}
	}
}

// TestServeEntry200EmptyBodyNoSuccessLog 验证 200 且 body 为空时
// 不输出「成功返回内容」日志（判据要求 body 非空）。
func TestServeEntry200EmptyBodyNoSuccessLog(t *testing.T) {
	var buf bytes.Buffer
	s := newServeServer(t, &buf)

	eff := s.cfg.ResolveOptions("", candidatesTargetURL)
	r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
	rec := httptest.NewRecorder()
	e := &cache.Entry{Status: http.StatusOK, Header: http.Header{}, Body: nil, Source: "direct"}

	s.serveEntry(rec, r, e, "MISS", eff, time.Now())

	if rec.Code != http.StatusOK {
		t.Errorf("Code = %d, want %d", rec.Code, http.StatusOK)
	}
	if strings.Contains(buf.String(), "成功返回内容") {
		t.Errorf("200 空 body 不应输出「成功返回内容」日志，实际: %s", buf.String())
	}
}
