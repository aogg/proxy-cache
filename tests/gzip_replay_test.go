// gzip_replay_test.go —— gzip / Content-Encoding 缓存回放的黑盒回归（缺口#7，
// session-cbe6d976 用户质疑「缓存文件乱码能不能复用」的取证结论固化）：
// 上游返回 Content-Encoding: gzip 的原始压缩流时，代理必须原样落盘（不解压、不转存），
// MISS 与 HIT 两次响应的头（Content-Encoding / Content-Type / Content-Length）与体
// （逐字节同一份压缩原文）完全一致——客户端自行解压，代理绝不改变字节语义。
//
// 确定性设计：客户端显式带 Accept-Encoding: gzip（白名单头透传上游 → Go 传输层不做
// 透明解压），上游恒定返回同一份预先生成的 gzip 压缩载荷；规则 match 精确锚定假上游
// Host:Port，请求以完整 URL 驱动（web 目标路径）。
package tests

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"proxy-cache/config"
)

// gzipReplayOriginal 预先压缩的响应载荷的解压原文与其压缩形态。
var (
	gzipReplayOriginal = []byte("proxy-cache gzip replay: Content-Encoding 必须原样回放，重复内容重复内容重复内容重复内容重复内容。")
	gzipReplayBody     = mustGzip(gzipReplayOriginal)
)

// mustGzip 生成 gzip 压缩载荷（测试期固定，失败即 panic 便于发现）。
func mustGzip(raw []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		panic("gzip 压缩失败: " + err.Error())
	}
	if err := zw.Close(); err != nil {
		panic("gzip 收尾失败: " + err.Error())
	}
	return buf.Bytes()
}

// TestGzipContentEncodingMissHitReplay 缺口#7：MISS 与 HIT 的头+体一致回放，
// 上游恰被请求 1 次（第二次命中本地缓存）。
func TestGzipContentEncodingMissHitReplay(t *testing.T) {
	var hits int
	srv := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", itoa(len(gzipReplayBody)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(gzipReplayBody)
	})
	hostPort := strings.TrimPrefix(srv.URL, "http://")
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.Match = `http://` + regexp.QuoteMeta(hostPort) + `/`
	})

	// web 目标以代理形态驱动：目标完整 URL 放在请求路径里（/<目标URL>）
	fetch := func() *httptest.ResponseRecorder {
		req := requestTo("http://registry.proxycache.test/" + srv.URL + "/assets/app.js")
		req.Header.Set("Accept-Encoding", "gzip")
		return runServe(env.srv, req)
	}

	// MISS：原始压缩流原样透传 + 落盘
	miss := fetch()
	if miss.Code != http.StatusOK {
		t.Fatalf("MISS Code = %d, want 200, body: %s", miss.Code, miss.Body.String())
	}
	if got := miss.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("MISS Content-Encoding = %q, want gzip", got)
	}
	if got := miss.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("MISS Content-Type = %q, want text/plain", got)
	}
	if !bytes.Equal(miss.Body.Bytes(), gzipReplayBody) {
		t.Fatalf("MISS body 必须是原始压缩字节（长度 got=%d want=%d）——代理不得解压/转存", miss.Body.Len(), len(gzipReplayBody))
	}
	if got := miss.Header().Get("X-Cache"); got == "HIT" {
		t.Errorf("首次请求应为 MISS，实际 X-Cache=%q", got)
	}

	// HIT：头+体与 MISS 完全一致（Content-Encoding 原样回放，body 逐字节同一份）
	hit := fetch()
	if hit.Code != http.StatusOK {
		t.Fatalf("HIT Code = %d, want 200", hit.Code)
	}
	if got := hit.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("第二次请求应命中缓存 X-Cache=HIT，实际 %q", got)
	}
	if got := hit.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("HIT Content-Encoding = %q, want gzip（回放必须保留压缩头）", got)
	}
	if got := hit.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("HIT Content-Type = %q, want text/plain", got)
	}
	if !bytes.Equal(hit.Body.Bytes(), gzipReplayBody) {
		t.Fatalf("HIT body 与 MISS 压缩原文不一致（长度 got=%d want=%d）", hit.Body.Len(), len(gzipReplayBody))
	}
	if got, want := hit.Header().Get("Content-Length"), itoa(len(gzipReplayBody)); got != want {
		t.Errorf("HIT Content-Length = %q, want %q", got, want)
	}
	if hits != 1 {
		t.Errorf("上游应恰被请求 1 次（第二次走本地缓存），实际 %d", hits)
	}
}
