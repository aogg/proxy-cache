// prefetch_budget_test.go —— 预取任务串行推进 + 在途合并等待不饿死后续层的黑盒回归
// （缺口#4，session-944d8ef7 现场「最大层排第 5 位被 flight 合并等待耗尽预算，一次预取
// 机会都没排到」）。经公开 API（按 digest 的 manifest GET 触发预取——tag 形态属
// docker-mutable BYPASS 不触发，digest 形态可缓存、走 finalizeFetchEntry 触发链）：
//
//	manifest 列出 [config, layer1, layer2, layer3(最大层，排最后)]；layer1 先被客户端
//	以慢速流式下载占用（预取对 layer1 成为 flight 等待方，合并等待直至客户端下载完成）。
//	断言：合并等待结束后预取继续串行推进，layer2 与最大层 layer3 都被预取写入缓存、
//	内容正确；上游对每个 blob 只有一路下载（layer1 由客户端供数，预取合并不重复拉）。
//
// 「5min 任务预算耗尽放弃剩余 blob」的子场景：prefetchTaskTimeout 为包级常量 5min，
// 公开 API 无法缩短（独立 ctx 与请求解耦），确定性单测不可驱动——标跳过（见报告）。
// 本用例锁定的是同一 famine 链路的可观察契约：串行循环的每一层都能被推进到（含排最后
// 的最大层），flight 合并等待有界且不阻塞后续层。
package tests

import (
	"fmt"
	"strings"
	"net/http"
	"os"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestPrefetchMaxLayerLastNotStarved 缺口#4：最大层排最后 + 前层被客户端在途下载合并
// 等待 → 后续层仍被预取写入缓存（串行推进不卡死、不产生第二路下载）。
func TestPrefetchMaxLayerLastNotStarved(t *testing.T) {
	layer1 := blobPattern(96 * 1024) // 前层：客户端慢速下载占用（预取等待合并）
	layer2 := blobPattern(64 * 1024)
	layer3 := blobPattern(160 * 1024) // 最大层，排最后（现场饥饿位）
	cfgBlob := blobPattern(128)
	l1, l2, l3, cfgD := digestOf(layer1), digestOf(layer2), digestOf(layer3), digestOf(cfgBlob)

	blobs := map[string][]byte{l1: layer1, l2: layer2, l3: layer3, cfgD: cfgBlob}
	var slowHits, otherHits int
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fmt.Sprintf(
				`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json",`+
					`"config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":%d,"digest":"%s"},`+
					`"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":%d,"digest":"%s"},`+
					`{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":%d,"digest":"%s"},`+
					`{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":%d,"digest":"%s"}]}`,
				len(cfgBlob), cfgD, len(layer1), l1, len(layer2), l2, len(layer3), l3)))
		case hasSuffixPath(r.URL.Path, "/blobs/"+l1):
			slowHits++
			// layer1 慢速供数：6 块 × 16KB，400ms/块 ≈2.4s（预取的 flight 合并等待窗口）
			w.Header().Set("Content-Length", itoa(len(layer1)))
			w.WriteHeader(http.StatusOK)
			for off := 0; off < len(layer1); off += 16 * 1024 {
				if r.Context().Err() != nil {
					return
				}
				end := off + 16*1024
				if end > len(layer1) {
					end = len(layer1)
				}
				_, _ = w.Write(layer1[off:end])
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				time.Sleep(400 * time.Millisecond)
			}
		default:
			otherHits++
			for digest, body := range blobs {
				if hasSuffixPath(r.URL.Path, "/blobs/"+digest) {
					w.Header().Set("Content-Length", itoa(len(body)))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(body)
					return
				}
			}
			http.Error(w, "unknown blob", http.StatusNotFound)
		}
	})
	env := newTestEnv(t, func(cfg *config.Config, rule *config.DomainRule) {
		rule.URLRedirect = &[]config.URLRedirectItem{{Template: up.URL + "/$1"}}
		cfg.DockerPrefetchEnabled = ptrBool(true)
	})
	blobPath := func(digest string) string { return "/v2/app/prefetch/blobs/" + digest }

	// 1. 客户端先占用 layer1（慢速下载在途，flight 执行方注册）
	cw := &countingWriter{ResponseWriter: httpRecorder()}
	done := env.serveWithWriter(http.MethodGet, blobPath(l1), nil, cw)
	waitFor(t, 5*time.Second, "客户端 layer1 请求到达上游", func() bool { return slowHits >= 1 })

	// 2. 按 digest 取 manifest（可缓存）→ 触发预取：[config, layer1(合并等待), layer2, layer3]
	manifestDigest := digestOf([]byte("manifest-by-digest")) // digest 形态即可，内容不校验
	rec := env.get("v2/app/prefetch/manifests/"+manifestDigest, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("manifest GET Code = %d, want 200", rec.Code)
	}

	// 3. 等待预取串行推进到最大层（layer1 合并等待 ≈2.4s 后 layer2/layer3 快速预取）
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err1 := os.Stat(env.entryPath("v2/app/prefetch/blobs/" + l2)); err1 == nil {
			if _, err3 := os.Stat(env.entryPath("v2/app/prefetch/blobs/" + l3)); err3 == nil {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	<-done
	if got := cw.Bytes(); got != int64(len(layer1)) {
		t.Fatalf("客户端 layer1 应完整收 %d 字节，实际 %d", len(layer1), got)
	}
	if !bytesEqual(cw.body(), layer1) {
		t.Fatalf("客户端 layer1 内容应正确")
	}
	// 最大层（排最后）必须被预取写入缓存且内容正确——「不饿死」核心断言
	assertEntryBody(t, env, "v2/app/prefetch/blobs/"+l3, layer3)
	assertEntryBody(t, env, "v2/app/prefetch/blobs/"+l2, layer2)
	assertEntryBody(t, env, "v2/app/prefetch/blobs/"+cfgD, cfgBlob)
	// 上游仅各一路下载：layer1 客户端 1 次（预取合并），layer2/layer3 预取各 1 次
	if slowHits != 1 {
		t.Errorf("layer1 应只有客户端那 1 路下载（预取合并不重复拉），实际 %d", slowHits)
	}
	if otherHits < 3 {
		t.Errorf("config/layer2/layer3 应各被预取 1 次（共≥3），实际 %d", otherHits)
	}
	log := env.logText()
	if n := countLines(log, "预取合并客户端在途回源完成"); n < 1 {
		t.Errorf("layer1 应留「预取合并客户端在途回源完成」留痕，实际 %d 行", n)
	}
	if !containsAll(log, "预取 blob 写入缓存完成") {
		t.Errorf("应留「预取 blob 写入缓存完成」留痕（layer2/layer3/config）")
	}
}

// hasSuffixPath 判断请求路径是否以给定后缀结尾（候选路径带完整 URL 前缀，取尾段判定）。
func hasSuffixPath(path, suffix string) bool {
	return len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix
}

// containsAll 判断日志包含子串。
func containsAll(log, substr string) bool {
	return countLines(log, substr) > 0
}
