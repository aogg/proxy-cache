package logging

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestRenderDownloadClientVsUpstream 面板的核心可读性诉求：客户端进度与代理侧进度分成
// 两行，多源并发时代理侧领先量单独标注（只看一个 bytes 分不清是谁慢）。
func TestRenderDownloadClientVsUpstream(t *testing.T) {
	b := DownloadBlock{
		ID: 3, Mode: "resume", Image: "adockero/opencode-to-openai",
		Digest: "sha256:6e4673cf7d63579467ba6ee2e885b161cc66975f6fbf7bb4b3c67998e0a0d547",
		Method: "GET", Target: "/v2/adockero/opencode-to-openai/blobs/sha256:6e46…",
		Via: "", Total: 356731507, TotalSrc: "upstream",
		ClientBytes: 58216448, UpstreamBytes: 131072000, ClientState: "ok",
		SpeedBPS: 75997, UpstreamSpeedBPS: 100000, AvgBPS: 105000,
		Elapsed: 572 * time.Second,
	}
	out := RenderDownload(b)
	lines := strings.Split(out, "\n")

	if !strings.HasPrefix(lines[0], PanelSeparator+" 下载 dl#3 · resume "+PanelSeparator) {
		t.Fatalf("首行不是 15 个 = 包裹的标题: %q", lines[0])
	}
	if lines[len(lines)-1] != PanelSeparator {
		t.Fatalf("末行不是分隔线: %q", lines[len(lines)-1])
	}
	clientLine := findPanelLine(lines, "客户端")
	proxyLine := findPanelLine(lines, "代理")
	if clientLine == "" || !strings.Contains(clientLine, "16%") || !strings.Contains(clientLine, "55.52MB / 340.21MB") {
		t.Fatalf("客户端行不符: %q", clientLine)
	}
	if proxyLine == "" || !strings.Contains(proxyLine, "36%") || !strings.Contains(proxyLine, "125MB") {
		t.Fatalf("代理行不符: %q", proxyLine)
	}
	if !strings.Contains(proxyLine, "多源已收（领先 69.48MB）") {
		t.Fatalf("代理行缺少领先量标注: %q", proxyLine)
	}
	// 半截行：resume 场景必须给出起点与缺口
	if l := findPanelLine(lines, "半截"); l != "" {
		t.Fatalf("本用例未设置 Partial，不应出现半截行: %q", l)
	}
	b.Partial = PartialInfo{Present: true, Bytes: 58216448}
	if out := RenderDownload(b); !strings.Contains(out, "半截    55.52MB（起点 0B · 缺口 无）") {
		t.Fatalf("半截行不符:\n%s", out)
	}
}

// TestRenderDownloadPrefetchNoClient 预取块没有客户端：客户端行必须写「无客户端」而不是 0。
func TestRenderDownloadPrefetchNoClient(t *testing.T) {
	out := RenderDownload(DownloadBlock{
		ID: 11, Mode: "prefetch", Image: "foxhui/webai-2api",
		Digest: "sha256:c1a4f0e9d7a0c", Total: 209715200, TotalSrc: "manifest",
		UpstreamBytes: 163577856, ClientBytes: 0,
	})
	if !strings.Contains(out, "—（后台预取，无客户端）") {
		t.Fatalf("预取块客户端行不符:\n%s", out)
	}
	if strings.Contains(out, "多源已收") {
		t.Fatalf("预取块不应出现「多源已收」领先标注:\n%s", out)
	}
}

// TestRenderChunkShowsRemote 分块行必须写明「现在下载的远端是哪个」（用户明确要求）。
func TestRenderChunkShowsRemote(t *testing.T) {
	out := RenderDownload(DownloadBlock{
		ID: 3, Mode: "resume", Total: 356731507, TotalSrc: "upstream",
		ClientBytes: 58216448, UpstreamBytes: 131072000,
		ChunkTotal: 3, ChunkAlive: 2, Cursor: 58216448,
		Chunks: []ChunkEntry{
			{N: 1, Start: 0, End: 32400998, HasEnd: true, Remote: "docker.1panel.live",
				State: "EOF", Got: 32400998, SpeedBPS: 75776},
			{N: 2, Start: 32400999, End: 64801997, HasEnd: true, Remote: "docker.1ms.run",
				State: "下载中", Got: 31876710, SpeedBPS: 83968},
			{N: 3, Start: 64801998, End: 97202996, HasEnd: true, Remote: "hk.gh-proxy.org",
				State: "剪枝", Reason: "verify-mismatch", At: 103908966},
		},
	})
	for _, want := range []string{
		"#1 [0B – 30.9MB]  ← docker.1panel.live  ● 74KB/s  已收 30.9MB  EOF",
		"#2 [30.9MB – 61.8MB]  ← docker.1ms.run  ● 82KB/s  已收 30.4MB  下载中",
		"#3 [61.8MB – 92.7MB]  ← hk.gh-proxy.org  ● —  已收 0B  剪枝 reason=verify-mismatch @99.1MB",
		"拼接    连续 [0 – 55.52MB] · 下一块 55.52MB · 缺口 无",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("缺少分块行 %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "单源直连") {
		// 多源时不出现单源行（负向断言）
	} else {
		t.Fatalf("多源场景不应出现「单源直连」:\n%s", out)
	}
}

// TestRenderChunkFolded 超过 8 个源折叠成一行统计。
func TestRenderChunkFolded(t *testing.T) {
	b := DownloadBlock{ID: 1, Mode: "client", Total: 1000,
		Chunks: []ChunkEntry{{N: 1, Start: 0, HasEnd: true, End: 100, Remote: "a.example", State: "下载中"}},
		ChunksFolded: 7, ChunkFoldedIdle: 5, ChunkFoldedEOF: 2, ChunkPruned: 2,
		ChunkTotal: 15, Cursor: 1000}
	out := RenderDownload(b)
	if !strings.Contains(out, "… 其余 7 源（下载中 5 · EOF 2 · 剪枝 2）") {
		t.Fatalf("折叠行不符:\n%s", out)
	}
}

// TestRenderSummary 总总结块：活跃数按来源分布、吞吐、本轮计数。
func TestRenderSummary(t *testing.T) {
	out := RenderSummary(SummaryBlock{
		Time: time.Date(2026, 11, 21, 18, 47, 17, 0, time.Local),
		Active: 4, Client: 1, Ridealong: 1, Resume: 1, Prefetch: 1,
		ClientBPS: 75997, PrefetchBPS: 8808038, TotalBPS: 8884035,
		New: 2, Done: 1, Failed: 1, CacheWritten: 566231040,
		Details: []string{"dl#3 resume 16%", "dl#7 client 24%"},
	})
	for _, want := range []string{
		PanelSeparator + " 汇总 18:47:17 " + PanelSeparator,
		"活跃    4 个下载（client 1 · 搭车 1 · 续传 1 · 预取 1）",
		"吞吐    client 74.22KB/s · prefetch 8.4MB/s · 合计 8.47MB/s",
		"本轮    新登记 2 · 完成 1 · 失败 1 · 写缓存 +540MB",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("缺少汇总行 %q:\n%s", want, out)
		}
	}
}

// TestJSONDownloadKeySet JSON 模式键集合：全英文短键、数值保持原始字节（不 humanize）。
func TestJSONDownloadKeySet(t *testing.T) {
	line := JSONDownload(DownloadBlock{
		ID: 3, Mode: "resume", Image: "adockero/opencode-to-openai",
		Digest: "sha256:6e4673cf7d63", Req: "7c1a2b3f", Via: "", Total: 356731507, TotalSrc: "upstream",
		ClientBytes: 58216448, UpstreamBytes: 131072000, ClientState: "ok",
		SpeedBPS: 75997, UpstreamSpeedBPS: 100000, AvgBPS: 105000, Elapsed: 572 * time.Second,
		Partial: PartialInfo{Present: true, Bytes: 58216448},
		Polls:   []PollEntry{{N: 1, OK: true, Host: "docker.1panel.live", Status: "206", Cost: "0.31s", Result: "选中"}},
		Chunks:  []ChunkEntry{{N: 1, Start: 0, End: 32400998, HasEnd: true, Remote: "docker.1panel.live", State: "EOF", Got: 32400998, SpeedBPS: 75776}},
		PollTotal: 1, Cursor: 58216448, ChunkTotal: 1, ChunkAlive: 1,
	})
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("JSON 非法: %v\n%s", err, line)
	}
	wantKeys := []string{"type", "ts", "dl", "mode", "image", "digest", "req", "via",
		"total", "total_src", "client_bytes", "upstream_bytes", "percent", "speed_bps",
		"client_speed_bps", "avg_bps", "eta_s", "elapsed_s", "client", "partial",
		"polls", "poll_total", "chunks", "cursor", "gap", "alive", "sources"}
	for _, k := range wantKeys {
		if _, ok := m[k]; !ok {
			t.Fatalf("缺少 JSON 键 %q\n%s", k, line)
		}
	}
	// 原始字节，不做 humanize
	if got := m["client_bytes"].(float64); got != 58216448 {
		t.Fatalf("client_bytes 应为原始字节 58216448，实际 %v", got)
	}
	chunks := m["chunks"].([]any)
	if remote := chunks[0].(map[string]any)["remote"]; remote != "docker.1panel.live" {
		t.Fatalf("chunks[0].remote 不符: %v", remote)
	}
}

// TestJSONSummary json 汇总键集合。
func TestJSONSummary(t *testing.T) {
	line := JSONSummary(SummaryBlock{Active: 2, Client: 1, Prefetch: 1, New: 1, Done: 1,
		ClientBPS: 100, PrefetchBPS: 200, TotalBPS: 300, CacheWritten: 1024})
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("JSON 非法: %v", err)
	}
	for _, k := range []string{"type", "ts", "active", "by_source", "throughput_bps", "new", "done", "failed", "cache_written_bytes"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("缺少 JSON 键 %q: %s", k, line)
		}
	}
}

// findPanelLine 取以标签开头的行（标签后跟空格；中文标签按显示列对齐）。
func findPanelLine(lines []string, label string) string {
	for _, l := range lines {
		if strings.HasPrefix(l, label) {
			return l
		}
	}
	return ""
}
