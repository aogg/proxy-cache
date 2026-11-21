package proxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"proxy-cache/config"
	"proxy-cache/logging"
)

// panelTestServer 构造一个 line 模式的 Server（无缓存、stderr 之外的面板输出由测试注入）。
func panelTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Log.Type = config.LogTypeLine
	iv := config.Duration(30 * time.Second)
	cfg.Log.PanelInterval = &iv
	if err := cfg.Validate(); err != nil {
		t.Fatalf("配置校验失败: %v", err)
	}
	s, err := New(cfg, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.progressOrInit().panelEnabled.Store(true)
	return s
}

// TestMergerPanelSnapshotChunks 分块快照：每源一行带远端 host、区间、状态与速率基线。
func TestMergerPanelSnapshotChunks(t *testing.T) {
	now := time.Now()
	m := newSlowMerger([]*slowSource{
		{name: "https://docker.1panel.live/v2/x/blobs/sha256:aa", start: 0, readPos: 32400998, end: 32400998, eof: true},
		{name: "https://docker.1ms.run/v2/x/blobs/sha256:aa", start: 32400999, readPos: 55000000, end: 64801997},
		{name: "https://hk.gh-proxy.org/v2/x/blobs/sha256:aa", start: 64801998, readPos: 64801998, end: 97202996, pruned: "verify-mismatch"},
	}, 58216448, 97202996, 8<<20, nil)

	chunks, cursor, gap, _, alive, total, pruned, _ := m.panelSnapshot(now)
	if total != 3 || alive != 1 || pruned != 1 {
		t.Fatalf("源统计不符: total=%d alive=%d pruned=%d", total, alive, pruned)
	}
	if cursor != 58216448 || gap != 0 {
		t.Fatalf("游标/缺口不符: cursor=%d gap=%d", cursor, gap)
	}
	if chunks[0].Remote != "docker.1panel.live" || chunks[0].State != "EOF" || chunks[0].Got != 32400998 {
		t.Fatalf("源#1 快照不符: %+v", chunks[0])
	}
	if chunks[1].Remote != "docker.1ms.run" || chunks[1].State != "下载中" {
		t.Fatalf("源#2 快照不符: %+v", chunks[1])
	}
	if chunks[2].State != "剪枝" || chunks[2].Reason != "verify-mismatch" {
		t.Fatalf("源#3 快照不符: %+v", chunks[2])
	}
	if !chunks[0].HasEnd || chunks[0].End != 32400998 {
		t.Fatalf("源#1 区间终点不符: %+v", chunks[0])
	}
	// 第二次快照：源#2 readPos 未变 → 速率为 0（基线由首轮建立）
	chunks2, _, _, _, _, _, _, _ := m.panelSnapshot(now.Add(10 * time.Second))
	if chunks2[1].SpeedBPS != 0 {
		t.Fatalf("无新增读取时速率应为 0，实际 %v", chunks2[1].SpeedBPS)
	}
}

// TestPanelRegistryFinishedBlock 收尾块：面板模式下带结果的下载注销后进入收尾队列，
// 被 panelSnapshot 取走一次即清空（收尾块每个下载只输出一次）。
func TestPanelRegistryFinishedBlock(t *testing.T) {
	s := panelTestServer(t)
	prog := s.progressRegister(dockerLogSourceClient, "foxhui/webai-2api", "sha256:9f2c4a1e", "GET", "/v2/foxhui/webai-2api/blobs/sha256:9f2c4a1e", 0, 500)
	prog.setPanelMeta("", "upstream", "abcd1234")
	prog.addBytes(500)
	prog.finish("完成")
	prog.unregister()

	reg := s.progressOrInit()
	entries, _, finished := reg.panelSnapshot(time.Now())
	if len(entries) != 0 {
		t.Fatalf("注销后不应有活跃条目，实际 %d", len(entries))
	}
	if len(finished) != 1 || finished[0].Result != "完成" {
		t.Fatalf("收尾块不符: %+v", finished)
	}
	if finished[0].Target != "/v2/foxhui/webai-2api/blobs/sha256:9f2c4a1e" {
		t.Fatalf("收尾块应带上请求路径 target, got %q", finished[0].Target)
	}
	// 再取一次：收尾队列已清空
	if _, _, again := reg.panelSnapshot(time.Now()); len(again) != 0 {
		t.Fatalf("收尾块应只输出一次，第二次仍拿到 %d 条", len(again))
	}
}

// TestEmitLogPanelClientVsProxy 端到端：面板一轮输出「每下载一块 + 汇总块」，
// 客户端行与代理行数值不同（多源领先），分块行带远端 host；无变化时整轮静默。
func TestEmitLogPanelClientVsProxy(t *testing.T) {
	s := panelTestServer(t)
	reg := s.progressOrInit()
	now := time.Now()

	// 一条 resume 下载：客户端已收 55.5MB，代理侧（3 源）已收 130MB
	prog := reg.register(dockerLogSourceResume, "adockero/opencode-to-openai",
		"sha256:6e4673cf7d63579467ba6ee2e885b161cc66975f6fbf7bb4b3c67998e0a0d547",
		"GET", "/v2/adockero/opencode-to-openai/blobs/sha256:6e4673cf7d63579467ba6ee2e885b161cc66975f6fbf7bb4b3c67998e0a0d547",
		58216448, 356731507, now)
	prog.setPanelMeta("", blobTotalSourceUpstream, "7c1a2b3f")
	prog.setPartial(58216448, 0, 0)
	rec := newPanelRecorder()
	rec.record(logging.PollEntry{OK: true, Host: "docker.1panel.live", Status: "206", Cost: "0.31s", Result: "选中"})
	rec.record(logging.PollEntry{Host: "docker.1ms.run", Cost: "5.00s", Result: "probe-timeout"})
	prog.attachRecorder(rec)
	m := newSlowMerger([]*slowSource{
		{name: "https://docker.1panel.live/v2/adockero/opencode-to-openai/blobs/sha256:6e46", start: 0, readPos: 32400998, end: 32400998, eof: true},
		{name: "https://docker.1ms.run/v2/adockero/opencode-to-openai/blobs/sha256:6e46", start: 32400999, readPos: 64801997, end: 64801997},
		{name: "https://hk.gh-proxy.org/v2/adockero/opencode-to-openai/blobs/sha256:6e46", start: 64801998, readPos: 97202996, end: 97202996},
	}, 58216448, 356731507, 8<<20, nil)
	prog.attachMerger(m)
	// 代理侧总量 = mergerBase(55.5MB) + Σgot(30.9+30.9+30.9MB) ≈ 148MB（领先客户端）

	var buf bytes.Buffer
	s.SetPanelWriter(&buf)
	s.emitLogPanel(now, false)

	out := buf.String()
	t.Logf("面板真实渲染输出:\n%s", out)
	for _, want := range []string{
		"=============== 下载 dl#",
		"镜像    adockero/opencode-to-openai",
		"请求    GET /v2/adockero/opencode-to-openai/blobs/sha256:6e4673cf7d63579467ba6ee2e885b161cc66975f6fbf7bb4b3c67998e0a0d547",
		"客户端  ▕",
		"代理    ▕",
		"多源已收（领先",
		"轮询    #1 ✔ docker.1panel.live 206 0.31s 选中",
		"#2 ✘ docker.1ms.run — 5.00s probe-timeout",
		"分块    #1 [0B – 30.9MB]  ← docker.1panel.live",
		"#3 [61.8MB – 92.7MB]  ← hk.gh-proxy.org",
		"拼接    连续 [0 – 55.52MB]",
		"=============== 汇总 ",
		"活跃    1 个下载（client 0 · 搭车 0 · 续传 1 · 预取 0）",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("面板输出缺少 %q:\n%s", want, out)
		}
	}
	// 客户端 55.5MB/340.2MB = 16%，代理侧 ≈148MB/340.2MB = 43%（两者必须不同）
	if !strings.Contains(out, "客户端  ▕") || !strings.Contains(out, "16%") {
		t.Fatalf("客户端进度行不符:\n%s", out)
	}
	if !strings.Contains(out, "43%") {
		t.Fatalf("代理侧进度行应为 43%%（领先客户端），实际:\n%s", out)
	}

	// 无变化：整轮静默
	buf.Reset()
	s.emitLogPanel(now.Add(time.Second), false)
	if buf.Len() != 0 {
		t.Fatalf("本轮无变化应静默，实际输出:\n%s", buf.String())
	}
}

// TestEmitLogPanelJSON json 模式：每块一行合法 JSON（download + summary）。
func TestEmitLogPanelJSON(t *testing.T) {
	s := panelTestServer(t)
	reg := s.progressOrInit()
	now := time.Now()
	prog := reg.register(dockerLogSourceClient, "foxhui/webai-2api", "sha256:9f2c4a1e", "GET", "/v2/foxhui/webai-2api/blobs/sha256:9f2c4a1e", 125829120, 524288000, now)
	prog.setPanelMeta("http://v2raya.common-all:20171", blobTotalSourceUpstream, "3f9a2c1d")
	prog.addBytes(0)

	var buf bytes.Buffer
	s.SetPanelWriter(&buf)
	s.emitLogPanel(now, true)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("json 模式应输出 2 行（download + summary），实际 %d:\n%s", len(lines), buf.String())
	}
	for i, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("第 %d 行不是合法 JSON: %v\n%s", i, err, line)
		}
	}
	if !strings.Contains(lines[0], `"via":"http://v2raya.common-all:20171"`) {
		t.Fatalf("download 行未带代理通道: %s", lines[0])
	}
	if !strings.Contains(lines[0], `"method":"GET"`) || !strings.Contains(lines[0], `"target":"/v2/foxhui/webai-2api/blobs/sha256:9f2c4a1e"`) {
		t.Fatalf("download 行应同时带 method 与 target: %s", lines[0])
	}
	if !strings.Contains(lines[1], `"type":"summary"`) {
		t.Fatalf("第二行应为 summary: %s", lines[1])
	}
	t.Logf("JSON 模式真实输出:\n%s", buf.String())
}
