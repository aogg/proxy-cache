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
	rec.record(logging.PollEntry{OK: true, Host: "docker.1panel.live", Status: "206", Cost: "0.31s",
		Result: logging.PollResultSelected, Reason: "第一个通过 success-check=content 的候选（状态码 206）"})
	rec.record(logging.PollEntry{Host: "docker.1ms.run", Cost: "5.00s",
		Result: logging.PollResultFailed, Reason: "超时：probe-timeout（候选探测预算 5s 内响应头未就绪）"})
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
		"轮询    共 2 · 成功 1 · 失败 1 · 取消 0",
		"#1 ✔ docker.1panel.live 206 0.31s 选中  reason=第一个通过 success-check=content 的候选",
		"#2 ✘ docker.1ms.run — 5.00s 失败  reason=超时：probe-timeout",
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

// //////////////////  轮询段全量输出（不折叠、不截断）  //////////////////////////////////////

// TestPanelRecorderKeepsAllPolls 记录器不再设上限：旧版 maxPanelPolls=64 会在候选多 /
// 重试多（候选 11 × item 重试 4 × 整轮 2 ≈ 88 条）时把第 65 条起悄悄丢掉，正是「信息
// 丢失」的一部分。条目数天然有界（候选数 × 尝试数 + 未放行补记），无需丢弃。
func TestPanelRecorderKeepsAllPolls(t *testing.T) {
	rec := newPanelRecorder()
	const total = 120 // 故意超过旧版上限 64
	for i := 0; i < total; i++ {
		if n := rec.record(logging.PollEntry{Host: "h.example", Result: logging.PollResultFailed}); n != i+1 {
			t.Fatalf("第 %d 条的面板序号应为 %d，实际 %d（序号必须跨轮次递增）", i+1, i+1, n)
		}
	}
	if got := len(rec.snapshot()); got != total {
		t.Fatalf("记录器应保留全部 %d 条（不截断），实际 %d 条", total, got)
	}
}

// TestBuildDownloadBlockListsAllPolls 每个面板周期都把全部候选尝试列出来：第二帧没有任何
// 新增尝试时也照样全量输出（旧版这时会折叠成「本轮无新增 · 累计 N 次 · 最近 …」），
// 且超过 8 条也不截断。
func TestBuildDownloadBlockListsAllPolls(t *testing.T) {
	s := panelTestServer(t)
	reg := s.progressOrInit()
	now := time.Now()
	prog := reg.register(dockerLogSourceClient, "adockero/tinc",
		"sha256:17977f56026a1758cabf2eea1e3430a4269a6724ab7249f48b97efe2d60ee11d",
		"GET", "/v2/adockero/tinc/blobs/sha256:17977f56026a", 1000, 45880718, now)
	prog.setPanelMeta("", blobTotalSourceUpstream, "a1b2c3d4")

	rec := newPanelRecorder()
	rec.record(logging.PollEntry{Host: "registry-proxy-cache.linkease.net:5480",
		Result: logging.PollResultFailed, Reason: "自引用候选（展开后指向本次请求自身），已跳过本节候选"})
	rec.record(logging.PollEntry{Host: "mirror.houlang.cloud", Status: "200", Cost: "89ms",
		Result: logging.PollResultFailed, HardFail: "html-error-page",
		Reason: "hard_fail=html-error-page：状态码 200 未过 success-check=content"})
	rec.record(logging.PollEntry{OK: true, Host: "docker.1ms.run", Status: "206", Cost: "4.304s",
		Result: logging.PollResultSelected, Reason: "第一个通过 success-check=content 的候选（状态码 206）"})
	for i := 0; i < 10; i++ { // 超过 8 条，验证不截断
		rec.record(logging.PollEntry{Host: "docker.1panel.live", Cost: "213ms",
			Result: logging.PollResultCanceled, Reason: "其他候选胜出（#3 docker.1ms.run 已命中）"})
	}
	prog.attachRecorder(rec)

	first, _ := s.buildDownloadBlock(prog, now)
	if len(first.Polls) != 13 || first.PollTotal != 13 {
		t.Fatalf("首帧应带全部 13 条候选尝试，实际 polls=%d poll_total=%d", len(first.Polls), first.PollTotal)
	}
	// 第二帧：没有新增尝试，也必须全量输出（不折叠、不截断）
	second, _ := s.buildDownloadBlock(prog, now.Add(time.Second))
	if len(second.Polls) != 13 {
		t.Fatalf("第二帧应仍输出全部 13 条（不折叠），实际 %d 条", len(second.Polls))
	}
	out := logging.RenderDownload(second)
	if strings.Contains(out, "本轮无新增") {
		t.Fatalf("不应再出现「本轮无新增」折叠行:\n%s", out)
	}
	for _, want := range []string{
		"轮询    共 13 · 成功 1 · 失败 2 · 取消 10",
		"#3 ✔ docker.1ms.run 206 4.304s 选中  reason=第一个通过 success-check=content 的候选",
		"#13 ✘ docker.1panel.live — 213ms 已取消  reason=其他候选胜出（#3 docker.1ms.run 已命中）",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("面板输出缺少 %q:\n%s", want, out)
		}
	}
	prog.finish("完成")
	prog.unregister()
}

// TestFinishedBlockCarriesPolls 收尾块同样要带全部候选尝试：下载一结束就只剩进度数字的话，
// 「谁试过、谁失败、谁被取消、为什么」就永久丢了（用户抱怨的信息丢失点）。
func TestFinishedBlockCarriesPolls(t *testing.T) {
	s := panelTestServer(t)
	reg := s.progressOrInit()
	prog := reg.register(dockerLogSourceClient, "adockero/tinc", "sha256:17977f56026a",
		"GET", "/v2/adockero/tinc/blobs/sha256:17977f56026a", 45880718, 45880718, time.Now())
	rec := newPanelRecorder()
	rec.record(logging.PollEntry{OK: true, Host: "docker.1ms.run", Status: "206", Cost: "4.304s",
		Result: logging.PollResultSelected, Reason: "第一个通过 success-check=content 的候选（状态码 206）"})
	rec.record(logging.PollEntry{Host: "docker.1panel.live", Cost: "213ms",
		Result: logging.PollResultCanceled, Reason: "其他候选胜出（#1 docker.1ms.run 已命中）"})
	prog.attachRecorder(rec)
	prog.finish("完成")
	prog.unregister()

	_, _, finished := reg.panelSnapshot(time.Now())
	if len(finished) != 1 {
		t.Fatalf("应有一个收尾块，实际 %d", len(finished))
	}
	if len(finished[0].Polls) != 2 || finished[0].PollTotal != 2 {
		t.Fatalf("收尾块应带全部 2 条候选尝试，实际 polls=%d poll_total=%d",
			len(finished[0].Polls), finished[0].PollTotal)
	}
	out := logging.RenderDownload(finished[0])
	if !strings.Contains(out, "轮询    共 2 · 成功 1 · 失败 0 · 取消 1") {
		t.Fatalf("收尾块轮询段汇总行不符:\n%s", out)
	}
	if !strings.Contains(out, "reason=其他候选胜出（#1 docker.1ms.run 已命中）") {
		t.Fatalf("收尾块轮询段缺少取消原因:\n%s", out)
	}
}
