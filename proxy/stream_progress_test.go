package proxy

// stream_progress_test.go —— 流式透传进度日志（commit 497da64）单测：
//   - humanBytes / humanBytesPerSec 边界格式：不足 1MB 原样字节数（8192→"8192B"）；
//     1MB 起按 1024 进制换算、两位小数去尾零（8.39MB / 342.7MB / 8.00→8MB / 1.21GB）；
//     humanBytesPerSec 的 d<=0（时钟回拨等异常）→ "0B/s"；
//   - 进度日志行为（fetchViaCandidatesStreaming，httptest 慢/快 body，内存 Recorder）：
//     ProgressLogInterval 到期且传输仍在进行 → ≥1 条「流式透传进度」（断言 bytes/speed/
//     elapsed/client 字段存在）；Interval<=0 → 0 条；流在间隔内完成 → 0 条。

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxy-cache/config"
)

// TestHumanBytes 表驱动验证字节格式化：<1MB 原样字节数；1024 进制两位小数去尾零。
func TestHumanBytes(t *testing.T) {
	const mb = 1 << 20
	tests := []struct {
		name string
		in   int64
		want string
	}{
		{name: "零字节", in: 0, want: "0B"},
		{name: "1MB以下原样字节数", in: 8192, want: "8192B"},
		{name: "1MB边界前一字节仍是B档", in: mb - 1, want: "1048575B"},
		{name: "整1MB去尾零", in: mb, want: "1MB"},
		{name: "整8MB去两位尾零", in: 8 * mb, want: "8MB"},
		{name: "8.39MB两位小数", in: 8797553, want: "8.39MB"},
		{name: "342.7MB一位小数去尾零", in: 359346995, want: "342.7MB"},
		{name: "GB档1.21GB", in: 1299227607, want: "1.21GB"},
		{name: "GB档整1GB去尾零", in: 1 << 30, want: "1GB"},
		{name: "TB档1.5TB", in: 1649267441664, want: "1.5TB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := humanBytes(tt.in); got != tt.want {
				t.Errorf("humanBytes(%d) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestHumanBytesPerSec 表驱动验证速率格式化：折算 B/s 后复用 humanBytes；d<=0 → "0B/s"。
func TestHumanBytesPerSec(t *testing.T) {
	const mb = 1 << 20
	tests := []struct {
		name  string
		bytes int64
		d     time.Duration
		want  string
	}{
		{name: "每秒8192字节", bytes: 8192, d: time.Second, want: "8192B/s"},
		{name: "每秒2MB", bytes: 2 * mb, d: time.Second, want: "2MB/s"},
		{name: "每秒13.2MB", bytes: 13841203, d: time.Second, want: "13.2MB/s"},
		{name: "两秒4MB折算每秒2MB", bytes: 4 * mb, d: 2 * time.Second, want: "2MB/s"},
		{name: "零字节", bytes: 0, d: time.Second, want: "0B/s"},
		{name: "时长为零兜底", bytes: 1000, d: 0, want: "0B/s"},
		{name: "时长为负兜底", bytes: 1000, d: -time.Second, want: "0B/s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := humanBytesPerSec(tt.bytes, tt.d); got != tt.want {
				t.Errorf("humanBytesPerSec(%d, %s) = %q, want %q", tt.bytes, tt.d, got, tt.want)
			}
		})
	}
}

// TestStreamingProgressLogEmitted 限速慢 body + ProgressLogInterval=30ms：
// 传输期间必须输出 ≥1 条「流式透传进度」，且进度行带 bytes/speed/elapsed/client 字段。
func TestStreamingProgressLogEmitted(t *testing.T) {
	data := blobPattern(384 * 1024) // 12 块 × 32KB，每 20ms 一块 ≈ 240ms 传输期
	var hits atomic.Int64
	up := slowChunkUpstream(t, data, 32<<10, 20*time.Millisecond, &hits)
	defer up.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	// 单候选（候选数=1 恒不升级）+ 关闭慢速升级，隔离进度日志行为
	eff := slowEff([]config.URLRedirectItem{{Template: up.URL + "/$1"}}, config.RuleTypeDocker, func(o *config.Options) {
		o.SlowDownloadSpeed = 0
		o.ProgressLogInterval = 30 * time.Millisecond
	})

	rec, _, committed, err := runStreamFetch(t, s, eff, slowPath, "", nil)
	if err != nil {
		t.Fatalf("慢速流应完整返回: %v", err)
	}
	if !committed {
		t.Error("流式回源应已 commit")
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Errorf("客户端应收到完整 body（got %d 字节, want %d）", rec.Body.Len(), len(data))
	}

	lines := progressLines(logBuf.String())
	if len(lines) < 1 {
		t.Fatalf("传输期超过进度间隔，应输出 ≥1 条进度日志，实际日志:\n%s", logBuf.String())
	}
	// 断言最后一条进度行的字段存在（slog 文本格式 key=value）
	last := lines[len(lines)-1]
	for _, field := range []string{"bytes=", "speed=", "elapsed=", "client="} {
		if !strings.Contains(last, field) {
			t.Errorf("进度行应包含字段 %q，实际: %s", field, last)
		}
	}
}

// TestStreamingProgressLogDisabled ProgressLogInterval=0（显式关闭）：
// 同样的慢速流，整条传输 0 条进度日志。
func TestStreamingProgressLogDisabled(t *testing.T) {
	data := blobPattern(384 * 1024)
	var hits atomic.Int64
	up := slowChunkUpstream(t, data, 32<<10, 20*time.Millisecond, &hits)
	defer up.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	eff := slowEff([]config.URLRedirectItem{{Template: up.URL + "/$1"}}, config.RuleTypeDocker, func(o *config.Options) {
		o.SlowDownloadSpeed = 0
		o.ProgressLogInterval = 0 // 显式关闭
	})

	if _, _, _, err := runStreamFetch(t, s, eff, slowPath, "", nil); err != nil {
		t.Fatalf("慢速流应完整返回: %v", err)
	}
	if lines := progressLines(logBuf.String()); len(lines) != 0 {
		t.Errorf("Interval=0 应关闭进度日志，实际输出 %d 条: %v", len(lines), lines)
	}
}

// TestStreamingProgressLogFastStreamNoLog 流在进度间隔内完成：
// 快上游一次性送完全部 body，整条流远短于间隔 → 0 条进度日志（最终成败日志已有留痕）。
func TestStreamingProgressLogFastStreamNoLog(t *testing.T) {
	data := blobPattern(256 * 1024)
	var hits atomic.Int64
	up := fastFullUpstream(t, data, &hits)
	defer up.Close()

	var logBuf bytes.Buffer
	s := newSlowTestServer(t, &logBuf)

	eff := slowEff([]config.URLRedirectItem{{Template: up.URL + "/$1"}}, config.RuleTypeDocker, func(o *config.Options) {
		o.SlowDownloadSpeed = 0
		o.ProgressLogInterval = 500 * time.Millisecond // 间隔远大于整条流的耗时
	})

	rec, _, _, err := runStreamFetch(t, s, eff, slowPath, "", nil)
	if err != nil {
		t.Fatalf("快流应完整返回: %v", err)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Errorf("客户端应收到完整 body（got %d 字节, want %d）", rec.Body.Len(), len(data))
	}
	if lines := progressLines(logBuf.String()); len(lines) != 0 {
		t.Errorf("流在间隔内完成应输出 0 条进度日志，实际 %d 条: %v", len(lines), lines)
	}
}
