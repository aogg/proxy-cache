package proxy

// slowdownload_unit_test.go —— 慢速升级多源并发续传（slowdownload.go）纯函数与
// 有序合并器 slowMerger 的单测（全部内存假 body / 假 probeResult，不需要真 HTTP）：
//   - parseContentRange：合法（含前缀/空白容忍）与非法串（* 总长、缺斜杠、非数字等）；
//   - slowEscalationTarget：200+Content-Length / 206+Content-Range / 其余状态码与解析失败；
//   - slowEscalationDue：速率阈值边界（等于阈值不触发、略低触发、阈值<=0 恒 false）；
//   - slowEscalationRemaining：已知/未知总长语义；
//   - escalationCandidates / resumeRangeHeader；
//   - validateResumeResponse：206 且起始==offset 通过；200 / 起始不匹配 / Content-Range 缺失拒绝；
//   - slowMerger：多源交错到达仍按绝对偏移严格有序、过期前缀丢弃（整块弹出与跨界截断）、
//     max-behind 剪枝（reason 含 behind）与 error 剪枝、全源耗尽未到 knownTotal 报错、
//     游标到达 knownTotal 正常完成。

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// //////////////////  纯函数  //////////////////////////////////////////////////////////////////////////

// TestParseContentRange 表驱动验证 Content-Range 解析：
// "bytes " 前缀与空白容忍（scheme 名不参与判定）、total 为 "*" 或格式不符时 ok=false。
func TestParseContentRange(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantStart int64
		wantEnd   int64
		wantTotal int64
		wantOK    bool
	}{
		{name: "标准docker形态", in: "bytes 100-999/12345", wantStart: 100, wantEnd: 999, wantTotal: 12345, wantOK: true},
		{name: "零起止单字节区间", in: "bytes 0-0/1", wantStart: 0, wantEnd: 0, wantTotal: 1, wantOK: true},
		{name: "前后空白容忍", in: "  bytes 7-8/9  ", wantStart: 7, wantEnd: 8, wantTotal: 9, wantOK: true},
		{name: "无前缀纯区间也容忍", in: "1-2/10", wantStart: 1, wantEnd: 2, wantTotal: 10, wantOK: true},
		{name: "总长星号不可知不支持", in: "bytes 0-0/*", wantOK: false},
		{name: "区间星号解析失败", in: "bytes */123", wantOK: false},
		{name: "空串", in: "", wantOK: false},
		{name: "缺斜杠", in: "bytes 1-2", wantOK: false},
		{name: "总长非数字", in: "bytes 1-2/abc", wantOK: false},
		{name: "总长为空", in: "bytes 1-2/", wantOK: false},
		{name: "区间非数字", in: "bytes a-b/10", wantOK: false},
		{name: "等号形态不支持", in: "bytes=0-9/10", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, total, ok := parseContentRange(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("parseContentRange(%q) ok = %v, want %v", tt.in, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if start != tt.wantStart || end != tt.wantEnd || total != tt.wantTotal {
				t.Errorf("parseContentRange(%q) = (%d, %d, %d), want (%d, %d, %d)",
					tt.in, start, end, total, tt.wantStart, tt.wantEnd, tt.wantTotal)
			}
		})
	}
}

// TestSlowEscalationTarget 表驱动验证升级目标区间解析：
// 200+Content-Length → (0, CL, CL-1)；206+Content-Range → 起始/区间终点/对象总长；
// 其余状态码、总长不可知或解析失败 → ok=false。
func TestSlowEscalationTarget(t *testing.T) {
	hdr := func(kv ...string) http.Header {
		h := http.Header{}
		for i := 0; i+1 < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}
	tests := []struct {
		name      string
		status    int
		header    http.Header
		wantStart int64
		wantTotal int64
		wantEnd   int64
		wantOK    bool
	}{
		{
			name: "200带Content-Length", status: http.StatusOK,
			header:    hdr("Content-Length", "12345"),
			wantStart: 0, wantTotal: 12345, wantEnd: 12344, wantOK: true,
		},
		{
			name: "206从Content-Range解析", status: http.StatusPartialContent,
			header:    hdr("Content-Range", "bytes 100-999/12345"),
			wantStart: 100, wantTotal: 12345, wantEnd: 999, wantOK: true,
		},
		{
			name: "206零起点", status: http.StatusPartialContent,
			header:    hdr("Content-Range", "bytes 0-4095/8192"),
			wantStart: 0, wantTotal: 8192, wantEnd: 4095, wantOK: true,
		},
		{name: "200缺Content-Length", status: http.StatusOK, header: hdr(), wantOK: false},
		{name: "200非法Content-Length", status: http.StatusOK, header: hdr("Content-Length", "abc"), wantOK: false},
		{name: "200零Content-Length", status: http.StatusOK, header: hdr("Content-Length", "0"), wantOK: false},
		{name: "200负Content-Length", status: http.StatusOK, header: hdr("Content-Length", "-5"), wantOK: false},
		{name: "206缺Content-Range", status: http.StatusPartialContent, header: hdr(), wantOK: false},
		{name: "206总长不可知", status: http.StatusPartialContent, header: hdr("Content-Range", "bytes 0-9/*"), wantOK: false},
		{name: "206零总长", status: http.StatusPartialContent, header: hdr("Content-Range", "bytes 0-9/0"), wantOK: false},
		{name: "404不支持", status: http.StatusNotFound, header: hdr("Content-Length", "10"), wantOK: false},
		{name: "304不支持", status: http.StatusNotModified, header: hdr(), wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, total, end, ok := slowEscalationTarget(tt.status, tt.header)
			if ok != tt.wantOK {
				t.Fatalf("slowEscalationTarget(%d) ok = %v, want %v", tt.status, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if start != tt.wantStart || total != tt.wantTotal || end != tt.wantEnd {
				t.Errorf("slowEscalationTarget(%d) = (start=%d, total=%d, end=%d), want (%d, %d, %d)",
					tt.status, start, total, end, tt.wantStart, tt.wantTotal, tt.wantEnd)
			}
		})
	}
}

// TestSlowEscalationDue 表驱动验证触发判定：窗口平均速率低于阈值才判慢；
// 等于阈值不触发、略低触发；阈值 <=0 恒 false（关闭）；d<=0 按 windowAvgSpeed 兜底 0 处理。
func TestSlowEscalationDue(t *testing.T) {
	tests := []struct {
		name     string
		sent     int64
		elapsed  time.Duration
		speedBPs int64
		want     bool
	}{
		{name: "等于阈值不触发", sent: 100, elapsed: time.Second, speedBPs: 100, want: false},
		{name: "略低于阈值触发", sent: 99, elapsed: time.Second, speedBPs: 100, want: true},
		{name: "高于阈值不触发", sent: 101, elapsed: time.Second, speedBPs: 100, want: false},
		{name: "零阈值关闭恒不触发", sent: 100, elapsed: time.Second, speedBPs: 0, want: false},
		{name: "负阈值关闭恒不触发", sent: 100, elapsed: time.Second, speedBPs: -5, want: false},
		{name: "零字节且阈值正触发", sent: 0, elapsed: time.Second, speedBPs: 1, want: true},
		{name: "时长零按零速率兜底", sent: 1000000, elapsed: 0, speedBPs: 1, want: true},
		{name: "时长负且阈值零不触发", sent: 100, elapsed: -time.Second, speedBPs: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slowEscalationDue(tt.sent, tt.elapsed, tt.speedBPs); got != tt.want {
				t.Errorf("slowEscalationDue(%d, %s, %d) = %v, want %v", tt.sent, tt.elapsed, tt.speedBPs, got, tt.want)
			}
		})
	}
}

// TestSlowEscalationRemaining 表驱动验证剩余字节语义：已知总长 - 当前绝对位置；
// knownTotal<=0（总长不可知）返回 -1 表示永不满足触发条件；越过总长可为负。
func TestSlowEscalationRemaining(t *testing.T) {
	tests := []struct {
		name        string
		knownTotal  int64
		winnerStart int64
		consumed    int64
		want        int64
	}{
		{name: "200胜者从头消费", knownTotal: 1000, winnerStart: 0, consumed: 100, want: 900},
		{name: "206胜者带起始偏移", knownTotal: 1000, winnerStart: 200, consumed: 300, want: 500},
		{name: "恰好消费完为0", knownTotal: 1000, winnerStart: 0, consumed: 1000, want: 0},
		{name: "总长不可知返回-1", knownTotal: 0, winnerStart: 0, consumed: 100, want: -1},
		{name: "负总长同样返回-1", knownTotal: -5, winnerStart: 0, consumed: 0, want: -1},
		{name: "越过总长为负", knownTotal: 100, winnerStart: 50, consumed: 60, want: -10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slowEscalationRemaining(tt.knownTotal, tt.winnerStart, tt.consumed); got != tt.want {
				t.Errorf("slowEscalationRemaining(%d, %d, %d) = %d, want %d",
					tt.knownTotal, tt.winnerStart, tt.consumed, got, tt.want)
			}
		})
	}
}

// TestEscalationCandidates 验证续传候选 = 排除胜者且保持原顺序。
func TestEscalationCandidates(t *testing.T) {
	candidates := []candidate{{url: "https://a.example.com"}, {url: "https://b.example.com"}, {url: "https://c.example.com"}}

	others := escalationCandidates(candidates, "https://b.example.com")
	if len(others) != 2 || others[0].url != "https://a.example.com" || others[1].url != "https://c.example.com" {
		t.Errorf("escalationCandidates 应排除胜者且保序，得到 %v", others)
	}

	all := escalationCandidates(candidates, "https://not-in-list.example.com")
	if len(all) != 3 {
		t.Errorf("胜者不在候选列表时应返回全部候选，得到 %d 个", len(all))
	}

	if got := escalationCandidates(nil, "https://a.example.com"); len(got) != 0 {
		t.Errorf("空候选列表应返回空，得到 %v", got)
	}
}

// TestResumeRangeHeader 验证续传 Range 头构造：bytes={offset}-{end}。
func TestResumeRangeHeader(t *testing.T) {
	if got := resumeRangeHeader(0, 2999); got != "bytes=0-2999" {
		t.Errorf("resumeRangeHeader(0, 2999) = %q, want %q", got, "bytes=0-2999")
	}
	if got := resumeRangeHeader(150, 199); got != "bytes=150-199" {
		t.Errorf("resumeRangeHeader(150, 199) = %q, want %q", got, "bytes=150-199")
	}
}

// TestValidateResumeResponse 表驱动验证续传响应校验：
// 206 且 Content-Range 起始等于请求偏移才通过；200 / 起始不匹配 / Content-Range 缺失拒绝。
func TestValidateResumeResponse(t *testing.T) {
	hdr := func(kv ...string) http.Header {
		h := http.Header{}
		for i := 0; i+1 < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}
	tests := []struct {
		name      string
		status    int
		header    http.Header
		offset    int64
		wantErrIn []string // 非空 = 期望报错且错误信息包含这些子串
	}{
		{
			name: "206且起始等于偏移通过", status: http.StatusPartialContent,
			header: hdr("Content-Range", "bytes 100-199/200"), offset: 100,
		},
		{
			name: "206零偏移通过", status: http.StatusPartialContent,
			header: hdr("Content-Range", "bytes 0-4095/8192"), offset: 0,
		},
		{
			name: "200整段响应拒绝", status: http.StatusOK,
			header: hdr("Content-Length", "200"), offset: 100,
			wantErrIn: []string{"206"},
		},
		{
			name: "404拒绝", status: http.StatusNotFound, header: hdr(), offset: 0,
			wantErrIn: []string{"206"},
		},
		{
			name: "起始不匹配拒绝", status: http.StatusPartialContent,
			header: hdr("Content-Range", "bytes 99-199/200"), offset: 100,
			wantErrIn: []string{"99", "100"},
		},
		{
			name: "Content-Range缺失拒绝", status: http.StatusPartialContent, header: hdr(), offset: 100,
			wantErrIn: []string{"Content-Range"},
		},
		{
			name: "Content-Range不可解析拒绝", status: http.StatusPartialContent,
			header: hdr("Content-Range", "bytes */200"), offset: 100,
			wantErrIn: []string{"Content-Range"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResumeResponse(tt.status, tt.header, tt.offset)
			if len(tt.wantErrIn) == 0 {
				if err != nil {
					t.Fatalf("validateResumeResponse(%d, ..., %d) 意外报错: %v", tt.status, tt.offset, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateResumeResponse(%d, ..., %d) 期望报错，实际为 nil", tt.status, tt.offset)
			}
			for _, want := range tt.wantErrIn {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
				}
			}
		})
	}
}

// //////////////////  有序合并器 slowMerger  ///////////////////////////////////////////////////////////

// blobPattern 生成确定性测试数据：byte(i%251) 模式，任何错位/丢失/重复都可被逐字节比对发现。
func blobPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// sliceReader 把字节切片包成 ReadCloser（假上游活体 body）。
func sliceReader(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(b))
}

// newFakeSource 构造一个假续传源：内存 body + 独立看门狗（timeout=0 不启用计时器，无泄漏），
// 起始绝对偏移为 offset（Range 请求偏移）。
func newFakeSource(name string, body io.ReadCloser, offset int64) *slowSource {
	return &slowSource{
		name: name,
		res: &probeResult{
			url:  name,
			body: body,
			wd:   newProgressWatchdog(context.Background(), 0),
		},
		start:   offset,
		readPos: offset,
	}
}

// sourceState 在合并器锁内读取源状态（与 pump 协程并发安全）。
func sourceState(m *slowMerger, src *slowSource) (pruned string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return src.pruned, src.err
}

// slowBytesReader 限速吐数据的假 body：每次 Read 返回至多 chunk 字节并歇 delay，
// 用于让胜者以可观察的节奏出数据（给其他源的剪枝/报错留出确定的先后顺序）。
type slowBytesReader struct {
	data  []byte
	chunk int
	delay time.Duration
	off   int
}

func (r *slowBytesReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	n := copy(p, r.data[r.off:min(r.off+r.chunk, len(r.data))])
	r.off += n
	return n, nil
}

// TestSlowMergerInterleavedOrderedMerge 多源交错到达仍按绝对偏移严格有序：
// 三个源各持对象 1/3 区间（Range 续传切片），到达顺序由调度决定（天然交错），
// 合并输出必须等于原始数据的完整拼接，游标推进到 knownTotal 正常完成。
func TestSlowMergerInterleavedOrderedMerge(t *testing.T) {
	data := blobPattern(3 * 1024)
	m := newSlowMerger([]*slowSource{
		newFakeSource("src-0", sliceReader(data[0:1024]), 0),
		newFakeSource("src-1024", sliceReader(data[1024:2048]), 1024),
		newFakeSource("src-2048", sliceReader(data[2048:3072]), 2048),
	}, 0, int64(len(data)), 1<<20, quietLogger())

	var out bytes.Buffer
	n, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {})
	if err != nil {
		t.Fatalf("三源分段合并应正常完成: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("run 返回游标 = %d, want %d", n, len(data))
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("交错到达的合并输出必须按绝对偏移严格有序（长度 got=%d want=%d）", out.Len(), len(data))
	}
}

// TestSlowMergerOverlappingSourcesDropsStale 多源重叠（全部持整段 Range）时：
// 只有持有下一期望偏移的源出数据，其余源的落后数据作为过期前缀被丢弃，输出仍等于原始数据。
func TestSlowMergerOverlappingSourcesDropsStale(t *testing.T) {
	data := blobPattern(2048)
	m := newSlowMerger([]*slowSource{
		newFakeSource("winner", sliceReader(data), 0),
		newFakeSource("resume-same-range", sliceReader(data), 0),
	}, 0, int64(len(data)), 1<<20, quietLogger())

	var out bytes.Buffer
	if _, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {}); err != nil {
		t.Fatalf("重叠源合并应正常完成: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("重叠源的合并输出不应重复/乱序（长度 got=%d want=%d）", out.Len(), len(data))
	}
}

// TestSlowMergerStalePrefixTruncated 过期前缀丢弃（白盒预置缓冲）：
// 缓冲中落后 next 的整块被弹出、跨界块只丢过期前缀（截断），剩余部分仍按序写出。
// 预置缓冲 [0,100) 与 [100,200)，游标 next=150 → 输出必须从 150 开始。
func TestSlowMergerStalePrefixTruncated(t *testing.T) {
	data := blobPattern(512)
	src := newFakeSource("winner", sliceReader(data[200:]), 200)
	// 预置两块缓冲（模拟已从上游读入但尚未写出的数据），并声明游标已推进到 150：
	src.buf = append(src.buf, data[0:100], data[100:200])
	src.bufBytes = 200
	src.readPos = 200
	m := newSlowMerger([]*slowSource{src}, 150, int64(len(data)), 1<<20, quietLogger())

	var out bytes.Buffer
	if _, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {}); err != nil {
		t.Fatalf("含过期前缀的合并应正常完成: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data[150:]) {
		t.Errorf("输出应恰好为 data[150:]（过期整块弹出 + 跨界块截断，长度 got=%d want=%d）", out.Len(), len(data)-150)
	}
}

// TestSlowMergerMaxBehindPrune max-behind 剪枝：领先游标越过落后源超过 max-behind 后，
// 该源被关闭（reason 含 behind），合并仍由其余源正常完成。
func TestSlowMergerMaxBehindPrune(t *testing.T) {
	data := blobPattern(1024)
	// 落后源用 io.Pipe 构造「永远不出数据」的活体流；被剪枝时经 res.Close() 关闭解除阻塞
	pipeR, pipeW := io.Pipe()
	defer pipeW.Close() // 写端兜底关闭（本测试不写数据）
	stalled := newFakeSource("stalled", pipeR, 0)
	m := newSlowMerger([]*slowSource{
		newFakeSource("winner", sliceReader(data), 0),
		stalled,
	}, 0, int64(len(data)), 100, quietLogger())

	var out bytes.Buffer
	if _, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {}); err != nil {
		t.Fatalf("落后源被剪枝后其余源应正常完成: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("剪枝不应影响输出正确性（长度 got=%d want=%d）", out.Len(), len(data))
	}
	pruned, _ := sourceState(m, stalled)
	if !strings.Contains(pruned, "behind") {
		t.Errorf("落后源剪枝 reason 应含 behind，得到 %q", pruned)
	}
}

// TestSlowMergerErrorPrune error 剪枝：源读错误后该源被剪枝（reason 含 error），
// 不影响其余源完成合并。胜者用限速 reader（约 240ms 吐完），保证出错源在对象完成前
// 确定性地记录错误并被剪枝（瞬时完成的胜者会让出错源被收尾标 completed 而非 error）。
func TestSlowMergerErrorPrune(t *testing.T) {
	data := blobPattern(1024)
	errSrc := newFakeSource("broken", io.NopCloser(errReader{err: fmt.Errorf("boom-read")}), 0)
	m := newSlowMerger([]*slowSource{
		newFakeSource("winner", io.NopCloser(&slowBytesReader{data: data, chunk: 128, delay: 30 * time.Millisecond}), 0),
		errSrc,
	}, 0, int64(len(data)), 1<<20, quietLogger())

	var out bytes.Buffer
	if _, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {}); err != nil {
		t.Fatalf("出错源被剪枝后其余源应正常完成: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("剪枝不应影响输出正确性（长度 got=%d want=%d）", out.Len(), len(data))
	}
	pruned, srcErr := sourceState(m, errSrc)
	if !strings.Contains(pruned, "error") || !strings.Contains(pruned, "boom-read") {
		t.Errorf("出错源剪枝 reason 应含 error 与原始错误，得到 %q", pruned)
	}
	if srcErr == nil || !strings.Contains(srcErr.Error(), "boom-read") {
		t.Errorf("出错源 src.err 应保留原始错误，得到 %v", srcErr)
	}
}

// errReader 是读即报错的假 body（模拟上游连接中途失败）。
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// TestSlowMergerExhaustedBeforeTotal 全部源耗尽仍未到已知总长：run 返回错误
// （沿用「commit 后读上游失败」语义），且错误含进度（已取/总长）。
func TestSlowMergerExhaustedBeforeTotal(t *testing.T) {
	data := blobPattern(100)
	src := newFakeSource("short", sliceReader(data), 0)
	m := newSlowMerger([]*slowSource{src}, 0, 1000, 1<<20, quietLogger())

	var out bytes.Buffer
	n, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {})
	if err == nil {
		t.Fatal("全部源耗尽且未到 knownTotal 应返回错误")
	}
	if n != int64(len(data)) {
		t.Errorf("run 返回游标 = %d, want 已取到的 %d", n, len(data))
	}
	for _, want := range []string{"所有源耗尽", "100/1000"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息 %q 应包含 %q", err.Error(), want)
		}
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("耗尽前已取到的数据应完整写出（长度 got=%d want=%d）", out.Len(), len(data))
	}
}

// TestSlowMergerCompleteAtKnownTotal 游标推进到已知总长：正常完成（nil 错误），
// 返回游标 == knownTotal，输出等于原始数据。
func TestSlowMergerCompleteAtKnownTotal(t *testing.T) {
	data := blobPattern(100)
	m := newSlowMerger([]*slowSource{newFakeSource("exact", sliceReader(data), 0)}, 0, int64(len(data)), 1<<20, quietLogger())

	var out bytes.Buffer
	n, err := m.run(func(p []byte) { out.Write(p) }, func(time.Time, int) {})
	if err != nil {
		t.Fatalf("恰好到 knownTotal 应正常完成: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("run 返回游标 = %d, want %d", n, len(data))
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("输出应完整等于原始数据（长度 got=%d want=%d）", out.Len(), len(data))
	}
}
