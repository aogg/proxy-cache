// body_bucket.go —— 流式回源的响应体累积缓冲（大 body 落盘，进程内不持有完整内容）。
//
// 2026-11-20 线上事故：767MB 的 layer 在 MISS 主路径被 bytes.Buffer 整份累积（加上再复制
// 一份写缓存），容器内存峰值 5.48GB；配上 HIT 侧 io.ReadAll 读缓存体，最终以
// 「read: cannot allocate memory」失败并把缓存文件当损坏删掉。本文件提供 bodyBucket：
//
//   - 小响应（<= bodySpillThreshold）：全部驻留内存，行为与旧版 bytes.Buffer 完全一致
//     （manifest / config / 小 blob 零额外 IO，既有对账与回放语义不变）；
//   - 超过阈值：已有的内存字节整体落到缓存目录下的 .tmp- 临时文件，后续追加直接写文件；
//     对账（verifyStaleAgainstConfirmed）、回放、写缓存都按偏移区间流式读，内存量级
//     恒为单个块缓冲（与 blob 大小无关）；
//   - 写侧滚动计算 sha256（SumHex）：写缓存前的 blob 摘要强校验（551a505 防毒缓存语义）
//     直接用这份增量摘要，无需为校验再把整份内容读一遍或物化进内存。
//
// 并发约定：与旧版 bytes.Buffer 一致——调用方保证写与对账读不发生并发（见 stream.go /
// partial_resume.go 的注释：writeOut 与合并循环的 verify 在同一 goroutine 串行）。
package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"

	"proxy-cache/cache"
)

const (
	// bodySpillThreshold 累积缓冲的落盘阈值：累计超过该字节数即整体落到同目录临时文件，
	// 之后追加直接写文件。取 8MB——manifest / config / 小 blob 仍走内存（零额外 IO，
	// 既有测试与语义完全不变），只有现场 767MB 这类大 layer 才落盘。
	bodySpillThreshold int64 = 8 << 20
	// bodySpoolPattern 落盘临时文件的创建模板（.tmp- 前缀与缓存写入临时文件一致，
	// 异常退出残留由 cache.Clean 的后台清理兜底）。
	bodySpoolPattern = ".tmp-spool-*"
)

// bodyBucket 是流式回源期间的响应体累积器：小体量驻留内存，超过阈值后整体落盘。
// 非并发安全（与旧版 bytes.Buffer 的使用约定一致）。
type bodyBucket struct {
	// dir 落盘目录（缓存目录；空串回退系统临时目录）。
	dir string
	// buf 内存阶段累积的响应体（落盘后置 nil）。
	buf []byte
	// file 落盘阶段追加写入的文件句柄。
	file *os.File
	// path 落盘临时文件路径（落盘后有效）。
	path string
	// spilled 是否已落盘。
	spilled bool
	// n 已累积的响应体字节数（落盘后等于文件正文长度）。
	n int64
	// h 写侧滚动 sha256（写缓存前的摘要强校验用）。
	h hash.Hash
}

// newBodyBucket 创建累积缓冲；dir 为落盘目录（缓存目录，空串回退系统临时目录）。
func newBodyBucket(dir string) *bodyBucket {
	return &bodyBucket{dir: dir, h: sha256.New()}
}

// Write 追加一块响应体（实现 io.Writer）：未达阈值追加到内存，达到阈值先整体落盘再写文件；
// 落盘失败（目录不可写 / 临时文件创建失败）时降级为继续内存累积并 Warn——绝不因为
// 「落盘失败」让本次回源失败（可用性优先，内存峰值退化为旧版行为）。
func (b *bodyBucket) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.h.Write(p)
	if !b.spilled && b.n+int64(len(p)) <= bodySpillThreshold {
		b.buf = append(b.buf, p...)
		b.n += int64(len(p))
		return len(p), nil
	}
	if !b.spilled {
		if err := b.spill(); err != nil {
			b.buf = append(b.buf, p...)
			b.n += int64(len(p))
			return len(p), nil
		}
	}
	n, err := b.file.Write(p)
	b.n += int64(n)
	return n, err
}

// spill 把内存阶段的内容整体落到同目录临时文件（后续追加直接写文件）。
func (b *bodyBucket) spill() error {
	dir := b.dir
	if dir == "" {
		dir = os.TempDir()
	}
	f, err := os.CreateTemp(dir, bodySpoolPattern)
	if err != nil {
		slog.Warn("创建响应体临时文件失败，退化为内存累积（内存占用将随响应体增长）",
			"dir", dir, "err", err)
		return err
	}
	if len(b.buf) > 0 {
		if _, werr := f.Write(b.buf); werr != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			slog.Warn("写入响应体临时文件失败，退化为内存累积", "file", f.Name(), "err", werr)
			return werr
		}
	}
	b.file, b.path, b.spilled = f, f.Name(), true
	b.buf = nil
	return nil
}

// Len 返回已累积的响应体字节数。
func (b *bodyBucket) Len() int64 { return b.n }

// Spilled 是否已落盘（落盘后 Bytes 不可用）。
func (b *bodyBucket) Spilled() bool { return b.spilled }

// Bytes 返回内存阶段的完整响应体（未落盘时有效）；落盘后返回 nil，调用方应改用 Stream。
func (b *bodyBucket) Bytes() []byte {
	if b.spilled {
		return nil
	}
	return b.buf
}

// SumHex 返回已累积响应体的 sha256 十六进制摘要（写侧滚动计算，防毒缓存强校验用）。
func (b *bodyBucket) SumHex() string { return hex.EncodeToString(b.h.Sum(nil)) }

// Section 读取 [off, off+n) 区间（n<0 表示到末尾，越界收窄）：内存阶段直接切片
// （调用方只读），落盘阶段按偏移 ReadAt 到新分配的块。用于多源对账等小块读。
func (b *bodyBucket) Section(off, n int64) ([]byte, error) {
	if off < 0 || off > b.n {
		return nil, fmt.Errorf("响应体区间越界（off=%d, size=%d）", off, b.n)
	}
	if n < 0 || off+n > b.n {
		n = b.n - off
	}
	if n == 0 {
		return nil, nil
	}
	if !b.spilled {
		return b.buf[off : off+n], nil
	}
	out := make([]byte, n)
	if _, err := b.file.ReadAt(out, off); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("读取响应体临时文件失败: %w", err)
	}
	return out, nil
}

// CopyTo 把整段响应体流式写给 w（固定块缓冲），返回写出字节数。
func (b *bodyBucket) CopyTo(w io.Writer) (int64, error) {
	if !b.spilled {
		n, err := w.Write(b.buf)
		return int64(n), err
	}
	src, err := b.SectionReader(0, -1)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	return io.CopyBuffer(w, src, make([]byte, streamChunkSize))
}

// SectionReader 返回 [off, off+n) 区间的只读流（n<0 到末尾）：内存阶段包一层内存读取器，
// 落盘阶段用文件 ReadAt 的区间读取器（独立 fd，不与追加写句柄共享偏移）。
func (b *bodyBucket) SectionReader(off, n int64) (io.ReadCloser, error) {
	if off < 0 || off > b.n {
		return nil, fmt.Errorf("响应体区间越界（off=%d, size=%d）", off, b.n)
	}
	if n < 0 || off+n > b.n {
		n = b.n - off
	}
	if !b.spilled {
		return io.NopCloser(newByteSliceReader(b.buf[off : off+n])), nil
	}
	f, err := os.Open(b.path)
	if err != nil {
		return nil, fmt.Errorf("打开响应体临时文件失败: %w", err)
	}
	return &bucketSection{f: f, r: io.NewSectionReader(f, off, n)}, nil
}

// bucketSection 落盘响应体的区间只读流（Close 关闭独立 fd）。
type bucketSection struct {
	f *os.File
	r io.Reader
}

// Read 实现 io.Reader。
func (s *bucketSection) Read(p []byte) (int, error) { return s.r.Read(p) }

// Close 关闭独立 fd。
func (s *bucketSection) Close() error { return s.f.Close() }

// byteSliceReader 是 []byte 的最小只读实现（避免为内存阶段引入额外依赖）。
type byteSliceReader struct {
	b []byte
	i int
}

// newByteSliceReader 返回 b 的只读流。
func newByteSliceReader(b []byte) *byteSliceReader { return &byteSliceReader{b: b} }

// Read 实现 io.Reader。
func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// Stream 把落盘阶段的内容交给条目：落盘返回 BodyStream（Temp=true，缓存写入成功或
// 显式释放时删除），未落盘返回 nil（调用方改用 Bytes）。
func (b *bodyBucket) Stream() *cache.BodyStream {
	if !b.spilled {
		return nil
	}
	return &cache.BodyStream{Path: b.path, Offset: 0, Size: b.n, Sum: b.SumHex(), Temp: true}
}

// Install 把累积结果装进条目：未落盘 → Entry.Body（内存），落盘 → Entry.BodyStream
// （流式，文件所有权移交条目）。调用后不应再向 bucket 追加内容；落盘句柄在此关闭
// （后续读取由 Entry 按路径独立打开，追加已结束）。
func (b *bodyBucket) Install(e *cache.Entry) {
	if e == nil {
		return
	}
	if bs := b.Stream(); bs != nil {
		if b.file != nil {
			_ = b.file.Close()
			b.file = nil
		}
		e.Body = nil
		e.BodyStream = bs
		b.path = "" // 所有权已移交条目：Close 不得再删除该文件
		return
	}
	e.BodyStream = nil
	e.Body = b.buf
}

// Close 丢弃累积结果并删除落盘临时文件（幂等；已通过 Install 移交条目时不要调用——
// 临时文件归条目所有，由 cache.Set 成功后删除或后台清理兜底）。
func (b *bodyBucket) Close() {
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
	if b.spilled && b.path != "" {
		_ = os.Remove(b.path)
		b.path = ""
	}
	b.spilled = false
	b.buf = nil
}
