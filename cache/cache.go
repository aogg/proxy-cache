// Package cache 实现基于本地文件的 HTTP 响应缓存。
//
// 设计要点：
//   - 缓存键：目标 URL 的 sha256 十六进制（64 字符），直接作为 cache.path 下的文件名；
//   - 文件格式：8 字节文件头（4 字节魔数 + 4 字节 JSON 元信息长度）+ JSON 元信息 + 响应体原文；
//   - 并发安全：写入总是先落同目录临时文件再 rename（原子替换），读取方要么看到旧条目、
//     要么看到完整新条目，不会读到半截数据；所有方法可并发调用；
//   - **流式读写（内存安全）**：读取走 Open（文件句柄 + 区间读取，body 绝不整份读进内存），
//     写入走 Writer（同目录临时文件边收边写 + 原子 rename，正文绝不整份拼进内存）。
//     2026-11-20 线上事故：旧版 Get 用 io.ReadAll 把 767MB 缓存体读进内存，容器峰值 5.48GB、
//     ENOMEM 后又把文件当「损坏」删掉，客户端被迫回源慢速镜像站；本包的全部读写路径
//     都不再以 O(body) 占用内存；
//   - 读取错误分级：只有「结构确实损坏」（魔数 / 元信息长度 / 短读 / JSON 解析失败）才删除
//     文件；ENOMEM / EIO 等临时 IO 错误重试一次后仍失败时按未命中回源，但**保留文件**
//     （临时错误不是用户数据的错，删掉只会让下一次请求同样 MISS）；
//   - TTL 在写入时固化到条目内，读取判定过期与后台清理均按条目自身 TTL 进行
//     （TTL<=0 表示永不过期）；
//   - 过期采用「读取时惰性删除 + 可选后台定期扫描（StartJanitor）」双策略。
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// formatMagic 缓存文件格式版本魔数（"PC1\x01"），用于识别文件格式与版本。
	formatMagic uint32 = 0x50433101
	// headLen 文件头长度：4 字节魔数 + 4 字节元信息 JSON 长度。
	headLen = 8
	// metaMaxLen 元信息 JSON 的最大长度（1MB），防御异常/损坏文件。
	metaMaxLen = 1 << 20
	// bodyChunkSize 流式读写响应体的块大小（写缓存 / 回放 / 摘要复核共用）：
	// 内存占用恒定为该块大小，与响应体大小无关。
	bodyChunkSize = 64 * 1024
	// tmpPrefix 写入时的临时文件名前缀。
	tmpPrefix = ".tmp-"
	// partPrefix blob 半截文件的文件名前缀（.part-<缓存key>）：blob（/v2/<name>/blobs/
	// <digest>）流式下载中断/失败时已下载前缀保留为该文件（文件大小即已下载字节数），
	// 供下次无 Range 完整 GET 从断点拼接续传（见 proxy 包 partial_resume.go）。
	partPrefix = ".part-"
	// partMaxAge 半截文件的孤儿残留判定年龄：最后修改时间早于该年龄的半截文件视为
	// 孤儿（对应下载早已结束、再无续传请求），由 Clean 一并清理——量级与缓存条目的
	// 过期 TTL 相当。活跃续传/下载中的半截文件持续被追加写入（mtime 不断刷新），
	// 不会被该规则误删。
	partMaxAge = 24 * time.Hour
)

// openEntryHook 是同包单测注入读取故障的钩子（模拟 ENOMEM / EIO 等临时 IO 错误，
// 见 stream_test.go 的「临时错误不删文件」用例）；生产代码永不赋值（恒 nil，无开销）。
var openEntryHook func() error

// ErrCorrupt 标记「缓存文件结构确实损坏」：魔数不匹配、元信息长度异常、文件头/元信息
// 短读、JSON 解析失败。只有这类错误允许删除缓存文件（见 Open 的错误语义）；
// ENOMEM / EIO / EMFILE 等临时 IO 错误绝不算损坏。
var ErrCorrupt = errors.New("缓存文件损坏")

// meta 是缓存文件中 JSON 序列化的元信息。
type meta struct {
	// Version 元信息格式版本。
	Version int `json:"version"`
	// Status 上游响应状态码。
	Status int `json:"status"`
	// Header 上游响应头（回放时透传给客户端）。
	Header http.Header `json:"header"`
	// StoredAt 写入时刻（Unix 秒）。
	StoredAt int64 `json:"stored_at"`
	// TTLSeconds 写入时固化的缓存时长（秒）；0 = 永不过期标记。
	TTLSeconds int64 `json:"ttl_seconds"`
}

// BodyStream 是条目响应体在磁盘文件中的一段区间：流式读取来源（大 body 不驻留内存）。
//
// 三种来源共用本结构：
//   - 缓存条目（Open）：Path = 缓存文件，Offset = 文件头 + 元信息长度；
//   - 流式回源的落盘累积（proxy 包 bodyBucket）：Path = 同目录临时文件，Offset = 0，Temp = true；
//   - blob 半截文件（续传执行方）：Path = .part-<key>，Offset = 0。
//
// 区间读取永远通过文件 ReadAt / SectionReader 完成，内存量级恒为块大小。
type BodyStream struct {
	// Path 磁盘文件路径。
	Path string
	// Offset 响应体在文件中的起始偏移（缓存文件的正文段偏移 >0，其余来源通常为 0）。
	Offset int64
	// Size 响应体字节数。
	Size int64
	// Sum 写侧滚动计算的响应体 sha256（小写十六进制）；空串 = 未计算，校验时现读现算。
	// 只在「字节由本进程写入」时可信（流式回源 / 半截文件）；Open 读到的历史条目恒为空，
	// 从而 HIT 侧的摘要复核永远基于磁盘上的真实字节（毒缓存自愈语义不变）。
	Sum string
	// Temp 是否为临时文件：缓存写入成功（正文已被复制进正式缓存文件）后由 Set 删除。
	Temp bool
}

// Entry 是一份可回放给客户端的上游响应。
//
// 响应体有两种承载方式（二选一）：
//   - Body：内存字节（小响应 / 测试 / 上游整包缓冲路径）；
//   - BodyStream：磁盘区间（缓存命中 / 大响应落盘），回放与摘要复核都流式读，不占内存。
//
// 一律通过 BodySize / Section / CopyBody / BodyBytes / SHA256 访问响应体，不要直接读 Body
// （BodyStream 非空时 Body 必为 nil）。
type Entry struct {
	// Status 响应状态码。
	Status int
	// Header 响应头（已剔除逐跳头之前的原始头，由 proxy 层负责过滤）。
	Header http.Header
	// Body 响应体原文（内存承载方式；BodyStream 非空时为空）。
	Body []byte
	// BodyStream 响应体的磁盘区间来源（流式承载方式；与 Body 二选一）。
	BodyStream *BodyStream
	// StoredAt 写入时刻。
	StoredAt time.Time
	// TTL 该条目的缓存时长（<=0 表示永不过期，与配置层 cache.ttl: 0 语义一致）。
	TTL time.Duration
	// Source 获取该内容时使用的上游（direct 或 url-redirect 候选 URL），仅用于观测。
	Source string
	// file Open 打开的文件句柄（流式条目专用，Close 释放；BodyStream 恒指向该文件）。
	// 共享同一 Entry 的多个 goroutine 通过 ReadAt 并发读取是安全的（ReadAt 无共享偏移）。
	file *os.File
}

// Expired 判断条目在 t 时刻是否已过期。
// e==nil 视为已过期；TTL<=0 表示永不过期（恒返回 false）。
func (e *Entry) Expired(t time.Time) bool {
	if e == nil {
		return true
	}
	if e.TTL <= 0 {
		return false // 永不过期
	}
	return t.Sub(e.StoredAt) >= e.TTL
}

// BodySize 返回响应体字节数（内存 body 或磁盘区间）：调用方用它替代 len(e.Body)。
func (e *Entry) BodySize() int64 {
	if e == nil {
		return 0
	}
	if e.BodyStream != nil {
		return e.BodyStream.Size
	}
	return int64(len(e.Body))
}

// Section 返回响应体 [off, off+n) 区间的只读流（n<0 表示到末尾，越界自动收窄）：
// 内存 body 直接切片包装，磁盘 body 用文件句柄 + io.SectionReader（不占内存）。
// 调用方必须关闭返回的 ReadCloser（关闭文件副本，不影响 Entry 自身句柄）。
func (e *Entry) Section(off, n int64) (io.ReadCloser, error) {
	if e == nil {
		return nil, errors.New("缓存条目为空")
	}
	total := e.BodySize()
	if off < 0 {
		off = 0
	}
	if off > total {
		off = total
	}
	if n < 0 || off+n > total {
		n = total - off
	}
	if e.BodyStream == nil {
		return io.NopCloser(bytes.NewReader(e.Body[off : off+n])), nil
	}
	if e.file != nil {
		// 条目自带句柄（Open 的缓存文件）：复用同一 fd 的 ReadAt，无并发偏移问题
		return &sectionReadCloser{r: io.NewSectionReader(e.file, e.BodyStream.Offset+off, n)}, nil
	}
	// 按路径打开（落盘临时文件 / 半截文件）：每次读取独立 fd，共享同一 Entry 的多个
	// 读取方（单飞等待方并发回放）互不影响。
	f, err := os.Open(e.BodyStream.Path)
	if err != nil {
		return nil, fmt.Errorf("打开缓存体文件 %s 失败: %w", e.BodyStream.Path, err)
	}
	return &sectionReadCloser{f: f, r: io.NewSectionReader(f, e.BodyStream.Offset+off, n)}, nil
}

// sectionReadCloser 把 io.SectionReader 包装成可关闭的读取器（f 为 nil 时只读不关闭）。
type sectionReadCloser struct {
	f *os.File
	r io.Reader
}

// Read 实现 io.Reader。
func (s *sectionReadCloser) Read(p []byte) (int, error) { return s.r.Read(p) }

// Close 关闭底层文件句柄（f 为 nil 时无操作）。
func (s *sectionReadCloser) Close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}

// CopyBody 把响应体整段流式写给 w（固定块缓冲，不整份读入内存），返回写出字节数。
func (e *Entry) CopyBody(w io.Writer) (int64, error) {
	src, err := e.Section(0, -1)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	return io.CopyBuffer(w, src, make([]byte, bodyChunkSize))
}

// BodyBytes 读出完整响应体字节（仅用于确实需要整份字节的小响应：manifest JSON 解析、
// 测试注入的摘要校验钩子等）。大 body 请改用 Section / CopyBody。
func (e *Entry) BodyBytes() ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	if e.BodyStream == nil {
		return e.Body, nil
	}
	src, err := e.Section(0, -1)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	return io.ReadAll(src)
}

// SHA256 计算响应体 sha256 十六进制摘要（blob 内容寻址校验用）：
// 写侧已滚动计算过（BodyStream.Sum 非空）时直接复用；否则流式现读现算
// （缓存命中侧复核必须基于磁盘上的真实字节，绝不复用写入时的结论）。
func (e *Entry) SHA256() (string, error) {
	if e == nil {
		return "", errors.New("缓存条目为空")
	}
	if e.BodyStream != nil && e.BodyStream.Sum != "" {
		return e.BodyStream.Sum, nil
	}
	h := sha256.New()
	src, err := e.Section(0, -1)
	if err != nil {
		return "", err
	}
	defer src.Close()
	if _, err := io.CopyBuffer(h, src, make([]byte, bodyChunkSize)); err != nil {
		return "", fmt.Errorf("读取缓存体计算摘要失败: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ReleaseBody 释放条目的响应体（毒内容 / 拒绝入库 / 不再需要回放时调用）：
// 内存 body 清空；临时落盘文件直接删除（非临时来源如半截/正式缓存文件不动，归其拥有者管理）。
func (e *Entry) ReleaseBody() {
	if e == nil {
		return
	}
	if e.BodyStream != nil && e.BodyStream.Temp && e.BodyStream.Path != "" {
		_ = os.Remove(e.BodyStream.Path)
	}
	e.BodyStream = nil
	e.Body = nil
	_ = e.Close()
}

// Close 释放 Open 打开的文件句柄（幂等；BodyStream 为落盘临时文件的条目无需 Close）。
func (e *Entry) Close() error {
	if e == nil || e.file == nil {
		return nil
	}
	err := e.file.Close()
	e.file = nil
	return err
}

// Cache 是文件缓存实例，所有方法可并发调用。
type Cache struct {
	// dir 缓存目录（初始化后只读）。
	dir string
}

// New 创建缓存目录并返回 Cache 实例。
func New(dir string) (*Cache, error) {
	if strings.TrimSpace(dir) == "" {
		dir = "./cache-data"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建缓存目录 %s 失败: %w", dir, err)
	}
	return &Cache{dir: dir}, nil
}

// Dir 返回缓存目录路径。
func (c *Cache) Dir() string { return c.dir }

// FilePath 返回缓存键对应的磁盘文件完整路径（dir/key），
// 供上层日志与观测输出「实际写盘/命中的是哪个文件」，不做任何磁盘操作。
func (c *Cache) FilePath(key string) string { return c.path(key) }

// Key 返回目标 URL 对应的缓存键（sha256 十六进制，64 字符）。
func Key(target string) string {
	sum := sha256.Sum256([]byte(target))
	return hex.EncodeToString(sum[:])
}

// path 返回缓存键对应的磁盘文件路径。
func (c *Cache) path(key string) string { return filepath.Join(c.dir, key) }

// PartPath 返回缓存键对应 blob 半截文件的完整路径（dir/.part-<key>）。
// 半截文件内容 = 该 blob 已下载前缀的原文（无缓存文件头），文件大小即已下载字节数。
func (c *Cache) PartPath(key string) string { return filepath.Join(c.dir, partPrefix+key) }

// PartSize 返回半截文件当前大小（字节）；文件不存在返回 ok=false。并发语义：调用方
// （proxy 包）以按 key 的写锁串行化半截文件的写侧操作，本读取仅作快照观测。
func (c *Cache) PartSize(key string) (int64, bool) {
	st, err := os.Stat(c.PartPath(key))
	if err != nil {
		return 0, false
	}
	return st.Size(), true
}

// RemovePart 删除半截文件（幂等；不存在静默成功，删除失败仅 Warn 留痕——残留由
// Clean 的孤儿清理规则兜底，不影响主流程）。调用时机：对应 blob 正式缓存条目落盘后
// （半截使命完成）或半截判定不可续传需丢弃时。
func (c *Cache) RemovePart(key string) {
	if err := os.Remove(c.PartPath(key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Warn("删除 blob 半截文件失败", "file", c.PartPath(key), "err", err)
	}
}

// Remove 删除缓存条目（幂等；不存在静默成功）。调用方：HIT 侧 blob 摘要复核失败时
// 清除毒条目（防毒缓存，proxy 包 entryDigestMismatch/purgePoisonEntry），与 Open 的
// 惰性过期删除同语义。
func (c *Cache) Remove(key string) {
	c.RemoveFile(c.path(key))
}

// RemoveFile 删除缓存目录内的指定文件（幂等；不存在静默成功，删除失败仅 Warn 留痕）。
// 供上层清理「Open 报损坏」等需要按路径删除的场景复用。
func (c *Cache) RemoveFile(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Warn("删除缓存文件失败", "file", path, "err", err)
	}
}

// Open 以流式方式打开缓存条目：返回元信息 + 可区间读取的 body（文件句柄 + io.SectionReader），
// **响应体绝不整份读入内存**。调用方必须对返回的条目调用 Close 释放文件句柄。
//
// 错误语义（版本重点：不再把临时 IO 错误误判成「文件损坏」而删文件）：
//   - 条目不存在 / 已过期：返回 errors.Is(err, fs.ErrNotExist) 为真的错误（普通未命中；
//     过期条目已按既有语义惰性删除）；
//   - 缓存文件**结构确实损坏**（魔数不符 / 元信息长度异常 / 文件头或元信息短读 / JSON
//     解析失败）：返回 ErrCorrupt，并删除该文件（损坏文件必须清掉才能自愈，与历史语义一致）；
//   - **临时 IO 错误**（ENOMEM / EIO / EMFILE 等，现场 767MB 条目 read: cannot allocate memory）：
//     自动重试一次；仍失败则返回该错误并打一条 WARN 说明「临时错误、文件已保留」，
//     调用方按未命中回源即可——下一次请求还能再用这份缓存。
func (c *Cache) Open(key string) (*Entry, error) {
	path := c.path(key)
	for attempt := 0; ; attempt++ {
		e, err := c.openOnce(path)
		if err == nil {
			return e, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, err // 不存在 / 已过期：普通未命中，无需留痕（由调用方 Debug）
		}
		if errors.Is(err, ErrCorrupt) {
			// 真正损坏：删除后按未命中处理（旧版对一切错误都这么干，导致临时错误也删文件）
			slog.Warn("缓存文件损坏，删除后按未命中处理", "file", path, "err", err)
			c.RemoveFile(path)
			return nil, err
		}
		if attempt == 0 {
			// 临时 IO 错误：重试一次（ENOMEM 多为瞬时内存压力，重试常能成功）
			slog.Debug("读取缓存文件遇到临时错误，重试一次", "file", path, "err", err)
			continue
		}
		// 重试后仍失败：保留缓存文件（临时错误不代表内容损坏），本次按未命中回源
		slog.Warn("读取缓存文件遇到临时 IO 错误（已保留缓存文件），本次按未命中回源",
			"file", path, "err", err)
		return nil, err
	}
}

// openOnce 单次打开并解析缓存文件（Open 的重试单位）。
func (c *Cache) openOnce(path string) (*Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("缓存条目不存在: %w", err)
		}
		return nil, fmt.Errorf("打开缓存文件失败: %w", err)
	}
	if openEntryHook != nil {
		// 单测注入点：返回非 nil 时本次打开按临时 IO 错误失败（协议与真实读错误一致）
		if herr := openEntryHook(); herr != nil {
			_ = f.Close()
			return nil, fmt.Errorf("读取缓存文件失败（注入的临时错误）: %w", herr)
		}
	}
	m, bodyOff, err := readEntryMeta(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	st, serr := f.Stat()
	if serr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("读取缓存文件大小失败: %w", serr)
	}
	if st.Size() < bodyOff {
		_ = f.Close()
		return nil, fmt.Errorf("%w: 缓存体长度异常（文件 %d 字节 < 正文起点 %d）", ErrCorrupt, st.Size(), bodyOff)
	}
	e := &Entry{
		Status:   m.Status,
		Header:   m.Header,
		StoredAt: time.Unix(m.StoredAt, 0),
		// TTLSeconds=0 是「永不过期」标记（Entry.TTL=0 配合 Expired 即永不过期）；
		// 旧格式文件 TTLSeconds>=1 照常过期，无需迁移。旧版本程序读到新「永久」条目
		// 会按旧语义（TTL<=0 视为已过期）将其删除，无害，向前不兼容方向可接受。
		TTL:        time.Duration(m.TTLSeconds) * time.Second,
		BodyStream: &BodyStream{Path: path, Offset: bodyOff, Size: st.Size() - bodyOff},
		file:       f,
	}
	if e.Expired(time.Now()) {
		_ = f.Close()
		_ = os.Remove(path) // 惰性过期清理；失败不影响结果，后台清理会兜底
		return nil, fmt.Errorf("缓存条目已过期: %w", fs.ErrNotExist)
	}
	return e, nil
}

// Get 读取缓存并返回完整条目（内存字节版：保留给历史调用方与测试）。
//
// 生产读取路径请使用 Open（流式）：本函数会把 body 整份读进内存，大 blob 会造成
// O(body) 内存占用（2026-11-20 事故根因之一）。未命中、已过期或损坏时返回 (nil, false)；
// 临时 IO 错误同样按未命中返回，但**绝不删除文件**（错误分级见 Open）。
func (c *Cache) Get(key string) (*Entry, bool) {
	e, err := c.Open(key)
	if err != nil {
		return nil, false
	}
	defer e.Close()
	body, berr := e.BodyBytes()
	if berr != nil {
		slog.Warn("读取缓存体失败（临时 IO 错误，已保留缓存文件），按未命中处理",
			"file", c.path(key), "err", berr)
		return nil, false
	}
	e.Body = body
	e.BodyStream = nil
	return e, true
}

// Writer 是流式缓存写入器：创建时先写好文件头 + 元信息（同目录临时文件），正文由调用方
// 分块追加（边收边写），Commit 时原子 rename 为正式缓存文件，Abort 时删除临时文件。
//
// 语义保证：正式缓存文件永远只可能是「文件头 + 元信息 + 完整正文」的整体——中途失败
// 只会留下 .tmp- 临时文件（由 Abort 或 cache.Clean 清理），绝不会出现半成品正式文件。
// 非并发安全：单个 Writer 只由一个写者使用。
type Writer struct {
	// c 所属缓存实例。
	c *Cache
	// key 缓存键（Commit 时 rename 到 c.path(key)）。
	key string
	// f 临时文件句柄（正文写入口）。
	f *os.File
	// tmp 临时文件路径（Commit 成功后置空）。
	tmp string
	// bodyOffset 正文在文件中的起始偏移（文件头 + 元信息长度）。
	bodyOffset int64
	// size 已写入正文的字节数。
	size int64
	// done 是否已 Commit / Abort（幂等保护）。
	done bool
}

// NewWriter 创建流式缓存写入器：条目元信息（状态码 / 响应头 / 写入时刻 / TTL）在正文
// 开始前即已确定并落盘，正文随后由 Write 分块追加。e.Body 与 e.BodyStream 不参与本函数
// （元信息与正文分离）。返回的 Writer 必须 Commit 或 Abort。
func (c *Cache) NewWriter(key string, e *Entry) (*Writer, error) {
	if e == nil {
		return nil, errors.New("缓存条目为空")
	}
	mj, err := encodeMeta(e)
	if err != nil {
		return nil, err
	}
	var head [headLen]byte
	binary.BigEndian.PutUint32(head[0:4], formatMagic)
	binary.BigEndian.PutUint32(head[4:8], uint32(len(mj)))
	tmp, err := os.CreateTemp(c.dir, tmpPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("创建缓存临时文件失败: %w", err)
	}
	name := tmp.Name()
	prefix := make([]byte, 0, headLen+len(mj))
	prefix = append(prefix, head[:]...)
	prefix = append(prefix, mj...)
	if _, werr := tmp.Write(prefix); werr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("写入缓存临时文件失败: %w", werr)
	}
	return &Writer{c: c, key: key, f: tmp, tmp: name, bodyOffset: int64(len(prefix))}, nil
}

// Write 追加一块正文（实现 io.Writer）：直接写临时文件，不缓存整份内容。
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("缓存写入器已结束（Commit/Abort 之后不可再写）")
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

// Size 返回已写入正文的字节数。
func (w *Writer) Size() int64 { return w.size }

// BodyOffset 返回正文在缓存文件中的起始偏移（文件头 + 元信息长度）。
func (w *Writer) BodyOffset() int64 { return w.bodyOffset }

// Commit 结束写入并原子替换为正式缓存文件（同目录 rename）：失败时删除临时文件并返回错误，
// 正式文件保持原状（要么旧条目、要么完整新条目，绝无半成品）。
func (w *Writer) Commit() error {
	if w.done {
		return errors.New("缓存写入器已结束（Commit/Abort 之后不可再 Commit）")
	}
	w.done = true
	if err := w.f.Close(); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("关闭缓存临时文件失败: %w", err)
	}
	if err := os.Rename(w.tmp, w.c.path(w.key)); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("缓存文件落盘失败: %w", err)
	}
	w.tmp = ""
	return nil
}

// Abort 放弃本次写入：关闭并删除临时文件（幂等）。
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true
	_ = w.f.Close()
	if w.tmp != "" {
		_ = os.Remove(w.tmp)
		w.tmp = ""
	}
}

// Path 返回临时文件路径（观测用；Commit 成功后为空）。
func (w *Writer) Path() string { return w.tmp }

// Set 原子写入缓存条目（仅 e==nil 直接返回；TTL<=0 表示永不过期，同样落盘）。
//
// 写入流程（全程流式，不拼完整内容进内存）：同目录临时文件写「文件头 + 元信息」→ 正文
// 从内存 Body 或磁盘 BodyStream 分块拷贝 → rename 覆盖目标文件（同目录下原子替换）。
// 写入成功后，若条目本来以 BodyStream 承载（大 body 落盘），把来源改指向正式缓存文件并
// 删除原临时文件——同一份内容，读取方（并发单飞等待方）继续可读。
func (c *Cache) Set(key string, e *Entry) error {
	if e == nil {
		return nil
	}
	w, err := c.NewWriter(key, e)
	if err != nil {
		return err
	}
	if e.BodyStream != nil {
		src, serr := e.Section(0, -1)
		if serr != nil {
			w.Abort()
			return fmt.Errorf("读取缓存体失败: %w", serr)
		}
		_, cerr := io.CopyBuffer(w, src, make([]byte, bodyChunkSize))
		_ = src.Close()
		if cerr != nil {
			w.Abort()
			return fmt.Errorf("写入缓存体失败: %w", cerr)
		}
	} else if len(e.Body) > 0 {
		if _, werr := w.Write(e.Body); werr != nil {
			w.Abort()
			return fmt.Errorf("写入缓存体失败: %w", werr)
		}
	}
	if err := w.Commit(); err != nil {
		return err
	}
	if e.BodyStream != nil {
		old := e.BodyStream
		e.BodyStream = &BodyStream{
			Path:   c.path(key),
			Offset: w.bodyOffset,
			Size:   w.size,
			// 正式缓存文件里的字节就是刚校验/刚写入的正文：保留写侧滚动摘要
			Sum: old.Sum,
		}
		e.Body = nil
		if old.Temp && old.Path != "" && old.Path != e.BodyStream.Path {
			// 临时落盘来源已完成使命（正文已复制进正式文件）：删除，避免 767MB 级临时
			// 文件滞留到下次后台清理
			_ = os.Remove(old.Path)
		}
	}
	return nil
}

// Put 以流式方式写入缓存条目：body 从 r 分块读出并落盘（同目录临时文件 + 原子 rename），
// 返回写入的正文字节数。语义与 Set 一致（文件头 + 元信息 + 正文的整体落盘），区别只在
// 正文来源是任意 io.Reader——大响应体无需先物化成 []byte。
func (c *Cache) Put(key string, e *Entry, body io.Reader) (int64, error) {
	if e == nil {
		return 0, nil
	}
	w, err := c.NewWriter(key, e)
	if err != nil {
		return 0, err
	}
	if body != nil {
		if _, cerr := io.CopyBuffer(w, body, make([]byte, bodyChunkSize)); cerr != nil {
			w.Abort()
			return 0, fmt.Errorf("写入缓存体失败: %w", cerr)
		}
	}
	if err := w.Commit(); err != nil {
		return 0, err
	}
	return w.size, nil
}

// encodeMeta 把条目元信息序列化为 JSON（TTL 语义见注释）。
// TTL<=0（永不过期）时 TTLSeconds 写 0（0 = 永不过期标记）；TTL>0 时向上取整为
// 至少 1 秒，避免亚秒级 TTL 落盘后被判为立即过期。
func encodeMeta(e *Entry) ([]byte, error) {
	var ttlSeconds int64
	if e.TTL > 0 {
		ttl := e.TTL.Round(time.Second)
		if ttl <= 0 {
			ttl = time.Second
		}
		ttlSeconds = int64(ttl / time.Second)
	}
	m := meta{
		Version:    1,
		Status:     e.Status,
		Header:     e.Header,
		StoredAt:   e.StoredAt.Unix(),
		TTLSeconds: ttlSeconds,
	}
	mj, err := json.Marshal(&m)
	if err != nil {
		return nil, fmt.Errorf("序列化缓存元信息失败: %w", err)
	}
	return mj, nil
}

// encodeEntry 把内存条目编码为磁盘文件字节流：文件头(8B) + 元信息 JSON + 响应体。
// 仅测试与极小条目使用（大 body 走 NewWriter/Put 流式写，绝不整份拼内存）。
func encodeEntry(e *Entry) ([]byte, error) {
	mj, err := encodeMeta(e)
	if err != nil {
		return nil, err
	}
	var head [headLen]byte
	binary.BigEndian.PutUint32(head[0:4], formatMagic)
	binary.BigEndian.PutUint32(head[4:8], uint32(len(mj)))
	buf := bytes.NewBuffer(make([]byte, 0, headLen+len(mj)+len(e.Body)))
	buf.Write(head[:])
	buf.Write(mj)
	buf.Write(e.Body)
	return buf.Bytes(), nil
}

// readEntryMeta 从文件头 + 元信息解析条目元信息，返回正文起始偏移。
//
// 错误分级：文件头/元信息短读、魔数不匹配、元信息长度异常、JSON 解析失败一律包装为
// ErrCorrupt（真正损坏，可删除）；其余读写错误（ENOMEM / EIO 等）原样返回（临时错误，
// 绝不删文件）。
func readEntryMeta(r io.ReaderAt) (meta, int64, error) {
	var head [headLen]byte
	if _, err := io.ReadFull(io.NewSectionReader(r, 0, headLen), head[:]); err != nil {
		return meta{}, 0, wrapReadErr("读取缓存文件头失败", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != formatMagic {
		return meta{}, 0, fmt.Errorf("%w: 缓存文件魔数不匹配", ErrCorrupt)
	}
	metaLen := binary.BigEndian.Uint32(head[4:8])
	if metaLen == 0 || metaLen > metaMaxLen {
		return meta{}, 0, fmt.Errorf("%w: 缓存元信息长度异常: %d", ErrCorrupt, metaLen)
	}
	mj := make([]byte, metaLen)
	if _, err := io.ReadFull(io.NewSectionReader(r, headLen, int64(metaLen)), mj); err != nil {
		return meta{}, 0, wrapReadErr("读取缓存元信息失败", err)
	}
	var m meta
	if err := json.Unmarshal(mj, &m); err != nil {
		return meta{}, 0, fmt.Errorf("%w: 解析缓存元信息失败: %v", ErrCorrupt, err)
	}
	return m, headLen + int64(metaLen), nil
}

// wrapReadErr 把「读取缓存文件（头/元信息）」阶段的错误按损坏/临时 IO 分级：
// EOF 系（短读）说明文件结构不完整 → ErrCorrupt；其余（ENOMEM / EIO / EMFILE 等）
// 属于临时 IO 错误 → 原样保留，绝不包装成损坏。
func wrapReadErr(stage string, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %s: %v", ErrCorrupt, stage, err)
	}
	return fmt.Errorf("%s: %w", stage, err)
}

// readEntry 从 r 一次性解析缓存文件内容为条目（兼容入口：仅测试与极小文件使用；
// 生产读取走 Open 流式，不整份读入内存）。
func readEntry(r io.Reader) (*Entry, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读取缓存文件失败: %w", err)
	}
	m, bodyOff, err := readEntryMeta(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) < bodyOff {
		return nil, fmt.Errorf("%w: 缓存体长度异常（文件 %d 字节 < 正文起点 %d）", ErrCorrupt, len(data), bodyOff)
	}
	return &Entry{
		Status:   m.Status,
		Header:   m.Header,
		Body:     data[bodyOff:],
		StoredAt: time.Unix(m.StoredAt, 0),
		TTL:      time.Duration(m.TTLSeconds) * time.Second,
	}, nil
}

// StartJanitor 启动后台过期清理协程，每隔 interval 扫描一次缓存目录；
// interval<=0 时不启动。协程随 ctx 取消退出。
func (c *Cache) StartJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if removed := c.Clean(); removed > 0 {
					slog.Info("缓存后台清理完成", "removed", removed)
				}
			}
		}
	}()
}

// Clean 扫描缓存目录，删除过期/损坏条目与残留临时文件，返回删除数量。
// TTL<=0 的永久条目按未过期处理不会被删除（仍会清理损坏文件与临时文件）。
// blob 半截文件（.part-<key>，见 PartPath）不按条目解析：空文件或 mtime 早于
// partMaxAge 的孤儿残留即删除（规则见 Clean 内注释）。
//
// 内存安全：条目解析只读文件头 + 元信息（readEntryMeta），绝不把正文读进内存
// （历史实现会对每个文件 io.ReadAll，清理 767MB 级条目时同样会打爆内存）。
func (c *Cache) Clean() int {
	des, err := os.ReadDir(c.dir)
	if err != nil {
		slog.Warn("扫描缓存目录失败", "dir", c.dir, "err", err)
		return 0
	}
	now := time.Now()
	removed := 0
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		full := filepath.Join(c.dir, name)
		// 先清理上次异常退出可能残留的临时文件
		if strings.HasPrefix(name, tmpPrefix) {
			if rmErr := os.Remove(full); rmErr == nil {
				removed++
			}
			continue
		}
		// blob 半截文件清理（.part-<key>）：空文件（下载刚起步就中断的残留，无续传
		// 价值）或孤儿残留（mtime 早于 partMaxAge，对应下载早已结束、再无续传请求）
		// 即删除；活跃下载/续传中的半截文件持续追加写入、mtime 新鲜，不受影响。
		// 内容为前缀原文（无缓存文件头），不走下方条目解析分支。
		if strings.HasPrefix(name, partPrefix) {
			info, ierr := de.Info()
			if ierr == nil && (info.Size() == 0 || now.Sub(info.ModTime()) >= partMaxAge) {
				if rmErr := os.Remove(full); rmErr == nil {
					removed++
				} else if !errors.Is(rmErr, fs.ErrNotExist) {
					slog.Warn("清理 blob 半截文件失败", "file", full, "err", rmErr)
				}
			}
			continue
		}
		// 逐个解析条目元信息（不读正文），过期或损坏即删除
		e, ok := c.readFileEntry(full)
		if !ok || e.Expired(now) {
			if rmErr := os.Remove(full); rmErr == nil {
				removed++
			} else if !errors.Is(rmErr, fs.ErrNotExist) {
				slog.Warn("清理缓存文件失败", "file", full, "err", rmErr)
			}
		}
	}
	return removed
}

// readFileEntry 读取单个缓存文件的元信息（用于后台清理，失败仅返回 ok=false；不读正文）。
func (c *Cache) readFileEntry(full string) (*Entry, bool) {
	f, err := os.Open(full)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	m, bodyOff, err := readEntryMeta(f)
	if err != nil {
		return nil, false
	}
	st, serr := f.Stat()
	if serr != nil || st.Size() < bodyOff {
		return nil, false
	}
	return &Entry{
		Status:     m.Status,
		Header:     m.Header,
		StoredAt:   time.Unix(m.StoredAt, 0),
		TTL:        time.Duration(m.TTLSeconds) * time.Second,
		BodyStream: &BodyStream{Path: full, Offset: bodyOff, Size: st.Size() - bodyOff},
	}, true
}
