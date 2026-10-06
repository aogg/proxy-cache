// Package cache 实现基于本地文件的 HTTP 响应缓存。
//
// 设计要点：
//   - 缓存键：目标 URL 的 sha256 十六进制（64 字符），直接作为 cache.path 下的文件名；
//   - 文件格式：8 字节文件头（4 字节魔数 + 4 字节 JSON 元信息长度）+ JSON 元信息 + 响应体原文；
//   - 并发安全：写入总是先落同目录临时文件再 rename（原子替换），读取方要么看到旧条目、
//     要么看到完整新条目，不会读到半截数据；所有方法可并发调用；
//   - TTL 在写入时固化到条目内，读取判定过期与后台清理均按条目自身 TTL 进行；
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
	// metaMaxLen 元信息 JSON 的最大长度（1MB），防御异常/损坏文件。
	metaMaxLen = 1 << 20
	// tmpPrefix 写入时的临时文件名前缀。
	tmpPrefix = ".tmp-"
)

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
	// TTLSeconds 写入时固化的缓存时长（秒）。
	TTLSeconds int64 `json:"ttl_seconds"`
}

// Entry 是一份可回放给客户端的上游响应。
type Entry struct {
	// Status 响应状态码。
	Status int
	// Header 响应头（已剔除逐跳头之前的原始头，由 proxy 层负责过滤）。
	Header http.Header
	// Body 响应体原文。
	Body []byte
	// StoredAt 写入时刻。
	StoredAt time.Time
	// TTL 该条目的缓存时长（<=0 视为不缓存/已过期）。
	TTL time.Duration
	// Source 获取该内容时使用的上游（direct 或 url-redirect 候选 URL），仅用于观测。
	Source string
}

// Expired 判断条目在 t 时刻是否已过期。
func (e *Entry) Expired(t time.Time) bool {
	if e == nil || e.TTL <= 0 {
		return true
	}
	return t.Sub(e.StoredAt) >= e.TTL
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

// Key 返回目标 URL 对应的缓存键（sha256 十六进制，64 字符）。
func Key(target string) string {
	sum := sha256.Sum256([]byte(target))
	return hex.EncodeToString(sum[:])
}

// path 返回缓存键对应的磁盘文件路径。
func (c *Cache) path(key string) string { return filepath.Join(c.dir, key) }

// Get 读取缓存；未命中、已过期或损坏时返回 (nil, false)。
// 过期条目会被惰性删除（删除失败不影响本次按未命中处理的结果）。
func (c *Cache) Get(key string) (*Entry, bool) {
	now := time.Now()
	f, err := os.Open(c.path(key))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			// 非常规 IO 错误必须留痕，不静默吞掉
			slog.Warn("读取缓存文件失败", "file", c.path(key), "err", err)
		}
		return nil, false
	}
	defer f.Close()

	e, err := readEntry(f)
	if err != nil {
		slog.Warn("缓存文件损坏，删除后按未命中处理", "file", c.path(key), "err", err)
		if rmErr := os.Remove(c.path(key)); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			slog.Warn("删除损坏缓存文件失败", "file", c.path(key), "err", rmErr)
		}
		return nil, false
	}
	if e.Expired(now) {
		_ = os.Remove(c.path(key)) // 惰性过期清理；失败不影响结果，后台清理会兜底
		return nil, false
	}
	return e, true
}

// Set 原子写入缓存条目（TTL<=0 视为不需要缓存，直接返回）。
// 写入流程：完整内容写入临时文件 -> rename 覆盖目标文件（同目录下原子替换）。
func (c *Cache) Set(key string, e *Entry) error {
	// //////////////////  准备数据  start  ////////////////////////////////////////////////
	if e == nil || e.TTL <= 0 {
		return nil
	}
	data, err := encodeEntry(e)
	if err != nil {
		return err
	}
	// //////////////////  准备数据  end  ////////////////////////////////////////////////

	// //////////////////  写入临时文件并原子替换  start  ////////////////////////////////////////////////
	tmp, err := os.CreateTemp(c.dir, tmpPrefix+"*")
	if err != nil {
		return fmt.Errorf("创建缓存临时文件失败: %w", err)
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return fmt.Errorf("写入缓存临时文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return fmt.Errorf("关闭缓存临时文件失败: %w", err)
	}
	if err := os.Rename(name, c.path(key)); err != nil {
		os.Remove(name)
		return fmt.Errorf("缓存文件落盘失败: %w", err)
	}
	// //////////////////  写入临时文件并原子替换  end  ////////////////////////////////////////////////
	return nil
}

// encodeEntry 把条目编码为磁盘文件字节流：文件头(8B) + 元信息 JSON + 响应体。
func encodeEntry(e *Entry) ([]byte, error) {
	// TTL 不足 1 秒时向上取整为 1 秒，避免亚秒级 TTL 落盘后被判为立即过期
	ttl := e.TTL.Round(time.Second)
	if ttl <= 0 {
		ttl = time.Second
	}
	m := meta{
		Version:    1,
		Status:     e.Status,
		Header:     e.Header,
		StoredAt:   e.StoredAt.Unix(),
		TTLSeconds: int64(ttl / time.Second),
	}
	mj, err := json.Marshal(&m)
	if err != nil {
		return nil, fmt.Errorf("序列化缓存元信息失败: %w", err)
	}
	var head [8]byte
	binary.BigEndian.PutUint32(head[0:4], formatMagic)
	binary.BigEndian.PutUint32(head[4:8], uint32(len(mj)))
	buf := bytes.NewBuffer(make([]byte, 0, len(mj)+len(e.Body)+8))
	buf.Write(head[:])
	buf.Write(mj)
	buf.Write(e.Body)
	return buf.Bytes(), nil
}

// readEntry 从 r 解析缓存文件内容为条目。
func readEntry(r io.Reader) (*Entry, error) {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, fmt.Errorf("读取缓存文件头失败: %w", err)
	}
	if binary.BigEndian.Uint32(head[0:4]) != formatMagic {
		return nil, errors.New("缓存文件魔数不匹配")
	}
	metaLen := binary.BigEndian.Uint32(head[4:8])
	if metaLen == 0 || metaLen > metaMaxLen {
		return nil, fmt.Errorf("缓存元信息长度异常: %d", metaLen)
	}
	mj := make([]byte, metaLen)
	if _, err := io.ReadFull(r, mj); err != nil {
		return nil, fmt.Errorf("读取缓存元信息失败: %w", err)
	}
	var m meta
	if err := json.Unmarshal(mj, &m); err != nil {
		return nil, fmt.Errorf("解析缓存元信息失败: %w", err)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读取缓存体失败: %w", err)
	}
	return &Entry{
		Status:   m.Status,
		Header:   m.Header,
		Body:     body,
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
		// 逐个解析条目，过期或损坏即删除
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

// readFileEntry 读取单个缓存文件为条目（用于后台清理，失败仅返回 ok=false）。
func (c *Cache) readFileEntry(full string) (*Entry, bool) {
	f, err := os.Open(full)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	e, err := readEntry(f)
	if err != nil {
		return nil, false
	}
	return e, true
}
