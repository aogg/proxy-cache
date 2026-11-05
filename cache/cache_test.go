package cache

// cache_test.go —— cache.ttl: 0 = 永不过期 语义与落盘取整的单测（同包白盒）：
//   - Entry.Expired：TTL<=0 恒不过期（含 StoredAt 很久以前的条目）；e==nil 视为过期；
//   - Set/Get 往返：TTL=0 条目可落盘、可命中，读回 TTL 仍为 0；
//   - Clean：永久条目（StoredAt 很久以前）不删除；损坏文件照删；
//   - encodeEntry/readEntry：亚秒 TTL 向上取整为至少 1s（绝不能落盘成 ttl_seconds=0 的永久标记），
//     TTL<=0 写 ttl_seconds=0，读回 Entry.TTL=0。

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// farFuture 距现在约 100 年后的时刻，用于「任意未来时刻都不过期」断言。
var farFuture = time.Now().Add(100 * 365 * 24 * time.Hour)

// mustSet 写入缓存条目，失败时终止测试。
func mustSet(t *testing.T, c *Cache, key string, e *Entry) {
	t.Helper()
	if err := c.Set(key, e); err != nil {
		t.Fatalf("Set(key=%s...) 意外报错: %v", key[:8], err)
	}
}

// TestEntryExpired 表驱动验证 Expired 对 TTL<=0（永不过期）与正常 TTL 的判定：
// e==nil 视为过期；TTL<=0 恒为 false（即使 StoredAt 在很久以前）；TTL>0 按时长判定。
func TestEntryExpired(t *testing.T) {
	stored := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // 固定的过去时刻

	tests := []struct {
		name string
		e    *Entry
		at   time.Time // 判定过期所用时刻
		want bool
	}{
		{"nil条目_视为过期", nil, farFuture, true},
		{"TTL为0_StoredAt很久以前_永不过期", &Entry{StoredAt: stored, TTL: 0}, farFuture, false},
		{"TTL为负_同样永不过期", &Entry{StoredAt: stored, TTL: -time.Second}, farFuture, false},
		{"TTL为0_当前时刻_不过期", &Entry{StoredAt: stored, TTL: 0}, time.Now(), false},
		{"TTL正_未到期_不过期", &Entry{StoredAt: stored, TTL: time.Hour}, stored.Add(time.Hour - time.Second), false},
		{"TTL正_恰好满时长_过期", &Entry{StoredAt: stored, TTL: time.Hour}, stored.Add(time.Hour), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Expired(tt.at); got != tt.want {
				t.Errorf("Expired(%v) = %v, want %v (TTL=%v)", tt.at, got, tt.want, tt.e.TTL)
			}
		})
	}
}

// TestSetGetPermanentEntry TTL=0 条目正常落盘并可命中：
// 读回 TTL 仍为 0、任意未来时刻不过期、内容完整往返；StoredAt 特意取过去时刻，
// 排除「刚写入还没到期」造成的命中假阳性。
func TestSetGetPermanentEntry(t *testing.T) {
	c, err := New(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}

	key := Key("https://example.com/permanent.yaml")
	storedAt := time.Now().Add(-30 * 24 * time.Hour) // 30 天前写入
	mustSet(t, c, key, &Entry{
		Status:   http.StatusOK,
		Header:   http.Header{"Content-Type": []string{"text/plain"}},
		Body:     []byte("permanent-body"),
		StoredAt: storedAt,
		TTL:      0, // 0 = 永不过期
	})

	got, ok := c.Get(key)
	if !ok {
		t.Fatal("TTL=0 条目 StoredAt=30 天前，Get 应命中，实际未命中")
	}
	if got.TTL != 0 {
		t.Errorf("读回 TTL = %v, want 0（0 = 永不过期标记跨落盘保持）", got.TTL)
	}
	if got.Expired(farFuture) {
		t.Error("读回的永久条目在任意未来时刻都不应过期")
	}
	if got.Status != http.StatusOK {
		t.Errorf("读回 Status = %d, want %d", got.Status, http.StatusOK)
	}
	if string(got.Body) != "permanent-body" {
		t.Errorf("读回 Body = %q, want %q", got.Body, "permanent-body")
	}
	if got.StoredAt.Unix() != storedAt.Unix() {
		t.Errorf("读回 StoredAt = %v, want %v（Unix 秒精度）", got.StoredAt, storedAt)
	}
}

// TestCleanKeepsPermanentEntry 后台清理不得删除永久条目：
// TTL=0 且 StoredAt=30 天前的条目 Clean 后仍在且可命中；损坏文件（格式非法）仍被删除。
func TestCleanKeepsPermanentEntry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := New(dir)
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}

	// 永久条目：TTL=0，写入时刻为 30 天前（若被当作普通条目早已过期）
	permKey := Key("https://example.com/perm.yaml")
	mustSet(t, c, permKey, &Entry{
		Status:   http.StatusOK,
		Body:     []byte("keep-me"),
		StoredAt: time.Now().Add(-30 * 24 * time.Hour),
		TTL:      0,
	})

	// 损坏文件：内容不是合法缓存格式，Clean 应删除
	badPath := filepath.Join(dir, Key("https://example.com/broken.yaml"))
	if err := os.WriteFile(badPath, []byte("not a cache file"), 0o644); err != nil {
		t.Fatalf("写损坏缓存文件失败: %v", err)
	}

	if removed := c.Clean(); removed != 1 {
		t.Errorf("Clean() removed = %d, want 1（仅损坏文件被删，永久条目必须保留）", removed)
	}
	if _, err := os.Stat(c.path(permKey)); err != nil {
		t.Errorf("永久条目（TTL=0, StoredAt=30天前）不应被 Clean 删除: %v", err)
	}
	if _, err := os.Stat(badPath); !os.IsNotExist(err) {
		t.Errorf("损坏文件应被 Clean 删除，Stat err = %v, want NotNotExist", err)
	}
	// 清理后永久条目仍可正常命中
	if _, ok := c.Get(permKey); !ok {
		t.Error("Clean 后永久条目仍应可命中")
	}
}

// TestEncodeEntryTTLRounding 白盒验证落盘编码的 TTL 取整规则（encodeEntry -> readEntry 往返）：
//   - TTL<=0（含负数）写 ttl_seconds=0，读回 Entry.TTL=0（永不过期标记）；
//   - TTL>0 的亚秒值向上取整为至少 1s —— 400ms 落盘必须按 1s 过期，绝不能变成永久；
//   - 整秒与超过 1s 的值按 Round 取整，且读回条目在 StoredAt+TTL 时刻确实过期（非永久）。
func TestEncodeEntryTTLRounding(t *testing.T) {
	storedAt := time.Unix(1700000000, 0) // 固定写入时刻，保证断言确定性

	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration // readEntry 读回的 Entry.TTL
	}{
		{"TTL为0_写永久标记读回0", 0, 0},
		{"TTL为负_同样写永久标记", -time.Second, 0},
		{"亚秒400ms_向上取整为1s_绝不变永久", 400 * time.Millisecond, time.Second},
		{"整秒1s_保持1s", time.Second, time.Second},
		{"1点4秒_取整为1s", 1400 * time.Millisecond, time.Second},
		{"1点5秒_取整为2s", 1500 * time.Millisecond, 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := encodeEntry(&Entry{
				Status:   http.StatusOK,
				Body:     []byte("x"),
				StoredAt: storedAt,
				TTL:      tt.ttl,
			})
			if err != nil {
				t.Fatalf("encodeEntry(TTL=%v) 意外报错: %v", tt.ttl, err)
			}
			e, err := readEntry(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("readEntry 意外报错: %v", err)
			}
			if e.TTL != tt.wantTTL {
				t.Errorf("TTL=%v 落盘读回 = %v, want %v", tt.ttl, e.TTL, tt.wantTTL)
			}
			// TTL>0 的条目取整后必须仍是「会过期」的条目：到期时刻判定为过期（非永久）；
			// TTL<=0 的条目任意未来时刻都不过期。
			if tt.ttl > 0 && !e.Expired(e.StoredAt.Add(e.TTL)) {
				t.Errorf("TTL=%v 读回条目在 StoredAt+TTL 时刻应判过期，实际未过期（疑似被写成永久）", tt.ttl)
			}
			if tt.ttl <= 0 && e.Expired(farFuture) {
				t.Errorf("TTL=%v 读回条目应为永久（任意未来时刻不过期）", tt.ttl)
			}
		})
	}
}

// TestPartPathSizeRemove 验证 blob 半截文件 API（PartPath / PartSize / RemovePart，
// 配套 proxy 包 partial_resume.go 的半截保留与无感续传）：路径定位、大小读取、幂等删除。
func TestPartPathSizeRemove(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir)
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	key := Key("https://registry.example.com/v2/app/blobs/sha256:aa")

	// PartPath 定位到缓存目录内的 .part-<key>
	wantPath := filepath.Join(dir, ".part-"+key)
	if got := c.PartPath(key); got != wantPath {
		t.Errorf("PartPath = %q, want %q", got, wantPath)
	}

	// 不存在：ok=false
	if _, ok := c.PartSize(key); ok {
		t.Error("半截文件不存在时 PartSize 应返回 ok=false")
	}

	// 写入后：大小与内容一致
	if err := os.WriteFile(c.PartPath(key), []byte("12345"), 0o644); err != nil {
		t.Fatalf("写半截文件失败: %v", err)
	}
	if n, ok := c.PartSize(key); !ok || n != 5 {
		t.Errorf("PartSize = (%d, %v), want (5, true)", n, ok)
	}

	// 删除幂等：删除后 ok=false，再次删除静默成功不 panic
	c.RemovePart(key)
	if _, ok := c.PartSize(key); ok {
		t.Error("RemovePart 后半截应不存在")
	}
	c.RemovePart(key) // 幂等
}

// TestCleanPartialFiles 表驱动验证 Clean 的半截清理规则（对 .part- 前缀文件不按条目
// 解析）：新鲜非空半截（活跃下载/续传中，mtime 新鲜）保留；空半截（下载刚起步残留）
// 与 mtime 早于 partMaxAge（24h）的孤儿半截删除。
func TestCleanPartialFiles(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir)
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}

	freshKey := Key("https://registry.example.com/v2/app/blobs/sha256:01")  // 活跃下载中的半截
	emptyKey := Key("https://registry.example.com/v2/app/blobs/sha256:02")  // 刚起步就中断的空残留
	orphanKey := Key("https://registry.example.com/v2/app/blobs/sha256:03") // 早已结束的孤儿残留

	if err := os.WriteFile(c.PartPath(freshKey), []byte("alive"), 0o644); err != nil {
		t.Fatalf("写新鲜半截失败: %v", err)
	}
	if err := os.WriteFile(c.PartPath(emptyKey), nil, 0o644); err != nil {
		t.Fatalf("写空半截失败: %v", err)
	}
	if err := os.WriteFile(c.PartPath(orphanKey), []byte("stale"), 0o644); err != nil {
		t.Fatalf("写孤儿半截失败: %v", err)
	}
	stale := time.Now().Add(-partMaxAge - time.Hour) // 早于 24h 孤儿判定年龄
	if err := os.Chtimes(c.PartPath(orphanKey), stale, stale); err != nil {
		t.Fatalf("改孤儿半截 mtime 失败: %v", err)
	}

	removed := c.Clean()
	if removed != 2 {
		t.Errorf("Clean 删除数量 = %d, want 2（空半截 + 孤儿半截）", removed)
	}

	// 新鲜非空半截（持续被追加写入、mtime 新鲜）不被清理
	if n, ok := c.PartSize(freshKey); !ok || n != 5 {
		t.Errorf("活跃半截应保留, PartSize = (%d, %v), want (5, true)", n, ok)
	}
	// 空半截与孤儿半截已删除
	if _, ok := c.PartSize(emptyKey); ok {
		t.Error("空半截应被 Clean 删除（无续传价值）")
	}
	if _, ok := c.PartSize(orphanKey); ok {
		t.Error("超过 partMaxAge 的孤儿半截应被 Clean 删除")
	}
}
