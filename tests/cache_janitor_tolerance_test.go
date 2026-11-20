// cache_janitor_tolerance_test.go —— 缓存 janitor/Clean 对异常文件容错不炸的黑盒回归
// （缺口#9，session-944d8ef7 现场权限事故链：janitor 把异常文件当损坏清掉 → 半截读回
// short read got 0）。经 cache 包公开 API（Manager.Acquire / Cache.Set/Get/Clean）驱动：
//  1. 目录形态的「条目」、无后缀垃圾文件、截断损坏条目、0 字节半截、过期 .tmp- 残留
//     共存时 Clean 不 panic、逐类按文档语义处置；
//  2. 有效条目（TTL 内）与新鲜半截绝不被误删——正常条目永远是第一优先级；
//  3. 永久条目（ttl_seconds=0 标记）不被清理；
//  4. 真实 EACCES（无权限文件）在本测试环境以 root 运行、chmod 无法复现读取失败，
//     该子形态标注跳过（见报告）——但「root 属主有效条目不被误删」在本环境即默认形态，
//     由用例 2 直接覆盖。
package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"proxy-cache/cache"
)

// janitorEnv 建一个独立缓存目录实例（不经 proxy，直接驱动 cache 包公开 API）。
func janitorEnv(t *testing.T) (*cache.Cache, string) {
	t.Helper()
	dir := t.TempDir()
	mgr := cache.NewManager(context.Background())
	c, err := mgr.Acquire(dir, time.Hour)
	if err != nil {
		t.Fatalf("Acquire 失败: %v", err)
	}
	return c, dir
}

// janitorEntry 构造一个有效缓存条目。
func janitorEntry(body []byte, ttl time.Duration) *cache.Entry {
	return &cache.Entry{
		Status:   200,
		Header:   map[string][]string{"Content-Type": {"application/octet-stream"}},
		Body:     body,
		StoredAt: time.Now(),
		TTL:      ttl,
	}
}

// TestCleanToleratesAbnormalFiles 缺口#9：异常文件共存时 Clean 容错不炸、不误删。
func TestCleanToleratesAbnormalFiles(t *testing.T) {
	c, dir := janitorEnv(t)

	// 有效条目（TTL 1h，root 属主）与永久条目（TTL 0 = 永不过期标记）
	validBody := []byte("valid-entry-body")
	if err := c.Set("valid-key", janitorEntry(validBody, time.Hour)); err != nil {
		t.Fatalf("写有效条目失败: %v", err)
	}
	foreverBody := []byte("forever-entry-body")
	if err := c.Set("forever-key", janitorEntry(foreverBody, 0)); err != nil {
		t.Fatalf("写永久条目失败: %v", err)
	}

	// 异常文件族：
	//   a. 目录形态「条目」（ReadDir 返回 dir，Clean 必须跳过而非 panic/删除）
	if err := os.MkdirAll(filepath.Join(dir, "a-directory-named-entry"), 0o755); err != nil {
		t.Fatalf("建目录形态条目失败: %v", err)
	}
	//   b. 无后缀垃圾文件（解析失败按损坏清理——文档化语义，绝不 panic）
	if err := os.WriteFile(filepath.Join(dir, "garbage-junk"), []byte("not-a-cache-entry"), 0o644); err != nil {
		t.Fatalf("写垃圾文件失败: %v", err)
	}
	//   c. 截断损坏条目（头部合法前缀 + 半截 JSON）
	if err := os.WriteFile(filepath.Join(dir, "truncated-entry"), []byte("\x00\x00\x00\x00\x00\x00\x00\x00{\"status\":20"), 0o644); err != nil {
		t.Fatalf("写截断条目失败: %v", err)
	}
	//   d. 0 字节半截（无续传价值，按规则清理）+ 新鲜半截（活跃下载，必须保留）
	if err := os.WriteFile(c.PartPath("empty-part"), nil, 0o644); err != nil {
		t.Fatalf("写空半截失败: %v", err)
	}
	freshPart := filepath.Join(dir, ".part-fresh-part")
	if err := os.WriteFile(freshPart, []byte("fresh-part-prefix"), 0o644); err != nil {
		t.Fatalf("写新鲜半截失败: %v", err)
	}
	//   e. 残留 .tmp- 临时文件（异常退出残留，必清理）
	if err := os.WriteFile(filepath.Join(dir, ".tmp-leftover"), []byte("partial-write"), 0o644); err != nil {
		t.Fatalf("写临时残留失败: %v", err)
	}

	// Clean 执行：不 panic（炸了直接红）、返回删除数
	removed := c.Clean()
	t.Logf("Clean 删除 %d 个文件", removed)

	// 有效条目与永久条目绝不被误删
	if e, ok := c.Get("valid-key"); !ok || string(e.Body) != string(validBody) {
		t.Errorf("有效条目被 Clean 误删或内容损坏（ok=%v）", ok)
	}
	if e, ok := c.Get("forever-key"); !ok || string(e.Body) != string(foreverBody) {
		t.Errorf("永久条目（ttl_seconds=0）被 Clean 误删或内容损坏（ok=%v）", ok)
	}
	// 新鲜半截保留
	if data, err := os.ReadFile(freshPart); err != nil || string(data) != "fresh-part-prefix" {
		t.Errorf("活跃半截被 Clean 误删或内容损坏（err=%v）", err)
	}
	// 按文档语义应被清理的：垃圾/截断条目、空半截、.tmp- 残留
	for _, name := range []string{"garbage-junk", "truncated-entry", ".part-empty-part", ".tmp-leftover"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("异常文件 %s 应被 Clean 删除，仍存在", name)
		}
	}
	// 目录形态条目被跳过（不删除）
	if _, err := os.Stat(filepath.Join(dir, "a-directory-named-entry")); err != nil {
		t.Errorf("目录形态条目应被 Clean 跳过（不删除），实际已不存在: %v", err)
	}
	// 二次 Clean 幂等收敛（删除数 0，不因残留状态炸）
	if again := c.Clean(); again != 0 {
		t.Errorf("二次 Clean 应删除 0 个文件（收敛），实际 %d", again)
	}
}
