package cache

// manager_test.go —— Manager.Acquire 按目录复用 Cache 实例的单测：
//   - 同目录重复 Acquire 复用同一实例，目录在首次创建时建立；
//   - 不同目录返回不同实例，且各自目录都被创建；
//   - cleanInterval<=0 时不启动后台清理（无确定性行为可断言，见用例内说明）。

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mustAcquire 调用 Manager.Acquire，失败时终止测试。
func mustAcquire(t *testing.T, m *Manager, dir string, cleanInterval time.Duration) *Cache {
	t.Helper()
	c, err := m.Acquire(dir, cleanInterval)
	if err != nil {
		t.Fatalf("Acquire(%q, %v) 意外报错: %v", dir, cleanInterval, err)
	}
	return c
}

// assertDirExists 断言 dir 已存在且为目录。
func assertDirExists(t *testing.T, dir string) {
	t.Helper()
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("缓存目录 %s 未创建: %v", dir, err)
	}
	if !fi.IsDir() {
		t.Fatalf("%s 已存在但不是目录", dir)
	}
}

// TestManagerAcquireSameDir 同目录两次 Acquire 返回同一 *Cache 实例，
// 且目录在首次创建时已建立；同目录清理间隔以首次创建为准。
func TestManagerAcquireSameDir(t *testing.T) {
	m := NewManager(context.Background())
	// dir 本身尚不存在，由 Acquire 内部 New() 的 MkdirAll 创建（顺带验证建目录行为）
	dir := filepath.Join(t.TempDir(), "cache-a")

	c1 := mustAcquire(t, m, dir, 0)
	assertDirExists(t, dir)

	// 第二次传入不同的 cleanInterval：仍必须复用首次创建的同一实例
	//（间隔语义：同目录首次创建时生效，之后传入的间隔不再生效）。
	c2 := mustAcquire(t, m, dir, time.Hour)
	if c1 != c2 {
		t.Fatal("同目录两次 Acquire 应返回同一 *Cache 实例，实际不同")
	}
	if c1.Dir() != dir {
		t.Errorf("Cache.Dir() = %q, want %q", c1.Dir(), dir)
	}
}

// TestManagerAcquireDifferentDirs 不同目录返回不同实例，且各自目录都被创建。
func TestManagerAcquireDifferentDirs(t *testing.T) {
	m := NewManager(context.Background())
	base := t.TempDir()
	dirA := filepath.Join(base, "cache-a")
	dirB := filepath.Join(base, "cache-b")

	cA := mustAcquire(t, m, dirA, time.Minute)
	cB := mustAcquire(t, m, dirB, time.Minute)

	if cA == cB {
		t.Fatal("不同目录的 Acquire 应返回不同 *Cache 实例，实际相同")
	}
	if cA.Dir() != dirA {
		t.Errorf("cA.Dir() = %q, want %q", cA.Dir(), dirA)
	}
	if cB.Dir() != dirB {
		t.Errorf("cB.Dir() = %q, want %q", cB.Dir(), dirB)
	}
	assertDirExists(t, dirA)
	assertDirExists(t, dirB)
}

// TestManagerAcquireZeroCleanInterval cleanInterval=0 表示该目录不启动后台过期清理。
// 「未启动清理协程」没有可直接观测的确定性行为（无返回值、无状态变化），
// 依赖时序的断言在单测中脆弱，因此这里只验证 Acquire 正常可用、目录正常创建。
func TestManagerAcquireZeroCleanInterval(t *testing.T) {
	m := NewManager(context.Background())
	dir := filepath.Join(t.TempDir(), "cache-zero")

	c := mustAcquire(t, m, dir, 0)
	assertDirExists(t, dir)
	if c == nil {
		t.Fatal("Acquire 返回 nil 实例")
	}
}
