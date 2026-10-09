package cache

// manager.go 实现按目录复用 Cache 实例的管理器，支撑「全局缓存关闭、
// 命中域名规则的流量按规则写入各自独立缓存目录」的多目录场景；
// 所有请求都落同一目录时退化为原来的单实例行为（完全兼容旧用法）。

import (
	"context"
	"sync"
	"time"
)

// Manager 按目录管理多个 Cache 实例：同一目录全局复用同一个实例（并发安全）。
type Manager struct {
	// ctx 生命周期上下文：各目录的后台清理协程随其取消而退出。
	ctx context.Context
	// mu 保护 caches 的并发读写。
	mu sync.RWMutex
	// caches 目录 -> 已创建的 Cache 实例。
	caches map[string]*Cache
}

// NewManager 创建缓存管理器；ctx 用于派生各目录后台清理协程的生命周期
// （ctx 取消后全部 janitor 退出，与 Cache.StartJanitor 语义一致），传 nil 时兜底 Background。
func NewManager(ctx context.Context) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Manager{ctx: ctx, caches: make(map[string]*Cache)}
}

// Acquire 返回 dir 对应的 Cache 实例：已存在直接复用；否则创建目录
// （mkdir 失败快速返回错误）并在 cleanInterval>0 时启动后台过期清理。
// 同一目录的清理间隔以首次创建时传入的为准，之后传入的间隔不再生效
// （避免重复创建清理协程；各目录间隔来自配置，运行期只读不变）。
func (m *Manager) Acquire(dir string, cleanInterval time.Duration) (*Cache, error) {
	// 快路径：读锁直接查已存在实例
	m.mu.RLock()
	c, ok := m.caches[dir]
	m.mu.RUnlock()
	if ok {
		return c, nil
	}

	// 慢路径：写锁内创建（双重检查，保证并发 Acquire 同一目录时只创建一次）
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok = m.caches[dir]; ok {
		return c, nil
	}
	c, err := New(dir)
	if err != nil {
		return nil, err
	}
	c.StartJanitor(m.ctx, cleanInterval)
	m.caches[dir] = c
	return c, nil
}
