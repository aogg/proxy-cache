package proxy

import (
	"fmt"
	"sync"

	"proxy-cache/cache"
)

// flightGroup 是针对缓存键的简易 singleflight：
// 同一 key 的并发回源只会真正执行一次 fn，其余调用阻塞等待并共享同一份结果，
// 用于避免热门 URL 缓存过期瞬间的回源风暴（缓存击穿）。
type flightGroup struct {
	// mu 保护 m 的并发访问。
	mu sync.Mutex
	// m 记录在途调用：key -> call。
	m map[string]*flightCall
}

// flightCall 是一次在途回源调用。
type flightCall struct {
	// done 结束后关闭，等待方通过它同步结果。
	done chan struct{}
	// entry 回源结果（成功时非 nil，返回后只读共享）。
	entry *cache.Entry
	// err 回源错误。
	err error
}

// Do 执行或等待 key 对应的在途调用：首个调用方真正执行 fn，后续同 key 调用等待其完成。
func (g *flightGroup) Do(key string, fn func() (*cache.Entry, error)) (*cache.Entry, error) {
	// //////////////////  加入或创建在途调用  start  ////////////////////////////////////////////////
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done // 等待首个调用方完成
		return c.entry, c.err
	}
	c := &flightCall{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()
	// //////////////////  加入或创建在途调用  end  ////////////////////////////////////////////////

	// fn panic 时也必须清理在途记录并唤醒等待方，否则等待方会永久阻塞
	defer func() {
		if r := recover(); r != nil {
			c.entry, c.err = nil, fmt.Errorf("回源回调 panic: %v", r)
			g.finish(key, c)
			panic(r)
		}
	}()

	// //////////////////  执行回源并广播结果  start  ////////////////////////////////////////////////
	c.entry, c.err = fn()
	g.finish(key, c)
	return c.entry, c.err
	// //////////////////  执行回源并广播结果  end  ////////////////////////////////////////////////
}

// finish 从在途表移除调用并关闭 done 通道广播结果（每个调用恰好执行一次）。
func (g *flightGroup) finish(key string, c *flightCall) {
	g.mu.Lock()
	if cur, ok := g.m[key]; ok && cur == c {
		delete(g.m, key)
	}
	g.mu.Unlock()
	close(c.done)
}
