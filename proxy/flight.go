package proxy

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"proxy-cache/cache"
)

// flightWaitLimit 单飞等待上限：大 blob 流式回源可持续几十秒到几分钟，等待 flight.Do
// 的并发请求不能无限期吊死（docker 客户端等响应头约 30s 就超时断开，等待方比执行方
// 更晚拿到响应头必然跟着超时）。等待超过该上限仍未完成就返回 errFlightWaitTimeout，
// 由调用方自行走候选回源（cache.Set 的临时文件 + rename 原子替换保证并发写缓存
// 不会互相污染，多份完整结果谁后到谁覆盖，读取方永远看不到半截数据）。
const flightWaitLimit = 10 * time.Second

// errFlightWaitTimeout 等待同 key 单飞回源超过上限的错误标识（errors.Is 判定），
// 不是回源本身的失败：调用方应据此自行回源，而不是直接给客户端报错。
var errFlightWaitTimeout = errors.New("等待同 key 单飞回源超限")

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

// Do 执行或等待 key 对应的在途调用：首个调用方真正执行 fn，后续同 key 调用等待其完成
//（无限期等待，与旧版行为一致；等价于 DoLimit(key, 0, fn)）。
func (g *flightGroup) Do(key string, fn func() (*cache.Entry, error)) (*cache.Entry, error) {
	e, _, err := g.DoLimit(key, 0, fn)
	return e, err
}

// DoLimit 与 Do 相同，但等待方最多阻塞 limit：超限返回 errFlightWaitTimeout（entry 为
// nil），由调用方自行回源，不再注册新的在途调用。limit<=0 表示无限期等待。
// 返回值 executed=true 表示本调用是执行方（fn 在本调用内真正运行了一次）；
// false 表示本调用是等待方（共享执行方的 entry/err）或等待超限。
// 流式回源场景依赖 executed 区分：执行方的响应已在 fn 内直接写给客户端，
// 等待方才需要拿完整 Entry 走 serveEntry 回放。
func (g *flightGroup) DoLimit(key string, limit time.Duration, fn func() (*cache.Entry, error)) (entry *cache.Entry, executed bool, err error) {
	// //////////////////  加入或创建在途调用  start  ////////////////////////////////////////////////
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		if limit <= 0 {
			<-c.done // 等待首个调用方完成（无限期，旧版语义）
			return c.entry, false, c.err
		}
		timer := time.NewTimer(limit)
		defer timer.Stop()
		select {
		case <-c.done:
			return c.entry, false, c.err
		case <-timer.C:
			return nil, false, errFlightWaitTimeout
		}
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
	return c.entry, true, c.err
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
