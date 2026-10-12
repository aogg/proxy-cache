// watchdog.go —— 单次上游尝试的「无新增数据」看门狗（read-timeout）：
// 进度标志 = 该次尝试累计收到的字节数（响应头到达同样算一次进度），窗口内有进度就
// 重置计时器，窗口内无任何新数据才触发超时并取消该次尝试——慢速但持续传输的大文件
// 不会被误砍；它与 http-timeout（尝试总超时）互相独立、先到先取消。

package proxy

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// progressWatchdog 是一次上游尝试的 read-timeout 看门狗。
// 生命周期 = 一次尝试（doRequest）：构造时基于尝试父 ctx 派生子 ctx，尝试结束时
// Stop 停表；timeout <= 0 表示未启用（各方法快速返回，零开销）。
//
// 覆盖范围（谁会 reset / 记进度）：
//   - 响应头到达（client.Do 正常返回）——主请求、token 请求、dance 带 token 重试请求；
//   - body 每读到 n>0 字节（progressReader 包装）——主响应、token 响应、dance 重试响应；
//   - dance 触发后排干 401 旧 body 的 io.Copy 同样经 progressReader 记进度。
//
// 触发效果：cancel 该次尝试的 ctx，所有在途读写（含 token dance）随之失败，
// 错误经 wrapErr 标注为含 "read-timeout" 字样的错误，便于日志辨认。
type progressWatchdog struct {
	// timeout 无新增数据超时窗口；<=0 表示未启用看门狗。
	timeout time.Duration
	// ctx 派生自该次尝试父 ctx 的子 ctx：看门狗触发（或 Stop）时被 cancel。
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	timer   *time.Timer
	fired   bool
	stopped bool
	// bytes 该次尝试累计收到的字节数（进度标志，供错误信息与观测）。
	bytes atomic.Int64
	// lastProgressAt 最近一次进度（响应头/数据）发生的时钟纳秒数：
	// onFire 触发前复核该值，避免「进度到达瞬间旧定时器回调已出队」的竞态误触发
	//（Reset 只能撤销尚未出队的回调）。
	lastProgressAt atomic.Int64
}

// newProgressWatchdog 基于父 ctx 创建看门狗并启动计时；timeout <= 0 表示未启用
// （不启动计时器，仅保留派生 ctx 语义）。
func newProgressWatchdog(parent context.Context, timeout time.Duration) *progressWatchdog {
	ctx, cancel := context.WithCancel(parent)
	w := &progressWatchdog{timeout: timeout, ctx: ctx, cancel: cancel}
	if timeout <= 0 {
		return w
	}
	w.timer = time.AfterFunc(timeout, w.onFire)
	return w
}

// onFire 窗口内无任何新增数据：标记触发并取消该次尝试（Stop 之后再触发直接忽略）。
// 复核最近进度时间：若窗口内其实刚发生过进度（竞态窗口内 Reset 未能撤销本次回调），
// 放弃本次触发，由 Reset 重新武装的定时器在正确时刻再判。
func (w *progressWatchdog) onFire() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	if since := time.Since(time.Unix(0, w.lastProgressAt.Load())); since < w.timeout {
		return // 刚有进度：本次回调过期，不触发
	}
	w.mu.Lock()
	w.fired = true
	w.mu.Unlock()
	w.cancel()
}

// reset 报告一次进度（响应头到达），重置看门狗计时器。
func (w *progressWatchdog) reset() {
	if w.timeout <= 0 {
		return
	}
	w.lastProgressAt.Store(time.Now().UnixNano())
	w.mu.Lock()
	if !w.stopped && w.timer != nil {
		w.timer.Reset(w.timeout)
	}
	w.mu.Unlock()
}

// addBytes 记录本次读到的字节数：n>0 才算进度（累计字节数变化）并重置计时器；
// n<=0（读调用无数据返回）不算进度、不重置。
func (w *progressWatchdog) addBytes(n int64) {
	if n <= 0 {
		return
	}
	w.bytes.Add(n)
	w.reset()
}

// Stop 停止看门狗（一次尝试结束时调用，防计时器泄漏）；幂等。
// 触发后的 cancel 对已完成的读是安全的；尝试结束时 cancel 也不会影响已构建的 Entry。
func (w *progressWatchdog) Stop() {
	w.mu.Lock()
	if w.timer != nil {
		w.timer.Stop()
	}
	w.stopped = true
	w.mu.Unlock()
	w.cancel()
}

// Fired 返回看门狗是否已因「窗口内无新增数据」触发过。
func (w *progressWatchdog) Fired() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fired
}

// wrapErr 在看门狗触发时把该次尝试的错误标注为 read-timeout 错误
// （信息含 "read-timeout" 字样、无数据窗口与累计字节数，保留原错误链便于排查）；
// 未触发时原样返回。
func (w *progressWatchdog) wrapErr(err error) error {
	if err == nil || !w.Fired() {
		return err
	}
	return fmt.Errorf("read-timeout（%s 内无新增数据，本次尝试累计收到 %d 字节）: %w",
		w.timeout, w.bytes.Load(), err)
}

// progressReader 包装上游响应 body：每读到 n>0 字节就向看门狗报告一次进度
// （以累计字节数变化为进度标志，正是 read-timeout 的判定依据）。
type progressReader struct {
	r  io.Reader
	wd *progressWatchdog
}

// Read 透传读取并把本次读到的字节数记入看门狗。
func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	pr.wd.addBytes(int64(n))
	return n, err
}
