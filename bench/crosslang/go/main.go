// bench/crosslang/go/main.go
//
// 跨语言对比基准 — Go 端
//
// 方法论：固定消息数（非固定时长），测完成全部收发的墙钟时间。
//   - 每个生产者发送固定 K 条；消费者收到通道关闭为止 → 收发条数精确相等。
//   - 时长校准：小消息数定时跑一遍估吞吐，再标定正式测量消息数到 ~1.5s。
//
// 两种变体：
//   direct — 生产者 ch<-v / 消费者 range ch（核心路径）
//   select — 生产者/消费者各跑一次 2-case select（含永不就绪的 dummy 第二路），
//            对标 C chan_select / Rust select!。
//
// 输出（CSV）：lang,np,nc,cap,mops   lang ∈ {go_direct, go_select}

package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	targetSec = 1.5
	calibMsgs = 200000
	minMsgs   = 200000
	maxMsgs   = 80000000
)

var dummy = make(chan int32) // 永不就绪的 select 第二路

func makeChan(cap int) chan int32 {
	if cap == 0 {
		return make(chan int32)
	}
	return make(chan int32, cap)
}

// ── direct 变体 ──────────────────────────────────────────────
func runDirect(np, nc, cap int, total int) (int64, time.Duration) {
	k := total / np
	total = k * np
	ch := makeChan(cap)
	var recvd int64
	var wg sync.WaitGroup

	for i := 0; i < nc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c int64
			for range ch {
				c++
			}
			atomic.AddInt64(&recvd, c)
		}()
	}
	t0 := time.Now()
	var pwg sync.WaitGroup
	for i := 0; i < np; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var v int32
			for j := 0; j < k; j++ {
				ch <- v
				v++
			}
		}()
	}
	pwg.Wait()
	close(ch)
	wg.Wait()
	dt := time.Since(t0)
	if atomic.LoadInt64(&recvd) != int64(total) {
		fmt.Fprintf(os.Stderr, "  [warn] go_direct np=%d nc=%d cap=%d: 预期 %d 收到 %d\n",
			np, nc, cap, total, recvd)
	}
	return int64(total), dt
}

// ── select 变体 ──────────────────────────────────────────────
func runSelect(np, nc, cap int, total int) (int64, time.Duration) {
	k := total / np
	total = k * np
	ch := makeChan(cap)
	done := make(chan struct{})
	var recvd int64
	var wg sync.WaitGroup

	for i := 0; i < nc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c int64
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						atomic.AddInt64(&recvd, c)
						return
					}
					c++
				case <-dummy: // 永不就绪
				}
			}
		}()
	}
	t0 := time.Now()
	var pwg sync.WaitGroup
	for i := 0; i < np; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var v int32
			for j := 0; j < k; j++ {
				select {
				case ch <- v:
					v++
				case <-dummy: // 永不就绪
				}
			}
		}()
	}
	pwg.Wait()
	close(ch)
	wg.Wait()
	_ = done
	dt := time.Since(t0)
	if atomic.LoadInt64(&recvd) != int64(total) {
		fmt.Fprintf(os.Stderr, "  [warn] go_select np=%d nc=%d cap=%d: 预期 %d 收到 %d\n",
			np, nc, cap, total, recvd)
	}
	return int64(total), dt
}

func measure(np, nc, cap int, run func(int, int, int, int) (int64, time.Duration)) float64 {
	// 1) 校准
	_, cdt := run(np, nc, cap, calibMsgs)
	calibTotal := float64((calibMsgs / np) * np)
	rate := calibTotal / cdt.Seconds()
	// 2) 标定
	msgs := int(rate * targetSec)
	if msgs < minMsgs {
		msgs = minMsgs
	}
	if msgs > maxMsgs {
		msgs = maxMsgs
	}
	// 3) 正式测量
	total, dt := run(np, nc, cap, msgs)
	return float64(total) / dt.Seconds() / 1e6
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	scenarios := [][3]int{
		{1, 1, 0}, {1, 1, 64}, {1, 1, 1024}, {2, 2, 1024}, {4, 4, 1024}, {8, 8, 1024},
	}
	for _, s := range scenarios {
		mops := measure(s[0], s[1], s[2], runDirect)
		fmt.Fprintf(os.Stdout, "go_direct,%d,%d,%d,%.3f\n", s[0], s[1], s[2], mops)
	}
	for _, s := range scenarios {
		mops := measure(s[0], s[1], s[2], runSelect)
		fmt.Fprintf(os.Stdout, "go_select,%d,%d,%d,%.3f\n", s[0], s[1], s[2], mops)
	}
}
