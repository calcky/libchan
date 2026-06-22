// bench/crosslang/go/main.go
//
// 跨语言对比基准 — Go 端
//
// 6 个固定场景（与 C/Rust 端完全一致）：
//   S1: cap=0,    1P+1C  (unbuffered rendezvous)
//   S2: cap=64,   1P+1C
//   S3: cap=1024, 1P+1C
//   S4: cap=1024, 2P+2C
//   S5: cap=1024, 4P+4C
//   S6: cap=1024, 8P+8C
//
// 停止机制：
//   close(done) 广播给所有 goroutine；
//   goroutine 在 select 中同时监听 done，收到后退出。
//   close(done) 使 "<-done" case 立即就绪，等价于 C 端 chan_close。
//
// 输出格式（CSV）：
//   lang,np,nc,cap,mops
//
// 编译：go build -o bench_go .

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
	warmupMs  = 400
	measureMs = 1500
)

func benchCase(np, nc, capSize int) float64 {
	var ch chan int32
	if capSize == 0 {
		ch = make(chan int32)
	} else {
		ch = make(chan int32, capSize)
	}

	done := make(chan struct{})
	var ops int64
	var wg sync.WaitGroup

	// producers
	for i := 0; i < np; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var v int32
			for {
				select {
				case ch <- v:
					v++
				case <-done:
					return
				}
			}
		}()
	}

	// consumers
	for i := 0; i < nc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch:
					atomic.AddInt64(&ops, 1)
				case <-done:
					return
				}
			}
		}()
	}

	// warmup
	time.Sleep(warmupMs * time.Millisecond)
	atomic.StoreInt64(&ops, 0)

	// measure
	time.Sleep(measureMs * time.Millisecond)
	close(done) // 广播停止
	wg.Wait()

	// drain 残留消息（避免 goroutine 泄漏）
	for len(ch) > 0 {
		<-ch
	}

	n := atomic.LoadInt64(&ops)
	return float64(n) / (float64(measureMs) / 1000.0) / 1e6
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // 已是默认值，显式声明以示公平性

	type scenario struct{ np, nc, cap int }
	scenarios := []scenario{
		{1, 1, 0},
		{1, 1, 64},
		{1, 1, 1024},
		{2, 2, 1024},
		{4, 4, 1024},
		{8, 8, 1024},
	}

	for _, s := range scenarios {
		mops := benchCase(s.np, s.nc, s.cap)
		fmt.Fprintf(os.Stdout, "go,%d,%d,%d,%.3f\n", s.np, s.nc, s.cap, mops)
	}
}
