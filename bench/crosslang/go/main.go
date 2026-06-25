// bench/crosslang/go/main.go
//
// Cross-language comparison benchmark — Go side
//
// Methodology: fixed message count (not fixed duration); measure the wall-clock
// time to complete all sends and receives.
//   - Each producer sends a fixed K messages; each consumer keeps receiving until
//     the channel closes -> sent and received counts are exactly equal.
//   - Duration calibration: run once for a fixed time with a small message count to
//     estimate throughput, then calibrate the real measurement's message count to ~1.5s.
//
// Two variants:
//   direct — producer ch<-v / consumer range ch (core path)
//   select — producer/consumer each run one 2-case select (with a dummy second case
//            that is never ready), matching C chan_select / Rust select!.
//
// Output (CSV): lang,np,nc,cap,mops   lang in {go_direct, go_select}

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

var dummy = make(chan int32) // never-ready second case for select

func makeChan(cap int) chan int32 {
	if cap == 0 {
		return make(chan int32)
	}
	return make(chan int32, cap)
}

// ── direct variant ──────────────────────────────────────────
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
		fmt.Fprintf(os.Stderr, "  [warn] go_direct np=%d nc=%d cap=%d: expected %d received %d\n",
			np, nc, cap, total, recvd)
	}
	return int64(total), dt
}

// ── select variant ──────────────────────────────────────────
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
				case <-dummy: // never ready
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
				case <-dummy: // never ready
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
		fmt.Fprintf(os.Stderr, "  [warn] go_select np=%d nc=%d cap=%d: expected %d received %d\n",
			np, nc, cap, total, recvd)
	}
	return int64(total), dt
}

func measure(np, nc, cap int, run func(int, int, int, int) (int64, time.Duration)) float64 {
	// 1) Calibration
	_, cdt := run(np, nc, cap, calibMsgs)
	calibTotal := float64((calibMsgs / np) * np)
	rate := calibTotal / cdt.Seconds()
	// 2) Calibrate
	msgs := int(rate * targetSec)
	if msgs < minMsgs {
		msgs = minMsgs
	}
	if msgs > maxMsgs {
		msgs = maxMsgs
	}
	// 3) Real measurement
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
