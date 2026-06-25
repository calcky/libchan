# Cross-Language Channel Performance Benchmark

Compare the throughput of three channel implementations on **the same machine, the same set of scenarios, and an equivalent stop mechanism**:

| Implementation | Language | Channel type |
|------|------|-------------|
| [libchan](libchan/bench.c) | C11 | MPMC bounded, DPDK-style lock-free ring + Linux futex park |
| Go built-in `chan` | Go | MPMC bounded/unbuffered, goroutine-scheduler park |
| [`crossbeam-channel`](rust/src/main.rs) | Rust | MPMC bounded, crossbeam ring + futex Parker |

> See [`doc/comparison.md`](../../doc/comparison.md) for full results and analysis.

---

## One-Command Run

```bash
# Run from the repository root
bash bench/crosslang/run_comparison.sh
```

The script automatically:

1. Builds the libchan static library with CMake (Release, `-O3`)
2. Compiles the C benchmark (statically linked against libchan), the Go benchmark (`go build`), and the Rust benchmark (`cargo build --release`)
3. Runs all three in turn, with 400 ms warmup + 1500 ms measurement per scenario
4. Merges the CSVs, prints the comparison table, and writes `doc/comparison.md`

Total time is about 6 scenarios × 3 languages × 1.9 s ≈ **35 s**.

### Dependencies

| Tool | Purpose |
|------|------|
| `cmake` + `gcc` (C11, with `-pthread` support) | Build libchan and the C benchmark |
| `go` (≥ 1.18) | Go benchmark |
| `cargo` / `rustc` | Rust benchmark (first run downloads `crossbeam-channel` over the network) |

---

## Directory Layout

```
bench/crosslang/
├── run_comparison.sh     # build + run + generate the report
├── libchan/
│   └── bench.c           # C benchmark (chan_select + stop channel)
├── go/
│   ├── go.mod
│   └── main.go           # Go benchmark (select + close(done))
└── rust/
    ├── Cargo.toml
    └── src/main.rs       # Rust benchmark (select! + drop(stop_tx))
```

---

## Test Scenarios

Elements are uniformly 4-byte integers (C `int` / Go `int32` / Rust `i32`).

| No. | cap | P | C | Focus |
|------|-----|---|---|---------|
| S1 | 0 (unbuffered) | 1 | 1 | rendezvous overhead, park/unpark latency |
| S2 | 64 | 1 | 1 | small-buffer SPSC, occasional blocking |
| S3 | 1024 | 1 | 1 | large-buffer SPSC, lock-free fast-path ceiling |
| S4 | 1024 | 2 | 2 | symmetric low-contention MPMC |
| S5 | 1024 | 4 | 4 | symmetric medium-contention MPMC |
| S6 | 1024 | 8 | 8 | symmetric high-contention MPMC (16 threads) |

---

## Fairness Methodology

All three are strictly aligned on the following dimensions to eliminate benchmark bias:

| Dimension | Consistent approach across all three |
|------|------------|
| **Hot path** | Every iteration runs one **2-case select**, listening on both the data channel and the stop channel |
| **Counting basis** | Count only the **messages actually received by the consumer** (C's `min(sent,recv)` is always equal to `recv`) |
| **Time window** | 400 ms warmup (zeroes the count) + 1500 ms measurement |
| **Data size** | All 4 B |
| **Stop mechanism** | Broadcast-close the stop channel: C `chan_close` / Go `close(done)` / Rust `drop(stop_tx)` |
| **Optimization level** | C `-O3` / Rust `--release` (LLVM O3) / Go standard release build |

Hot-path shape for each:

```c
// C / libchan
chan_select({ send(data_ch, v), recv(stop_ch) });
```
```go
// Go
select { case ch <- v: ...; case <-done: return }
```
```rust
// Rust
select! { send(data_tx, v) -> _ => {...}, recv(stop_rx) -> _ => break }
```

### Two inherent differences (declared honestly, not benchmark bias)

1. **Thread model M:N vs 1:1** — Go uses goroutines (user-space scheduler park, ~200 ns),
   while C/Rust use OS threads + Linux futex (one kernel round-trip per rendezvous, ~1–2 µs).
   This is the fundamental reason Go leads in the S1 unbuffered scenario; crossbeam also uses futex, and in S1 it is
   of the same order of magnitude as libchan, confirming this is the shared ceiling of OS-thread park.

2. **Semantic relaxation in libchan's select** — to avoid MPMC lock serialization, libchan's select
   stub does not increment the waiter count on buffered channels, at the cost that a parked select-sender may be
   delayed by at most "one ring capacity" worth of messages before being woken. In this benchmark, this relaxation **actually slows libchan slightly**,
   so it is not an unfair advantage in its favor.

---

## Results at a Glance

> 13th Gen Intel i7-13700H / WSL2 / gcc 13.3 / go 1.23 / rustc 1.96, unit **Mops/s (higher is better)**.
> The actual numbers fluctuate slightly per run; the latest run in `doc/comparison.md` is authoritative.

| Scenario | libchan (C) | Go chan | crossbeam (Rust) | Winner |
|------|------------:|--------:|-----------------:|------|
| S1  1P+1C cap=0    |  0.235 | **4.748** |  0.150 | Go |
| S2  1P+1C cap=64   | **14.436** | 12.548 |  5.227 | libchan |
| S3  1P+1C cap=1024 | **24.155** | 12.799 |  9.410 | libchan |
| S4  2P+2C cap=1024 | **12.885** | 10.734 |  9.873 | libchan |
| S5  4P+4C cap=1024 | **11.020** |  6.150 |  9.314 | libchan |
| S6  8P+8C cap=1024 |  **9.620** |  3.238 |  6.053 | libchan |

**Conclusion**: libchan wins 5 of the 6 scenarios. It beats both Go and crossbeam in all buffered scenarios (S2–S6),
thanks to the lock-free ring fast path (S3 ≈ 40 ns/op). The only one it trails, S1, reflects the inherent disadvantage of
OS threads versus Go coroutines on synchronous rendezvous, not an implementation flaw.
