# libchan

> A high-performance channel library in pure C11 — bringing Go / Rust channel semantics to C.

[![CI](https://github.com/calcky/libchan/actions/workflows/ci.yml/badge.svg)](https://github.com/calcky/libchan/actions/workflows/ci.yml)
[![Benchmark trend](https://img.shields.io/badge/benchmark-trend-2da44e?logo=github)](https://calcky.github.io/libchan/dev/bench/)

`libchan` provides unbuffered synchronous handoff, buffered asynchronous queues, multi-producer
multi-consumer (MPMC), close notification, and Go-like `select` multiplexing. At its core is a
**lock-free fast path** (DPDK-style lock-free ring + Linux futex park). Its `select` path beats Go
and crossbeam at large buffers; the default MPMC direct send/recv is slower than both, but the
opt-in **`chan_create_spsc` (single-producer single-consumer)** direct throughput **beats crossbeam**
(see the honest comparison in [Performance](#performance)).

---

## Features

- **Three channel semantics** —— `cap == 0` unbuffered synchronous rendezvous; `cap > 0` bounded FIFO buffer.
- **MPMC** —— concurrency-safe for any number of producers/consumers.
- **Lock-free fast path** —— with no contention, `send`/`recv`/`select` bypass the mutex entirely and go straight to the atomic ring.
- **Go-like select** —— `chan_select` multiplexing, with uniform random choice when multiple cases are ready.
- **Complete blocking semantics** —— blocking / non-blocking (`try_`) / timeout (`_timeout`), three sets of interfaces.
- **close broadcast** —— wakes all blocked parties; a buffered channel can still be drained after close (Go semantics).
- **Reference counting** —— `chan_retain` / `chan_destroy` for safe lifetime management.
- **Portable park backend** —— futex on Linux, falling back to pthread mutex+cond on other platforms.
- **Pure C11 + pthreads** —— no third-party dependencies.

---

## Quick start

```c
#include "libchan.h"
#include <pthread.h>
#include <stdio.h>

static void *producer(void *arg) {
    chan_t *ch = arg;
    for (int i = 0; i < 5; i++)
        chan_send(ch, &i);
    chan_close(ch);          // signal consumer that data is done
    return NULL;
}

int main(void) {
    chan_t *ch = chan_create(sizeof(int), 2);   // buffered, cap=2

    pthread_t t;
    pthread_create(&t, NULL, producer, ch);

    int v;
    while (chan_recv(ch, &v) == CHAN_OK)         // returns CHAN_ERR_CLOSED after close+drain
        printf("recv %d\n", v);

    pthread_join(t, NULL);
    chan_destroy(ch);
    return 0;
}
```

`select` multiplexing:

```c
int a, b;
chan_select_case_t cases[2] = {
    { .ch = ch_a, .op = CHAN_OP_RECV, .data = &a },
    { .ch = ch_b, .op = CHAN_OP_RECV, .data = &b },
};
int idx = chan_select(cases, 2);                 // blocks until some case is ready
if (cases[idx].result == CHAN_OK)
    printf("case %d ready\n", idx);
```

See [`examples/`](examples/) for more examples.

---

## Build

Dependencies: CMake ≥ 3.16, a C11 compiler (GCC/Clang), pthreads.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

Or use the top-level `Makefile`:

```bash
make          # build the library (static + shared)
make test     # build and run all unit tests
make asan     # AddressSanitizer + UBSan
make tsan     # ThreadSanitizer (requires native Linux)
make bench    # build throughput benchmarks
```

Build artifacts: `build/libchan.so`, `build/libchan.a`, header `include/libchan.h`.

### Build options

| Option | Default | Description |
|------|------|------|
| `LIBCHAN_BUILD_SHARED` | `ON` | shared library `libchan.so` |
| `LIBCHAN_BUILD_STATIC` | `ON` | static library `libchan.a` |
| `LIBCHAN_BUILD_TESTS` | `ON` | unit tests |
| `LIBCHAN_BUILD_BENCH` | `OFF` | throughput benchmarks |
| `LIBCHAN_BUILD_EXAMPLES` | `ON` | example programs |
| `LIBCHAN_FORCE_PTHREAD_PARK` | `OFF` | force the pthread park backend (disable futex) |
| `LIBCHAN_SPIN_LIMIT` | `40` | spin count before entering the kernel park |

---

## API overview

| Category | Functions |
|------|------|
| **Lifecycle** | `chan_create` · `chan_destroy` · `chan_retain` |
| **Send** | `chan_send` · `chan_try_send` · `chan_send_timeout` |
| **Receive** | `chan_recv` · `chan_try_recv` · `chan_recv_timeout` |
| **Close** | `chan_close` · `chan_is_closed` |
| **Multiplexing** | `chan_select` · `chan_select_try` · `chan_select_timeout` |
| **Introspection** | `chan_len` · `chan_cap` |
| **Diagnostics** | `chan_strerror` |

Return codes `chan_err_t`: `CHAN_OK` / `CHAN_ERR_CLOSED` / `CHAN_ERR_TIMEOUT` /
`CHAN_ERR_WOULDBLOCK` / `CHAN_ERR_INVALID` / `CHAN_ERR_NOMEM`.

For full signatures and semantics, see [`doc/api_reference.md`](doc/api_reference.md).

---

## Design highlights

- **Lock-free ring (`src/ring_lf.c`)** —— a DPDK rte_ring-style lock-free MPMC queue:
  CAS-reserve `head` → write the slot → commit `tail`, capacity rounded up to a power of two.
- **Fast path** —— `send`/`recv` go straight to `ring_lf_push/pop` when
  `send_waiter_cnt == 0 && recv_waiter_cnt == 0`, with zero locks and zero syscalls.
- **Slow path** —— takes the mutex, registers a waiter, and parks only when blocking is needed.
- **Park backend (`src/park.c`)** —— Linux uses futex (`FUTEX_WAIT/WAKE`),
  spinning `LIBCHAN_SPIN_LIMIT` times before entering the kernel; other platforms fall back to pthread cond.
- **select (`src/select.c`)** —— first tries the lock-free fast path to execute a ready case;
  when none are ready, locks all channels in address order, registers a shared stub, parks, and cleans up on wake.

For architecture details, see [`doc/design.md`](doc/design.md).

---

## Performance

> 📈 **Per-commit trend:** every push to `master` records throughput on CI; browse the interactive
> per-commit charts (click a point to open its commit) at
> **[calcky.github.io/libchan/dev/bench](https://calcky.github.io/libchan/dev/bench/)**.

### Performance ladder (libchan itself, 13th Gen i7-13700H / WSL2, median)

From the hardware floor to the full blocking path, see the source of overhead layer by layer (`bench/run_showcase.sh`):

| Layer | ns/op | Mops/s | Description |
|------|------:|-------:|------|
| bare memcpy | 0.11 | 9487 | hardware floor |
| atomic_fetch_add | 3.82 | 261 | lock-free atomic lower bound |
| lock-free ring pure queue | 14.6 | 68 | the queue data structure itself |
| chan try_send/recv | 25.0 | 40 | + channel semantics, single-core no-wait |
| chan **MPMC** cross-core steady-state | 121 | 8.2 | + cross-core cache coherence: **cursor bounces per message** ← wall |
| chan **SPSC** cross-core steady-state | 23.9 | 42 | + cursor caching: **breaks the coherence wall** (same method, 5×) |

**Key point**: channel semantics cost only ~10 ns more than the pure queue (the fast path has
almost no software tax); the 24→121 jump is **cross-core cache coherence** —— MPMC reads, for every
message, a hot cursor that the peer repeatedly rewrites, hitting a physical wall (8 Mops);
`chan_create_spsc` uses **cursor caching** to eliminate this bounce, going 8→42 (5×) under the same
busy-poll method. SPSC blocking streaming is higher still (73 Mops in the table below: the producer
runs ahead and amortizes cursor reads, beating lock-step busy-poll).
For the full A/B tiered tables and methodology, see [`doc/benchmarks.md`](doc/benchmarks.md).

### Cross-language comparison (in Mops/s, higher is better, fixed message count + exact counting, i7-13700H/WSL2)

**Direct chan_send / chan_recv (core path):**

| Scenario | libchan (MPMC) | **libchan SPSC** | Go chan | crossbeam (Rust) |
|------|--------:|--------:|--------:|-----------------:|
| 1P+1C cap=64   |  7.70 | **64.28** | 32.50 | 50.91 |
| 1P+1C cap=1024 |  8.81 | **69.48** | 34.97 | 66.73 |
| 2P+2C cap=1024 |  6.26 | — | 27.66 | **48.55** |
| 4P+4C cap=1024 |  3.97 | — | 14.00 | **25.90** |
| 8P+8C cap=1024 |  3.18 | — |  4.06 | **11.77** |

**select multiplexing:**

| Scenario | libchan | Go chan | crossbeam (Rust) |
|------|--------:|--------:|-----------------:|
| 1P+1C cap=0 (unbuf) |  0.108 | **4.596** |  0.126 |
| 1P+1C cap=1024      | **19.043** | 12.870 | 16.301 |
| 4P+4C cap=1024      |  5.403 |  5.254 | **7.480** |
| 8P+8C cap=1024      |  3.736 |  2.441 | **5.979** |

**Conclusion (honest)**: pick the path by usage shape —— ① **single-producer single-consumer +
mostly direct → `chan_create_spsc`, direct throughput beats crossbeam** (64–69 vs 51–67, about 8× of
its own MPMC direct); ② multi-producer/multi-consumer + mostly direct → crossbeam/Go are faster
(libchan's default MPMC direct pays a fence + immediate-wakeup cost to fix the buffered lost-wakeup
deadlock); ③ mostly select multiplexing → libchan has the edge. The SPSC column only has values for
1P1C buffered (contract: at most one producer + one consumer). For the full tables, methodology, and
analysis, see [`doc/comparison.md`](doc/comparison.md), and the benchmark code in
[`bench/crosslang/`](bench/crosslang/).

---

## Testing

```bash
make test          # all unit tests
make asan          # ASan + UBSan
make tsan          # ThreadSanitizer (native Linux)
```

Coverage: basic send/recv, blocking/timeout, close semantics, MPMC, `select`, high-contention stress
tests (8P+8C × 50k messages, verifying zero loss).

---

## Project structure

```
libchan/
├── include/libchan.h     # public API
├── src/                  # implementation
│   ├── chan_core.c       #   lifecycle / close
│   ├── chan_send.c       #   send (fast path + slow path)
│   ├── chan_recv.c       #   recv
│   ├── select.c          #   chan_select multiplexing
│   ├── ring_lf.c         #   DPDK-style lock-free MPMC ring
│   ├── waitq.c           #   waiter wait queue
│   └── park.c            #   futex / pthread park backend
├── tests/                # unit + stress tests
├── examples/             # usage examples
├── bench/                # performance benchmarks (incl. cross-language comparison crosslang/)
└── doc/                  # documentation
```

---

## Documentation

| Document | Contents |
|------|------|
| [getting_started.md](doc/getting_started.md) | build and getting started |
| [api_reference.md](doc/api_reference.md) | full API reference |
| [architecture.md](doc/architecture.md) | illustrated walkthrough (layering / fast-slow paths / rendezvous timing) |
| [design.md](doc/design.md) | internal architecture and concurrency design |
| [benchmarks.md](doc/benchmarks.md) | internal benchmark methodology and data |
| [comparison.md](doc/comparison.md) | cross-language comparison with Go / Rust |
