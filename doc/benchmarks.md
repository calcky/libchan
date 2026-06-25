# libchan Benchmark Results

**Environment**
- CPU: 13th Gen Intel Core i7-13700H (20 logical cores)
- OS: WSL2 / Linux 6.6.87.2-microsoft-standard-WSL2
- Compiler: GCC 13.3.0, `-O3`, `LIBCHAN_SPIN_LIMIT=40`
- Test date: 2026-06-24
- Benchmark programs: `bench/bench_showcase` (performance ladder), `bench/bench_lock_overhead`, `bench/bench_mpmc`

**Measurement method**
- Each data point runs 7 times, reporting the **median** and **min** (min ≈ interference-free lower bound).
- Run with `taskset` core pinning (`bench/run_showcase.sh`) to reduce thread-migration jitter.
- Reports both **ns/op** and **Mops/s**.
- ⚠️ **WSL2 caveat**: running on top of the Windows scheduler, park-involving scenarios (Tier B) jitter noticeably, so the numbers are an order-of-magnitude reference only; Tier A (lock-free fast path, no park) is more stable. For serious measurement, run on native Linux + isolcpus.

---

## 0. Performance ladder (the main showcase)

`bench/bench_showcase` strings "from the hardware floor to the full blocking path" into a single
ladder, making the source of overhead at each layer obvious at a glance. **Key insight: libchan's
lock-free fast path is close to the hardware floor; the slow parts are the OS's park/scheduling,
not the library itself.**

### Tier A · almost no park (measures "how fast the channel itself is")

| # | Scenario | med ns | min ns | Mops/s | What this layer adds |
|---|------|--------:|-------:|-------:|---------------|
| 1 | bare memcpy | 0.11 | 0.10 | 9487 | hardware floor |
| 2 | atomic_fetch_add | 3.82 | 3.77 | 261 | lock-free atomic primitive lower bound |
| 3 | lock-free ring pure queue | 14.6 | 14.5 | 68 | queue data structure (CAS + memory order + memcpy) |
| 4 | chan try_send/recv | 25.0 | 24.7 | 40 | + channel semantics (closed check / waiter gate), no wait |
| 5 | chan **MPMC** cross-core steady-state | 121.4 | 119.4 | 8.2 | + **cross-core cache coherence**: every message reads a hot cursor the peer repeatedly rewrites ← physical wall |
| 6 | chan **SPSC** cross-core steady-state | 23.9 | 22.1 | 42 | + **cursor caching**: eliminates the bounce, breaks the wall (same busy-poll method, 5×) |

> **How to read this table**:
> - 1→4 all happen on a **single core**, with no cross-core traffic: channel semantics cost only
>   ~10 ns more than the pure queue and ~20 ns more than the bare atomic, showing that libchan's
>   fast path has almost no "software tax."
> - The 4→5 jump (24→121 ns) is **not the lock, not park**, but **cross-core cache coherence**: under
>   MPMC two threads on different cores alternately read and write the same set of ring cursors, and
>   every message triggers one cross-core cache-line migration (~100 ns). This is the physical wall of
>   a **naive** cross-core queue (~8 Mops).
> - 5→6: `chan_create_spsc` uses **cursor caching** (each side caches a lower bound of the peer's
>   cursor, reading back the real hot cursor only when the cache shows full/empty) to eliminate this
>   bounce —— 8→42 Mops (5×) under the same busy-poll method, nearly back to the single-core try level.
>   The single-owner cache is safe only under the SPSC contract (1 producer + 1 consumer), so it is opt-in.
> - SPSC **blocking streaming** (Tier B #7) is even higher than the busy-poll steady-state here (73 vs 42):
>   streaming lets the producer run ahead and amortizes cursor reads across a batch, whereas lock-step
>   busy-poll intensifies cache contention.

### Tier B · blocking latency (includes park + OS scheduling, order-of-magnitude reference only)

| # | Scenario | med ns | min ns | Mops/s | Dominant overhead |
|---|------|--------:|-------:|-------:|---------|
| 7 | chan SPSC blocking cap=1024 | 13.8 | 13.2 | 72.8 | streaming fast path + occasional park (cursor caching in effect) |
| 8 | chan unbuffered rendezvous | 285.3 | 259.1 | 3.5 | must park every op (futex round trip) |
| 9 | chan MPMC 4P+4C cap=1024 | 208.1 | 162.0 | 4.8 | contention + park + scheduling |

> In Tier B each operation may enter a kernel park/wake, so what is measured is the **end-to-end
> latency of the channel as a synchronization primitive**, not the queue itself. This tier mainly
> reflects the OS's futex + scheduling efficiency (see §2 futex vs pthread), and when comparing across
> languages/implementations it especially must **not be ranked in the same table as Tier A**.

### Reference: GitHub Actions shared runner (measured in CI)

CI runs the same `bench_showcase` suite on `ubuntu-latest` (~4 logical cores, virtualized) on every
push. The absolute numbers are clearly lower than the dev machine above (fewer cores + virtualized
scheduling), and are a **trend-only reference**; but the ladder shape and the "SPSC breaks the wall,
blocking streaming beats busy-poll" conclusions are consistent.

| # | Scenario | med ns | min ns | Mops/s |
|---|------|--------:|-------:|-------:|
| 1 | bare memcpy | 0.16 | 0.15 | 6315.91 |
| 2 | atomic_fetch_add | 2.21 | 2.10 | 453.08 |
| 3 | lock-free ring pure queue | 11.53 | 10.89 | 86.71 |
| 4 | chan try_send/recv | 19.68 | 19.61 | 50.80 |
| 5 | chan **MPMC** cross-core steady-state (cache-coherence wall) | 176.88 | 171.40 | 5.65 |
| 6 | chan **SPSC** cross-core steady-state (cursor caching breaks the wall) | 63.43 | 63.04 | 15.77 |
| 7 | chan SPSC blocking cap=1024 | 27.09 | 21.34 | 36.92 |
| 8 | chan unbuffered rendezvous | 701.29 | 696.18 | 1.43 |
| 9 | chan MPMC 4P+4C cap=1024 | 1002.05 | 680.79 | 1.00 |

> SPSC cross-core (15.77) ≈ **2.8×** of MPMC cross-core (5.65) (5× on the dev machine; 4-core
> contention flattens the "wall" but it's still there); SPSC blocking streaming (36.9 Mops) >
> busy-poll steady-state (15.8), for the same reason as on the dev machine (the producer runs ahead and
> amortizes cursor reads). For the live trend, see the `gh-pages` dashboard maintained by
> github-action-benchmark.

---

## 0.5 Bulk API breaks the "cross-core wall" (`bench_bulk`)

The 121 ns / 8 Mops in §0 is the physical wall of the **single-element** MPMC cross-core
steady-state — but that wall charges one cross-core round trip **per CAS + per Phase-3 commit**,
**not per element**. `ring_lf_enqueue_burst` / `ring_lf_dequeue_burst` reserve k contiguous slots
in one CAS and commit the whole batch with one Phase-3 store, amortizing that fixed cross-core
traffic over k elements — **per-element cost drops ~1/k, until the memcpy itself dominates**.

`bench_bulk` (dev machine i7-13700H, `-O3`, core-pinned, one op = one element through
enqueue + dequeue):

| batch | 1T ns/elem | 1T Mops/s | 2P+2C ns/elem | 2P+2C Mops/s |
|------:|-----------:|----------:|--------------:|-------------:|
| 1     | 16.80      | 59.5      | 170.4         | 5.9 |
| 8     | 2.40       | 417       | 22.3          | 44.9 |
| 32    | 0.78       | 1284      | 5.74          | 174 |
| 128   | 0.26       | 3888      | 1.56          | **641** |

> **How to read this**:
> - `batch=1` equals the single-element `ring_lf_push/pop` baseline; the 2P+2C 170 ns / 5.9 Mops
>   is exactly the §0 cross-core wall (here 2P+2C contention is slightly heavier than §0's 1P1C).
> - **Key insight**: batching does not make cache coherence faster — it **carries more payload per
>   cross-core round trip**. Under 2P+2C, batch 1→128 lifts throughput from **5.9 → 641 Mops (109×)**,
>   far past the "physical wall", because the wall is per-CAS/per-commit, not per-element.
> - Single-thread batch 128 reaches 0.26 ns/elem (3888 Mops), the order of an 8 B in-L1 memcpy —
>   the lock-free bookkeeping is essentially amortized away, leaving only the raw data move.
> - Safety is unchanged: the bulk path runs the same reserve→write→commit three-phase protocol and
>   memory ordering (passes TSan), remains MPMC-safe, and never touches the SPSC cache fields.

---

## 1. Park backend comparison: futex vs pthread

`bench_lock_overhead` runs once each with futex (the Linux default) and pthread condvar
(`-DLIBCHAN_FORCE_PTHREAD_PARK=ON`), where each operation = send 1 int + recv 1 int.

```
scenario                              futex                  pthread condvar
                                 ns/op   Mops/s           ns/op   Mops/s
-------------------------------------------------------------------
1. bare memcpy (baseline)        0.10    9579          0.11    9443
2. atomic_fetch_add relaxed      3.86     259          3.86     259
3. mutex lock+nop+unlock (uncont) 2.64     379          2.57     389
4. mutex lock+nop+unlock (2 thr) 83.2    12.0         92.2    10.9
5. try_send+try_recv cap=1024    25.0    40.0         26.1    38.3
   (single thread, pure fast path + fence)
6a. send+recv cap=1024 (1+1 thr) 136     7.34          134     7.48
6b. send+recv unbuffered (1+1 thr) 215     4.65          774     1.29  ← key gap
6c. send+recv cap=1024 (2+2 thr) 205     4.88          133     7.54
6d. send+recv cap=1024 (4+4 thr) 2102     0.48         2483     0.40
```

**Key conclusions**

| Scenario | futex | pthread | Ratio |
|------|-------|---------|------|
| fast path (try_send+try_recv, no park) | 25.0 ns | 26.1 ns | ≈1.0× |
| buffered blocking (1+1, cap=1024) | 136 ns | 134 ns | ≈1.0× |
| **unbuffered blocking (1+1, must park)** | **215 ns** | **774 ns** | **futex 3.6× faster** |
| buffered high-contention (4+4, cap=1024) | 2102 ns | 2483 ns | comparable order (noisy) |

- **The fast path does not involve park**, so the two backends are nearly identical; fence overhead is the same for both.
- **An unbuffered channel forces a park/unpark on every rendezvous**, where the futex syscall overhead (~100 ns) is far below the pthread_cond_wait + mutex sequence (~500 ns), a **3.6×** gap.
- High-contention scenarios (2+2, 4+4) are noisy (std dev above ±20%); futex/pthread trade wins, so don't over-interpret.

---

## 2. N×M producer-consumer throughput (Mops/s)

`bench_mpmc` measures a fixed 1500 ms with 400 ms warmup, unit = completed send+recv pairs / second.

### capacity = 0 (unbuffered, forced rendezvous)

```
            1C      2C      4C      8C
  1P |    3.84    2.64    0.35    0.17
  2P |    2.76    2.34    1.97    0.76
  4P |    0.50    1.59    1.41    1.15
  8P |    0.15    0.83    1.14    1.15
```

### capacity = 64

```
            1C      2C      4C      8C
  1P |    4.87    4.38    0.44    0.12
  2P |    6.78    6.86    3.71    0.78
  4P |    0.42    4.63    3.71    1.80
  8P |    0.14    0.73    2.03    2.32
```

### capacity = 1024

```
            1C      2C      4C      8C
  1P |    7.23    6.44    1.23    0.15
  2P |   10.00    7.70    4.92    1.61
  4P |    1.66    5.60    4.43    3.47
  8P |    0.14    2.80    3.62    3.19
```

> Compared with the 2026-06-22 data, the **direct** send/recv throughput for buffered scenarios
> (cap=64/1024) has dropped somewhat (e.g. 1P+1C cap=1024 from ~13.6→7.2 Mops/s). The cause is that
> after the lost-wakeup fix, when the peer has already parked, the fast path takes the lock to deliver
> the data to it and wake it (previously the wakeup would be missed — a bug). Correctly waking a parked
> waiter has lock overhead by itself. When you need maximum direct throughput, switch to `chan_select`
> (which doesn't go through this path).

**Pattern summary**

1. **The diagonal (nP+nC) has the highest throughput**: balanced production/consumption triggers the slow path (park) least.
2. **Throughput collapses under severe asymmetry** (lower-left / upper-right corners near 0): the minority side becomes the bottleneck, the surplus threads all park and wait, and park/unpark overhead dominates.
   - 8P+1C: 0.15 Mops/s — 8 producers contend for one consumer, spending nearly all the time in the park/wake loop
   - 1P+8C: same, 0.15–0.19 Mops/s
3. **A large buffer (cap=1024) mitigates asymmetry**: it absorbs bursts so park is triggered less under asymmetry; 2P+1C goes from 10.2→11.5 Mops/s, 4P+2C from 4.87→5.89.
4. **Unbuffered (cap=0) is lower overall**: every send/recv must wait for the peer, with no concurrent pipelining possible.
5. **WSL2 degrades noticeably at high thread counts**: 4P+4C cap=1024 (4.56) is far below ideal linear scaling (1P+1C×4 = 54.4), mainly due to single-channel mutex serialization + WSL2 thread-scheduling overhead.

---

## 3. Re-running

```bash
# performance ladder (A/B tiers, core pinning + median)
bash bench/run_showcase.sh

# park comparison (futex vs pthread)
bash bench/run_park_cmp.sh

# N×M throughput
cmake -B build -DLIBCHAN_BUILD_BENCH=ON && cmake --build build --parallel
build/bench/bench_mpmc
```

---

## 4. Cross-language comparison

For the comparison with Go's built-in `chan` and Rust's `crossbeam-channel` (three paths: direct /
spsc / select, fixed message count + exact counting), see [`comparison.md`](comparison.md). One-line
conclusion: **pick the path by usage shape** —— single-producer single-consumer direct uses
`chan_create_spsc`, whose throughput **beats crossbeam** (consistent with §0 ladder #6); under
multi-producer/multi-consumer direct, crossbeam/Go are faster (libchan's default MPMC pays a fence +
immediate-wakeup cost for correctness); for select multiplexing libchan has the edge.
