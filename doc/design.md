# Internal Design

This document is intended for readers who want to understand libchan's internal implementation, perform performance tuning, or contribute code.

---

## Overall Architecture

libchan adopts the classic model of a **single mutex + FIFO wait queue**, consistent with the Go runtime's channel implementation strategy (Go is not lock-free either).

**Why not a lock-free MPMC queue?**

Lock-free approaches (such as the Vyukov bounded queue) have extremely low latency under no contention, but their correctness rests on the premise of not involving external synchronization. libchan requires:

1. **close semantics**: on close, atomically wake all waiters. This requires a strict ordering between "writing the close flag" and "dequeuing waiters"—a lock-free queue cannot directly express this cross-operation atomicity.
2. **blocking semantics**: a park/unpark mechanism is needed regardless; once this layer is added, the latency advantage of lock-free shrinks dramatically.
3. **ABA and memory reclamation**: C has no GC, so a lock-free queue needs hazard pointers or epoch-based reclamation, whose engineering complexity far exceeds the benefit.

Under low/moderate contention, spin backoff (see [Park Abstraction](#park-abstraction)) makes the "extremely short locked critical section" characteristic sufficient to compensate for the theoretical disadvantage of a single global lock.

---

## Core Data Structures

### `struct chan` (`src/libchan_internal.h`)

```
struct chan {
    /* ---- immutable fields (read-only after creation, no synchronization needed) ---- */
    size_t        elem_size;
    size_t        capacity;       /* 0 = unbuffered */

    /* ---- the following fields are protected by lock ---- */
    chan_mutex_t  lock;           /* CHAN_ALIGNED: aligned to 64-byte cache line */

    void         *buf;            /* ring buffer, allocated when capacity > 0 */
    size_t        head, tail;     /* ring buffer read/write pointers */
    size_t        count;          /* current element count, disambiguates full/empty */

    chan_waitq_t  send_waiters;   /* CHAN_ALIGNED */
    chan_waitq_t  recv_waiters;   /* CHAN_ALIGNED */

    /* ---- atomic fields (readable without holding the lock) ---- */
    _Atomic bool  closed;
    _Atomic int   refcount;
};
```

`CHAN_ALIGNED` (64-byte aligned) places `lock`, `send_waiters`, and `recv_waiters` on separate cache lines respectively, avoiding false sharing among the three under high concurrency.

### `chan_waiter_t` (`src/libchan_internal.h`)

The waiter node is **stack-allocated** on the stack of the thread calling `chan_send` / `chan_recv`, with no heap allocation overhead.

```
struct chan_waiter {
    struct chan_waiter *next;       /* intrusive linked-list pointer */
    void              *data;        /* SEND: points to caller data; RECV: points to output buffer */
    chan_op_t          op;
    chan_err_t         result;      /* filled in by the waker */
    size_t             case_idx;    /* which select case won (-1 when not a select) */
    chan_park_t       *wake_park;   /* wake target: normal waiter points to &self.park; select stub points to shared_park */
    _Atomic int       *select_state; /* NULL (normal); select stub points to the shared atomic state */
    chan_park_t        park;        /* park storage (used only by non-select waiters) */
};
```

The two pointer fields `wake_park` and `select_state` are the key to the select implementation (see [Select Multiplexing](#select-multiplexing)).

### `chan_waitq_t`

A FIFO singly-linked list with a head and tail pointer:

```c
typedef struct { chan_waiter_t *head, *tail; } chan_waitq_t;
```

All operations (`waitq_push`, `waitq_pop`, `waitq_remove`) are defined as inline functions, called while holding the lock, with O(1) time complexity (`waitq_remove` is O(n), but the wait queue is usually extremely short).

---

## Send Path (`src/chan_send.c`)

### Unbuffered channel (capacity == 0)

```
send(data):
  lock
  if closed → return CLOSED
  if recv_waiters non-empty:
    r = pop(recv_waiters)
    memcpy(r->data, data, elem_size)   ← data written directly into receiver's buffer
    r->result = OK
    unlock
    chan_park_wake(r->wake_park)        ← wake the receiver
    return OK
  if try_send → return WOULDBLOCK
  push self to send_waiters
  unlock
  park_wait(...)                        ← block
  return self.result
```

**Key design**: data is copied from the sender's stack into the receiver's output buffer while holding the lock, passing through no intermediate storage, with zero extra copies.

### Buffered channel (capacity > 0)

```
send(data):
  lock
  if closed → return CLOSED
  if recv_waiters non-empty AND ring is empty:
    deliver directly (same as unbuffered logic, skipping the ring buffer)
  if ring not full:
    ring_push(data)
    if recv_waiters non-empty → pop & wake one receiver
    unlock; return OK
  /* ring is full */
  if try_send → return WOULDBLOCK
  push self to send_waiters (self.data = &data)
  unlock; park_wait(...)               ← block
  /* after waking, the data has been pushed into the ring by the receiver, see below */
  return self.result
```

**"Receiver helps push" design**: after the receiver takes data out of the ring, if it finds a blocked sender, it **holds the lock** and pushes the sender's data directly into the ring, then wakes the sender to signal "done." After waking, the sender **does not need to re-acquire the lock to push**, avoiding the back-and-forth of "the buffer may be filled again after the sender wakes."

**Avoiding lost wakeup**: there is a race window between the lock-free fast path and "registering a waiter"—the fast path may push data into the ring after the receiver has checked the ring (empty) but before its `recv_waiter_cnt` is visible to the sender, causing the sender to skip the wake while the receiver holds the data in the ring and parks forever (two tightly coupled threads will deadlock outright). The fix:
- when registering, the waiter increments a `seq_cst` counter, then **re-checks the ring once before parking**;
- after a successful push/pop on the fast path, insert a `seq_cst` fence, then check the peer's `*_waiter_cnt`; if non-zero, acquire the lock to deliver the data to the already-parked waiter and wake it.

The `seq_cst` fences on both sides form a StoreLoad (Dekker) barrier: **either the waiter sees the data on its re-check after registering, or the fast path sees the count on its re-check and wakes it—the two cannot both be missed**. The cost is one extra fence per successful fast-path operation (about +7 ns/op for direct send/recv; the `chan_select` path does not go through here and is unaffected). Every delivery point checks the return value of `ring_pop`—`ring_pop` may still return false due to a race after `!ring_empty`, and if the return value is ignored and the receiver is woken with `CHAN_OK` regardless, it would deliver a nonexistent "phantom" message.

See the send/recv fast/slow path diagrams in [`architecture.md`](architecture.md).

---

## SPSC Fast Mode (`chan_create_spsc`)

`chan_create_spsc(elem_size, capacity)` creates an **opt-in** single-producer single-consumer channel.
Contract: **at most one producer thread + one consumer thread** (violation is UB). When the contract is satisfied, its throughput under concurrent streaming
load can reach about 9x that of the default MPMC channel (~73 Mops vs ~8 Mops on an i7-13700H, matching
Rust crossbeam). It **does not require or depend on `chan_close`**—suitable for in-process, long-lived, never-closed
inter-thread channels.

**Why it's fast — cursor caching**: the default MPMC ring must acquire-load
the peer's hot cursor on every operation (the producer reads `cons.tail`, the consumer reads `prod.tail`), and that cursor is release-
stored by the peer on every operation, so each message triggers one cross-core cache-line bounce (~100 ns on this machine). SPSC mode lets each side
cache a **lower bound** of the peer's cursor, reading back the real cursor only when the cache indicates "possibly full/empty." Single-owner caches
(`prod.cached_cons_tail` is read/written only by the producer, `cons.cached_prod_tail` only by the consumer) keep the
"slot data happens-before peer visibility" chain intact—this is precisely the root reason why a shared cache is **not** safe under MPMC but
is safe under SPSC. See the SAFETY CONTRACT in `src/ring_lf.h` and `ring_spsc_push/pop`.

**Why the per-op fence can be dropped**: the Dekker barrier from the previous section requires one extra `seq_cst`
fence per successful fast-path operation (~+7 ns/op). SPSC mode **drops this fence** on the hot path (the producer/consumer are each left with only a relaxed
peer-count check), pushing throughput past Rust. The cost: if a push/pop happens to hit the StoreLoad
race window (sub-microsecond) against the peer's "register and park," it may miss that wake. Under streaming load, "the next push/pop"
makes up for it; but in **request/response (ping-pong)** mode, the initiator will not send again before getting the response, so a missed wake becomes a
**permanent deadlock**.

**Park-side bounded recheck (backstop)—eliminating deadlock without depending on close or touching the hot path**: an indefinitely-waiting
SPSC park does **not** sleep indefinitely outright; instead it first parks with a bounded timeout `LIBCHAN_SPSC_PARK_BACKSTOP_NS`
(default 1 ms); after the timeout wakes it, it **rechecks the ring once while holding the lock**:
- caught the missed-wake push/pop → take it, return;
- channel already closed → return CLOSED;
- confirmed still empty/full → re-enqueue and **park indefinitely**.

Correctness: the race belongs only to a wait count that is "just written, still propagating"; once the recheck confirms the peer's state is unchanged, that count is
stably visible far beyond any propagation delay, and **any subsequent relaxed read by a peer operation will necessarily see it and wake it**—so
the subsequent indefinite park is safe and no longer needs any timeout. Net effect: each park round **wakes at most one extra time** (after ~1 ms),
then goes silent; **this is not polling**. The hot path (fast-path spin + lock-free send/recv) is unchanged, and throughput is unaffected. This protocol is implemented symmetrically on
the send/recv sides; see the `ch->spsc && timeout_ns < 0`
branch in `src/chan_send.c` / `src/chan_recv.c`.

> Verification: ping-pong deadlock stress test of 4 million rounds + ASan/UBSan + TSan (0 data races) + both futex/pthread
> park backends; SPSC checksum cap=1/64/1024, ×30 each, zero errors.

---

## Receive Path (`src/chan_recv.c`)

### Unbuffered

Symmetric to the send path: first check `send_waiters`; if present, take the data directly and wake the sender; otherwise park and wait.

### Buffered

```
recv(out):
  lock
  if ring non-empty:
    ring_pop(out)
    if send_waiters non-empty:
      s = pop(send_waiters)
      ring_push(s->data)               ← help the sender push the data (holding the lock)
      s->result = OK
      unlock
      wake(s->wake_park)
    else:
      unlock
    return OK
  if send_waiters non-empty:
    take data directly from the sender (same as unbuffered logic)
  if closed → return CLOSED
  push self to recv_waiters; unlock; park
  return self.result
```

**Drain-after-close semantics**: the `ring non-empty` check comes before the `closed` check, ensuring that even if the channel is already closed, data already in the ring buffer can be fully drained, consistent with Go's `for v := range ch` semantics.

---

## Close Semantics

`chan_close` (`src/chan_core.c`):

```
close():
  lock
  if already closed → return CHAN_ERR_CLOSED
  atomic_store(closed, true, release)
  waitq_close_all(send_waiters)   ← all blocked senders: result = CLOSED, wake
  waitq_close_all(recv_waiters)   ← all blocked receivers: result = CLOSED, wake
  unlock
```

`waitq_close_all` attempts a CAS on each waiter in the wait queue (for select stubs); on success it sets the result and wakes it.

**Why also directly wake blocked receivers on a buffered channel and return CLOSED?**  
A blocked receiver means the ring buffer is empty at this moment (otherwise they would not be blocked), so directly waking and signaling CLOSED is correct.

---

## Reference Counting

```
chan_retain:  atomic_fetch_add(refcount, 1, relaxed)

chan_destroy: if atomic_fetch_sub(refcount, 1, acq_rel) == 1:
                lock(); unlock()   ← drain barrier: ensure no thread is still in the critical section
                free(buf); free(ch)
```

The `acq_rel` semantics of `fetch_sub`:
- **release** side: the last `destroy` caller ensures all prior writes are visible to "the thread that makes the release decision."
- **acquire** side: the thread that makes the release decision can see the writes of all other threads before their respective `fetch_sub`, i.e., the results of all completed operations.

This is the standard reference-counting memory-ordering pattern of `std::shared_ptr` / `Arc`.

---

## Park Abstraction (`src/park.c`)

`chan_park_t` is the low-level primitive for thread sleep/wake, with two implementations:

### Linux futex implementation (default)

```c
typedef struct { _Atomic uint32_t word; } chan_park_t;

park_wait():
  /* spin phase: LIBCHAN_SPIN_LIMIT times (default 40) */
  for i in 0..SPIN_LIMIT:
    if word != 0 → return true   // already woken
    if i < 8: PAUSE/YIELD        // x86: pause, ARM: yield
    else: sched_yield()
  /* enter the kernel */
  syscall(FUTEX_WAIT, &word, 0, timeout)
  return word != 0

park_wake():
  atomic_store(word, 1, release)
  syscall(FUTEX_WAKE, &word, 1)
```

Advantages: `chan_park_t` occupies only 4 bytes (embedded in the waiter node), with no extra heap allocation; the `FUTEX_WAKE` kernel path is shorter than `pthread_cond_signal`.

### POSIX pthread fallback (non-Linux or when forced on)

```c
typedef struct { pthread_mutex_t mu; pthread_cond_t cv; bool signaled; } chan_park_t;
```

Standard mutex + condvar pattern, compatible with all POSIX platforms.

### Spin Backoff Strategy

By default spins 40 times: the first 8 use CPU pause/yield instructions (x86: `PAUSE`, ARM: `YIELD`), with extremely low latency (about 10–40ns each) and almost no impact on the L1 cache; subsequent iterations use `sched_yield()` to yield scheduling, avoiding wasting CPU on busy spinning.

Only if still not woken after spinning does it trap into the kernel (futex/cond_wait), with a typical context switch of about 1–5µs.

Setting `LIBCHAN_SPIN_LIMIT=0` fully disables spinning (suitable for embedded scenarios with tight CPU cores).

---

## Select Multiplexing (`src/select.c`)

### 1. Lock-order sorting (deadlock prevention)

```c
sort(cases, by=ch_pointer_value)   // ascending by channel address
lock_all(sorted_channels)          // all threads lock in the same order → no deadlock
```

All concurrent `chan_select` calls involving the same set of channels lock in the same order, so they cannot form a deadlock cycle.

### 2. Fast path (first scan)

While holding all locks, check whether each case is immediately feasible. If multiple are ready, choose one **uniformly at random** (using `rand() % nready`), avoiding systematically starving certain cases, consistent with the fairness requirement of the Go select spec.

Execute the chosen case, unlock all channels, and return the case index.

### 3. Wait path (second round, no ready case)

Core design: **shared state + stub waiter**

```c
_Atomic int  shared_state;   // initially WAITER_WAITING
chan_park_t   shared_park;    // all stubs signal here when woken

chan_waiter_t stubs[n];       // stack-allocated, one per case
for each case i:
    stubs[i].select_state = &shared_state
    stubs[i].wake_park    = &shared_park
    register stubs[i] on cases[i].ch's queue
unlock_all
park_wait(&shared_park, timeout)   // a single sleep point
```

### 4. CAS claim protocol

When a channel is ready to wake a waiter (`waitq_pop_sender/receiver` in the send/recv path), it calls `waiter_try_claim`:

```c
bool waiter_try_claim(chan_waiter_t *w):
    if w->select_state == NULL: return true   // normal waiter, claim directly
    CAS(w->select_state, WAITING → WOKEN)     // select stub: atomic race
    return success
```

- **CAS success (first winner)**: deliver data normally and call `chan_park_wake(w->wake_park)` to wake the shared park.
- **CAS failure (already claimed by another channel)**: the stub is harmlessly discarded (already popped from the queue; the select caller will find it not in the queue during cleanup), and no data delivery is performed.

This guarantees that even if multiple channels become ready almost simultaneously, the select caller is woken only once, and only one channel's operation actually completes.

### 5. Winner identification and cleanup

```c
lock_all
for each stub i:
    if stubs[i].result != CHAN_ERR_WOULDBLOCK:
        winner = i                    // the waker set the result (only the winner does)
    else:
        waitq_remove(cases[i].ch, &stubs[i])   // still in the queue, remove; no effect if already discarded
unlock_all
```

The winner stub's `result` has been set to `CHAN_OK` or `CHAN_ERR_CLOSED` by the waker; the other stubs' `result` retains the initial value `CHAN_ERR_WOULDBLOCK`, which is a reliable marker for identifying the winner.

### 6. Known limitation: delayed wakeup on buffered channels

To avoid MPMC serialization, select stubs **do not increment** `send_waiter_cnt`/`recv_waiter_cnt` (incrementing would make all threads immediately give up the lock-free fast path whenever any select parks, which measured down to <1 Mops/s in throughput). The cost is: **a select waiter parked on a buffered channel will not be woken promptly by lock-free fast-path operations on that channel**. It is only woken at the following moments:

- the ring is filled/drained until some thread switches to the slow path (the slow path checks the wait queue and delivers)—latency is bounded by the ring capacity; or
- the channel is `chan_close`'d (`waitq_close_all` wakes all stubs).

**Under normal sustained traffic this is just "batching latency"** (the producer fills the ring lock-free → one wake → the consumer drains in batch, which is precisely the source of select's high throughput).

**But there is one pathological scenario that hangs permanently**: a producer uses select/send to send a **small** amount of data (not enough to fill the ring) to a buffered channel, then **goes silent forever and does not `chan_close`**—at which point a select consumer parked on that channel never receives this data.

> **Workaround**: use `chan_close` to indicate "done sending" (Go's general convention). close always wakes all parked selects, fully avoiding this problem. In practice nearly all select usage ends with close, so this limitation is rarely hit.
>
> Direct `chan_send`/`chan_recv` (non-select) **are not subject to this limitation**—their waiters increment the waiter count + `seq_cst` handshake and are woken promptly (see the "Avoiding lost wakeup" subsection in the send path above). Fully fixing select's delayed wakeup requires waking stubs without breaking the batching fast path, which is a separate piece of design work.

### 7. Known limitation: tiny count skew under select MPMC

Under a high-contention scenario of **≥2 producers + 2 consumers** all going through select, there is an approximately **0.01% count skew** (slightly more received than sent). Root cause: the SEND branch of `execute_case_locked`, after a successful `ring_push`, wakes a parked receive stub and sets `result=CHAN_OK`, but the data actually went into the ring (popped by another consumer), while the woken stub treats this wake as "received one" and counts it—forming a duplicate count.

This is **deeply coupled** with the bounded delayed-wakeup protocol of §6: a complete fix would require redoing select's wake/claim protocol (attempted before, causing deadlock + a 2–3× throughput drop, since reverted). Given that the skew is extremely small (0.01%), affects only exact counting rather than throughput order-of-magnitude, and appears only under "all-select MPMC," it is currently **recorded as a known limitation** rather than forcibly fixed.

> **Not affected**: direct `chan_send`/`chan_recv` count exactly under any MPMC configuration (verified with a fixed-message-count benchmark, 120/120 groups); single-producer single-consumer (SPSC) select is also exact.

---

## Memory Ordering Cheat Sheet

| Operation | Memory ordering | Rationale |
|------|--------|------|
| `closed` write (`chan_close`) | `memory_order_release` | ensures writes before close are visible to threads observing `closed=true` |
| `closed` read (`chan_is_closed` and send/recv fast path) | `memory_order_acquire` | pairs with the release side |
| `select_state` CAS | `acq_rel` (success) / `relaxed` (failure) | success needs bidirectional acquire+release sync; failure only needs the current value |
| `refcount` fetch_sub | `memory_order_acq_rel` | lets the last destroy see the writes of all predecessor threads |
| `refcount` fetch_add | `memory_order_relaxed` | the count itself is atomic and needs no extra ordering |
| futex word | `memory_order_release` (write) / `memory_order_acquire` (read) | the futex syscall itself provides a kernel-level barrier |
| fields inside the lock-protected region | plain reads/writes | pthread_mutex's acquire/release semantics already provide full memory-ordering guarantees |

---

## Performance Data (reference values)

Test environment: WSL2 (Linux 6.6, x86-64), `RelWithDebInfo`, `LIBCHAN_SPIN_LIMIT=40`, 1 million messages:

| Mode | Thread pairs | Capacity | Throughput |
|------|----------|------|--------|
| unbuffered | 1 | 0 | ~3.5 Mops/s |
| buffered | 1 | 1024 | ~5.0 Mops/s |
| buffered | 2 | 64 | ~4.9 Mops/s |
| buffered | 2 | 1024 | ~9.7 Mops/s |
| buffered | 4 | 1024 | ~6.3 Mops/s |

Use `make bench` to re-run on your own machine (outputs a Mops/sec table).

---

## Source File Navigation

| File | Responsibility |
|------|------|
| `include/libchan.h` | public ABI: all types, function declarations |
| `src/libchan_internal.h` | internal data structures, inline functions, park declarations |
| `src/chan_core.c` | `chan_create` / `chan_destroy` / `chan_retain` / `chan_close` |
| `src/chan_send.c` | `chan_send_impl` (unified entry for the three variants) |
| `src/chan_recv.c` | `chan_recv_impl` |
| `src/select.c` | `chan_select_impl` (includes lock-order sorting, CAS claim) |
| `src/park.c` | futex and pthread condvar park implementations |
| `src/util.c` | `chan_spin_hint` (CPU pause/yield inline assembly) |
| `src/ring_buffer.c` | compilation-unit placeholder (logic inlined in `libchan_internal.h`) |
| `src/waitq.c` | compilation-unit placeholder (logic inlined in `libchan_internal.h`) |
