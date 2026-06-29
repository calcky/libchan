# API Reference

This document covers all public interfaces in `include/libchan.h`.

---

## Header

```c
#include "libchan.h"
```

Linking requires `-lchan -lpthread` (for the static library, use `-l:libchan.a -lpthread`).

---

## Types

### `chan_t`

```c
typedef struct chan chan_t;
```

An opaque handle for a channel. Its internal layout is not part of the public ABI, and its fields should not be accessed directly. All operations are performed through function calls.

---

### `chan_err_t`

```c
typedef enum {
    CHAN_OK             = 0,
    CHAN_ERR_CLOSED     = 1,
    CHAN_ERR_TIMEOUT    = 2,
    CHAN_ERR_WOULDBLOCK = 3,
    CHAN_ERR_INVALID    = 4,
    CHAN_ERR_NOMEM      = 5,
} chan_err_t;
```

| Value | Meaning |
|-------|---------|
| `CHAN_OK` | The operation completed successfully |
| `CHAN_ERR_CLOSED` | The channel was already closed on send; or, on receive, the channel is closed and the buffer is empty |
| `CHAN_ERR_TIMEOUT` | The operation did not complete before the timeout deadline expired |
| `CHAN_ERR_WOULDBLOCK` | A non-blocking operation (`try_*`) could not complete immediately |
| `CHAN_ERR_INVALID` | An argument was invalid (e.g. a NULL pointer) |
| `CHAN_ERR_NOMEM` | Memory allocation failed during `chan_create` |

---

### `chan_op_t`

```c
typedef enum {
    CHAN_OP_SEND = 0,
    CHAN_OP_RECV = 1,
} chan_op_t;
```

Used in `chan_select_case_t` to specify whether the case is a send or a receive.

---

### `chan_select_case_t`

```c
typedef struct {
    chan_t     *ch;
    chan_op_t   op;
    void       *data;
    chan_err_t  result;
} chan_select_case_t;
```

| Field | Description |
|-------|-------------|
| `ch` | The target channel; must not be NULL |
| `op` | `CHAN_OP_SEND` or `CHAN_OP_RECV` |
| `data` | **SEND**: a pointer to the data to send (read-only; its size must match the `elem_size` from `chan_create`)<br>**RECV**: a pointer to the receive buffer (written on success) |
| `result` | Filled in by the library after `chan_select*` returns, giving the result of this case's operation (`CHAN_OK`, `CHAN_ERR_CLOSED`, etc.) |

---

## Lifecycle

### `chan_create`

```c
chan_t *chan_create(size_t elem_size, size_t capacity);
```

Creates a new channel.

| Parameter | Description |
|-----------|-------------|
| `elem_size` | The byte size of each element; must be > 0 |
| `capacity` | `0` = unbuffered (synchronous handshake); `> 0` = buffered (a fixed-capacity FIFO queue) |

**Return value**: on success, a non-NULL channel handle (with an initial reference count of 1); on insufficient memory, `NULL`.

**Thread safety**: yes (a newly created object; the call itself involves no concurrency).

---

### `chan_destroy`

```c
void chan_destroy(chan_t *ch);
```

Decrements the reference count. When the reference count reaches zero, all internal resources are freed.

**Call contract**: before calling, you must ensure that this thread has completed all operations on `ch`, and `ch` must not be accessed afterward.

**Thread safety**: yes (atomic reference counting).

---

### `chan_retain`

```c
chan_t *chan_retain(chan_t *ch);
```

Increments the reference count and returns `ch` itself. Useful when multiple threads each need to hold an independent reference.

**Example**:

```c
chan_t *ref = chan_retain(ch);   /* there are now two references */
/* ... thread A uses ref, thread B uses ch ... */
chan_destroy(ref);               /* thread A releases */
chan_destroy(ch);                /* thread B releases; destroyed for real here */
```

---

## Closing

### `chan_close`

```c
chan_err_t chan_close(chan_t *ch);
```

Closes the channel, with the following semantics:

- All currently blocked **senders** are woken immediately and return `CHAN_ERR_CLOSED`.
- All currently blocked **receivers** are woken immediately and return `CHAN_ERR_CLOSED` (unbuffered channel), or return `CHAN_ERR_CLOSED` only after the buffer is drained (buffered channel; see `chan_recv`).
- After closing, `chan_send` / `chan_try_send` always return `CHAN_ERR_CLOSED`.
- Idempotent: repeated calls return `CHAN_ERR_CLOSED` and do not crash.

**Return value**: the first close returns `CHAN_OK`; an already-closed channel returns `CHAN_ERR_CLOSED`.

**Thread safety**: yes, callable from any thread.

---

### `chan_is_closed`

```c
bool chan_is_closed(const chan_t *ch);
```

A lock-free, fast check of whether the channel is closed (based on an atomic read). The return value reflects only the state at the moment of the call and provides no synchronization guarantee -- in multithreaded scenarios it should be used together with return-value checks, not as the sole basis for a decision.

---

## Sending

All send functions transfer a **shallow copy** (`memcpy`) of the memory pointed to by `data`, with the size determined by the `elem_size` from `chan_create`.

### `chan_send`

```c
chan_err_t chan_send(chan_t *ch, const void *data);
```

Blocking send; equivalent to `chan_send_timeout(ch, data, -1)`.

| Return value | Condition |
|--------------|-----------|
| `CHAN_OK` | The data was successfully delivered to a receiver or enqueued |
| `CHAN_ERR_CLOSED` | The channel is closed |
| `CHAN_ERR_INVALID` | `ch` or `data` is NULL |

---

### `chan_try_send`

```c
chan_err_t chan_try_send(chan_t *ch, const void *data);
```

Non-blocking send; equivalent to `chan_send_timeout(ch, data, 0)`. If no receiver is ready (unbuffered) or the buffer is full (buffered), it returns `CHAN_ERR_WOULDBLOCK` immediately without blocking.

---

### `chan_send_timeout`

```c
chan_err_t chan_send_timeout(chan_t *ch, const void *data, int64_t timeout_ns);
```

A send with a timeout.

| `timeout_ns` | Behavior |
|--------------|----------|
| `< 0` | Wait forever (equivalent to `chan_send`) |
| `== 0` | Do not wait (equivalent to `chan_try_send`) |
| `> 0` | Wait at most `timeout_ns` nanoseconds |

On timeout it returns `CHAN_ERR_TIMEOUT`, and the channel state is unaffected (the data was not sent).

---

## Receiving

### `chan_recv`

```c
chan_err_t chan_recv(chan_t *ch, void *out);
```

Blocking receive; on success it writes the data into `out`.

| Return value | Condition |
|--------------|-----------|
| `CHAN_OK` | The data was written into `*out` |
| `CHAN_ERR_CLOSED` | The channel is closed **and** the buffer is empty (the close semantics of a buffered channel: existing data is drained first, then this error is returned) |
| `CHAN_ERR_INVALID` | `ch` or `out` is NULL |

**Note**: a buffered channel can still receive enqueued data after close, until it is drained, matching the behavior of Go's `for v := range ch`.

---

### `chan_try_recv`

```c
chan_err_t chan_try_recv(chan_t *ch, void *out);
```

Non-blocking receive. If no data is available, it returns `CHAN_ERR_WOULDBLOCK` immediately.

---

### `chan_recv_timeout`

```c
chan_err_t chan_recv_timeout(chan_t *ch, void *out, int64_t timeout_ns);
```

A receive with a timeout; `timeout_ns` has the same semantics as `chan_send_timeout`.

---

## Burst (bulk transfer)

Non-blocking batch operations that move up to `n` contiguous elements in **one** reservation, amortizing the per-operation cross-core atomic cost over the whole batch. They apply to **buffered channels only** and never block or park.

`data` / `out` point to an array of `n` elements, each of the `elem_size` given to `chan_create`.

### `chan_try_send_burst`

```c
size_t chan_try_send_burst(chan_t *ch, const void *data, size_t n);
```

Enqueues as many of the `n` elements as currently fit, in order.

**Return value**: the number of elements actually enqueued (`0..n`). Returns `0` when the ring is full, when the channel is closed, or when the channel is unbuffered (`capacity == 0`).

### `chan_try_recv_burst`

```c
size_t chan_try_recv_burst(chan_t *ch, void *out, size_t n);
```

Dequeues up to `n` buffered elements, oldest first, into `out`.

**Return value**: the number of elements actually received (`0..n`). Returns `0` when the ring is empty or the channel is unbuffered. Like `chan_recv`, it keeps draining buffered items after `chan_close`, returning `0` only once the buffer is empty.

**Notes**

- MPMC-safe and usable on any channel, including one created with `chan_create_spsc`.
- A successful burst wakes a parked single-element peer exactly as the single-element fast path does, so bursts may be freely mixed with blocking `chan_send` / `chan_recv` without losing wakeups.
- Not strictly FIFO-fair toward already-parked senders (a burst may fill the ring ahead of them); such senders are admitted by the next receiver.
- Performance: see `bench/bench_showcase` row `6b` and [`benchmarks.md`](benchmarks.md) §0.5 — batching lifts MPMC cross-core throughput far past the single-element "cache-coherence wall".

---

## Introspection

### `chan_len`

```c
size_t chan_len(const chan_t *ch);
```

Returns the current number of elements in the buffer (requires taking the lock, with brief blocking). An unbuffered channel always returns 0.

### `chan_cap`

```c
size_t chan_cap(const chan_t *ch);
```

Returns the buffer capacity (fixed after creation, no lock required). An unbuffered channel returns 0.

---

## Select Multiplexing

### `chan_select`

```c
int chan_select(chan_select_case_t *cases, size_t n);
```

Blocks until at least one of the cases in `cases[0..n-1]` is ready, executes that case, and returns its index.

- If multiple cases are ready at once, one is chosen **uniformly at random** (fairness, consistent with the Go select specification).
- Returns `-1` if the arguments are invalid (`cases == NULL` or `n == 0`).
- After execution, `cases[winner].result` is set to the result of that case's operation (`CHAN_OK` or `CHAN_ERR_CLOSED`).

**Lock-ordering guarantee**: internally it locks in ascending order of channel pointer address, so concurrent select calls cannot deadlock due to inconsistent lock ordering.

> **Known limitation (buffered channels)**: a select waiter parked on a buffered channel is not woken promptly by lock-free fast-path send/recv on that channel -- it is woken only once the ring fills/empties enough that some thread switches to the slow path, or when the channel is closed via `chan_close`. Under sustained traffic this is merely batching latency; but if a producer sends a little data and then **falls silent forever without closing**, a parked select consumer may never receive it. **Workaround**: use `chan_close` to signal that sending is done (close always wakes all parked selects). Direct `chan_send`/`chan_recv` are not subject to this limitation. See the Select section of [`design.md`](design.md) for details.

---

### `chan_select_try`

```c
int chan_select_try(chan_select_case_t *cases, size_t n);
```

A non-blocking version. If no case is immediately ready, it returns `-1` and **does not modify any channel state** (equivalent to Go's `select { … default: }`).

---

### `chan_select_timeout`

```c
int chan_select_timeout(chan_select_case_t *cases, size_t n, int64_t timeout_ns);
```

A select with a timeout; `timeout_ns` has the same semantics as `chan_send_timeout`. On timeout it returns `-1` and every `cases[i].result` is set to `CHAN_ERR_TIMEOUT`.

---

## Diagnostics

### `chan_strerror`

```c
const char *chan_strerror(chan_err_t err);
```

Returns a static string corresponding to the error code, for use in logging and debugging. The return value points to a static constant and should not be freed or modified.

---

## Thread Safety

| Function category | Thread-safe |
|-------------------|-------------|
| `chan_create` | Yes (creates an independent object) |
| `chan_destroy` / `chan_retain` | Yes (atomic reference counting) |
| `chan_send*` / `chan_recv*` / `chan_try_*_burst` | Yes (protected by an internal mutex; bursts use the lock-free ring + slow-path wake) |
| `chan_close` | Yes (atomic + mutex, idempotent) |
| `chan_is_closed` | Yes (atomic read, weak consistency) |
| `chan_len` / `chan_cap` | Yes (`len` takes the lock, `cap` needs no lock) |
| `chan_select*` | Yes (unified lock ordering prevents deadlock) |

**Additional contract for `chan_destroy`**: the caller must ensure all of this thread's operations have completed before calling, and must not use the pointer afterward. This is an inherent constraint of reference-counting semantics; with no GC assistance in C, callers must observe it by discipline.
