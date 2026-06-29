#ifndef LIBCHAN_H
#define LIBCHAN_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Symbol visibility */
#if defined(_WIN32)
  #ifdef LIBCHAN_BUILDING
    #define LIBCHAN_API __declspec(dllexport)
  #else
    #define LIBCHAN_API __declspec(dllimport)
  #endif
#else
  #define LIBCHAN_API __attribute__((visibility("default")))
#endif

/* Opaque handle — internal layout is not part of the public ABI */
typedef struct chan chan_t;

typedef enum {
    CHAN_OK             = 0,
    CHAN_ERR_CLOSED     = 1,  /* channel closed: send fails; recv on closed+empty channel */
    CHAN_ERR_TIMEOUT    = 2,  /* timed operation expired */
    CHAN_ERR_WOULDBLOCK = 3,  /* try_send/try_recv found no immediate partner */
    CHAN_ERR_INVALID    = 4,  /* bad argument (NULL, size mismatch) */
    CHAN_ERR_NOMEM      = 5,  /* allocation failure */
} chan_err_t;

/* ---- Lifecycle ---- */

/* capacity == 0 → unbuffered (synchronous rendezvous)
 * capacity  > 0 → buffered (fixed-size FIFO ring buffer)
 * Returns NULL on allocation failure.  Initial refcount is 1. */
LIBCHAN_API chan_t *chan_create(size_t elem_size, size_t capacity);

/* Like chan_create, but enables a single-producer-single-consumer fast path
 * (cursor-cached ring ops) that is markedly faster for buffered throughput.
 *
 * CONTRACT: the caller guarantees AT MOST ONE producer thread and AT MOST ONE
 * consumer thread use the channel concurrently.  Using it with multiple
 * producers or multiple consumers is undefined behaviour (data corruption on
 * weakly-ordered CPUs).  For capacity == 0 it behaves exactly like
 * chan_create (the SPSC optimisation only applies to buffered rings).
 *
 * Blocking chan_send/chan_recv on an SPSC channel are deadlock-free WITHOUT any
 * reliance on chan_close, including request/response (ping-pong) patterns: a
 * parked waiter is always recovered (see the park-side backstop in design.md).
 * Suitable for long-lived, never-closed in-process thread-to-thread channels. */
LIBCHAN_API chan_t *chan_create_spsc(size_t elem_size, size_t capacity);

/* Decrement refcount; free when it reaches zero. */
LIBCHAN_API void    chan_destroy(chan_t *ch);

/* Increment refcount — for callers that store an extra reference. */
LIBCHAN_API chan_t *chan_retain(chan_t *ch);

/* ---- Close ---- */

/* Signal no more data will be sent.  Unblocks all waiting senders
 * (→ CHAN_ERR_CLOSED) and, once the buffer drains, all waiting receivers.
 * Idempotent: second call returns CHAN_ERR_CLOSED without crashing. */
LIBCHAN_API chan_err_t chan_close(chan_t *ch);

LIBCHAN_API bool chan_is_closed(const chan_t *ch);

/* ---- Send ---- */

LIBCHAN_API chan_err_t chan_send(chan_t *ch, const void *data);
LIBCHAN_API chan_err_t chan_try_send(chan_t *ch, const void *data);
/* timeout_ns < 0  → wait forever (same as chan_send)
 * timeout_ns == 0 → no wait       (same as chan_try_send) */
LIBCHAN_API chan_err_t chan_send_timeout(chan_t *ch, const void *data, int64_t timeout_ns);

/* ---- Recv ---- */

LIBCHAN_API chan_err_t chan_recv(chan_t *ch, void *out);
LIBCHAN_API chan_err_t chan_try_recv(chan_t *ch, void *out);
LIBCHAN_API chan_err_t chan_recv_timeout(chan_t *ch, void *out, int64_t timeout_ns);

/* ---- Burst (bulk) — non-blocking, buffered channels only ----
 *
 * Move up to `n` contiguous elements (each of elem_size bytes) in ONE batch:
 * `data`/`out` point to an array of n elements.  These reserve a run of slots
 * with a single CAS and one commit, amortising the per-op cross-core cost over
 * the batch (see ring_lf.h / doc/benchmarks.md §0.5).
 *
 * Semantics — best-effort, never block, never park:
 *   - return the number of elements actually moved (0..n);
 *   - chan_try_send_burst returns < n (possibly 0) when the ring fills, and 0
 *     if the channel is closed or unbuffered (capacity == 0);
 *   - chan_try_recv_burst returns < n (possibly 0) when the ring drains, and 0
 *     on an unbuffered channel.  Like chan_recv it keeps draining buffered items
 *     after close (Go semantics); 0 once empty.
 *
 * MPMC-safe and usable on any channel (including chan_create_spsc); a successful
 * burst wakes a parked single-element peer exactly as the single-element fast
 * path does, so bursts may be freely mixed with blocking chan_send/chan_recv.
 * Not strictly FIFO-fair toward already-parked senders (a burst may fill the
 * ring ahead of them); they are admitted by the next receiver. */
LIBCHAN_API size_t chan_try_send_burst(chan_t *ch, const void *data, size_t n);
LIBCHAN_API size_t chan_try_recv_burst(chan_t *ch, void *out, size_t n);

/* ---- Introspection ---- */

LIBCHAN_API size_t chan_len(const chan_t *ch);   /* elements currently buffered */
LIBCHAN_API size_t chan_cap(const chan_t *ch);   /* buffer capacity (0 = unbuffered) */

/* ---- Select ---- */

typedef enum {
    CHAN_OP_SEND = 0,
    CHAN_OP_RECV = 1,
} chan_op_t;

typedef struct {
    chan_t     *ch;     /* target channel */
    chan_op_t   op;     /* SEND or RECV */
    void       *data;   /* SEND: pointer to data to send (read-only)
                           RECV: pointer to output buffer (written on success) */
    chan_err_t  result; /* filled in by chan_select* on return */
} chan_select_case_t;

/* Block until exactly one ready case is executed; returns its index.
 * Returns -1 on invalid arguments.
 * When multiple cases are ready simultaneously, one is chosen uniformly
 * at random (matches Go select fairness semantics). */
LIBCHAN_API int chan_select(chan_select_case_t *cases, size_t n);

/* Non-blocking: if no case is immediately ready, returns -1 without
 * modifying any channel state (equivalent to Go select+default). */
LIBCHAN_API int chan_select_try(chan_select_case_t *cases, size_t n);

/* timeout_ns < 0  → wait forever (same as chan_select)
 * timeout_ns == 0 → no wait       (same as chan_select_try) */
LIBCHAN_API int chan_select_timeout(chan_select_case_t *cases, size_t n,
                                    int64_t timeout_ns);

/* ---- Diagnostics ---- */
LIBCHAN_API const char *chan_strerror(chan_err_t err);

#ifdef __cplusplus
}
#endif

#endif /* LIBCHAN_H */
