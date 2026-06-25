#ifndef LIBCHAN_INTERNAL_H
#define LIBCHAN_INTERNAL_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#include "libchan.h"
#include "ring_lf.h"

#define CHAN_CACHELINE 64
#define CHAN_ALIGNED   __attribute__((aligned(CHAN_CACHELINE)))

#define WAITER_WAITING  0
#define WAITER_WOKEN    1

/* Bounded lock-free retry count before chan_send/recv fall to the locked
 * slow path and park.  Spinning here keeps the waiter counts at 0 during
 * transient full/empty, so the opposite side stays on its fast path and a
 * buffered pipeline can actually pipeline instead of parking per message. */
#ifndef LIBCHAN_FASTPATH_SPIN
#define LIBCHAN_FASTPATH_SPIN 64
#endif

/* SPSC blocking-park backstop (nanoseconds).
 *
 * The SPSC fast paths omit the per-op seq_cst fence the MPMC paths use, so a
 * push/pop that races a parking waiter's count store may fail to wake it.  An
 * infinite-wait SPSC park therefore parks with THIS bounded timeout and, on
 * timeout, rechecks the ring once before parking indefinitely.  The recheck
 * catches the rare missed wake (the window is sub-µs even on weak memory); once
 * the ring is confirmed still empty/full, the waiter's count has been durably
 * visible far longer than any propagation delay, so every future op reliably
 * sees it and wakes the waiter — the indefinite re-park is then safe and adds
 * no further timeouts.  Net effect: exactly one extra wakeup per park episode,
 * then silence.  Keeps the producer/consumer hot path fence-free (surpassing
 * the per-op-fence throughput) while staying deadlock-free in request/response
 * patterns WITHOUT relying on chan_close. */
#ifndef LIBCHAN_SPSC_PARK_BACKSTOP_NS
#define LIBCHAN_SPSC_PARK_BACKSTOP_NS (1000000LL)   /* 1 ms */
#endif

/* ---- Park abstraction ---- */
#if defined(LIBCHAN_USE_FUTEX)
#  include <stdatomic.h>
   typedef struct { _Atomic uint32_t word; } chan_park_t;
#else
   typedef struct {
       pthread_mutex_t mu;
       pthread_cond_t  cv;
       bool            signaled;
   } chan_park_t;
#endif

void chan_park_init(chan_park_t *p);
void chan_park_destroy(chan_park_t *p);
bool chan_park_wait(chan_park_t *p, int64_t timeout_ns);
void chan_park_wake(chan_park_t *p);

/* ---- Waiter node (stack-allocated by the blocking thread) ----
 *
 * wake_park    — for regular waiters, points to &self.park.
 *                for select stubs, points to the shared park.
 * select_state — NULL for regular waiters.
 *                for select stubs, points to the shared _Atomic int that
 *                the first successful waker CAS's WAITING→WOKEN.
 *                If the CAS fails the stub was already claimed; discard it.
 */
struct chan_waiter {
    struct chan_waiter *next;
    void              *data;          /* SEND: src ptr; RECV: dst ptr */
    chan_op_t          op;
    chan_err_t         result;
    size_t             case_idx;      /* which select case index won (select only) */
    chan_park_t       *wake_park;     /* park to signal on wake */
    _Atomic int       *select_state;  /* shared CAS target; NULL for non-select */
    chan_park_t        park;          /* owns park storage for non-select waiters */
};
typedef struct chan_waiter chan_waiter_t;

/* ---- Wait queue (FIFO intrusive list) ---- */
typedef struct {
    chan_waiter_t *head;
    chan_waiter_t *tail;
} chan_waitq_t;

static inline void waitq_push(chan_waitq_t *q, chan_waiter_t *w) {
    w->next = NULL;
    if (q->tail) q->tail->next = w;
    else          q->head = w;
    q->tail = w;
}

static inline void waitq_push_front(chan_waitq_t *q, chan_waiter_t *w) {
    w->next = q->head;
    if (!q->head) q->tail = w;
    q->head = w;
}

static inline chan_waiter_t *waitq_pop(chan_waitq_t *q) {
    chan_waiter_t *w = q->head;
    if (!w) return NULL;
    q->head = w->next;
    if (!q->head) q->tail = NULL;
    w->next = NULL;
    return w;
}

static inline bool waitq_remove(chan_waitq_t *q, chan_waiter_t *target) {
    chan_waiter_t *prev = NULL, *cur = q->head;
    while (cur) {
        if (cur == target) {
            if (prev) prev->next = cur->next;
            else      q->head   = cur->next;
            if (q->tail == cur) q->tail = prev;
            cur->next = NULL;
            return true;
        }
        prev = cur; cur = cur->next;
    }
    return false;
}

static inline bool waitq_empty(const chan_waitq_t *q) { return !q->head; }

/* ---- Channel mutex ---- */
typedef pthread_mutex_t chan_mutex_t;
static inline void chan_mutex_init(chan_mutex_t *m)    { pthread_mutex_init(m, NULL); }
static inline void chan_mutex_destroy(chan_mutex_t *m) { pthread_mutex_destroy(m); }
static inline void chan_lock(chan_mutex_t *m)           { pthread_mutex_lock(m); }
static inline void chan_unlock(chan_mutex_t *m)         { pthread_mutex_unlock(m); }

/* ---- Channel structure ---- */
struct chan {
    size_t        elem_size;   /* immutable */
    size_t        capacity;    /* immutable; 0 = unbuffered (user-visible value) */
    bool          spsc;        /* immutable; opt-in single-producer-single-consumer
                                * fast path (cursor-cached ring ops). User contract:
                                * at most one producer thread and one consumer thread. */

    chan_mutex_t  lock CHAN_ALIGNED;

    chan_ring_lf_t ring;                    /* lock-free MPMC ring (buffered only) */
    _Atomic int    send_waiter_cnt;         /* # threads sleeping in send_waiters */
    _Atomic int    recv_waiter_cnt;         /* # threads sleeping in recv_waiters */

    chan_waitq_t  send_waiters CHAN_ALIGNED;
    chan_waitq_t  recv_waiters CHAN_ALIGNED;

    _Atomic bool  closed;
    _Atomic int   refcount;
};

/* ---- Ring helpers (thin wrappers; caller may or may not hold lock) ---- */
static inline bool ring_push(struct chan *ch, const void *data) {
    return ring_lf_push(&ch->ring, data);
}
static inline bool ring_pop(struct chan *ch, void *out) {
    return ring_lf_pop(&ch->ring, out);
}
static inline bool ring_full(const struct chan *ch) {
    uint32_t ph = atomic_load_explicit(&ch->ring.prod.head, memory_order_relaxed);
    uint32_t ct = atomic_load_explicit(&ch->ring.cons.tail, memory_order_relaxed);
    return (uint32_t)(ph - ct) >= ch->ring.capacity;
}
static inline bool ring_empty(const struct chan *ch) {
    uint32_t pt = atomic_load_explicit(&ch->ring.prod.tail, memory_order_relaxed);
    uint32_t csh = atomic_load_explicit(&ch->ring.cons.head, memory_order_relaxed);
    return pt == csh;
}

/* Fast-path ring dispatch: SPSC channels use the cursor-cached variants
 * (single-owner caches — safe only because the SPSC contract guarantees one
 * producer + one consumer); all other channels use the MPMC-safe variants.
 * The locked slow path and the lost-wakeup helpers always use the plain
 * ring_push/ring_pop wrappers (never the cached variants), so the caches are
 * only ever advanced by their owning side and at worst go stale-low.
 *
 * The SPSC variants are defined here as static inline (rather than out-of-line
 * in ring_lf.c) so the compiler inlines them straight into chan_send/recv,
 * removing the per-op cross-TU call overhead on the hot path. */
static inline bool ring_spsc_push(chan_ring_lf_t *r, const void *data) {
    /* Single producer ⇒ prod.head == prod.tail; it alone advances them. */
    uint32_t ph = atomic_load_explicit(&r->prod.head, memory_order_relaxed);
    /* Full check against the private cache (a lower bound on cons.tail); only
     * read the real hot cons.tail when the cache says we might be full. */
    if ((uint32_t)(ph - r->prod.cached_cons_tail) >= r->capacity) {
        r->prod.cached_cons_tail =
            atomic_load_explicit(&r->cons.tail, memory_order_acquire);
        if ((uint32_t)(ph - r->prod.cached_cons_tail) >= r->capacity)
            return false;   /* full */
    }
    memcpy(r->slots + (ph & r->mask) * r->elem_size, data, r->elem_size);
    atomic_store_explicit(&r->prod.tail, ph + 1, memory_order_release);
    atomic_store_explicit(&r->prod.head, ph + 1, memory_order_relaxed);
    return true;
}
static inline bool ring_spsc_pop(chan_ring_lf_t *r, void *out) {
    /* Single consumer ⇒ cons.head == cons.tail; it alone advances them. */
    uint32_t ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
    /* Item at ch exists iff ch is strictly before prod.tail (modular).  The
     * cache is a lower bound; reload the real prod.tail when ch is not proven
     * before it (ch >= cache, incl. when a slow-path pop advanced past a stale
     * cache).  `== cache` would be a bug — it misses ch > cache. */
    if ((int32_t)(ch - r->cons.cached_prod_tail) >= 0) {
        r->cons.cached_prod_tail =
            atomic_load_explicit(&r->prod.tail, memory_order_acquire);
        if ((int32_t)(ch - r->cons.cached_prod_tail) >= 0)
            return false;   /* empty */
    }
    memcpy(out, r->slots + (ch & r->mask) * r->elem_size, r->elem_size);
    atomic_store_explicit(&r->cons.tail, ch + 1, memory_order_release);
    atomic_store_explicit(&r->cons.head, ch + 1, memory_order_relaxed);
    return true;
}

static inline bool ring_lf_push_dispatch(struct chan *ch, const void *data) {
    return ch->spsc ? ring_spsc_push(&ch->ring, data)
                    : ring_lf_push(&ch->ring, data);
}
static inline bool ring_lf_pop_dispatch(struct chan *ch, void *out) {
    return ch->spsc ? ring_spsc_pop(&ch->ring, out)
                    : ring_lf_pop(&ch->ring, out);
}

/* ---- Waiter helpers ---- */
static inline void waiter_init(chan_waiter_t *w, void *data, chan_op_t op) {
    memset(w, 0, sizeof(*w));
    w->data         = data;
    w->op           = op;
    w->result       = CHAN_OK;
    w->case_idx     = (size_t)-1;
    w->select_state = NULL;
    chan_park_init(&w->park);
    w->wake_park    = &w->park;   /* default: own park */
}

static inline void waiter_destroy(chan_waiter_t *w) {
    chan_park_destroy(&w->park);
}

/* Try to claim a waiter (for select: CAS the shared state).
 * Returns true if this thread owns the delivery; false if already claimed.
 * Caller must hold the channel lock. */
static inline bool waiter_try_claim(chan_waiter_t *w) {
    if (!w->select_state) return true;   /* regular waiter: always claimable */
    int expected = WAITER_WAITING;
    return atomic_compare_exchange_strong_explicit(
        w->select_state, &expected, WAITER_WOKEN,
        memory_order_acq_rel, memory_order_relaxed);
}

/* Pop the first claimable sender from the queue.
 * Skips already-claimed select stubs (removes them from the queue but
 * doesn't try to deliver; the select caller cleans them up). */
static inline chan_waiter_t *waitq_pop_sender(chan_waitq_t *q) {
    while (q->head) {
        chan_waiter_t *w = waitq_pop(q);
        if (waiter_try_claim(w)) return w;
        /* stub already claimed by another channel: drop it, keep scanning */
    }
    return NULL;
}

static inline chan_waiter_t *waitq_pop_receiver(chan_waitq_t *q) {
    while (q->head) {
        chan_waiter_t *w = waitq_pop(q);
        if (waiter_try_claim(w)) return w;
    }
    return NULL;
}

/* Broadcast CHAN_ERR_CLOSED to every waiter in a queue (caller holds lock). */
static inline void waitq_close_all(chan_waitq_t *q) {
    chan_waiter_t *w;
    while ((w = waitq_pop(q))) {
        if (!waiter_try_claim(w)) continue;
        w->result = CHAN_ERR_CLOSED;
        chan_park_wake(w->wake_park);
    }
}

/* ---- Lost-wakeup recovery (buffered channels) ----
 *
 * A lock-free fast-path op and a slow-path waiter registration race at the
 * channel boundary: a sender may push to the ring after a receiver checked the
 * ring empty but before the receiver's recv_waiter_cnt store is visible, so the
 * sender skips waking and the receiver parks on data it never sees (and vice
 * versa for a full ring).  The fast paths pair a seq_cst fence with these
 * helpers: after a successful push/pop, if the opposite waiter count is
 * non-zero, hand parked waiters the buffered data and wake them.  Best-effort
 * and FIFO-preserving (oldest ring slot to the next receiver). */
static inline void chan_deliver_ring_to_receivers(struct chan *ch) {
    chan_lock(&ch->lock);
    while (!ring_empty(ch)) {
        chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
        if (!r) break;
        if (!ring_pop(ch, r->data)) {       /* lost the slot to a racing pop */
            waitq_push_front(&ch->recv_waiters, r);
            break;
        }
        r->result = CHAN_OK;
        chan_park_wake(r->wake_park);
    }
    chan_unlock(&ch->lock);
}

static inline void chan_admit_senders_to_ring(struct chan *ch) {
    chan_lock(&ch->lock);
    while (!ring_full(ch)) {
        chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
        if (!s) break;
        if (!ring_push(ch, s->data)) { waitq_push_front(&ch->send_waiters, s); break; }
        s->result = CHAN_OK;
        chan_park_wake(s->wake_park);
    }
    chan_unlock(&ch->lock);
}

/* Internal entry points */
chan_err_t chan_send_impl(chan_t *ch, const void *data, int64_t timeout_ns);
chan_err_t chan_recv_impl(chan_t *ch, void *out,        int64_t timeout_ns);
int        chan_select_impl(chan_select_case_t *cases, size_t n, int64_t timeout_ns);
void       chan_spin_hint(int iteration);

#endif /* LIBCHAN_INTERNAL_H */
