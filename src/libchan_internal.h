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

/* Internal entry points */
chan_err_t chan_send_impl(chan_t *ch, const void *data, int64_t timeout_ns);
chan_err_t chan_recv_impl(chan_t *ch, void *out,        int64_t timeout_ns);
int        chan_select_impl(chan_select_case_t *cases, size_t n, int64_t timeout_ns);
void       chan_spin_hint(int iteration);

#endif /* LIBCHAN_INTERNAL_H */
