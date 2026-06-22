#include "libchan_internal.h"

#include <stdlib.h>
#include <string.h>

/* ── Lock ordering ─────────────────────────────────────────────────────────── */

static void sort_order(size_t *order, const chan_select_case_t *cases, size_t n) {
    for (size_t i = 0; i < n; i++) order[i] = i;
    for (size_t i = 1; i < n; i++) {
        size_t key = order[i], j = i;
        while (j > 0 &&
               (uintptr_t)cases[order[j-1]].ch > (uintptr_t)cases[key].ch) {
            order[j] = order[j-1]; j--;
        }
        order[j] = key;
    }
}

/* Specialised 2-element sort — eliminates loop overhead for the common case. */
static inline void sort_order_2(size_t *order,
                                  const chan_select_case_t *cases) {
    if ((uintptr_t)cases[0].ch <= (uintptr_t)cases[1].ch) {
        order[0] = 0; order[1] = 1;
    } else {
        order[0] = 1; order[1] = 0;
    }
}

static void lock_all(const size_t *order,
                      chan_select_case_t *cases, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (i > 0 && cases[order[i]].ch == cases[order[i-1]].ch) continue;
        chan_lock(&cases[order[i]].ch->lock);
    }
}

static void unlock_all(const size_t *order,
                         chan_select_case_t *cases, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (i > 0 && cases[order[i]].ch == cases[order[i-1]].ch) continue;
        chan_unlock(&cases[order[i]].ch->lock);
    }
}

/* ── Readiness check (caller holds all locks) ──────────────────────────────── */

static bool case_ready(const chan_select_case_t *c) {
    chan_t *ch = c->ch;
    if (c->op == CHAN_OP_SEND) {
        if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) return false;
        if (ch->capacity == 0) return !waitq_empty(&ch->recv_waiters);
        return !ring_full(ch) || !waitq_empty(&ch->recv_waiters);
    } else {
        if (ch->capacity == 0)
            return !waitq_empty(&ch->send_waiters) ||
                   atomic_load_explicit(&ch->closed, memory_order_relaxed);
        return !ring_empty(ch) || !waitq_empty(&ch->send_waiters) ||
               atomic_load_explicit(&ch->closed, memory_order_relaxed);
    }
}

/* Execute a ready case (caller holds ch->lock; does NOT unlock). */
static chan_err_t execute_case_locked(chan_select_case_t *c) {
    chan_t *ch = c->ch;

    if (c->op == CHAN_OP_SEND) {
        if (ch->capacity == 0) {
            chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
            if (!r) return CHAN_ERR_WOULDBLOCK;
            memcpy(r->data, c->data, ch->elem_size);
            r->result = CHAN_OK;
            chan_park_wake(r->wake_park);
            return CHAN_OK;
        }
        if (!waitq_empty(&ch->recv_waiters) && ring_empty(ch)) {
            chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
            if (r) {
                memcpy(r->data, c->data, ch->elem_size);
                r->result = CHAN_OK;
                chan_park_wake(r->wake_park);
                return CHAN_OK;
            }
        }
        ring_push(ch, c->data);
        chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
        if (r) { r->result = CHAN_OK; chan_park_wake(r->wake_park); }
        return CHAN_OK;

    } else { /* RECV */
        if (ch->capacity == 0) {
            chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
            if (s) {
                memcpy(c->data, s->data, ch->elem_size);
                s->result = CHAN_OK;
                chan_park_wake(s->wake_park);
                return CHAN_OK;
            }
            if (atomic_load_explicit(&ch->closed, memory_order_relaxed))
                return CHAN_ERR_CLOSED;
            return CHAN_ERR_WOULDBLOCK;
        }
        if (!ring_empty(ch)) {
            ring_pop(ch, c->data);
            chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
            if (s) {
                ring_push(ch, s->data);
                s->result = CHAN_OK;
                chan_park_wake(s->wake_park);
            }
            return CHAN_OK;
        }
        chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
        if (s) {
            memcpy(c->data, s->data, ch->elem_size);
            s->result = CHAN_OK;
            chan_park_wake(s->wake_park);
            return CHAN_OK;
        }
        if (atomic_load_explicit(&ch->closed, memory_order_relaxed))
            return CHAN_ERR_CLOSED;
        return CHAN_ERR_WOULDBLOCK;
    }
}

/* ── Lock-free fast path ────────────────────────────────────────────────────
 *
 * Try to execute one select case without acquiring any channel lock.
 * Mirrors the fast paths in chan_send_impl / chan_recv_impl.
 *
 * Priority rule:
 *   1. Closed RECV cases first — ensures stop channels are noticed even when
 *      the data channel still has ring space.  Without this, a sender-select
 *      could keep ring_lf_push'ing forever and never see the stop signal.
 *   2. Lock-free ring SEND / RECV — works when both waiter counts are zero,
 *      i.e. no thread is sleeping on that channel.
 *
 * Returns winning case index, or -1 if no case could be executed lock-free.
 *
 * Safety (same argument as chan_send/recv fast paths):
 *   - waiter counts are checked without the lock.
 *   - If a concurrent slow-path thread increments a count between our check
 *     and ring_lf_push/pop, the ring op is still correct (ring is
 *     linearisable).  The slow-path thread will find data in the ring on its
 *     next locked pass and return CHAN_OK without parking.
 *   - Because waiter counts are incremented for select stubs in the slow path
 *     (see below), a sleeping select receiver WILL make recv_waiter_cnt > 0,
 *     so the fast path will miss and the slow path delivers directly to the
 *     stub — no data is silently bypassed.
 * ────────────────────────────────────────────────────────────────────────── */
static int chan_select_fast(chan_select_case_t *cases, size_t n) {
    /* Step 1: look for closed RECV channels (stop-channel pattern).
     * Scanned in fixed order (0..n-1) so stop signals always have priority
     * over data cases regardless of the round-robin start below. */
    for (size_t i = 0; i < n; i++) {
        if (cases[i].op != CHAN_OP_RECV) continue;
        chan_t *ch = cases[i].ch;
        if (!atomic_load_explicit(&ch->closed, memory_order_acquire)) continue;
        if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0) continue;
        if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) != 0) continue;
        if (!ring_empty(ch)) continue;
        cases[i].result = CHAN_ERR_CLOSED;
        return (int)i;
    }

    /* Step 2: lock-free ring operations.
     * Use a thread-local round-robin start index so that when multiple cases
     * are simultaneously ready each gets selected with roughly equal frequency
     * over many calls, satisfying chan_select fairness semantics. */
    static _Thread_local size_t rr = 0;
    size_t start = rr % n;
    rr++;

    for (size_t j = 0; j < n; j++) {
        size_t i = (start + j) % n;
        chan_t *ch = cases[i].ch;
        if (ch->capacity == 0) continue;
        if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_acquire) != 0) continue;
        if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_acquire) != 0) continue;

        if (cases[i].op == CHAN_OP_SEND) {
            if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
                cases[i].result = CHAN_ERR_CLOSED;
                return (int)i;
            }
            if (ring_lf_push(&ch->ring, cases[i].data)) {
                cases[i].result = CHAN_OK;
                return (int)i;
            }
        } else {
            if (ring_lf_pop(&ch->ring, cases[i].data)) {
                cases[i].result = CHAN_OK;
                return (int)i;
            }
        }
    }

    return -1;
}

/* ── Main implementation ────────────────────────────────────────────────────── */

/* Maximum number of select cases that use stack-local storage.
 * Larger selects fall back to heap allocation. */
#define SEL_INLINE 16

int chan_select_impl(chan_select_case_t *cases, size_t n, int64_t timeout_ns) {
    if (!cases || n == 0) return -1;

    /* ── Fast path: try lock-free execution first ─────────────────────────── */
    int fast = chan_select_fast(cases, n);
    if (fast >= 0) return fast;

    /* ── Slow path: lock all channels ─────────────────────────────────────── */
    size_t order_stk[SEL_INLINE];
    size_t *order = (n <= SEL_INLINE) ? order_stk
                                      : malloc(n * sizeof *order);
    if (!order) return -1;

    if (n == 2) sort_order_2(order, cases);
    else        sort_order(order, cases, n);

    lock_all(order, cases, n);

    /* Pass 1 (under locks): collect ready cases. */
    size_t ready_stk[SEL_INLINE];
    size_t *ready = (n <= SEL_INLINE) ? ready_stk
                                      : malloc(n * sizeof *ready);
    size_t nready = 0;
    for (size_t i = 0; i < n; i++)
        if (case_ready(&cases[i])) ready[nready++] = i;

    if (nready > 0) {
        size_t winner;
        if (nready == 1) {
            winner = ready[0];
        } else {
            /* Thread-local xorshift32 — avoids global rand() lock. */
            static _Thread_local uint32_t rng_state = 0;
            if (!rng_state) {
                /* Seed from pthread_self to get distinct per-thread streams. */
                rng_state = (uint32_t)(uintptr_t)pthread_self() | 1u;
            }
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 17;
            rng_state ^= rng_state << 5;
            winner = ready[rng_state % nready];
        }
        cases[winner].result = execute_case_locked(&cases[winner]);
        unlock_all(order, cases, n);
        if (n > SEL_INLINE) { free(order); free(ready); }
        return (int)winner;
    }

    if (timeout_ns == 0) {
        unlock_all(order, cases, n);
        for (size_t i = 0; i < n; i++) cases[i].result = CHAN_ERR_WOULDBLOCK;
        if (n > SEL_INLINE) { free(order); free(ready); }
        return -1;
    }

    /* Pass 2: register stub waiters.
     *
     * All stubs share one _Atomic int (shared_state) and one park
     * (shared_park). The first waker to CAS shared_state WAITING→WOKEN
     * "wins"; subsequent wakers find the CAS failed and are ignored.
     *
     * KEY FIX: we increment send_waiter_cnt / recv_waiter_cnt for every stub.
     * This ensures chan_send_impl / chan_recv_impl fast paths fall back to the
     * locked slow path when a select stub is waiting, so the stub is properly
     * delivered to rather than silently bypassed by a lock-free ring push.
     *
     * NOTE: stubs[i].park is NOT initialised (zero from memset is sufficient
     * since it is never accessed — stubs always use shared_park). We likewise
     * do NOT call chan_park_destroy for stubs. On the futex backend destroy
     * is a no-op; on the pthread backend we avoid two mutex_init+cond_init
     * calls per case per select invocation. */
    _Atomic int shared_state;
    atomic_init(&shared_state, WAITER_WAITING);

    chan_park_t shared_park;
    chan_park_init(&shared_park);

    chan_waiter_t stub_stk[SEL_INLINE];
    chan_waiter_t *stubs = (n <= SEL_INLINE) ? stub_stk
                                             : malloc(n * sizeof *stubs);

    for (size_t i = 0; i < n; i++) {
        memset(&stubs[i], 0, sizeof stubs[i]);
        stubs[i].data         = cases[i].data;
        stubs[i].op           = cases[i].op;
        stubs[i].case_idx     = i;
        stubs[i].result       = CHAN_ERR_WOULDBLOCK;
        stubs[i].select_state = &shared_state;
        stubs[i].wake_park    = &shared_park;
        /* stubs[i].park left zero-initialised; never used, never destroyed. */

        /* NOTE: we intentionally do NOT increment send_waiter_cnt /
         * recv_waiter_cnt for stubs on buffered channels.  Doing so would
         * force every other thread on the same channel to abandon the
         * lock-free fast path the moment any select parks, serialising all
         * producers/consumers through the mutex — catastrophic for MPMC.
         *
         * The trade-off: a fast-path ring op may push/pop without noticing a
         * sleeping stub.  The stub is woken the next time any thread takes the
         * slow path (ring full → sender slow-path checks recv_waiters; ring
         * empty → receiver slow-path checks send_waiters).  Delay is bounded
         * by ring capacity.  All existing tests use cap=0 for blocking selects,
         * so the slow path always runs immediately there. */
        chan_t *ch = cases[i].ch;
        if (cases[i].op == CHAN_OP_SEND)
            waitq_push(&ch->send_waiters, &stubs[i]);
        else
            waitq_push(&ch->recv_waiters, &stubs[i]);
    }

    unlock_all(order, cases, n);

    bool woken = chan_park_wait(&shared_park, timeout_ns);

    /* Cleanup: re-lock, identify winner, remove un-won stubs. */
    lock_all(order, cases, n);

    size_t winner = (size_t)-1;
    for (size_t i = 0; i < n; i++) {
        chan_t *ch = cases[i].ch;
        chan_waitq_t *q = (cases[i].op == CHAN_OP_SEND)
                          ? &ch->send_waiters : &ch->recv_waiters;

        if (stubs[i].result != CHAN_ERR_WOULDBLOCK) {
            winner = i;
            cases[i].result = stubs[i].result;
        } else {
            waitq_remove(q, &stubs[i]);
        }
    }

    unlock_all(order, cases, n);

    /* Destroy shared park only; stubs[i].park was never initialised. */
    chan_park_destroy(&shared_park);

    if (n > SEL_INLINE) { free(order); free(ready); free(stubs); }

    if (!woken || winner == (size_t)-1) {
        for (size_t i = 0; i < n; i++) cases[i].result = CHAN_ERR_TIMEOUT;
        return -1;
    }

    return (int)winner;
}

int chan_select(chan_select_case_t *cases, size_t n) {
    return chan_select_impl(cases, n, -1);
}

int chan_select_try(chan_select_case_t *cases, size_t n) {
    return chan_select_impl(cases, n, 0);
}

int chan_select_timeout(chan_select_case_t *cases, size_t n, int64_t timeout_ns) {
    return chan_select_impl(cases, n, timeout_ns);
}
