#include "libchan_internal.h"

chan_err_t chan_recv_impl(chan_t *ch, void *out, int64_t timeout_ns) {
    if (!ch || !out) return CHAN_ERR_INVALID;

    /*
     * Buffered fast path (lock-free), with bounded spin before parking.
     *
     * Retrying the lock-free pop a few times on a transiently-empty ring keeps
     * recv_waiter_cnt at 0, so a concurrent producer stays on ITS fast path and
     * can run ahead instead of being dragged onto the locked slow path the
     * instant we would otherwise park.  This is what lets a buffered pipeline
     * actually pipeline (producer fills while we briefly spin), rather than
     * ping-ponging through park on every message.
     *
     * Bail out of the spin immediately if any waiter is registered (a parked
     * sender must be served via the slow path) or the channel is closed.
     */
    if (ch->spsc) {
        /* Dedicated lean SPSC consumer fast path.  Single consumer ⇒ skip the
         * recv_waiter_cnt check; pop first and only check send_waiter_cnt (to
         * admit a parked producer) on success.  No per-op fence. */
        for (int spin = 0; ; spin++) {
            if (ring_spsc_pop(&ch->ring, out)) {
                if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0)
                    chan_admit_senders_to_ring(ch);
                return CHAN_OK;
            }
            /* ring empty */
            if (atomic_load_explicit(&ch->closed, memory_order_acquire)) break;
            if (spin >= LIBCHAN_FASTPATH_SPIN) break;
            chan_spin_hint(spin & 7);
        }
        /* fall through to the locked slow path */
    } else if (ch->capacity > 0) {
        for (int spin = 0; ; spin++) {
            if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0) break;
            if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) != 0) break;
            if (ring_lf_pop(&ch->ring, out)) {
                /* Wake a parked sender if one is present.  MPMC pairs a seq_cst
                 * fence with the sender's seq_cst registration. */
                atomic_thread_fence(memory_order_seq_cst);
                if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0)
                    chan_admit_senders_to_ring(ch);
                return CHAN_OK;
            }
            /* ring empty */
            if (atomic_load_explicit(&ch->closed, memory_order_acquire)) break;
            if (spin >= LIBCHAN_FASTPATH_SPIN) break;
            chan_spin_hint(spin & 7);  /* pure pause, no yield */
        }
        /* fall through to the locked slow path */
    }

    /* ---------- Slow path: acquire lock ---------- */
    chan_lock(&ch->lock);

    if (ch->capacity == 0) {
        /* ---- Unbuffered ---- */
        chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
        if (s) {
            memcpy(out, s->data, ch->elem_size);
            s->result = CHAN_OK;
            chan_unlock(&ch->lock);
            chan_park_wake(s->wake_park);
            return CHAN_OK;
        }
        if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
            chan_unlock(&ch->lock);
            return CHAN_ERR_CLOSED;
        }
        if (timeout_ns == 0) { chan_unlock(&ch->lock); return CHAN_ERR_WOULDBLOCK; }

        chan_waiter_t self;
        waiter_init(&self, out, CHAN_OP_RECV);
        atomic_fetch_add_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
        waitq_push(&ch->recv_waiters, &self);
        chan_unlock(&ch->lock);

        if (!chan_park_wait(self.wake_park, timeout_ns)) {
            chan_lock(&ch->lock);
            waitq_remove(&ch->recv_waiters, &self);
            chan_unlock(&ch->lock);
            atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
            waiter_destroy(&self);
            return CHAN_ERR_TIMEOUT;
        }
        atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return self.result;
    }

    /* ---- Buffered slow path ---- */

    /*
     * Drain buffered data first — even after close (Go semantics).
     *
     * "Receiver helps push": when we pop a slot from the ring there is now
     * at least one free slot.  We try to push a waiting sender's data into
     * the ring.  ring_push CAN fail if two stale fast-path senders both
     * reserved slots (CAS'd prod.head) before the receiver's ring_pop
     * committed cons.tail, leaving prod.head - cons.tail >= capacity.
     * In that case we put the sender back at the front of the queue and
     * let the next receiver attempt the push.
     */
    if (ring_pop(ch, out)) {
        chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
        if (s) {
            if (ring_push(ch, s->data)) {
                s->result = CHAN_OK;
                chan_unlock(&ch->lock);
                chan_park_wake(s->wake_park);
            } else {
                /* Push failed: put s back so the next receiver retries. */
                waitq_push_front(&ch->send_waiters, s);
                chan_unlock(&ch->lock);
            }
        } else {
            chan_unlock(&ch->lock);
        }
        return CHAN_OK;
    }

    /* Ring empty: try direct rendezvous with a waiting sender. */
    chan_waiter_t *s = waitq_pop_sender(&ch->send_waiters);
    if (s) {
        memcpy(out, s->data, ch->elem_size);
        s->result = CHAN_OK;
        chan_unlock(&ch->lock);
        chan_park_wake(s->wake_park);
        return CHAN_OK;
    }

    if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
        chan_unlock(&ch->lock);
        return CHAN_ERR_CLOSED;
    }
    if (timeout_ns == 0) { chan_unlock(&ch->lock); return CHAN_ERR_WOULDBLOCK; }

    chan_waiter_t self;
    waiter_init(&self, out, CHAN_OP_RECV);
    atomic_fetch_add_explicit(&ch->recv_waiter_cnt, 1, memory_order_seq_cst);
    waitq_push(&ch->recv_waiters, &self);

    /* Re-check the ring after registering (still under lock): a lock-free
     * sender may have pushed in the window before our count became visible.
     * The seq_cst increment above pairs with the sender's seq_cst-fenced
     * recheck so we never both miss — no lost wakeup. */
    atomic_thread_fence(memory_order_seq_cst);
    if (ring_pop(ch, out)) {
        waitq_remove(&ch->recv_waiters, &self);
        atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
        chan_unlock(&ch->lock);
        waiter_destroy(&self);
        return CHAN_OK;
    }
    chan_unlock(&ch->lock);

    /* SPSC blocking recv: the single-producer fast path omits the per-op
     * seq_cst fence, so a push that races our registration may not observe our
     * recv_waiter_cnt store and thus skip waking us.  Park with a bounded
     * backstop and recheck once; if the ring is still empty afterwards, our
     * count has been durably visible long enough that ANY future push reliably
     * sees it and wakes us, so we then park indefinitely.  One extra wakeup per
     * park episode, no per-op cost on the producer, no reliance on chan_close.
     * (See LIBCHAN_SPSC_PARK_BACKSTOP_NS.) */
    if (ch->spsc && timeout_ns < 0) {
        if (!chan_park_wait(self.wake_park, LIBCHAN_SPSC_PARK_BACKSTOP_NS)) {
            chan_lock(&ch->lock);
            if (waitq_remove(&ch->recv_waiters, &self)) {
                /* Still queued — nobody delivered.  Grab a racing push that
                 * failed to wake us; else re-queue and park indefinitely. */
                if (ring_pop(ch, out)) {
                    self.result = CHAN_OK;
                    chan_unlock(&ch->lock);
                } else if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
                    self.result = CHAN_ERR_CLOSED;
                    chan_unlock(&ch->lock);
                } else {
                    waitq_push(&ch->recv_waiters, &self);
                    chan_unlock(&ch->lock);
                    chan_park_wait(self.wake_park, -1);
                }
            } else {
                chan_unlock(&ch->lock);   /* delivered while we timed out */
            }
        }
        atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return self.result;
    }

    if (!chan_park_wait(self.wake_park, timeout_ns)) {
        chan_lock(&ch->lock);
        /* If we are still queued, nobody delivered → genuine timeout.
         * If a sender already dequeued us, our data is in out → OK. */
        bool still_queued = waitq_remove(&ch->recv_waiters, &self);
        chan_unlock(&ch->lock);
        atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return still_queued ? CHAN_ERR_TIMEOUT : self.result;
    }
    atomic_fetch_sub_explicit(&ch->recv_waiter_cnt, 1, memory_order_relaxed);
    waiter_destroy(&self);
    return self.result;
}

chan_err_t chan_recv(chan_t *ch, void *out)                        { return chan_recv_impl(ch, out, -1); }
chan_err_t chan_try_recv(chan_t *ch, void *out)                    { return chan_recv_impl(ch, out,  0); }
chan_err_t chan_recv_timeout(chan_t *ch, void *out, int64_t ns)    { return chan_recv_impl(ch, out, ns); }

size_t chan_try_recv_burst(chan_t *ch, void *out, size_t n) {
    if (!ch || !out || n == 0) return 0;
    if (ch->capacity == 0) return 0;   /* unbuffered: no ring to drain */

    /* ring_lf_dequeue_burst acquire-loads prod.tail, so it sees data published
     * by a concurrent sender; like chan_recv it keeps draining after close. */
    uint32_t want = n > UINT32_MAX ? UINT32_MAX : (uint32_t)n;
    uint32_t got  = ring_lf_dequeue_burst(&ch->ring, out, want);
    if (got == 0) return 0;

    /* Admit parked senders — mirrors the single-element fast path's wake. */
    if (!ch->spsc) atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0)
        chan_admit_senders_to_ring(ch);
    return got;
}
