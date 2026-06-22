#include "libchan_internal.h"

chan_err_t chan_recv_impl(chan_t *ch, void *out, int64_t timeout_ns) {
    if (!ch || !out) return CHAN_ERR_INVALID;

    /*
     * Buffered fast path (lock-free).
     *
     * Same guard as the sender: both waiter counts must be zero.  When
     * send_waiter_cnt > 0 we use the slow path so that the "receiver helps
     * push" pattern below is safe from concurrent fast-path senders.
     */
    if (ch->capacity > 0 &&
        atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) == 0 &&
        atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) == 0) {
        if (ring_lf_pop(&ch->ring, out))
            return CHAN_OK;
        /* ring was empty — fall through; check closed inside lock */
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

chan_err_t chan_recv(chan_t *ch, void *out)                        { return chan_recv_impl(ch, out, -1); }
chan_err_t chan_try_recv(chan_t *ch, void *out)                    { return chan_recv_impl(ch, out,  0); }
chan_err_t chan_recv_timeout(chan_t *ch, void *out, int64_t ns)    { return chan_recv_impl(ch, out, ns); }
