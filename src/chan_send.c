#include "libchan_internal.h"

chan_err_t chan_send_impl(chan_t *ch, const void *data, int64_t timeout_ns) {
    if (!ch || !data) return CHAN_ERR_INVALID;

    /*
     * Buffered fast path (lock-free).
     *
     * Condition: both waiter counts are zero — meaning no thread is sleeping
     * in send_waiters or recv_waiters.  When any counter is non-zero all
     * threads use the locked slow path, which guarantees that the "receiver
     * helps push" pattern in chan_recv.c never races with a concurrent
     * fast-path push.
     */
    if (ch->capacity > 0 &&
        !atomic_load_explicit(&ch->closed, memory_order_acquire) &&
        atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) == 0 &&
        atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) == 0) {
        if (ring_lf_push(&ch->ring, data))
            return CHAN_OK;
        /* ring was full — fall through to slow path */
    }

    /* ---------- Slow path: acquire lock ---------- */
    chan_lock(&ch->lock);

    if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
        chan_unlock(&ch->lock);
        return CHAN_ERR_CLOSED;
    }

    if (ch->capacity == 0) {
        /* ---- Unbuffered: direct rendezvous ---- */
        chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
        if (r) {
            memcpy(r->data, data, ch->elem_size);
            r->result = CHAN_OK;
            chan_unlock(&ch->lock);
            chan_park_wake(r->wake_park);
            return CHAN_OK;
        }
        if (timeout_ns == 0) { chan_unlock(&ch->lock); return CHAN_ERR_WOULDBLOCK; }

        chan_waiter_t self;
        waiter_init(&self, (void *)data, CHAN_OP_SEND);
        atomic_fetch_add_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        waitq_push(&ch->send_waiters, &self);
        chan_unlock(&ch->lock);

        if (!chan_park_wait(self.wake_park, timeout_ns)) {
            chan_lock(&ch->lock);
            waitq_remove(&ch->send_waiters, &self);
            chan_unlock(&ch->lock);
            atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
            waiter_destroy(&self);
            return CHAN_ERR_TIMEOUT;
        }
        atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return self.result;
    }

    /* ---- Buffered slow path ---- */

    /* Direct hand-off if a receiver is waiting.
     * Do NOT guard with ring_empty(): a fast-path sender may have pushed
     * after the receiver checked the ring but before it incremented
     * recv_waiter_cnt, leaving the ring non-empty yet the receiver sleeping. */
    if (!waitq_empty(&ch->recv_waiters)) {
        chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
        if (r) {
            memcpy(r->data, data, ch->elem_size);
            r->result = CHAN_OK;
            chan_unlock(&ch->lock);
            chan_park_wake(r->wake_park);
            return CHAN_OK;
        }
    }

    /*
     * Try to push into the ring under the lock.
     *
     * Safety: when send_waiter_cnt or recv_waiter_cnt is non-zero, no thread
     * uses the lock-free fast path.  Therefore while we hold the lock no
     * concurrent fast-path push can steal a slot we freed.  ring_push here
     * is effectively single-threaded on the producer side.
     */
    if (ring_push(ch, data)) {
        chan_waiter_t *r = waitq_pop_receiver(&ch->recv_waiters);
        chan_unlock(&ch->lock);
        if (r) { r->result = CHAN_OK; chan_park_wake(r->wake_park); }
        return CHAN_OK;
    }

    /* Ring still full — park. */
    if (timeout_ns == 0) { chan_unlock(&ch->lock); return CHAN_ERR_WOULDBLOCK; }

    chan_waiter_t self;
    waiter_init(&self, (void *)data, CHAN_OP_SEND);
    atomic_fetch_add_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
    waitq_push(&ch->send_waiters, &self);
    chan_unlock(&ch->lock);

    if (!chan_park_wait(self.wake_park, timeout_ns)) {
        chan_lock(&ch->lock);
        waitq_remove(&ch->send_waiters, &self);
        chan_unlock(&ch->lock);
        atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return CHAN_ERR_TIMEOUT;
    }
    atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
    waiter_destroy(&self);
    /* Data was already pushed into the ring by the receiver that woke us. */
    return self.result;
}

chan_err_t chan_send(chan_t *ch, const void *data)                        { return chan_send_impl(ch, data, -1); }
chan_err_t chan_try_send(chan_t *ch, const void *data)                    { return chan_send_impl(ch, data,  0); }
chan_err_t chan_send_timeout(chan_t *ch, const void *data, int64_t ns)    { return chan_send_impl(ch, data, ns); }
