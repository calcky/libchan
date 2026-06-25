#include "libchan_internal.h"

chan_err_t chan_send_impl(chan_t *ch, const void *data, int64_t timeout_ns) {
    if (!ch || !data) return CHAN_ERR_INVALID;

    /*
     * Buffered fast path (lock-free), with bounded spin before parking.
     *
     * Symmetric to chan_recv: retrying the lock-free push a few times on a
     * transiently-full ring keeps send_waiter_cnt at 0, so a concurrent
     * consumer stays on ITS fast path and keeps draining instead of dragging
     * us onto the locked slow path the instant we would park.  This is what
     * lets a buffered pipeline pipeline rather than park on every message.
     *
     * Bail out of the spin immediately if any waiter is registered (a parked
     * receiver must be served via the slow path) or the channel is closed.
     */
    if (ch->spsc) {
        /* Dedicated lean SPSC producer fast path.  Single producer ⇒ we are
         * never a parked sender, so skip the send_waiter_cnt check; and we may
         * push even when a receiver is parked (it parked on an empty ring) and
         * just deliver to it afterwards, so skip the recv_waiter_cnt pre-check.
         * Net hot path: one closed-load + the inlined cursor-cached push +
         * (only on success) one relaxed recv_waiter_cnt load.  No per-op fence
         * (SPSC bounded-delay edge — see doc/design.md). */
        for (int spin = 0; ; spin++) {
            if (atomic_load_explicit(&ch->closed, memory_order_acquire)) break;
            if (ring_spsc_push(&ch->ring, data)) {
                if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) != 0)
                    chan_deliver_ring_to_receivers(ch);
                return CHAN_OK;
            }
            if (spin >= LIBCHAN_FASTPATH_SPIN) break;   /* full → slow path / park */
            chan_spin_hint(spin & 7);
        }
        /* fall through to the locked slow path (returns CLOSED or parks) */
    } else if (ch->capacity > 0) {
        for (int spin = 0; ; spin++) {
            if (atomic_load_explicit(&ch->closed, memory_order_acquire)) break;
            if (atomic_load_explicit(&ch->send_waiter_cnt, memory_order_relaxed) != 0) break;
            if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) != 0) break;
            if (ring_lf_push(&ch->ring, data)) {
                /* Wake a parked receiver if one is present.
                 *
                 * MPMC: pair a seq_cst fence with the receiver's seq_cst
                 * registration (full Dekker handshake) — never miss a
                 * concurrently-parking receiver.
                 *
                 * SPSC: skip the per-op fence (it costs ~3x on this hot path).
                 * A parked consumer is still woken once its count store is
                 * visible to this relaxed load; the narrow race where it is not
                 * yet visible is resolved by our next push or by chan_close
                 * (SPSC bounded-delay edge — see doc/design.md). */
                if (!ch->spsc) atomic_thread_fence(memory_order_seq_cst);
                if (atomic_load_explicit(&ch->recv_waiter_cnt, memory_order_relaxed) != 0)
                    chan_deliver_ring_to_receivers(ch);
                return CHAN_OK;
            }
            /* ring full */
            if (spin >= LIBCHAN_FASTPATH_SPIN) break;
            chan_spin_hint(spin & 7);  /* pure pause, no yield */
        }
        /* fall through to the locked slow path */
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
            /* Preserve FIFO: if the ring holds older items (a fast-path sender
             * may have pushed after this receiver checked the ring empty but
             * before registering), give the receiver the OLDEST item and queue
             * our new data at the tail.  Handing the new item directly while
             * older data sits in the ring would deliver out of order.
             * ring_pop may fail (a slot we saw vanished to a racing pop); only
             * swap when it actually yields an item, else hand off directly. */
            if (!ring_empty(ch) && ring_pop(ch, r->data)) {
                ring_push(ch, data);   /* slot just freed → succeeds */
            } else {
                memcpy(r->data, data, ch->elem_size);
            }
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
    atomic_fetch_add_explicit(&ch->send_waiter_cnt, 1, memory_order_seq_cst);
    waitq_push(&ch->send_waiters, &self);

    /* Re-check the ring after registering (still under lock): a lock-free
     * receiver may have freed a slot before our count became visible.  The
     * seq_cst increment above pairs with the receiver's seq_cst-fenced recheck
     * so we never both miss — no lost wakeup. */
    atomic_thread_fence(memory_order_seq_cst);
    if (ring_push(ch, data)) {
        waitq_remove(&ch->send_waiters, &self);
        atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        chan_unlock(&ch->lock);
        waiter_destroy(&self);
        return CHAN_OK;
    }
    chan_unlock(&ch->lock);

    /* SPSC blocking send — symmetric to chan_recv: the single-consumer fast
     * path omits the per-op fence, so a pop that frees a slot may not observe
     * our send_waiter_cnt store and skip admitting us.  Bounded-backstop
     * recheck, then indefinite park (see chan_recv.c / LIBCHAN_SPSC_PARK_-
     * BACKSTOP_NS for the full rationale). */
    if (ch->spsc && timeout_ns < 0) {
        if (!chan_park_wait(self.wake_park, LIBCHAN_SPSC_PARK_BACKSTOP_NS)) {
            chan_lock(&ch->lock);
            if (waitq_remove(&ch->send_waiters, &self)) {
                /* Still queued — nobody admitted us.  Push into a slot a racing
                 * pop just freed; else re-queue and park indefinitely. */
                if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
                    self.result = CHAN_ERR_CLOSED;
                    chan_unlock(&ch->lock);
                } else if (ring_push(ch, data)) {
                    self.result = CHAN_OK;
                    chan_unlock(&ch->lock);
                } else {
                    waitq_push(&ch->send_waiters, &self);
                    chan_unlock(&ch->lock);
                    chan_park_wait(self.wake_park, -1);
                }
            } else {
                chan_unlock(&ch->lock);   /* admitted while we timed out */
            }
        }
        atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return self.result;
    }

    if (!chan_park_wait(self.wake_park, timeout_ns)) {
        chan_lock(&ch->lock);
        /* If we are still queued, nobody admitted us → genuine timeout.
         * If an admitter already dequeued us, our data is in the ring → OK. */
        bool still_queued = waitq_remove(&ch->send_waiters, &self);
        chan_unlock(&ch->lock);
        atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
        waiter_destroy(&self);
        return still_queued ? CHAN_ERR_TIMEOUT : self.result;
    }
    atomic_fetch_sub_explicit(&ch->send_waiter_cnt, 1, memory_order_relaxed);
    waiter_destroy(&self);
    /* Data was already pushed into the ring by the receiver that woke us. */
    return self.result;
}

chan_err_t chan_send(chan_t *ch, const void *data)                        { return chan_send_impl(ch, data, -1); }
chan_err_t chan_try_send(chan_t *ch, const void *data)                    { return chan_send_impl(ch, data,  0); }
chan_err_t chan_send_timeout(chan_t *ch, const void *data, int64_t ns)    { return chan_send_impl(ch, data, ns); }
