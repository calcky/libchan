#include "libchan_internal.h"

#include <stdlib.h>
#include <string.h>

static chan_t *chan_create_impl(size_t elem_size, size_t capacity, bool spsc) {
    if (elem_size == 0) return NULL;

    chan_t *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;

    ch->elem_size = elem_size;
    ch->capacity  = capacity;
    ch->spsc      = spsc && capacity > 0;   /* caching only applies to buffered rings */

    if (capacity > 0) {
        if (!ring_lf_init(&ch->ring, capacity, elem_size)) {
            free(ch);
            return NULL;
        }
    }

    chan_mutex_init(&ch->lock);
    atomic_init(&ch->closed,          false);
    atomic_init(&ch->refcount,        1);
    atomic_init(&ch->send_waiter_cnt, 0);
    atomic_init(&ch->recv_waiter_cnt, 0);

    ch->send_waiters.head = NULL;
    ch->send_waiters.tail = NULL;
    ch->recv_waiters.head = NULL;
    ch->recv_waiters.tail = NULL;

    return ch;
}

chan_t *chan_create(size_t elem_size, size_t capacity) {
    return chan_create_impl(elem_size, capacity, false);
}

chan_t *chan_create_spsc(size_t elem_size, size_t capacity) {
    return chan_create_impl(elem_size, capacity, true);
}

static void chan_free(chan_t *ch) {
    /* Drain any waiters that might still be registered (safety net). */
    waitq_close_all(&ch->send_waiters);
    waitq_close_all(&ch->recv_waiters);
    chan_mutex_destroy(&ch->lock);
    if (ch->capacity > 0)
        ring_lf_destroy(&ch->ring);
    free(ch);
}

chan_t *chan_retain(chan_t *ch) {
    if (!ch) return NULL;
    atomic_fetch_add_explicit(&ch->refcount, 1, memory_order_relaxed);
    return ch;
}

void chan_destroy(chan_t *ch) {
    if (!ch) return;
    /* Acquire fence: ensure we see all writes from threads that decremented
     * before us, matching their release decrement. */
    if (atomic_fetch_sub_explicit(&ch->refcount, 1, memory_order_acq_rel) == 1) {
        /* Drain the lock to confirm no thread is still inside a critical
         * section holding a raw pointer to ch. */
        chan_lock(&ch->lock);
        chan_unlock(&ch->lock);
        chan_free(ch);
    }
}

chan_err_t chan_close(chan_t *ch) {
    if (!ch) return CHAN_ERR_INVALID;

    chan_lock(&ch->lock);

    if (atomic_load_explicit(&ch->closed, memory_order_relaxed)) {
        chan_unlock(&ch->lock);
        return CHAN_ERR_CLOSED;
    }

    atomic_store_explicit(&ch->closed, true, memory_order_release);

    /* Wake all blocked senders — they cannot deliver after close. */
    waitq_close_all(&ch->send_waiters);

    /* Wake blocked receivers.
     *
     * A sleeping receiver registered because the ring was empty when it
     * locked, but a concurrent fast-path sender may have pushed data into
     * the ring in the narrow window between that ring check and the waiter
     * registration (both outside the lock).  So we cannot assume the ring
     * is still empty.  For each waiting receiver: deliver a ring item if
     * one is available (result = CHAN_OK), otherwise signal CHAN_ERR_CLOSED.
     * This prevents a 1-item loss when the last send races chan_close. */
    {
        chan_waiter_t *r;
        while ((r = waitq_pop(&ch->recv_waiters))) {
            if (!waiter_try_claim(r)) continue;
            /* ring_pop (= ring_lf_pop) uses acquire on prod.tail, so it correctly
             * sees any release-store by a concurrent fast-path sender.  Checking
             * ring_empty first would use relaxed and could falsely show empty. */
            if (ch->capacity > 0 && ring_pop(ch, r->data)) {
                r->result = CHAN_OK;
            } else {
                r->result = CHAN_ERR_CLOSED;
            }
            chan_park_wake(r->wake_park);
        }
    }

    chan_unlock(&ch->lock);
    return CHAN_OK;
}

bool chan_is_closed(const chan_t *ch) {
    if (!ch) return true;
    return atomic_load_explicit(&ch->closed, memory_order_acquire);
}

size_t chan_len(const chan_t *ch) {
    if (!ch || ch->capacity == 0) return 0;
    /* ring_lf_count is an approximation under concurrency, but matches the
     * documented semantics (brief non-blocking snapshot). */
    return ring_lf_count(&ch->ring);
}

size_t chan_cap(const chan_t *ch) {
    if (!ch) return 0;
    return ch->capacity;   /* immutable after create */
}

const char *chan_strerror(chan_err_t err) {
    switch (err) {
    case CHAN_OK:             return "ok";
    case CHAN_ERR_CLOSED:     return "channel closed";
    case CHAN_ERR_TIMEOUT:    return "timeout";
    case CHAN_ERR_WOULDBLOCK: return "would block";
    case CHAN_ERR_INVALID:    return "invalid argument";
    case CHAN_ERR_NOMEM:      return "out of memory";
    default:                  return "unknown error";
    }
}
