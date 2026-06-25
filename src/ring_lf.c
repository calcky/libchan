#include "ring_lf.h"
#include "libchan_internal.h"   /* chan_spin_hint */

#include <stdlib.h>
#include <string.h>

static uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

bool ring_lf_init(chan_ring_lf_t *r, size_t cap, size_t elem_size) {
    if (cap == 0 || elem_size == 0) return false;
    uint32_t actual = next_pow2((uint32_t)cap);
    r->slots = malloc((size_t)actual * elem_size);
    if (!r->slots) return false;
    atomic_init(&r->prod.head, 0);
    atomic_init(&r->prod.tail, 0);
    atomic_init(&r->cons.head, 0);
    atomic_init(&r->cons.tail, 0);
    r->prod.cached_cons_tail = 0;
    r->cons.cached_prod_tail = 0;
    r->mask      = actual - 1;
    r->capacity  = actual;
    r->elem_size = elem_size;
    return true;
}

void ring_lf_destroy(chan_ring_lf_t *r) {
    free(r->slots);
    r->slots = NULL;
}

bool ring_lf_push(chan_ring_lf_t *r, const void *data) {
    uint32_t ph, pnext;
    int spin = 0;

    /* Phase 1 — Reserve a producer slot via CAS on prod.head. */
    do {
        ph = atomic_load_explicit(&r->prod.head, memory_order_relaxed);
        uint32_t ct = atomic_load_explicit(&r->cons.tail, memory_order_acquire);
        if ((uint32_t)(ph - ct) >= r->capacity)
            return false;   /* full */
        pnext = ph + 1;
    } while (!atomic_compare_exchange_weak_explicit(
                 &r->prod.head, &ph, pnext,
                 memory_order_relaxed, memory_order_relaxed));

    /* Phase 2 — Write data into the reserved slot. */
    memcpy(r->slots + (ph & r->mask) * r->elem_size, data, r->elem_size);

    /* Phase 3 — Commit: wait for our turn, then advance prod.tail.
     * The release store ensures Phase 2 data is visible before prod.tail
     * is seen by consumers.
     *
     * The wait-load is ACQUIRE, not relaxed: it synchronises-with the
     * predecessor producer's release-store of prod.tail, so this producer
     * happens-after every earlier producer's slot write.  That builds a
     * transitive happens-before chain along prod.tail, so a consumer's
     * acquire-load of prod.tail orders-after ALL prior producers' writes —
     * not just the one whose value it happens to read.  Without it, a plain
     * cross-thread store breaks the C11 release sequence and the slot read
     * races the slot write (benign on x86 TSO, real on weak memory; TSan
     * flags it). */
    spin = 0;
    while (atomic_load_explicit(&r->prod.tail, memory_order_acquire) != ph)
        chan_spin_hint(spin++);
    atomic_store_explicit(&r->prod.tail, pnext, memory_order_release);
    return true;
}

bool ring_lf_pop(chan_ring_lf_t *r, void *out) {
    uint32_t ch, cnext;
    int spin = 0;

    /* Phase 1 — Reserve a consumer slot via CAS on cons.head.
     * The acquire-load of prod.tail synchronises with the producer's
     * release-store of prod.tail, making the written data visible. */
    do {
        ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
        uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_acquire);
        if (pt == ch)
            return false;   /* empty */
        cnext = ch + 1;
    } while (!atomic_compare_exchange_weak_explicit(
                 &r->cons.head, &ch, cnext,
                 memory_order_relaxed, memory_order_relaxed));

    /* Phase 2 — Read data from the reserved slot. */
    memcpy(out, r->slots + (ch & r->mask) * r->elem_size, r->elem_size);

    /* Phase 3 — Commit: wait for our turn, then advance cons.tail.
     * ACQUIRE wait-load (symmetric to ring_lf_push): chains happens-before
     * along cons.tail so a producer's acquire-load of cons.tail orders-after
     * ALL prior consumers' slot reads, not just one — preventing a later
     * producer's slot overwrite from racing an earlier consumer's read. */
    spin = 0;
    while (atomic_load_explicit(&r->cons.tail, memory_order_acquire) != ch)
        chan_spin_hint(spin++);
    atomic_store_explicit(&r->cons.tail, cnext, memory_order_release);
    return true;
}

uint32_t ring_lf_enqueue_burst(chan_ring_lf_t *r, const void *data, uint32_t n) {
    uint32_t ph, pnext, k;
    int spin = 0;
    if (n == 0) return 0;

    /* Phase 1 — Reserve up to n contiguous producer slots via one CAS.
     * free = capacity - in-use; clamp the batch to it. */
    do {
        ph = atomic_load_explicit(&r->prod.head, memory_order_relaxed);
        uint32_t ct = atomic_load_explicit(&r->cons.tail, memory_order_acquire);
        uint32_t free_entries = r->capacity - (ph - ct);
        if (free_entries == 0)
            return 0;   /* full */
        k = n < free_entries ? n : free_entries;
        pnext = ph + k;
    } while (!atomic_compare_exchange_weak_explicit(
                 &r->prod.head, &ph, pnext,
                 memory_order_relaxed, memory_order_relaxed));

    /* Phase 2 — Write the reserved run, splitting at the power-of-2 wrap. */
    uint32_t idx   = ph & r->mask;
    uint32_t first = r->capacity - idx;   /* slots from idx to end-of-buffer */
    if (first >= k) {
        memcpy(r->slots + (size_t)idx * r->elem_size,
               data, (size_t)k * r->elem_size);
    } else {
        memcpy(r->slots + (size_t)idx * r->elem_size,
               data, (size_t)first * r->elem_size);
        memcpy(r->slots,
               (const char *)data + (size_t)first * r->elem_size,
               (size_t)(k - first) * r->elem_size);
    }

    /* Phase 3 — Commit: one ordered wait + release-store for the whole run.
     * Same release-sequence reasoning as ring_lf_push (acquire wait-load). */
    spin = 0;
    while (atomic_load_explicit(&r->prod.tail, memory_order_acquire) != ph)
        chan_spin_hint(spin++);
    atomic_store_explicit(&r->prod.tail, pnext, memory_order_release);
    return k;
}

uint32_t ring_lf_dequeue_burst(chan_ring_lf_t *r, void *out, uint32_t n) {
    uint32_t ch, cnext, k;
    int spin = 0;
    if (n == 0) return 0;

    /* Phase 1 — Reserve up to n contiguous consumer slots via one CAS.
     * avail = committed - already-reserved; clamp the batch to it. */
    do {
        ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
        uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_acquire);
        uint32_t avail = pt - ch;
        if (avail == 0)
            return 0;   /* empty */
        k = n < avail ? n : avail;
        cnext = ch + k;
    } while (!atomic_compare_exchange_weak_explicit(
                 &r->cons.head, &ch, cnext,
                 memory_order_relaxed, memory_order_relaxed));

    /* Phase 2 — Read the reserved run, splitting at the power-of-2 wrap. */
    uint32_t idx   = ch & r->mask;
    uint32_t first = r->capacity - idx;
    if (first >= k) {
        memcpy(out, r->slots + (size_t)idx * r->elem_size,
               (size_t)k * r->elem_size);
    } else {
        memcpy(out, r->slots + (size_t)idx * r->elem_size,
               (size_t)first * r->elem_size);
        memcpy((char *)out + (size_t)first * r->elem_size,
               r->slots, (size_t)(k - first) * r->elem_size);
    }

    /* Phase 3 — Commit (symmetric to enqueue burst). */
    spin = 0;
    while (atomic_load_explicit(&r->cons.tail, memory_order_acquire) != ch)
        chan_spin_hint(spin++);
    atomic_store_explicit(&r->cons.tail, cnext, memory_order_release);
    return k;
}

uint32_t ring_lf_count(const chan_ring_lf_t *r) {
    uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_relaxed);
    uint32_t ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
    return pt - ch;
}
