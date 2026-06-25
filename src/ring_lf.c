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
     * is seen by consumers. */
    spin = 0;
    while (atomic_load_explicit(&r->prod.tail, memory_order_relaxed) != ph)
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

    /* Phase 3 — Commit: wait for our turn, then advance cons.tail. */
    spin = 0;
    while (atomic_load_explicit(&r->cons.tail, memory_order_relaxed) != ch)
        chan_spin_hint(spin++);
    atomic_store_explicit(&r->cons.tail, cnext, memory_order_release);
    return true;
}

uint32_t ring_lf_count(const chan_ring_lf_t *r) {
    uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_relaxed);
    uint32_t ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
    return pt - ch;
}
