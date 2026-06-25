/*
 * ring_lf.h — DPDK rte_ring-style MPMC lock-free ring buffer
 *
 * Protocol (single-element enqueue):
 *   Phase 1 — Reserve: CAS prod.head old→old+1.
 *             Full check: (prod.head - cons.tail) >= capacity → return false.
 *   Phase 2 — Write:   copy data into slots[(old_head & mask) * elem_size].
 *   Phase 3 — Commit:  spin (acquire-load) until prod.tail == old_head,
 *                       then store_release(prod.tail, old_head+1).
 *
 * Protocol (single-element dequeue) is symmetric using cons.head/cons.tail.
 *
 * Bulk variants (ring_lf_enqueue_burst / ring_lf_dequeue_burst) run the SAME
 * three-phase protocol but reserve up to n contiguous slots in ONE CAS, copy
 * the whole run (split into two memcpy when it wraps the power-of-2 boundary),
 * and do ONE Phase-3 commit.  This amortises the CAS, the cross-core read of
 * the opposite cursor, and the Phase-3 wait over the whole batch — per-element
 * cross-core traffic drops ~1/k — while keeping the single-element memory
 * ordering verbatim (the release-store of prod.tail/cons.tail still publishes
 * every slot write/read in the run).  Burst semantics: enqueue/dequeue as many
 * as fit/are available, up to n; return the count actually moved (0 when full/
 * empty).  Both are MPMC-safe and never touch the SPSC cache fields.
 *
 * The acquire-load of prod.tail in dequeue Phase 1 synchronises with the
 * producer's release-store of prod.tail, so the data is visible before it
 * is read.  No additional fence is required between Phase 1 and Phase 2.
 *
 * The Phase-3 wait-load is ACQUIRE (not relaxed): each producer thus
 * synchronises-with its predecessor, chaining happens-before along prod.tail
 * so a consumer ordered-after the latest tail value is ordered-after EVERY
 * prior producer's slot write — not just the one whose value it read (a plain
 * cross-thread store would otherwise break the C11 release sequence). Same on
 * the cons.tail side. Without this, the slot read/write races on weak memory
 * (benign on x86 TSO; flagged by ThreadSanitizer).
 *
 * Capacity is rounded up to the next power of 2 at init time.
 * uint32_t indices wrap naturally at 2^32; the unsigned subtraction
 * used for full/empty checks is always correct.
 */
#ifndef RING_LF_H
#define RING_LF_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define RING_LF_CACHELINE 64
#define RING_LF_ALIGNED   __attribute__((aligned(RING_LF_CACHELINE)))

typedef struct {
    /* Producer cursors — one cache line */
    struct {
        _Atomic uint32_t head;   /* reserved-up-to by producers */
        _Atomic uint32_t tail;   /* committed-up-to by producers (visible to consumers) */
        uint32_t cached_cons_tail; /* SPSC-only: producer-private cache of cons.tail */
    } prod RING_LF_ALIGNED;

    /* Consumer cursors — separate cache line */
    struct {
        _Atomic uint32_t head;   /* reserved-up-to by consumers */
        _Atomic uint32_t tail;   /* committed-up-to by consumers (visible to producers) */
        uint32_t cached_prod_tail; /* SPSC-only: consumer-private cache of prod.tail */
    } cons RING_LF_ALIGNED;

    uint32_t  mask;       /* capacity - 1; index via [idx & mask] */
    uint32_t  capacity;   /* actual allocated capacity, always a power of 2 */
    size_t    elem_size;
    char     *slots;      /* capacity * elem_size bytes */
} chan_ring_lf_t;

/* Initialise ring.  cap is rounded up to next power of 2 internally. */
bool ring_lf_init   (chan_ring_lf_t *r, size_t cap, size_t elem_size);
void ring_lf_destroy(chan_ring_lf_t *r);

/* Returns true on success; false if full (push) or empty (pop). */
bool ring_lf_push(chan_ring_lf_t *r, const void *data);
bool ring_lf_pop (chan_ring_lf_t *r, void *out);

/* Bulk variants — MPMC-safe burst enqueue/dequeue.
 * `data`/`out` point to n contiguous elements of elem_size each.
 * Reserve up to n slots in one CAS, copy the run (wrap-aware), one Phase-3
 * commit.  Return the number of elements actually moved (<= n; 0 when the ring
 * is full / empty).  See the protocol note above. */
uint32_t ring_lf_enqueue_burst(chan_ring_lf_t *r, const void *data, uint32_t n);
uint32_t ring_lf_dequeue_burst(chan_ring_lf_t *r, void *out,        uint32_t n);

/* SPSC-only cursor-cached fast variants are defined as static inline in
 * libchan_internal.h (ring_spsc_push / ring_spsc_pop) so they inline into
 * chan_send/recv.  SAFETY CONTRACT — correct only with exactly ONE producer
 * thread and ONE consumer thread:
 *   - prod.cached_cons_tail is read/written ONLY by the single producer;
 *   - cons.cached_prod_tail is read/written ONLY by the single consumer;
 *   - each cache only ever holds a value that that side itself acquire-loaded
 *     from the real opposite cursor, so the slot-data happens-before is
 *     preserved (single-owner ⇒ the transitive-visibility hole that makes
 *     cursor caching unsafe under MPMC cannot occur).
 * The plain ring_lf_push/pop (with CAS + Phase-3) remain MPMC-safe and are
 * still used by the locked slow path and the lost-wakeup helpers; those never
 * touch the cache fields, so a cache merely goes stale-low (always safe). */

/* Approximate count of committed items not yet reserved by consumers. */
uint32_t ring_lf_count(const chan_ring_lf_t *r);

#endif /* RING_LF_H */
