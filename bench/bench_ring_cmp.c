/*
 * bench_ring_cmp.c
 *
 * Quantify the throughput gap between the DPDK 4-cursor rte_ring and the
 * Vyukov per-slot MPMC queue.
 *
 * DPDK rte_ring: in Phase 3 a producer must spin-wait until prod.tail equals
 * its reserved position before it can commit, which serializes the commits of
 * concurrent producers (convoy effect).
 *
 * Vyukov per-slot: each slot owns an independent _Atomic uint64_t seq; after
 * writing, a producer directly store_releases that slot's seq, with no
 * cross-producer Phase 3 wait.
 *
 * Key variables:
 *   - thread count: 1P1C / 2P2C / 4P4C / 8P8C
 *   - element size: 8 B (int64) and 64 B (one cache line)
 *     larger elements make Phase 2 memcpy slower -> rte_ring Phase 3 convoy
 *     gets worse
 */

#define _GNU_SOURCE
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* timing                                                              */
/* ------------------------------------------------------------------ */

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ------------------------------------------------------------------ */
/* spin hint                                                           */
/* ------------------------------------------------------------------ */

static inline void spin_hint(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

/* ------------------------------------------------------------------ */
/* DPDK 4-cursor rte_ring (same logic as src/ring_lf.c, inline impl)   */
/* ------------------------------------------------------------------ */

#define RING_ALIGNED __attribute__((aligned(64)))

typedef struct {
    struct { _Atomic uint32_t head; _Atomic uint32_t tail; } prod RING_ALIGNED;
    struct { _Atomic uint32_t head; _Atomic uint32_t tail; } cons RING_ALIGNED;
    uint32_t mask;
    uint32_t capacity;
    size_t   elem_size;
    char    *slots;
} dpdk_ring_t;

static bool dpdk_init(dpdk_ring_t *r, size_t cap, size_t elem_size) {
    /* round up to a power of two */
    uint32_t n = (uint32_t)cap - 1;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    n++;
    r->slots = malloc((size_t)n * elem_size);
    if (!r->slots) return false;
    atomic_init(&r->prod.head, 0); atomic_init(&r->prod.tail, 0);
    atomic_init(&r->cons.head, 0); atomic_init(&r->cons.tail, 0);
    r->mask = n - 1; r->capacity = n; r->elem_size = elem_size;
    return true;
}
static void dpdk_destroy(dpdk_ring_t *r) { free(r->slots); }

/* blocking push (spin-wait when full) */
static void dpdk_push_wait(dpdk_ring_t *r, const void *data) {
    uint32_t ph, pnext;
    for (;;) {
        ph = atomic_load_explicit(&r->prod.head, memory_order_relaxed);
        uint32_t ct = atomic_load_explicit(&r->cons.tail, memory_order_acquire);
        if ((uint32_t)(ph - ct) >= r->capacity) { spin_hint(); continue; }
        pnext = ph + 1;
        if (atomic_compare_exchange_weak_explicit(&r->prod.head, &ph, pnext,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
    /* Phase 2: write data */
    memcpy(r->slots + (ph & r->mask) * r->elem_size, data, r->elem_size);
    /* Phase 3: wait for the predecessor to commit, then advance prod.tail */
    while (atomic_load_explicit(&r->prod.tail, memory_order_relaxed) != ph)
        spin_hint();
    atomic_store_explicit(&r->prod.tail, pnext, memory_order_release);
}

/* blocking pop (spin-wait when empty) */
static void dpdk_pop_wait(dpdk_ring_t *r, void *out) {
    uint32_t ch, cnext;
    for (;;) {
        ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
        uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_acquire);
        if (pt == ch) { spin_hint(); continue; }
        cnext = ch + 1;
        if (atomic_compare_exchange_weak_explicit(&r->cons.head, &ch, cnext,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
    memcpy(out, r->slots + (ch & r->mask) * r->elem_size, r->elem_size);
    while (atomic_load_explicit(&r->cons.tail, memory_order_relaxed) != ch)
        spin_hint();
    atomic_store_explicit(&r->cons.tail, cnext, memory_order_release);
}

/* ------------------------------------------------------------------ */
/* Vyukov per-slot MPMC                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t          mask;
    uint32_t          capacity;
    size_t            elem_size;
    _Atomic uint64_t  prod_head;
    _Atomic uint64_t  cons_head;
    _Atomic uint64_t *seqs;    /* per-slot sequence number, initialized to i */
    char             *slots;
} vy_ring_t;

static bool vy_init(vy_ring_t *r, size_t cap, size_t elem_size) {
    uint32_t n = (uint32_t)cap - 1;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    n++;
    r->seqs  = malloc((size_t)n * sizeof(_Atomic uint64_t));
    r->slots = malloc((size_t)n * elem_size);
    if (!r->seqs || !r->slots) { free(r->seqs); free(r->slots); return false; }
    for (uint32_t i = 0; i < n; i++)
        atomic_init(&r->seqs[i], (uint64_t)i);
    atomic_init(&r->prod_head, 0);
    atomic_init(&r->cons_head, 0);
    r->mask = n - 1; r->capacity = n; r->elem_size = elem_size;
    return true;
}
static void vy_destroy(vy_ring_t *r) { free(r->seqs); free(r->slots); }

/*
 * Vyukov push logic:
 *   pos is the write position this thread has reserved.
 *   seq == pos -> slot free, writable; seq < pos -> slot occupied (full);
 *   seq > pos -> CAS failed, reload.
 *
 * After writing, store_release(seq, pos+1); the consumer reads the data once
 * it sees seq == pos+1.
 * Key point: there is no cross-producer Phase 3 spin; each producer commits
 * its own slot independently.
 */
static void vy_push_wait(vy_ring_t *r, const void *data) {
    uint64_t pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
    for (;;) {
        uint32_t idx = (uint32_t)(pos & r->mask);
        uint64_t seq = atomic_load_explicit(&r->seqs[idx], memory_order_acquire);
        int64_t  diff = (int64_t)(seq - pos);
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&r->prod_head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;          /* reservation succeeded */
            /* CAS failed: another producer got ahead; pos has been updated to
               the new expected value, continue */
        } else if (diff < 0) {
            /* full: wait for a consumer to release */
            spin_hint();
            pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
        } else {
            /* diff > 0: another producer already took this slot, reload pos */
            pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
        }
    }
    memcpy(r->slots + (pos & r->mask) * r->elem_size, data, r->elem_size);
    /* commit: let the consumer see seq == pos + 1 */
    atomic_store_explicit(&r->seqs[pos & r->mask], pos + 1, memory_order_release);
}

/*
 * Vyukov pop logic:
 *   seq == pos+1 -> data ready; seq < pos+1 -> empty;
 *   seq > pos+1 -> CAS failed, reload.
 *
 * After reading, store_release(seq, pos + capacity) so the next-round producer
 * sees the slot as free.
 */
static void vy_pop_wait(vy_ring_t *r, void *out) {
    uint64_t pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
    for (;;) {
        uint32_t idx = (uint32_t)(pos & r->mask);
        uint64_t seq = atomic_load_explicit(&r->seqs[idx], memory_order_acquire);
        int64_t  diff = (int64_t)(seq - (pos + 1));
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&r->cons_head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            spin_hint();
            pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
        } else {
            pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
        }
    }
    memcpy(out, r->slots + (pos & r->mask) * r->elem_size, r->elem_size);
    /* release the slot: the next-round producer reserves it at pos + capacity */
    atomic_store_explicit(&r->seqs[pos & r->mask],
                          pos + r->capacity, memory_order_release);
}

/* ------------------------------------------------------------------ */
/* benchmark framework                                                 */
/* ------------------------------------------------------------------ */

#define RING_CAP   1024
#define TOTAL_OPS  4000000L   /* total send+recv count per test */
#define WARMUP_OPS  200000L

typedef enum { IMPL_DPDK, IMPL_VYUKOV } impl_t;

typedef struct {
    impl_t        impl;
    void         *ring;      /* dpdk_ring_t* or vy_ring_t* */
    size_t        elem_size;
    _Atomic long  sent;      /* global sent count */
    _Atomic long  recvd;     /* global received count */
    long          target;
} bench_state_t;

static void *producer_fn(void *arg) {
    bench_state_t *s = arg;
    char buf[64] = {0};
    for (;;) {
        long idx = atomic_fetch_add_explicit(&s->sent, 1, memory_order_relaxed);
        if (idx >= s->target) break;
        /* write the sequence number into the head of buf (extensible for
           correctness checking) */
        memcpy(buf, &idx, sizeof(idx));
        if (s->impl == IMPL_DPDK)
            dpdk_push_wait((dpdk_ring_t *)s->ring, buf);
        else
            vy_push_wait((vy_ring_t *)s->ring, buf);
    }
    return NULL;
}

static void *consumer_fn(void *arg) {
    bench_state_t *s = arg;
    char buf[64];
    for (;;) {
        long idx = atomic_fetch_add_explicit(&s->recvd, 1, memory_order_relaxed);
        if (idx >= s->target) break;
        if (s->impl == IMPL_DPDK)
            dpdk_pop_wait((dpdk_ring_t *)s->ring, buf);
        else
            vy_pop_wait((vy_ring_t *)s->ring, buf);
    }
    return NULL;
}

/* returns throughput in Mops/s */
static double run_bench(impl_t impl, void *ring, size_t elem_size,
                        int nprод, int ncons, long total) {
    bench_state_t s = {
        .impl      = impl,
        .ring      = ring,
        .elem_size = elem_size,
        .target    = total,
    };
    atomic_init(&s.sent,  0);
    atomic_init(&s.recvd, 0);

    pthread_t *pthr = malloc(sizeof(pthread_t) * (size_t)(nprод + ncons));

    int64_t t0 = now_ns();
    for (int i = 0; i < nprод; i++)
        pthread_create(&pthr[i], NULL, producer_fn, &s);
    for (int i = 0; i < ncons; i++)
        pthread_create(&pthr[nprод + i], NULL, consumer_fn, &s);
    for (int i = 0; i < nprод + ncons; i++)
        pthread_join(pthr[i], NULL);
    int64_t elapsed = now_ns() - t0;

    free(pthr);
    return (double)total / ((double)elapsed / 1e9) / 1e6;
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(void) {
    static const int  thread_counts[] = { 1, 2, 4, 8 };
    static const size_t elem_sizes[]  = { 8, 64 };
    static const char *ec_names[]     = { "8B ", "64B" };
    const int ntc = (int)(sizeof(thread_counts) / sizeof(thread_counts[0]));
    const int nes = (int)(sizeof(elem_sizes)    / sizeof(elem_sizes[0]));

    printf("CPU: rte_ring (DPDK 4-cursor) vs Vyukov per-slot MPMC\n");
    printf("ring capacity: %d  total ops: %ldM/test  one op=push+pop\n\n",
           RING_CAP, TOTAL_OPS / 1000000L);

    printf("%-14s  %-6s  %12s  %12s  %8s\n",
           "scenario", "elem", "DPDK Mops/s", "Vyukov Mops/s", "Vyukov/DPDK");
    printf("%-14s  %-6s  %12s  %12s  %8s\n",
           "--------------", "------",
           "------------", "-------------", "----------");

    for (int ei = 0; ei < nes; ei++) {
        size_t esz = elem_sizes[ei];
        for (int ti = 0; ti < ntc; ti++) {
            int np = thread_counts[ti];

            /* initialize both rings */
            dpdk_ring_t dr; vy_ring_t vr;
            if (!dpdk_init(&dr, RING_CAP, esz) || !vy_init(&vr, RING_CAP, esz)) {
                fprintf(stderr, "ring init failed\n"); return 1;
            }

            /* warmup (use the DPDK ring; both need to warm the CPU cache) */
            bench_state_t ws = { .impl=IMPL_DPDK, .ring=&dr, .elem_size=esz,
                                  .target=WARMUP_OPS };
            atomic_init(&ws.sent, 0); atomic_init(&ws.recvd, 0);
            {
                pthread_t *pt = malloc(sizeof(pthread_t) * (size_t)(np * 2));
                for (int i = 0; i < np; i++) pthread_create(&pt[i], NULL, producer_fn, &ws);
                for (int i = 0; i < np; i++) pthread_create(&pt[np+i], NULL, consumer_fn, &ws);
                for (int i = 0; i < np*2; i++) pthread_join(pt[i], NULL);
                free(pt);
            }

            double dpdk_mops = run_bench(IMPL_DPDK,   &dr, esz, np, np, TOTAL_OPS);
            double vy_mops   = run_bench(IMPL_VYUKOV, &vr, esz, np, np, TOTAL_OPS);
            double ratio     = vy_mops / dpdk_mops;

            char label[16];
            snprintf(label, sizeof(label), "%dP+%dC", np, np);
            printf("%-14s  %-6s  %12.2f  %13.2f  %9.2fx\n",
                   label, ec_names[ei], dpdk_mops, vy_mops, ratio);

            dpdk_destroy(&dr);
            vy_destroy(&vr);
        }
        if (ei < nes - 1) printf("\n");
    }

    printf("\nNotes:\n");
    printf("  DPDK Phase 3 spin: after N producers reserve concurrently, the k-th\n");
    printf("  producer must wait for the first k-1 to commit before it can advance\n");
    printf("  prod.tail (serial convoy).\n");
    printf("  Vyukov per-slot: each producer writes its slot and directly\n");
    printf("  store_releases that slot's seq, with no wait.\n");
    printf("  The larger the element (the longer the Phase 2 memcpy), the wider the\n");
    printf("  convoy window and the more pronounced DPDK's disadvantage.\n");
    return 0;
}
