/*
 * test_spsc_order.c — FIFO ordering guarantee.
 *
 * A single producer sends 0,1,2,...,K-1 in order; a single consumer must
 * receive them in exactly that order (strictly increasing, no gaps, no
 * reordering). Verified for:
 *   - chan_create_spsc (the opt-in single-producer/single-consumer fast path),
 *     across several capacities incl. a non-power-of-2 (rounds up internally);
 *   - the default chan_create with one producer (single producer ⇒ also FIFO);
 *   - an unbuffered channel (every message is a rendezvous).
 *
 * cap=1 / cap=2 force frequent parking, exercising the blocking path's order
 * preservation, not just the lock-free fast path.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)

typedef struct { chan_t *ch; long k; } prod_t;
typedef struct { chan_t *ch; _Atomic long *recv; _Atomic long *order_errs; } cons_t;

static void *order_prod(void *a_) {
    prod_t *a = a_;
    for (long i = 0; i < a->k; i++) {
        long v = i;
        chan_send(a->ch, &v);
    }
    return NULL;
}

static void *order_cons(void *a_) {
    cons_t *a = a_;
    long expect = 0;
    for (;;) {
        long v;
        chan_err_t e = chan_recv(a->ch, &v);
        if (e == CHAN_ERR_CLOSED) break;
        if (e != CHAN_OK) continue;
        if (v != expect) {
            long n = atomic_fetch_add_explicit(a->order_errs, 1, memory_order_relaxed);
            if (n < 3) fprintf(stderr, "  OUT-OF-ORDER: expected %ld got %ld\n", expect, v);
        }
        expect++;
        atomic_fetch_add_explicit(a->recv, 1, memory_order_relaxed);
    }
    return NULL;
}

static void run_order(int spsc, size_t cap, long k, const char *tag) {
    chan_t *ch = spsc ? chan_create_spsc(sizeof(long), cap)
                      : chan_create(sizeof(long), cap);
    _Atomic long recv, order_errs;
    atomic_init(&recv, 0);
    atomic_init(&order_errs, 0);

    pthread_t pt, ct;
    prod_t pa = { ch, k };
    cons_t ca = { ch, &recv, &order_errs };
    pthread_create(&ct, NULL, order_cons, &ca);
    pthread_create(&pt, NULL, order_prod, &pa);
    pthread_join(pt, NULL);
    chan_close(ch);
    pthread_join(ct, NULL);

    long got = atomic_load(&recv);
    long oe  = atomic_load(&order_errs);
    if (got != k || oe != 0) {
        fprintf(stderr, "FAIL %s cap=%zu: expected %ld got %ld, %ld out-of-order\n",
                tag, cap, k, got, oe);
        g_failures++;
    }
    chan_destroy(ch);
}

int main(void) {
    static const size_t caps[] = { 1, 2, 64, 100 /* ->128 */, 1024, 4096 };
    const int n = (int)(sizeof(caps) / sizeof(caps[0]));

    printf("test_spsc_order: running (FIFO order, 1 producer + 1 consumer)...\n");

    for (int i = 0; i < n; i++) {
        /* small caps park almost every op → fewer messages keeps CI bounded */
        long k = (caps[i] <= 2) ? 60000 : 250000;
        run_order(1, caps[i], k, "spsc");        /* chan_create_spsc */
        run_order(0, caps[i], k, "1p-mpmc");     /* default MPMC, single producer */
    }
    /* unbuffered: strict rendezvous order */
    run_order(0, 0, 40000, "unbuffered");

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_spsc_order: all passed\n");
    return 0;
}
