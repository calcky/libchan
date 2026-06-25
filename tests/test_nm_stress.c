/*
 * test_nm_stress.c — exhaustive N×M producer/consumer exactly-once stress.
 *
 * For a matrix of (producers, consumers, capacity) shapes: each producer p
 * sends K distinct values (p*K .. p*K+K-1); consumers drain until the channel
 * is closed. Afterwards we verify, for every shape:
 *   - count conservation: exactly np*K messages were received;
 *   - exactly-once: every value was received exactly once — no loss, no
 *     duplication (a per-value atomic seen-counter catches both).
 *
 * This is the strongest end-to-end correctness check: it would have caught the
 * buffered-send message-loss bug (a missing value) and any double-delivery.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)

#define MAX_T 16

typedef struct { chan_t *ch; long base; long k; } prod_arg_t;
typedef struct { chan_t *ch; _Atomic int *seen; _Atomic long *count; _Atomic long *dups; } cons_arg_t;

static void *nm_prod(void *a_) {
    prod_arg_t *a = a_;
    for (long i = 0; i < a->k; i++) {
        int v = (int)(a->base + i);
        chan_send(a->ch, &v);
    }
    return NULL;
}

static void *nm_cons(void *a_) {
    cons_arg_t *a = a_;
    for (;;) {
        int v;
        chan_err_t e = chan_recv(a->ch, &v);
        if (e == CHAN_ERR_CLOSED) break;
        if (e != CHAN_OK) continue;
        int prev = atomic_fetch_add_explicit(&a->seen[v], 1, memory_order_relaxed);
        if (prev != 0) atomic_fetch_add_explicit(a->dups, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(a->count, 1, memory_order_relaxed);
    }
    return NULL;
}

static void run_nm(int np, int nc, size_t cap, long k) {
    long total = (long)np * k;
    _Atomic int *seen = calloc((size_t)total, sizeof(_Atomic int));
    if (!seen) { fprintf(stderr, "OOM (%ld slots)\n", total); exit(2); }

    _Atomic long count, dups;
    atomic_init(&count, 0);
    atomic_init(&dups, 0);

    chan_t *ch = chan_create(sizeof(int), cap);
    pthread_t pt[MAX_T], ct[MAX_T];
    prod_arg_t pa[MAX_T];
    cons_arg_t ca[MAX_T];

    for (int i = 0; i < nc; i++) {
        ca[i] = (cons_arg_t){ ch, seen, &count, &dups };
        pthread_create(&ct[i], NULL, nm_cons, &ca[i]);
    }
    for (int i = 0; i < np; i++) {
        pa[i] = (prod_arg_t){ ch, (long)i * k, k };
        pthread_create(&pt[i], NULL, nm_prod, &pa[i]);
    }
    for (int i = 0; i < np; i++) pthread_join(pt[i], NULL);
    chan_close(ch);
    for (int i = 0; i < nc; i++) pthread_join(ct[i], NULL);

    long got = atomic_load(&count);
    long dd  = atomic_load(&dups);
    long bad = 0;
    for (long v = 0; v < total; v++)
        if (atomic_load_explicit(&seen[v], memory_order_relaxed) != 1) bad++;

    if (got != total || dd != 0 || bad != 0) {
        fprintf(stderr,
            "FAIL np=%d nc=%d cap=%zu: expected %ld, got %ld, dups %ld, wrong-count slots %ld\n",
            np, nc, cap, total, got, dd, bad);
        g_failures++;
    }
    chan_destroy(ch);
    free(seen);
}

int main(void) {
    /* Shapes cover symmetric, asymmetric, and prime counts. */
    static const int shapes[][2] = {
        {1,1}, {2,2}, {4,4}, {8,8},
        {1,8}, {8,1}, {3,5}, {5,3}, {2,7}, {7,2},
    };
    static const size_t caps[] = { 0, 1, 4, 64, 1024 };
    const int ns   = (int)(sizeof(shapes) / sizeof(shapes[0]));
    const int ncap = (int)(sizeof(caps)   / sizeof(caps[0]));

    /* Unbuffered (cap=0) forces a rendezvous per message and is far slower, so
     * use fewer messages there to keep CI (TSan/coverage) time bounded. */
    const long K_BUF   = 5000;
    const long K_UNBUF = 1500;

    printf("test_nm_stress: running (%d shapes x %d caps, exactly-once)...\n", ns, ncap);
    for (int s = 0; s < ns; s++) {
        for (int c = 0; c < ncap; c++) {
            long k = (caps[c] == 0) ? K_UNBUF : K_BUF;
            run_nm(shapes[s][0], shapes[s][1], caps[c], k);
        }
    }

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_nm_stress: all passed\n");
    return 0;
}
