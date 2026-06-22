#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)

#define STRESS_PRODUCERS  8
#define STRESS_CONSUMERS  8
#define STRESS_MSGS       50000
#define STRESS_TOTAL      (STRESS_PRODUCERS * STRESS_MSGS)

static _Atomic long stress_received;

typedef struct { chan_t *ch; int id; } targ_t;

static void *stress_prod(void *arg) {
    targ_t *a = arg;
    for (int i = 0; i < STRESS_MSGS; i++) {
        int v = a->id * STRESS_MSGS + i;
        chan_send(a->ch, &v);
    }
    return NULL;
}

static void *stress_cons(void *arg) {
    targ_t *a = arg;
    for (;;) {
        int v;
        chan_err_t e = chan_recv(a->ch, &v);
        if (e == CHAN_ERR_CLOSED) break;
        if (e == CHAN_OK) atomic_fetch_add(&stress_received, 1);
    }
    return NULL;
}

static void run_stress(size_t cap, const char *label) {
    atomic_store(&stress_received, 0);
    chan_t *ch = chan_create(sizeof(int), cap);

    pthread_t prod[STRESS_PRODUCERS], cons[STRESS_CONSUMERS];
    targ_t pa[STRESS_PRODUCERS], ca[STRESS_CONSUMERS];

    for (int i = 0; i < STRESS_CONSUMERS; i++) {
        ca[i] = (targ_t){ ch, i };
        pthread_create(&cons[i], NULL, stress_cons, &ca[i]);
    }
    for (int i = 0; i < STRESS_PRODUCERS; i++) {
        pa[i] = (targ_t){ ch, i };
        pthread_create(&prod[i], NULL, stress_prod, &pa[i]);
    }
    for (int i = 0; i < STRESS_PRODUCERS; i++) pthread_join(prod[i], NULL);
    chan_close(ch);
    for (int i = 0; i < STRESS_CONSUMERS; i++) pthread_join(cons[i], NULL);

    long got = atomic_load(&stress_received);
    if (got != STRESS_TOTAL) {
        fprintf(stderr, "FAIL stress %s: expected %d got %ld\n",
                label, STRESS_TOTAL, got);
        g_failures++;
    }
    chan_destroy(ch);
    printf("  stress %s: %d msgs, all received\n", label, STRESS_TOTAL);
}

int main(void) {
    printf("test_stress: running (this may take a few seconds)...\n");
    run_stress(0,    "unbuffered");
    run_stress(1,    "cap=1");
    run_stress(64,   "cap=64");
    run_stress(1024, "cap=1024");

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_stress: all passed\n");
    return 0;
}
