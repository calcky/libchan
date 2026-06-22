#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)

#define N_PRODUCERS 4
#define N_CONSUMERS 4
#define MSGS_PER_PRODUCER 10000
#define TOTAL_MSGS (N_PRODUCERS * MSGS_PER_PRODUCER)

typedef struct { chan_t *ch; int id; } thread_arg_t;

static _Atomic int received_count;
/* Bitset: mark each message ID as received exactly once */
static _Atomic int received_ids[TOTAL_MSGS];

static void *producer(void *arg) {
    thread_arg_t *a = arg;
    for (int i = 0; i < MSGS_PER_PRODUCER; i++) {
        int msg = a->id * MSGS_PER_PRODUCER + i;
        chan_send(a->ch, &msg);
    }
    return NULL;
}

static void *consumer(void *arg) {
    thread_arg_t *a = arg;
    for (;;) {
        int v;
        chan_err_t e = chan_recv(a->ch, &v);
        if (e == CHAN_ERR_CLOSED) break;
        if (e != CHAN_OK) continue;
        /* Mark received — detect duplicates */
        int prev = atomic_fetch_add_explicit(&received_ids[v], 1, memory_order_relaxed);
        CHECK(prev == 0); /* should be first time */
        atomic_fetch_add_explicit(&received_count, 1, memory_order_relaxed);
    }
    return NULL;
}

static void run_mpmc(size_t capacity) {
    atomic_store(&received_count, 0);
    for (int i = 0; i < TOTAL_MSGS; i++) atomic_store(&received_ids[i], 0);

    chan_t *ch = chan_create(sizeof(int), capacity);

    pthread_t prod[N_PRODUCERS], cons[N_CONSUMERS];
    thread_arg_t pargs[N_PRODUCERS], cargs[N_CONSUMERS];

    for (int i = 0; i < N_CONSUMERS; i++) {
        cargs[i] = (thread_arg_t){ ch, i };
        pthread_create(&cons[i], NULL, consumer, &cargs[i]);
    }
    for (int i = 0; i < N_PRODUCERS; i++) {
        pargs[i] = (thread_arg_t){ ch, i };
        pthread_create(&prod[i], NULL, producer, &pargs[i]);
    }
    for (int i = 0; i < N_PRODUCERS; i++) pthread_join(prod[i], NULL);

    chan_close(ch);
    for (int i = 0; i < N_CONSUMERS; i++) pthread_join(cons[i], NULL);

    CHECK(atomic_load(&received_count) == TOTAL_MSGS);
    chan_destroy(ch);
}

static void run_mpmc_unbuffered(void) {
    atomic_store(&received_count, 0);
    for (int i = 0; i < TOTAL_MSGS; i++) atomic_store(&received_ids[i], 0);

    chan_t *ch = chan_create(sizeof(int), 0);

    pthread_t prod[N_PRODUCERS], cons[N_CONSUMERS];
    thread_arg_t pargs[N_PRODUCERS], cargs[N_CONSUMERS];

    for (int i = 0; i < N_CONSUMERS; i++) {
        cargs[i] = (thread_arg_t){ ch, i };
        pthread_create(&cons[i], NULL, consumer, &cargs[i]);
    }
    for (int i = 0; i < N_PRODUCERS; i++) {
        pargs[i] = (thread_arg_t){ ch, i };
        pthread_create(&prod[i], NULL, producer, &pargs[i]);
    }
    for (int i = 0; i < N_PRODUCERS; i++) pthread_join(prod[i], NULL);

    chan_close(ch);
    for (int i = 0; i < N_CONSUMERS; i++) pthread_join(cons[i], NULL);

    CHECK(atomic_load(&received_count) == TOTAL_MSGS);
    chan_destroy(ch);
}

int main(void) {
    run_mpmc(1);
    run_mpmc(16);
    run_mpmc(256);
    run_mpmc_unbuffered();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_mpmc: all passed\n");
    return 0;
}
