#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include "libchan.h"

#define BENCH_MSGS 1000000

typedef struct { chan_t *ch; long msgs; } barg_t;

static void *bench_prod(void *arg) {
    barg_t *a = arg;
    int v = 1;
    for (long i = 0; i < a->msgs; i++) chan_send(a->ch, &v);
    return NULL;
}

static void *bench_cons(void *arg) {
    barg_t *a = arg;
    int v;
    for (long i = 0; i < a->msgs; i++) chan_recv(a->ch, &v);
    return NULL;
}

static double bench(size_t cap, int nthreads, long msgs_per_pair) {
    chan_t *ch = chan_create(sizeof(int), cap);
    barg_t pa = { ch, msgs_per_pair };
    barg_t ca = { ch, msgs_per_pair };

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    pthread_t *prod = malloc(nthreads * sizeof(pthread_t));
    pthread_t *cons = malloc(nthreads * sizeof(pthread_t));
    for (int i = 0; i < nthreads; i++) {
        pthread_create(&cons[i], NULL, bench_cons, &ca);
        pthread_create(&prod[i], NULL, bench_prod, &pa);
    }
    for (int i = 0; i < nthreads; i++) {
        pthread_join(prod[i], NULL);
        pthread_join(cons[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    double ops = (double)msgs_per_pair * nthreads;
    free(prod); free(cons);
    chan_destroy(ch);
    return ops / secs;
}

int main(void) {
    printf("%-20s %8s %8s %14s\n", "mode", "threads", "cap", "Mops/sec");
    printf("%-20s %8s %8s %14s\n", "----", "-------", "---", "--------");

    int threads[] = {1, 2, 4};
    size_t caps[] = {0, 1, 64, 1024};

    for (int ti = 0; ti < 3; ti++) {
        for (int ci = 0; ci < 4; ci++) {
            int t = threads[ti];
            size_t cap = caps[ci];
            char label[32];
            snprintf(label, sizeof label, cap == 0 ? "unbuf" : "cap=%zu", cap);
            double mops = bench(cap, t, BENCH_MSGS / t) / 1e6;
            printf("%-20s %8d %8zu %14.2f\n", label, t, cap, mops);
        }
    }
    return 0;
}
