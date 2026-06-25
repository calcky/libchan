/*
 * worker_pool.c — Fan-out / Fan-in worker pool
 *
 * The most practical parallel-compute skeleton, demonstrating MPMC + close
 * broadcast + buffered backpressure.
 *
 *   submitter ──► [jobs channel] ──► 4 workers compute in parallel ──► [results channel] ──► collector (fan-in)
 *
 * The jobs channel uses a small buffer → natural backpressure when submission
 * outpaces consumption. After submission finishes, chan_close(jobs) makes
 * workers exit once they drain the remaining jobs. The main thread collects N
 * results from results, printing live progress.
 */
#include <stdio.h>
#include <pthread.h>

#include "libchan.h"

#define NWORKERS 4
#define NJOBS    40

typedef struct { int id; int n; }                 job_t;
typedef struct { int job_id, worker_id, result; } res_t;

/* A bit of real CPU work: count the primes below n (trial division). */
static int count_primes_below(int n) {
    int c = 0;
    for (int x = 2; x < n; x++) {
        int prime = 1;
        for (int d = 2; (long)d * d <= x; d++)
            if (x % d == 0) { prime = 0; break; }
        c += prime;
    }
    return c;
}

typedef struct { chan_t *jobs, *results; int wid; } warg_t;

static void *worker(void *arg) {
    warg_t *w = arg;
    job_t j;
    while (chan_recv(w->jobs, &j) == CHAN_OK) {          /* jobs closed → exit */
        res_t r = { j.id, w->wid, count_primes_below(j.n) };
        chan_send(w->results, &r);
    }
    return NULL;
}

int main(void) {
    chan_t *jobs    = chan_create(sizeof(job_t), 8);     /* small buffer → backpressure */
    chan_t *results = chan_create(sizeof(res_t), NJOBS);

    pthread_t wth[NWORKERS];
    warg_t    wa[NWORKERS];
    for (int i = 0; i < NWORKERS; i++) {
        wa[i] = (warg_t){ jobs, results, i };
        pthread_create(&wth[i], NULL, worker, &wa[i]);
    }

    printf("Fan-out worker pool — %d workers processing %d jobs in parallel (primes per number)\n\n",
           NWORKERS, NJOBS);

    /* Submit jobs (produce) */
    for (int i = 0; i < NJOBS; i++) {
        job_t j = { i, 20000 + i * 1500 };
        chan_send(jobs, &j);
    }
    chan_close(jobs);   /* broadcast shutdown */

    /* Collect results (fan-in) + live progress */
    long total = 0;
    for (int got = 0; got < NJOBS; got++) {
        res_t r;
        chan_recv(results, &r);
        total += r.result;
        printf("\rProgress [%2d/%2d]  job#%-2d → worker%d: %d primes        ",
               got + 1, NJOBS, r.job_id, r.worker_id, r.result);
        fflush(stdout);
    }
    printf("\n\nAll done, total primes = %ld\n", total);

    for (int i = 0; i < NWORKERS; i++) pthread_join(wth[i], NULL);
    chan_destroy(jobs);
    chan_destroy(results);
    return 0;
}
