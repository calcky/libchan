/*
 * select_fairness.c — Visualise Go-style select fairness
 *
 * When multiple chan_select cases are simultaneously ready, libchan picks one
 * uniformly at random (see doc/design.md). This demo keeps three buffered
 * channels perpetually non-empty and runs many selects, then prints how often
 * each case won.
 *
 *   ch0 ──┐
 *   ch1 ──┼──► chan_select (all recv cases ready every round)
 *   ch2 ──┘
 *
 * With fair random choice, each channel should land near 33%% over many trials.
 */
#include <stdio.h>

#include "libchan.h"

#define N_CH     3
#define TRIALS   30000
#define TOL_PCT  8.0    /* warn if any channel deviates more than this from 33.3% */

static void refill(chan_t *chs[N_CH]) {
    int v = 0;
    for (int i = 0; i < N_CH; i++) {
        if (chan_len(chs[i]) == 0)
            chan_try_send(chs[i], &v);
    }
}

int main(void) {
    chan_t *chs[N_CH];
    unsigned counts[N_CH] = {0};
    int out;

    for (int i = 0; i < N_CH; i++)
        chs[i] = chan_create(sizeof(int), 4);

    refill(chs);

    printf("select_fairness — %d trials, %d recv cases always ready\n\n",
           TRIALS, N_CH);

    for (int t = 0; t < TRIALS; t++) {
        refill(chs);

        chan_select_case_t cases[N_CH];
        for (int i = 0; i < N_CH; i++)
            cases[i] = (chan_select_case_t){ chs[i], CHAN_OP_RECV, &out, CHAN_OK };

        int w = chan_select(cases, N_CH);
        if (w < 0 || w >= N_CH) {
            fprintf(stderr, "unexpected select result %d\n", w);
            return 1;
        }
        counts[w]++;
    }

    printf("Wins per channel (expect ~%.1f%% each):\n", 100.0 / N_CH);
    int failures = 0;
    for (int i = 0; i < N_CH; i++) {
        double pct = 100.0 * (double)counts[i] / (double)TRIALS;
        printf("  ch%d: %6u  (%5.2f%%)", i, counts[i], pct);
        if (pct < 100.0 / N_CH - TOL_PCT || pct > 100.0 / N_CH + TOL_PCT) {
            printf("  <- outside ±%.0f%%", TOL_PCT);
            failures++;
        }
        printf("\n");
    }

    if (failures)
        printf("\nNote: %d channel(s) outside tolerance — randomness can vary; re-run or increase TRIALS.\n",
               failures);
    else
        printf("\nDistribution within ±%.0f%% of uniform — select fairness looks good.\n", TOL_PCT);

    for (int i = 0; i < N_CH; i++) {
        chan_close(chs[i]);
        chan_destroy(chs[i]);
    }
    return failures ? 1 : 0;
}
