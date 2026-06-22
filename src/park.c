#include "libchan_internal.h"

#include <errno.h>
#include <time.h>

#if defined(LIBCHAN_USE_FUTEX)
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

void chan_park_init(chan_park_t *p) {
    atomic_init(&p->word, 0);
}

void chan_park_destroy(chan_park_t *p) {
    (void)p;
}

bool chan_park_wait(chan_park_t *p, int64_t timeout_ns) {
    /* Spin briefly before entering the kernel */
    for (int i = 0; i < LIBCHAN_SPIN_LIMIT; i++) {
        if (atomic_load_explicit(&p->word, memory_order_acquire) != 0)
            return true;
        chan_spin_hint(i);
    }

    if (atomic_load_explicit(&p->word, memory_order_acquire) != 0)
        return true;

    if (timeout_ns == 0)
        return false;

    struct timespec ts;
    struct timespec *tsp = NULL;
    if (timeout_ns > 0) {
        ts.tv_sec  = timeout_ns / 1000000000LL;
        ts.tv_nsec = timeout_ns % 1000000000LL;
        tsp = &ts;
    }

    /* Loop to handle spurious EINTR */
    while (atomic_load_explicit(&p->word, memory_order_acquire) == 0) {
        long r = syscall(SYS_futex, &p->word, FUTEX_WAIT, (uint32_t)0, tsp, NULL, 0);
        if (r == -1 && errno == ETIMEDOUT)
            return false;
    }
    return true;
}

void chan_park_wake(chan_park_t *p) {
    atomic_store_explicit(&p->word, 1, memory_order_release);
    syscall(SYS_futex, &p->word, FUTEX_WAKE, 1, NULL, NULL, 0);
}

#else /* POSIX pthread fallback */

void chan_park_init(chan_park_t *p) {
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv, NULL);
    p->signaled = false;
}

void chan_park_destroy(chan_park_t *p) {
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv);
}

bool chan_park_wait(chan_park_t *p, int64_t timeout_ns) {
    /* Spin briefly */
    for (int i = 0; i < LIBCHAN_SPIN_LIMIT; i++) {
        /* Load with a plain atomic peek (signaled is plain bool here) */
        pthread_mutex_lock(&p->mu);
        bool s = p->signaled;
        pthread_mutex_unlock(&p->mu);
        if (s) return true;
        chan_spin_hint(i);
    }

    pthread_mutex_lock(&p->mu);
    if (p->signaled) {
        pthread_mutex_unlock(&p->mu);
        return true;
    }

    if (timeout_ns == 0) {
        pthread_mutex_unlock(&p->mu);
        return false;
    }

    bool woken = false;
    if (timeout_ns < 0) {
        while (!p->signaled)
            pthread_cond_wait(&p->cv, &p->mu);
        woken = true;
    } else {
        struct timespec abs;
        clock_gettime(CLOCK_REALTIME, &abs);
        abs.tv_sec  += timeout_ns / 1000000000LL;
        abs.tv_nsec += timeout_ns % 1000000000LL;
        if (abs.tv_nsec >= 1000000000L) {
            abs.tv_sec++;
            abs.tv_nsec -= 1000000000L;
        }
        while (!p->signaled) {
            int rc = pthread_cond_timedwait(&p->cv, &p->mu, &abs);
            if (rc == ETIMEDOUT) break;
        }
        woken = p->signaled;
    }

    pthread_mutex_unlock(&p->mu);
    return woken;
}

void chan_park_wake(chan_park_t *p) {
    pthread_mutex_lock(&p->mu);
    p->signaled = true;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mu);
}

#endif /* LIBCHAN_USE_FUTEX */
