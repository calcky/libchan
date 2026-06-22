#include "libchan_internal.h"

#include <sched.h>

void chan_spin_hint(int iteration) {
    if (iteration < 8) {
#if defined(__x86_64__) || defined(__i386__)
        __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
        __asm__ volatile("yield" ::: "memory");
#else
        __asm__ volatile("" ::: "memory");
#endif
    } else {
        sched_yield();
    }
}
