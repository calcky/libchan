# Getting Started with libchan

libchan is a high-performance channel library implemented in pure C11, providing channel semantics similar to Go / Rust: unbuffered synchronous handshakes, buffered asynchronous queues, MPMC (multiple producers, multiple consumers), close notification, and Go-like `select` multiplexing.

---

## Building

### Dependencies

- CMake ≥ 3.16
- A C11 compiler (GCC or Clang)
- pthreads (included by default on Linux)

### Quick Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

Or use the top-level `Makefile` directly:

```bash
make          # build the library
make test     # build and run all tests
make bench    # build the benchmarks (requires LIBCHAN_BUILD_BENCH=ON)
make asan     # AddressSanitizer + UBSan tests
make tsan     # ThreadSanitizer tests (requires native Linux; not supported on WSL2)
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `LIBCHAN_BUILD_SHARED` | `ON` | Build the shared library `libchan.so` |
| `LIBCHAN_BUILD_STATIC` | `ON` | Build the static library `libchan.a` |
| `LIBCHAN_BUILD_TESTS` | `ON` | Build the unit tests |
| `LIBCHAN_BUILD_BENCH` | `OFF` | Build the throughput benchmarks |
| `LIBCHAN_FORCE_PTHREAD_PARK` | `OFF` | Force the pthread backend (disables futex, to test the portable path on Linux) |
| `LIBCHAN_SPIN_LIMIT` | `40` | Number of spins before entering a kernel sleep; 0 = disable spinning |
| `LIBCHAN_SANITIZE` | empty | Enable a sanitizer: `address`, `thread`, `undefined`, `address,undefined` |

Example: enabling AddressSanitizer:

```bash
cmake -B build-asan -DLIBCHAN_SANITIZE=address,undefined
cmake --build build-asan --parallel
ctest --test-dir build-asan
```

### Using It in Your Project

**Link the static library directly** (simplest):

```cmake
target_link_libraries(my_app PRIVATE /path/to/libchan.a Threads::Threads)
target_include_directories(my_app PRIVATE /path/to/libchan/include)
```

**CMake find_package** (after installing):

```bash
cmake --install build --prefix /usr/local
```

```cmake
find_package(libchan REQUIRED)
target_link_libraries(my_app PRIVATE chan)
```

---

## Core Concepts

### Unbuffered vs. Buffered

```c
chan_t *unbuf = chan_create(sizeof(int), 0);   /* unbuffered: capacity == 0 */
chan_t *buf   = chan_create(sizeof(int), 64);  /* buffered: capacity 64 */
```

- **Unbuffered**: the sender blocks until a receiver is ready (synchronous handshake, rendezvous). Lowest latency; forces producer/consumer synchronization.
- **Buffered**: the sender returns immediately when the queue has space, and the receiver returns immediately when the queue is non-empty. Decouples producer and consumer pacing.

### Element Size

A channel transfers data by **byte copy**, similar to Go's value-passing semantics:

```c
/* pass an int */
chan_t *ch = chan_create(sizeof(int), 8);

/* pass a pointer (you manage the lifetime of the memory it points to) */
chan_t *ptr_ch = chan_create(sizeof(void *), 8);
```

---

## Common Patterns

### 1. Basic Send/Receive

```c
#include "libchan.h"
#include <stdio.h>
#include <pthread.h>

static void *producer(void *arg) {
    chan_t *ch = arg;
    for (int i = 0; i < 5; i++)
        chan_send(ch, &i);
    chan_close(ch);
    return NULL;
}

int main(void) {
    chan_t *ch = chan_create(sizeof(int), 2);

    pthread_t t;
    pthread_create(&t, NULL, producer, ch);

    int v;
    while (chan_recv(ch, &v) == CHAN_OK)
        printf("got %d\n", v);

    pthread_join(t, NULL);
    chan_destroy(ch);
}
```

### 2. Non-blocking Operations (try_send / try_recv)

Suited to "handle it if there's something, otherwise do something else" scenarios, equivalent to Go's `select { case … default: }`:

```c
int v = 42;
switch (chan_try_send(ch, &v)) {
case CHAN_OK:        /* sent successfully */ break;
case CHAN_ERR_WOULDBLOCK: /* queue full, retry later */ break;
case CHAN_ERR_CLOSED:     /* channel is closed */ break;
default: break;
}
```

```c
int out;
if (chan_try_recv(ch, &out) == CHAN_OK)
    printf("recv: %d\n", out);
```

### 3. Timed Operations

```c
int v = 99;
/* wait at most 200 milliseconds */
chan_err_t e = chan_send_timeout(ch, &v, 200000000LL);
if (e == CHAN_ERR_TIMEOUT)
    fprintf(stderr, "send timed out\n");

int out;
e = chan_recv_timeout(ch, &out, 200000000LL);
```

The timeout argument is in **nanoseconds**:
- `< 0`: wait forever (equivalent to the no-timeout version)
- `== 0`: do not wait (equivalent to the `try_*` version)
- `> 0`: a specified timeout duration

### 4. Close and Draining the Buffer (Go Semantics)

After closing a buffered channel, already-stored data can still be received until it is drained:

```c
chan_t *ch = chan_create(sizeof(int), 4);
for (int i = 0; i < 3; i++) chan_send(ch, &i);
chan_close(ch);

int v;
/* receive 0, 1, 2 in turn, then return CHAN_ERR_CLOSED */
while (chan_recv(ch, &v) == CHAN_OK)
    printf("%d\n", v);

chan_destroy(ch);
```

### 5. Select Multiplexing

Equivalent to Go's `select` statement, picking a ready case with random fairness:

```c
chan_t *timer_ch = chan_create(sizeof(int), 1);
chan_t *data_ch  = chan_create(sizeof(int), 8);

int timer_val, data_val;
chan_select_case_t cases[2] = {
    { data_ch,  CHAN_OP_RECV, &data_val,  CHAN_OK },
    { timer_ch, CHAN_OP_RECV, &timer_val, CHAN_OK },
};

int w = chan_select(cases, 2);   /* block until any is ready */
switch (w) {
case 0: printf("data: %d\n",  data_val);  break;
case 1: printf("timer fired\n");          break;
case -1: /* argument error */ break;
}
```

**Select with a timeout** (emulating `time.After`):

```c
/* wait for data, at most 500ms */
int w = chan_select_timeout(cases, 2, 500000000LL);
if (w == -1)
    printf("timeout or no data\n");
```

**Non-blocking select** (equivalent to `select { … default: }`):

```c
int w = chan_select_try(cases, 2);
if (w == -1)
    printf("no case ready, doing other work\n");
```

### 6. Mixed Send/Receive Cases

```c
int send_val = 10, recv_val;
chan_select_case_t cases[2] = {
    { out_ch, CHAN_OP_SEND, &send_val, CHAN_OK },
    { in_ch,  CHAN_OP_RECV, &recv_val, CHAN_OK },
};
int w = chan_select(cases, 2);
if (w == 0) printf("sent %d\n",    send_val);
if (w == 1) printf("received %d\n", recv_val);
```

---

## Lifecycle and Thread Safety

### Basic Rules

```c
chan_t *ch = chan_create(sizeof(int), 8);  /* refcount = 1 */

/* when multiple threads each need to hold a reference */
chan_t *ref2 = chan_retain(ch);            /* refcount = 2 */

/* each "holder" calls destroy when it is no longer in use */
chan_destroy(ref2);                        /* refcount = 1 */
chan_destroy(ch);                          /* refcount = 0 → frees memory */
```

### Contracts

1. Before calling `chan_destroy`, you **must ensure that this thread has completed all operations on the channel**.
2. Do not access the channel pointer after `chan_destroy`.
3. **All public functions are thread-safe** (protected internally by a mutex) and can be called from any thread.
4. `chan_close` can be called from any thread and is idempotent (repeated calls return `CHAN_ERR_CLOSED` and do not crash).

---

## Error Handling

Always check the return value:

```c
chan_err_t e = chan_send(ch, &v);
if (e != CHAN_OK) {
    fprintf(stderr, "chan_send: %s\n", chan_strerror(e));
    /* handle the error */
}
```

Quick reference of common error codes:

| Error code | Meaning |
|------------|---------|
| `CHAN_OK` | Success |
| `CHAN_ERR_CLOSED` | The channel is closed (send failed; or, on receive, closed and already drained) |
| `CHAN_ERR_TIMEOUT` | The timeout deadline expired |
| `CHAN_ERR_WOULDBLOCK` | A `try_*` operation had no immediately available sender/receiver |
| `CHAN_ERR_INVALID` | An invalid argument (a NULL pointer, etc.) |
| `CHAN_ERR_NOMEM` | Memory allocation failed in `chan_create` |

For full details, see the [API Reference](api_reference.md).
