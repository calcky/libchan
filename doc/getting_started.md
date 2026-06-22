# Getting Started with libchan

libchan 是一个纯 C11 实现的高性能 channel 库，提供与 Go / Rust 类似的 channel 语义：无缓冲同步握手、有缓冲异步队列、MPMC（多生产者多消费者）、close 通知、以及类 Go `select` 的多路复用。

---

## 构建

### 依赖

- CMake ≥ 3.16
- C11 编译器（GCC 或 Clang）
- pthreads（Linux 默认包含）

### 快速构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

或者直接用顶层 `Makefile`：

```bash
make          # 构建库
make test     # 构建并运行所有测试
make bench    # 构建性能基准（需要 LIBCHAN_BUILD_BENCH=ON）
make asan     # AddressSanitizer + UBSan 测试
make tsan     # ThreadSanitizer 测试（需要原生 Linux，WSL2 不支持）
```

### 构建选项

| 选项 | 默认 | 说明 |
|------|------|------|
| `LIBCHAN_BUILD_SHARED` | `ON` | 构建动态库 `libchan.so` |
| `LIBCHAN_BUILD_STATIC` | `ON` | 构建静态库 `libchan.a` |
| `LIBCHAN_BUILD_TESTS` | `ON` | 构建单元测试 |
| `LIBCHAN_BUILD_BENCH` | `OFF` | 构建吞吐量基准 |
| `LIBCHAN_FORCE_PTHREAD_PARK` | `OFF` | 强制使用 pthread 后端（禁用 futex，便于在 Linux 上测试可移植路径） |
| `LIBCHAN_SPIN_LIMIT` | `40` | 进入内核睡眠前的自旋次数，0 = 禁用自旋 |
| `LIBCHAN_SANITIZE` | 空 | 开启 sanitizer：`address`、`thread`、`undefined`、`address,undefined` |

示例：开启 AddressSanitizer：

```bash
cmake -B build-asan -DLIBCHAN_SANITIZE=address,undefined
cmake --build build-asan --parallel
ctest --test-dir build-asan
```

### 在你的项目中使用

**直接链接静态库**（最简单）：

```cmake
target_link_libraries(my_app PRIVATE /path/to/libchan.a Threads::Threads)
target_include_directories(my_app PRIVATE /path/to/libchan/include)
```

**CMake find_package**（安装后）：

```bash
cmake --install build --prefix /usr/local
```

```cmake
find_package(libchan REQUIRED)
target_link_libraries(my_app PRIVATE chan)
```

---

## 核心概念

### 无缓冲 vs. 有缓冲

```c
chan_t *unbuf = chan_create(sizeof(int), 0);   /* 无缓冲：capacity == 0 */
chan_t *buf   = chan_create(sizeof(int), 64);  /* 有缓冲：容量 64 */
```

- **无缓冲**：发送方阻塞直到接收方就位（同步握手，rendezvous）。延迟最低，强迫生产消费同步。
- **有缓冲**：发送方在队列有空间时立即返回，接收方在队列非空时立即返回。解耦生产消费节奏。

### 元素大小

channel 按**字节拷贝**传递数据，类似 Go 的值传递语义：

```c
/* 传递 int */
chan_t *ch = chan_create(sizeof(int), 8);

/* 传递指针（自行管理指针所指内存的生命周期） */
chan_t *ptr_ch = chan_create(sizeof(void *), 8);
```

---

## 常用模式

### 1. 基础发送/接收

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

### 2. 非阻塞操作（try_send / try_recv）

适用于"有就处理，没有就干别的"场景，等价 Go 的 `select { case … default: }`:

```c
int v = 42;
switch (chan_try_send(ch, &v)) {
case CHAN_OK:        /* 发送成功 */ break;
case CHAN_ERR_WOULDBLOCK: /* 队列满，稍后再试 */ break;
case CHAN_ERR_CLOSED:     /* 通道已关闭 */ break;
default: break;
}
```

```c
int out;
if (chan_try_recv(ch, &out) == CHAN_OK)
    printf("recv: %d\n", out);
```

### 3. 超时操作

```c
int v = 99;
/* 等待最多 200 毫秒 */
chan_err_t e = chan_send_timeout(ch, &v, 200000000LL);
if (e == CHAN_ERR_TIMEOUT)
    fprintf(stderr, "send timed out\n");

int out;
e = chan_recv_timeout(ch, &out, 200000000LL);
```

超时参数单位为**纳秒**：
- `< 0`：永远等待（等价无超时版本）
- `== 0`：不等待（等价 `try_*` 版本）
- `> 0`：指定超时时长

### 4. close 与排空缓冲区（Go 语义）

关闭有缓冲 channel 后，已存入的数据仍可继续接收，直到排空为止：

```c
chan_t *ch = chan_create(sizeof(int), 4);
for (int i = 0; i < 3; i++) chan_send(ch, &i);
chan_close(ch);

int v;
/* 依次收到 0, 1, 2，然后返回 CHAN_ERR_CLOSED */
while (chan_recv(ch, &v) == CHAN_OK)
    printf("%d\n", v);

chan_destroy(ch);
```

### 5. Select 多路复用

等价 Go 的 `select` 语句，随机公平选择一个就绪的 case：

```c
chan_t *timer_ch = chan_create(sizeof(int), 1);
chan_t *data_ch  = chan_create(sizeof(int), 8);

int timer_val, data_val;
chan_select_case_t cases[2] = {
    { data_ch,  CHAN_OP_RECV, &data_val,  CHAN_OK },
    { timer_ch, CHAN_OP_RECV, &timer_val, CHAN_OK },
};

int w = chan_select(cases, 2);   /* 阻塞直到任一就绪 */
switch (w) {
case 0: printf("data: %d\n",  data_val);  break;
case 1: printf("timer fired\n");          break;
case -1: /* 参数错误 */ break;
}
```

**带超时的 select**（模拟 `time.After`）：

```c
/* 等待数据，最多 500ms */
int w = chan_select_timeout(cases, 2, 500000000LL);
if (w == -1)
    printf("timeout or no data\n");
```

**非阻塞 select**（等价 `select { … default: }`）：

```c
int w = chan_select_try(cases, 2);
if (w == -1)
    printf("no case ready, doing other work\n");
```

### 6. 多发送/接收 case 混合

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

## 生命周期与线程安全

### 基本规则

```c
chan_t *ch = chan_create(sizeof(int), 8);  /* refcount = 1 */

/* 当多个线程需要各自持有一份引用时 */
chan_t *ref2 = chan_retain(ch);            /* refcount = 2 */

/* 每个"持有者"在不再使用时调用 destroy */
chan_destroy(ref2);                        /* refcount = 1 */
chan_destroy(ch);                          /* refcount = 0 → 释放内存 */
```

### 契约

1. 调用 `chan_destroy` 之前，**必须保证本线程对该 channel 的所有操作已经完成**。
2. 不要在 `chan_destroy` 之后再访问该 channel 指针。
3. **所有公开函数均线程安全**（内部由 mutex 保护），可从任意线程调用。
4. `chan_close` 可以从任意线程调用，且幂等（重复调用返回 `CHAN_ERR_CLOSED`，不崩溃）。

---

## 错误处理

始终检查返回值：

```c
chan_err_t e = chan_send(ch, &v);
if (e != CHAN_OK) {
    fprintf(stderr, "chan_send: %s\n", chan_strerror(e));
    /* 处理错误 */
}
```

常见错误码速查：

| 错误码 | 含义 |
|--------|------|
| `CHAN_OK` | 成功 |
| `CHAN_ERR_CLOSED` | channel 已关闭（发送失败；或接收时已关闭且已排空） |
| `CHAN_ERR_TIMEOUT` | 超时期限到期 |
| `CHAN_ERR_WOULDBLOCK` | `try_*` 操作无立即可用的发送/接收方 |
| `CHAN_ERR_INVALID` | 参数无效（NULL 指针等） |
| `CHAN_ERR_NOMEM` | `chan_create` 内存分配失败 |

完整说明见 [API Reference](api_reference.md)。
