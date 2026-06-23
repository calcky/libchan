# API Reference

本文档覆盖 `include/libchan.h` 中的全部公开接口。

---

## 头文件

```c
#include "libchan.h"
```

链接时需要 `-lchan -lpthread`（静态库用 `-l:libchan.a -lpthread`）。

---

## 类型

### `chan_t`

```c
typedef struct chan chan_t;
```

Channel 的不透明句柄。内部布局不属于公开 ABI，不应直接访问其字段。所有操作均通过函数调用完成。

---

### `chan_err_t`

```c
typedef enum {
    CHAN_OK             = 0,
    CHAN_ERR_CLOSED     = 1,
    CHAN_ERR_TIMEOUT    = 2,
    CHAN_ERR_WOULDBLOCK = 3,
    CHAN_ERR_INVALID    = 4,
    CHAN_ERR_NOMEM      = 5,
} chan_err_t;
```

| 值 | 含义 |
|----|------|
| `CHAN_OK` | 操作成功完成 |
| `CHAN_ERR_CLOSED` | 发送时 channel 已关闭；或接收时 channel 已关闭且缓冲区已空 |
| `CHAN_ERR_TIMEOUT` | 超时期限到期前操作未完成 |
| `CHAN_ERR_WOULDBLOCK` | 非阻塞操作（`try_*`）无法立即完成 |
| `CHAN_ERR_INVALID` | 参数无效（如 NULL 指针） |
| `CHAN_ERR_NOMEM` | `chan_create` 时内存分配失败 |

---

### `chan_op_t`

```c
typedef enum {
    CHAN_OP_SEND = 0,
    CHAN_OP_RECV = 1,
} chan_op_t;
```

用于 `chan_select_case_t`，指定该 case 是发送还是接收。

---

### `chan_select_case_t`

```c
typedef struct {
    chan_t     *ch;
    chan_op_t   op;
    void       *data;
    chan_err_t  result;
} chan_select_case_t;
```

| 字段 | 说明 |
|------|------|
| `ch` | 目标 channel，不能为 NULL |
| `op` | `CHAN_OP_SEND` 或 `CHAN_OP_RECV` |
| `data` | **SEND**：指向待发送数据的指针（只读，大小须与 `chan_create` 时的 `elem_size` 一致）<br>**RECV**：指向接收缓冲区的指针（成功时写入） |
| `result` | `chan_select*` 返回后由库填充，表示该 case 的操作结果（`CHAN_OK`、`CHAN_ERR_CLOSED` 等） |

---

## 生命周期

### `chan_create`

```c
chan_t *chan_create(size_t elem_size, size_t capacity);
```

创建一个新 channel。

| 参数 | 说明 |
|------|------|
| `elem_size` | 每个元素的字节大小，必须 > 0 |
| `capacity` | `0` = 无缓冲（同步握手）；`> 0` = 有缓冲（固定容量 FIFO 队列） |

**返回值**：成功返回非 NULL 的 channel 句柄（初始引用计数为 1）；内存不足返回 `NULL`。

**线程安全**：是（新创建的对象，调用本身不涉及并发）。

---

### `chan_destroy`

```c
void chan_destroy(chan_t *ch);
```

递减引用计数。当引用计数归零时释放所有内部资源。

**调用契约**：调用前必须保证本线程对 `ch` 的所有操作已完成，之后不得再访问 `ch`。

**线程安全**：是（原子引用计数）。

---

### `chan_retain`

```c
chan_t *chan_retain(chan_t *ch);
```

递增引用计数，返回 `ch` 本身。适用于需要在多个线程中各自持有独立引用的场景。

**示例**：

```c
chan_t *ref = chan_retain(ch);   /* 现在有两个引用 */
/* ... 线程 A 使用 ref，线程 B 使用 ch ... */
chan_destroy(ref);               /* 线程 A 释放 */
chan_destroy(ch);                /* 线程 B 释放，此时真正销毁 */
```

---

## 关闭

### `chan_close`

```c
chan_err_t chan_close(chan_t *ch);
```

关闭 channel，语义：

- 所有当前阻塞的**发送方**立即被唤醒，返回 `CHAN_ERR_CLOSED`。
- 所有当前阻塞的**接收方**立即被唤醒，返回 `CHAN_ERR_CLOSED`（无缓冲 channel），或在缓冲区排空后才返回 `CHAN_ERR_CLOSED`（有缓冲 channel，见 `chan_recv`）。
- 关闭后的 `chan_send` / `chan_try_send` 始终返回 `CHAN_ERR_CLOSED`。
- 幂等：重复调用返回 `CHAN_ERR_CLOSED`，不崩溃。

**返回值**：首次关闭返回 `CHAN_OK`；已关闭返回 `CHAN_ERR_CLOSED`。

**线程安全**：是，可从任意线程调用。

---

### `chan_is_closed`

```c
bool chan_is_closed(const chan_t *ch);
```

无锁快速检测 channel 是否已关闭（基于原子读）。返回值仅反映调用时刻的状态，不提供同步保证——在多线程场景中应配合返回值检查使用，而非作为唯一判断依据。

---

## 发送

所有发送函数传递 `data` 所指内存的**浅拷贝**（`memcpy`），大小由 `chan_create` 时的 `elem_size` 决定。

### `chan_send`

```c
chan_err_t chan_send(chan_t *ch, const void *data);
```

阻塞发送，等价 `chan_send_timeout(ch, data, -1)`。

| 返回值 | 条件 |
|--------|------|
| `CHAN_OK` | 数据已成功交付接收方或已入队 |
| `CHAN_ERR_CLOSED` | channel 已关闭 |
| `CHAN_ERR_INVALID` | `ch` 或 `data` 为 NULL |

---

### `chan_try_send`

```c
chan_err_t chan_try_send(chan_t *ch, const void *data);
```

非阻塞发送，等价 `chan_send_timeout(ch, data, 0)`。若无接收方就位（无缓冲）或缓冲区已满（有缓冲），立即返回 `CHAN_ERR_WOULDBLOCK`，不阻塞。

---

### `chan_send_timeout`

```c
chan_err_t chan_send_timeout(chan_t *ch, const void *data, int64_t timeout_ns);
```

带超时的发送。

| `timeout_ns` | 行为 |
|--------------|------|
| `< 0` | 永远等待（等价 `chan_send`） |
| `== 0` | 不等待（等价 `chan_try_send`） |
| `> 0` | 等待最多 `timeout_ns` 纳秒 |

超时返回 `CHAN_ERR_TIMEOUT`，channel 状态不受影响（数据未发送）。

---

## 接收

### `chan_recv`

```c
chan_err_t chan_recv(chan_t *ch, void *out);
```

阻塞接收，成功时将数据写入 `out`。

| 返回值 | 条件 |
|--------|------|
| `CHAN_OK` | 数据已写入 `*out` |
| `CHAN_ERR_CLOSED` | channel 已关闭**且**缓冲区已空（有缓冲 channel 的 close 语义：先排空已有数据，再返回此错误） |
| `CHAN_ERR_INVALID` | `ch` 或 `out` 为 NULL |

**注意**：有缓冲 channel 在 close 后仍可继续接收已入队的数据，直到排空为止，行为与 Go 的 `for v := range ch` 一致。

---

### `chan_try_recv`

```c
chan_err_t chan_try_recv(chan_t *ch, void *out);
```

非阻塞接收。若无数据可取，立即返回 `CHAN_ERR_WOULDBLOCK`。

---

### `chan_recv_timeout`

```c
chan_err_t chan_recv_timeout(chan_t *ch, void *out, int64_t timeout_ns);
```

带超时的接收，`timeout_ns` 语义与 `chan_send_timeout` 相同。

---

## 内省

### `chan_len`

```c
size_t chan_len(const chan_t *ch);
```

返回当前缓冲区中的元素数量（需要加锁，有短暂阻塞）。无缓冲 channel 始终返回 0。

### `chan_cap`

```c
size_t chan_cap(const chan_t *ch);
```

返回缓冲区容量（创建后不变，无需加锁）。无缓冲 channel 返回 0。

---

## Select 多路复用

### `chan_select`

```c
int chan_select(chan_select_case_t *cases, size_t n);
```

阻塞直到 `cases[0..n-1]` 中至少一个 case 就绪，执行该 case 并返回其下标。

- 若多个 case 同时就绪，**均匀随机**选择其中一个（公平性，与 Go select 规范一致）。
- 返回 `-1` 表示参数无效（`cases == NULL` 或 `n == 0`）。
- 执行后，`cases[winner].result` 被设置为该 case 的操作结果（`CHAN_OK` 或 `CHAN_ERR_CLOSED`）。

**锁顺序保证**：内部按 channel 指针地址升序加锁，所有并发 select 调用不会因锁顺序不一致而死锁。

> **已知限制（有缓冲通道）**：park 在有缓冲通道上的 select 等待者，不会被该通道上的无锁快路径 send/recv 及时唤醒——它在环填满/取空到有线程改走慢路径、或通道被 `chan_close` 时才被唤醒。持续流量下这只是批处理延迟；但若生产者发少量数据后**永久沉默且不 close**，park 的 select 消费者可能一直收不到。**规避**：用 `chan_close` 表示发送完毕（close 必定唤醒所有 park 的 select）。直连 `chan_send`/`chan_recv` 不受此限制。详见 [`design.md`](design.md) 的 Select 小节。

---

### `chan_select_try`

```c
int chan_select_try(chan_select_case_t *cases, size_t n);
```

非阻塞版本。若没有任何 case 立即就绪，返回 `-1`，**不修改任何 channel 状态**（等价 Go 的 `select { … default: }`）。

---

### `chan_select_timeout`

```c
int chan_select_timeout(chan_select_case_t *cases, size_t n, int64_t timeout_ns);
```

带超时的 select，`timeout_ns` 语义与 `chan_send_timeout` 相同。超时时返回 `-1`，所有 `cases[i].result` 被设为 `CHAN_ERR_TIMEOUT`。

---

## 诊断

### `chan_strerror`

```c
const char *chan_strerror(chan_err_t err);
```

返回错误码对应的静态字符串，用于日志和调试。返回值指向静态常量，不应被释放或修改。

---

## 线程安全性

| 函数类别 | 线程安全 |
|----------|----------|
| `chan_create` | 是（创建独立对象） |
| `chan_destroy` / `chan_retain` | 是（原子引用计数） |
| `chan_send*` / `chan_recv*` | 是（内部 mutex 保护） |
| `chan_close` | 是（原子 + mutex，幂等） |
| `chan_is_closed` | 是（原子读，弱一致性） |
| `chan_len` / `chan_cap` | 是（`len` 加锁，`cap` 无需锁） |
| `chan_select*` | 是（统一锁序防死锁） |

**`chan_destroy` 的额外契约**：调用者必须在调用前确保本线程所有操作完成，且此后不再使用该指针。这是引用计数语义的固有约束，C 语言无 GC 辅助，需要调用方自律遵守。
