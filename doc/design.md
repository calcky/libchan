# Internal Design

本文档面向希望了解 libchan 内部实现、进行性能调优或贡献代码的读者。

---

## 整体架构

libchan 采用**单把 mutex + FIFO 等待队列**的经典模型，与 Go runtime 的 channel 实现策略一致（Go 也不是 lock-free）。

**为何不用 lock-free MPMC 队列？**

lock-free 方案（如 Vyukov bounded queue）在无竞争时延迟极低，但其正确性建立在不涉及外部同步的前提上。libchan 需要：

1. **close 语义**：关闭时原子地唤醒所有等待者，这要求在 "close 标志写入" 与 "等待者出队" 之间建立严格的顺序——lock-free 队列无法直接表达这种跨操作原子性。
2. **阻塞语义**：无论如何都需要一套 park/unpark 机制；加上这一层后，lock-free 带来的延迟优势大幅缩小。
3. **ABA 与内存回收**：C 没有 GC，lock-free 队列需要 hazard pointer 或 epoch-based reclamation，工程复杂度远超收益。

在低/中等竞争下，自旋退避（见 [Park 抽象](#park-抽象)）使得"加锁临界区极短"这一特点足以补偿单一全局锁的理论劣势。

---

## 核心数据结构

### `struct chan`（`src/libchan_internal.h`）

```
struct chan {
    /* ---- 不可变字段（创建后只读，无需同步） ---- */
    size_t        elem_size;
    size_t        capacity;       /* 0 = 无缓冲 */

    /* ---- 以下字段由 lock 保护 ---- */
    chan_mutex_t  lock;           /* CHAN_ALIGNED：64 字节缓存行对齐 */

    void         *buf;            /* 环形缓冲区，capacity > 0 时分配 */
    size_t        head, tail;     /* 环形缓冲区读/写指针 */
    size_t        count;          /* 当前元素数，区分满/空二义性 */

    chan_waitq_t  send_waiters;   /* CHAN_ALIGNED */
    chan_waitq_t  recv_waiters;   /* CHAN_ALIGNED */

    /* ---- 原子字段（无需持锁即可读） ---- */
    _Atomic bool  closed;
    _Atomic int   refcount;
};
```

`CHAN_ALIGNED`（64 字节对齐）将 `lock`、`send_waiters`、`recv_waiters` 分别放在独立的缓存行，避免在高并发下三者之间的伪共享（false sharing）。

### `chan_waiter_t`（`src/libchan_internal.h`）

等待节点**栈分配**于调用 `chan_send` / `chan_recv` 的线程栈上，无堆分配开销。

```
struct chan_waiter {
    struct chan_waiter *next;       /* 侵入式链表指针 */
    void              *data;        /* SEND: 指向调用方数据；RECV: 指向输出缓冲区 */
    chan_op_t          op;
    chan_err_t         result;      /* 唤醒方填写 */
    size_t             case_idx;    /* 被哪个 select case 赢得（非 select 时为 -1） */
    chan_park_t       *wake_park;   /* 唤醒目标：普通等待者指向 &self.park；select 存根指向 shared_park */
    _Atomic int       *select_state; /* NULL（普通）；select 存根指向共享原子状态 */
    chan_park_t        park;        /* 停泊存储（只有非 select 等待者使用） */
};
```

`wake_park` 和 `select_state` 这两个指针字段是 select 实现的关键（详见 [Select 多路复用](#select-多路复用)）。

### `chan_waitq_t`

FIFO 单向链表，头尾各一个指针：

```c
typedef struct { chan_waiter_t *head, *tail; } chan_waitq_t;
```

所有操作（`waitq_push`、`waitq_pop`、`waitq_remove`）均定义为内联函数，在持锁状态下调用，时间复杂度 O(1)（`waitq_remove` 为 O(n)，但等待队列通常极短）。

---

## 发送路径（`src/chan_send.c`）

### 无缓冲 channel（capacity == 0）

```
send(data):
  lock
  if closed → return CLOSED
  if recv_waiters 非空:
    r = pop(recv_waiters)
    memcpy(r->data, data, elem_size)   ← 数据直接写入接收方缓冲区
    r->result = OK
    unlock
    chan_park_wake(r->wake_park)        ← 唤醒接收方
    return OK
  if try_send → return WOULDBLOCK
  push self to send_waiters
  unlock
  park_wait(...)                        ← 阻塞
  return self.result
```

**关键设计**：数据在持锁时从发送方栈复制到接收方输出缓冲区，不经过任何中间存储，零额外拷贝。

### 有缓冲 channel（capacity > 0）

```
send(data):
  lock
  if closed → return CLOSED
  if recv_waiters 非空 AND ring 为空:
    直接交付（同无缓冲逻辑，跳过 ring buffer）
  if ring 未满:
    ring_push(data)
    if recv_waiters 非空 → pop & wake 一个接收方
    unlock; return OK
  /* ring 已满 */
  if try_send → return WOULDBLOCK
  push self to send_waiters (self.data = &data)
  unlock; park_wait(...)               ← 阻塞
  /* 醒来后数据已由接收方推入 ring，见下文 */
  return self.result
```

**"接收方帮助推送"设计**：当接收方从 ring 中取出数据后，若发现有阻塞的发送方，它**持锁**直接将发送方的数据推入 ring，然后唤醒发送方通知"已完成"。发送方醒来后**无需重新加锁推送**，避免了"发送方在醒来后缓冲区可能再次被填满"的反复。

**避免丢唤醒（lost wakeup）**：无锁快路径与"注册等待者"之间存在竞态窗口——快路径可能在接收方检查完 ring（为空）、但其 `recv_waiter_cnt` 尚未对发送方可见时把数据推入 ring，导致发送方跳过唤醒、接收方抱着 ring 里的数据永久 park（紧耦合的两线程会直接死锁）。修复：
- 等待方注册时用 `seq_cst` 自增计数，随后**在 park 前再查一次 ring**；
- 快路径成功 push/pop 后插入一道 `seq_cst` fence，再查对端 `*_waiter_cnt`，若非零则取锁把数据交付给已 park 的等待方并唤醒。

两侧的 `seq_cst` fence 构成 StoreLoad（Dekker）屏障：**要么等待方注册后重查时看到数据，要么快路径重查时看到计数并唤醒它，二者不会同时错过**。代价是快路径每次成功操作多一道 fence（直连 send/recv 约 +7 ns/op；`chan_select` 路径不经过此处，不受影响）。所有交付点都会检查 `ring_pop` 的返回值——`ring_pop` 在 `!ring_empty` 之后仍可能因竞态返回 false，若忽略返回值直接以 `CHAN_OK` 唤醒接收方，会交付一条不存在的"幻象"消息。

详见 [`architecture.md`](architecture.md) 的 send/recv 快慢路径图。

---

## 接收路径（`src/chan_recv.c`）

### 无缓冲

对称于发送路径：先检查 `send_waiters`，有则直接取数据并唤醒发送方；否则 park 等待。

### 有缓冲

```
recv(out):
  lock
  if ring 非空:
    ring_pop(out)
    if send_waiters 非空:
      s = pop(send_waiters)
      ring_push(s->data)               ← 帮助发送方推入数据（持锁）
      s->result = OK
      unlock
      wake(s->wake_park)
    else:
      unlock
    return OK
  if send_waiters 非空:
    直接从发送方取数据（同无缓冲逻辑）
  if closed → return CLOSED
  push self to recv_waiters; unlock; park
  return self.result
```

**close 后排空语义**：`ring 非空` 的检查在 `closed` 检查之前，保证即使 channel 已关闭，ring buffer 中已有的数据也能被完整取走，与 Go `for v := range ch` 语义一致。

---

## Close 语义

`chan_close`（`src/chan_core.c`）：

```
close():
  lock
  if already closed → return CHAN_ERR_CLOSED
  atomic_store(closed, true, release)
  waitq_close_all(send_waiters)   ← 所有阻塞发送方：result = CLOSED, wake
  waitq_close_all(recv_waiters)   ← 所有阻塞接收方：result = CLOSED, wake
  unlock
```

`waitq_close_all` 对等待队列中每个 waiter 尝试 CAS（对于 select 存根），成功则设置 result 并唤醒。

**为何对有缓冲 channel 的阻塞接收方也直接唤醒返回 CLOSED？**  
阻塞中的接收方意味着此刻 ring buffer 已空（否则它们不会阻塞），因此直接唤醒并通知 CLOSED 是正确的。

---

## 引用计数

```
chan_retain:  atomic_fetch_add(refcount, 1, relaxed)

chan_destroy: if atomic_fetch_sub(refcount, 1, acq_rel) == 1:
                lock(); unlock()   ← 排空屏障：确保没有线程仍在临界区
                free(buf); free(ch)
```

`fetch_sub` 的 `acq_rel` 语义：
- **release** 端：最后一个 `destroy` 调用者确保此前所有写操作对"做释放决定的那个线程"可见。
- **acquire** 端：做释放决定的那个线程能看到所有其他线程在各自 `fetch_sub` 之前的写入，即所有已完成操作的结果。

这是 `std::shared_ptr` / `Arc` 的标准引用计数内存序模式。

---

## Park 抽象（`src/park.c`）

`chan_park_t` 是线程睡眠/唤醒的底层原语，有两种实现：

### Linux futex 实现（默认）

```c
typedef struct { _Atomic uint32_t word; } chan_park_t;

park_wait():
  /* 自旋阶段：LIBCHAN_SPIN_LIMIT 次（默认 40） */
  for i in 0..SPIN_LIMIT:
    if word != 0 → return true   // 已被唤醒
    if i < 8: PAUSE/YIELD        // x86: pause, ARM: yield
    else: sched_yield()
  /* 进入内核 */
  syscall(FUTEX_WAIT, &word, 0, timeout)
  return word != 0

park_wake():
  atomic_store(word, 1, release)
  syscall(FUTEX_WAKE, &word, 1)
```

优势：`chan_park_t` 只占 4 字节（嵌入 waiter 节点），无额外堆分配；`FUTEX_WAKE` 内核路径比 `pthread_cond_signal` 更短。

### POSIX pthread fallback（非 Linux 或强制启用时）

```c
typedef struct { pthread_mutex_t mu; pthread_cond_t cv; bool signaled; } chan_park_t;
```

标准 mutex + condvar 模式，所有 POSIX 平台兼容。

### 自旋退避策略

默认自旋 40 次：前 8 次用 CPU pause/yield 指令（x86: `PAUSE`，ARM: `YIELD`），延迟极低（约 10–40ns/次），对 L1 cache 几乎没有影响；后续用 `sched_yield()` 让出调度，避免空转浪费 CPU。

只有在自旋后仍未被唤醒，才陷入内核（futex/cond_wait），典型上下文切换约 1–5µs。

设置 `LIBCHAN_SPIN_LIMIT=0` 可完全禁用自旋（适用于 CPU 核心紧张的嵌入式场景）。

---

## Select 多路复用（`src/select.c`）

### 1. 锁顺序排序（防死锁）

```c
sort(cases, by=ch_pointer_value)   // 按 channel 地址升序
lock_all(sorted_channels)          // 所有线程用相同顺序加锁 → 无死锁
```

所有并发的 `chan_select` 调用若涉及相同的 channel 集合，都会以相同的顺序加锁，因此不会形成死锁环。

### 2. 快路径（第一轮扫描）

持有所有锁时检查每个 case 是否立即可行。若有多个就绪，**均匀随机**选择一个（用 `rand() % nready`），避免某些 case 系统性地被饿死，与 Go select 规范的公平性要求一致。

执行选中 case，解锁所有 channel，返回 case 索引。

### 3. 等待路径（第二轮，无就绪 case）

核心设计：**共享状态 + 存根节点（stub waiter）**

```c
_Atomic int  shared_state;   // 初始 WAITER_WAITING
chan_park_t   shared_park;    // 所有存根唤醒时发信号到这里

chan_waiter_t stubs[n];       // 栈分配，每个 case 一个
for each case i:
    stubs[i].select_state = &shared_state
    stubs[i].wake_park    = &shared_park
    register stubs[i] on cases[i].ch's queue
unlock_all
park_wait(&shared_park, timeout)   // 只有一个睡眠点
```

### 4. CAS 抢占协议

当某个 channel 准备唤醒一个等待者时（send/recv 路径中的 `waitq_pop_sender/receiver`），会调用 `waiter_try_claim`：

```c
bool waiter_try_claim(chan_waiter_t *w):
    if w->select_state == NULL: return true   // 普通等待者，直接领取
    CAS(w->select_state, WAITING → WOKEN)     // select 存根：原子竞争
    return success
```

- **CAS 成功（第一个赢得者）**：正常交付数据，调用 `chan_park_wake(w->wake_park)` 唤醒共享 park。
- **CAS 失败（已被其他 channel 抢先）**：存根被无害丢弃（已从队列弹出，select 调用方在清理阶段会发现它不在队列中），不进行数据交付。

这保证了即使多个 channel 几乎同时就绪，select 调用方只被唤醒一次，且只有一个 channel 的操作真正完成。

### 5. 胜者识别与清理

```c
lock_all
for each stub i:
    if stubs[i].result != CHAN_ERR_WOULDBLOCK:
        winner = i                    // 唤醒方设置了 result（仅胜者会设置）
    else:
        waitq_remove(cases[i].ch, &stubs[i])   // 仍在队列中，移除；已被丢弃的无影响
unlock_all
```

胜者 stub 的 `result` 已被唤醒方设为 `CHAN_OK` 或 `CHAN_ERR_CLOSED`；其他 stub 的 `result` 保持初始值 `CHAN_ERR_WOULDBLOCK`，这是识别胜者的可靠标志。

### 6. 已知限制：有缓冲通道上的延迟唤醒

为避免 MPMC 串行化，select 存根**不增加** `send_waiter_cnt`/`recv_waiter_cnt`（增加会让所有线程在任何 select park 时立即放弃无锁快路径，实测把吞吐打到 <1 Mops/s）。代价是：**一个 park 在有缓冲通道上的 select 等待者，不会被该通道上的无锁快路径操作及时唤醒**。它只会在以下时机被唤醒：

- 环被填满/取空到某个线程改走慢路径（慢路径会检查等待队列并交付）——延迟有界于环容量；或
- 通道被 `chan_close`（`waitq_close_all` 唤醒全部存根）。

**正常的持续流量下这只是"批处理延迟"**（生产者无锁灌满环 → 一次唤醒 → 消费者批量取走，这正是 select 高吞吐的来源）。

**但有一个病态场景会永久卡住**：生产者用 select/send 向有缓冲通道发**少量**数据（不足以填满环），随后**永久沉默且不 `chan_close`**——此时 park 在该通道上的 select 消费者会一直收不到这些数据。

> **规避**：用 `chan_close` 表示"发送完毕"（Go 的通用惯例)。close 一定会唤醒所有 park 的 select，彻底规避此问题。实践中几乎所有 select 用法都以 close 收尾，因此该限制极少触及。
>
> 直连 `chan_send`/`chan_recv`（非 select）**不受此限制**——它们的等待者增加 waiter 计数 + `seq_cst` 握手，会被及时唤醒（见上文发送路径的"避免丢唤醒"小节）。彻底修复 select 的延迟唤醒需要在不破坏批处理快路径的前提下唤醒存根，是一项独立的设计工作。

---

## 内存序速查

| 操作 | 内存序 | 理由 |
|------|--------|------|
| `closed` 写（`chan_close`） | `memory_order_release` | 确保关闭前的写入对观察到 `closed=true` 的线程可见 |
| `closed` 读（`chan_is_closed` 及 send/recv 快路径） | `memory_order_acquire` | 与 release 端配对 |
| `select_state` CAS | `acq_rel`（成功）/ `relaxed`（失败） | 成功时需要 acquire+release 两向同步；失败时只需知道当前值 |
| `refcount` fetch_sub | `memory_order_acq_rel` | 使最后一个 destroy 能看到所有前驱线程的写入 |
| `refcount` fetch_add | `memory_order_relaxed` | 计数本身是原子的，不需要额外 ordering |
| futex word | `memory_order_release`（写）/ `memory_order_acquire`（读） | futex 系统调用本身提供内核级屏障 |
| lock 保护区内的字段 | 普通读写 | pthread_mutex 的 acquire/release 语义已提供全量内存序保证 |

---

## 性能数据（参考值）

测试环境：WSL2（Linux 6.6，x86-64），`RelWithDebInfo`，`LIBCHAN_SPIN_LIMIT=40`，100 万条消息：

| 模式 | 线程对数 | 容量 | 吞吐量 |
|------|----------|------|--------|
| 无缓冲 | 1 | 0 | ~3.5 Mops/s |
| 有缓冲 | 1 | 1024 | ~5.0 Mops/s |
| 有缓冲 | 2 | 64 | ~4.9 Mops/s |
| 有缓冲 | 2 | 1024 | ~9.7 Mops/s |
| 有缓冲 | 4 | 1024 | ~6.3 Mops/s |

可用 `make bench` 在本机重跑（输出 Mops/sec 表格）。

---

## 源文件导航

| 文件 | 职责 |
|------|------|
| `include/libchan.h` | 公开 ABI：所有类型、函数声明 |
| `src/libchan_internal.h` | 内部数据结构、内联函数、park 声明 |
| `src/chan_core.c` | `chan_create` / `chan_destroy` / `chan_retain` / `chan_close` |
| `src/chan_send.c` | `chan_send_impl`（三种变体的统一入口） |
| `src/chan_recv.c` | `chan_recv_impl` |
| `src/select.c` | `chan_select_impl`（含锁序排序、CAS 抢占） |
| `src/park.c` | futex 与 pthread condvar 两种 park 实现 |
| `src/util.c` | `chan_spin_hint`（CPU pause/yield 内联汇编） |
| `src/ring_buffer.c` | 编译单元占位（逻辑内联于 `libchan_internal.h`） |
| `src/waitq.c` | 编译单元占位（逻辑内联于 `libchan_internal.h`） |
