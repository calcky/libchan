# libchan 架构与工作原理

本文用四张图描述 libchan 的工作原理：**分层组件**、**send 快慢路径**、**recv 快慢路径**、
**无缓冲 rendezvous 时序**。图均为 Mermaid，可在 GitHub 及多数 Markdown 查看器直接渲染。

---

## 1. 分层组件图

libchan 把"无竞争时零锁、需要阻塞时才取锁 park"作为核心策略，分三层：
公开 API → 操作实现（每个 op 先试无锁快路径，失败回退加锁慢路径）→ 底层原语。

```mermaid
flowchart TB
    subgraph API["公开 API (include/libchan.h)"]
        A1["chan_send / chan_recv<br/>chan_try_* / chan_*_timeout"]
        A2["chan_select / _try / _timeout"]
        A3["chan_create / destroy / retain<br/>chan_close / len / cap"]
    end

    subgraph OPS["操作实现 (src/)"]
        direction TB
        S["chan_send.c<br/>chan_send_impl"]
        R["chan_recv.c<br/>chan_recv_impl"]
        SEL["select.c<br/>chan_select_impl"]
        CORE["chan_core.c<br/>生命周期 / close / 引用计数"]
    end

    subgraph PRIM["底层原语"]
        direction LR
        RING["ring_lf.c<br/>无锁 MPMC 环<br/>(快路径)"]
        WAITQ["waitq.c<br/>等待者队列<br/>(慢路径)"]
        PARK["park.c<br/>futex / pthread cond<br/>阻塞与唤醒"]
        UTIL["util.c<br/>自旋退避"]
    end

    A1 --> S & R
    A2 --> SEL
    A3 --> CORE

    S -.->|无竞争| RING
    R -.->|无竞争| RING
    SEL -.->|某 case 就绪| RING
    S -->|需阻塞| WAITQ
    R -->|需阻塞| WAITQ
    SEL -->|全不就绪| WAITQ
    WAITQ --> PARK
    RING --> UTIL
    PARK --> UTIL
    CORE --> WAITQ

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class RING fast;
    class WAITQ,PARK slow;
```

> 绿色 = 无锁快路径，橙色 = 加锁慢路径。每个通道含一个无锁环（缓冲）、
> 两个等待队列（send/recv）、两个原子 waiter 计数，外加一把保护慢路径的 mutex。

---

## 2. send 快慢路径流程图

核心机制：当**两个 waiter 计数都为 0**（无人阻塞）时，直接对无锁环做 `ring_lf_push`，
完全绕过 mutex；否则取锁走慢路径，必要时注册等待者并 park。注意快路径推送成功后会用
一道 `seq_cst` fence 重查 `recv_waiter_cnt`，把数据交给在竞态窗口里刚 park 的接收者，
避免丢唤醒。下图忠于 `chan_send_impl`（`src/chan_send.c`）。

```mermaid
flowchart TD
    SA["chan_send(ch, data)"] --> SB{"cap>0 且未关闭<br/>且 send_waiter_cnt==0 且 recv_waiter_cnt==0?"}
    SB -->|是| SC["ring_lf_push 无锁入环"]
    SC -->|成功| SW["seq_cst fence<br/>recv_waiter_cnt≠0 则唤醒 park 的接收者"]
    SW --> SOK["返回 CHAN_OK<br/>✓ 快路径，零锁"]
    SC -->|环满| SL["取 ch->lock"]
    SB -->|否| SL
    SL --> SD{"已关闭?"}
    SD -->|是| SCLOSED["返回 CHAN_ERR_CLOSED"]
    SD -->|否| SE{"有等待的接收者?"}
    SE -->|是| SF["FIFO 交手：环非空给最老的、新数据入环尾<br/>否则 memcpy；chan_park_wake 唤醒"] --> SOK
    SE -->|否| SG{"ring_push 成功?"}
    SG -->|是| SOK
    SG -->|"否，环满"| SH["注册 send_waiter（seq_cst）+ 重查 ring_push"]
    SH --> SH2{"重查入环成功?"}
    SH2 -->|是| SOK
    SH2 -->|否| SP["park 阻塞，被唤醒后返回 CHAN_OK"]

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class SC,SW,SOK fast;
    class SL,SH,SP slow;
```

---

## 3. recv 快慢路径流程图

对称地，无竞争时直接 `ring_lf_pop` 出环；否则取锁，依次尝试"取环中数据"、"与等待发送者
rendezvous"，都不行则注册等待者 park。同样地，快路径弹出后用 `seq_cst` fence 重查
`send_waiter_cnt`，把刚腾出的空位让给 park 的发送者；注册后也会再查一次环才真正 park。
下图忠于 `chan_recv_impl`（`src/chan_recv.c`）。

```mermaid
flowchart TD
    RA["chan_recv(ch, out)"] --> RB{"cap>0 且<br/>send_waiter_cnt==0 且 recv_waiter_cnt==0?"}
    RB -->|是| RC["ring_lf_pop 无锁出环"]
    RC -->|成功| RW["seq_cst fence<br/>send_waiter_cnt≠0 则放行 park 的发送者入环"]
    RW --> ROK["返回 CHAN_OK<br/>✓ 快路径，零锁"]
    RC -->|环空| RL["取 ch->lock"]
    RB -->|否| RL
    RL --> RD{"环里有数据?"}
    RD -->|是| RE["ring_pop 取出<br/>有等待发送者则帮其入环并唤醒"] --> ROK
    RD -->|否| RF{"有等待的发送者?"}
    RF -->|是| RG["与发送者 rendezvous<br/>memcpy + 唤醒"] --> ROK
    RF -->|否| RH{"已关闭?"}
    RH -->|是| RCLOSED["返回 CHAN_ERR_CLOSED"]
    RH -->|否| RI["注册 recv_waiter（seq_cst）+ 重查环"]
    RI --> RI2{"重查 ring_pop 命中?"}
    RI2 -->|是| ROK
    RI2 -->|否| RP["park 阻塞，被唤醒后返回 CHAN_OK"]

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class RC,RW,ROK fast;
    class RL,RI,RP slow;
```

> **为什么用 waiter 计数当门槛**：只要有线程睡在 send/recv 队列里，所有线程就一律走
> 加锁慢路径，保证"接收者帮发送者入环"等操作不会和并发的无锁 push 竞争。无竞争时
> 计数全 0，快路径生效，这是 SPSC/MPMC 高吞吐的来源。
>
> **避免丢唤醒**：无锁快路径与"注册等待者"之间存在竞态窗口——快路径可能在接收者检查
> 完环、但其计数尚未对发送者可见时推入数据，导致接收者抱着环里的数据永久 park。两侧各用
> 一道 `seq_cst` fence 构成 StoreLoad（Dekker）屏障：要么等待者注册后重查时看到数据，
> 要么快路径重查时看到计数并唤醒它，二者不会同时错过。

---

## 4. 无缓冲 rendezvous 时序图

`cap==0` 的通道没有缓冲，发送与接收必须**同步握手**：先到者注册等待者并 park 睡眠，
后到者直接把数据交给它并 `chan_park_wake` 唤醒。下图是"接收者先到、发送者后到"的情形。

```mermaid
sequenceDiagram
    participant C as 消费者线程
    participant Ch as channel (cap=0)
    participant P as 生产者线程

    Note over C: chan_recv(ch, out)
    C->>Ch: 取 ch->lock
    C->>Ch: 无发送者等待、未关闭
    C->>Ch: 注册 recv_waiter，recv_waiter_cnt=1
    C->>Ch: 释放锁
    C-->>C: chan_park_wait 睡眠 💤

    Note over P: chan_send(ch, data) — 稍后到达
    P->>Ch: 快路径跳过 (recv_waiter_cnt≠0)，取 ch->lock
    P->>Ch: waitq_pop_receiver 弹出消费者
    P->>C: memcpy data 到消费者的 out 缓冲
    P->>C: 设 result=CHAN_OK
    P->>Ch: 释放锁
    P->>C: chan_park_wake 唤醒 🔔
    P-->>P: 返回 CHAN_OK
    C-->>C: 醒来，返回 CHAN_OK（out 已就绪）
```

> 对称地，若发送者先到，则它注册 send_waiter 并 park，由后到的接收者完成 memcpy 与唤醒。
> 唤醒底层是 Linux futex（`park.c`，无 futex 的平台回退到 pthread cond）。
> 这条 park/wake 往返（~1–2 µs 内核延迟）正是无缓冲场景吞吐的主导成本。

---

## 延伸阅读

- 并发设计与内存序细节：[`design.md`](design.md)
- 无锁环协议（reserve→write→commit）：源码 [`src/ring_lf.h`](../src/ring_lf.h) 顶部注释
- 跨语言性能对比：[`comparison.md`](comparison.md)
- API 参考：[`api_reference.md`](api_reference.md)
