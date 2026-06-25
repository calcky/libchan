# libchan Architecture and Internals

This document explains how libchan works through four diagrams: **layered components**, **send fast/slow path**, **recv fast/slow path**, and the **unbuffered rendezvous sequence**. All diagrams are Mermaid and render directly on GitHub and in most Markdown viewers.

---

## 1. Layered Component Diagram

libchan adopts "zero locks when uncontended, take the lock and park only when blocking is required" as its core strategy, organized into three layers:
public API -> operation implementations (each op first tries the lock-free fast path, then falls back to the locked slow path on failure) -> low-level primitives.

```mermaid
flowchart TB
    subgraph API["Public API (include/libchan.h)"]
        A1["chan_send / chan_recv<br/>chan_try_* / chan_*_timeout"]
        A2["chan_select / _try / _timeout"]
        A3["chan_create / destroy / retain<br/>chan_close / len / cap"]
    end

    subgraph OPS["Operation Implementations (src/)"]
        direction TB
        S["chan_send.c<br/>chan_send_impl"]
        R["chan_recv.c<br/>chan_recv_impl"]
        SEL["select.c<br/>chan_select_impl"]
        CORE["chan_core.c<br/>lifecycle / close / reference counting"]
    end

    subgraph PRIM["Low-level Primitives"]
        direction LR
        RING["ring_lf.c<br/>lock-free MPMC ring<br/>(fast path)"]
        WAITQ["waitq.c<br/>waiter queue<br/>(slow path)"]
        PARK["park.c<br/>futex / pthread cond<br/>blocking and wake"]
        UTIL["util.c<br/>spin backoff"]
    end

    A1 --> S & R
    A2 --> SEL
    A3 --> CORE

    S -.->|uncontended| RING
    R -.->|uncontended| RING
    SEL -.->|a case is ready| RING
    S -->|must block| WAITQ
    R -->|must block| WAITQ
    SEL -->|none ready| WAITQ
    WAITQ --> PARK
    RING --> UTIL
    PARK --> UTIL
    CORE --> WAITQ

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class RING fast;
    class WAITQ,PARK slow;
```

> Green = lock-free fast path, orange = locked slow path. Each channel holds one lock-free ring (the buffer),
> two wait queues (send/recv), two atomic waiter counts, plus one mutex protecting the slow path.

---

## 2. Send Fast/Slow Path Flowchart

Core mechanism: when **both waiter counts are 0** (nobody is blocked), `ring_lf_push` is performed directly on the lock-free ring,
bypassing the mutex entirely; otherwise the lock is taken and the slow path runs, registering a waiter and parking if necessary. Note that after a successful fast-path push, a
`seq_cst` fence re-checks `recv_waiter_cnt` to hand the data to a receiver that just parked in the race window,
avoiding a lost wakeup. The diagram below faithfully follows `chan_send_impl` (`src/chan_send.c`).

```mermaid
flowchart TD
    SA["chan_send(ch, data)"] --> SB{"cap>0 and not closed<br/>and send_waiter_cnt==0 and recv_waiter_cnt==0?"}
    SB -->|yes| SC["ring_lf_push lock-free enqueue"]
    SC -->|success| SW["seq_cst fence<br/>if recv_waiter_cnt!=0, wake a parked receiver"]
    SW --> SOK["return CHAN_OK<br/>✓ fast path, zero locks"]
    SC -->|ring full| SL["take ch->lock"]
    SB -->|no| SL
    SL --> SD{"closed?"}
    SD -->|yes| SCLOSED["return CHAN_ERR_CLOSED"]
    SD -->|no| SE{"any waiting receiver?"}
    SE -->|yes| SF["FIFO handoff: if ring non-empty give oldest, push new data to ring tail<br/>otherwise memcpy; chan_park_wake to wake"] --> SOK
    SE -->|no| SG{"ring_push success?"}
    SG -->|yes| SOK
    SG -->|"no, ring full"| SH["register send_waiter (seq_cst) + re-check ring_push"]
    SH --> SH2{"re-check enqueue success?"}
    SH2 -->|yes| SOK
    SH2 -->|no| SP["park and block; on wake return CHAN_OK"]

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class SC,SW,SOK fast;
    class SL,SH,SP slow;
```

---

## 3. Recv Fast/Slow Path Flowchart

Symmetrically, when uncontended `ring_lf_pop` dequeues directly from the ring; otherwise the lock is taken, and it tries in order to "take data from the ring", "rendezvous with a waiting sender",
and if neither works, registers a waiter and parks. Likewise, after a fast-path pop a `seq_cst` fence re-checks
`send_waiter_cnt` to give the just-freed slot to a parked sender; after registering it also re-checks the ring once before actually parking.
The diagram below faithfully follows `chan_recv_impl` (`src/chan_recv.c`).

```mermaid
flowchart TD
    RA["chan_recv(ch, out)"] --> RB{"cap>0 and<br/>send_waiter_cnt==0 and recv_waiter_cnt==0?"}
    RB -->|yes| RC["ring_lf_pop lock-free dequeue"]
    RC -->|success| RW["seq_cst fence<br/>if send_waiter_cnt!=0, let a parked sender enqueue"]
    RW --> ROK["return CHAN_OK<br/>✓ fast path, zero locks"]
    RC -->|ring empty| RL["take ch->lock"]
    RB -->|no| RL
    RL --> RD{"data in ring?"}
    RD -->|yes| RE["ring_pop to take it<br/>if a sender is waiting, help it enqueue and wake it"] --> ROK
    RD -->|no| RF{"any waiting sender?"}
    RF -->|yes| RG["rendezvous with sender<br/>memcpy + wake"] --> ROK
    RF -->|no| RH{"closed?"}
    RH -->|yes| RCLOSED["return CHAN_ERR_CLOSED"]
    RH -->|no| RI["register recv_waiter (seq_cst) + re-check ring"]
    RI --> RI2{"re-check ring_pop hit?"}
    RI2 -->|yes| ROK
    RI2 -->|no| RP["park and block; on wake return CHAN_OK"]

    classDef fast fill:#d6f5d6,stroke:#2e7d32;
    classDef slow fill:#ffe0b2,stroke:#e65100;
    class RC,RW,ROK fast;
    class RL,RI,RP slow;
```

> **Why use waiter counts as the gate**: as long as a thread is sleeping in the send/recv queue, every thread takes the
> locked slow path, ensuring that operations like "receiver helps a sender enqueue" never race with a concurrent lock-free push. When uncontended the
> counts are all 0, the fast path takes effect, and this is the source of high SPSC/MPMC throughput.
>
> **Avoiding lost wakeups**: a race window exists between the lock-free fast path and "registering a waiter" -- the fast path may push data after a receiver has
> finished checking the ring but before its count is visible to the sender, leaving the receiver parked forever while data sits in the ring. Each side uses
> a `seq_cst` fence to form a StoreLoad (Dekker) barrier: either the waiter sees the data when it re-checks after registering,
> or the fast path sees the count when it re-checks and wakes it -- the two cannot both miss.

---

## 4. Unbuffered Rendezvous Sequence Diagram

A `cap==0` channel has no buffer, so send and receive must perform a **synchronous handshake**: whoever arrives first registers a waiter and parks to sleep,
and the later arrival hands the data directly to it and wakes it via `chan_park_wake`. The diagram below shows the "receiver arrives first, sender arrives later" case.

```mermaid
sequenceDiagram
    participant C as Consumer thread
    participant Ch as channel (cap=0)
    participant P as Producer thread

    Note over C: chan_recv(ch, out)
    C->>Ch: take ch->lock
    C->>Ch: no sender waiting, not closed
    C->>Ch: register recv_waiter, recv_waiter_cnt=1
    C->>Ch: release lock
    C-->>C: chan_park_wait sleeps 💤

    Note over P: chan_send(ch, data) — arrives later
    P->>Ch: fast path skipped (recv_waiter_cnt!=0), take ch->lock
    P->>Ch: waitq_pop_receiver pops the consumer
    P->>C: memcpy data into the consumer's out buffer
    P->>C: set result=CHAN_OK
    P->>Ch: release lock
    P->>C: chan_park_wake wakes it 🔔
    P-->>P: return CHAN_OK
    C-->>C: wakes up, returns CHAN_OK (out is ready)
```

> Symmetrically, if the sender arrives first it registers a send_waiter and parks, and the later-arriving receiver completes the memcpy and the wake.
> The wake is backed by the Linux futex (`park.c`; platforms without futex fall back to pthread cond).
> This park/wake round trip (~1–2 µs of kernel latency) is the dominant cost of throughput in the unbuffered case.

---

## Further Reading

- Concurrency design and memory-ordering details: [`design.md`](design.md)
- Lock-free ring protocol (reserve->write->commit): top-of-file comments in [`src/ring_lf.h`](../src/ring_lf.h)
- Cross-language performance comparison: [`comparison.md`](comparison.md)
- API reference: [`api_reference.md`](api_reference.md)
