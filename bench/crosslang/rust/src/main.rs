// bench/crosslang/rust/src/main.rs
//
// 跨语言对比基准 — Rust 端（crossbeam-channel）
//
// 方法论：固定消息数（非固定时长），测完成全部收发的墙钟时间。
//   - 每个生产者发送固定 K 条；消费者收到通道断开为止 → 收发条数精确相等。
//   - 时长校准：小消息数定时跑一遍估吞吐，再标定正式测量消息数到 ~1.5s。
//
// 两种变体：
//   direct — 生产者 tx.send / 消费者 for rx.iter()（核心路径）
//   select — 生产者/消费者各跑一次 2-case select!（含永不就绪的 dummy 第二路），
//            对标 C chan_select / Go select。
//
// 输出（CSV）：lang,np,nc,cap,mops   lang ∈ {crossbeam_direct, crossbeam_select}

use crossbeam_channel::{bounded, select, Receiver, Sender};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const TARGET_SEC: f64 = 1.5;
const CALIB_MSGS: usize = 200_000;
const MIN_MSGS: usize = 200_000;
const MAX_MSGS: usize = 80_000_000;

fn make_chan(cap: usize) -> (Sender<i32>, Receiver<i32>) {
    bounded::<i32>(cap) // cap==0 → rendezvous
}

// ── direct 变体 ──────────────────────────────────────────────
fn run_direct(np: usize, nc: usize, cap: usize, total: usize) -> (usize, f64) {
    let k = total / np;
    let total = k * np;
    let (tx, rx) = make_chan(cap);
    let recvd = Arc::new(AtomicI64::new(0));

    let mut chandles = Vec::with_capacity(nc);
    for _ in 0..nc {
        let rx = rx.clone();
        let recvd = Arc::clone(&recvd);
        chandles.push(thread::spawn(move || {
            let mut c: i64 = 0;
            for _ in rx.iter() {
                c += 1;
            }
            recvd.fetch_add(c, Ordering::Relaxed);
        }));
    }
    drop(rx);

    let t0 = Instant::now();
    let mut phandles = Vec::with_capacity(np);
    for _ in 0..np {
        let tx = tx.clone();
        phandles.push(thread::spawn(move || {
            let mut v: i32 = 0;
            for _ in 0..k {
                tx.send(v).unwrap();
                v = v.wrapping_add(1);
            }
        }));
    }
    drop(tx);
    for h in phandles {
        h.join().unwrap();
    }
    // 所有 tx 已 drop（包括线程内的 clone 随 join 结束）→ rx.iter() 结束
    for h in chandles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    if recvd.load(Ordering::Relaxed) != total as i64 {
        eprintln!(
            "  [warn] crossbeam_direct np={} nc={} cap={}: 预期 {} 收到 {}",
            np, nc, cap, total, recvd.load(Ordering::Relaxed)
        );
    }
    (total, dt)
}

// ── select 变体 ──────────────────────────────────────────────
fn run_select(np: usize, nc: usize, cap: usize, total: usize) -> (usize, f64) {
    let k = total / np;
    let total = k * np;
    let (tx, rx) = make_chan(cap);
    let (_dtx, drx) = bounded::<i32>(0); // 永不就绪的第二路（_dtx 不发送也不 drop）
    let recvd = Arc::new(AtomicI64::new(0));

    let mut chandles = Vec::with_capacity(nc);
    for _ in 0..nc {
        let rx = rx.clone();
        let drx = drx.clone();
        let recvd = Arc::clone(&recvd);
        chandles.push(thread::spawn(move || {
            let mut c: i64 = 0;
            loop {
                select! {
                    recv(rx) -> msg => match msg {
                        Ok(_) => c += 1,
                        Err(_) => break,   // 断开 → 结束
                    },
                    recv(drx) -> _ => {},  // 永不就绪
                }
            }
            recvd.fetch_add(c, Ordering::Relaxed);
        }));
    }
    drop(rx);

    let t0 = Instant::now();
    let mut phandles = Vec::with_capacity(np);
    for _ in 0..np {
        let tx = tx.clone();
        let drx = drx.clone();
        phandles.push(thread::spawn(move || {
            let mut v: i32 = 0;
            for _ in 0..k {
                select! {
                    send(tx, v) -> _ => v = v.wrapping_add(1),
                    recv(drx) -> _ => {},  // 永不就绪
                }
            }
        }));
    }
    drop(tx);
    for h in phandles {
        h.join().unwrap();
    }
    for h in chandles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    if recvd.load(Ordering::Relaxed) != total as i64 {
        eprintln!(
            "  [warn] crossbeam_select np={} nc={} cap={}: 预期 {} 收到 {}",
            np, nc, cap, total, recvd.load(Ordering::Relaxed)
        );
    }
    (total, dt)
}

fn measure(
    np: usize,
    nc: usize,
    cap: usize,
    run: fn(usize, usize, usize, usize) -> (usize, f64),
) -> f64 {
    // 1) 校准
    let (_ct, cdt) = run(np, nc, cap, CALIB_MSGS);
    let calib_total = ((CALIB_MSGS / np) * np) as f64;
    let rate = calib_total / cdt;
    // 2) 标定
    let mut msgs = (rate * TARGET_SEC) as usize;
    msgs = msgs.clamp(MIN_MSGS, MAX_MSGS);
    // 3) 正式测量
    let (total, dt) = run(np, nc, cap, msgs);
    total as f64 / dt / 1e6
}

fn main() {
    let scenarios: &[(usize, usize, usize)] = &[
        (1, 1, 0),
        (1, 1, 64),
        (1, 1, 1024),
        (2, 2, 1024),
        (4, 4, 1024),
        (8, 8, 1024),
    ];
    for &(np, nc, cap) in scenarios {
        let mops = measure(np, nc, cap, run_direct);
        println!("crossbeam_direct,{},{},{},{:.3}", np, nc, cap, mops);
    }
    for &(np, nc, cap) in scenarios {
        let mops = measure(np, nc, cap, run_select);
        println!("crossbeam_select,{},{},{},{:.3}", np, nc, cap, mops);
    }
}
