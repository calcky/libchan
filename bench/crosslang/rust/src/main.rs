// bench/crosslang/rust/src/main.rs
//
// 跨语言对比基准 — Rust 端（crossbeam-channel）
//
// 6 个固定场景（与 C/Go 端完全一致）：
//   S1: cap=0,    1P+1C  (rendezvous / bounded(0))
//   S2: cap=64,   1P+1C
//   S3: cap=1024, 1P+1C
//   S4: cap=1024, 2P+2C
//   S5: cap=1024, 4P+4C
//   S6: cap=1024, 8P+8C
//
// 停止机制：
//   用一个额外的 stop channel（bounded(0)）作信号；
//   drop(stop_tx) 关闭该 channel，使所有 select! 中的 recv(stop_rx)
//   立即返回 Err(Disconnected)，goroutine（线程）退出。
//   等价于 C 端 chan_close + Go 端 close(done)。
//
// 输出格式（CSV）：
//   lang,np,nc,cap,mops
//
// 编译：cargo build --release

use crossbeam_channel::{bounded, select};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const WARMUP_MS: u64 = 400;
const MEASURE_MS: u64 = 1500;

fn bench_case(np: usize, nc: usize, cap: usize) -> f64 {
    // 数据 channel
    let (data_tx, data_rx) = bounded::<i32>(cap);
    // 停止信号 channel：drop(stop_tx) 即广播关闭
    let (stop_tx, stop_rx) = bounded::<()>(0);

    let ops = Arc::new(AtomicI64::new(0));
    let mut handles = Vec::with_capacity(np + nc);

    // producers
    for _ in 0..np {
        let data_tx = data_tx.clone();
        let stop_rx = stop_rx.clone();
        handles.push(thread::spawn(move || {
            let mut v: i32 = 0;
            loop {
                select! {
                    send(data_tx, v) -> res => {
                        if res.is_err() { break; }
                        v = v.wrapping_add(1);
                    }
                    recv(stop_rx) -> _ => break,
                }
            }
        }));
    }
    drop(data_tx); // 仅保留线程内的 clone

    // consumers
    for _ in 0..nc {
        let data_rx = data_rx.clone();
        let stop_rx = stop_rx.clone();
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            loop {
                select! {
                    recv(data_rx) -> res => {
                        if res.is_err() { break; }
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                    recv(stop_rx) -> _ => break,
                }
            }
        }));
    }
    drop(data_rx);
    drop(stop_rx); // 仅保留线程内的 clone

    // warmup
    thread::sleep(Duration::from_millis(WARMUP_MS));
    ops.store(0, Ordering::Relaxed);

    // measure
    let _t0 = Instant::now();
    thread::sleep(Duration::from_millis(MEASURE_MS));

    // 停止：drop(stop_tx) 关闭 stop channel，所有线程的 recv(stop_rx) 立即就绪
    drop(stop_tx);

    for h in handles {
        h.join().unwrap();
    }

    let n = ops.load(Ordering::Relaxed);
    n as f64 / (MEASURE_MS as f64 / 1000.0) / 1e6
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
        let mops = bench_case(np, nc, cap);
        println!("crossbeam,{},{},{},{:.3}", np, nc, cap, mops);
    }
}
