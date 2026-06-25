// bench/crosslang/rust/src/main.rs
//
// Cross-language comparison benchmark — Rust side (crossbeam-channel)
//
// Methodology: fixed message count (not fixed duration); measure the wall-clock
// time to complete all sends and receives.
//   - Each producer sends a fixed K messages; each consumer keeps receiving until
//     the channel disconnects -> sent and received counts are exactly equal.
//   - Duration calibration: run once for a fixed time with a small message count to
//     estimate throughput, then calibrate the real measurement's message count to ~1.5s.
//
// Two variants:
//   direct — producer tx.send / consumer for rx.iter() (core path)
//   select — producer/consumer each run one 2-case select! (with a dummy second case
//            that is never ready), matching C chan_select / Go select.
//
// Output (CSV): lang,np,nc,cap,mops   lang in {crossbeam_direct, crossbeam_select}

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

// ── direct variant ──────────────────────────────────────────
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
    // All tx have been dropped (including the in-thread clones once join finishes) -> rx.iter() ends
    for h in chandles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    if recvd.load(Ordering::Relaxed) != total as i64 {
        eprintln!(
            "  [warn] crossbeam_direct np={} nc={} cap={}: expected {} received {}",
            np, nc, cap, total, recvd.load(Ordering::Relaxed)
        );
    }
    (total, dt)
}

// ── select variant ──────────────────────────────────────────
fn run_select(np: usize, nc: usize, cap: usize, total: usize) -> (usize, f64) {
    let k = total / np;
    let total = k * np;
    let (tx, rx) = make_chan(cap);
    let (_dtx, drx) = bounded::<i32>(0); // never-ready second case (_dtx is never sent on, never dropped)
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
                        Err(_) => break,   // disconnected -> done
                    },
                    recv(drx) -> _ => {},  // never ready
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
                    recv(drx) -> _ => {},  // never ready
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
            "  [warn] crossbeam_select np={} nc={} cap={}: expected {} received {}",
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
    // 1) Calibration
    let (_ct, cdt) = run(np, nc, cap, CALIB_MSGS);
    let calib_total = ((CALIB_MSGS / np) * np) as f64;
    let rate = calib_total / cdt;
    // 2) Calibrate
    let mut msgs = (rate * TARGET_SEC) as usize;
    msgs = msgs.clamp(MIN_MSGS, MAX_MSGS);
    // 3) Real measurement
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
