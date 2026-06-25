window.BENCHMARK_DATA = {
  "lastUpdate": 1782369333625,
  "repoUrl": "https://github.com/calcky/libchan",
  "entries": {
    "libchan throughput (Mops/s)": [
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": true,
          "id": "790dc4fd593ee60b639482e9bb6ed183f3d852bf",
          "message": "ci: add GitHub Actions workflow (tests + sanitizers + benchmarks)\n\nAdd .github/workflows/ci.yml running on every push and PR:\n\n- unit-tests: cmake + ctest (RelWithDebInfo). Uses --no-tests=error so a\n  zero-test run fails instead of passing vacuously; emits JUnit XML.\n- sanitizers: matrix of ASan+UBSan and TSan (Debug, -DLIBCHAN_SANITIZE),\n  with `sysctl vm.mmap_rnd_bits=28` to avoid the high-ASLR \"unexpected\n  memory mapping\" sanitizer abort on GitHub runners.\n- benchmark: builds bench_showcase and runs it; runs the Go/Rust\n  cross-language comparison (informational, non-blocking); tracks\n  libchan throughput over time via benchmark-action/github-action-benchmark\n  (customBiggerIsBetter, gh-pages history, lenient 200% alert, no CI fail\n  given shared-runner noise; baseline pushed only on master).\n\nReports render on each run's Summary page via $GITHUB_STEP_SUMMARY\n(ctest table + sanitizer status + showcase table + cross-language table),\nwith JUnit/showcase/comparison uploaded as artifacts. Two small helper\nscripts under .github/scripts/ do the JUnit->Markdown and showcase->JSON\nconversion (verified locally). Also gitignore .claude/ and CI scratch.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T10:16:20+08:00",
          "tree_id": "e7430d90731117b1b3953ba0a287f67a526127a5",
          "url": "https://github.com/calcky/libchan/commit/790dc4fd593ee60b639482e9bb6ed183f3d852bf"
        },
        "date": 1782354465863,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv（无等待）",
            "value": 49.36,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC 跨核稳态（缓存一致性墙）",
            "value": 5.56,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC 跨核稳态（游标缓存破墙）",
            "value": 25.25,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC 阻塞 cap=1024",
            "value": 35.27,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan 无缓冲 rendezvous",
            "value": 1.42,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1.26,
            "unit": "Mops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": true,
          "id": "f45eb7052c14aafaeb39a416f69a2a07988d41a7",
          "message": "fix: acquire Phase-3 wait-load in MPMC ring (fix data race on weak memory)\n\nThe DPDK-style ring committed prod.tail/cons.tail in Phase 3 with a relaxed\nspin-load. A consumer's acquire-load of prod.tail then synchronises-with only\nthe single producer that wrote the exact value it read; when it observes the\ntail advanced past its slot by a *later* producer, the C11 release sequence is\nbroken (a plain cross-thread store ends it), so there is no happens-before\nedge to the producer that actually wrote that slot — a genuine data race on\nthe slot memcpy. Benign on x86 (TSO), real on weak memory (ARM); ThreadSanitizer\nin CI flagged it on test_mpmc/test_stress (functionally still lossless).\n\nFix: make the Phase-3 wait-loads acquire so each producer synchronises-with its\npredecessor, chaining happens-before along the tail cursor. A consumer ordered\nafter the latest tail value is then ordered after every prior producer's slot\nwrite. Symmetric on cons.tail for slot-reuse ordering.\n\nAcquire-load is a plain mov on x86 → zero throughput change (showcase MPMC/SPSC\nsteady unchanged within noise). Verified: TSan now 0 races on all 6 tests\n(was 14+ on test_mpmc); normal + ASan suites 6/6.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T10:35:48+08:00",
          "tree_id": "b6143ccddc826dc82e055beb96e87bf52c5ae6a0",
          "url": "https://github.com/calcky/libchan/commit/f45eb7052c14aafaeb39a416f69a2a07988d41a7"
        },
        "date": 1782355014855,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv（无等待）",
            "value": 50.8,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC 跨核稳态（缓存一致性墙）",
            "value": 5.65,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC 跨核稳态（游标缓存破墙）",
            "value": 15.77,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC 阻塞 cap=1024",
            "value": 36.92,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan 无缓冲 rendezvous",
            "value": 1.43,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1,
            "unit": "Mops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": true,
          "id": "69097acfc0e5f07854898967150b9393a8940140",
          "message": "docs/bench: correct ring naming (DPDK rte_ring, not Vyukov) + CI reference table\n\nThe lock-free ring (src/ring_lf.{c,h}) is DPDK rte_ring 4-cursor style, not a\nVyukov per-slot queue. Fix the remaining mislabels that called ring_lf\n\"Vyukov\": the bench_showcase ladder row 3 label + comments, architecture.md,\nand bench/crosslang/README.md. (bench_ring_cmp.c keeps \"Vyukov\" — it really\ndoes compare a Vyukov per-slot impl against the DPDK ring; design.md keeps the\nconceptual \"Vyukov bounded queue\" reference.)\n\nAlso add a \"GitHub Actions shared runner (CI)\" reference table to\nbenchmarks.md §0 with the bench_showcase numbers from CI — clearly marked as a\nseparate, noisier, ~4-core environment (trend-only), distinct from the\ndev-machine ladder.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T10:44:19+08:00",
          "tree_id": "7a0ddc9cc4b2ad5901909818ae8146e809eafeca",
          "url": "https://github.com/calcky/libchan/commit/69097acfc0e5f07854898967150b9393a8940140"
        },
        "date": 1782355522870,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv（无等待）",
            "value": 50.8,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC 跨核稳态（缓存一致性墙）",
            "value": 5.78,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC 跨核稳态（游标缓存破墙）",
            "value": 17.15,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC 阻塞 cap=1024",
            "value": 35.85,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan 无缓冲 rendezvous",
            "value": 1.43,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1.63,
            "unit": "Mops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": true,
          "id": "ff08e928fed1166e65c15ddf10f099702938939f",
          "message": "docs/i18n: translate all Chinese comments and docs to English\n\nConvert every Chinese comment, doc, CI string, and benchmark label across\nthe repository to natural technical English. Going forward, comments and\ndocs are English-only.\n\nScope: README, all doc/*.md, all examples/*.c, all bench code + scripts +\ncrosslang (C/Go/Rust/sh), .github workflow + parser scripts, and the two\nremaining src/ inline comments. No code logic, identifiers, numbers, paths,\nor output formats changed; CI parsers and the full test suite verified green.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T11:20:15+08:00",
          "tree_id": "f1fcc06b505d264ec04accd268138eb751371507",
          "url": "https://github.com/calcky/libchan/commit/ff08e928fed1166e65c15ddf10f099702938939f"
        },
        "date": 1782357690927,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv (no wait)",
            "value": 49.4,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC cross-core steady-state (cache-coherence wall)",
            "value": 5.29,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC cross-core steady-state (cursor caching breaks the wall)",
            "value": 19.46,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC blocking cap=1024",
            "value": 34.85,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan unbuffered rendezvous",
            "value": 1.4,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1.24,
            "unit": "Mops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": false,
          "id": "60a0a0f8677fcfc884ce549a3a25d8d9317edf59",
          "message": "feat(ring): bulk MPMC enqueue/dequeue to amortize the cross-core wall\n\nAdd ring_lf_enqueue_burst / ring_lf_dequeue_burst: reserve up to n\ncontiguous slots in one CAS, copy the run (wrap-aware, two memcpy when it\ncrosses the power-of-2 boundary), and do one Phase-3 commit. This amortizes\nthe per-op CAS, the cross-core acquire-read of the opposite cursor, and the\nordered commit over the whole batch — per-element cross-core traffic drops\n~1/k — while keeping the single-element reserve→write→commit memory ordering\nverbatim. Burst semantics: move as many as fit/are available up to n, return\nthe count moved (0 when full/empty). MPMC-safe; never touches SPSC caches.\n\nThe 121 ns / 8 Mops single-element MPMC steady-state is a per-CAS/per-commit\nwall, not per-element. bench_bulk shows 2P+2C throughput rising 5.9 → 641\nMops (109x) from batch 1 → 128, far past that wall.\n\n- src/ring_lf.{c,h}: implementation + protocol notes\n- tests/test_bulk.c: wrap FIFO, partial clamp, 4P+4C exactly-once stress\n  (passes under TSan/ASan/UBSan)\n- bench/bench_bulk.c: single-thread + 2P2C amortization across batch sizes\n- doc: benchmarks.md §0.5 + architecture.md further-reading entry\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T11:43:49+08:00",
          "tree_id": "59d89c98f10bf3450782f0c379866bcdc0e546e1",
          "url": "https://github.com/calcky/libchan/commit/60a0a0f8677fcfc884ce549a3a25d8d9317edf59"
        },
        "date": 1782359314643,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv (no wait)",
            "value": 49.36,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC cross-core steady-state (cache-coherence wall)",
            "value": 6.24,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC cross-core steady-state (cursor caching breaks the wall)",
            "value": 36.79,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC blocking cap=1024",
            "value": 35.31,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan unbuffered rendezvous",
            "value": 1.42,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1.61,
            "unit": "Mops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "committer": {
            "email": "chenkeyu@wsdashi.com",
            "name": "chenkeyu"
          },
          "distinct": true,
          "id": "ea7642d6edbe75a9f638f96e898e66f209b4f0cf",
          "message": "fix: MPMC buffered send dropped a message on a failed FIFO-swap push\n\nSymptom\n-------\nCI's TSan job failed intermittently with a message LOSS (not a data race):\n\n    test_stress cap=1: expected 400000 got 399999\n\ni.e. chan_send returned CHAN_OK for a message that was never received.\n\nCause\n-----\nIn the buffered send slow path, when a receiver was waiting, the code did a\nFIFO-preserving swap: pop the OLDEST ring item for the receiver, then push the\nnew `data` at the tail:\n\n    if (!ring_empty(ch) && ring_pop(ch, r->data)) {\n        ring_push(ch, data);    /* \"slot just freed -> succeeds\"  <-- WRONG */\n    }\n\nThe `ring_push` return value was ignored on the assumption that the slot we\njust freed guarantees room. It does not: a *stale* fast-path sender — one that\npassed the `send_waiter_cnt==0 && recv_waiter_cnt==0` gate BEFORE this receiver\nregistered — can still be mid `ring_lf_push` and reserve the freed slot\nconcurrently (the same \"stale fast-path op\" race the recv side already guards\nagainst). When that happens ring_push fails, `data` is neither buffered nor\ndelivered, yet chan_send returns CHAN_OK. One message vanishes.\n\nBenign on a lightly-loaded box; the window widens sharply with more threads\nthan cores (heavy preemption between the gate check and the push), which is\nwhy it surfaced on the 4-core CI runner under TSan but not on a 20-core dev box.\n\nReproduction\n------------\n8 producers + 8 consumers, cap=1, all hammering the single slot; instrument the\ntest to read ring_lf_count(&ch->ring) after every thread has exited:\n\n    build: cmake -B b -DLIBCHAN_SANITIZE=thread && cmake --build b\n    run:   setarch -R taskset -c 0-3 ./repro   # pin to 4 cores, ASLR off\n\nBefore the fix this loses ~1 message within ~1000 iterations, always with\nring_left_after_drain == 0 — proving the lost item never entered the ring\n(a send-side drop), not a stuck/undrained buffer.\n\nFix\n---\nReplace the fragile pop-then-push swap with buffer-then-drain, checking EVERY\nring op:\n\n  - ring_push(data) first; then deliver buffered items to parked receivers in\n    FIFO order via a new lock-held helper chan_deliver_ring_to_receivers_locked\n    (oldest slot to the next receiver, each ring_pop checked).\n  - If ring_push fails (ring genuinely full / lost the slot to a stale sender),\n    `data` stays in hand and we park as a sender — never dropped.\n\nThis also removes latent dead-code that could wake a receiver with no data\nwritten (a phantom delivery). The recv side already checked its ring_push\nreturn value, so only the send side was affected.\n\nVerification\n------------\nAfter the fix: 0 lost across 150k normal iterations + TSan iterations of the\n8P+8C cap=1 repro (was failing by iter ~909 before); full suite 7/7 on normal\nand ASan; TSan test_stress + test_mpmc 0 races / 0 losses.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-25T14:32:14+08:00",
          "tree_id": "f1401ec910c3465f0f0b55e019e7691976ffcaeb",
          "url": "https://github.com/calcky/libchan/commit/ea7642d6edbe75a9f638f96e898e66f209b4f0cf"
        },
        "date": 1782369332903,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "4. chan try_send/recv (no wait)",
            "value": 70.19,
            "unit": "Mops/s"
          },
          {
            "name": "5. chan MPMC cross-core steady-state (cache-coherence wall)",
            "value": 8.77,
            "unit": "Mops/s"
          },
          {
            "name": "6. chan SPSC cross-core steady-state (cursor caching breaks the wall)",
            "value": 68.13,
            "unit": "Mops/s"
          },
          {
            "name": "7. chan SPSC blocking cap=1024",
            "value": 86.43,
            "unit": "Mops/s"
          },
          {
            "name": "8. chan unbuffered rendezvous",
            "value": 1.44,
            "unit": "Mops/s"
          },
          {
            "name": "9. chan MPMC 4P+4C cap=1024",
            "value": 1.19,
            "unit": "Mops/s"
          }
        ]
      }
    ]
  }
}