window.BENCHMARK_DATA = {
  "lastUpdate": 1782355523691,
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
      }
    ]
  }
}