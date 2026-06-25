window.BENCHMARK_DATA = {
  "lastUpdate": 1782354466658,
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
      }
    ]
  }
}