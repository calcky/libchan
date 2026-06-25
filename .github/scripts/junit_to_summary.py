#!/usr/bin/env python3
"""Render a CTest JUnit XML file as a GitHub-flavoured Markdown report.

Usage:  junit_to_summary.py <ctest-junit.xml> [title]

Writes the report to stdout (intended for >> "$GITHUB_STEP_SUMMARY").
Exit code is always 0 — this is a reporter, not a gate; CTest itself fails
the job. If the file is missing/unparseable it still emits a visible note.
"""
import sys
import xml.etree.ElementTree as ET


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else ""
    title = sys.argv[2] if len(sys.argv) > 2 else "Unit tests (ctest)"

    try:
        root = ET.parse(path).getroot()
    except Exception as exc:  # missing file, bad XML, etc.
        print(f"### ❌ {title}\n\n> Could not read JUnit report `{path}`: {exc}\n")
        return 0

    # CTest may emit <testsuites><testsuite>... or a top-level <testsuite>.
    suites = root.findall(".//testsuite") or ([root] if root.tag == "testsuite" else [])
    cases = []
    for s in suites:
        cases.extend(s.findall("testcase"))

    def failed(tc) -> bool:
        if tc.find("failure") is not None or tc.find("error") is not None:
            return True
        status = (tc.get("status") or "").lower()
        return status in ("fail", "failed", "error")

    total = len(cases)
    n_fail = sum(1 for tc in cases if failed(tc))
    n_pass = total - n_fail
    icon = "✅" if n_fail == 0 and total > 0 else ("❌" if n_fail else "⚠️")

    out = [f"### {icon} {title}", ""]
    if total == 0:
        out.append("> No test cases found (check `LIBCHAN_BUILD_TESTS=ON` and `--no-tests=error`).")
        print("\n".join(out) + "\n")
        return 0

    out.append(f"**{n_pass}/{total} passed**, {n_fail} failed.")
    out += ["", "| Test case | Result | Time (s) |", "|------|------|---------:|"]
    for tc in cases:
        name = tc.get("name", "?")
        t = tc.get("time", "")
        try:
            t = f"{float(t):.2f}"
        except (TypeError, ValueError):
            t = "—"
        out.append(f"| `{name}` | {'❌ FAIL' if failed(tc) else '✅ pass'} | {t} |")

    # Append failure details (truncated) for quick triage.
    details = []
    for tc in cases:
        node = tc.find("failure") if tc.find("failure") is not None else tc.find("error")
        if node is not None:
            text = (node.get("message") or node.text or "").strip()
            if text:
                details.append(f"#### `{tc.get('name', '?')}`\n```\n{text[:2000]}\n```")
    if details:
        out += ["", "<details><summary>Failure details</summary>", ""] + details + ["", "</details>"]

    print("\n".join(out) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
