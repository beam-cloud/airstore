#!/usr/bin/env python3
"""
Generate performance charts and a GitHub Actions job summary from
integration test results.

Usage:
    python e2e/plot_results.py e2e/results.json e2e/plots/

Outputs:
    - e2e/plots/bytes_comparison.png
    - e2e/plots/reduction_pct.png
    - e2e/plots/latency_comparison.png
    - e2e/plots/summary_donut.png
    - Markdown appended to $GITHUB_STEP_SUMMARY (if set)
"""

import json
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def truncate(name: str, maxlen: int = 30) -> str:
    if len(name) <= maxlen:
        return name
    return name[: maxlen - 3] + "..."


def save(fig, path: str) -> None:
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  wrote {path}")


# ---------------------------------------------------------------------------
# Chart 1: Bytes comparison (grouped bar)
# ---------------------------------------------------------------------------


def plot_bytes_comparison(files: list[dict], out: str) -> None:
    if not files:
        return
    # Sort by raw size descending
    files = sorted(files, key=lambda f: f["raw_bytes"], reverse=True)
    names = [truncate(f["name"]) for f in files]
    raw = [f["raw_bytes"] for f in files]
    strip = [f["strip_bytes"] for f in files]

    fig, ax = plt.subplots(figsize=(max(8, len(files) * 0.8), 5))
    x = range(len(names))
    w = 0.35
    ax.bar(
        [i - w / 2 for i in x], raw, w, label="Raw", color="#9e9e9e", edgecolor="white"
    )
    ax.bar(
        [i + w / 2 for i in x],
        strip,
        w,
        label="Strip",
        color="#1976d2",
        edgecolor="white",
    )

    ax.set_xlabel("File")
    ax.set_ylabel("Bytes")
    ax.set_title("Raw vs Strip: Byte Size per File")
    ax.set_xticks(list(x))
    ax.set_xticklabels(names, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(
            lambda v, _: f"{v / 1000:.0f}k" if v >= 1000 else f"{v:.0f}"
        )
    )
    ax.legend()
    fig.tight_layout()
    save(fig, out)


# ---------------------------------------------------------------------------
# Chart 2: Reduction % (horizontal bar)
# ---------------------------------------------------------------------------


def plot_reduction_pct(files: list[dict], threshold: int, out: str) -> None:
    if not files:
        return
    files = sorted(files, key=lambda f: f["reduction_pct"])
    names = [truncate(f["name"]) for f in files]
    pcts = [f["reduction_pct"] for f in files]
    colors = []
    for p in pcts:
        if p >= 20:
            colors.append("#4caf50")  # green
        elif p >= threshold:
            colors.append("#ff9800")  # yellow
        else:
            colors.append("#f44336")  # red

    fig, ax = plt.subplots(figsize=(8, max(4, len(files) * 0.4)))
    ax.barh(names, pcts, color=colors, edgecolor="white")
    ax.axvline(
        x=threshold,
        color="#f44336",
        linestyle="--",
        linewidth=1.5,
        label=f"Threshold ({threshold}%)",
    )
    ax.set_xlabel("Reduction %")
    ax.set_title("Compression Reduction per File")
    ax.legend(loc="lower right")
    for i, v in enumerate(pcts):
        ax.text(v + 0.5, i, f"{v}%", va="center", fontsize=8)
    fig.tight_layout()
    save(fig, out)


# ---------------------------------------------------------------------------
# Chart 3: Latency comparison (grouped bar)
# ---------------------------------------------------------------------------


def plot_latency_comparison(files: list[dict], out: str) -> None:
    if not files:
        return
    names = [truncate(f["name"]) for f in files]
    raw_lat = [f["raw_latency_ms"] for f in files]
    strip_lat = [f["strip_latency_ms"] for f in files]

    fig, ax = plt.subplots(figsize=(max(8, len(files) * 0.8), 5))
    x = range(len(names))
    w = 0.35
    ax.bar(
        [i - w / 2 for i in x],
        raw_lat,
        w,
        label="Raw",
        color="#9e9e9e",
        edgecolor="white",
    )
    ax.bar(
        [i + w / 2 for i in x],
        strip_lat,
        w,
        label="Strip",
        color="#1976d2",
        edgecolor="white",
    )

    ax.set_xlabel("File")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Read Latency: Raw vs Strip")
    ax.set_xticks(list(x))
    ax.set_xticklabels(names, rotation=45, ha="right", fontsize=8)
    ax.legend()
    fig.tight_layout()
    save(fig, out)


# ---------------------------------------------------------------------------
# Chart 4: Summary donut
# ---------------------------------------------------------------------------


def plot_summary_donut(total_raw: int, total_strip: int, out: str) -> None:
    if total_raw <= 0:
        return
    saved = total_raw - total_strip
    pct = saved * 100 / total_raw

    fig, ax = plt.subplots(figsize=(5, 5))
    sizes = [total_strip, saved]
    colors = ["#1976d2", "#4caf50"]
    labels = [f"Kept ({total_strip:,} B)", f"Saved ({saved:,} B)"]

    wedges, texts, autotexts = ax.pie(
        sizes,
        labels=labels,
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.8,
        wedgeprops=dict(width=0.4, edgecolor="white"),
    )
    ax.text(
        0,
        0,
        f"{pct:.0f}%\nreduced",
        ha="center",
        va="center",
        fontsize=18,
        fontweight="bold",
    )
    ax.set_title("Overall Compression Savings")
    fig.tight_layout()
    save(fig, out)


# ---------------------------------------------------------------------------
# Chart 5: I/O test latency bar
# ---------------------------------------------------------------------------


def plot_io_latency(io_tests: list[dict], out: str) -> None:
    tests = [t for t in io_tests if t.get("latency_ms", 0) > 0]
    if not tests:
        return
    names = [t["name"] for t in tests]
    latencies = [t["latency_ms"] for t in tests]
    colors = ["#4caf50" if t["passed"] else "#f44336" for t in tests]

    fig, ax = plt.subplots(figsize=(max(6, len(tests) * 1.2), 4))
    ax.bar(names, latencies, color=colors, edgecolor="white")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("I/O Smoke Test Latency")
    for i, v in enumerate(latencies):
        ax.text(i, v + 1, f"{v}ms", ha="center", fontsize=9)
    fig.tight_layout()
    save(fig, out)


# ---------------------------------------------------------------------------
# Markdown summary for GitHub Actions job summary
# ---------------------------------------------------------------------------


def write_summary(data: dict, plots_dir: str) -> str:
    io = data.get("io_tests", {})
    comp = data.get("compression", {})
    files = comp.get("files", [])

    io_ok = io.get("passed", False)
    comp_ok = comp.get("passed", False)
    overall = io_ok and comp_ok

    lines = []
    status = "PASSED" if overall else "FAILED"
    badge = "white_check_mark" if overall else "x"
    lines.append(f"## :{badge}: Integration Tests {status}")
    lines.append("")
    lines.append(f"**Gateway**: `{data.get('gateway', 'unknown')}`  ")
    lines.append(f"**Query path**: `{data.get('query_path', 'unknown')}`  ")
    lines.append(f"**Timestamp**: {data.get('timestamp', 'unknown')}")
    lines.append("")

    # I/O summary
    lines.append("### I/O Smoke Tests")
    lines.append("")
    io_tests = io.get("tests", [])
    if io_tests:
        lines.append("| Test | Status | Latency | Bytes |")
        lines.append("|------|--------|---------|-------|")
        for t in io_tests:
            s = "PASS" if t["passed"] else "FAIL"
            lat = f"{t['latency_ms']}ms" if t.get("latency_ms") else "-"
            b = f"{t['bytes']:,}" if t.get("bytes") else "-"
            lines.append(f"| `{t['name']}` | {s} | {lat} | {b} |")
    lines.append("")

    # Compression summary
    lines.append("### Compression A/B Tests")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Files tested | {len(files)} |")
    lines.append(f"| Total raw bytes | {comp.get('total_raw_bytes', 0):,} |")
    lines.append(f"| Total strip bytes | {comp.get('total_strip_bytes', 0):,} |")
    lines.append(f"| Average reduction | **{comp.get('avg_reduction_pct', 0)}%** |")
    lines.append(f"| Min threshold | {comp.get('min_reduction_pct', 0)}% |")
    lines.append(
        f"| Cache consistent | {'Yes' if comp.get('cache_consistent') else 'No'} |"
    )
    lines.append("")

    if files:
        lines.append("<details>")
        lines.append("<summary>Per-file results</summary>")
        lines.append("")
        lines.append("| File | Raw | Strip | Reduction | Raw ms | Strip ms |")
        lines.append("|------|-----|-------|-----------|--------|----------|")
        for f in sorted(files, key=lambda x: x["reduction_pct"]):
            lines.append(
                f"| `{truncate(f['name'], 40)}` "
                f"| {f['raw_bytes']:,} "
                f"| {f['strip_bytes']:,} "
                f"| {f['reduction_pct']}% "
                f"| {f['raw_latency_ms']}ms "
                f"| {f['strip_latency_ms']}ms |"
            )
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results.json> <plots_dir/>")
        return 1

    results_path = sys.argv[1]
    plots_dir = sys.argv[2]

    if not os.path.isfile(results_path):
        print(f"Results file not found: {results_path}")
        return 1

    Path(plots_dir).mkdir(parents=True, exist_ok=True)

    with open(results_path) as f:
        data = json.load(f)

    comp = data.get("compression", {})
    files = comp.get("files", [])
    io_tests = data.get("io_tests", {}).get("tests", [])
    threshold = comp.get("min_reduction_pct", 10)

    print("Generating charts...")

    if files:
        plot_bytes_comparison(files, os.path.join(plots_dir, "bytes_comparison.png"))
        plot_reduction_pct(
            files, threshold, os.path.join(plots_dir, "reduction_pct.png")
        )
        plot_latency_comparison(
            files, os.path.join(plots_dir, "latency_comparison.png")
        )
        plot_summary_donut(
            comp.get("total_raw_bytes", 0),
            comp.get("total_strip_bytes", 0),
            os.path.join(plots_dir, "summary_donut.png"),
        )

    if io_tests:
        plot_io_latency(io_tests, os.path.join(plots_dir, "io_latency.png"))

    # Write markdown summary
    summary_md = write_summary(data, plots_dir)

    # Write to file for local use
    summary_path = os.path.join(plots_dir, "summary.md")
    with open(summary_path, "w") as f:
        f.write(summary_md)
    print(f"  wrote {summary_path}")

    # Append to GitHub Actions job summary if available
    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a") as f:
            f.write(summary_md)
            f.write("\n")
        print(f"  appended to $GITHUB_STEP_SUMMARY")

    # Exit non-zero if tests failed
    io_ok = data.get("io_tests", {}).get("passed", False)
    comp_ok = comp.get("passed", False)
    if not (io_ok and comp_ok):
        print("\nTests FAILED")
        return 1

    print("\nAll tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
