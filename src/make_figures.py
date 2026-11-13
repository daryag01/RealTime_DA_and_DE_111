"""Generate report figures: architecture diagram, benchmark charts, window chart."""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = Path(__file__).resolve().parent.parent
OUT, DOCS = ROOT / "output", ROOT / "docs"
BLUE, ORANGE, AQUA, YELLOW, VIOLET = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#6250d6"
INK, INK2, SURF, GRID = "#0b0b0b", "#52514e", "#fcfcfb", "#e4e3df"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": INK2,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2})


def box(ax, x, y, w, h, title, sub, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                fc="white", ec=color, lw=2))
    ax.add_patch(FancyBboxPatch((x, y + h - 0.08), w, 0.08, boxstyle="square,pad=0", fc=color, ec=color))
    ax.text(x + w / 2, y + h - 0.32, title, ha="center", va="center", fontsize=10.5, weight="bold", color=INK)
    ax.text(x + w / 2, y + (h - 0.4) / 2, sub, ha="center", va="center", fontsize=8.3, color=INK2,
            linespacing=1.35)


def arrow(ax, a, b, label="", style="-|>", color=INK2, ls="-"):
    ax.add_patch(FancyArrowPatch(a, b, arrowstyle=style, mutation_scale=14, lw=1.5, color=color, ls=ls))
    if label:
        ax.text((a[0] + b[0]) / 2, (a[1] + b[1]) / 2 + 0.12, label, ha="center", fontsize=7.8, color=INK2)


def architecture():
    fig, ax = plt.subplots(figsize=(13.6, 6.6))
    ax.set_xlim(0, 13.6); ax.set_ylim(0, 6.6); ax.axis("off")
    fig.patch.set_facecolor(SURF)
    ax.text(0.1, 6.35, "AITU research-event streaming architecture", fontsize=14, weight="bold", color=INK)
    ax.text(0.1, 6.0, "topic aitu.research.events · 3 partitions · key = research_group · group research-analytics",
            fontsize=9.5, color=INK2)
    # producers
    for i, (g, y) in enumerate([("G01", 4.4), ("G02", 2.9), ("G03", 1.4)]):
        box(ax, 0.2, y, 2.0, 1.1, f"Producer {g}", "producer.py\nacks=all, retries=5\nlinger_ms=5", BLUE)
    # kafka cluster
    ax.add_patch(FancyBboxPatch((3.0, 0.9), 3.1, 4.9, boxstyle="round,pad=0.02,rounding_size=0.15",
                                fc="#f0efec", ec=INK2, lw=1.2, ls="--"))
    ax.text(4.55, 5.5, "Kafka cluster (KRaft)", ha="center", weight="bold", color=INK)
    ax.text(4.55, 5.2, "topic: aitu.research.events", ha="center", fontsize=8.5, color=INK2)
    parts = [("partition 2", "key G01 · E001,E003,E006,E009", 4.25), ("partition 1", "key G02 · E002,E005,E008", 2.95),
             ("partition 0", "key G03 · E004,E007,E010", 1.65)]
    for name, sub, y in parts:
        ax.add_patch(FancyBboxPatch((3.2, y), 2.7, 0.85, boxstyle="round,pad=0.01,rounding_size=0.08",
                                    fc="white", ec=ORANGE, lw=1.6))
        ax.text(4.55, y + 0.57, name, ha="center", weight="bold", fontsize=9.5, color=INK)
        ax.text(4.55, y + 0.25, sub, ha="center", fontsize=7.6, color=INK2)
    for (g, y), (_, _, py) in zip([("G01", 4.4), ("G02", 2.9), ("G03", 1.4)], parts):
        arrow(ax, (2.25, y + 0.55), (3.18, py + 0.42))
    # consumer group
    ax.add_patch(FancyBboxPatch((6.7, 0.9), 2.6, 4.9, boxstyle="round,pad=0.02,rounding_size=0.15",
                                fc="#f0efec", ec=INK2, lw=1.2, ls="--"))
    ax.text(8.0, 5.5, "Consumer group", ha="center", weight="bold", color=INK)
    ax.text(8.0, 5.2, "research-analytics", ha="center", fontsize=8.5, color=INK2)
    box(ax, 6.9, 2.6, 2.2, 2.3, "Python processor",
        "consumer.py\n1. deserialize JSON\n2. validate schema\n   → dead-letter\n3. running aggregates\n4. 5 s event-time\n   windows (wm 2 s)", AQUA)
    box(ax, 6.9, 1.05, 2.2, 1.3, "Spark Structured\nStreaming", "\nspark_streaming.py\nwindow × job_type", VIOLET)
    for _, _, py in parts:
        arrow(ax, (5.92, py + 0.42), (6.88, 3.7))
    arrow(ax, (5.92, 1.9), (6.88, 1.6), color=VIOLET, ls="--")
    # aggregation layer
    box(ax, 10.5, 3.4, 2.9, 1.9, "Aggregation layer",
        "output/consumer_results.json\nstatus counts · avg duration/type\navg CPU/group · window table\nSpark console / sink", YELLOW)
    arrow(ax, (9.12, 4.3), (10.48, 4.3))
    ax.text(9.9, 4.42, "commit after batch", ha="center", fontsize=7.8, color=INK2)
    arrow(ax, (9.12, 2.1), (10.48, 3.5), color=VIOLET, ls="--")
    # monitoring
    box(ax, 10.5, 0.9, 2.9, 2.1, "Monitoring",
        "consumer lag per partition\nthroughput · p95 latency · CPU\n(benchmark.py, Kafka UI :8080)\nalert: lag↑, failure rate > 20 %", "#e34948")
    arrow(ax, (11.95, 3.38), (11.95, 3.02))
    arrow(ax, (10.48, 1.4), (9.12, 1.4), color="#e34948", ls=":")
    ax.text(9.9, 1.52, "lag / metrics", ha="center", fontsize=7.8, color=INK2)
    fig.savefig(DOCS / "architecture.png", dpi=170, bbox_inches="tight", facecolor=SURF)


def perf():
    base = json.loads((OUT / "benchmark_baseline.json").read_text())["results"]
    opt = json.loads((OUT / "benchmark_optimized.json").read_text())["results"]
    rates = [r["input_rate"] for r in base]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.3))
    fig.patch.set_facecolor(SURF)
    for ax, key, title, ylab in [(axes[0], "avg_latency_ms", "Average end-to-end latency", "ms (log scale)"),
                                 (axes[1], "cpu_pct", "Process CPU utilisation", "% of one core")]:
        ax.set_facecolor(SURF)
        for rows, c, name in [(base, ORANGE, "baseline: per-record poll + log"), (opt, BLUE, "optimized: batch poll (500)")]:
            ax.plot(rates, [r[key] for r in rows], color=c, lw=2, marker="o", ms=7,
                    mec=SURF, mew=2, label=name)
        ax.set_xscale("log"); ax.set_xticks(rates); ax.set_xticklabels([str(r) for r in rates])
        if key == "avg_latency_ms":
            ax.set_yscale("log")
        ax.set_title(title, loc="left", fontsize=11.5, weight="bold", color=INK)
        ax.set_xlabel("input rate (events/s, log scale)"); ax.set_ylabel(ylab)
        ax.grid(color=GRID, lw=0.8); ax.spines[["top", "right"]].set_visible(False)
    axes[0].annotate("baseline saturates:\n18.7k ev/s, 866 ms", xy=(20000, base[-1]["avg_latency_ms"]),
                     xytext=(900, 120), fontsize=8.5, color=INK2, arrowprops=dict(arrowstyle="-", color=INK2))
    axes[0].legend(frameon=False, fontsize=8.5, loc="upper left")
    fig.tight_layout()
    fig.savefig(DOCS / "performance.png", dpi=170, facecolor=SURF)


def windows():
    w = json.loads((OUT / "consumer_results.json").read_text())["windows"]
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.2))
    fig.patch.set_facecolor(SURF)
    labels = [x["window"].replace("-", "–\n") for x in w]
    for ax, key, title in [(axes[0], "count", "Events per window"), (axes[1], "failure_rate_pct", "Failure rate, %"),
                           (axes[2], "avg_cpu", "Average CPU, %")]:
        vals = [x[key] for x in w]
        bars = ax.bar(labels, vals, color=BLUE, width=0.55, edgecolor=SURF, lw=2)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:g}", ha="center", va="bottom", fontsize=9, color=INK)
        ax.set_title(title, loc="left", fontsize=11, weight="bold", color=INK)
        ax.set_facecolor(SURF); ax.grid(axis="y", color=GRID); ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False); ax.tick_params(labelsize=8)
    fig.tight_layout()
    fig.savefig(DOCS / "windows.png", dpi=170, facecolor=SURF)


if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    architecture(); perf(); windows()
    print("figures written to", DOCS)
