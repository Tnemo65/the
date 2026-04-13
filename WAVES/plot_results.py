"""Visualize ablation experiment results.

Reads: results/aggregate/summary_by_baseline.csv
Output: results/figs/*.png

Usage:
    python plot_results.py --aggregate-dir results/aggregate --output-dir results/figs
"""
import argparse
import json
import sys
from pathlib import Path
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd


BASELINE_ORDER = [
    "nl_stream", "single_tree", "static_box",
    "waves_single_rule", "buffer_wait", "waves_full",
]

BASELINE_LABELS = {
    "nl_stream":          "NL-Stream",
    "single_tree":       "Single-Tree",
    "static_box":        "Static-Box",
    "waves_single_rule":  "WAVES-SingleRule",
    "buffer_wait":        "Buffer-Wait",
    "waves_full":         "WAVES-Full",
}

COLORS = {
    "nl_stream":         "#e74c3c",
    "single_tree":        "#e67e22",
    "static_box":         "#3498db",
    "waves_single_rule":  "#9b59b6",
    "buffer_wait":        "#1abc9c",
    "waves_full":         "#27ae60",
}

RC = {
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.figsize": (12, 5),
    "figure.dpi": 150,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.family": "DejaVu Sans",
}


def _fmt(val, fmt="{:.0f}"):
    return fmt.format(val)


def _sort_df(df: pd.DataFrame) -> pd.DataFrame:
    if "baseline" not in df.columns:
        return df
    order = {b: i for i, b in enumerate(BASELINE_ORDER)}
    df = df.copy()
    df["_sort"] = df["baseline"].map(lambda b: order.get(b, 99))
    return df.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)


def _bar(ax, labels, values, stds, colors, ylabel, title, fmt="{:.0f}"):
    x = range(len(labels))
    bars = ax.bar(x, values, yerr=stds, capsize=5,
                  color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=10)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(stds) * 0.02,
                _fmt(val, fmt), ha="center", va="bottom", fontsize=9)
    return bars


def plot_rq1_system(summary_path: Path, output_dir: Path):
    """RQ1: Throughput + Latency bar charts."""
    try:
        df = _sort_df(pd.read_csv(summary_path))
    except Exception as ex:
        print(f"  [skip] {summary_path}: {ex}")
        return

    labels = [BASELINE_LABELS.get(b, b) for b in df["baseline"]]
    colors = [COLORS.get(b, "#95a5a6") for b in df["baseline"]]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    plt.rc_context(RC)

    # Throughput
    tp_mean = df["throughput_mean"].values
    tp_std  = df.get("throughput_std", pd.Series([0]*len(df))).fillna(0).values
    _bar(axes[0], labels, tp_mean, tp_std, colors,
          "Throughput (events/s)", "RQ1: Throughput",
          fmt="{:.0f}")

    # P99 Latency
    if "p99_latency_ms_mean" in df.columns:
        lat_mean = df["p99_latency_ms_mean"].values
        lat_std  = df.get("p99_latency_ms_std", pd.Series([0]*len(df))).fillna(0).values
        _bar(axes[1], labels, lat_mean, lat_std, colors,
              "P99 Latency (ms)", "RQ1: P99 Latency",
              fmt="{:.1f}")

    plt.tight_layout()
    out = output_dir / "rq1_system.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


def plot_rq2_accuracy(summary_path: Path, output_dir: Path):
    """RQ2: Precision / Recall / F1 grouped bar charts."""
    try:
        df = _sort_df(pd.read_csv(summary_path))
    except Exception as ex:
        print(f"  [skip] {summary_path}: {ex}")
        return

    labels = [BASELINE_LABELS.get(b, b) for b in df["baseline"]]
    colors = [COLORS.get(b, "#95a5a6") for b in df["baseline"]]

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    plt.rc_context(RC)

    for ax, metric, label in zip(
        axes,
        ["precision", "recall", "f1"],
        ["Precision", "Recall", "F1"],
    ):
        m_mean = df[f"{metric}_mean"].values
        m_std  = df.get(f"{metric}_std", pd.Series([0]*len(df))).fillna(0).values
        _bar(ax, labels, m_mean, m_std, colors, label,
              f"RQ2: {label}", fmt="{:.3f}")

    plt.tight_layout()
    out = output_dir / "rq2_accuracy.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


def plot_rq3_alerts(summary_path: Path, output_dir: Path):
    """RQ3: Alerts by DC per baseline."""
    try:
        df = _sort_df(pd.read_csv(summary_path))
    except Exception as ex:
        print(f"  [skip] {summary_path}: {ex}")
        return

    dc_cols = ["DC1", "DC2", "DC3"]
    available_dc = [c for c in dc_cols if c in df.columns]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    plt.rc_context(RC)

    labels = [BASELINE_LABELS.get(b, b) for b in df["baseline"]]
    colors = [COLORS.get(b, "#95a5a6") for b in df["baseline"]]

    # Left: Total alerts
    if "alerts_total_mean" in df.columns:
        total_mean = df["alerts_total_mean"].values
        total_std  = df.get("alerts_total_std", pd.Series([0]*len(df))).fillna(0).values
        _bar(axes[0], labels, total_mean, total_std, colors,
              "Total Alerts", "RQ3: Alert Volume", fmt="{:.0f}")

    # Right: Stacked by DC
    ax = axes[1]
    x = range(len(labels))
    bottoms = np.zeros(len(x))
    dc_colors = ["#e74c3c", "#3498db", "#27ae60"]
    for i, dc in enumerate(available_dc):
        vals = df[dc].fillna(0).values
        ax.bar(x, vals, bottom=bottoms, label=dc, color=dc_colors[i % len(dc_colors)], alpha=0.85, edgecolor="white")
        bottoms += vals
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Alerts", fontsize=11)
    ax.set_title("RQ3: Alerts by DC", fontsize=13, fontweight="bold")
    ax.legend(title="DC")
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)

    plt.tight_layout()
    out = output_dir / "rq3_alerts.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


def plot_ablation_summary(summary_path: Path, output_dir: Path):
    """Full ablation summary: 4-panel figure."""
    try:
        df = _sort_df(pd.read_csv(summary_path))
    except Exception as ex:
        print(f"  [skip] {summary_path}: {ex}")
        return

    labels = [BASELINE_LABELS.get(b, b) for b in df["baseline"]]
    colors = [COLORS.get(b, "#95a5a6") for b in df["baseline"]]
    no_fill = [(*c, 0.4) for c in colors]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    plt.rc_context(RC)

    # Throughput
    _bar(axes[0, 0], labels,
         df["throughput_mean"].values,
         df.get("throughput_std", pd.Series([0]*len(df))).fillna(0).values,
         colors, "Throughput (ev/s)", "Throughput")

    # P99 Latency
    if "p99_latency_ms_mean" in df.columns:
        _bar(axes[0, 1], labels,
             df["p99_latency_ms_mean"].values,
             df.get("p99_latency_ms_std", pd.Series([0]*len(df))).fillna(0).values,
             colors, "P99 Latency (ms)", "P99 Latency", fmt="{:.1f}")

    # Precision
    _bar(axes[1, 0], labels,
         df["precision_mean"].values,
         df.get("precision_std", pd.Series([0]*len(df))).fillna(0).values,
         colors, "Precision", "Precision", fmt="{:.3f}")

    # F1
    _bar(axes[1, 1], labels,
         df["f1_mean"].values,
         df.get("f1_std", pd.Series([0]*len(df))).fillna(0).values,
         colors, "F1 Score", "F1 Score", fmt="{:.3f}")

    plt.suptitle("WAVES Ablation Study — Full Summary", fontsize=15, fontweight="bold", y=1.01)
    plt.tight_layout()
    out = output_dir / "ablation_summary.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


def main():
    parser = argparse.ArgumentParser(description="Plot ablation experiment results")
    parser.add_argument("--aggregate-dir", type=Path, default=Path("results/aggregate"))
    parser.add_argument("--output-dir", type=Path, default=Path("results/figs"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nPlotting results from {args.aggregate_dir}/ → {args.output_dir}/")

    summary = args.aggregate_dir / "summary_by_baseline.csv"
    if summary.exists():
        plot_ablation_summary(summary, args.output_dir)
        plot_rq1_system(summary, args.output_dir)
        plot_rq2_accuracy(summary, args.output_dir)

    rq3 = args.aggregate_dir / "rq3_alerts_by_dc.csv"
    if rq3.exists():
        plot_rq3_alerts(rq3, args.output_dir)

    rq1_sys = args.aggregate_dir / "rq1_system.csv"
    if rq1_sys.exists() and rq1_sys != summary:
        plot_rq1_system(rq1_sys, args.output_dir)

    rq2_acc = args.aggregate_dir / "rq2_accuracy.csv"
    if rq2_acc.exists() and rq2_acc != summary:
        plot_rq2_accuracy(rq2_acc, args.output_dir)

    generated = list(args.output_dir.glob("*.png"))
    print(f"\nGenerated {len(generated)} figure(s):")
    for f in sorted(generated):
        size_kb = f.stat().st_size // 1024
        print(f"  {f.name} ({size_kb} KB)")


if __name__ == "__main__":
    main()
