"""Visualize benchmark results từ summary CSV files.

Usage:
    python -m scripts.plot_results --results-dir results/aggregate
    python -m scripts.plot_results --results-dir results/aggregate --output-dir results/figs
"""
import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np


BAR_COLORS = {
    "NL-Stream":        "#e74c3c",
    "Single-Tree-DaQ":  "#e67e22",
    "Static-Box-DaQ":  "#3498db",
    "WAVES-SingleRule":"#9b59b6",
    "Buffer-Wait-DaQ": "#1abc9c",
    "WAVES-Full":      "#27ae60",
}

MARKER_STYLES = ["o", "s", "^", "D", "v", "p"]

RC_PARAMS = {
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.figsize": (10, 6),
    "figure.dpi": 120,
    "axes.spines.top": False,
    "axes.spines.right": False,
}


def _bar_chart(ax, labels, means, stds, colors, ylabel, title, rotation=0):
    x = np.arange(len(labels))
    bars = ax.bar(x, means, yerr=stds, capsize=4, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotation, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    return bars


def _line_chart(ax, xs, ys, labels, colors, marker_styles, xlabel, ylabel, title, yerrs=None):
    for i, (x, y, label) in enumerate(zip(xs, ys, labels)):
        color = colors[i] if i < len(colors) else None
        marker = marker_styles[i] if i < len(marker_styles) else "o"
        if yerrs is not None and i < len(yerrs):
            ax.errorbar(x, y, yerr=yerrs[i], label=label, color=color,
                       marker=marker, markersize=6, capsize=3, linewidth=2, alpha=0.85)
        else:
            ax.plot(x, y, label=label, color=color, marker=marker, markersize=6, linewidth=2, alpha=0.85)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)


def plot_rq1_throughput(summary_path: Path, output_dir: Path):
    """RQ1: Throughput bar chart cho 6 baselines."""
    try:
        df = pd.read_csv(summary_path)
    except Exception:
        print(f"  [skip] cannot read {summary_path}")
        return

    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Left: throughput
        ax = axes[0]
        baselines = df.get("baseline", df.columns[:1].tolist())
        throughput_cols = ["throughput_mean", "throughput_std"]
        if not all(c in df.columns for c in throughput_cols):
            print(f"  [skip] missing columns in {summary_path}")
            return

        _bar_chart(
            ax,
            baselines,
            df["throughput_mean"].values,
            df["throughput_std"].fillna(0).values,
            [BAR_COLORS.get(b, "#95a5a6") for b in baselines],
            "Throughput (events/s)",
            "RQ1: Throughput Comparison",
        )

        # Right: P99 latency
        ax = axes[1]
        latency_cols = ["p99_latency_ms_mean", "p99_latency_ms_std"]
        if all(c in df.columns for c in latency_cols):
            _bar_chart(
                ax,
                baselines,
                df["p99_latency_ms_mean"].values,
                df["p99_latency_ms_std"].fillna(0).values,
                [BAR_COLORS.get(b, "#95a5a6") for b in baselines],
                "P99 Latency (ms)",
                "RQ1: P99 Latency Comparison",
            )

        plt.tight_layout()
        out = output_dir / "rq1_throughput.png"
        plt.savefig(out, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {out}")


def plot_rq2_accuracy(summary_path: Path, output_dir: Path):
    """RQ2: F1 grouped bar chart (with/without drift, with/without late)."""
    try:
        df = pd.read_csv(summary_path)
    except Exception:
        print(f"  [skip] cannot read {summary_path}")
        return

    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))

        for ax, metric, ylabel in zip(
            axes,
            ["precision", "recall", "f1"],
            ["Precision", "Recall", "F1"],
        ):
            m_mean = f"{metric}_mean"
            m_std = f"{metric}_std"
            if m_mean not in df.columns:
                continue
            _bar_chart(
                ax,
                df["baseline"].tolist() if "baseline" in df.columns else df.index.tolist(),
                df[m_mean].values,
                df[m_std].fillna(0).values,
                [BAR_COLORS.get(b, "#95a5a6") for b in (df.get("baseline", pd.Series(df.index)).tolist())],
                ylabel,
                f"RQ2: {ylabel} Comparison",
            )

        plt.tight_layout()
        out = output_dir / "rq2_accuracy.png"
        plt.savefig(out, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {out}")


def plot_rq3_scalability(summary_path: Path, output_dir: Path):
    """RQ3: Scalability line chart (throughput vs number of DC rules)."""
    try:
        df = pd.read_csv(summary_path)
    except Exception:
        print(f"  [skip] cannot read {summary_path}")
        return

    if "n_rules" not in df.columns or "throughput_mean" not in df.columns:
        print(f"  [skip] missing n_rules or throughput columns")
        return

    with plt.rc_context(RC_PARAMS):
        fig, ax = plt.subplots(figsize=(10, 6))

        systems = df["system"].unique() if "system" in df.columns else ["WAVES-Full"]
        for i, (sys_name, grp) in enumerate(df.groupby("system") if "system" in df.columns else [(None, df)]):
            label = sys_name or "WAVES-Full"
            _line_chart(
                ax,
                [grp["n_rules"].tolist()],
                [grp["throughput_mean"].tolist()],
                [label],
                [BAR_COLORS.get(label, "#95a5a6")],
                MARKER_STYLES,
                "Number of DC Rules",
                "Throughput (events/s)",
                "RQ3: Scalability with DC Rules",
                yerrs=[grp["throughput_std"].fillna(0).tolist()] if "throughput_std" in grp.columns else None,
            )

        plt.tight_layout()
        out = output_dir / "rq3_scalability.png"
        plt.savefig(out, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {out}")


def plot_rq4_sensitivity(results_dir: Path, output_dir: Path):
    """RQ4: Sensitivity analysis heatmaps (E1 pane size, E2 alpha EMA, E3 k_max)."""
    sensitivity_files = {
        "e1_pane_size": results_dir / "rq4_pane_size.csv",
        "e2_alpha": results_dir / "rq4_alpha.csv",
        "e3_kmax": results_dir / "rq4_kmax.csv",
    }

    for exp_name, exp_path in sensitivity_files.items():
        if not exp_path.exists():
            continue
        try:
            df = pd.read_csv(exp_path)
        except Exception:
            continue

        param_col = {"e1_pane_size": "pane_size", "e2_alpha": "alpha", "e3_kmax": "k_max"}.get(exp_name)
        if param_col not in df.columns:
            continue

        with plt.rc_context(RC_PARAMS):
            fig, axes = plt.subplots(1, 2, figsize=(14, 5))

            # F1 heatmap
            ax = axes[0]
            pivot_f1 = df.pivot_table(values="f1_mean" if "f1_mean" in df.columns else "f1",
                                      index=param_col, aggfunc="first")
            if not pivot_f1.empty:
                im = ax.imshow(pivot_f1.values, aspect="auto", cmap="RdYlGn")
                ax.set_yticks(range(len(pivot_f1.index)))
                ax.set_yticklabels([f"{v:.1f}" for v in pivot_f1.index])
                ax.set_xlabel(param_col)
                ax.set_ylabel(param_col)
                ax.set_title(f"RQ4: F1 vs {param_col}")
                plt.colorbar(im, ax=ax, label="F1")

            # P99 latency heatmap
            ax = axes[1]
            lat_col = "p99_latency_ms_mean" if "p99_latency_ms_mean" in df.columns else "p99_latency_ms"
            pivot_lat = df.pivot_table(values=lat_col, index=param_col, aggfunc="first")
            if not pivot_lat.empty:
                im = ax.imshow(pivot_lat.values, aspect="auto", cmap="RdYlGn_r")
                ax.set_yticks(range(len(pivot_lat.index)))
                ax.set_yticklabels([f"{v:.1f}" for v in pivot_lat.index])
                ax.set_xlabel(param_col)
                ax.set_ylabel(param_col)
                ax.set_title(f"RQ4: P99 Latency vs {param_col}")
                plt.colorbar(im, ax=ax, label="P99 Latency (ms)")

            plt.tight_layout()
            out = output_dir / f"rq4_{exp_name}.png"
            plt.savefig(out, bbox_inches="tight")
            plt.close()
            print(f"  Saved: {out}")


def plot_ablation(summary_path: Path, output_dir: Path):
    """Ablation: Contribution bar chart (A1–A5)."""
    try:
        df = pd.read_csv(summary_path)
    except Exception:
        print(f"  [skip] cannot read {summary_path}")
        return

    ablations = [f"A{i}" for i in range(1, 6)]
    ab_names = ["No KD-Tree", "No Pane", "No EMA", "No Retraction", "No Shared Opt"]

    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))

        for ax, metric, ylabel in zip(
            axes,
            ["throughput", "f1", "memory_mb"],
            ["Throughput (events/s)", "F1 Score", "Memory (MB)"],
        ):
            m_mean = f"{metric}_mean" if f"{metric}_mean" in df.columns else metric
            m_std = f"{metric}_std" if f"{metric}_std" in df.columns else None

            if m_mean not in df.columns:
                continue

            vals = df[m_mean].values
            stds = df[m_std].values if m_std and m_std in df.columns else [0] * len(vals)
            colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(vals)))

            _bar_chart(
                ax,
                ab_names,
                vals,
                stds,
                colors,
                ylabel,
                f"Ablation: {ylabel}",
                rotation=30,
            )

        plt.tight_layout()
        out = output_dir / "ablation.png"
        plt.savefig(out, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {out}")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument("--results-dir", type=Path, default=Path("results/aggregate"))
    parser.add_argument("--output-dir", type=Path, default=Path("results/figs"))
    args = parser.parse_args()

    if not args.results_dir.exists():
        print(f"Error: {args.results_dir} not found.")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load config if exists
    cfg_path = args.results_dir / ".."
    cfg_path = args.results_dir
    summary_overall = cfg_path / "summary_overall.csv"
    summary_per_seed = cfg_path / "summary_per_seed.csv"
    rq1_path = cfg_path / "rq1_summary.csv"
    rq2_path = cfg_path / "rq2_summary.csv"
    rq3_path = cfg_path / "rq3_summary.csv"

    print("Generating plots...")
    plot_rq1_throughput(rq1_path if rq1_path.exists() else summary_per_seed, args.output_dir)
    plot_rq2_accuracy(rq2_path if rq2_path.exists() else summary_per_seed, args.output_dir)
    plot_rq3_scalability(rq3_path if rq3_path.exists() else summary_per_seed, args.output_dir)
    plot_rq4_sensitivity(cfg_path, args.output_dir)
    plot_ablation(summary_per_seed if summary_per_seed.exists() else summary_overall, args.output_dir)

    print(f"\nAll plots saved to: {args.output_dir}")


if __name__ == "__main__":
    main()
