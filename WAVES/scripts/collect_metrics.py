"""Aggregate metrics từ nhiều experiment runs → summary CSV.

Usage:
    python -m scripts.collect_metrics --results-dir results --seeds 42 43 44
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def load_runs(results_dir: Path, seeds: list[int]) -> pd.DataFrame:
    """Load all metrics.json from run_* subdirectories matching the seeds."""
    records = []

    for seed in seeds:
        # Match run dirs: run_YYYYMMDD_HHMMSS_seed{seed}/
        for run_dir in sorted(results_dir.glob("run_*")):
            if not run_dir.is_dir():
                continue
            # Check seed in dir name
            if f"_seed{seed}" not in run_dir.name:
                continue
            metrics_path = run_dir / "metrics.json"
            if not metrics_path.exists():
                continue

            with open(metrics_path) as f:
                m = json.load(f)

            cfg = m.get("config", {})
            sys_m = m.get("system", {})
            acc = m.get("accuracy", {})
            alr = m.get("alerts", {})

            record = {
                "run_dir": run_dir.name,
                "seed": seed,
                "total_events": cfg.get("total_events", 0),
                "elapsed_seconds": cfg.get("elapsed_seconds", 0),
                "throughput": sys_m.get("throughput_events_per_sec", 0),
                "avg_latency_ms": sys_m.get("avg_latency_ms", 0),
                "p50_latency_ms": sys_m.get("p50_latency_ms", 0),
                "p95_latency_ms": sys_m.get("p95_latency_ms", 0),
                "p99_latency_ms": sys_m.get("p99_latency_ms", 0),
                "precision": acc.get("precision", 0),
                "recall": acc.get("recall", 0),
                "f1": acc.get("f1", 0),
                "tp": acc.get("tp", 0),
                "fp": acc.get("fp", 0),
                "fn": acc.get("fn", 0),
                "alerts_total": alr.get("total", 0),
                "alerts_provisional": alr.get("provisional", 0),
                "alerts_final": alr.get("final", 0),
                "alerts_retraction": alr.get("retraction", 0),
            }
            records.append(record)

    return pd.DataFrame(records)


def compute_summary(df: pd.DataFrame, group_by: str | list[str]) -> pd.DataFrame:
    """Compute mean ± std summary grouped by specified columns."""
    numeric_cols = [
        "throughput", "avg_latency_ms", "p50_latency_ms",
        "p95_latency_ms", "p99_latency_ms",
        "precision", "recall", "f1",
        "tp", "fp", "fn",
        "alerts_total", "alerts_provisional", "alerts_final", "alerts_retraction",
    ]

    available = [c for c in numeric_cols if c in df.columns]
    agg_dict = {}
    for c in available:
        agg_dict[c] = ["mean", "std", "min", "max"]

    summary = df.groupby(group_by, as_index=False)[available].agg(agg_dict)
    # Flatten multi-level columns
    summary.columns = ["_".join(col).strip("_") if isinstance(col, tuple) else col
                      for col in summary.columns]
    return summary.round(4)


def compute_rq_tables(df: pd.DataFrame) -> dict:
    """Build RQ-specific summary tables."""
    tables = {}

    # RQ1: Throughput + latency
    rq1_cols = ["throughput_mean", "throughput_std",
                "p99_latency_ms_mean", "p99_latency_ms_std"]
    available_rq1 = [c for c in rq1_cols if c in df.columns]
    if available_rq1:
        tables["rq1"] = df[["run_dir", "seed"] + available_rq1].copy()

    # RQ2: Accuracy (no drift vs drift vs late)
    rq2_cols = ["precision_mean", "precision_std",
                "recall_mean", "recall_std",
                "f1_mean", "f1_std"]
    available_rq2 = [c for c in rq2_cols if c in df.columns]
    if available_rq2:
        tables["rq2"] = df[["run_dir", "seed"] + available_rq2].copy()

    # RQ3: Scalability (throughput vs # DC rules)
    rq3_cols = ["throughput_mean", "throughput_std",
                "alerts_total_mean", "alerts_total_std"]
    available_rq3 = [c for c in rq3_cols if c in df.columns]
    if available_rq3:
        tables["rq3"] = df[["run_dir", "seed"] + available_rq3].copy()

    # RQ4: Sensitivity
    rq4_cols = ["p99_latency_ms_mean", "f1_mean", "throughput_mean"]
    available_rq4 = [c for c in rq4_cols if c in df.columns]
    if available_rq4:
        tables["rq4"] = df[["run_dir", "seed"] + available_rq4].copy()

    return tables


def main():
    parser = argparse.ArgumentParser(description="Aggregate benchmark metrics")
    parser.add_argument("--results-dir", type=Path, default=Path("results"))
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    parser.add_argument("--output-dir", type=Path, default=Path("results/aggregate"))
    parser.add_argument("--format", choices=["csv", "json", "both"], default="both")
    args = parser.parse_args()

    if not args.results_dir.exists():
        print(f"Error: {args.results_dir} not found. Run benchmarks first.")
        sys.exit(1)

    df = load_runs(args.results_dir, args.seeds)
    if df.empty:
        print(f"No runs found in {args.results_dir} with seeds {args.seeds}")
        sys.exit(1)

    print(f"Loaded {len(df)} run(s):")
    for _, row in df.iterrows():
        print(f"  {row['run_dir']} seed={row['seed']}: "
              f"throughput={row['throughput']:.1f} ev/s, "
              f"F1={row['f1']:.3f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Per-seed summary
    summary_per_seed = compute_summary(df, group_by=["seed"])
    summary_path = args.output_dir / "summary_per_seed.csv"
    summary_per_seed.to_csv(summary_path, index=False)
    print(f"\nPer-seed summary: {summary_path}")

    # Overall summary (all seeds)
    summary_overall = compute_summary(df, group_by=[])
    overall_path = args.output_dir / "summary_overall.csv"
    summary_overall.to_csv(overall_path, index=False)
    print(f"Overall summary: {overall_path}")

    # RQ tables
    rq_tables = compute_rq_tables(df)
    for rq_name, table in rq_tables.items():
        rq_path = args.output_dir / f"{rq_name}_summary.csv"
        table.to_csv(rq_path, index=False)
        print(f"RQ table ({rq_name}): {rq_path}")

    # JSON export
    if args.format in ("json", "both"):
        json_path = args.output_dir / "metrics_aggregate.json"
        with open(json_path, "w") as f:
            json.dump({
                "seeds": args.seeds,
                "n_runs": len(df),
                "summary_per_seed": summary_per_seed.to_dict(orient="records"),
                "summary_overall": summary_overall.to_dict(orient="records"),
            }, f, indent=2, default=str)
        print(f"JSON export: {json_path}")

    print(f"\nDone. {len(df)} run(s) aggregated.")


if __name__ == "__main__":
    main()
