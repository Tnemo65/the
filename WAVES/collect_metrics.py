"""Aggregate metrics từ nhiều experiment runs → summary CSV.

Supports two directory structures:
  - Flat:  results/run_*/metrics.json         (run_benchmark.py)
  - Nested: results/run_*/baseline/metrics.json (run_ablation.py)

Usage:
    python collect_metrics.py --results-dir results/full --seeds 42 43 44
"""
import argparse
import json
import sys
from pathlib import Path
import pandas as pd


def load_runs(results_dir: Path, seeds: list[int]) -> pd.DataFrame:
    records = []
    for seed in seeds:
        for run_dir in sorted(results_dir.glob("run_*")):
            if not run_dir.is_dir():
                continue
            if f"_seed{seed}" not in run_dir.name:
                continue

            # Try nested: run_dir/baseline/metrics.json
            nested_found = False
            for baseline_dir in run_dir.iterdir():
                if not baseline_dir.is_dir():
                    continue
                metrics_path = baseline_dir / "metrics.json"
                if not metrics_path.exists():
                    continue
                nested_found = True
                with open(metrics_path) as f:
                    m = json.load(f)
                _append_record(records, run_dir.name, baseline_dir.name, seed, m)

            if not nested_found:
                # Fallback flat: run_dir/metrics.json
                metrics_path = run_dir / "metrics.json"
                if metrics_path.exists():
                    with open(metrics_path) as f:
                        m = json.load(f)
                    _append_record(records, run_dir.name, m.get("config", {}).get("baseline", "unknown"),
                                   seed, m)

    return pd.DataFrame(records)


def _append_record(records, run_dir, baseline, seed, m: dict):
    cfg = m.get("config", {})
    sys_m = m.get("system", {})
    acc = m.get("accuracy", {})
    alr = m.get("alerts", {})
    records.append({
        "run_dir": run_dir,
        "baseline": baseline,
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
        "gt_in_window": acc.get("gt_in_window", 0),
        "alerts_total": alr.get("total", 0),
        "alerts_provisional": alr.get("provisional", 0),
        "alerts_final": alr.get("final", 0),
        "alerts_retraction": alr.get("retraction", 0),
        "alerts_by_dc": alr.get("by_dc", {}),
    })


def compute_summary(df: pd.DataFrame, group_by: str | list[str]) -> pd.DataFrame:
    numeric_cols = [
        "throughput", "avg_latency_ms", "p50_latency_ms",
        "p95_latency_ms", "p99_latency_ms",
        "precision", "recall", "f1",
        "tp", "fp", "fn", "alerts_total",
        "alerts_provisional", "alerts_final", "alerts_retraction",
    ]
    available = [c for c in numeric_cols if c in df.columns]
    agg_dict = {c: ["mean", "std", "min", "max"] for c in available}
    summary = df.groupby(group_by, as_index=False)[available].agg(agg_dict)
    summary.columns = ["_".join(col).strip("_") if isinstance(col, tuple) else col
                      for col in summary.columns]
    return summary.round(4)


def main():
    parser = argparse.ArgumentParser(description="Aggregate benchmark metrics")
    parser.add_argument("--results-dir", type=Path, default=Path("results"))
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    parser.add_argument("--output-dir", type=Path, default=Path("results/aggregate"))
    parser.add_argument("--baselines", type=str, nargs="+",
                        default=["waves_full", "nl_stream", "single_tree",
                                  "static_box", "waves_single_rule", "buffer_wait"])
    args = parser.parse_args()

    df = load_runs(args.results_dir, args.seeds)
    if df.empty:
        print(f"No runs found. Check --results-dir ({args.results_dir}) and --seeds.")
        sys.exit(1)

    print(f"\nLoaded {len(df)} run(s):")
    if "baseline" in df.columns:
        for _, row in df.iterrows():
            print(f"  {row['baseline']:<22} seed={row['seed']}: "
                  f"tp={row['throughput']:.0f} ev/s, F1={row['f1']:.4f}, "
                  f"P={row['precision']:.4f} R={row['recall']:.4f}")
    else:
        for _, row in df.iterrows():
            print(f"  {row['run_dir']} seed={row['seed']}: "
                  f"tp={row['throughput']:.0f} ev/s, F1={row['f1']:.4f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Summary by seed × baseline
    if "baseline" in df.columns:
        summary = compute_summary(df, group_by=["baseline", "seed"])
        summary.to_csv(args.output_dir / "summary_by_baseline_seed.csv", index=False)
        print(f"\nBy baseline×seed: {args.output_dir / 'summary_by_baseline_seed.csv'}")

        # Overall by baseline (avg across seeds)
        overall = compute_summary(df, group_by=["baseline"])
        overall.to_csv(args.output_dir / "summary_by_baseline.csv", index=False)
        print(f"By baseline:        {args.output_dir / 'summary_by_baseline.csv'}")

        # RQ tables
        _write_rq_tables(df, args.output_dir)
    else:
        summary = compute_summary(df, group_by=[])
        summary.to_csv(args.output_dir / "summary.csv", index=False)

    # JSON export
    json_path = args.output_dir / "metrics_aggregate.json"
    with open(json_path, "w") as f:
        json.dump({
            "seeds": args.seeds,
            "n_runs": len(df),
            "records": df.to_dict(orient="records"),
        }, f, indent=2, default=str)
    print(f"JSON:                {json_path}")
    print(f"\nDone. {len(df)} run(s) aggregated → {args.output_dir}/")


def _write_rq_tables(df: pd.DataFrame, out_dir: Path):
    # RQ1: Throughput vs NL-Stream baseline
    rq1_cols = ["baseline", "throughput_mean", "throughput_std",
                 "p99_latency_ms_mean", "p99_latency_ms_std"]
    avail = [c for c in rq1_cols if c in df.columns]
    if avail:
        rq1 = df[avail].groupby("baseline", as_index=False).first()
        rq1.to_csv(out_dir / "rq1_system.csv", index=False)
        print(f"RQ1 table:           {out_dir / 'rq1_system.csv'}")

    # RQ2: Accuracy (F1) per baseline
    rq2_cols = ["baseline", "precision_mean", "precision_std",
                 "recall_mean", "recall_std", "f1_mean", "f1_std"]
    avail2 = [c for c in rq2_cols if c in df.columns]
    if avail2:
        rq2 = df[avail2].groupby("baseline", as_index=False).first()
        rq2.to_csv(out_dir / "rq2_accuracy.csv", index=False)
        print(f"RQ2 table:           {out_dir / 'rq2_accuracy.csv'}")

    # RQ3: Alerts per DC
    rq3_rows = []
    for _, row in df.iterrows():
        by_dc = row.get("alerts_by_dc", {})
        rq3_rows.append({"baseline": row["baseline"], **by_dc})
    if rq3_rows:
        rq3 = pd.DataFrame(rq3_rows).groupby("baseline", as_index=False).mean(numeric_only=True)
        rq3.to_csv(out_dir / "rq3_alerts_by_dc.csv", index=False)
        print(f"RQ3 table:           {out_dir / 'rq3_alerts_by_dc.csv'}")


if __name__ == "__main__":
    main()
