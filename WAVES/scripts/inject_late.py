"""Tiêm late data và out-of-order events vào drift.parquet (B4).

Usage:
    python -m scripts.inject_late --input data/drift.parquet
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd


def inject_late_and_ooo(
    df: pd.DataFrame,
    late_ratio: float = 0.10,
    min_lateness: float = 120.0,
    max_lateness: float = 300.0,
    oo_ratio: float = 0.05,
    seed: int = 47,
) -> Tuple[pd.DataFrame, list[dict]]:
    """Inject late data và out-of-order events.

    Late: thêm lateness (uniform) vào ingestion_time của late_ratio% events.
    OoO: đảo ngẫu nhiên ingestion_time của một cửa sổ nhỏ (oo_ratio%).
    """
    ground_truth = []
    df = df.copy()
    rng = np.random.default_rng(seed)

    # --- Late data ---
    n_late = int(len(df) * late_ratio)
    if n_late > 0:
        late_indices = rng.choice(df.index, size=n_late, replace=False)
        lateness = rng.uniform(min_lateness, max_lateness, size=n_late)

        for idx, lat in zip(late_indices, lateness):
            df.loc[idx, "ingestion_time"] = df.loc[idx, "ingestion_time"] + pd.Timedelta(seconds=lat)
            ground_truth.append({
                "event_id": str(df.loc[idx, "event_id"]),
                "noise_type": "late",
                "true_label": True,
                "lateness_seconds": float(lat),
            })
        print(f"  Late: {n_late} events ({late_ratio*100:.0f}%), "
              f"lateness in [{min_lateness:.0f}, {max_lateness:.0f}]s")

    # --- Out-of-order ---
    n_ooo = int(len(df) * oo_ratio)
    if n_ooo > 0 and n_ooo < len(df):
        ooo_window_start = rng.integers(0, len(df) - n_ooo)
        ooo_indices = list(range(ooo_window_start, ooo_window_start + n_ooo))

        ooo_times = df.loc[ooo_indices, "ingestion_time"].values.copy()
        rng.shuffle(ooo_times)
        df.loc[ooo_indices, "ingestion_time"] = ooo_times
        print(f"  OoO: {n_ooo} events ({oo_ratio*100:.0f}%), "
              f"shuffled window starting at index {ooo_window_start}")

    # Sort lại theo ingestion_time (tạo OoO pattern thực sự)
    df = df.sort_values("ingestion_time").reset_index(drop=True)

    return df, ground_truth


def main():
    parser = argparse.ArgumentParser(description="Inject late data and out-of-order events")
    parser.add_argument("--input", type=Path, default=Path("data/drift.parquet"))
    parser.add_argument("--output", type=Path, default=Path("data/benchmark.parquet"))
    parser.add_argument("--ground-truth", type=Path, default=Path("data/ground_truth.jsonl"))
    parser.add_argument("--late-ratio", type=float, default=0.10)
    parser.add_argument("--oo-ratio", type=float, default=0.05)
    parser.add_argument("--min-lateness", type=float, default=120.0)
    parser.add_argument("--max-lateness", type=float, default=300.0)
    parser.add_argument("--seed", type=int, default=47)
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found. Run inject_drift.py first.")
        sys.exit(1)

    df = pd.read_parquet(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    # Load existing ground truth (DC1/2/3 violations)
    ground_truth = []
    if args.ground_truth.exists():
        with open(args.ground_truth) as f:
            for line in f:
                line = line.strip()
                if line:
                    ground_truth.append(json.loads(line))
        print(f"  Loaded {len(ground_truth)} existing ground truth records")

    # Inject late + OoO
    df, late_gt = inject_late_and_ooo(
        df,
        late_ratio=args.late_ratio,
        min_lateness=args.min_lateness,
        max_lateness=args.max_lateness,
        oo_ratio=args.oo_ratio,
        seed=args.seed,
    )
    ground_truth.extend(late_gt)
    print(f"  Late/OoO injected: {len(late_gt)} late records added")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    print(f"Benchmark dataset: {args.output} ({len(df)} rows)")

    args.ground_truth.parent.mkdir(parents=True, exist_ok=True)
    with open(args.ground_truth, "w") as f:
        for record in ground_truth:
            f.write(json.dumps(record) + "\n")
    print(f"Ground truth: {args.ground_truth} ({len(ground_truth)} total records)")

    # Summary
    gt_summary = {}
    for r in ground_truth:
        gt_summary[r["noise_type"]] = gt_summary.get(r["noise_type"], 0) + 1
    print("\nGround truth summary:")
    for noise_type, count in sorted(gt_summary.items()):
        print(f"  {noise_type}: {count}")


if __name__ == "__main__":
    main()
