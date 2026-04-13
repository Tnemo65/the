"""Tiêm vi phạm logic (DC1–DC3) vào clean.parquet, ghi ground truth (B3).

Usage:
    python -m scripts.inject_fraud --input data/clean.parquet --output data/fraud.parquet
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def inject_dc1(df: pd.DataFrame, ratio: float = 0.02, seed: int = 42) -> tuple[pd.DataFrame, list[dict]]:
    """DC1: Fare–Distance Dominance.

    Hai xe cùng quãng đường (±0.5 mile), xe ngắn không fare cao bất thường
    (> fare_xe_dài × 1.5 + $2). Inject bằng cách tăng fare của xe ngắn hơn.
    """
    ground_truth = []
    df = df.copy()
    df["_fare_original"] = df["fare_amount"].copy()

    df_sorted = df.sort_values("trip_distance").reset_index(drop=True)
    n = len(df_sorted)
    n_inject = max(1, int(n * ratio))
    rng = np.random.default_rng(seed)

    injected = set()
    selected_indices = rng.choice(n, size=min(n_inject, n), replace=False)

    for i in selected_indices:
        dist = df_sorted.loc[i, "trip_distance"]

        # Tìm bản ghi cùng quãng đường (±0.5) chưa bị inject
        mask = (
            (abs(df_sorted["trip_distance"] - dist) <= 0.5)
            & (df_sorted.index != i)
            & (~df_sorted.index.isin(injected))
        )
        candidates = df_sorted[mask]
        if candidates.empty:
            continue

        j = candidates.index[0]
        fare_j = df_sorted.loc[j, "fare_amount"]
        # Inject: fare_i > fare_j * 1.5 + 2
        new_fare = fare_j * 1.5 + 2.0
        df_sorted.loc[i, "fare_amount"] = new_fare

        injected.add(i)
        injected.add(j)
        ground_truth.append({
            "event_id": str(df_sorted.loc[i, "event_id"]),
            "noise_type": "dc1",
            "true_label": True,
            "violation_id": f"dc1_{df_sorted.loc[i, 'event_id']}_{df_sorted.loc[j, 'event_id']}",
            "paired_event_id": str(df_sorted.loc[j, "event_id"]),
            "trip_distance_s": float(df_sorted.loc[i, "trip_distance"]),
            "trip_distance_t": float(df_sorted.loc[j, "trip_distance"]),
        })

    # Merge _fare_original back
    idx_map = df_sorted["_fare_original"].index
    df.loc[idx_map, "_fare_original"] = df_sorted["_fare_original"].values

    # Re-apply injected changes to original df
    for gt in ground_truth:
        eid = gt["event_id"].replace("evt_", "")
        mask = df["event_id"] == gt["event_id"]
        if mask.any():
            s_idx = df[mask].index[0]
            df.loc[s_idx, "fare_amount"] = df_sorted.loc[df_sorted["event_id"] == gt["event_id"], "fare_amount"].values[0]

    return df, ground_truth


def inject_dc2(df: pd.DataFrame, ratio: float = 0.01, seed: int = 43) -> tuple[pd.DataFrame, list[dict]]:
    """DC2: Context-Aware Duration Anomaly.

    Cùng tuyến (PULoc == PULoc, DOLoc == DOLoc), duration không chênh quá lớn
    ngoài biên độ ngữ cảnh. Inject bằng cách tăng duration của một số event.
    """
    ground_truth = []
    df = df.copy()
    df["_duration_original"] = df["trip_duration"].copy()

    groups = df.groupby(["PULocationID", "DOLocationID"], sort=False)
    rng = np.random.default_rng(seed)

    for (pu, do), grp in groups:
        if len(grp) < 5:
            continue
        n_inject = max(1, int(len(grp) * ratio))
        median_dur = grp["trip_duration"].median()

        candidates = grp.sample(n=min(n_inject, len(grp)), random_state=rng.integers(0, 2**31))
        for idx in candidates.index:
            new_dur = median_dur * 2.5 + rng.uniform(0, 300)
            df.loc[idx, "trip_duration"] = new_dur
            ground_truth.append({
                "event_id": str(df.loc[idx, "event_id"]),
                "noise_type": "dc2",
                "true_label": True,
                "violation_id": f"dc2_{df.loc[idx, 'event_id']}",
                "paired_group": f"{pu}_{do}",
                "median_duration": float(median_dur),
                "injected_duration": float(new_dur),
            })

    return df, ground_truth


def inject_dc3(df: pd.DataFrame, ratio: float = 0.01, seed: int = 44) -> tuple[pd.DataFrame, list[dict]]:
    """DC3: Toll Route Anomaly.

    Cùng tuyến, tolls không chênh bất thường. Inject bằng cách tăng tolls của một số event.
    """
    ground_truth = []
    df = df.copy()
    df["_toll_original"] = df["tolls_amount"].copy()

    groups = df.groupby(["PULocationID", "DOLocationID"], sort=False)
    rng = np.random.default_rng(seed)

    for (pu, do), grp in groups:
        if len(grp) < 3:
            continue
        n_pairs = max(1, int(len(grp) * ratio))
        total_needed = n_pairs * 2
        if total_needed > len(grp):
            total_needed = len(grp)

        sampled = grp.sample(n=min(total_needed, len(grp)), random_state=rng.integers(0, 2**31))
        mid = len(sampled) // 2

        for i in range(mid):
            s_idx = sampled.index[i]
            t_idx = sampled.index[i + mid]
            toll_t = df.loc[t_idx, "tolls_amount"]
            # Inject: toll_s > toll_t * 3.0 + 2
            df.loc[s_idx, "tolls_amount"] = toll_t * 3.0 + 2.0
            ground_truth.append({
                "event_id": str(df.loc[s_idx, "event_id"]),
                "noise_type": "dc3",
                "true_label": True,
                "violation_id": f"dc3_{df.loc[s_idx, 'event_id']}_{df.loc[t_idx, 'event_id']}",
                "paired_event_id": str(df.loc[t_idx, "event_id"]),
                "route": f"{pu}_{do}",
            })

    return df, ground_truth


def main():
    parser = argparse.ArgumentParser(description="Inject DC1–DC3 violations into clean dataset")
    parser.add_argument("--input", type=Path, default=Path("data/clean.parquet"))
    parser.add_argument("--output", type=Path, default=Path("data/fraud.parquet"))
    parser.add_argument("--ground-truth", type=Path, default=Path("data/ground_truth.jsonl"))
    parser.add_argument("--dc1-ratio", type=float, default=0.02)
    parser.add_argument("--dc2-ratio", type=float, default=0.01)
    parser.add_argument("--dc3-ratio", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found. Run prepare_benchmark.py first.")
        sys.exit(1)

    df = pd.read_parquet(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    all_gt = []

    print(f"DC1 injection (ratio={args.dc1_ratio}, seed={args.seed})...")
    df, gt1 = inject_dc1(df, ratio=args.dc1_ratio, seed=args.seed)
    all_gt.extend(gt1)
    print(f"  Injected {len(gt1)} DC1 violations")

    print(f"DC2 injection (ratio={args.dc2_ratio}, seed={args.seed+1})...")
    df, gt2 = inject_dc2(df, ratio=args.dc2_ratio, seed=args.seed + 1)
    all_gt.extend(gt2)
    print(f"  Injected {len(gt2)} DC2 violations")

    print(f"DC3 injection (ratio={args.dc3_ratio}, seed={args.seed+2})...")
    df, gt3 = inject_dc3(df, ratio=args.dc3_ratio, seed=args.seed + 2)
    all_gt.extend(gt3)
    print(f"  Injected {len(gt3)} DC3 violations")

    # Sort by ingestion_time
    df = df.sort_values("ingestion_time").reset_index(drop=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    print(f"Saved fraud dataset: {args.output} ({len(df)} rows)")

    args.ground_truth.parent.mkdir(parents=True, exist_ok=True)
    with open(args.ground_truth, "w") as f:
        for record in all_gt:
            f.write(json.dumps(record) + "\n")
    print(f"Ground truth: {args.ground_truth} ({len(all_gt)} records)")

    # Summary
    print("\nGround truth summary:")
    for noise_type in ("dc1", "dc2", "dc3"):
        count = sum(1 for r in all_gt if r["noise_type"] == noise_type)
        print(f"  {noise_type}: {count} violations")


if __name__ == "__main__":
    main()
