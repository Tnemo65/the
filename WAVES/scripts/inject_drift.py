"""Tiêm concept drift vào fraud.parquet, ghi drift metadata (B2).

Usage:
    python -m scripts.inject_drift --input data/fraud.parquet --output data/drift.parquet
    python -m scripts.inject_drift --drift-type both --duration-hours 120
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def inject_sudden_drift(df: pd.DataFrame, drift_hour: int,
                        multiplier: float = 2.0) -> pd.DataFrame:
    """Tăng trip_duration lên multiplier× trong một giờ cụ thể (sudden drift)."""
    df = df.copy()
    df["_duration_pre_drift"] = df["trip_duration"].copy()
    mask = df["event_time"].dt.hour == drift_hour
    affected = mask.sum()
    df.loc[mask, "trip_duration"] = df.loc[mask, "trip_duration"] * multiplier
    return df, {"type": "sudden", "hour": drift_hour, "multiplier": multiplier, "affected": int(affected)}


def inject_incremental_drift(df: pd.DataFrame, duration_hours: int = 60,
                              rate: float = 0.005, start_hour: int = 0) -> pd.DataFrame:
    """Tăng dần trip_duration theo rate mỗi phút trong duration_hours (incremental drift)."""
    df = df.copy()
    df["_duration_pre_drift"] = df["trip_duration"].copy()

    start_time = df["event_time"].min()
    end_time = start_time + pd.Timedelta(hours=duration_hours)
    mask = df["event_time"] < end_time
    affected = mask.sum()

    elapsed_minutes = (df.loc[mask, "event_time"] - start_time).dt.total_seconds() / 60.0
    df.loc[mask, "trip_duration"] = df.loc[mask, "trip_duration"] * (1.0 + rate * elapsed_minutes)

    return df, {"type": "incremental", "duration_hours": duration_hours,
                "rate": rate, "start_hour": start_hour, "affected": int(affected)}


def main():
    parser = argparse.ArgumentParser(description="Inject concept drift into fraud dataset")
    parser.add_argument("--input", type=Path, default=Path("data/fraud.parquet"))
    parser.add_argument("--output", type=Path, default=Path("data/drift.parquet"))
    parser.add_argument("--metadata", type=Path, default=Path("data/drift_metadata.json"))
    parser.add_argument("--drift-type", choices=["sudden", "incremental", "both"],
                        default="sudden")
    parser.add_argument("--drift-hour", type=int, default=18,
                        help="Hour of day for sudden drift (0-23)")
    parser.add_argument("--multiplier", type=float, default=2.0,
                        help="Duration multiplier for sudden drift")
    parser.add_argument("--duration-hours", type=int, default=60,
                        help="Duration of incremental drift window")
    parser.add_argument("--rate", type=float, default=0.005,
                        help="Rate per minute for incremental drift")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found. Run inject_fraud.py first.")
        sys.exit(1)

    df = pd.read_parquet(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    metadata = {"source": str(args.input), "drifts": []}

    if args.drift_type in ("sudden", "both"):
        print(f"Sudden drift: hour={args.drift_hour}, multiplier={args.multiplier}")
        df, drift_meta = inject_sudden_drift(df, args.drift_hour, args.multiplier)
        metadata["drifts"].append(drift_meta)
        print(f"  Affected events: {drift_meta['affected']}")

    if args.drift_type in ("incremental", "both"):
        print(f"Incremental drift: duration={args.duration_hours}h, rate={args.rate}/min")
        df, drift_meta = inject_incremental_drift(df, args.duration_hours, args.rate)
        metadata["drifts"].append(drift_meta)
        print(f"  Affected events: {drift_meta['affected']}")

    df = df.sort_values("ingestion_time").reset_index(drop=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    print(f"Saved drift dataset: {args.output} ({len(df)} rows)")

    args.metadata.parent.mkdir(parents=True, exist_ok=True)
    with open(args.metadata, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Drift metadata: {args.metadata}")


if __name__ == "__main__":
    main()
