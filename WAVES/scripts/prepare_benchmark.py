"""Download và chuẩn bị NYC Taxi dataset sạch cho benchmark (B1).

Usage:
    python -m scripts.prepare_benchmark --year 2024 --months 1 2 3
    python -m scripts.prepare_benchmark --download-only
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

NYC_TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def download_month(year: int, month: int, output_dir: Path) -> Path:
    url = f"{NYC_TLC_BASE}/yellow_tripdata_{year}-{month:02d}.parquet"
    out_path = output_dir / f"yellow_tripdata_{year}-{month:02d}.parquet"
    if out_path.exists():
        print(f"  Already exists: {out_path.name} — skipping download")
        return out_path
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=f"{year}-{month:02d}") as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))
    return out_path


def clean_and_prepare(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    original_count = len(df)

    # 1. Filter: fare_amount > 0, trip_distance > 0
    df = df[df["fare_amount"] > 0].copy()
    df = df[df["trip_distance"] > 0].copy()

    # 2. Parse datetime
    df["event_time"] = pd.to_datetime(df["tpep_pickup_datetime"], utc=True)
    df["dropoff_time"] = pd.to_datetime(df["tpep_dropoff_datetime"], utc=True)

    # 3. Compute trip_duration (seconds)
    df["trip_duration"] = (df["dropoff_time"] - df["event_time"]).dt.total_seconds()

    # 4. Filter: 60s <= duration <= 10800s (3h)
    df = df[(df["trip_duration"] >= 60) & (df["trip_duration"] <= 10800)].copy()

    # 5. Filter: RatecodeID valid (1-99)
    if "RatecodeID" in df.columns:
        df = df[df["RatecodeID"].isin(range(1, 100))].copy()

    # 6. Generate event_id (stable across months using original index)
    df = df.reset_index(drop=True)
    df["event_id"] = [f"evt_{i:08d}" for i in range(len(df))]

    # 7. ingestion_time = event_time + small random jitter (0-10s) để simulate streaming
    rng = np.random.default_rng(seed)
    jitter = rng.uniform(0, 10, size=len(df))
    df["ingestion_time"] = df["event_time"] + pd.to_timedelta(jitter, unit="s")

    # 8. Sort by ingestion_time
    df = df.sort_values("ingestion_time").reset_index(drop=True)

    # 9. Keep only needed columns
    keep_cols = [
        "event_id",
        "event_time",
        "ingestion_time",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "tolls_amount",
        "trip_duration",
    ]
    available = [c for c in keep_cols if c in df.columns]
    df = df[available].copy()

    filtered_count = len(df)
    print(f"  Filtered: {filtered_count}/{original_count} rows retained ({filtered_count/original_count*100:.1f}%)")
    return df


def main():
    parser = argparse.ArgumentParser(description="Prepare NYC Taxi benchmark dataset")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--months", type=int, nargs="+", default=[1])
    parser.add_argument("--output", type=Path, default=Path("data/clean.parquet"))
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    raw_dir: Path = args.raw_dir
    dfs_clean = []

    print(f"Downloading {len(args.months)} month(s) from NYC TLC...")
    for month in args.months:
        path = download_month(args.year, month, raw_dir)
        print(f"  Cleaning {path.name}...")
        df = pd.read_parquet(path)
        if not args.download_only:
            df_clean = clean_and_prepare(df, seed=args.seed + month)
            dfs_clean.append(df_clean)

    if args.download_only:
        print("Download complete. Skipping cleaning (--download-only).")
        return

    if not dfs_clean:
        print("No data to combine. Check --months and --download-only flag.")
        sys.exit(1)

    print("Combining months...")
    combined = pd.concat(dfs_clean, ignore_index=True)
    combined = combined.sort_values("ingestion_time").reset_index(drop=True)

    # Re-assign event_ids sequentially after combine
    combined["event_id"] = [f"evt_{i:08d}" for i in range(len(combined))]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(args.output, index=False)
    print(f"Saved: {args.output} ({len(combined)} rows)")


if __name__ == "__main__":
    main()
