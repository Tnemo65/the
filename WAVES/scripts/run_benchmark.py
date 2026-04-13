"""Run WAVES benchmark — đo throughput, latency, accuracy.

Usage:
    python -m scripts.run_benchmark --benchmark data/benchmark.parquet --seed 42
    python -m scripts.run_benchmark --benchmark data/benchmark.parquet --seed 42 --warmup 1000
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from waves import DataEvent, Schema
from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import parse_and_normalize, ParseError
from waves.output import AlertEvent, AlertOutput


NYC_SCHEMA = Schema(
    event_time_field="event_time",
    event_time_format=None,  # let parse_and_normalize handle ISO format
    ingestion_time_field="ingestion_time",
    ingestion_time_format=None,
    required_fields=["trip_distance", "fare_amount"],
    field_types={
        "PULocationID": int,
        "DOLocationID": int,
        "trip_distance": float,
        "fare_amount": float,
        "tolls_amount": float,
        "trip_duration": float,
        "total_amount": float,
    },
)


def load_dc_rules(path: Path) -> list:
    """Load DC rules from JSON file → list of raw rule dicts."""
    with open(path) as f:
        raw = json.load(f)
    return raw["dc_rules"]


def compute_accuracy(alerts: list[AlertEvent], ground_truth: dict) -> dict:
    """So sánh alerts với ground truth để tính precision/recall/F1.

    Mapping: alert.event_id → ground_truth[event_id] (violation_id).
    Retraction alerts không tính vào TP/FP — chúng chỉ confirm rằng provisional là FP.
    """
    gt_violation_ids = set()
    gt_event_ids = set()
    for eid, rec in ground_truth.items():
        if rec.get("noise_type") in ("dc1", "dc2", "dc3"):
            gt_violation_ids.add(rec["violation_id"])
            gt_event_ids.add(eid)

    # Detected: alerts có event_id nằm trong ground truth (không tính late)
    detected_ids = set()
    for alert in alerts:
        if alert.event_type == "retraction":
            continue
        detected_ids.add(alert.event_id)

    tp = len(gt_event_ids & detected_ids)
    fp = len(detected_ids - gt_event_ids)
    fn = len(gt_event_ids - detected_ids)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "tp": tp,
        "fp": fp,
        "fn": fn,
    }


def run_benchmark(
    benchmark_path: Path,
    dc_rules_path: Path,
    seed: int,
    warmup: int = 1000,
    config_override: dict | None = None,
) -> dict:
    """Chạy WAVES pipeline trên benchmark dataset, thu metrics."""
    cfg_dict = config_override or {}
    cfg = PipelineConfig(
        window_width_seconds=cfg_dict.get("window_width_seconds", 600.0),
        slide_step_seconds=cfg_dict.get("slide_step_seconds", 120.0),
        pane_size_seconds=cfg_dict.get("pane_size_seconds", 60.0),
        wait_for_late_seconds=cfg_dict.get("wait_for_late_seconds", 300.0),
        alert_ttl_seconds=cfg_dict.get("alert_ttl_seconds", 3600.0),
        k_max=cfg_dict.get("k_max", 5),
    )

    pipeline = WavePipeline(cfg)
    dc_rules = load_dc_rules(dc_rules_path)
    pipeline.load_dc_rules(dc_rules)

    # Collectors
    alerts: list[AlertEvent] = []
    meta_events = []

    def alert_sink(e: AlertEvent):
        alerts.append(e)

    def meta_sink(e):
        meta_events.append(e)

    # Swap output with new sink callbacks
    pipeline._output = AlertOutput(alert_sink=alert_sink, meta_sink=meta_sink)

    # Ground truth
    gt_path = benchmark_path.parent / "ground_truth.jsonl"
    ground_truth: dict = {}
    if gt_path.exists():
        with open(gt_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                ground_truth[rec["event_id"]] = rec

    # Load dataset
    df = pd.read_parquet(benchmark_path)
    n_rows = len(df)
    print(f"Loaded {n_rows} events from {benchmark_path}")
    print(f"  DC rules: {dc_rules_path}")
    print(f"  Ground truth: {len(ground_truth)} records")

    # Process
    latencies: list[float] = []
    warmup_remaining = warmup
    n_warmed = 0

    start_wall = time.perf_counter()
    last_progress = 0

    for row_idx, row in df.iterrows():
        raw = row.to_dict()

        # Skip internal columns
        if raw.get("noise_type") is not None and raw.get("noise_type") != "":
            pass  # keep for processing

        t0 = time.perf_counter()
        try:
            event = parse_and_normalize(raw, NYC_SCHEMA)
        except ParseError:
            continue
        pipeline.process(event)
        t1 = time.perf_counter()

        latencies.append((t1 - t0) * 1000)

        # Progress
        progress = int((row_idx + 1) / n_rows * 20)
        if progress > last_progress:
            print(f"\r  [{('=' * progress)}{(' ' * (20 - progress))}] {(row_idx + 1):,}/{n_rows:,}", end="", flush=True)
            last_progress = progress

    print()  # newline after progress bar
    end_wall = time.perf_counter()
    elapsed = end_wall - start_wall

    # Finalize
    n_cleaned = pipeline.cleanup()

    # Metrics
    latencies_sorted = sorted(latencies)
    p50_idx = int(len(latencies_sorted) * 0.50)
    p95_idx = int(len(latencies_sorted) * 0.95)
    p99_idx = int(len(latencies_sorted) * 0.99)

    metrics = {
        "config": {
            "benchmark": str(benchmark_path),
            "seed": seed,
            "total_events": n_rows,
            "warmup": warmup,
            "elapsed_seconds": round(elapsed, 3),
            "pipeline_config": cfg_dict,
        },
        "system": {
            "throughput_events_per_sec": round(n_rows / elapsed, 2) if elapsed > 0 else 0,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 3) if latencies else 0,
            "p50_latency_ms": round(latencies_sorted[p50_idx], 3) if latencies_sorted else 0,
            "p95_latency_ms": round(latencies_sorted[p95_idx], 3) if latencies_sorted else 0,
            "p99_latency_ms": round(latencies_sorted[p99_idx], 3) if latencies_sorted else 0,
        },
        "alerts": {
            "total": len(alerts),
            "provisional": sum(1 for a in alerts if a.event_type == "provisional"),
            "final": sum(1 for a in alerts if a.event_type == "final"),
            "retraction": sum(1 for a in alerts if a.event_type == "retraction"),
            "by_dc": _alerts_by_dc(alerts),
        },
        "accuracy": compute_accuracy(alerts, ground_truth),
        "meta_events": len(meta_events),
        "cleaned_alerts": n_cleaned,
    }

    return metrics


def _alerts_by_dc(alerts: list[AlertEvent]) -> dict:
    counts = {}
    for a in alerts:
        if a.event_type == "retraction":
            continue
        counts[a.dc_id] = counts.get(a.dc_id, 0) + 1
    return counts


def main():
    parser = argparse.ArgumentParser(description="Run WAVES benchmark")
    parser.add_argument("--benchmark", type=Path, default=Path("data/benchmark.parquet"))
    parser.add_argument("--dc-rules", type=Path, default=Path("configs/dc_rules.json"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=Path("results/metrics.json"))
    parser.add_argument("--run-dir", type=Path, default=None,
                        help="Output directory for this run (overrides --output)")
    parser.add_argument(
        "--window-width", type=float, default=None,
        help="Override window_width_seconds"
    )
    parser.add_argument(
        "--slide-step", type=float, default=None,
        help="Override slide_step_seconds"
    )
    parser.add_argument(
        "--pane-size", type=float, default=None,
        help="Override pane_size_seconds"
    )
    parser.add_argument(
        "--k-max", type=int, default=None,
        help="Override k_max for optimizer"
    )
    args = parser.parse_args()

    if not args.benchmark.exists():
        print(f"Error: {args.benchmark} not found. Run prepare_benchmark → inject_fraud → inject_drift → inject_late first.")
        sys.exit(1)

    cfg_override = {}
    if args.window_width is not None:
        cfg_override["window_width_seconds"] = args.window_width
    if args.slide_step is not None:
        cfg_override["slide_step_seconds"] = args.slide_step
    if args.pane_size is not None:
        cfg_override["pane_size_seconds"] = args.pane_size
    if args.k_max is not None:
        cfg_override["k_max"] = args.k_max

    print(f"[WAVES Benchmark] seed={args.seed}, warmup={args.warmup}")
    print(f"  benchmark : {args.benchmark}")
    print(f"  dc_rules  : {args.dc_rules}")

    metrics = run_benchmark(
        args.benchmark,
        args.dc_rules,
        seed=args.seed,
        warmup=args.warmup,
        config_override=cfg_override if cfg_override else None,
    )

    # Save
    if args.run_dir is not None:
        run_dir = args.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / "metrics.json"
        config_path = run_dir / "config.json"
        with open(config_path, "w") as f:
            json.dump({
                "benchmark": str(args.benchmark),
                "dc_rules": str(args.dc_rules),
                "seed": args.seed,
                "warmup": args.warmup,
                "pipeline_config": metrics["config"]["pipeline_config"],
            }, f, indent=2)
    else:
        output_path = args.output
        output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print(f"\nMetrics saved: {output_path}")
    print(f"  Throughput   : {metrics['system']['throughput_events_per_sec']:.1f} events/s")
    print(f"  P99 Latency  : {metrics['system']['p99_latency_ms']:.1f} ms")
    print(f"  P95 Latency  : {metrics['system']['p95_latency_ms']:.1f} ms")
    print(f"  Avg Latency  : {metrics['system']['avg_latency_ms']:.1f} ms")
    print(f"  Alerts       : {metrics['alerts']['total']} "
          f"(prov={metrics['alerts']['provisional']}, "
          f"final={metrics['alerts']['final']}, "
          f"retract={metrics['alerts']['retraction']})")
    print(f"  Accuracy     : P={metrics['accuracy']['precision']:.3f} "
          f"R={metrics['accuracy']['recall']:.3f} "
          f"F1={metrics['accuracy']['f1']:.3f} "
          f"(TP={metrics['accuracy']['tp']}, FP={metrics['accuracy']['fp']}, FN={metrics['accuracy']['fn']})")


if __name__ == "__main__":
    main()
