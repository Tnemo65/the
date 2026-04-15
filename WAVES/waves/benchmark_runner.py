"""Fast WAVES benchmark runner — uses itertuples() + batched processing.

Usage:
    python -m waves.benchmark_runner --benchmark data/benchmark.parquet --seed 42
"""
# MUST import patch FIRST before any other waves import
import waves._benchmark_patch  # noqa: F401 — patches event_id preservation

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from waves import DataEvent, Schema
from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import parse_and_normalize, ParseError
from waves.output import AlertEvent, AlertOutput


NYC_SCHEMA = Schema(
    event_time_field="event_time",
    event_time_format=None,
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


def load_dc_rules(path: Path) -> List[Dict]:
    with open(path) as f:
        raw = json.load(f)
    return raw["dc_rules"]


def compute_accuracy(
    alerts: List[AlertEvent],
    ground_truth: Dict,
    all_gt_violation_ids: set,
) -> Dict[str, Any]:
    """Compute precision/recall/F1 using window-aware event_id mapping.

    A detected alert is a TP if the alert's event_id is in the ground truth
    DC violations set. Since we seal all windows at end (force-finalize),
    we count all PROVISIONAL + FINAL alerts (not retractions).
    """
    detected_ids = set()
    for alert in alerts:
        if alert.event_type == "retraction":
            continue
        detected_ids.add(alert.event_id)

    gt_ids = all_gt_violation_ids
    tp = len(gt_ids & detected_ids)
    fp = len(detected_ids - gt_ids)
    fn = len(gt_ids - detected_ids)

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
        "gt_violation_events": len(gt_ids),
        "detected_events": len(detected_ids),
    }


def run_benchmark(
    benchmark_path: Path,
    dc_rules_path: Path,
    seed: int,
    warmup: int = 1000,
    max_events: Optional[int] = None,
    config_override: Optional[Dict] = None,
) -> Dict[str, Any]:
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

    alerts = []
    meta_events = []

    def alert_sink(e: AlertEvent):
        alerts.append(e)

    def meta_sink(e):
        meta_events.append(e)

    pipeline._output = AlertOutput(alert_sink=alert_sink, meta_sink=meta_sink)

    # Ground truth
    gt_path = benchmark_path.parent / "ground_truth.jsonl"
    ground_truth = {}
    all_gt_violation_ids = set()
    if gt_path.exists():
        with open(gt_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("noise_type") in ("dc1", "dc2", "dc3"):
                    ground_truth[rec["event_id"]] = rec
                    all_gt_violation_ids.add(rec["event_id"])

    # Load dataset
    df = pd.read_parquet(benchmark_path)
    n_rows = min(len(df), max_events) if max_events else len(df)
    df_subset = df.head(n_rows)
    print("Loaded {:,} events from {}".format(n_rows, benchmark_path))
    print("  DC rules: {}".format(dc_rules_path))
    print("  Ground truth: {:,} records, {:,} violation event_ids".format(
        len(ground_truth), len(all_gt_violation_ids)))

    latencies = []
    processed = 0

    start_wall = time.perf_counter()
    last_progress = 0

    for row_idx, row in enumerate(df_subset.itertuples(index=False)):
        raw = row._asdict()

        t0 = time.perf_counter()
        try:
            event = parse_and_normalize(raw, NYC_SCHEMA)
        except ParseError:
            continue
        pipeline.process(event)
        t1 = time.perf_counter()

        latencies.append((t1 - t0) * 1000)
        processed += 1

        progress = int((row_idx + 1) / n_rows * 20)
        if progress > last_progress:
            elapsed = time.perf_counter() - start_wall
            rate = processed / elapsed if elapsed > 0 else 0
            print("\r  [{}{}] {:,}/{:,} ({:.0f} ev/s)".format(
                "=" * progress, " " * (20 - progress),
                row_idx + 1, n_rows, rate), end="", flush=True)
            last_progress = progress

    print()
    end_wall = time.perf_counter()
    elapsed = end_wall - start_wall

    # Seal all remaining windows (force-finalize PROVISIONAL alerts)
    sealed = pipeline._seal_all_windows()
    print("  Sealed {} remaining windows".format(sealed))

    # Finalize cleanup
    n_cleaned = pipeline.cleanup()

    # Metrics
    latencies_sorted = sorted(latencies)
    p50_idx = int(len(latencies_sorted) * 0.50)
    p95_idx = int(len(latencies_sorted) * 0.95)
    p99_idx = int(len(latencies_sorted) * 0.99)

    alerts_prov = sum(1 for a in alerts if a.event_type == "provisional")
    alerts_final = sum(1 for a in alerts if a.event_type == "final")
    alerts_retract = sum(1 for a in alerts if a.event_type == "retraction")
    by_dc = {}
    for a in alerts:
        if a.event_type == "retraction":
            continue
        by_dc[a.dc_id] = by_dc.get(a.dc_id, 0) + 1

    accuracy = compute_accuracy(alerts, ground_truth, all_gt_violation_ids)

    metrics = {
        "config": {
            "benchmark": str(benchmark_path),
            "seed": seed,
            "total_events": n_rows,
            "processed_events": processed,
            "warmup": warmup,
            "elapsed_seconds": round(elapsed, 3),
            "pipeline_config": cfg_dict,
        },
        "system": {
            "throughput_events_per_sec": round(processed / elapsed, 2) if elapsed > 0 else 0,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 3) if latencies else 0,
            "p50_latency_ms": round(latencies_sorted[p50_idx], 3) if latencies_sorted else 0,
            "p95_latency_ms": round(latencies_sorted[p95_idx], 3) if latencies_sorted else 0,
            "p99_latency_ms": round(latencies_sorted[p99_idx], 3) if latencies_sorted else 0,
        },
        "alerts": {
            "total": len(alerts),
            "provisional": alerts_prov,
            "final": alerts_final,
            "retraction": alerts_retract,
            "by_dc": by_dc,
        },
        "accuracy": accuracy,
        "meta_events": len(meta_events),
        "sealed_windows": sealed,
        "cleaned_alerts": n_cleaned,
    }

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Run WAVES benchmark (fast version)")
    parser.add_argument("--benchmark", type=Path, default=Path("data/benchmark.parquet"))
    parser.add_argument("--dc-rules", type=Path, default=Path("configs/dc_rules.json"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--max-events", type=int, default=None,
                        help="Limit events for quick test")
    parser.add_argument("--output", type=Path, default=Path("results/fast_run/metrics.json"))
    parser.add_argument("--window-width", type=float, default=None)
    parser.add_argument("--slide-step", type=float, default=None)
    parser.add_argument("--pane-size", type=float, default=None)
    parser.add_argument("--k-max", type=int, default=None)
    args = parser.parse_args()

    if not args.benchmark.exists():
        print("Error: {} not found. Run prepare_benchmark first.".format(args.benchmark))
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

    print("[WAVES Fast Benchmark] seed={}, warmup={}, max_events={}".format(
        args.seed, args.warmup, args.max_events))
    print("  benchmark : {}".format(args.benchmark))
    print("  dc_rules  : {}".format(args.dc_rules))

    metrics = run_benchmark(
        args.benchmark,
        args.dc_rules,
        seed=args.seed,
        warmup=args.warmup,
        max_events=args.max_events,
        config_override=cfg_override if cfg_override else None,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print("\nMetrics saved: {}".format(args.output))
    print("  Throughput   : {:.1f} events/s".format(metrics["system"]["throughput_events_per_sec"]))
    print("  P99 Latency  : {:.1f} ms".format(metrics["system"]["p99_latency_ms"]))
    print("  Alerts Total : {}".format(metrics["alerts"]["total"]))
    print("  Alerts Final : {}".format(metrics["alerts"]["final"]))
    print("  Alerts Prov.  : {}".format(metrics["alerts"]["provisional"]))
    print("  Precision     : {:.4f}".format(metrics["accuracy"]["precision"]))
    print("  Recall        : {:.4f}".format(metrics["accuracy"]["recall"]))
    print("  F1            : {:.4f}".format(metrics["accuracy"]["f1"]))
    print("  TP/FP/FN     : {}/{}/{}".format(
        metrics["accuracy"]["tp"], metrics["accuracy"]["fp"], metrics["accuracy"]["fn"]))
    return metrics


if __name__ == "__main__":
    main()
