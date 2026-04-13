"""Run ablation baselines — WAVES-Full + 5 variant configurations.

Each baseline is a runtime-patched WavePipeline.
No modification to waves/ or scripts/ source needed.

Baselines:
  nl_stream         — O(N²) nested loop, bypasses KD-Tree entirely
  single_tree       — pane_size = window_width → 1 pane → 1 large tree
  static_box        — alpha=0 → delta_min padding (no EMA)
  waves_single_rule — k_max=2 → each DC in own group (no shared index)
  buffer_wait       — enable_retraction=False → no tombstone/retract
  waves_full        — full WAVES with all features enabled
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
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


def load_dc_rules(path: Path) -> list:
    with open(path) as f:
        raw = json.load(f)
    return raw["dc_rules"]


def compute_accuracy(alerts: list[AlertEvent], ground_truth: dict,
                    processed_event_ids: set[str] | None = None) -> dict:
    """Compute precision/recall against injected DC violations.

    If processed_event_ids is provided, only ground truth events within the
    processed dataset are used for recall (avoids false-low recall from
    comparing partial run against full ground truth).
    """
    # Filter GT to only events that were actually in the processed dataset
    if processed_event_ids is not None:
        gt_event_ids = {eid for eid, rec in ground_truth.items()
                        if rec.get("noise_type") in ("dc1", "dc2", "dc3")
                        and eid in processed_event_ids}
    else:
        gt_event_ids = {eid for eid, rec in ground_truth.items()
                        if rec.get("noise_type") in ("dc1", "dc2", "dc3")}

    detected = set()
    for alert in alerts:
        if alert.event_type != "retraction":
            detected.add(alert.event_id)

    tp = len(gt_event_ids & detected)
    fp = len(detected - gt_event_ids)
    fn = len(gt_event_ids - detected)

    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f = 2 * p * r / (p + r) if (p + r) > 0 else 0.0

    return {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f, 4),
            "tp": tp, "fp": fp, "fn": fn, "gt_in_window": len(gt_event_ids)}


def _nl_stream_traverse(pipeline, pane, event) -> list:
    """Nested-loop: O(N) per event, O(N²) total. Uses pane.buffer directly."""
    candidates = []
    point = pipeline._extract_point(event)
    if not point or not pipeline._active_boxes:
        return candidates

    from waves.rapidash.candidate import CandidateViolation

    for box in pipeline._active_boxes:
        matched_ids = []
        pane_id_of_match = ""
        for pt, pt_id in pane.buffer:
            if pt_id == event.event_id:
                continue
            pane_id_t = pipeline._event_store.get_pane_id(pt_id)
            if pipeline._tombstone_mgr is not None and pipeline._tombstone_mgr.contains(pt_id, pane_id_t):
                continue
            in_box = True
            for dim, (lo, hi) in box.padded_bounds.items():
                v = pt[dim] if dim < len(pt) else 0.0
                if v < lo or v > hi:
                    in_box = False
                    break
            if in_box:
                matched_ids.append(pt_id)
                pane_id_of_match = pane_id_t

        if matched_ids:
            candidates.append(CandidateViolation(
                dc_id=box.dc_id, window_id=event.window_id or "",
                pane_id=pane_id_of_match, query_id=event.event_id,
                matched_ids=matched_ids, box_id=box.box_id, timestamp_ms=0,
            ))
    return candidates


def run_one_baseline(
    benchmark_path: Path,
    dc_rules_path: Path,
    baseline: str,
    seed: int,
    warmup: int = 1000,
    max_events: int | None = None,
    filter_year: int | None = None,
) -> dict:
    """Run one baseline. Pane closing is handled via watermark advance."""

    alerts: list[AlertEvent] = []
    meta_events = []

    def alert_sink(e: AlertEvent):
        alerts.append(e)

    def meta_sink(e):
        meta_events.append(e)

    # ── Config per baseline ──────────────────────────────────────────────────
    match baseline:
        case "nl_stream":
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                                  alert_ttl_seconds=3600.0, k_max=5)
        case "single_tree":
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=600.0,
                                  wait_for_late_seconds=300.0, alert_ttl_seconds=3600.0, k_max=5)
        case "static_box":
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                                  alert_ttl_seconds=3600.0, k_max=5)
        case "waves_single_rule":
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                                  alert_ttl_seconds=3600.0, k_max=2)
        case "buffer_wait":
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                                  alert_ttl_seconds=3600.0, k_max=5)
        case _:  # waves_full
            cfg = PipelineConfig(window_width_seconds=600.0, slide_step_seconds=120.0,
                                  pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                                  alert_ttl_seconds=3600.0, k_max=5)

    pipeline = WavePipeline(cfg)
    dc_rules = load_dc_rules(dc_rules_path)
    pipeline.load_dc_rules(dc_rules)
    # After load_dc_rules, boxes are built with empty static_bounds (pipeline bug).
    # Rebuild boxes with correct NYC_TAXI_BOUNDS so detection actually works.
    from waves.optimizer import DCParser, GreedyRuleGrouper, OptimizerConfig, build_active_boxes
    from waves.optimizer.config import NYC_TAXI_BOUNDS
    parser = DCParser()
    enriched = parser.parse_dc_rules(dc_rules)
    opt_cfg = OptimizerConfig(k_max=cfg.k_max, infinite_padding=True,
                              static_bounds=dict(NYC_TAXI_BOUNDS))
    grouper = GreedyRuleGrouper(opt_cfg)
    groups = grouper.group(enriched)
    grouper.assign_dim_map_to_dcs(enriched, groups)
    pipeline._active_boxes = build_active_boxes(enriched, groups, opt_cfg)
    pipeline._dim_map = {}
    for g in groups:
        pipeline._dim_map.update(g.column_to_dim)
    max_dim = max(pipeline._dim_map.values()) if pipeline._dim_map else -1
    pipeline._lo_bounds = tuple(0.0 for _ in range(max_dim + 1))
    pipeline._hi_bounds = tuple(100.0 for _ in range(max_dim + 1))
    # Update OptimizerConfig for later use
    pipeline._optimizer_cfg = opt_cfg

    # ── Runtime patches per baseline ───────────────────────────────────────
    if baseline == "nl_stream":
        pipeline._traverse_pane = lambda pane, event: _nl_stream_traverse(pipeline, pane, event)
    elif baseline == "static_box":
        pipeline._logical_engine._config.alpha = 0.0
    elif baseline == "buffer_wait":
        import waves.decision.decision as dec_mod
        dec_mod.retract_alert = lambda *args, **kwargs: []

    # ── Patched process() with pane closing + watermark ─────────────────────
    # Root cause: pane_close is only called via window_slide, which never fires
    # without a WatermarkClock integration. We integrate WatermarkClock here and
    # close panes when their end_time <= watermark - wait_for_late.
    from waves.windowing.watermark import WatermarkClock, WatermarkConfig
    wm_config = WatermarkConfig(
        wait_for_late=int(cfg.wait_for_late_seconds),
    )
    wm_clock = WatermarkClock(wm_config)
    # Sync watermark to first event time
    wm_synced = False

    # Capture original event_id from raw row before it's lost after parse
    def _patched_process(event, original_event_id=None):
        nonlocal wm_synced
        # Preserve original event_id (parse_and_normalize generates UUID)
        if original_event_id is not None:
            event.event_id = original_event_id
        if not wm_synced:
            wm_clock._watermark = event.event_time
            wm_synced = True

        # Advance watermark
        wm_clock.on_event(event)

        # Assign window / pane
        wm_result = pipeline._window_mgr.assign_window(event, datetime.now(timezone.utc))
        if not wm_result:
            return []
        we = wm_result[0]
        event.window_id = we.window_id
        event.pane_id = we.pane_id

        # Close OLD panes only (end_time < wm, not <=):
        # pane.end_time == wm means the pane is still receiving inserts;
        # pane.end_time < wm means it's fully behind the watermark.
        if pipeline._active_boxes:
            dim_count = len(pipeline._lo_bounds)
            for pane in list(pipeline._pane_forest.panes):
                if pane.is_active and pane.end_time < wm_clock.get():
                    pipeline._pane_forest.pane_close(
                        pane.pane_id, dim_count,
                        pipeline._lo_bounds, pipeline._hi_bounds,
                    )

        # Basic DQ
        pipeline._dq_checker.check_event(event)

        # Store event
        pipeline._event_store.put(event)

        # Pane insert
        if event.pane_id and event.window_id:
            pipeline._pane_forest.pane_insert(
                event_time=event.event_time, event_id=event.event_id,
                point=pipeline._extract_point(event),
                window_id=event.window_id,
                config=pipeline.config.window_config(),
            )
            pane = pipeline._pane_forest.get_pane_by_id(event.pane_id)
            if pane is not None and pane.kdtree is not None:
                candidates = pipeline._traverse_pane(pane, event)
                decisions = []
                from waves.decision.decision import process_candidate
                for cand in candidates:
                    decs = process_candidate(cand, pipeline._alert_store, pipeline._tombstone_mgr)
                    for dec in decs:
                        pipeline._output.emit(dec)
                        decisions.append(dec)
                return decisions
        return []

    pipeline.process = _patched_process
    pipeline._output = AlertOutput(alert_sink=alert_sink, meta_sink=meta_sink)

    # ── Load ground truth ───────────────────────────────────────────────────
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

    # ── Load dataset ───────────────────────────────────────────────────────
    df = pd.read_parquet(benchmark_path)
    if filter_year is not None:
        df = df[df["event_time"].dt.year >= filter_year].reset_index(drop=True)
    n_rows = min(len(df), max_events or len(df))
    # ── Process events ──────────────────────────────────────────────────────
    latencies: list[float] = []
    start_wall = time.perf_counter()
    processed_eids: set[str] = set()

    # Use itertuples (~10x faster than iterrows on large DataFrames)
    df_subset = df.iloc[:n_rows]
    for row_idx, row in enumerate(df_subset.itertuples(index=False)):
        t0 = time.perf_counter()
        try:
            raw = row._asdict()
            event = parse_and_normalize(raw, NYC_SCHEMA)
            original_eid = str(raw.get("event_id", ""))
        except ParseError:
            continue
        pipeline.process(event, original_event_id=original_eid)
        latencies.append((time.perf_counter() - t0) * 1000)
        processed_eids.add(original_eid)

    elapsed = time.perf_counter() - start_wall
    n_processed = len(processed_eids)
    pipeline.cleanup()

    elapsed = time.perf_counter() - start_wall
    n_processed = min(n_rows, len(df))
    pipeline.cleanup()

    # ── Metrics ─────────────────────────────────────────────────────────────
    lat = sorted(latencies)
    p50 = lat[int(len(lat) * 0.50)]
    p95 = lat[int(len(lat) * 0.95)]
    p99 = lat[int(len(lat) * 0.99)]

    alerts_by_dc = {}
    for a in alerts:
        if a.event_type != "retraction":
            alerts_by_dc[a.dc_id] = alerts_by_dc.get(a.dc_id, 0) + 1

    pane_stats = {}
    for pid, pane in pipeline._pane_forest.panes_by_id.items():
        pane_stats[pid] = {"closed": not pane.is_active, "kdtree": pane.kdtree is not None,
                            "buffer_size": len(pane.buffer)}

    return {
        "config": {"baseline": baseline, "seed": seed,
                   "total_events": n_rows, "warmup": warmup,
                   "elapsed_seconds": round(elapsed, 3)},
        "system": {
            "throughput_events_per_sec": round(n_processed / elapsed, 2) if elapsed > 0 else 0,
            "avg_latency_ms": round(sum(lat) / len(lat), 3) if lat else 0,
            "p50_latency_ms": round(p50, 3),
            "p95_latency_ms": round(p95, 3),
            "p99_latency_ms": round(p99, 3),
        },
        "alerts": {
            "total": len(alerts),
            "provisional": sum(1 for a in alerts if a.event_type == "provisional"),
            "final": sum(1 for a in alerts if a.event_type == "final"),
            "retraction": sum(1 for a in alerts if a.event_type == "retraction"),
            "by_dc": alerts_by_dc,
        },
        "accuracy": compute_accuracy(alerts, ground_truth, processed_eids),
        "meta_events": len(meta_events),
        "pane_stats": pane_stats,
    }


def main():
    parser = argparse.ArgumentParser(description="Run ablation baselines")
    parser.add_argument("--benchmark", type=Path, default=Path("data/benchmark.parquet"))
    parser.add_argument("--dc-rules", type=Path, default=Path("configs/dc_rules.json"))
    parser.add_argument("--baselines", nargs="+",
                        default=["waves_full", "nl_stream", "single_tree",
                                  "static_box", "waves_single_rule", "buffer_wait"])
    parser.add_argument("--seeds", type=int, nargs="+", default=[42])
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    parser.add_argument("--max-events", type=int, default=None,
                        help="Limit to N events (for fast testing)")
    parser.add_argument("--filter-year", type=int, default=2024,
                        help="Only process events from this year onward (default: 2024 for NYC taxi)")
    args = parser.parse_args()

    if not args.benchmark.exists():
        print(f"Error: {args.benchmark} not found.")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[Ablation] baselines={args.baselines}, seeds={args.seeds}")
    print(f"  benchmark : {args.benchmark}")
    print(f"  dc_rules  : {args.dc_rules}")

    all_results = {}
    t0 = time.time()

    for baseline in args.baselines:
        print(f"\n=== {baseline} ===")
        all_results[baseline] = {}
        for seed in args.seeds:
            print(f"  seed={seed}...", end=" ", flush=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_dir = args.output_dir / f"run_{ts}_seed{seed}" / baseline

            m = run_one_baseline(
                args.benchmark, args.dc_rules,
                baseline, seed, args.warmup, args.max_events,
                filter_year=args.filter_year,
            )

            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "metrics.json"
            with open(out_path, "w") as f:
                json.dump(m, f, indent=2, default=str)

            all_results[baseline][seed] = m
            print(f"throughput={m['system']['throughput_events_per_sec']:.0f} ev/s, "
                  f"F1={m['accuracy']['f1']:.3f} (P={m['accuracy']['precision']:.3f} R={m['accuracy']['recall']:.3f}), "
                  f"alerts={m['alerts']['total']}")

    total_time = time.time() - t0
    print(f"\n{'='*60}")
    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f} min)")

    # Summary table
    print(f"\n{'Baseline':<22} {'Throughput':>12} {'P99':>8} {'P':>7} {'R':>7} {'F1':>7} {'Alerts':>7}")
    print("-" * 72)
    for baseline in args.baselines:
        if baseline not in all_results:
            continue
        seeds_data = list(all_results[baseline].values())
        avg_tp = sum(d["system"]["throughput_events_per_sec"] for d in seeds_data) / len(seeds_data)
        avg_p99 = sum(d["system"]["p99_latency_ms"] for d in seeds_data) / len(seeds_data)
        avg_p = sum(d["accuracy"]["precision"] for d in seeds_data) / len(seeds_data)
        avg_r = sum(d["accuracy"]["recall"] for d in seeds_data) / len(seeds_data)
        avg_f = sum(d["accuracy"]["f1"] for d in seeds_data) / len(seeds_data)
        total_alerts = sum(d["alerts"]["total"] for d in seeds_data)
        print(f"{baseline:<22} {avg_tp:>12.0f} {avg_p99:>8.1f} {avg_p:>7.3f} {avg_r:>7.3f} {avg_f:>7.3f} {total_alerts:>7}")

    summary_path = args.output_dir / "ablation_summary.json"
    with open(summary_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nSaved: {summary_path}")


if __name__ == "__main__":
    main()
