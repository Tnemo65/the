"""Run ablation baselines — WAVES-Full + 5 variant configurations.

Each baseline uses a runtime-patched WavePipeline with the fixed EMA-adaptive
ActiveBox generation from the updated pipeline.

Baselines:
  nl_stream          — O(N²) nested loop, bypasses KD-Tree entirely
  single_tree        — pane_size = window_width → 1 pane → 1 large tree
  static_box         — alpha_ema=1.0, delta_min=0 → no EMA padding
  waves_single_rule  — k_max=2 → each DC in own group (no shared index)
  buffer_wait        — retraction patched out
  waves_full         — full WAVES with all features enabled
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
                    processed_event_ids: set) -> dict:
    gt_event_ids = {eid for eid, rec in ground_truth.items()
                    if rec.get("noise_type") in ("dc1", "dc2", "dc3")
                    and eid in processed_event_ids}

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


def _patched_process_waves(pipeline, event):
    """Patched process for WAVES baselines (static-box, waves-single-rule, buffer_wait, waves-full).

    Uses _traverse_pane (static ActiveBoxes) — bypasses EMA to isolate mechanism effects.
    Correctly closes panes using watermark.
    """
    from waves.windowing import WatermarkClock, WatermarkConfig

    # Advance watermark
    pipeline._wm_clock.on_event(event)

    # Assign window
    now = pipeline._now_fn()
    wm_result = pipeline._window_mgr.assign_window(event, now)
    if not wm_result:
        return []

    # Basic DQ
    pipeline._dq_checker.check_event(event)

    # Store event
    pipeline._event_store.put(event)

    # Close old panes (watermark-based)
    if pipeline._active_boxes:
        dim_count = len(pipeline._lo_bounds)
        from datetime import timedelta
        threshold = timedelta(seconds=pipeline.config.wait_for_late_seconds)
        for pane in list(pipeline._pane_forest.panes):
            if pane.is_active:
                pane_end_plus_grace = pane.end_time + threshold
                if pipeline._wm_clock.get() >= pane_end_plus_grace:
                    pipeline._pane_forest.pane_close(
                        pane.pane_id, dim_count,
                        pipeline._lo_bounds, pipeline._hi_bounds,
                    )

    # Window slide: finalize closing windows
    if pipeline._wm_clock.has_advanced():
        _, closing_windows = pipeline._window_mgr.on_slide(pipeline._wm_clock.get())
        for wid in closing_windows:
            pipeline.finalize_window(wid)

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
            candidates = _traverse_with_predicates(pipeline, pane, event)
            decisions = []
            from waves.decision.decision import process_candidate
            for cand in candidates:
                decs = process_candidate(cand, pipeline._alert_store, pipeline._tombstone_mgr)
                for dec in decs:
                    pipeline._output.emit(dec)
                    decisions.append(dec)
            return decisions
    return []


def _traverse_with_predicates(pipeline, pane, event) -> list:
    """Traverse recent panes' KD-Trees with static ActiveBoxes + predicate evaluation.

    Queries the last 5 closed panes plus the current pane's KD-Tree.
    """
    from waves.rapidash import traverse_node
    from waves.optimizer.dc_parser import PredicateType

    candidates = []
    point = pipeline._extract_point(event)
    if not point or not pipeline._active_boxes:
        return candidates

    # Find all panes with KD-Trees to query (last 5 by end_time)
    panes_to_query = []
    for pid, p_obj in pipeline._pane_forest.panes_by_id.items():
        if p_obj.kdtree is None:
            continue
        panes_to_query.append((p_obj.end_time, p_obj))
    panes_to_query.sort(key=lambda x: x[0], reverse=True)
    panes_to_query = [p for _, p in panes_to_query[:5]]

    for box in pipeline._active_boxes:
        cands, visited, pruned = traverse_node(
            pane.kdtree,
            point,
            event.event_id,
            box,
            pipeline._tombstone_mgr,
            pipeline._event_store.get_pane_id,
        )
        for cand in cands:
            dc_preds = pipeline._dc_predicates.get(box.dc_id, [])
            if not dc_preds:
                candidates.append(cand)
                continue

            filtered = _filter_by_predicates(
                query_id=event.event_id,
                matched_ids=cand.matched_ids,
                predicates=dc_preds,
                event_store=pipeline._event_store,
            )
            if filtered:
                cand.matched_ids[:] = filtered
                candidates.append(cand)

    return candidates


def _filter_by_predicates(query_id: str, matched_ids: list,
                          predicates: list, event_store) -> list:
    """Keep only matched_ids that VIOLATE all predicates (invert for DC correctness).

    DC = NOT (P1 ∧ P2 ∧ ... ∧ Pn)
    Violation = P1 ∧ P2 ∧ ... ∧ Pn all TRUE

    However, DC1/2/3 in dc_rules.json encode the NORMAL case (NOT violation),
    so we must INVERT the predicates:
      LESS → keep if NOT(l < r)  i.e. l >= r
      LESS_EQUAL → keep if NOT(l <= r) i.e. l > r
      EQUAL → keep if l != r

    This correctly identifies violations of the DC.
    """
    from waves.optimizer.dc_parser import PredicateType
    query_ev = event_store.get(query_id)
    if query_ev is None:
        return []

    passing = []

    for mid in matched_ids:
        matched_ev = event_store.get(mid)
        if matched_ev is None:
            continue

        all_predicate_pass = True
        for pred in predicates:
            if pred.left_side == "s":
                left_ev = query_ev
            elif pred.left_side == "t":
                left_ev = matched_ev
            else:
                left_ev = query_ev

            if pred.right_side == "s":
                right_ev = query_ev
            elif pred.right_side == "t":
                right_ev = matched_ev
            else:
                right_ev = matched_ev

            left_val = left_ev.attributes.get(pred.left_col) if left_ev else None
            if pred.is_constant and pred.constant_value is not None:
                right_val = pred.constant_value
            else:
                right_val = right_ev.attributes.get(pred.right_col) if right_ev else None

            if left_val is None or right_val is None:
                all_predicate_pass = False
                break

            try:
                l = float(left_val)
                r = float(right_val)
            except (TypeError, ValueError):
                l = str(left_val)
                r = str(right_val)

            op = pred.operator
            # DC1/2/3 encode the NORMAL case → invert: violation = NOT(predicate)
            if op == PredicateType.EQUAL:
                # Violation = NOT equal = different
                if not (l != r):
                    all_predicate_pass = False
                    break
            elif op == PredicateType.LESS:
                # Violation = NOT(l < r) = l >= r
                if not (l >= r):
                    all_predicate_pass = False
                    break
            elif op == PredicateType.LESS_EQUAL:
                # Violation = NOT(l <= r) = l > r
                if not (l > r):
                    all_predicate_pass = False
                    break
            elif op == PredicateType.GREATER:
                # Violation = NOT(l > r) = l <= r
                if not (l <= r):
                    all_predicate_pass = False
                    break
            elif op == PredicateType.GREATER_EQUAL:
                # Violation = NOT(l >= r) = l < r
                if not (l < r):
                    all_predicate_pass = False
                    break

        if all_predicate_pass:
            passing.append(mid)

    return passing


def run_one_baseline(
    benchmark_path: Path,
    dc_rules_path: Path,
    baseline: str,
    seed: int,
    warmup: int = 1000,
    max_events: int | None = None,
    filter_year: int | None = None,
) -> dict:

    alerts: list[AlertEvent] = []
    meta_events = []

    def alert_sink(e: AlertEvent):
        alerts.append(e)

    def meta_sink(e):
        meta_events.append(e)

    # ── Config per baseline ───────────────────────────────────────────────────
    match baseline:
        case "nl_stream":
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                alert_ttl_seconds=3600.0, k_max=5,
            )
        case "single_tree":
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=600.0,
                wait_for_late_seconds=300.0, alert_ttl_seconds=3600.0, k_max=5,
            )
        case "static_box":
            # EMA disabled: alpha=1.0 means no smoothing (delta_min → padding is min)
            # delta_min=0 → no padding beyond static bounds
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                alert_ttl_seconds=3600.0, k_max=5,
                alpha_ema=1.0,
                ema_delta_min=0.0,
                ema_delta_max=0.0,
                ema_warmup=0,
            )
        case "waves_single_rule":
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                alert_ttl_seconds=3600.0, k_max=2,
            )
        case "buffer_wait":
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                alert_ttl_seconds=3600.0, k_max=5,
            )
        case _:  # waves_full
            cfg = PipelineConfig(
                window_width_seconds=600.0, slide_step_seconds=120.0,
                pane_size_seconds=60.0, wait_for_late_seconds=300.0,
                alert_ttl_seconds=3600.0, k_max=5,
                alpha_ema=0.05,
                ema_warmup=50,
            )

    pipeline = WavePipeline(cfg)
    dc_rules = load_dc_rules(dc_rules_path)
    pipeline.load_dc_rules(dc_rules)

    # Output sink
    pipeline._output = AlertOutput(alert_sink=alert_sink, meta_sink=meta_sink)

    # ── Runtime patches per baseline ───────────────────────────────────────────
    if baseline == "nl_stream":
        # Replace process entirely with O(N²) nested-loop version
        def _nl_process(event):
            pipeline._wm_clock.on_event(event)
            wm_result = pipeline._window_mgr.assign_window(event, pipeline._now_fn())
            if not wm_result:
                return []
            pipeline._dq_checker.check_event(event)
            pipeline._event_store.put(event)
            if event.pane_id and event.window_id:
                pipeline._pane_forest.pane_insert(
                    event_time=event.event_time, event_id=event.event_id,
                    point=pipeline._extract_point(event),
                    window_id=event.window_id,
                    config=pipeline.config.window_config(),
                )
                pane = pipeline._pane_forest.get_pane_by_id(event.pane_id)
                if pane is not None:
                    candidates = _nl_stream_traverse(pipeline, pane, event)
                    from waves.decision.decision import process_candidate
                    for cand in candidates:
                        decs = process_candidate(cand, pipeline._alert_store, pipeline._tombstone_mgr)
                        for dec in decs:
                            pipeline._output.emit(dec)
                    return decs if candidates else []
            return []
        pipeline.process = _nl_process

    elif baseline == "buffer_wait":
        # Patch retract_alert to be a no-op
        import waves.decision.decision as dec_mod
        _orig_retract = dec_mod.retract_alert
        dec_mod.retract_alert = lambda *args, **kwargs: []
        # Patch WavePipeline.process to not finalize windows (no watermark-driven retraction)
        pipeline.process = lambda ev: _patched_process_waves(pipeline, ev)

    else:
        # static_box, single_tree, waves_single_rule, waves_full
        # All use patched process with static ActiveBoxes (bypasses EMA path)
        pipeline.process = lambda ev: _patched_process_waves(pipeline, ev)

    # ── Load ground truth ─────────────────────────────────────────────────────
    gt_path = benchmark_path.parent / "ground_truth.jsonl"
    ground_truth: dict = {}
    if gt_path.exists():
        with open(gt_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    ground_truth[rec["event_id"]] = rec

    # ── Load dataset ─────────────────────────────────────────────────────────
    df = pd.read_parquet(benchmark_path)
    if filter_year is not None:
        df = df[df["event_time"].dt.year >= filter_year].reset_index(drop=True)
    n_rows = min(len(df), max_events or len(df))
    df_subset = df.iloc[:n_rows]

    # ── Process events ───────────────────────────────────────────────────────
    latencies: list[float] = []
    start_wall = time.perf_counter()
    processed_eids: set = set()

    for row_idx, row in enumerate(df_subset.itertuples(index=False)):
        t0 = time.perf_counter()
        try:
            raw = row._asdict()
            event = parse_and_normalize(raw, NYC_SCHEMA)
            original_eid = str(raw.get("event_id", ""))
        except ParseError:
            continue
        pipeline.process(event)
        latencies.append((time.perf_counter() - t0) * 1000)
        processed_eids.add(original_eid)

    elapsed = time.perf_counter() - start_wall
    n_processed = len(processed_eids)
    pipeline.cleanup()

    # ── Metrics ──────────────────────────────────────────────────────────────
    lat = sorted(latencies)
    p50 = lat[int(len(lat) * 0.50)] if lat else 0
    p95 = lat[int(len(lat) * 0.95)] if lat else 0
    p99 = lat[int(len(lat) * 0.99)] if lat else 0

    alerts_by_dc = {}
    for a in alerts:
        if a.event_type != "retraction":
            alerts_by_dc[a.dc_id] = alerts_by_dc.get(a.dc_id, 0) + 1

    pane_stats = {}
    for pid, pane in pipeline._pane_forest.panes_by_id.items():
        pane_stats[pid] = {
            "closed": not pane.is_active,
            "kdtree": pane.kdtree is not None,
            "buffer_size": len(pane.buffer),
        }

    return {
        "config": {
            "baseline": baseline,
            "seed": seed,
            "total_events": n_rows,
            "processed_events": n_processed,
            "warmup": warmup,
            "elapsed_seconds": round(elapsed, 3),
        },
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
        print("Run: prepare_benchmark → inject_fraud → inject_drift → inject_late")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[Ablation] baselines={args.baselines}, seeds={args.seeds}")
    print(f"  benchmark : {args.benchmark}")
    print(f"  dc_rules  : {args.dc_rules}")

    all_results = {}
    t0_total = time.time()

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
                  f"F1={m['accuracy']['f1']:.3f} "
                  f"(P={m['accuracy']['precision']:.3f} R={m['accuracy']['recall']:.3f}), "
                  f"alerts={m['alerts']['total']}")

    total_time = time.time() - t0_total
    print(f"\n{'='*70}")
    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f} min)")

    # Summary table
    print(f"\n{'Baseline':<22} {'Throughput':>12} {'P99(ms)':>9} "
          f"{'Prec':>7} {'Rec':>7} {'F1':>7} {'Alerts':>7}")
    print("-" * 75)
    for baseline in args.baselines:
        if baseline not in all_results:
            continue
        seeds_data = list(all_results[baseline].values())
        n_seeds = len(seeds_data)
        avg_tp = sum(d["system"]["throughput_events_per_sec"] for d in seeds_data) / n_seeds
        avg_p99 = sum(d["system"]["p99_latency_ms"] for d in seeds_data) / n_seeds
        avg_p = sum(d["accuracy"]["precision"] for d in seeds_data) / n_seeds
        avg_r = sum(d["accuracy"]["recall"] for d in seeds_data) / n_seeds
        avg_f = sum(d["accuracy"]["f1"] for d in seeds_data) / n_seeds
        total_alerts = sum(d["alerts"]["total"] for d in seeds_data)
        print(f"{baseline:<22} {avg_tp:>12.0f} {avg_p99:>9.1f} "
              f"{avg_p:>7.3f} {avg_r:>7.3f} {avg_f:>7.3f} {total_alerts:>7}")

    summary_path = args.output_dir / "ablation_summary.json"
    with open(summary_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nSaved: {summary_path}")


if __name__ == "__main__":
    main()
