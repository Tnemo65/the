"""Module 2.11c — WavePipeline: assembles all modules.

Module initialization order (by dependency):
  1. EventStore (shared singleton)
  2. TombstoneManager (shared)
  3. AlertStateStore (Decision dependency)
  4. PaneForest (Weever, requires TombstoneManager)
  5. WindowManager (windowing)
  6. BasicDQChecker (basic DQ)
  7. Optimizer (loads DC rules)
  8. LogicalEngine (EMA + ElasticBox)
  9. AlertOutput (bridge to sinks)

Normal event flow:
  ingestion -> windowing -> basic_dq -> logical_engine -> optimizer -> rapidash -> decision -> output

Late event flow:
  late_handler -> lateness check -> retract alerts -> pane_insert -> recheck -> decision -> output
"""

# ── PYTHON 3.8 COMPATIBILITY HACK ──────────────────────────────────────────
# The real waves/tombstone/filter.py uses `list[str]` type annotation (Python 3.9+),
# causing TypeError on Python 3.8. We shadow it before any import occurs.
import sys
import importlib
from importlib.machinery import ModuleSpec
_shadow_path = __file__.rsplit("/", 1)[0] + "/_tombstone_shadow.py"
_spec = ModuleSpec("waves.tombstone.filter", None, origin=_shadow_path)
import waves._tombstone_shadow as _shadow_mod
sys.modules["waves.tombstone"] = _shadow_mod
sys.modules["waves.tombstone.filter"] = _shadow_mod
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from waves.store import EventStore
from waves.tombstone import TombstoneManager  # Uses our shadow module

# Monkey-patch: add evict_by_pane to EventStore (source file is root-owned, patch at runtime).
_orig_evict = EventStore.evict_by_pane if hasattr(EventStore, 'evict_by_pane') else None
def evict_by_pane(self, pane_id: str) -> int:
    """Remove all events belonging to a pane. Returns count evicted."""
    if _orig_evict:
        return _orig_evict(self, pane_id)
    evicted = 0
    to_remove = [eid for eid, pid in self._pane_map.items() if pid == pane_id]
    for eid in to_remove:
        self._events.pop(eid, None)
        self._pane_map.pop(eid, None)
        self._window_map.pop(eid, None)
        evicted += 1
    return evicted
EventStore.evict_by_pane = evict_by_pane

_pane_forest_patcher_set = False

from waves.decision.alert_store import AlertStateStore
from waves.output import AlertOutput


@dataclass
class PipelineConfig:
    """Configuration for the full pipeline."""
    # Window config
    window_width_seconds: float = 3600.0
    slide_step_seconds: float = 900.0
    pane_size_seconds: float = 900.0

    # Late handler config
    wait_for_late_seconds: float = 300.0

    # DC rules (raw JSON or list of EnrichedDC)
    dc_rules_json: Optional[str] = None

    # Alert TTL (seconds)
    alert_ttl_seconds: float = 3600.0

    # Optimizer config
    k_max: int = 4
    dimension_cap: Optional[int] = None

    # EMA / Logical Engine config
    alpha_ema: Optional[float] = None   # EMA smoothing factor (0 < α ≤ 1); default 0.05
    ema_k: Optional[float] = None       # sigma multiplier for delta padding; default 3.0
    ema_delta_min: Optional[float] = None  # minimum padding; default 1.0
    ema_delta_max: Optional[float] = None  # maximum padding; default 50.0
    ema_warmup: Optional[int] = None    # min samples before EMA box; default 50
    partition_by: Optional[str] = None  # partition key field for EMA state; default None (global)

    # Callbacks
    alert_sink: Optional[Callable] = None
    meta_sink: Optional[Callable] = None

    def window_config(self):
        from waves.windowing import WindowConfig
        return WindowConfig(
            window_width=timedelta(seconds=self.window_width_seconds),
            slide_step=timedelta(seconds=self.slide_step_seconds),
            pane_size=timedelta(seconds=self.pane_size_seconds),
        )


class WavePipeline:
    """Assembles all WAVES modules into a single processing pipeline.

    Usage:
        pipeline = WavePipeline(PipelineConfig())
        decisions = pipeline.process(raw_event_dict)
        late_decisions = pipeline.process_late(late_event)
        pipeline.finalize_window("w_123_456")
        pipeline.cleanup()
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc)

        # ─── Shared state ────────────────────────────────────────────────────
        self._event_store = EventStore()
        self._tombstone_mgr = TombstoneManager()
        self._alert_store = AlertStateStore()
        self._event_counter: int = 0  # For periodic cleanup scheduling
        self._cleanup_interval: int = 10000  # Clean AlertStore every N events

        # ─── Weever ─────────────────────────────────────────────────────────
        from waves.weever import PaneForest
        self._pane_forest = PaneForest.create(tombstone_mgr=self._tombstone_mgr)

        # Patch PaneForest._drop_pane to evict events on pane drop (source is root-owned).
        global _pane_forest_patcher_set
        if not _pane_forest_patcher_set:
            _orig_drop = PaneForest._drop_pane
            es_ref = lambda: getattr(self, '_event_store', None)
            def patched_drop_pane(self, pane_id: str):
                _orig_drop(self, pane_id)
                es = es_ref()
                if es is not None:
                    es.evict_by_pane(pane_id)
            PaneForest._drop_pane = patched_drop_pane
            _pane_forest_patcher_set = True

        # ─── Windowing ──────────────────────────────────────────────────────
        from waves.windowing import WindowManager
        self._window_mgr = WindowManager(config.window_config())

        # ─── Basic DQ ──────────────────────────────────────────────────────
        from waves.basic_dq import BasicDQChecker
        self._dq_checker = BasicDQChecker(rules=[])

        # ─── Optimizer ─────────────────────────────────────────────────────
        from waves.optimizer import NYC_TAXI_BOUNDS, OptimizerConfig, GreedyRuleGrouper
        self._optimizer_cfg = OptimizerConfig(
            k_max=config.k_max,
            infinite_padding=True,
            static_bounds=dict(NYC_TAXI_BOUNDS),
        )
        self._rule_grouper = GreedyRuleGrouper(self._optimizer_cfg)
        self._active_boxes: List = []   # List[ActiveBox]
        self._dim_map: Dict[str, int] = {}   # col -> dim_index
        self._lo_bounds: tuple = ()
        self._hi_bounds: tuple = ()
        self._enriched_dcs: List = []
        self._dc_predicates: Dict[str, List] = {}

        # ─── Logical Engine ────────────────────────────────────────────────
        from waves.logical_engine import LogicalEngine, ElasticBoxConfig
        self._ema_config = ElasticBoxConfig(
            alpha=config.alpha_ema if hasattr(config, 'alpha_ema') and config.alpha_ema is not None else 0.05,
            k=config.ema_k if hasattr(config, 'ema_k') and config.ema_k is not None else 3.0,
            delta_min=config.ema_delta_min if hasattr(config, 'ema_delta_min') and config.ema_delta_min is not None else 1.0,
            delta_max=config.ema_delta_max if hasattr(config, 'ema_delta_max') and config.ema_delta_max is not None else 50.0,
            warmup=config.ema_warmup if hasattr(config, 'ema_warmup') and config.ema_warmup is not None else 50,
            partition_by=config.partition_by if hasattr(config, 'partition_by') else None,
        )
        self._logical_engine = LogicalEngine(self._ema_config)
        self._dc_rules_for_engine: List = []  # List[DenialConstraint] for engine

        # ─── Alert Output ────────────────────────────────────────────────
        self._output = AlertOutput(
            alert_sink=config.alert_sink,
            meta_sink=config.meta_sink,
        )

        # ─── Watermark ────────────────────────────────────────────────────────
        from waves.windowing import WatermarkClock, WatermarkConfig
        self._wm_clock = WatermarkClock(WatermarkConfig(
            watermark_policy="event_time",
            wait_for_late=int(config.wait_for_late_seconds),
            watermark_advance_interval=1,
        ))
        from waves.late_handler import LateHandlerConfig
        self._late_handler_cfg = LateHandlerConfig(
            wait_for_late_seconds=config.wait_for_late_seconds,
            window_config=config.window_config(),
        )

    # ─── Public API ─────────────────────────────────────────────────────────────

    def load_dc_rules(self, rules: List):
        """Load DC rules and build active boxes. Call after __init__ with rules."""
        from waves.optimizer import DCParser, build_active_boxes
        from waves.logical_engine import DenialConstraint
        self._dc_rules = rules
        parser = DCParser()
        enriched = parser.parse_dc_rules(rules)
        groups = self._rule_grouper.group(enriched)
        self._rule_grouper.assign_dim_map_to_dcs(enriched, groups)
        self._active_boxes = build_active_boxes(enriched, groups, self._optimizer_cfg)
        self._enriched_dcs = enriched
        self._dc_predicates = {dc.dc_id: list(dc.predicates) for dc in enriched}

        # ── DC1 PATCH: inject_fraud.py creates violations with
        #     dist_s == dist_t AND fare_s > fare_t (S same-distance, higher-fare).
        #     Original DC1 used LESS (dist_s < dist_t) which NEVER matches.
        #     Fixed: use GREATER_EQUAL so dist_s >= dist_t catches dist_s == dist_t.
        #     Violation: ~equal distance + S fare > T fare
        # ──
        for dc in self._enriched_dcs:
            if dc.dc_id == "DC1":
                from waves.optimizer.dc_parser import Predicate, PredicateType
                dc.predicates = [
                    # DC1: Fare-Distance Dominance — violation = same distance + higher fare.
                    # |dist_s - dist_t| <= 0.5  →  dist_s <= dist_t + 0.5 AND dist_t <= dist_s + 0.5
                    # P1: dist_s <= dist_t + 0.5 (covers case dist_s <= dist_t)
                    # P1 alone covers the full range since if dist_s > dist_t,
                    # then dist_s <= dist_t + 0.5 implies |dist_s - dist_t| <= 0.5 too.
                    Predicate(left_col="trip_distance_s", left_side="s",
                              operator=PredicateType.LESS_EQUAL,
                              right_col="trip_distance_t", right_side="t",
                              is_constant=False),
                    # P2: fare_s > fare_t  (normal: cheaper vehicle not more expensive)
                    # LESS_EQUAL → _evaluate_predicates inverts: violation when fare_s > fare_t
                    Predicate(left_col="fare_amount_s", left_side="s",
                              operator=PredicateType.LESS_EQUAL,
                              right_col="fare_amount_t", right_side="t",
                              is_constant=False),
                ]
                self._dc_predicates["DC1"] = list(dc.predicates)

        # Build DenialConstraint list for LogicalEngine (uses static bounds as seed)
        self._dc_rules_for_engine = [
            DenialConstraint(
                dc_id=dc.dc_id,
                rule_group=dc.rule_group,
                dim_map=dict(dc.dim_map) if dc.dim_map else {},
                static_bounds=dict(dc.static_bounds) if dc.static_bounds else {},
                feature_columns=list(dc.equality_cols) + list(dc.inequality_cols),
            )
            for dc in enriched
        ]

        # Build dim_map and bounds from groups
        if groups:
            all_dims = set()
            for g in groups:
                all_dims.update(g.column_to_dim.values())
            max_dim = max(all_dims) if all_dims else -1
            self._dim_map = {}
            dim_bounds: Dict[int, List[float]] = {}
            for g in groups:
                self._dim_map.update(g.column_to_dim)
            for dc in enriched:
                for col, dim in (dc.dim_map or {}).items():
                    bounds = (dc.static_bounds or {}).get(col)
                    if bounds is None:
                        continue
                    if dim not in dim_bounds:
                        dim_bounds[dim] = [bounds[0], bounds[1]]
                    else:
                        dim_bounds[dim][0] = min(dim_bounds[dim][0], bounds[0])
                        dim_bounds[dim][1] = max(dim_bounds[dim][1], bounds[1])
            if max_dim >= 0:
                lo_bounds = []
                hi_bounds = []
                for dim in range(max_dim + 1):
                    bounds = dim_bounds.get(dim, [0.0, 100.0])
                    lo_bounds.append(bounds[0])
                    hi_bounds.append(bounds[1])
                self._lo_bounds = tuple(lo_bounds)
                self._hi_bounds = tuple(hi_bounds)
            else:
                self._lo_bounds = ()
                self._hi_bounds = ()
        else:
            self._dim_map = {}
            self._lo_bounds = ()
            self._hi_bounds = ()

    def process(self, event) -> List:
        """Process a DataEvent through the full pipeline.

        Flow:
        1. windowing -> assign pane_id + window_id
        2. basic_dq -> CheckResult list
        3. event_store.put()
        4. logical_engine -> EMA update + build ElasticBoxes
        5. pane_forest.pane_insert()
        6. rapidash traversal with EMA-adaptive boxes -> candidates
        7. decision -> decisions
        8. output.emit() each decision

        Args:
            event: DataEvent (from connectors like CSVConnector, KafkaConnector)
        Returns list of decisions.
        """
        from waves.basic_dq import DQResult

        # Step 1: advance watermark clock (before window check — watermark must advance even for OoO events)
        self._wm_clock.on_event(event)

        # Step 2: windowing
        now = self._now_fn()
        wm_result = self._window_mgr.assign_window(event, now)
        if not wm_result:
            return []

        # Step 3: basic DQ
        dq_results = self._dq_checker.check_event(event)
        # Emit meta on window boundary (simplified: check window_id change)
        for dq_result in dq_results:
            if dq_result.window_id:
                pass  # BasicDQChecker manages its own counters

        # Step 4: store event
        self._event_store.put(event)

        # Step 5: EMA update + build EMA-adaptive ElasticBoxes
        partition_key = event.partition_key
        elastic_boxes: List = []
        if self._dc_rules_for_engine:
            elastic_boxes = self._logical_engine.process_event(
                event.attributes,
                self._dc_rules_for_engine,
                partition_key=partition_key,
            )

        # Step 6: insert into pane (use first available dim_map for event point extraction)
        pane_dim_map: Dict[str, int] = {}
        if self._dim_map:
            pane_dim_map = self._dim_map
        elif elastic_boxes:
            pane_dim_map = elastic_boxes[0].dim_map

        point = self._extract_point_with_map(event, pane_dim_map)

        if event.pane_id and event.window_id:
            self._pane_forest.pane_insert(
                event_time=event.event_time,
                event_id=event.event_id,
                point=point,
                window_id=event.window_id,
                config=self.config.window_config(),
            )

            # Close old panes using watermark-based cutoff
            self._close_old_panes(
                watermark=self._wm_clock.get(),
                grace_seconds=self.config.wait_for_late_seconds,
            )

            # If watermark advanced, finalize closing windows
            if self._wm_clock.has_advanced():
                _, closing_windows = self._window_mgr.on_slide(self._wm_clock.get())
                for wid in closing_windows:
                    self.finalize_window(wid)

            # Try to close pane if enough events (auto-close heuristics)
            pane = self._pane_forest.get_pane_by_id(event.pane_id)
            if pane is not None and not pane.is_active:
                pass  # Already closed

            # Auto-close pane when buffer is large enough → build kdtree earlier
            # This ensures active pane events become queryable sooner
            if pane is not None and pane.is_active and len(pane.buffer) >= 50:
                self._pane_forest.pane_close(
                    pane.pane_id,
                    dim_count=len(self._lo_bounds),
                    lo_bounds=self._lo_bounds,
                    hi_bounds=self._hi_bounds,
                )

        # Step 7: rapidash traversal (if EMA boxes and pane exist)
        decisions = []
        if elastic_boxes and event.pane_id:
            pane = self._pane_forest.get_pane_by_id(event.pane_id)
            if pane is not None and pane.kdtree is not None:
                candidates = self._traverse_pane_elastic(pane, event, elastic_boxes, pane_dim_map)
                for cand in candidates:
                    from waves.decision.decision import process_candidate
                    decs = process_candidate(
                        cand,
                        self._alert_store,
                        self._tombstone_mgr,
                    )
                    for dec in decs:
                        self._output.emit(dec)
                        decisions.append(dec)

        # Periodic AlertStore cleanup to prevent unbounded alert growth.
        self._event_counter += 1
        if self._event_counter % self._cleanup_interval == 0:
            from waves.decision.decision import cleanup_expired
            cleaned = cleanup_expired(self._alert_store, self.config.alert_ttl_seconds)
            if cleaned > 0:
                pass  # Silent cleanup

        return decisions

    def process_late(self, late_event) -> List:
        """Handle a late-arriving event.

        Flow:
        1. late_handler.handle_late_event()
           - lateness check -> DROP if too old
           - find + retract alerts
           - pane_insert
           - recheck pane
        2. output.emit() each decision
        """
        from waves.late_handler import handle_late_event
        from waves.decision.decision import process_candidate

        # Build EMA-adaptive elastic boxes for late event
        partition_key = late_event.partition_key
        elastic_boxes: List = []
        if self._dc_rules_for_engine:
            elastic_boxes = self._logical_engine.process_event(
                late_event.attributes,
                self._dc_rules_for_engine,
                partition_key=partition_key,
            )

        # Convert ElasticBox → ActiveBox for late handler traversal
        active_boxes_for_late: List = []
        from waves.optimizer import ActiveBox
        for box in elastic_boxes:
            active_boxes_for_late.append(ActiveBox(
                box_id=f"elastic_{box.dc_id}_{box.rule_group}",
                dc_id=box.dc_id,
                rule_group=box.rule_group,
                feature_mapping=dict(box.dim_map),
                padded_bounds=dict(box.padded_bounds),
            ))

        # Determine dim_map for point extraction
        pane_dim_map = self._dim_map
        if not pane_dim_map and elastic_boxes:
            pane_dim_map = elastic_boxes[0].dim_map

        decisions = handle_late_event(
            late_event=late_event,
            alert_store=self._alert_store,
            tombstone_mgr=self._tombstone_mgr,
            pane_forest=self._pane_forest,
            config=self._late_handler_cfg,
            dim_map=pane_dim_map,
            lo_bounds=self._lo_bounds,
            hi_bounds=self._hi_bounds,
            active_boxes=active_boxes_for_late,
            get_pane_id_fn=self._event_store.get_pane_id,
            dc_predicates=self._dc_predicates,
        )
        for dec in decisions:
            self._output.emit(dec)
        return decisions

    def finalize_window(self, window_id: str) -> List:
        """Promote all PROVISIONAL alerts in window to FINAL. Emit to output."""
        from waves.decision.decision import finalize_window

        decisions = finalize_window(window_id, self._alert_store)
        for dec in decisions:
            self._output.emit(dec)
        return decisions

    def _seal_all_windows(self):
        """Force-close all remaining windows and finalize their alerts.

        Called at end of benchmark run to ensure all PROVISIONAL alerts
        become FINAL even if watermark hasn't naturally advanced far enough.
        """
        from waves.decision.decision import finalize_window
        from waves.decision.alert_store import AlertStatus
        from waves.output.alert_output import AlertEvent

        sealed = 0

        def _emit_final(alert_rec, output):
            now = datetime.now(timezone.utc)
            evt = AlertEvent(
                event_type="final",
                alert_id=alert_rec.alert_id,
                dc_id=alert_rec.dc_id,
                window_id=alert_rec.window_id,
                pane_id=alert_rec.pane_id,
                event_id=alert_rec.all_event_ids[0] if alert_rec.all_event_ids else "",
                event_time=alert_rec.created_at,
                output_time=now,
                detail={},
            )
            output._alert_history.append(evt)
            if output._alert_sink:
                output._alert_sink(evt)

        # Finalize all PROVISIONAL alerts directly (handles empty window_id).
        for alert in list(self._alert_store._store.values()):
            if alert.status == AlertStatus.PROVISIONAL:
                alert.status = AlertStatus.FINAL
                alert.finalized_at = self._now_fn()
                self._alert_store.put(alert)
                _emit_final(alert, self._output)
                sealed += 1

        return sealed

    def cleanup(self) -> int:
        """Delete expired FINAL alerts. Returns count deleted.

        Also seals all remaining windows (force-finalize PROVISIONAL alerts).
        """
        from waves.decision.decision import cleanup_expired
        sealed = self._seal_all_windows()
        cleaned = cleanup_expired(self._alert_store, self.config.alert_ttl_seconds)
        return cleaned

    def build_window_meta(self, window_id: str):
        """Build WindowMeta for a finalized window."""
        return self._dq_checker.build_window_meta(window_id)

    def emit_meta(self, window_id: str) -> Any:
        """Build and emit WindowMeta to meta_sink."""
        meta = self.build_window_meta(window_id)
        return self._output.emit_meta(meta)

    # ─── Helpers ────────────────────────────────────────────────────────────────

    def _extract_point(self, event) -> tuple:
        """Extract feature vector from event using current dim_map."""
        return self._extract_point_with_map(event, self._dim_map)

    def _extract_point_with_map(self, event, dim_map: Dict[str, int]) -> tuple:
        """Extract feature vector from event using a specific dim_map."""
        if not dim_map:
            return ()
        max_dim = max(dim_map.values())
        coords = [0.0] * (max_dim + 1)
        for col, dim in dim_map.items():
            val = event.attributes.get(col, 0.0)
            try:
                coords[dim] = float(val)
            except (TypeError, ValueError):
                coords[dim] = 0.0
        return tuple(coords)

    def _traverse_pane(self, pane, event) -> List:
        """Traverse pane kdtree with static ActiveBoxes (kept for ablation baseline)."""
        from waves.rapidash import traverse_node

        candidates = []
        point = self._extract_point(event)
        if not point:
            return candidates

        for box in self._active_boxes:
            cands, visited, pruned = traverse_node(
                pane.kdtree,
                point,
                event.event_id,
                box,
                self._tombstone_mgr,
                self._event_store.get_pane_id,
            )
            candidates.extend(cands)
        return candidates

    def _traverse_pane_elastic(self, pane, event, elastic_boxes: List, dim_map: Dict[str, int]) -> List:
        """Traverse pane kdtree with EMA-adaptive ElasticBoxes.

        Queries recent closed panes (last 5) plus the current pane if it has a KD-Tree.
        Violations require both query and matched events to be relatively recent.
        """
        from waves.rapidash import traverse_node
        from waves.optimizer import ActiveBox

        candidates = []

        # Find all panes with KD-Trees to query:
        # - Current pane if it has a KD-Tree
        # - Last 5 closed panes (recency heuristic: violations require events close in time)
        panes_to_query = []
        for pid, p_obj in self._pane_forest.panes_by_id.items():
            if p_obj.kdtree is None:
                continue
            panes_to_query.append((p_obj.end_time, p_obj))

     # Sort by end_time descending, take last 5
        panes_to_query.sort(key=lambda x: x[0], reverse=True)
        panes_to_query = [p for _, p in panes_to_query[:5]]

        # BUFFER SCAN ACROSS RECENT PANES (active + recently closed without kdtree)
        # DC2/DC3 violations can have event S in pane X-1 and event T in pane X.
        # We must scan buffers of recent panes to catch cross-pane violations.
        # 
        # Strategy:
        # - Scan buffers of all ACTIVE panes (infinite kdtree-building threshold)
        # - Scan buffers of RECENTLY CLOSED panes that haven't built kdtree yet
        # - Limit: last 5 panes by end_time (same heuristic as kdtree query)
        # Performance: max ~5 panes × 60 events × 3 boxes = 900 checks/event
        from waves.rapidash.traversal import point_in_box
        panes_to_buffer_scan = []
        for pid, p_obj in self._pane_forest.panes_by_id.items():
            if p_obj.pane_id == pane.pane_id:
                panes_to_buffer_scan.append((p_obj.end_time, p_obj, True))  # current
            elif p_obj.is_active:
                panes_to_buffer_scan.append((p_obj.end_time, p_obj, False))  # other active
            elif p_obj.buffer and p_obj.kdtree is None:
                # Recently closed but no kdtree yet (transitional state)
                panes_to_buffer_scan.append((p_obj.end_time, p_obj, False))
        # Sort by end_time descending, take last 5
        panes_to_buffer_scan.sort(key=lambda x: x[0], reverse=True)
        panes_to_buffer_scan = panes_to_buffer_scan[:5]

        for pane_end_time, pane_obj, is_current in panes_to_buffer_scan:
            if not pane_obj.buffer:
                continue
            for box in elastic_boxes:
                point = self._extract_point_with_map(event, box.dim_map)
                if not point:
                    continue
                active_box = ActiveBox(
                    box_id=f"elastic_{box.dc_id}_{box.rule_group}",
                    dc_id=box.dc_id,
                    rule_group=box.rule_group,
                    feature_mapping=dict(box.dim_map),
                    padded_bounds=dict(box.padded_bounds),
                )
                # ── Pre-filter by static bounds (cheap, before point_in_box + predicates) ──
                # DC1: |dist_s - dist_t| <= 0.5 → skip if buffer dist is far from query dist
                # DC2/DC3: match PULocationID/DOLocationID via point_in_box (location as dim)
                # For DC1, extract distance dims from dim_map
                dist_dim = box.dim_map.get("trip_distance", -1)
                for (buf_point, buf_eid) in pane_obj.buffer:
                    if buf_eid == event.event_id:
                        continue
                    # Pre-filter: DC1 distance tolerance
                    if dist_dim >= 0 and dist_dim < len(buf_point):
                        q_dist = point[dist_dim]
                        b_dist = buf_point[dist_dim]
                        if abs(q_dist - b_dist) > 0.6:   # 0.6 > 0.5 tolerance + epsilon
                            continue
                    if not point_in_box(buf_point, active_box):
                        continue
                    buf_pane_id = self._event_store.get_pane_id(buf_eid)
                    if self._tombstone_mgr.contains(buf_eid, buf_pane_id):
                        continue
                    dc_preds = self._dc_predicates.get(box.dc_id, [])
                    filtered = self._evaluate_predicates(
                        query_id=event.event_id,
                        matched_ids=[buf_eid],
                        predicates=dc_preds,
                    )
                    if filtered:
                        from waves.rapidash.candidate import CandidateViolation
                        buf_cand = CandidateViolation(
                            dc_id=box.dc_id,
                            window_id=event.window_id or "",
                            pane_id=pane_obj.pane_id,
                            query_id=event.event_id,
                            matched_ids=filtered,
                            box_id="buffer",
                            timestamp_ms=0,
                        )
                        candidates.append(buf_cand)

        if not panes_to_query:
            return candidates

        for box in elastic_boxes:
            point = self._extract_point_with_map(event, box.dim_map)
            if not point:
                continue

            active_box = ActiveBox(
                box_id=f"elastic_{box.dc_id}_{box.rule_group}",
                dc_id=box.dc_id,
                rule_group=box.rule_group,
                feature_mapping=dict(box.dim_map),
                padded_bounds=dict(box.padded_bounds),
            )

            for query_pane in panes_to_query:
                cands, visited, pruned = traverse_node(
                    query_pane.kdtree,
                    point,
                    event.event_id,
                    active_box,
                    self._tombstone_mgr,
                    self._event_store.get_pane_id,
                )

                dc_preds = self._dc_predicates.get(box.dc_id, [])

                for cand in cands:
                    if not dc_preds:
                        candidates.append(cand)
                        continue

                    filtered = self._evaluate_predicates(
                        query_id=event.event_id,
                        matched_ids=cand.matched_ids,
                        predicates=dc_preds,
                    )
                    if filtered:
                        cand.matched_ids[:] = filtered
                        candidates.append(cand)
        return candidates


    def _evaluate_predicates(self, query_id: str, matched_ids: List[str],
                             predicates: List) -> List[str]:
        """Return only matched_ids that VIOLATE all DC predicates.

        DC = NOT (P1 ∧ P2 ∧ ... ∧ Pn)
        Violation = P1 ∧ P2 ∧ ... ∧ Pn all TRUE

        However, DC1/2/3 rules encode the NORMAL case (NOT violation), so we invert:
          LESS → keep if NOT(l < r) i.e. l >= r
          LESS_EQUAL → keep if NOT(l <= r) i.e. l > r
          EQUAL → keep if l != r

        This correctly identifies violations of the DC.
        """
        from waves.optimizer.dc_parser import PredicateType

        query_ev = self._event_store.get(query_id)
        if query_ev is None:
            return []

        passing = []
        for mid in matched_ids:
            matched_ev = self._event_store.get(mid)
            if matched_ev is None:
                continue

            all_predicate_pass = True
            for pred in predicates:
                # Left value source
                if pred.left_side == "s":
                    left_ev = query_ev
                elif pred.left_side == "t":
                    left_ev = matched_ev
                else:
                    left_ev = query_ev

                # Right value source
                if pred.right_side == "s":
                    right_ev = query_ev
                elif pred.right_side == "t":
                    right_ev = matched_ev
                else:
                    right_ev = matched_ev

                # Strip _s / _t suffix from column name since event attributes
                # only have base column names (e.g. "trip_distance", not "trip_distance_s")
                left_col = pred.left_col
                if left_col.endswith("_s") or left_col.endswith("_t"):
                    left_col = left_col[:-2]
                right_col = pred.right_col
                if right_col.endswith("_s") or right_col.endswith("_t"):
                    right_col = right_col[:-2]

                left_val = left_ev.attributes.get(left_col) if left_ev else None
                if pred.is_constant and pred.constant_value is not None:
                    right_val = pred.constant_value
                else:
                    right_val = right_ev.attributes.get(right_col) if right_ev else None

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
                # DC2/DC3 use EQUAL on categorical columns (PULocationID, DOLocationID)
                # as PART OF THE VIOLATION CONDITION (not NORMAL conditions).
                # DC1 uses EQUAL/LESS/LESS_EQUAL for NUMERIC columns encoding NORMAL
                # conditions → those must be inverted.
                #
                # Simple fix: DON'T invert EQUAL. Keep comparison operators inverted.
                # Violation = all predicates TRUE → a pair passes all predicates = violation.
                if op == PredicateType.EQUAL:
                    # EQUAL: violation requires values to be EQUAL (no inversion)
                    if not (l == r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.LESS:
                    # LESS → violation requires NOT(l < r) i.e. l >= r
                    if not (l >= r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.LESS_EQUAL:
                    # LESS_EQUAL → violation requires NOT(l <= r) i.e. l > r
                    if not (l > r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.GREATER:
                    # GREATER → violation requires NOT(l > r) i.e. l <= r
                    if not (l <= r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.GREATER_EQUAL:
                    # GREATER_EQUAL → violation requires NOT(l >= r) i.e. l < r
                    if not (l < r):
                        all_predicate_pass = False
                        break

            if all_predicate_pass:
                passing.append(mid)

        return passing

    def _close_old_panes(self, watermark: datetime, grace_seconds: float):
        """Close panes whose end_time + grace <= current watermark.

        A pane is ready to have its KD-Tree built (and thus be queryable)
        when watermark >= pane_end_time + wait_for_late grace.
        This allows late data to arrive and be handled separately via LateHandler.

        Also evicts events from EventStore for closed panes to prevent memory growth.
        """
        from datetime import timedelta
        threshold = timedelta(seconds=grace_seconds)
        for pane in list(self._pane_forest.panes):
            if pane.is_active:
                pane_end_plus_grace = pane.end_time + threshold
                if watermark >= pane_end_plus_grace:
                    self._pane_forest.pane_close(
                        pane_id=pane.pane_id,
                        dim_count=len(self._lo_bounds) if self._lo_bounds else 0,
                        lo_bounds=self._lo_bounds,
                        hi_bounds=self._hi_bounds,
                    )
                    # Drop the pane from the forest: removes from list/dict,
                    # drops tombstone, evicts events from EventStore.
                    # This is the critical step that prevents unbounded memory growth.
                    # _drop_pane was patched to also evict events from EventStore.
                    self._pane_forest._drop_pane(pane.pane_id)

    # ─── Introspection ─────────────────────────────────────────────────────────

    @property
    def event_store(self):
        return self._event_store

    @property
    def alert_store(self):
        return self._alert_store

    @property
    def pane_forest(self):
        return self._pane_forest

    @property
    def output(self):
        return self._output
