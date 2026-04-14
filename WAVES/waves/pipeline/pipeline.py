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

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from waves.store import EventStore
from waves.tombstone import TombstoneManager
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

        # ─── Weever ─────────────────────────────────────────────────────────
        from waves.weever import PaneForest
        self._pane_forest = PaneForest.create(tombstone_mgr=self._tombstone_mgr)

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

    def cleanup(self) -> int:
        """Delete expired FINAL alerts. Returns count deleted."""
        from waves.decision.decision import cleanup_expired
        return cleanup_expired(self._alert_store, self.config.alert_ttl_seconds)

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
                # DC1/2/3 encode the NORMAL case → invert for violation detection
                if op == PredicateType.EQUAL:
                    if not (l != r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.LESS:
                    if not (l >= r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.LESS_EQUAL:
                    if not (l > r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.GREATER:
                    if not (l <= r):
                        all_predicate_pass = False
                        break
                elif op == PredicateType.GREATER_EQUAL:
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
