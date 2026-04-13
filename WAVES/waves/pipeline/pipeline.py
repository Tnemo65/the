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
        from waves.optimizer import OptimizerConfig, GreedyRuleGrouper
        self._optimizer_cfg = OptimizerConfig(
            k_max=config.k_max,
            infinite_padding=20.0,
            static_bounds={},
        )
        self._rule_grouper = GreedyRuleGrouper(self._optimizer_cfg)
        self._active_boxes: List = []   # List[ActiveBox]
        self._dim_map: Dict[str, int] = {}   # col -> dim_index
        self._lo_bounds: tuple = ()
        self._hi_bounds: tuple = ()

        # ─── Logical Engine ────────────────────────────────────────────────
        from waves.logical_engine import LogicalEngine, ElasticBoxConfig
        self._logical_engine = LogicalEngine(
            ElasticBoxConfig(alpha=0.05, delta_min=1.0, delta_max=50.0),
        )

        # ─── Alert Output ────────────────────────────────────────────────
        self._output = AlertOutput(
            alert_sink=config.alert_sink,
            meta_sink=config.meta_sink,
        )

        # ─── Late Handler ─────────────────────────────────────────────────
        from waves.late_handler import LateHandlerConfig
        self._late_handler_cfg = LateHandlerConfig(
            wait_for_late_seconds=config.wait_for_late_seconds,
            window_config=config.window_config(),
        )

    # ─── Public API ─────────────────────────────────────────────────────────────

    def load_dc_rules(self, rules: List):
        """Load DC rules and build active boxes. Call after __init__ with rules."""
        from waves.optimizer import DCParser
        self._dc_rules = rules
        parser = DCParser()
        enriched = parser.parse_dc_rules(rules)
        groups = self._rule_grouper.group(enriched)
        from waves.optimizer import build_active_boxes
        self._active_boxes = build_active_boxes(enriched, groups, self._optimizer_cfg)

        # Build dim_map and bounds from groups
        if groups:
            all_dims = set()
            for g in groups:
                all_dims.update(g.column_to_dim.values())
            max_dim = max(all_dims) if all_dims else -1
            self._dim_map = {}
            for g in groups:
                self._dim_map.update(g.column_to_dim)
            if max_dim >= 0:
                self._lo_bounds = tuple(0.0 for _ in range(max_dim + 1))
                self._hi_bounds = tuple(100.0 for _ in range(max_dim + 1))
            else:
                self._lo_bounds = ()
                self._hi_bounds = ()

    def process(self, event) -> List:
        """Process a DataEvent through the full pipeline.

        Flow:
        1. windowing -> assign pane_id + window_id
        2. basic_dq -> CheckResult list
        3. event_store.put()
        4. pane_forest.pane_insert()
        5. logical_engine + optimizer + rapidash -> candidates
        6. decision -> decisions
        7. output.emit() each decision

        Args:
            event: DataEvent (from connectors like CSVConnector, KafkaConnector)
        Returns list of decisions.
        """
        from waves.basic_dq import DQResult

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

        # Step 5-6: insert into pane
        if event.pane_id and event.window_id:
            self._pane_forest.pane_insert(
                event_time=event.event_time,
                event_id=event.event_id,
                point=self._extract_point(event),
                window_id=event.window_id,
                config=self.config.window_config(),
            )
            # Try to close pane if enough events (auto-close heuristics)
            pane = self._pane_forest.get_pane_by_id(event.pane_id)
            if pane is not None and not pane.is_active:
                pass  # Already closed

        # Step 7: rapidash traversal (if boxes and pane exist)
        decisions = []
        if self._active_boxes and event.pane_id:
            pane = self._pane_forest.get_pane_by_id(event.pane_id)
            if pane is not None and pane.kdtree is not None:
                candidates = self._traverse_pane(pane, event)
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

        decisions = handle_late_event(
            late_event=late_event,
            alert_store=self._alert_store,
            tombstone_mgr=self._tombstone_mgr,
            pane_forest=self._pane_forest,
            config=self._late_handler_cfg,
            dim_map=self._dim_map,
            lo_bounds=self._lo_bounds,
            hi_bounds=self._hi_bounds,
            active_boxes=self._active_boxes,
            get_pane_id_fn=self._event_store.get_pane_id,
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
        if not self._dim_map:
            return ()
        max_dim = max(self._dim_map.values())
        coords = [0.0] * (max_dim + 1)
        for col, dim in self._dim_map.items():
            val = event.attributes.get(col, 0.0)
            try:
                coords[dim] = float(val)
            except (TypeError, ValueError):
                coords[dim] = 0.0
        return tuple(coords)

    def _traverse_pane(self, pane, event) -> List:
        """Traverse pane kdtree with active boxes, return candidate violations."""
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
