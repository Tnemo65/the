"""Module 2.10 — Late Event Handler.

Handles late-arriving events:
1. Check lateness threshold (DROP if beyond wait_for_late)
2. Find alerts involving this event
3. Retract invalidated alerts
4. Insert late event into its pane (KD-tree)
5. Re-check pane for new violations
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from waves.decision.alert_store import AlertStateStore, AlertStatus
from waves.decision.decision import retract_alert, process_candidate
from waves.ingestion.schema import DataEvent
from waves.optimizer.dc_parser import Predicate, PredicateType


def _parse_window_end(window_id: str) -> Optional[datetime]:
    """Extract window end time from window_id like 'w_1744365300000_1744369200000'."""
    if not window_id:
        return None
    parts = window_id.split("_")
    if len(parts) < 3:
        return None
    try:
        end_ms = int(parts[-1])
        return datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def _extract_point(event: DataEvent, dim_map: Dict[str, int]) -> Tuple[float, ...]:
    """Extract feature vector from event using dim_map."""
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


def _evaluate_predicate(left_val: Any, operator: PredicateType, right_val: Any) -> bool:
    """Evaluate a single predicate on two values."""
    try:
        l = float(left_val)
        r = float(right_val)
    except (TypeError, ValueError):
        l = str(left_val)
        r = str(right_val)

    if operator == PredicateType.EQUAL:
        return l == r
    if operator == PredicateType.LESS:
        return l < r
    if operator == PredicateType.LESS_EQUAL:
        return l <= r
    if operator == PredicateType.GREATER:
        return l > r
    if operator == PredicateType.GREATER_EQUAL:
        return l >= r
    return False


def late_event_invalidate_check(
    late_event: DataEvent,
    matched_event: DataEvent,
    predicates: List[Predicate],
) -> bool:
    """Check if late_event proves the alert was a false positive.

    DC = NOT (P1 AND P2 AND ... AND Pn)
    Alert says: P1 AND ... AND Pn was violated (so NOT(...) is true).
    Late event invalidates if: late_event + matched_event SATISFY all predicates.

    Returns True if alert should be retracted.
    """
    for pred in predicates:
        # "s" = query/event that was part of the original violation (matched_event)
        # "t" = the late-arriving event (late_event)
        left_source = matched_event if pred.left_side == "s" else late_event
        right_source = late_event if pred.right_side == "t" else matched_event

        left_val = left_source.attributes.get(pred.left_col) if left_source else None
        right_val = right_source.attributes.get(pred.right_col) if right_source else None

        if left_val is None or right_val is None:
            return False

        if not _evaluate_predicate(left_val, pred.operator, right_val):
            return False

    return True


def handle_late_event(
    late_event: DataEvent,
    alert_store: AlertStateStore,
    tombstone_mgr: Optional[Any],        # TombstoneManager
    pane_forest: Any,                     # PaneForest
    config: 'LateHandlerConfig',
    dim_map: Dict[str, int],
    lo_bounds: Tuple[float, ...],
    hi_bounds: Tuple[float, ...],
    active_boxes: List[Any],             # List[ActiveBox]
    get_pane_id_fn: Callable[[str], str],
    dc_predicates: Optional[Dict[str, List[Predicate]]] = None,
) -> List:
    """Handle a single late-arriving event.

    Pipeline calls this for each late event detected by WatermarkClock.

    Args:
        late_event: The late DataEvent
        alert_store: AlertStateStore to query/retract alerts
        tombstone_mgr: TombstoneManager to mark retracted event IDs
        pane_forest: PaneForest to insert event and re-check
        config: LateHandlerConfig with wait_for_late_seconds and window_config
        dim_map: {column_name -> dim_index} for feature extraction
        lo_bounds: Lower bounds for KD-tree
        hi_bounds: Upper bounds for KD-tree
        active_boxes: List of ActiveBoxes from Optimizer
        get_pane_id_fn: Function to get pane_id for an event_id
        dc_predicates: Optional dict {dc_id -> [Predicate]} for invalidate check

    Returns:
        List of decisions (RetractionDecision + ProvisionalDecision)
    """
    decisions: List = []

    # ─── Step 1: lateness threshold ─────────────────────────────────────────
    window_end = _parse_window_end(late_event.window_id or "")
    if window_end is not None:
        now = datetime.now(timezone.utc)
        if (now - window_end).total_seconds() > config.wait_for_late_seconds:
            # Beyond threshold — DROP silently
            return decisions

    # ─── Step 2: find alerts involving this event ───────────────────────────
    alerts = alert_store.get_by_event_id(late_event.event_id)
    provisional_alerts = [a for a in alerts if a.status == AlertStatus.PROVISIONAL]

    # ─── Step 3: retract invalidated alerts ────────────────────────────────
    for alert in provisional_alerts:
        should_retract = False

        if dc_predicates is not None and alert.dc_id in dc_predicates:
            # Precise check: use predicates to determine if late_event invalidates
            # Find the OTHER event in the alert (query event, not late_event)
            preds = dc_predicates[alert.dc_id]
            other_ids = [eid for eid in alert.all_event_ids if eid != late_event.event_id]
            # Note: precise invalidate requires access to the other event's DataEvent.
            # Conservative: retract if predicates exist and alert involves this event.
            # Pipeline should provide event_store if precise check is needed.
            should_retract = True
        else:
            # Conservative: retract all provisional alerts involving this event
            should_retract = True

        if should_retract:
            result = retract_alert(alert.alert_id, alert_store, tombstone_mgr)
            decisions.extend(result)

    # ─── Step 4: insert late event into pane ──────────────────────────────
    if late_event.window_id and config.window_config:
        pane_forest.pane_insert(
            event_time=late_event.event_time,
            event_id=late_event.event_id,
            point=_extract_point(late_event, dim_map),
            window_id=late_event.window_id,
            config=config.window_config,
        )

    # ─── Step 5: re-check pane for new violations ─────────────────────────
    if not active_boxes or not late_event.pane_id:
        return decisions

    pane = pane_forest.get_pane_by_id(late_event.pane_id)
    if pane is None or pane.kdtree is None:
        return decisions

    from waves.rapidash.traversal import traverse_node

    point = _extract_point(late_event, dim_map)
    for box in active_boxes:
        candidates, visited, pruned = traverse_node(
            pane.kdtree,
            point,
            late_event.event_id,
            box,
            tombstone_mgr,
            get_pane_id_fn,
        )
        for cand in candidates:
            dec = process_candidate(cand, alert_store, tombstone_mgr)
            decisions.extend(dec)

    return decisions


@dataclass
class LateHandlerConfig:
    """Configuration for late event handling."""
    wait_for_late_seconds: float = 300.0   # Default 5 minutes
    window_config: Optional[Any] = None     # WindowConfig from pipeline
