"""Module 2.11 — Alert Output bridge.

Bridges Decision layer -> Alert stream (provisional/final/retraction) and
BasicDQ -> Meta stream (window-level quality stats) to sinks.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from waves.basic_dq.meta_stream import MetaEvent, WindowMeta


@dataclass
class AlertEvent:
    """An alert event emitted to the output stream."""
    event_type: str           # "provisional" | "final" | "retraction"
    alert_id: str
    dc_id: str
    window_id: str
    pane_id: str
    event_id: str             # primary query event ID
    event_time: datetime
    output_time: datetime
    detail: Dict = field(default_factory=dict)  # extra context


class AlertOutput:
    """Output bridge: Decision -> Alert stream, BasicDQ -> Meta stream.

    Receives AlertDecision objects from the Decision layer and emits structured
    AlertEvents to the configured sink. Also forwards WindowMeta from BasicDQ
    as MetaEvents.

    Usage:
        output = AlertOutput(alert_sink=print_alert, meta_sink=print_meta)
        output.emit(provisional_decision)
        output.emit(final_decision)
        output.emit(retraction_decision)
        output.emit_meta(window_meta)
    """

    def __init__(
        self,
        alert_sink: Optional[Callable[[AlertEvent], None]] = None,
        meta_sink: Optional[Callable[[MetaEvent], None]] = None,
    ):
        self._alert_sink = alert_sink
        self._meta_sink = meta_sink
        self._alert_history: List[AlertEvent] = []
        self._meta_history: List[MetaEvent] = []

    def emit(self, decision) -> Optional[AlertEvent]:
        """Convert an AlertDecision to an AlertEvent and emit to sink.

        Dispatches by decision_type:
        - "provisional"  -> AlertEvent with alert details
        - "final"       -> AlertEvent marking confirmation
        - "retraction"   -> AlertEvent with tombstoned event IDs
        Returns None if decision_type is unknown.
        """
        dt = decision.decision_type
        now = _utcnow()

        if dt in ("provisional", "final"):
            alert = decision.alert
            event = AlertEvent(
                event_type=dt,
                alert_id=alert.alert_id,
                dc_id=alert.dc_id,
                window_id=alert.window_id,
                pane_id=alert.pane_id,
                event_id=alert.all_event_ids[0] if alert.all_event_ids else "",
                event_time=alert.created_at,
                output_time=now,
                detail={},
            )

        elif dt == "retraction":
            event = AlertEvent(
                event_type="retraction",
                alert_id=decision.retracted_alert.alert_id,
                dc_id=decision.retracted_alert.dc_id,
                window_id=decision.retracted_alert.window_id,
                pane_id=decision.retracted_alert.pane_id,
                event_id=decision.retracted_alert.all_event_ids[0]
                if decision.retracted_alert.all_event_ids
                else "",
                event_time=decision.retracted_alert.created_at,
                output_time=now,
                detail={"tombstoned_event_ids": list(decision.tombstoned_event_ids)},
            )

        else:
            # Unknown decision type — skip
            return None

        self._alert_history.append(event)
        if self._alert_sink:
            self._alert_sink(event)
        return event

    def emit_meta(self, meta: WindowMeta) -> Optional[MetaEvent]:
        """Convert a WindowMeta (from BasicDQ) to a MetaEvent and emit to sink."""
        fail_count = sum(meta.fail_counts.values())
        pass_count = meta.total_count - fail_count
        event = MetaEvent(
            window_id=meta.window_id,
            timestamp=_utcnow(),
            total=meta.total_count,
            pass_count=pass_count,
            fail_count=fail_count,
            violation_counts=dict(meta.fail_counts),
        )
        self._meta_history.append(event)
        if self._meta_sink:
            self._meta_sink(event)
        return event

    def get_alert_history(self) -> List[AlertEvent]:
        """Return copy of alert history."""
        return list(self._alert_history)

    def get_meta_history(self) -> List[MetaEvent]:
        """Return copy of meta history."""
        return list(self._meta_history)

    def clear(self):
        """Clear all histories."""
        self._alert_history.clear()
        self._meta_history.clear()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
