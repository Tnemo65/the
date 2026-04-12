"""Module 2.8 — Decision: Watermark-aware alert decision layer."""

from dataclasses import dataclass
from typing import List, Optional

from waves.decision.alert_store import AlertStateStore, AlertStatus, AlertRecord
from waves.rapidash.candidate import CandidateViolation

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from waves.tombstone import TombstoneManager


# ─── Decision output types ─────────────────────────────────────────────────────

class AlertDecision:
    """Base class for all decision outputs."""
    decision_type: str

    def __init__(self, decision_type: str, **kwargs):
        self.decision_type = decision_type
        for k, v in kwargs.items():
            setattr(self, k, v)


class ProvisionalDecision(AlertDecision):
    def __init__(self, alert):
        super().__init__(decision_type="provisional", alert=alert)


class FinalDecision(AlertDecision):
    def __init__(self, alert):
        super().__init__(decision_type="final", alert=alert)


class RetractionDecision(AlertDecision):
    def __init__(self, retracted_alert, tombstoned_event_ids):
        super().__init__(
            decision_type="retraction",
            retracted_alert=retracted_alert,
            tombstoned_event_ids=tombstoned_event_ids,
        )


# ─── Core decision functions ───────────────────────────────────────────────────

def process_candidate(
    candidate: CandidateViolation,
    alert_store: AlertStateStore,
    tombstone_mgr: Optional['TombstoneManager'],
) -> List[AlertDecision]:
    """Process a candidate violation from Rapidash.

    Creates a PROVISIONAL alert if no active alert exists for this (dc, window, query).
    Skips if already PROVISIONAL or FINAL (idempotent).

    Returns: list of decisions (usually 0 or 1 ProvisionalDecision)
    """
    all_ids = [candidate.query_id] + candidate.matched_ids
    alert_id = f"alert_{candidate.dc_id}_{candidate.window_id}_{candidate.query_id}"

    # Skip if alert already exists with non-RETRACTED status
    existing = alert_store.get(alert_id)
    if existing is not None and existing.status != AlertStatus.RETRACTED:
        return []

    alert = AlertRecord(
        alert_id=alert_id,
        dc_id=candidate.dc_id,
        window_id=candidate.window_id,
        pane_id=candidate.pane_id,
        status=AlertStatus.PROVISIONAL,
        all_event_ids=all_ids,
        created_at=_now(),
    )
    alert_store.put(alert)
    return [ProvisionalDecision(alert)]


def finalize_window(
    window_id: str,
    alert_store: AlertStateStore,
) -> List[AlertDecision]:
    """Promote all PROVISIONAL alerts in a window to FINAL.

    Called by pipeline when watermark seals a window.
    Returns list of FinalDecision for all confirmed alerts.
    """
    decisions: List[AlertDecision] = []
    alerts = alert_store.get_by_window(window_id)

    for alert in alerts:
        if alert.status == AlertStatus.PROVISIONAL:
            alert.status = AlertStatus.FINAL
            alert.finalized_at = _now()
            alert_store.put(alert)
            decisions.append(FinalDecision(alert))

    return decisions


def retract_alert(
    alert_id: str,
    alert_store: AlertStateStore,
    tombstone_mgr: Optional['TombstoneManager'],
) -> List[AlertDecision]:
    """Retract an alert and tombstone all its event IDs.

    Returns empty list if alert doesn't exist or already retracted.
    """
    alert = alert_store.get(alert_id)
    if alert is None or alert.status == AlertStatus.RETRACTED:
        return []

    # Tombstone all event IDs associated with this alert
    if tombstone_mgr is not None:
        for event_id in alert.all_event_ids:
            tombstone_mgr.add(event_id, alert.pane_id)

    # Retract removes from store immediately
    retracted = alert_store.retract(alert_id)
    if retracted is None:
        return []
    return [RetractionDecision(retracted, alert.all_event_ids)]


def cleanup_expired(
    alert_store: AlertStateStore,
    ttl_seconds: float,
) -> int:
    """Delete FINAL alerts older than ttl_seconds.

    Returns count of deleted alerts.
    """
    now = _now()
    deleted = 0
    to_delete = []
    for alert_id, alert in list(alert_store._store.items()):
        if alert.status == AlertStatus.FINAL and alert.finalized_at is not None:
            age = (now - alert.finalized_at).total_seconds()
            if age > ttl_seconds:
                to_delete.append(alert_id)

    for alert_id in to_delete:
        alert_store.delete(alert_id)
        deleted += 1
    return deleted


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)
