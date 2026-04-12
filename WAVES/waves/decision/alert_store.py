"""Module 2.8 — Decision: Alert State Store and data structures."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class AlertStatus(Enum):
    PROVISIONAL = "provisional"   # Watermark not reached, pending confirmation
    FINAL = "final"              # Watermark reached, confirmed
    RETRACTED = "retracted"      # Retracted after late data contradiction


@dataclass
class AlertRecord:
    """One DC violation alert tracked across provisional→final→retract lifecycle."""
    alert_id: str
    dc_id: str
    window_id: str
    pane_id: str
    status: AlertStatus
    all_event_ids: List[str]      # query_id + matched_ids
    created_at: datetime
    finalized_at: Optional[datetime] = None
    retracted_at: Optional[datetime] = None


class AlertStateStore:
    """KV store: alert_id → AlertRecord. Tracks provisional/final/retracted alerts.

    Lookup indexes:
    - _store: alert_id → AlertRecord
    - _by_window: window_id → [alert_id]
    - _by_event: event_id → [alert_id]
    """

    def __init__(self):
        self._store: Dict[str, AlertRecord] = {}
        self._by_window: Dict[str, List[str]] = {}
        self._by_event: Dict[str, List[str]] = {}

    # ─── Write ────────────────────────────────────────────────────────────────

    def put(self, alert: AlertRecord):
        """Insert or update an alert."""
        self._store[alert.alert_id] = alert
        # Index by window
        if alert.window_id not in self._by_window:
            self._by_window[alert.window_id] = []
        if alert.alert_id not in self._by_window[alert.window_id]:
            self._by_window[alert.window_id].append(alert.alert_id)
        # Index by event
        for eid in alert.all_event_ids:
            if eid not in self._by_event:
                self._by_event[eid] = []
            if alert.alert_id not in self._by_event[eid]:
                self._by_event[eid].append(alert.alert_id)

    def delete(self, alert_id: str):
        """Permanently remove an alert (after TTL expiry)."""
        self._store.pop(alert_id, None)
        for wids in self._by_window.values():
            if alert_id in wids:
                wids.remove(alert_id)
        for eids in self._by_event.values():
            if alert_id in eids:
                eids.remove(alert_id)

    def retract(self, alert_id: str) -> Optional[AlertRecord]:
        """Mark alert as RETRACTED and remove from indexes.

        Returns the retracted record.
        """
        alert = self._store.get(alert_id)
        if alert is None:
            return None
        alert.status = AlertStatus.RETRACTED
        alert.retracted_at = _now()
        # Retracted alerts are NOT kept in store — tombstoned immediately
        self.delete(alert_id)
        return alert

    # ─── Read ────────────────────────────────────────────────────────────────

    def get(self, alert_id: str) -> Optional[AlertRecord]:
        return self._store.get(alert_id)

    def get_by_window(self, window_id: str) -> List[AlertRecord]:
        alert_ids = self._by_window.get(window_id, [])
        return [self._store[a] for a in alert_ids if a in self._store]

    def get_by_window_and_dc(self, window_id: str, dc_id: str) -> List[AlertRecord]:
        return [a for a in self.get_by_window(window_id) if a.dc_id == dc_id]

    def get_by_event_id(self, event_id: str) -> List[AlertRecord]:
        alert_ids = self._by_event.get(event_id, [])
        return [self._store[a] for a in alert_ids if a in self._store]

    def has_active(self, alert_id: str) -> bool:
        """True if alert exists and status is not RETRACTED."""
        a = self._store.get(alert_id)
        return a is not None and a.status != AlertStatus.RETRACTED

    # ─── Introspection ──────────────────────────────────────────────────────

    def count(self) -> int:
        return len(self._store)

    def count_by_status(self, status: AlertStatus) -> int:
        return sum(1 for a in self._store.values() if a.status == status)

    def clear(self):
        self._store.clear()
        self._by_window.clear()
        self._by_event.clear()


def _now() -> datetime:
    return datetime.now(timezone.utc)
