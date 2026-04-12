"""Module 2.9 — Tombstone: O(1) retracted-event filter.

Owner: Pipeline level. ONE instance, shared by Decision, Weever, LateHandler.
NOT instantiated by individual modules.
"""

from dataclasses import dataclass
from typing import Dict, Set, Optional, Any


class TombstoneFilter:
    """O(1) lookup filter for one pane's retracted event IDs."""

    def __init__(self):
        self._ids: Set[str] = set()

    def add(self, event_id: str):
        self._ids.add(event_id)

    def contains(self, event_id: str) -> bool:
        return event_id in self._ids

    def clear(self):
        self._ids.clear()


class TombstoneManager:
    """O(1) pane-scoped tombstone filter.

    Pipeline creates ONE instance and injects it into:
    - Decision layer (retract_alert marks event_ids as retracted)
    - Weever PaneForest (drop_pane clears filters)
    - LateHandler (upsert checks before processing)
    """

    def __init__(self):
        self._filters: Dict[str, TombstoneFilter] = {}  # pane_id → filter

    # ─── Pane lifecycle ───────────────────────────────────────────────────────

    def create_pane(self, pane_id: str):
        """Called by Weever when a pane closes — creates empty filter for it."""
        if pane_id not in self._filters:
            self._filters[pane_id] = TombstoneFilter()

    def drop_pane(self, pane_id: str):
        """O(1) drop — pipeline calls this when pane expires from watermark slide."""
        self._filters.pop(pane_id, None)

    # ─── Retraction ───────────────────────────────────────────────────────────

    def add(self, event_id: str, pane_id: str):
        """Mark one event_id as retracted in its pane's filter."""
        f = self._filters.get(pane_id)
        if f is not None:
            f.add(event_id)

    def add_many(self, event_ids: list[str], pane_id: str):
        """Mark multiple event_ids as retracted."""
        f = self._filters.get(pane_id)
        if f is not None:
            for eid in event_ids:
                f.add(eid)

    # ─── Lookup ───────────────────────────────────────────────────────────────

    def contains(self, event_id: str, pane_id: str) -> bool:
        """O(1) check if event_id has been retracted in pane_id."""
        f = self._filters.get(pane_id)
        if f is None:
            return False
        return f.contains(event_id)

    # ─── Introspection ─────────────────────────────────────────────────────────

    def pane_count(self) -> int:
        return len(self._filters)

    def total_tombstoned(self) -> int:
        return sum(len(f._ids) for f in self._filters.values())

    def get_filter(self, pane_id: str) -> Optional[TombstoneFilter]:
        return self._filters.get(pane_id)
