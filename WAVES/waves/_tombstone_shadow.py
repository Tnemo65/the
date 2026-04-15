"""Shadow module: replaces waves.tombstone.filter for Python 3.8 compatibility.

The real filter.py uses list[str] annotation (Python 3.9+), causing
TypeError on Python 3.8. This module provides the same classes without
the incompatible annotation.

Must be registered in sys.modules BEFORE the first waves.tombstone import.
See pipeline.py top of file for the sys.modules registration.
"""
from typing import Dict, Set


class TombstoneFilter:
    """O(1) lookup filter attached to one pane."""

    def __init__(self):
        self._ids: Set[bytes] = set()

    def add(self, event_id: str):
        self._ids.add(event_id.encode())

    def contains(self, event_id: str) -> bool:
        return event_id.encode() in self._ids

    def clear(self):
        self._ids.clear()


class TombstoneManager:
    """
    Owner: Pipeline level.
    Must be a single instance shared by Decision and Weever.
    """

    def __init__(self):
        self._filters: Dict[str, TombstoneFilter] = {}

    def create_pane(self, pane_id: str):
        """Called by Weever when pane closes."""
        self._filters[pane_id] = TombstoneFilter()

    def add(self, event_id: str, pane_id: str):
        f = self._filters.get(pane_id)
        if f is not None:
            f.add(event_id)

    def add_many(self, event_ids, pane_id: str):
        """Mark multiple event_ids as retracted (Python 3.8 compatible)."""
        f = self._filters.get(pane_id)
        if f is not None:
            for eid in event_ids:
                f.add(eid)

    def contains(self, event_id: str, pane_id: str) -> bool:
        f = self._filters.get(pane_id)
        if f is None:
            return False
        return f.contains(event_id)

    def drop_pane(self, pane_id: str):
        """Called by Weever when pane is dropped. O(1)."""
        if pane_id in self._filters:
            self._filters[pane_id].clear()
            del self._filters[pane_id]

    def total_tombstoned(self) -> int:
        return sum(len(f._ids) for f in self._filters.values())
