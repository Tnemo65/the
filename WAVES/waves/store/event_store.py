"""Module 2.11b — EventStore (shared, pipeline-level).

event_id -> (DataEvent, pane_id, window_id).
Single instance, shared by WindowManager, Rapidash, LateHandler.
"""

from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from waves.ingestion.schema import DataEvent


class EventStore:
    """Shared event storage: event_id -> DataEvent with O(1) pane/window lookup.

    Pipeline creates ONE instance and injects into:
    - WindowManager (writes events after assigning pane_id/window_id)
    - Rapidash (reads pane_id for get_pane_id(event_id) calls)
    - LateHandler (reads full DataEvent for re-check logic)
    """

    def __init__(self):
        self._events: Dict[str, 'DataEvent'] = {}
        self._pane_map: Dict[str, str] = {}     # event_id -> pane_id
        self._window_map: Dict[str, str] = {}    # event_id -> window_id

    def put(self, event: 'DataEvent'):
        """Store event. Called by WindowManager after assigning pane_id + window_id."""
        self._events[event.event_id] = event
        if event.pane_id:
            self._pane_map[event.event_id] = event.pane_id
        if event.window_id:
            self._window_map[event.event_id] = event.window_id

    def get(self, event_id: str) -> Optional['DataEvent']:
        """Return full DataEvent or None."""
        return self._events.get(event_id)

    def get_pane_id(self, event_id: str) -> str:
        """O(1) pane_id lookup. Returns empty string if not found."""
        return self._pane_map.get(event_id, "")

    def get_window_id(self, event_id: str) -> str:
        """O(1) window_id lookup. Returns empty string if not found."""
        return self._window_map.get(event_id, "")

    def has(self, event_id: str) -> bool:
        return event_id in self._events

    def count(self) -> int:
        return len(self._events)

    def clear(self):
        self._events.clear()
        self._pane_map.clear()
        self._window_map.clear()
