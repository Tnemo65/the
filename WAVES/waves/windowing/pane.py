"""Module 2.2 — Window Manager data structures.

Pane is defined HERE and imported by all other modules (Weever, Tombstone, etc.)
ONE definition only.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any


# ─── Helper functions (Phụ lục A trong checklist) ────────────────────────────

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def floor_ts(dt: datetime, step: timedelta) -> datetime:
    """Floor datetime to the nearest step boundary."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    total_ms = int((dt - epoch).total_seconds() * 1000)
    step_ms = int(step.total_seconds() * 1000)
    if step_ms == 0:
        return dt
    floored_ms = (total_ms // step_ms) * step_ms
    return datetime.fromtimestamp(floored_ms / 1000, tz=timezone.utc)


# ─── Config ──────────────────────────────────────────────────────────────────

@dataclass
class WindowConfig:
    """Configuration for sliding windows."""
    window_width: timedelta      # W
    slide_step: timedelta        # S  (S == W → tumbling)
    pane_size: Optional[timedelta] = None  # None → equals window_width


# ─── Core data structures ─────────────────────────────────────────────────────

@dataclass
class WindowBuffer:
    """Buffer for a single window — not merged with Pane."""
    window_id: str
    start_time: datetime
    end_time: datetime
    events: List[Any] = field(default_factory=list)  # List[DataEvent]


@dataclass
class Pane:
    """
    Single definition of Pane. Used by:
    - WindowManager (membership)
    - Weever (KD-tree container)
    - Tombstone (lifecycle)

    Imported from: waves.windowing.pane
    """
    pane_id: str
    start_time: datetime
    end_time: datetime
    window_id: str                  # Which window this pane belongs to
    # Weever fields (set after pane close)
    kdtree: Optional[Any] = None
    is_active: bool = True
    # buffer: List[(point_tuple, event_id)] — managed by Weever
    buffer: List[Tuple[Tuple[float, ...], str]] = field(default_factory=list)
    size_hint: int = 0


@dataclass
class WindowedEvent:
    """DataEvent wrapped with window metadata."""
    data_event: Any  # DataEvent
    window_id: str
    pane_id: Optional[str] = None


@dataclass
class WindowDelta:
    """Delta of events entering/exiting a window on slide."""
    window_id: str
    inserts: List[Any] = field(default_factory=list)   # List[DataEvent]
    deletes: List[Any] = field(default_factory=list)    # List[DataEvent]


# ─── PaneManager (dict-based buffer management, O(1)) ────────────────────────

class PaneManager:
    """
    Manages panes for a single window using dict-based O(1) lookup.
    Pane membership by event_time.
    """

    def __init__(self, config: WindowConfig):
        self.config = config
        self._panes: Dict[str, Pane] = {}  # pane_id → Pane

    def pane_size(self) -> timedelta:
        return self.config.pane_size or self.config.window_width

    def compute_pane_id(self, event_time: datetime) -> str:
        ps = self.pane_size()
        pane_start = floor_ts(event_time, ps)
        pane_end = pane_start + ps
        return f"p_{to_ms(pane_start)}_{to_ms(pane_end)}"

    def compute_window_id(self, event_time: datetime) -> str:
        ws = self.config.window_width
        ss = self.config.slide_step
        pane_count = int(ws.total_seconds() / ss.total_seconds())
        earliest = floor_ts(event_time, ss)
        for i in range(pane_count):
            window_start = earliest - i * ss
            window_end = window_start + ws
            if window_start <= event_time < window_end:
                return f"w_{to_ms(window_start)}_{to_ms(window_end)}"
        return ""

    def find_or_create(self, pane_id: str, window_id: str) -> Pane:
        if pane_id not in self._panes:
            ps = self.pane_size()
            pane_start = datetime.fromtimestamp(
                int(pane_id.split("_")[1]) / 1000, tz=timezone.utc
            )
            self._panes[pane_id] = Pane(
                pane_id=pane_id,
                start_time=pane_start,
                end_time=pane_start + ps,
                window_id=window_id,
                is_active=True,
                buffer=[],
                size_hint=0,
            )
        return self._panes[pane_id]

    def get(self, pane_id: str) -> Optional[Pane]:
        return self._panes.get(pane_id)

    def drop(self, pane_id: str):
        self._panes.pop(pane_id, None)

    def active_panes(self) -> List[Pane]:
        return list(self._panes.values())
