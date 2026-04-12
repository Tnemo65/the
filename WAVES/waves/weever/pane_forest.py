"""Module 2.7 — Weever: Pane-based forest with incremental insert/delete."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from waves.windowing.pane import Pane, WindowConfig
from waves.rapidash import bulk_load

if TYPE_CHECKING:
    from waves.tombstone import TombstoneManager


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


@dataclass
class PaneForest:
    """Flat list of panes + O(1) lookup. Shared by Rapidash for tree roots.

    TombstoneManager is injected by pipeline (not created here).
    """
    panes: List[Pane]
    panes_by_id: Dict[str, Pane]

    tombstone_mgr: Optional['TombstoneManager'] = None

    @classmethod
    def create(cls, tombstone_mgr: Optional['TombstoneManager'] = None) -> 'PaneForest':
        return cls(panes=[], panes_by_id={}, tombstone_mgr=tombstone_mgr)

    # ─── Pane lookup ─────────────────────────────────────────────────────────

    def find_or_create_pane(
        self,
        event_time: datetime,
        window_id: str,
        config: WindowConfig,
    ) -> Pane:
        """Find or create pane for event_time, returning the pane."""
        pane_size = config.pane_size or config.window_width
        pane_start = _floor_ts(event_time, pane_size)
        pane_end = pane_start + pane_size
        pane_id = f"p_{int(pane_start.timestamp()*1000)}_{int(pane_end.timestamp()*1000)}"

        if pane_id in self.panes_by_id:
            return self.panes_by_id[pane_id]

        new_pane = Pane(
            pane_id=pane_id,
            start_time=pane_start,
            end_time=pane_end,
            window_id=window_id,
            is_active=True,
            buffer=[],
            size_hint=0,
        )
        self.panes.append(new_pane)
        self.panes_by_id[pane_id] = new_pane
        return new_pane

    def get_pane_by_id(self, pane_id: str) -> Optional[Pane]:
        return self.panes_by_id.get(pane_id)

    def get_roots(self, group_id: str) -> List[Any]:
        """Return KD-tree roots from all panes for a given group."""
        return [p.kdtree for p in self.panes if p.kdtree is not None]

    def active_panes(self) -> List[Pane]:
        return [p for p in self.panes if p.is_active]

    def closed_panes(self) -> List[Pane]:
        return [p for p in self.panes if not p.is_active]

    # ─── Incremental insert ───────────────────────────────────────────────────

    def pane_insert(
        self,
        event_time: datetime,
        event_id: str,
        point: tuple,
        window_id: str,
        config: WindowConfig,
    ) -> Pane:
        """Insert a point into the active pane's buffer.

        Called on every incoming event.
        Late events (closed pane) → NOT inserted here → handled by LateHandler.
        """
        pane = self.find_or_create_pane(event_time, window_id, config)
        if pane.is_active:
            pane.buffer.append((point, event_id))
            pane.size_hint += 1
        return pane

    # ─── Pane close ─────────────────────────────────────────────────────────

    def pane_close(
        self,
        pane_id: str,
        dim_count: int,
        lo_bounds: tuple,
        hi_bounds: tuple,
    ):
        """Bulk-load pane's buffer into KD-tree, then clear buffer.

        Called by window_slide when pane's event time is behind watermark.
        """
        pane = self.panes_by_id.get(pane_id)
        if pane is None:
            return

        if pane.buffer:
            pane.kdtree = bulk_load(
                pane.buffer,
                dim_count=dim_count,
                lo_bounds=lo_bounds,
                hi_bounds=hi_bounds,
            )
            pane.buffer = []
        pane.is_active = False

        if self.tombstone_mgr is not None:
            self.tombstone_mgr.create_pane(pane_id)

    # ─── Window slide ────────────────────────────────────────────────────────

    def window_slide(
        self,
        watermark: datetime,
        config: WindowConfig,
        dim_count: int,
        lo_bounds: tuple,
        hi_bounds: tuple,
    ) -> List[Pane]:
        """Close and drop all panes whose end_time <= watermark.

        Returns list of dropped panes.
        """
        dropped: List[Pane] = []
        for pane in list(self.panes):   # iterate over copy
            if pane.end_time <= watermark:
                self.pane_close(pane.pane_id, dim_count, lo_bounds, hi_bounds)
                self._drop_pane(pane.pane_id)
                dropped.append(pane)
        return dropped

    def _drop_pane(self, pane_id: str):
        """O(1) drop — removes from list and dict."""
        self.panes = [p for p in self.panes if p.pane_id != pane_id]
        self.panes_by_id.pop(pane_id, None)
        if self.tombstone_mgr is not None:
            self.tombstone_mgr.drop_pane(pane_id)


# ─── Helper ──────────────────────────────────────────────────────────────────

def _floor_ts(dt: datetime, step: timedelta) -> datetime:
    """Floor datetime to the nearest step boundary."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    total_ms = int((dt - epoch).total_seconds() * 1000)
    step_ms = int(step.total_seconds() * 1000)
    if step_ms == 0:
        return dt
    floored_ms = (total_ms // step_ms) * step_ms
    return datetime.fromtimestamp(floored_ms / 1000, tz=timezone.utc)
