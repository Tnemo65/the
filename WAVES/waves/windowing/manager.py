"""Module 2.2 — Window Manager.

assign_window: gán pane_id + window_id vào mỗi event.
on_slide: tính delta khi watermark advance.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from waves.windowing.pane import (
    WindowConfig,
    WindowBuffer,
    Pane,
    WindowedEvent,
    WindowDelta,
    PaneManager,
    floor_ts,
    to_ms,
)


class WindowManager:
    """
    Assigns events to sliding windows and computes deltas on slide.

    Key invariants (checklist 2.2):
    - Membership: t ≤ event_time < t+W
    - buffers: Dict[pane_id, WindowBuffer] — O(1) lookup
    - event_store.put(event) called after pane_id + window_id assigned
    """

    def __init__(self, config: WindowConfig):
        self.config = config
        self.pane_mgr = PaneManager(config)
        # Dict[window_id, WindowBuffer] — O(1) window lookup
        self.buffers: Dict[str, WindowBuffer] = {}
        # Track pane creation per window for pane close detection
        self._window_panes: Dict[str, List[str]] = {}  # window_id → [pane_ids]

    # ─── Public API ────────────────────────────────────────────────────────────

    def assign_window(
        self,
        event: any,
        watermark_clock: datetime,
    ) -> List[WindowedEvent]:
        """
        Assign pane_id and window_id to event, add to window buffer.

        Returns:
            List[WindowedEvent] — always returns [wrapped] even if late,
            caller decides whether to process based on watermark.
        """
        config = self.config
        event_time = event.event_time

        # Compute window_id using membership rule: t ≤ event_time < t+W
        pane_count = int(config.window_width.total_seconds() / config.slide_step.total_seconds())
        earliest = floor_ts(event_time, config.slide_step)

        for i in range(pane_count):
            window_start = earliest - i * config.slide_step
            window_end = window_start + config.window_width
            if window_start <= event_time < window_end:
                window_id = f"w_{to_ms(window_start)}_{to_ms(window_end)}"
                event.window_id = window_id

                # Compute pane_id
                pane_size = config.pane_size or config.window_width
                pane_start = floor_ts(event_time, pane_size)
                pane_end = pane_start + pane_size
                pane_id = f"p_{to_ms(pane_start)}_{to_ms(pane_end)}"
                event.pane_id = pane_id

                # Add to window buffer (O(1))
                if window_id not in self.buffers:
                    self.buffers[window_id] = WindowBuffer(
                        window_id=window_id,
                        start_time=window_start,
                        end_time=window_end,
                        events=[],
                    )
                self.buffers[window_id].events.append(event)

                # Track pane per window
                if window_id not in self._window_panes:
                    self._window_panes[window_id] = []
                if pane_id not in self._window_panes[window_id]:
                    self._window_panes[window_id].append(pane_id)

                return [WindowedEvent(event, window_id, pane_id)]

        return []

    def on_slide(
        self,
        current_watermark: datetime,
    ) -> Tuple[List[WindowDelta], List[str]]:
        """
        Called when watermark advances. Computes deltas and identifies closing windows.

        Returns:
            Tuple of (deltas, closing_window_ids)
            - deltas: WindowDelta for each window affected by the slide
            - closing_window_ids: windows whose end_time <= current_watermark (Decision finalizes these)
        """
        old_watermark = current_watermark - self.config.slide_step
        deltas: List[WindowDelta] = []
        closing_windows: List[str] = []

        # Find windows that have closed
        closed_window_ids = [
            wid for wid, buf in list(self.buffers.items())
            if buf.end_time <= current_watermark
        ]

        for window_id in closed_window_ids:
            buf = self.buffers.pop(window_id)
            # All events in the closed window are deletes (window no longer active)
            deltas.append(WindowDelta(
                window_id=window_id,
                inserts=[],
                deletes=list(buf.events),
            ))
            closing_windows.append(window_id)
            # Clean up pane tracking
            self._window_panes.pop(window_id, None)

        # For active windows: compute incremental delta
        for window_id, buf in self.buffers.items():
            delta_inserts = []
            delta_deletes = []

            for event in buf.events:
                # Event enters the new slide window
                if old_watermark <= event.event_time < current_watermark:
                    delta_inserts.append(event)
                # Event exits the old slide window
                if current_watermark <= event.event_time < current_watermark + self.config.slide_step:
                    delta_deletes.append(event)

            if delta_inserts or delta_deletes:
                deltas.append(WindowDelta(
                    window_id=window_id,
                    inserts=delta_inserts,
                    deletes=delta_deletes,
                ))

        return deltas, closing_windows

    def closing_panes(self, current_watermark: datetime) -> List[str]:
        """Return pane_ids whose end_time <= current_watermark (for Weever DROP)."""
        pane_ids = []
        for pane in self.pane_mgr.active_panes():
            if pane.end_time <= current_watermark:
                pane_ids.append(pane.pane_id)
        return pane_ids
