"""Module 2.2 — WatermarkClock.

Tracks event-time watermark. Does NOT block the main event flow.
Watermark advances when new data arrives, not by waiting.
"""
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class WatermarkConfig:
    """Configuration for watermark behavior."""
    watermark_policy: str = "event_time"   # "event_time" or "ingestion_time"
    wait_for_late: int = 300               # seconds — grace period before finalizing
    watermark_advance_interval: int = 1    # seconds between watermark advances


class WatermarkClock:
    """
    Tracks event-time watermark.

    Design doc (state_time_semantics):
    - Watermark does NOT block the main flow
    - It is used for: (1) closing windows, (2) garbage collection,
      (3) transitioning provisional → final alerts
    - Watermark advances when new data arrives, not by waiting

    Key methods:
    - on_event(event): update watermark on each new event
    - get(): current watermark value
    - is_ready(pane_end_time): check if pane is ready to be processed
    - has_advanced(): returns True if watermark advanced since last check
    """

    def __init__(self, config: WatermarkConfig | None = None):
        self.config = config or WatermarkConfig()
        self._watermark: datetime | None = None
        self._last_watermark: datetime | None = None  # for has_advanced() check
        self._event_count: int = 0

    def on_event(self, event) -> None:
        """Advance watermark if event's event_time is ahead."""
        ts = event.event_time
        if self._watermark is None or ts > self._watermark:
            self._watermark = ts
        self._event_count += 1

    def get(self) -> datetime:
        """Return current watermark. Defaults to epoch if no events yet."""
        if self._watermark is None:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        return self._watermark

    def is_ready(self, pane_end_time: datetime) -> bool:
        """
        Check if pane is ready for processing.

        A pane is ready when watermark >= pane_end_time + wait_for_late grace.
        This gives late events time to arrive before the pane is finalized.
        """
        wm = self.get()
        grace = self.config.wait_for_late
        if grace <= 0:
            return wm >= pane_end_time
        from datetime import timedelta
        threshold = pane_end_time + timedelta(seconds=grace)
        return wm >= threshold

    def has_advanced(self) -> bool:
        """Return True if watermark advanced since last has_advanced() call."""
        if self._watermark is None:
            return False
        if self._last_watermark is None:
            if self._event_count > 0:
                self._last_watermark = self._watermark
                return True
            return False
        result = self._watermark > self._last_watermark
        if result:
            self._last_watermark = self._watermark
        return result

    def advance(self, new_watermark: datetime) -> None:
        """
        Manually set watermark to a specific value.
        Used by tests and for simulating watermark advancement.
        """
        self._last_watermark = self._watermark
        self._watermark = new_watermark
        self._event_count += 1

    def reset(self) -> None:
        """Reset watermark. Used by tests."""
        self._watermark = None
        self._last_watermark = None
        self._event_count = 0
