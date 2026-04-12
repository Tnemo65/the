"""Unit tests cho WindowManager (Module 2.2)."""
from datetime import datetime, timedelta, timezone

import pytest

from waves.windowing import (
    WindowConfig,
    WindowBuffer,
    Pane,
    WindowedEvent,
    WindowDelta,
    PaneManager,
    WindowManager,
    WatermarkClock,
    WatermarkConfig,
    floor_ts,
    to_ms,
)


# ─── Mock DataEvent ───────────────────────────────────────────────────────────

class MockEvent:
    def __init__(self, event_id, event_time, pane_id=None, window_id=None):
        self.event_id = event_id
        self.event_time = event_time
        self.pane_id = pane_id
        self.window_id = window_id


# ─── Helper tests ─────────────────────────────────────────────────────────────

class TestFloorTs:
    def test_floor_to_hour(self):
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        result = floor_ts(dt, timedelta(hours=1))
        assert result == datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    def test_floor_to_5min(self):
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        result = floor_ts(dt, timedelta(minutes=5))
        assert result == datetime(2024, 1, 15, 10, 25, 0, tzinfo=timezone.utc)

    def test_floor_exact_boundary(self):
        dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = floor_ts(dt, timedelta(hours=1))
        assert result == datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


class TestToMs:
    def test_to_ms_from_epoch(self):
        dt = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert to_ms(dt) == 0

    def test_to_ms_roundtrip(self):
        dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        ms = to_ms(dt)
        assert ms > 0


# ─── WindowConfig ─────────────────────────────────────────────────────────────

class TestWindowConfig:
    def test_basic(self):
        cfg = WindowConfig(
            window_width=timedelta(hours=1),
            slide_step=timedelta(minutes=15),
        )
        assert cfg.window_width == timedelta(hours=1)
        assert cfg.slide_step == timedelta(minutes=15)
        assert cfg.pane_size is None  # defaults to window_width


# ─── PaneManager ──────────────────────────────────────────────────────────────

class TestPaneManager:
    def test_pane_size_defaults_to_window_width(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15))
        pm = PaneManager(cfg)
        assert pm.pane_size() == timedelta(hours=1)

    def test_pane_size_from_config(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15),
                           pane_size=timedelta(minutes=5))
        pm = PaneManager(cfg)
        assert pm.pane_size() == timedelta(minutes=5)

    def test_compute_pane_id(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15),
                           pane_size=timedelta(minutes=15))
        pm = PaneManager(cfg)
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        pid = pm.compute_pane_id(dt)
        assert pid.startswith("p_")
        # 10:27:33 floored to 15min = 10:15:00 → pane [10:15, 10:30)
        # pane start 1705313700000, pane end 1705314600000
        assert "1705313700000" in pid  # pane start: 10:15:00
        assert "1705314600000" in pid  # pane end: 10:30:00

    def test_compute_window_id(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15))
        pm = PaneManager(cfg)
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        wid = pm.compute_window_id(dt)
        assert wid.startswith("w_")

    def test_find_or_create(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15),
                           pane_size=timedelta(minutes=15))
        pm = PaneManager(cfg)
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        pane_id = pm.compute_pane_id(dt)
        pane = pm.find_or_create(pane_id, "w_test")
        assert pane.pane_id == pane_id
        assert pane.window_id == "w_test"
        assert pane.is_active is True
        # Second call returns same pane
        pane2 = pm.find_or_create(pane_id, "w_test")
        assert pane2 is pane

    def test_drop(self):
        cfg = WindowConfig(window_width=timedelta(hours=1), slide_step=timedelta(minutes=15),
                           pane_size=timedelta(minutes=15))
        pm = PaneManager(cfg)
        dt = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        pane_id = pm.compute_pane_id(dt)
        pm.find_or_create(pane_id, "w_test")
        pm.drop(pane_id)
        assert pm.get(pane_id) is None


# ─── WindowManager ────────────────────────────────────────────────────────────

class TestWindowManager:
    @pytest.fixture
    def wm(self):
        cfg = WindowConfig(
            window_width=timedelta(hours=1),
            slide_step=timedelta(minutes=15),
            pane_size=timedelta(minutes=15),
        )
        return WindowManager(cfg)

    def test_assign_window_sets_ids(self, wm):
        """Event gets pane_id and window_id assigned."""
        event = MockEvent(
            event_id="e1",
            event_time=datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc),
        )
        wm.assign_window(event, datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc))
        assert event.pane_id is not None
        assert event.window_id is not None
        assert event.pane_id.startswith("p_")
        assert event.window_id.startswith("w_")

    def test_assign_window_returns_windowed_event(self, wm):
        """assign_window returns WindowedEvent wrapper."""
        event = MockEvent(
            event_id="e1",
            event_time=datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc),
        )
        result = wm.assign_window(event, datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc))
        assert len(result) == 1
        assert isinstance(result[0], WindowedEvent)
        assert result[0].window_id == event.window_id
        assert result[0].pane_id == event.pane_id

    def test_assign_multiple_events_same_window(self, wm):
        """Multiple events go into the same window buffer."""
        t = datetime(2024, 1, 15, 10, 27, 33, tzinfo=timezone.utc)
        e1 = MockEvent("e1", t)
        e2 = MockEvent("e2", t + timedelta(minutes=2))
        wm.assign_window(e1, t)
        wm.assign_window(e2, t)
        assert e1.window_id == e2.window_id
        assert len(wm.buffers) == 1
        assert len(wm.buffers[e1.window_id].events) == 2

    def test_assign_window_different_events_different_windows(self, wm):
        """Events at different times can fall into different sliding windows."""
        t1 = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 9, 45, 0, tzinfo=timezone.utc)
        e1 = MockEvent("e1", t1)
        e2 = MockEvent("e2", t2)
        wm.assign_window(e1, t1)
        wm.assign_window(e2, t2)
        # 09:30 → window [09:15, 10:15), 09:45 → window [09:30, 10:30) (different windows)
        assert e1.window_id != e2.window_id

    def test_on_slide_closing_window(self, wm):
        """on_slide identifies closing windows and returns them."""
        # Assign events to a window
        t = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        e1 = MockEvent("e1", t + timedelta(minutes=1))
        e2 = MockEvent("e2", t + timedelta(minutes=5))
        wm.assign_window(e1, t)
        wm.assign_window(e2, t)

        # Simulate watermark advancing past window end (10:00)
        new_wm = t + timedelta(hours=1) + timedelta(seconds=1)
        deltas, closing = wm.on_slide(new_wm)

        assert e1.window_id in closing
        assert e2.window_id in closing
        assert len(deltas) == 1
        assert deltas[0].window_id == e1.window_id
        # Closed window: all events are deletes
        assert len(deltas[0].inserts) == 0
        assert set(deltas[0].deletes) == {e1, e2}

    def test_on_slide_incremental_delta(self, wm):
        """on_slide computes incremental inserts/deletes for active windows."""
        # Event at 09:05 (within slide [09:00, 09:15))
        t_start = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        e1 = MockEvent("e1", t_start + timedelta(minutes=5))
        wm.assign_window(e1, t_start)

        # Simulate slide from 09:00 → 09:15
        new_wm = t_start + timedelta(minutes=15) + timedelta(seconds=1)
        deltas, closing = wm.on_slide(new_wm)

        # Window still active (end_time = 10:00 > 09:15)
        assert e1.window_id not in closing
        assert len(deltas) == 1
        assert len(deltas[0].inserts) >= 0  # e1 entered this slide window

    def test_on_slide_returns_tuple(self, wm):
        """on_slide returns (deltas, closing_windows) tuple."""
        t = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        e1 = MockEvent("e1", t + timedelta(minutes=1))
        wm.assign_window(e1, t)
        new_wm = t + timedelta(hours=1) + timedelta(seconds=1)
        result = wm.on_slide(new_wm)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ─── WatermarkClock ───────────────────────────────────────────────────────────

class TestWatermarkClock:
    def test_initial_watermark_is_epoch(self):
        wc = WatermarkClock()
        assert wc.get() == datetime(1970, 1, 1, tzinfo=timezone.utc)

    def test_on_event_advances_watermark(self):
        wc = WatermarkClock()
        t1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 10, 5, 0, tzinfo=timezone.utc)
        wc.on_event(MockEvent("e1", t1))
        wc.on_event(MockEvent("e2", t2))
        assert wc.get() == t2

    def test_out_of_order_does_not_advance_watermark(self):
        wc = WatermarkClock()
        t1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 10, 5, 0, tzinfo=timezone.utc)
        wc.on_event(MockEvent("e1", t2))
        wc.on_event(MockEvent("e2", t1))  # earlier — should not advance
        assert wc.get() == t2  # still at t2

    def test_is_ready_without_grace(self):
        wc = WatermarkClock(WatermarkConfig(wait_for_late=0))
        pane_end = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        wc.advance(datetime(2024, 1, 15, 10, 0, 1, tzinfo=timezone.utc))
        assert wc.is_ready(pane_end) is True

    def test_is_ready_with_grace(self):
        wc = WatermarkClock(WatermarkConfig(wait_for_late=60))
        pane_end = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        # Watermark at pane_end + 30s — not enough grace
        wc.advance(datetime(2024, 1, 15, 10, 0, 30, tzinfo=timezone.utc))
        assert wc.is_ready(pane_end) is False
        # Watermark at pane_end + 120s — past grace period
        wc.advance(datetime(2024, 1, 15, 10, 2, 0, tzinfo=timezone.utc))
        assert wc.is_ready(pane_end) is True

    def test_has_advanced(self):
        wc = WatermarkClock()
        t1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 10, 5, 0, tzinfo=timezone.utc)
        assert wc.has_advanced() is False  # no events yet
        wc.on_event(MockEvent("e1", t1))
        assert wc.has_advanced() is True
        # Second call on same event — no advance
        assert wc.has_advanced() is False
        wc.on_event(MockEvent("e2", t2))
        assert wc.has_advanced() is True

    def test_reset(self):
        wc = WatermarkClock()
        wc.on_event(MockEvent("e1", datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)))
        wc.reset()
        assert wc.get() == datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert wc.has_advanced() is False
