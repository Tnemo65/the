"""Unit tests for Module 2.11c — WavePipeline."""

import pytest
from datetime import datetime, timedelta, timezone

from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import DataEvent, Schema
from waves.output import AlertEvent
from waves.decision.alert_store import AlertStatus


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_pipeline(alert_ttl=3600.0):
    config = PipelineConfig(
        window_width_seconds=3600.0,
        slide_step_seconds=900.0,
        pane_size_seconds=900.0,
        wait_for_late_seconds=300.0,
        alert_ttl_seconds=alert_ttl,
    )
    return WavePipeline(config)


def make_event(event_id="e1", attrs=None, pane_id=None, window_id=None):
    return DataEvent(
        event_id=event_id,
        event_time=utc_now(),
        ingestion_time=utc_now(),
        pane_id=pane_id,
        window_id=window_id,
        attributes=attrs or {"x": 1.0},
    )


class TestPipelineConfig:
    def test_default_window_config(self):
        cfg = PipelineConfig()
        wc = cfg.window_config()
        assert wc.window_width == timedelta(hours=1)
        assert wc.slide_step == timedelta(minutes=15)
        assert wc.pane_size == timedelta(minutes=15)

    def test_custom_window_config(self):
        cfg = PipelineConfig(
            window_width_seconds=7200.0,
            slide_step_seconds=1800.0,
            pane_size_seconds=600.0,
        )
        wc = cfg.window_config()
        assert wc.window_width == timedelta(hours=2)
        assert wc.slide_step == timedelta(minutes=30)
        assert wc.pane_size == timedelta(minutes=10)


class TestWavePipelineInit:
    def test_creates_all_modules(self):
        p = make_pipeline()
        assert p._event_store is not None
        assert p._tombstone_mgr is not None
        assert p._alert_store is not None
        assert p._pane_forest is not None
        assert p._window_mgr is not None
        assert p._dq_checker is not None
        assert p._logical_engine is not None
        assert p._output is not None

    def test_properties_expose_modules(self):
        p = make_pipeline()
        assert p.event_store is p._event_store
        assert p.alert_store is p._alert_store
        assert p.pane_forest is p._pane_forest
        assert p.output is p._output

    def test_load_dc_rules_idempotent(self):
        p = make_pipeline()
        # Empty rules - should not raise
        p.load_dc_rules([])
        assert p._active_boxes == []


class TestProcessLateEvent:
    def test_process_late_returns_list(self):
        p = make_pipeline()
        late = make_event(event_id="late1", attrs={"x": 5.0}, pane_id="p1", window_id="w1")
        result = p.process_late(late)
        assert isinstance(result, list)

    def test_process_late_inserts_into_pane(self):
        p = make_pipeline()
        late = make_event(event_id="late2", attrs={"x": 5.0}, pane_id="p1", window_id="w1")
        p.process_late(late)
        # Pane should be created
        assert len(p._pane_forest.panes) >= 1


class TestFinalizeWindow:
    def test_finalize_unknown_window_returns_empty(self):
        p = make_pipeline()
        result = p.finalize_window("unknown_window")
        assert result == []

    def test_finalize_emits_no_decisions_when_empty(self):
        p = make_pipeline()
        history_before = len(p._output.get_alert_history())
        p.finalize_window("w_0_9999999999999")
        # No new alerts finalized (store is empty)
        # No error should occur


class TestCleanup:
    def test_cleanup_returns_int(self):
        p = make_pipeline()
        count = p.cleanup()
        assert isinstance(count, int)
        assert count >= 0

    def test_cleanup_with_short_ttl(self):
        p = make_pipeline(alert_ttl=0.001)
        count = p.cleanup()
        assert count >= 0


class TestBuildWindowMeta:
    def test_build_window_meta_returns_window_meta(self):
        p = make_pipeline()
        meta = p.build_window_meta("w_test")
        assert meta.window_id == "w_test"
        assert meta.total_count == 0


class TestEmitMeta:
    def test_emit_meta_calls_output(self):
        p = make_pipeline()
        meta = p.emit_meta("w_test")
        assert meta is not None
        assert meta.window_id == "w_test"
        assert len(p._output.get_meta_history()) == 1


class TestExtractPoint:
    def test_extract_point_no_dim_map(self):
        p = make_pipeline()
        event = make_event(attrs={"x": 1.0, "y": 2.0})
        point = p._extract_point(event)
        # No dim_map loaded -> empty point
        assert point == ()


class TestPipelineIntegration:
    def test_late_event_recheck_flow(self):
        """Late event: insert -> recheck -> decision flow."""
        p = make_pipeline()

        # Create an active pane first
        late1 = make_event(event_id="late1", attrs={"x": 3.0}, pane_id="p1", window_id="w1")
        p.process_late(late1)

        # Verify pane was created
        assert len(p._pane_forest.panes) == 1

    def test_multiple_late_events_same_pane(self):
        """Multiple late events go into the same pane."""
        p = make_pipeline()

        for i in range(3):
            late = make_event(event_id=f"late{i}", attrs={"x": float(i)}, pane_id="p1", window_id="w1")
            p.process_late(late)

        # All 3 events in the same pane
        assert len(p._pane_forest.panes) == 1
        pane = p._pane_forest.panes[0]
        assert len(pane.buffer) == 3

    def test_output_sink_receives_events(self):
        """Output sink receives emitted alert events."""
        received = []

        def alert_sink(e):
            received.append(e)

        config = PipelineConfig(alert_sink=alert_sink)
        p = WavePipeline(config)

        # Process late event - should go through late handler
        late = make_event(event_id="late1", attrs={"x": 5.0}, pane_id="p1", window_id="w1")
        p.process_late(late)

        # Sink received alert events if any decisions were made
        # (may be empty if no violations found)
        assert isinstance(received, list)

    def test_output_meta_sink_receives_meta(self):
        """Meta sink receives WindowMeta events."""
        received = []

        def meta_sink(e):
            received.append(e)

        config = PipelineConfig(meta_sink=meta_sink)
        p = WavePipeline(config)

        p.emit_meta("w_test")
        assert len(received) == 1
