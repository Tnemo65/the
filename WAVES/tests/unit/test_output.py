"""Unit tests for Module 2.11 — AlertOutput."""

import pytest
from datetime import datetime, timezone

from waves.output import AlertEvent, AlertOutput
from waves.basic_dq.meta_stream import WindowMeta
from waves.decision.decision import ProvisionalDecision, FinalDecision, RetractionDecision
from waves.decision.alert_store import AlertRecord, AlertStatus


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_alert(
    alert_id="alert_DC1_w1_e1",
    dc_id="DC1",
    window_id="w1",
    pane_id="p1",
    status=AlertStatus.PROVISIONAL,
    event_ids=None,
):
    if event_ids is None:
        event_ids = ["e1", "e2"]
    return AlertRecord(
        alert_id=alert_id,
        dc_id=dc_id,
        window_id=window_id,
        pane_id=pane_id,
        status=status,
        all_event_ids=event_ids,
        created_at=utc_now(),
        finalized_at=utc_now() if status == AlertStatus.FINAL else None,
    )


class TestAlertEvent:
    def test_alert_event_fields(self):
        e = AlertEvent(
            event_type="provisional",
            alert_id="alert_DC1_w1_e1",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            event_id="e1",
            event_time=utc_now(),
            output_time=utc_now(),
            detail={"extra": "value"},
        )
        assert e.event_type == "provisional"
        assert e.alert_id == "alert_DC1_w1_e1"
        assert e.dc_id == "DC1"
        assert e.detail["extra"] == "value"


class TestAlertOutputInit:
    def test_no_sinks(self):
        output = AlertOutput()
        assert output._alert_sink is None
        assert output._meta_sink is None
        assert output.get_alert_history() == []

    def test_with_sinks(self):
        sink_calls = []

        def alert_sink(e):
            sink_calls.append(e)

        output = AlertOutput(alert_sink=alert_sink)
        assert output._alert_sink is alert_sink


class TestEmitProvisional:
    def test_emit_provisional_creates_alert_event(self):
        output = AlertOutput()
        alert = make_alert(status=AlertStatus.PROVISIONAL)
        decision = ProvisionalDecision(alert)
        event = output.emit(decision)

        assert event is not None
        assert event.event_type == "provisional"
        assert event.alert_id == alert.alert_id
        assert event.dc_id == "DC1"
        assert event.window_id == "w1"
        assert event.pane_id == "p1"
        assert event.event_id == "e1"
        assert event.event_time == alert.created_at

    def test_emit_provisional_calls_sink(self):
        received = []

        def sink(e):
            received.append(e)

        output = AlertOutput(alert_sink=sink)
        decision = ProvisionalDecision(make_alert())
        output.emit(decision)
        assert len(received) == 1
        assert received[0].event_type == "provisional"

    def test_emit_provisional_appends_to_history(self):
        output = AlertOutput()
        decision = ProvisionalDecision(make_alert())
        output.emit(decision)
        assert len(output.get_alert_history()) == 1


class TestEmitFinal:
    def test_emit_final_creates_alert_event(self):
        output = AlertOutput()
        alert = make_alert(status=AlertStatus.FINAL)
        decision = FinalDecision(alert)
        event = output.emit(decision)

        assert event is not None
        assert event.event_type == "final"
        assert event.alert_id == alert.alert_id

    def test_emit_final_empty_event_ids(self):
        output = AlertOutput()
        alert = make_alert(status=AlertStatus.FINAL, event_ids=[])
        decision = FinalDecision(alert)
        event = output.emit(decision)

        assert event is not None
        assert event.event_id == ""


class TestEmitRetraction:
    def test_emit_retraction_creates_event(self):
        output = AlertOutput()
        alert = make_alert(status=AlertStatus.PROVISIONAL)
        decision = RetractionDecision(alert, tombstoned_event_ids=["e1", "e2"])
        event = output.emit(decision)

        assert event is not None
        assert event.event_type == "retraction"
        assert event.alert_id == alert.alert_id
        assert event.detail["tombstoned_event_ids"] == ["e1", "e2"]

    def test_emit_retraction_calls_sink(self):
        received = []

        def sink(e):
            received.append(e)

        output = AlertOutput(alert_sink=sink)
        decision = RetractionDecision(make_alert(), tombstoned_event_ids=["e1"])
        output.emit(decision)
        assert len(received) == 1
        assert received[0].event_type == "retraction"


class TestEmitMeta:
    def test_emit_meta_creates_meta_event(self):
        output = AlertOutput()
        meta = WindowMeta(
            window_id="w1",
            total_count=100,
            fail_counts={"rule_null": 5, "rule_range": 3},
        )
        event = output.emit_meta(meta)

        assert event is not None
        assert event.window_id == "w1"
        assert event.total == 100
        assert event.pass_count == 92
        assert event.fail_count == 8
        assert event.violation_counts["rule_null"] == 5
        assert event.violation_counts["rule_range"] == 3

    def test_emit_meta_calls_meta_sink(self):
        received = []

        def sink(e):
            received.append(e)

        output = AlertOutput(meta_sink=sink)
        output.emit_meta(WindowMeta(window_id="w1", total_count=10, fail_counts={}))
        assert len(received) == 1

    def test_emit_meta_empty_fail_counts(self):
        output = AlertOutput()
        meta = WindowMeta(window_id="w1", total_count=50, fail_counts={})
        event = output.emit_meta(meta)
        assert event.pass_count == 50
        assert event.fail_count == 0

    def test_emit_meta_appends_to_meta_history(self):
        output = AlertOutput()
        output.emit_meta(WindowMeta(window_id="w1", total_count=10))
        output.emit_meta(WindowMeta(window_id="w2", total_count=20))
        assert len(output.get_meta_history()) == 2


class TestHistory:
    def test_get_alert_history_returns_copy(self):
        output = AlertOutput()
        output.emit(ProvisionalDecision(make_alert()))
        history = output.get_alert_history()
        history.clear()
        assert len(output.get_alert_history()) == 1

    def test_get_meta_history_returns_copy(self):
        output = AlertOutput()
        output.emit_meta(WindowMeta(window_id="w1", total_count=10))
        history = output.get_meta_history()
        history.clear()
        assert len(output.get_meta_history()) == 1

    def test_clear_removes_all_history(self):
        output = AlertOutput()
        output.emit(ProvisionalDecision(make_alert()))
        output.emit_meta(WindowMeta(window_id="w1", total_count=10))
        output.clear()
        assert output.get_alert_history() == []
        assert output.get_meta_history() == []

    def test_unknown_decision_type_returns_none(self):
        output = AlertOutput()

        class UnknownDecision:
            decision_type = "unknown"

        result = output.emit(UnknownDecision())
        assert result is None
        assert len(output.get_alert_history()) == 0


class TestEndToEnd:
    def test_full_lifecycle_provisional_final_retraction(self):
        """Provisional -> Final -> Retraction emits 3 events in order."""
        output = AlertOutput()
        alert = make_alert(status=AlertStatus.PROVISIONAL)

        d1 = ProvisionalDecision(alert)
        d2 = FinalDecision(alert)
        d3 = RetractionDecision(alert, tombstoned_event_ids=["e1", "e2"])

        output.emit(d1)
        output.emit(d2)
        output.emit(d3)

        history = output.get_alert_history()
        assert len(history) == 3
        assert history[0].event_type == "provisional"
        assert history[1].event_type == "final"
        assert history[2].event_type == "retraction"
        assert history[2].detail["tombstoned_event_ids"] == ["e1", "e2"]
