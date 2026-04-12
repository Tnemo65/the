"""Integration tests: late event handling, alert retraction, and state transitions."""

import pytest
from datetime import datetime, timedelta, timezone

from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import DataEvent
from waves.decision.alert_store import AlertStatus
from waves.decision.decision import RetractionDecision
from waves.store import EventStore
from waves.tombstone import TombstoneManager
from waves.decision.alert_store import AlertStateStore
from waves.weever import PaneForest
from waves.late_handler import (
    LateHandlerConfig,
    handle_late_event,
    late_event_invalidate_check,
    _parse_window_end,
)
from waves.optimizer.dc_parser import Predicate, PredicateType


# ─── Fixtures ────────────────────────────────────────────────────────────────

def make_event(event_id, attrs, event_time=None):
    if event_time is None:
        event_time = datetime.now(timezone.utc)
    return DataEvent(
        event_id=event_id,
        event_time=event_time,
        ingestion_time=event_time + timedelta(seconds=1),
        pane_id=None,
        window_id=None,
        attributes=attrs,
    )


def make_window_id(event_time, width_seconds=3600):
    start_ms = int(event_time.timestamp() * 1000)
    end_ms = start_ms + width_seconds * 1000
    return f"w_{start_ms}_{end_ms}"


# ─── Late Handler Core ────────────────────────────────────────────────────────

class TestLateEventThreshold:
    """Step 1 of late handling: lateness threshold check."""

    def test_parse_window_end_valid(self):
        """'w_start_end' -> extracts end timestamp."""
        end = _parse_window_end("w_1704110400000_1704114000000")
        assert end is not None
        assert end == datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)

    def test_parse_window_end_invalid_format(self):
        assert _parse_window_end("") is None
        assert _parse_window_end("w_abc") is None
        assert _parse_window_end("w_") is None

    def test_late_event_dropped_beyond_grace(self):
        """Event late by > wait_for_late_seconds: dropped."""
        config = PipelineConfig(wait_for_late_seconds=60.0)
        pipeline = WavePipeline(config)

        # Event at 12:00, window 12:00-12:01, watermark far in the future -> dropped
        t = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        wcfg = pipeline.config.window_config()

        late_event = make_event("late_drop", {"x": 1.0}, event_time=t)
        late_event.window_id = make_window_id(t, 60)
        pipeline._event_store.put(late_event)

        pane_count_before = len(pipeline._pane_forest.panes)
        tcfg = LateHandlerConfig(
            wait_for_late_seconds=60.0,
            window_config=wcfg,
        )
        handle_late_event(
            late_event=late_event,
            alert_store=pipeline._alert_store,
            tombstone_mgr=pipeline._tombstone_mgr,
            pane_forest=pipeline._pane_forest,
            config=tcfg,
            dim_map={},
            lo_bounds=(),
            hi_bounds=(),
            active_boxes=[],
            get_pane_id_fn=pipeline._event_store.get_pane_id,
        )
        assert len(pipeline._pane_forest.panes) == pane_count_before


# ─── Alert Retraction ─────────────────────────────────────────────────────────

class TestAlertRetraction:

    def test_invalidate_check_false_when_predicates_not_satisfied(self):
        """EQUAL predicate not satisfied -> no retract."""
        predicates = [
            Predicate("trip_distance", "s", PredicateType.EQUAL, "trip_distance", "t", False),
            Predicate("fare_amount", "s", PredicateType.LESS_EQUAL, "fare_amount", "t", False),
            Predicate("trip_distance", "s", PredicateType.LESS, "trip_distance", "t", False),
        ]
        # matched dist=5, late dist=7 -> EQUAL fails -> no retract
        matched = make_event("e2", {"trip_distance": 5.0, "fare_amount": 10.0})
        late = make_event("e3", {"trip_distance": 7.0, "fare_amount": 15.0})
        result = late_event_invalidate_check(late, matched, predicates)
        assert result is False

    def test_invalidate_check_true_when_all_predicates_satisfied(self):
        """All LESS_EQUAL predicates satisfied -> retract."""
        predicates = [
            Predicate("fare_amount", "s", PredicateType.LESS_EQUAL, "fare_amount", "t", False),
        ]
        matched = make_event("m", {"fare_amount": 10.0})
        late = make_event("l", {"fare_amount": 15.0})
        result = late_event_invalidate_check(late, matched, predicates)
        assert result is True


# ─── Alert State Transitions ─────────────────────────────────────────────────

class TestAlertStateTransitions:

    def test_provisional_to_final(self):
        """Provisional alert promoted to FINAL via finalize_window."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="alert_DC1_w1_e1",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            all_event_ids=["e1", "e2"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        decisions = pipeline.finalize_window("w1")
        assert len(decisions) >= 1
        assert decisions[0].decision_type == "final"
        assert decisions[0].alert.status == AlertStatus.FINAL

        stored = pipeline._alert_store.get("alert_DC1_w1_e1")
        assert stored.status == AlertStatus.FINAL

    def test_retraction_removes_from_store(self):
        """AlertStateStore.retract() removes alert and returns retracted record."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="alert_DC1_w1_e1",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.FINAL,
            all_event_ids=["e1", "e2"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        by_window = pipeline._alert_store.get_by_window("w1")
        assert len(by_window) >= 1

        retracted = pipeline._alert_store.retract("alert_DC1_w1_e1")
        assert retracted is not None
        assert retracted.status == AlertStatus.RETRACTED

        stored = pipeline._alert_store.get("alert_DC1_w1_e1")
        assert stored is None

    def test_final_alert_not_retracted_after_watermark(self):
        """Final alerts should not be retracted by late events."""
        config = PipelineConfig(wait_for_late_seconds=300.0)
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="alert_final",
            dc_id="DC1",
            window_id="w_final",
            pane_id="p1",
            status=AlertStatus.FINAL,
            all_event_ids=["e1", "e2"],
            created_at=datetime.now(timezone.utc),
            finalized_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        late = make_event("late1", {"x": 1.0})
        late.window_id = "w_final"
        pipeline._event_store.put(late)

        tcfg = LateHandlerConfig(
            wait_for_late_seconds=300.0,
            window_config=pipeline.config.window_config(),
        )
        decisions = handle_late_event(
            late_event=late,
            alert_store=pipeline._alert_store,
            tombstone_mgr=pipeline._tombstone_mgr,
            pane_forest=pipeline._pane_forest,
            config=tcfg,
            dim_map={},
            lo_bounds=(),
            hi_bounds=(),
            active_boxes=[],
            get_pane_id_fn=pipeline._event_store.get_pane_id,
        )
        retraction_decs = [d for d in decisions if d.decision_type == "retraction"]
        assert len(retraction_decs) == 0
        assert pipeline._alert_store.get("alert_final") is not None


# ─── Event Store Integration ─────────────────────────────────────────────────

class TestEventStoreIntegration:

    def test_get_event_by_id(self):
        """store.get(event_id) returns the full event."""
        store = EventStore()
        e1 = make_event("e_find", {"x": 1.0})
        e1.pane_id = "p1"
        e1.window_id = "w1"
        store.put(e1)

        retrieved = store.get("e_find")
        assert retrieved is not None
        assert retrieved.event_id == "e_find"
        assert retrieved.attributes["x"] == 1.0

    def test_pane_id_lookup_via_event_store(self):
        """event_store.get_pane_id() used by Rapidash."""
        store = EventStore()
        e1 = make_event("q1", {"x": 1.0})
        e1.pane_id = "p_q1"
        e1.window_id = "w1"
        store.put(e1)

        assert store.get_pane_id("q1") == "p_q1"

    def test_event_store_clear(self):
        """clear() resets all indexes."""
        store = EventStore()
        for i in range(5):
            e = make_event(f"ec{i}", {"x": i})
            e.pane_id = "p1"
            e.window_id = "w1"
            store.put(e)
        assert store.count() == 5
        store.clear()
        assert store.count() == 0


# ─── Tombstone Lifecycle ─────────────────────────────────────────────────────

class TestTombstoneLifecycle:

    def test_tombstone_added_on_retraction(self):
        """AlertStateStore.retract() tombstones all event IDs via decision layer."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        # Create the pane in TombstoneManager first (pane_close does this in real flow)
        pipeline._tombstone_mgr.create_pane("p_tomb")

        alert = AlertRecord(
            alert_id="alert_tomb",
            dc_id="DC1",
            window_id="w1",
            pane_id="p_tomb",
            status=AlertStatus.FINAL,
            all_event_ids=["e_t1", "e_t2"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        # Retract via decision layer (which tombstones events)
        from waves.decision.decision import retract_alert
        retract_alert("alert_tomb", pipeline._alert_store, pipeline._tombstone_mgr)

        assert pipeline._tombstone_mgr.contains("e_t1", "p_tomb") is True
        assert pipeline._tombstone_mgr.contains("e_t2", "p_tomb") is True

    def test_tombstone_prevents_match_in_traversal(self):
        """Tombstoned event is skipped during kdtree traversal."""
        # Use a DC rule so pipeline has active_boxes + dim_map for _traverse_pane
        config = PipelineConfig(k_max=4)
        pipeline = WavePipeline(config)
        pipeline.load_dc_rules([
            {
                "dc_id": "DC1",
                "rule_group": "fare_distance",
                "description": "Fare-Distance Dominance",
                "predicates": [
                    {"left_col": "trip_distance_s", "operator": "EQUAL", "right_col": "trip_distance_t"},
                    {"left_col": "fare_amount_s", "operator": "LESS_EQUAL", "right_col": "fare_amount_t"},
                    {"left_col": "trip_distance_s", "operator": "LESS", "right_col": "trip_distance_t"},
                ],
            },
        ])

        t = datetime.now(timezone.utc)
        wcfg = pipeline.config.window_config()

        e1 = make_event("e1", {"trip_distance": 5.0, "fare_amount": 20.0}, event_time=t)
        e2 = make_event("e2", {"trip_distance": 5.0, "fare_amount": 10.0}, event_time=t)

        pane = pipeline._pane_forest.pane_insert(t, "e1", (5.0, 20.0, 0.0), "w1", wcfg)
        e1.pane_id = pane.pane_id
        pipeline._event_store.put(e1)
        pipeline._pane_forest.pane_insert(t, "e2", (5.0, 10.0, 0.0), "w1", wcfg)
        e2.pane_id = pane.pane_id
        pipeline._event_store.put(e2)
        pane_id = pane.pane_id
        pipeline._pane_forest.pane_close(pane_id, dim_count=3, lo_bounds=(0.0, 0.0, 0.0), hi_bounds=(500.0, 100.0, 100.0))
        pane = pipeline._pane_forest.get_pane_by_id(pane_id)
        assert pane is not None

        candidates = pipeline._traverse_pane(pane, e2)
        assert len(candidates) >= 1
        assert "e1" in candidates[0].matched_ids

        pipeline._tombstone_mgr.add("e1", pane_id)

        candidates2 = pipeline._traverse_pane(pane, e2)
        for c in candidates2:
            assert "e1" not in c.matched_ids


# ─── Cleanup Expired Alerts ─────────────────────────────────────────────────

class TestCleanupExpiredAlerts:

    def test_cleanup_deletes_old_final_alerts(self):
        """Final alerts older than TTL are deleted."""
        config = PipelineConfig(alert_ttl_seconds=1.0)
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        old = AlertRecord(
            alert_id="old_alert",
            dc_id="DC1",
            window_id="w_old",
            pane_id="p1",
            status=AlertStatus.FINAL,
            all_event_ids=["e1"],
            created_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            finalized_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        pipeline._alert_store.put(old)

        recent = AlertRecord(
            alert_id="recent_alert",
            dc_id="DC1",
            window_id="w_recent",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            all_event_ids=["e2"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(recent)

        count = pipeline.cleanup()
        assert count >= 1
        assert pipeline._alert_store.get("old_alert") is None
        assert pipeline._alert_store.get("recent_alert") is not None

    def test_cleanup_keeps_recent_final_alerts(self):
        """Final alerts within TTL are preserved."""
        config = PipelineConfig(alert_ttl_seconds=3600.0)
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="recent_final",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.FINAL,
            all_event_ids=["e1"],
            created_at=datetime.now(timezone.utc),
            finalized_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        count = pipeline.cleanup()
        assert count == 0
        assert pipeline._alert_store.get("recent_final") is not None


# ─── Output Sink ─────────────────────────────────────────────────────────────

class TestOutputSink:

    def test_output_sink_receives_retraction_events(self):
        """RetractionDecision is emitted to alert_sink."""
        received = []

        def alert_sink(e):
            received.append(e)

        config = PipelineConfig(alert_sink=alert_sink)
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="alert_out",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.FINAL,
            all_event_ids=["e1"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        retraction = RetractionDecision(alert, tombstoned_event_ids=["e1"])
        pipeline._output.emit(retraction)

        assert len(received) == 1
        assert received[0].event_type == "retraction"
        assert received[0].alert_id == "alert_out"

    def test_output_sink_receives_provisional_events(self):
        """ProvisionalDecision is emitted to alert_sink."""
        received = []

        def alert_sink(e):
            received.append(e)

        config = PipelineConfig(alert_sink=alert_sink)
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord

        alert = AlertRecord(
            alert_id="alert_prov",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            all_event_ids=["e1"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        provisional = RetractionDecision(alert, tombstoned_event_ids=[])
        provisional.decision_type = "provisional"
        # Use the AlertOutput directly
        from waves.decision.decision import ProvisionalDecision
        prov = ProvisionalDecision(alert)
        pipeline._output.emit(prov)

        assert len(received) == 1
        assert received[0].event_type == "provisional"
