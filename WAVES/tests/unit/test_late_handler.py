"""Unit tests for Module 2.10 — Late Handler."""

import pytest
from datetime import datetime, timedelta, timezone

from waves.late_handler.handler import (
    LateHandlerConfig,
    handle_late_event,
    late_event_invalidate_check,
    _parse_window_end,
    _extract_point,
    _evaluate_predicate,
)
from waves.late_handler import handler as late_handler_module
from waves.ingestion.schema import DataEvent
from waves.decision.alert_store import AlertStateStore, AlertStatus, AlertRecord
from waves.optimizer.dc_parser import Predicate, PredicateType


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_event(event_id, attrs=None, pane_id=None, window_id=None):
    return DataEvent(
        event_id=event_id,
        event_time=utc_now(),
        ingestion_time=utc_now(),
        pane_id=pane_id,
        window_id=window_id,
        attributes=attrs or {},
    )


def make_alert(alert_id, dc_id="DC1", window_id="w1", pane_id="p1",
               status=AlertStatus.PROVISIONAL, event_ids=None):
    return AlertRecord(
        alert_id=alert_id,
        dc_id=dc_id,
        window_id=window_id,
        pane_id=pane_id,
        status=status,
        all_event_ids=event_ids or ["e1"],
        created_at=utc_now(),
    )


# =============================================================================
# _parse_window_end
# =============================================================================

class TestParseWindowEnd:
    def test_valid_window_id(self):
        # w_1744365300000_1744369200000 → end = 1744369200000 ms
        result = _parse_window_end("w_1744365300000_1744369200000")
        assert result is not None
        assert result.timestamp() == pytest.approx(1744369200.0, rel=1)

    def test_empty_string(self):
        assert _parse_window_end("") is None

    def test_none(self):
        assert _parse_window_end(None) is None

    def test_invalid_format(self):
        assert _parse_window_end("not_a_window_id") is None

    def test_no_underscore(self):
        assert _parse_window_end("w12345") is None


# =============================================================================
# _extract_point
# =============================================================================

class TestExtractPoint:
    def test_basic_extraction(self):
        event = make_event("e1", attrs={"fare_amount": 15.0, "trip_distance": 5.0})
        dim_map = {"fare_amount": 0, "trip_distance": 1}
        result = _extract_point(event, dim_map)
        assert result == (15.0, 5.0)

    def test_missing_attribute_uses_zero(self):
        event = make_event("e1", attrs={"fare_amount": 10.0})
        dim_map = {"fare_amount": 0, "trip_distance": 1}
        result = _extract_point(event, dim_map)
        assert result == (10.0, 0.0)

    def test_non_numeric_uses_zero(self):
        event = make_event("e1", attrs={"fare_amount": "abc"})
        dim_map = {"fare_amount": 0}
        result = _extract_point(event, dim_map)
        assert result == (0.0,)

    def test_empty_dim_map(self):
        event = make_event("e1", attrs={"x": 5.0})
        result = _extract_point(event, {})
        assert result == ()

    def test_sparse_dim_map(self):
        event = make_event("e1", attrs={"x": 1.0, "z": 3.0})
        dim_map = {"x": 0, "z": 2}
        result = _extract_point(event, dim_map)
        assert result == (1.0, 0.0, 3.0)


# =============================================================================
# _evaluate_predicate
# =============================================================================

class TestEvaluatePredicate:
    def test_less_true(self):
        assert _evaluate_predicate(3.0, PredicateType.LESS, 5.0) is True

    def test_less_false(self):
        assert _evaluate_predicate(5.0, PredicateType.LESS, 3.0) is False

    def test_less_equal(self):
        assert _evaluate_predicate(5.0, PredicateType.LESS_EQUAL, 5.0) is True

    def test_greater(self):
        assert _evaluate_predicate(5.0, PredicateType.GREATER, 3.0) is True

    def test_greater_equal(self):
        assert _evaluate_predicate(3.0, PredicateType.GREATER_EQUAL, 3.0) is True

    def test_equal(self):
        assert _evaluate_predicate(3.0, PredicateType.EQUAL, 3.0) is True
        assert _evaluate_predicate("abc", PredicateType.EQUAL, "abc") is True

    def test_string_less(self):
        assert _evaluate_predicate("abc", PredicateType.LESS, "abd") is True


# =============================================================================
# late_event_invalidate_check
# =============================================================================

class TestLateEventInvalidateCheck:
    def _pred(self, left_col, left_side, operator, right_col, right_side):
        return Predicate(
            left_col=left_col,
            left_side=left_side,
            operator=operator,
            right_col=right_col,
            right_side=right_side,
            is_constant=False,
            constant_value=None,
        )

    def test_invalidates_when_both_predicates_satisfied(self):
        # Both predicates satisfied: dist_equal AND fare_less
        # → DC NOT satisfied → original violation is false → RETRACT
        late_event = make_event("late1", attrs={"trip_distance": 3.5, "fare_amount": 20.0})
        matched_event = make_event("e1", attrs={"trip_distance": 3.5, "fare_amount": 15.0})
        preds = [
            self._pred("trip_distance", "s", PredicateType.EQUAL, "trip_distance", "t"),
            self._pred("fare_amount", "s", PredicateType.LESS, "fare_amount", "t"),
        ]
        # dist_equal: s.trip=3.5 == t.trip=3.5 ✓, fare_less: s.fare=15 < t.fare=20 ✓ → invalidate
        assert late_event_invalidate_check(late_event, matched_event, preds) == True

    def test_no_invalidate_dist_unequal(self):
        # Distances differ → first predicate fails → DC still violated → NO retract
        late_event = make_event("late1", attrs={"trip_distance": 3.0, "fare_amount": 20.0})
        matched_event = make_event("e1", attrs={"trip_distance": 3.5, "fare_amount": 15.0})
        preds = [
            self._pred("trip_distance", "s", PredicateType.EQUAL, "trip_distance", "t"),
            self._pred("fare_amount", "s", PredicateType.LESS, "fare_amount", "t"),
        ]
        # dist_equal: 3.5≠3.0 → false → DC still violated → NO retract
        assert late_event_invalidate_check(late_event, matched_event, preds) == False

    def test_no_invalidate_fare_not_less(self):
        # Fare not less: s=15 not < t=10 → DC still violated → NO retract
        late_event = make_event("late1", attrs={"trip_distance": 3.5, "fare_amount": 10.0})
        matched_event = make_event("e1", attrs={"trip_distance": 3.5, "fare_amount": 15.0})
        preds = [
            self._pred("trip_distance", "s", PredicateType.EQUAL, "trip_distance", "t"),
            self._pred("fare_amount", "s", PredicateType.LESS, "fare_amount", "t"),
        ]
        # s.trip=3.5 == t.trip=3.5 ✓, but s.fare=15 NOT < t.fare=10 → DC still violated
        assert late_event_invalidate_check(late_event, matched_event, preds) == False

    def test_missing_attribute_returns_false(self):
        late_event = make_event("late1", attrs={})
        matched_event = make_event("e1", attrs={"fare_amount": 15.0})
        preds = [self._pred("fare_amount", "t", PredicateType.LESS_EQUAL, "fare_amount", "s")]
        assert late_event_invalidate_check(late_event, matched_event, preds) is False


# =============================================================================
# LateHandlerConfig
# =============================================================================

class TestLateHandlerConfig:
    def test_default_wait_seconds(self):
        cfg = LateHandlerConfig()
        assert cfg.wait_for_late_seconds == 300.0

    def test_custom_wait_seconds(self):
        cfg = LateHandlerConfig(wait_for_late_seconds=600.0)
        assert cfg.wait_for_late_seconds == 600.0

    def test_window_config_default_none(self):
        cfg = LateHandlerConfig()
        assert cfg.window_config is None


# =============================================================================
# handle_late_event — integration
# =============================================================================

class TestHandleLateEvent:
    @pytest.fixture
    def store(self):
        return AlertStateStore()

    @pytest.fixture
    def forest(self):
        from waves.weever import PaneForest
        return PaneForest.create()

    @pytest.fixture
    def config(self):
        from waves.windowing import WindowConfig
        return LateHandlerConfig(
            wait_for_late_seconds=300.0,
            window_config=WindowConfig(
                window_width=timedelta(hours=1),
                slide_step=timedelta(minutes=15),
                pane_size=timedelta(minutes=15),
            ),
        )

    def test_drop_beyond_lateness_threshold(self, store, forest, config):
        # Event from window that closed 10 minutes ago, wait_for_late=5 minutes
        old_end = int((utc_now() - timedelta(minutes=10)).timestamp() * 1000)
        late = make_event(
            "e_late",
            attrs={"x": 1.0},
            pane_id="p_old",
            window_id=f"w_0_{old_end}",
        )
        decisions = handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest,
            config=config,
            dim_map={"x": 0},
            lo_bounds=(0.0,),
            hi_bounds=(10.0,),
            active_boxes=[],
            get_pane_id_fn=lambda eid: "p_old",
        )
        assert decisions == []
        # Event should NOT be inserted
        assert forest.panes == []

    def test_retract_provisional_alert(self, store, forest, config):
        # Alert exists for this event
        alert = make_alert(
            alert_id="alert_DC1_w1_e1",
            dc_id="DC1",
            window_id="w1",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            event_ids=["e1", "e2"],
        )
        store.put(alert)

        late = make_event("e1", attrs={"x": 1.0}, pane_id="p1", window_id="w1")
        decisions = handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest,
            config=config,
            dim_map={"x": 0},
            lo_bounds=(0.0,),
            hi_bounds=(10.0,),
            active_boxes=[],
            get_pane_id_fn=lambda eid: "p1",
        )
        assert len(decisions) >= 1
        assert decisions[0].decision_type == "retraction"

    def test_no_retract_final_alert(self, store, forest, config):
        # Alert is already FINAL — should NOT be retracted
        alert = make_alert(
            alert_id="alert_DC1_w1_e1",
            dc_id="DC1",
            status=AlertStatus.FINAL,
            event_ids=["e1"],
        )
        store.put(alert)

        late = make_event("e1", attrs={"x": 1.0}, pane_id="p1", window_id="w1")
        decisions = handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest,
            config=config,
            dim_map={"x": 0},
            lo_bounds=(0.0,),
            hi_bounds=(10.0,),
            active_boxes=[],
            get_pane_id_fn=lambda eid: "p1",
        )
        # Should NOT retract a FINAL alert
        assert all(d.decision_type != "retraction" for d in decisions)

    def test_insert_into_active_pane(self, store, forest, config):
        # Late event should be inserted into pane
        pane = forest.find_or_create_pane(utc_now(), "w1", config.window_config)
        pane.is_active = True

        late = make_event("e_late", attrs={"x": 5.0}, pane_id=pane.pane_id, window_id="w1")
        handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest,
            config=config,
            dim_map={"x": 0},
            lo_bounds=(0.0,),
            hi_bounds=(10.0,),
            active_boxes=[],
            get_pane_id_fn=lambda eid: pane.pane_id,
        )
        # Event inserted into pane buffer
        assert len(pane.buffer) == 1
        assert pane.buffer[0][1] == "e_late"
        assert pane.buffer[0][0] == (5.0,)

    def test_no_insert_without_window_config(self, store, forest):
        cfg = LateHandlerConfig(wait_for_late_seconds=300.0, window_config=None)
        forest2 = forest
        late = make_event("e_late", attrs={"x": 5.0}, pane_id="p1", window_id="w1")
        handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest2,
            config=cfg,
            dim_map={"x": 0},
            lo_bounds=(0.0,),
            hi_bounds=(10.0,),
            active_boxes=[],
            get_pane_id_fn=lambda eid: "p1",
        )
        # No panes exist, no insert
        assert len(forest2.panes) == 0

    def test_recheck_insert_creates_pane(self, store, forest, config):
        # handle_late_event should create a pane and insert the late event
        late = make_event("e_late", attrs={"trip_distance": 3.5, "fare_amount": 9.0},
                          pane_id="auto_pane", window_id="w1")
        decisions = handle_late_event(
            late_event=late,
            alert_store=store,
            tombstone_mgr=None,
            pane_forest=forest,
            config=config,
            dim_map={"trip_distance": 0, "fare_amount": 1},
            lo_bounds=(0.0, 0.0),
            hi_bounds=(100.0, 500.0),
            active_boxes=[],
            get_pane_id_fn=lambda eid: "auto_pane",
        )
        # Late event was inserted into the pane created by pane_insert
        assert len(forest.panes) == 1
        pane = forest.panes[0]
        assert len(pane.buffer) == 1
        assert pane.buffer[0][1] == "e_late"
