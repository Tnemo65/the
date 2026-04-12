"""Unit tests cho Module 2.3 — BasicDQChecks."""
from datetime import datetime, timezone

import pytest

from waves.basic_dq import (
    BasicDQChecker,
    BasicDQRule,
    CheckResult,
    DQResult,
    QualityMetaStream,
    WindowMeta,
    MetaEvent,
)


# ─── Mock DataEvent ───────────────────────────────────────────────────────────

class MockEvent:
    def __init__(self, event_id, attributes=None, window_id=None):
        self.event_id = event_id
        self.attributes = attributes or {}
        self.window_id = window_id


# ─── DQResult ─────────────────────────────────────────────────────────────────

class TestDQResult:
    def test_values(self):
        assert DQResult.PASS.value == "pass"
        assert DQResult.FAIL.value == "fail"
        assert DQResult.SKIP.value == "skip"


# ─── BasicDQRule ─────────────────────────────────────────────────────────────

class TestBasicDQRule:
    def test_null_rule(self):
        rule = BasicDQRule(rule_id="r1", rule_type="null", field="fare_amount")
        assert rule.rule_id == "r1"
        assert rule.rule_type == "null"

    def test_type_rule(self):
        rule = BasicDQRule(
            rule_id="r2", rule_type="type", field="trip_distance",
            params={"type": float}
        )
        assert rule.params["type"] == float

    def test_range_rule(self):
        rule = BasicDQRule(
            rule_id="r3", rule_type="range", field="fare_amount",
            params={"min": 0.0, "max": 1000.0}
        )
        assert rule.params["min"] == 0.0
        assert rule.params["max"] == 1000.0

    def test_regex_rule(self):
        rule = BasicDQRule(
            rule_id="r4", rule_type="regex", field="vendor_id",
            params={"pattern": r"^[A-Z]\d+$"}
        )
        assert rule.params["pattern"] == r"^[A-Z]\d+$"


# ─── BasicDQChecker ─────────────────────────────────────────────────────────

class TestBasicDQChecker:
    @pytest.fixture
    def checker(self):
        rules = [
            BasicDQRule(rule_id="null_fare", rule_type="null", field="fare_amount"),
            BasicDQRule(rule_id="type_dist", rule_type="type", field="trip_distance", params={"type": float}),
            BasicDQRule(rule_id="range_fare", rule_type="range", field="fare_amount", params={"min": 0, "max": 1000}),
            BasicDQRule(rule_id="regex_vendor", rule_type="regex", field="vendor_id", params={"pattern": r"^[A-Z]\d{2}$"}),
        ]
        return BasicDQChecker(rules)

    def test_null_pass(self, checker):
        event = MockEvent("e1", {"fare_amount": 10.0})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "null_fare")
        assert r.result == DQResult.PASS

    def test_null_fail(self, checker):
        event = MockEvent("e1", {"fare_amount": None})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "null_fare")
        assert r.result == DQResult.FAIL
        assert r.detail["field"] == "fare_amount"

    def test_type_pass(self, checker):
        event = MockEvent("e1", {"trip_distance": 5.5})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "type_dist")
        assert r.result == DQResult.PASS

    def test_type_fail_wrong_type(self, checker):
        event = MockEvent("e1", {"trip_distance": "five"})  # string instead of float
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "type_dist")
        assert r.result == DQResult.FAIL

    def test_range_pass(self, checker):
        event = MockEvent("e1", {"fare_amount": 50.0})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "range_fare")
        assert r.result == DQResult.PASS

    def test_range_fail_below_min(self, checker):
        event = MockEvent("e1", {"fare_amount": -5.0})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "range_fare")
        assert r.result == DQResult.FAIL
        assert r.detail["value"] == -5.0

    def test_range_fail_above_max(self, checker):
        event = MockEvent("e1", {"fare_amount": 2000.0})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "range_fare")
        assert r.result == DQResult.FAIL
        assert r.detail["value"] == 2000.0

    def test_regex_pass(self, checker):
        event = MockEvent("e1", {"vendor_id": "V99"})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "regex_vendor")
        assert r.result == DQResult.PASS

    def test_regex_fail(self, checker):
        event = MockEvent("e1", {"vendor_id": "INVALID"})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "regex_vendor")
        assert r.result == DQResult.FAIL

    def test_regex_none_value_passes(self, checker):
        event = MockEvent("e1", {"vendor_id": None})
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "regex_vendor")
        # None values are skipped for regex (field is null/not-present)
        assert r.result == DQResult.PASS

    def test_missing_field_is_none(self, checker):
        event = MockEvent("e1", {})  # no fare_amount
        results = checker.check_event(event)
        r = next(r for r in results if r.rule_id == "null_fare")
        assert r.result == DQResult.FAIL

    def test_counters_increment_on_fail(self, checker):
        event = MockEvent("e1", {"fare_amount": None, "trip_distance": 5.0,
                                  "vendor_id": "V99"})
        checker.check_event(event)
        counters = checker.get_counters()
        assert counters["null_fare"] == 1
        assert counters["type_dist"] == 0
        assert counters["range_fare"] == 0

    def test_multiple_events_counters(self, checker):
        for _ in range(3):
            e = MockEvent("e", {"fare_amount": None})
            checker.check_event(e)
        counters = checker.get_counters()
        assert counters["null_fare"] == 3

    def test_build_window_meta(self, checker):
        for i in range(10):
            e = MockEvent(f"e{i}", {"fare_amount": 10.0, "trip_distance": 5.0,
                                     "vendor_id": "V99"})
            checker.check_event(e)
        # Inject 2 failures
        e_bad = MockEvent("e_bad", {"fare_amount": None, "trip_distance": 5.0,
                                     "vendor_id": "V99"})
        checker.check_event(e_bad)
        e_bad2 = MockEvent("e_bad2", {"fare_amount": -1.0, "trip_distance": 5.0,
                                       "vendor_id": "V99"})
        checker.check_event(e_bad2)

        meta = checker.build_window_meta("w_test")
        assert meta.window_id == "w_test"
        # total = sum of all rule checks = 12 events * 4 rules
        assert meta.total_count == 48
        assert meta.fail_counts["null_fare"] == 1
        assert meta.fail_counts["range_fare"] == 1
        # pass_rate = 1 - 2/48
        assert abs(meta.pass_rate - (46.0 / 48.0)) < 0.001

    def test_get_totals(self, checker):
        e = MockEvent("e1", {"fare_amount": 10.0})
        checker.check_event(e)
        totals = checker.get_totals()
        assert totals["null_fare"] == 1

    def test_all_rules_checked(self, checker):
        event = MockEvent("e1", {"fare_amount": 10.0, "trip_distance": 5.0,
                                  "vendor_id": "V99"})
        results = checker.check_event(event)
        assert len(results) == 4  # all 4 rules checked


# ─── WindowMeta ───────────────────────────────────────────────────────────────

class TestWindowMeta:
    def test_basic(self):
        meta = WindowMeta(
            window_id="w_123",
            total_count=100,
            fail_counts={"null_fare": 2, "range_fare": 1},
            pass_rate=0.97,
        )
        assert meta.window_id == "w_123"
        assert meta.total_count == 100
        assert meta.fail_counts["null_fare"] == 2
        assert meta.pass_rate == 0.97


# ─── QualityMetaStream ────────────────────────────────────────────────────────

class TestQualityMetaStream:
    def test_emit_creates_meta_event(self):
        stream = QualityMetaStream()
        meta = WindowMeta(window_id="w_test", total_count=100,
                          fail_counts={"null_fare": 2}, pass_rate=0.98)
        event = stream.emit(meta)
        assert isinstance(event, MetaEvent)
        assert event.window_id == "w_test"
        assert event.total == 100
        assert event.pass_count == 98
        assert event.fail_count == 2
        assert event.violation_counts["null_fare"] == 2

    def test_history_stored(self):
        stream = QualityMetaStream()
        meta = WindowMeta(window_id="w_1", total_count=10,
                          fail_counts={}, pass_rate=1.0)
        stream.emit(meta)
        meta2 = WindowMeta(window_id="w_2", total_count=20,
                          fail_counts={"range_fare": 1}, pass_rate=0.95)
        stream.emit(meta2)
        history = stream.get_history()
        assert len(history) == 2
        assert history[1].window_id == "w_2"

    def test_sink_callback(self):
        received = []
        def sink(e):
            received.append(e)
        stream = QualityMetaStream(sink=sink)
        meta = WindowMeta(window_id="w_sink", total_count=5,
                          fail_counts={}, pass_rate=1.0)
        stream.emit(meta)
        assert len(received) == 1
        assert received[0].window_id == "w_sink"

    def test_clear_history(self):
        stream = QualityMetaStream()
        meta = WindowMeta(window_id="w_clear", total_count=5,
                          fail_counts={}, pass_rate=1.0)
        stream.emit(meta)
        stream.clear()
        assert len(stream.get_history()) == 0

    def test_pass_count_calculation(self):
        stream = QualityMetaStream()
        # total=100, fails: null_fare=3, range_fare=2 -> fail_count=5, pass_count=95
        meta = WindowMeta(window_id="w_calc", total_count=100,
                          fail_counts={"null_fare": 3, "range_fare": 2},
                          pass_rate=0.95)
        event = stream.emit(meta)
        assert event.fail_count == 5
        assert event.pass_count == 95
