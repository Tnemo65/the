"""Unit tests for Module 2.8 — Decision (AlertStore + Decision functions)."""

import pytest
from datetime import datetime, timezone

from waves.decision.alert_store import AlertStateStore, AlertStatus, AlertRecord
from waves.decision.decision import (
    process_candidate, finalize_window, retract_alert, cleanup_expired,
    ProvisionalDecision, FinalDecision, RetractionDecision,
)
from waves.tombstone import TombstoneManager


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_alert(
    alert_id="a1",
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
    )


# =============================================================================
# AlertStateStore — write operations
# =============================================================================

class TestAlertStateStorePut:
    def test_put_inserts_alert(self):
        store = AlertStateStore()
        alert = make_alert()
        store.put(alert)
        assert store.get("a1") is alert

    def test_put_updates_existing(self):
        store = AlertStateStore()
        a1 = make_alert(alert_id="a1", status=AlertStatus.PROVISIONAL)
        store.put(a1)
        a1.status = AlertStatus.FINAL
        store.put(a1)
        assert store.get("a1").status == AlertStatus.FINAL

    def test_put_indexes_by_window(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1"))
        store.put(make_alert("a2", window_id="w1"))
        store.put(make_alert("a3", window_id="w2"))
        by_w1 = store.get_by_window("w1")
        assert len(by_w1) == 2
        by_w2 = store.get_by_window("w2")
        assert len(by_w2) == 1

    def test_put_indexes_by_event(self):
        store = AlertStateStore()
        store.put(make_alert("a1", event_ids=["e1", "e2"]))
        by_e1 = store.get_by_event_id("e1")
        assert len(by_e1) == 1
        assert by_e1[0].alert_id == "a1"

    def test_put_idempotent_no_duplicate_window_index(self):
        store = AlertStateStore()
        a1 = make_alert("a1", window_id="w1")
        store.put(a1)
        store.put(a1)   # same alert, put twice
        assert len(store.get_by_window("w1")) == 1


class TestAlertStateStoreDelete:
    def test_delete_removes_alert(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        store.delete("a1")
        assert store.get("a1") is None

    def test_delete_removes_from_window_index(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1"))
        store.put(make_alert("a2", window_id="w1"))
        store.delete("a1")
        assert len(store.get_by_window("w1")) == 1

    def test_delete_removes_from_event_index(self):
        store = AlertStateStore()
        store.put(make_alert("a1", event_ids=["e1"]))
        store.put(make_alert("a2", event_ids=["e1"]))
        store.delete("a1")
        assert len(store.get_by_event_id("e1")) == 1

    def test_delete_nonexistent_noop(self):
        store = AlertStateStore()
        store.delete("nonexistent")  # no error


class TestAlertStateStoreRetract:
    def test_retract_sets_status_and_removes_from_store(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        retracted = store.retract("a1")
        assert retracted is not None
        assert retracted.status == AlertStatus.RETRACTED
        assert store.get("a1") is None

    def test_retract_removes_from_indexes(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1", event_ids=["e1"]))
        store.retract("a1")
        assert len(store.get_by_window("w1")) == 0
        assert len(store.get_by_event_id("e1")) == 0

    def test_retract_nonexistent_returns_none(self):
        store = AlertStateStore()
        assert store.retract("nonexistent") is None

    def test_retract_already_retracted_returns_none(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        store.retract("a1")
        # Already retracted — not in store anymore
        assert store.retract("a1") is None


# =============================================================================
# AlertStateStore — read operations
# =============================================================================

class TestAlertStateStoreRead:
    def test_get_returns_alert(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        assert store.get("a1").alert_id == "a1"

    def test_get_not_found(self):
        store = AlertStateStore()
        assert store.get("nonexistent") is None

    def test_get_by_window_and_dc(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1", dc_id="DC1"))
        store.put(make_alert("a2", window_id="w1", dc_id="DC2"))
        store.put(make_alert("a3", window_id="w1", dc_id="DC1"))
        results = store.get_by_window_and_dc("w1", "DC1")
        assert len(results) == 2

    def test_has_active_true_for_provisional(self):
        store = AlertStateStore()
        store.put(make_alert("a1", status=AlertStatus.PROVISIONAL))
        assert store.has_active("a1") is True

    def test_has_active_true_for_final(self):
        store = AlertStateStore()
        store.put(make_alert("a1", status=AlertStatus.FINAL))
        assert store.has_active("a1") is True

    def test_has_active_false_for_retracted(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        store.retract("a1")
        assert store.has_active("a1") is False

    def test_has_active_false_for_nonexistent(self):
        store = AlertStateStore()
        assert store.has_active("nonexistent") is False


# =============================================================================
# AlertStateStore — introspection
# =============================================================================

class TestAlertStateStoreIntrospection:
    def test_count(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        store.put(make_alert("a2"))
        assert store.count() == 2

    def test_count_by_status(self):
        store = AlertStateStore()
        store.put(make_alert("a1", status=AlertStatus.PROVISIONAL))
        store.put(make_alert("a2", status=AlertStatus.PROVISIONAL))
        store.put(make_alert("a3", status=AlertStatus.FINAL))
        assert store.count_by_status(AlertStatus.PROVISIONAL) == 2
        assert store.count_by_status(AlertStatus.FINAL) == 1

    def test_clear(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        store.clear()
        assert store.count() == 0


# =============================================================================
# process_candidate
# =============================================================================

class TestProcessCandidate:
    def _make_candidate(self, dc_id="DC1", window_id="w1", pane_id="p1",
                        query_id="e1", matched_ids=None):
        from waves.rapidash.candidate import CandidateViolation
        if matched_ids is None:
            matched_ids = ["e2"]
        return CandidateViolation(
            dc_id=dc_id,
            window_id=window_id,
            pane_id=pane_id,
            query_id=query_id,
            matched_ids=matched_ids,
            box_id="box1",
            timestamp_ms=0,
        )

    def test_creates_provisional_alert(self):
        from waves.tombstone import TombstoneManager
        store = AlertStateStore()
        tm = TombstoneManager()
        cand = self._make_candidate()
        decisions = process_candidate(cand, store, tm)
        assert len(decisions) == 1
        assert isinstance(decisions[0], ProvisionalDecision)
        assert decisions[0].alert.dc_id == "DC1"

    def test_idempotent_no_duplicate_for_same_candidate(self):
        store = AlertStateStore()
        tm = TombstoneManager()
        cand = self._make_candidate()
        process_candidate(cand, store, tm)
        decisions = process_candidate(cand, store, tm)
        assert decisions == []
        assert store.count() == 1

    def test_all_event_ids_indexed(self):
        store = AlertStateStore()
        tm = TombstoneManager()
        cand = self._make_candidate(query_id="e1", matched_ids=["e2", "e3"])
        process_candidate(cand, store, tm)
        assert len(store.get_by_event_id("e1")) == 1
        assert len(store.get_by_event_id("e2")) == 1
        assert len(store.get_by_event_id("e3")) == 1

    def test_skips_retracted_alert(self):
        store = AlertStateStore()
        tm = TombstoneManager()
        # Pre-existing retracted alert
        store.put(make_alert("alert_DC1_w1_e1", window_id="w1", dc_id="DC1"))
        store.retract("alert_DC1_w1_e1")
        # New candidate with same id
        cand = self._make_candidate()
        decisions = process_candidate(cand, store, tm)
        # Should create new alert (old one was retracted)
        assert len(decisions) == 1


# =============================================================================
# finalize_window
# =============================================================================

class TestFinalizeWindow:
    def test_promotes_provisional_to_final(self):
        store = AlertStateStore()
        a = make_alert("a1", window_id="w1", status=AlertStatus.PROVISIONAL)
        store.put(a)
        decisions = finalize_window("w1", store)
        assert len(decisions) == 1
        assert isinstance(decisions[0], FinalDecision)
        assert store.get("a1").status == AlertStatus.FINAL

    def test_does_not_change_final_alerts(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1", status=AlertStatus.FINAL))
        decisions = finalize_window("w1", store)
        assert decisions == []

    def test_finalize_unknown_window_returns_empty(self):
        store = AlertStateStore()
        decisions = finalize_window("nonexistent", store)
        assert decisions == []

    def test_multiple_alerts_finalized(self):
        store = AlertStateStore()
        store.put(make_alert("a1", window_id="w1", status=AlertStatus.PROVISIONAL))
        store.put(make_alert("a2", window_id="w1", status=AlertStatus.PROVISIONAL))
        store.put(make_alert("a3", window_id="w1", status=AlertStatus.PROVISIONAL))
        decisions = finalize_window("w1", store)
        assert len(decisions) == 3


# =============================================================================
# retract_alert
# =============================================================================

class TestRetractAlert:
    def test_retract_removes_alert_and_tombstones_events(self):
        from waves.tombstone import TombstoneManager
        store = AlertStateStore()
        tm = TombstoneManager()
        tm.create_pane("p1")   # pane must exist for tombstone to work
        store.put(make_alert("a1", event_ids=["e1", "e2"], pane_id="p1"))
        decisions = retract_alert("a1", store, tm)
        assert len(decisions) == 1
        assert isinstance(decisions[0], RetractionDecision)
        assert decisions[0].tombstoned_event_ids == ["e1", "e2"]
        assert store.get("a1") is None

    def test_retract_nonexistent_returns_empty(self):
        store = AlertStateStore()
        tm = TombstoneManager()
        decisions = retract_alert("nonexistent", store, tm)
        assert decisions == []

    def test_retract_already_retracted_returns_empty(self):
        store = AlertStateStore()
        tm = TombstoneManager()
        store.put(make_alert("a1"))
        store.retract("a1")
        decisions = retract_alert("a1", store, tm)
        assert decisions == []

    def test_retract_without_tombstone_mgr(self):
        store = AlertStateStore()
        store.put(make_alert("a1"))
        decisions = retract_alert("a1", store, None)
        assert len(decisions) == 1
        assert store.get("a1") is None

    def test_retract_tombstones_in_correct_pane(self):
        from waves.tombstone import TombstoneManager
        store = AlertStateStore()
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.create_pane("p2")
        store.put(make_alert("a1", pane_id="p1", event_ids=["e1", "e2"]))
        store.put(make_alert("a2", pane_id="p2", event_ids=["e3"]))
        retract_alert("a1", store, tm)  # tombstones e1, e2 in p1
        retract_alert("a2", store, tm)  # tombstones e3 in p2
        assert tm.contains("e1", "p1") is True
        assert tm.contains("e2", "p1") is True
        assert tm.contains("e3", "p2") is True
        assert tm.contains("e1", "p2") is False


# =============================================================================
# cleanup_expired
# =============================================================================

class TestCleanupExpired:
    def test_deletes_old_final_alerts(self):
        from datetime import timedelta
        store = AlertStateStore()
        alert = make_alert("a1", status=AlertStatus.FINAL)
        alert.finalized_at = utc_now() - timedelta(seconds=10)
        store.put(alert)
        deleted = cleanup_expired(store, ttl_seconds=5)
        assert deleted == 1
        assert store.get("a1") is None

    def test_keeps_recent_final_alerts(self):
        store = AlertStateStore()
        store.put(make_alert("a1", status=AlertStatus.FINAL))
        deleted = cleanup_expired(store, ttl_seconds=3600)
        assert deleted == 0
        assert store.get("a1") is not None

    def test_keeps_provisional_alerts(self):
        store = AlertStateStore()
        store.put(make_alert("a1", status=AlertStatus.PROVISIONAL))
        deleted = cleanup_expired(store, ttl_seconds=0)
        assert deleted == 0
        assert store.get("a1") is not None


# =============================================================================
# Decision class structure
# =============================================================================

class TestDecisionClasses:
    def test_provisional_decision_type(self):
        alert = make_alert()
        d = ProvisionalDecision(alert)
        assert d.decision_type == "provisional"
        assert d.alert is alert

    def test_final_decision_type(self):
        alert = make_alert(status=AlertStatus.FINAL)
        d = FinalDecision(alert)
        assert d.decision_type == "final"
        assert d.alert is alert

    def test_retraction_decision_type(self):
        alert = make_alert(status=AlertStatus.RETRACTED)
        d = RetractionDecision(alert, ["e1", "e2"])
        assert d.decision_type == "retraction"
        assert d.tombstoned_event_ids == ["e1", "e2"]
