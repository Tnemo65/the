"""Unit tests for Module 2.9 — Tombstone (TombstoneManager + TombstoneFilter)."""

import pytest
from waves.tombstone import TombstoneFilter, TombstoneManager


# =============================================================================
# TombstoneFilter
# =============================================================================

class TestTombstoneFilter:
    def test_add_and_contains(self):
        f = TombstoneFilter()
        f.add("e1")
        assert f.contains("e1") is True
        assert f.contains("e2") is False

    def test_contains_unknown_returns_false(self):
        f = TombstoneFilter()
        assert f.contains("nonexistent") is False

    def test_clear(self):
        f = TombstoneFilter()
        f.add("e1")
        f.clear()
        assert f.contains("e1") is False


class TestTombstoneManager:
    def test_create_pane(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        assert tm.get_filter("p1") is not None
        assert tm.pane_count() == 1

    def test_create_pane_idempotent(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.create_pane("p1")
        assert tm.pane_count() == 1

    def test_add_single(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.add("e1", "p1")
        assert tm.contains("e1", "p1") is True

    def test_add_unknown_pane_noop(self):
        tm = TombstoneManager()
        tm.add("e1", "nonexistent")
        assert tm.contains("e1", "nonexistent") is False

    def test_add_many(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.add_many(["e1", "e2", "e3"], "p1")
        assert tm.contains("e1", "p1") is True
        assert tm.contains("e2", "p1") is True
        assert tm.contains("e3", "p1") is True

    def test_contains_unknown_pane_returns_false(self):
        tm = TombstoneManager()
        assert tm.contains("e1", "nonexistent") is False

    def test_drop_pane(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.add("e1", "p1")
        tm.drop_pane("p1")
        assert tm.get_filter("p1") is None
        assert tm.pane_count() == 0
        assert tm.contains("e1", "p1") is False

    def test_drop_pane_idempotent(self):
        tm = TombstoneManager()
        tm.drop_pane("nonexistent")  # no error
        assert tm.pane_count() == 0

    def test_total_tombstoned(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.create_pane("p2")
        tm.add_many(["e1", "e2"], "p1")
        tm.add("e3", "p2")
        assert tm.total_tombstoned() == 3

    def test_pane_lifecycle_create_add_drop(self):
        tm = TombstoneManager()
        tm.create_pane("p1")
        tm.add("e1", "p1")
        tm.add("e2", "p1")
        assert tm.pane_count() == 1
        assert tm.total_tombstoned() == 2
        tm.drop_pane("p1")
        assert tm.pane_count() == 0
        assert tm.total_tombstoned() == 0
