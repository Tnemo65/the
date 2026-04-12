"""Unit tests for Module 2.7 — Weever (PaneForest)."""

import pytest
from datetime import datetime, timedelta, timezone
from typing import Optional

from waves.windowing.pane import WindowConfig
from waves.weever.pane_forest import PaneForest, _floor_ts


def utc(*args, **kwargs) -> datetime:
    return datetime(*args, **kwargs, tzinfo=timezone.utc)


@pytest.fixture
def config() -> WindowConfig:
    return WindowConfig(
        window_width=timedelta(hours=1),
        slide_step=timedelta(minutes=15),
        pane_size=timedelta(minutes=15),
    )


@pytest.fixture
def forest() -> PaneForest:
    return PaneForest.create()


# =============================================================================
# _floor_ts helper
# =============================================================================

class TestFloorTs:
    def test_floors_to_boundary(self):
        t = utc(2026, 4, 12, 10, 17, 30)
        step = timedelta(minutes=15)
        result = _floor_ts(t, step)
        assert result == utc(2026, 4, 12, 10, 15, 0)

    def test_exact_boundary_no_change(self):
        t = utc(2026, 4, 12, 10, 15, 0)
        step = timedelta(minutes=15)
        result = _floor_ts(t, step)
        assert result == utc(2026, 4, 12, 10, 15, 0)

    def test_zero_step_returns_original(self):
        t = utc(2026, 4, 12, 10, 0, 0)
        result = _floor_ts(t, timedelta(0))
        assert result == t


# =============================================================================
# PaneForest.create
# =============================================================================

class TestPaneForestCreate:
    def test_create_empty(self, forest):
        assert forest.panes == []
        assert forest.panes_by_id == {}
        assert forest.tombstone_mgr is None

    def test_create_with_tombstone(self):
        from waves.tombstone import TombstoneManager
        tm = TombstoneManager()
        forest = PaneForest.create(tombstone_mgr=tm)
        assert forest.tombstone_mgr is tm


# =============================================================================
# find_or_create_pane
# =============================================================================

class TestFindOrCreatePane:
    def test_creates_new_pane(self, forest, config):
        t = utc(2026, 4, 12, 10, 17, 0)
        pane = forest.find_or_create_pane(t, "w_test", config)
        assert pane is not None
        assert pane.is_active is True
        assert pane.buffer == []
        assert pane.window_id == "w_test"
        assert len(forest.panes) == 1

    def test_returns_existing_pane(self, forest, config):
        t1 = utc(2026, 4, 12, 10, 17, 0)
        t2 = utc(2026, 4, 12, 10, 18, 0)   # same pane window
        p1 = forest.find_or_create_pane(t1, "w_test", config)
        p2 = forest.find_or_create_pane(t2, "w_test", config)
        assert p1 is p2
        assert len(forest.panes) == 1

    def test_different_panes_different_ids(self, forest, config):
        t1 = utc(2026, 4, 12, 10, 10, 0)   # pane 10:00-10:15
        t2 = utc(2026, 4, 12, 10, 20, 0)  # pane 10:15-10:30
        p1 = forest.find_or_create_pane(t1, "w_test", config)
        p2 = forest.find_or_create_pane(t2, "w_test", config)
        assert p1.pane_id != p2.pane_id
        assert len(forest.panes) == 2

    def test_pane_id_format(self, forest, config):
        t = utc(2026, 4, 12, 10, 17, 0)
        pane = forest.find_or_create_pane(t, "w_test", config)
        assert pane.pane_id.startswith("p_")
        # Verify pane_id has two parts (start, end)
        parts = pane.pane_id.split("_")
        assert len(parts) == 3
        # End > start
        assert int(parts[2]) > int(parts[1])


# =============================================================================
# pane_insert
# =============================================================================

class TestPaneInsert:
    def test_insert_appends_to_buffer(self, forest, config):
        t = utc(2026, 4, 12, 10, 17, 0)
        point = (3.5, 7.5)
        pane = forest.pane_insert(t, "e1", point, "w_test", config)
        assert len(pane.buffer) == 1
        assert pane.buffer[0] == (point, "e1")
        assert pane.size_hint == 1

    def test_insert_multiple_events_same_pane(self, forest, config):
        t = utc(2026, 4, 12, 10, 17, 0)
        forest.pane_insert(t, "e1", (1.0, 2.0), "w_test", config)
        forest.pane_insert(t, "e2", (3.0, 4.0), "w_test", config)
        pane = forest.find_or_create_pane(t, "w_test", config)
        assert len(pane.buffer) == 2

    def test_insert_multiple_panes(self, forest, config):
        t1 = utc(2026, 4, 12, 10, 10, 0)
        t2 = utc(2026, 4, 12, 10, 25, 0)
        p1 = forest.pane_insert(t1, "e1", (1.0,), "w_test", config)
        p2 = forest.pane_insert(t2, "e2", (2.0,), "w_test", config)
        assert p1 is not p2
        assert p1.buffer[0][1] == "e1"
        assert p2.buffer[0][1] == "e2"

    def test_insert_into_closed_pane_returns_pane_but_does_not_append(self, forest, config):
        t = utc(2026, 4, 12, 10, 10, 0)
        pane = forest.pane_insert(t, "e1", (1.0,), "w_test", config)
        assert pane.is_active is True
        # Close it manually
        forest.pane_close(pane.pane_id, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert pane.is_active is False
        # Try to insert again — pane is closed, should not append
        old_len = len(pane.buffer)
        forest.pane_insert(t, "e2", (2.0,), "w_test", config)
        # Buffer unchanged since pane is closed
        assert len(pane.buffer) == old_len


# =============================================================================
# pane_close
# =============================================================================

class TestPaneClose:
    def test_close_builds_kdtree(self, forest, config):
        t = utc(2026, 4, 12, 10, 10, 0)
        forest.pane_insert(t, "e1", (1.0, 2.0), "w_test", config)
        forest.pane_insert(t, "e2", (3.0, 4.0), "w_test", config)
        pane = forest.find_or_create_pane(t, "w_test", config)
        forest.pane_close(pane.pane_id, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        assert pane.is_active is False
        assert pane.kdtree is not None
        assert pane.buffer == []

    def test_close_empty_buffer_no_kdtree(self, forest, config):
        t = utc(2026, 4, 12, 10, 10, 0)
        pane = forest.find_or_create_pane(t, "w_test", config)
        forest.pane_close(pane.pane_id, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        assert pane.is_active is False
        assert pane.kdtree is None

    def test_close_nonexistent_pane_noop(self, forest):
        forest.pane_close("nonexistent", dim_count=2, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert forest.panes == []

    def test_close_creates_tombstone_filter(self, forest, config):
        from waves.tombstone import TombstoneManager
        tm = TombstoneManager()
        forest = PaneForest.create(tombstone_mgr=tm)
        t = utc(2026, 4, 12, 10, 10, 0)
        forest.pane_insert(t, "e1", (1.0,), "w_test", config)
        pane = forest.find_or_create_pane(t, "w_test", config)
        forest.pane_close(pane.pane_id, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert tm.get_filter(pane.pane_id) is not None


# =============================================================================
# window_slide
# =============================================================================

class TestWindowSlide:
    def test_slide_closes_and_drops_expired_panes(self, forest, config):
        # Pane 09:45-10:00 (expired at watermark=10:00)
        t1 = utc(2026, 4, 12, 9, 50, 0)
        # Pane 10:00-10:15 (still active at watermark=10:00)
        t2 = utc(2026, 4, 12, 10, 5, 0)

        p1 = forest.pane_insert(t1, "e1", (1.0,), "w_test", config)
        p2 = forest.pane_insert(t2, "e2", (2.0,), "w_test", config)

        watermark = utc(2026, 4, 12, 10, 0, 0)
        dropped = forest.window_slide(
            watermark,
            config,
            dim_count=1,
            lo_bounds=(0.0,),
            hi_bounds=(100.0,),
        )
        assert len(dropped) == 1
        assert dropped[0].pane_id == p1.pane_id
        # p2 still in forest
        assert len(forest.panes) == 1
        assert p2.is_active is True

    def test_slide_no_expired_returns_empty(self, forest, config):
        t = utc(2026, 4, 12, 10, 5, 0)
        forest.pane_insert(t, "e1", (1.0,), "w_test", config)
        watermark = utc(2026, 4, 12, 10, 0, 0)   # before pane expires
        dropped = forest.window_slide(watermark, config, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert dropped == []
        assert len(forest.panes) == 1

    def test_slide_clears_tombstone_filters(self, forest, config):
        from waves.tombstone import TombstoneManager
        tm = TombstoneManager()
        forest = PaneForest.create(tombstone_mgr=tm)
        t = utc(2026, 4, 12, 9, 50, 0)
        forest.pane_insert(t, "e1", (1.0,), "w_test", config)
        pane = forest.find_or_create_pane(t, "w_test", config)
        forest.pane_close(pane.pane_id, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert tm.get_filter(pane.pane_id) is not None
        watermark = utc(2026, 4, 12, 10, 0, 0)
        dropped = forest.window_slide(watermark, config, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert tm.get_filter(pane.pane_id) is None

    def test_slide_returns_dropped_panes_with_kdtree(self, forest, config):
        t = utc(2026, 4, 12, 9, 50, 0)
        forest.pane_insert(t, "e1", (1.0,), "w_test", config)
        pane = forest.find_or_create_pane(t, "w_test", config)
        watermark = utc(2026, 4, 12, 10, 0, 0)
        dropped = forest.window_slide(watermark, config, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert len(dropped) == 1
        assert dropped[0].kdtree is not None


# =============================================================================
# get_pane_by_id / get_roots / active/closed
# =============================================================================

class TestPaneForestAccessors:
    def test_get_pane_by_id_found(self, forest, config):
        t = utc(2026, 4, 12, 10, 10, 0)
        pane = forest.find_or_create_pane(t, "w_test", config)
        assert forest.get_pane_by_id(pane.pane_id) is pane

    def test_get_pane_by_id_not_found(self, forest):
        assert forest.get_pane_by_id("nonexistent") is None

    def test_get_roots_returns_kdtrees_only(self, forest, config):
        t1 = utc(2026, 4, 12, 9, 50, 0)
        t2 = utc(2026, 4, 12, 10, 5, 0)
        forest.pane_insert(t1, "e1", (1.0,), "w_test", config)
        forest.pane_insert(t2, "e2", (2.0,), "w_test", config)
        pane1 = forest.find_or_create_pane(t1, "w_test", config)
        forest.pane_close(pane1.pane_id, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        roots = forest.get_roots("any_group")
        assert len(roots) == 1

    def test_active_and_closed_panes(self, forest, config):
        t1 = utc(2026, 4, 12, 9, 50, 0)
        t2 = utc(2026, 4, 12, 10, 5, 0)
        forest.pane_insert(t1, "e1", (1.0,), "w_test", config)
        forest.pane_insert(t2, "e2", (2.0,), "w_test", config)
        pane1 = forest.find_or_create_pane(t1, "w_test", config)
        forest.pane_close(pane1.pane_id, dim_count=1, lo_bounds=(0.0,), hi_bounds=(10.0,))
        assert len(forest.active_panes()) == 1
        assert len(forest.closed_panes()) == 1
