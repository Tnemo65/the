"""Unit tests for Module 2.11b — EventStore."""

import pytest
from datetime import datetime, timezone

from waves.store import EventStore
from waves.ingestion.schema import DataEvent


def make_event(event_id, pane_id=None, window_id=None, attrs=None):
    return DataEvent(
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        ingestion_time=datetime.now(timezone.utc),
        pane_id=pane_id,
        window_id=window_id,
        attributes=attrs or {},
    )


class TestEventStorePut:
    def test_put_stores_event(self):
        store = EventStore()
        e = make_event("e1", pane_id="p1", window_id="w1")
        store.put(e)
        assert store.get("e1") is e

    def test_put_indexes_pane_id(self):
        store = EventStore()
        e = make_event("e1", pane_id="p1")
        store.put(e)
        assert store.get_pane_id("e1") == "p1"

    def test_put_indexes_window_id(self):
        store = EventStore()
        e = make_event("e1", window_id="w1")
        store.put(e)
        assert store.get_window_id("e1") == "w1"

    def test_put_without_pane_id(self):
        store = EventStore()
        e = make_event("e1", pane_id=None)
        store.put(e)
        assert store.get_pane_id("e1") == ""

    def test_put_without_window_id(self):
        store = EventStore()
        e = make_event("e1", window_id=None)
        store.put(e)
        assert store.get_window_id("e1") == ""

    def test_put_overwrites(self):
        store = EventStore()
        e1 = make_event("e1", pane_id="p1", window_id="w1")
        store.put(e1)
        e2 = make_event("e1", pane_id="p2", window_id="w2")
        store.put(e2)
        assert store.get("e1") is e2
        assert store.get_pane_id("e1") == "p2"
        assert store.get_window_id("e1") == "w2"


class TestEventStoreGet:
    def test_get_returns_event(self):
        store = EventStore()
        e = make_event("e1")
        store.put(e)
        assert store.get("e1") is e

    def test_get_not_found(self):
        store = EventStore()
        assert store.get("nonexistent") is None

    def test_has_true(self):
        store = EventStore()
        store.put(make_event("e1"))
        assert store.has("e1") is True

    def test_has_false(self):
        store = EventStore()
        assert store.has("nonexistent") is False


class TestEventStoreLookup:
    def test_pane_id_not_found(self):
        store = EventStore()
        assert store.get_pane_id("nonexistent") == ""

    def test_window_id_not_found(self):
        store = EventStore()
        assert store.get_window_id("nonexistent") == ""

    def test_multiple_events_unique_ids(self):
        store = EventStore()
        for i in range(10):
            store.put(make_event(f"e{i}", pane_id=f"p{i}", window_id=f"w{i}"))
        for i in range(10):
            assert store.get_pane_id(f"e{i}") == f"p{i}"
            assert store.get_window_id(f"e{i}") == f"w{i}"


class TestEventStoreIntrospection:
    def test_count(self):
        store = EventStore()
        store.put(make_event("e1"))
        store.put(make_event("e2"))
        assert store.count() == 2

    def test_count_after_clear(self):
        store = EventStore()
        store.put(make_event("e1"))
        store.clear()
        assert store.count() == 0

    def test_clear(self):
        store = EventStore()
        store.put(make_event("e1"))
        store.clear()
        assert store.get("e1") is None
        assert store.get_pane_id("e1") == ""
