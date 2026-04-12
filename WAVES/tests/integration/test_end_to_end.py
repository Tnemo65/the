"""Integration tests: end-to-end DC1/DC2/DC3 flows through the full WavePipeline."""

import pytest
from datetime import datetime, timedelta, timezone

from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import DataEvent
from waves.decision.alert_store import AlertStatus
from waves.decision.decision import ProvisionalDecision, FinalDecision
from waves.store import EventStore
from waves.weever import PaneForest
from waves.tombstone import TombstoneManager
from waves.decision.alert_store import AlertStateStore
from waves.logical_engine import DenialConstraint


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def dc_rules():
    return [
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
        {
            "dc_id": "DC3",
            "rule_group": "toll_route",
            "description": "Toll Route Anomaly",
            "predicates": [
                {"left_col": "PULocationID_s", "operator": "EQUAL", "right_col": "PULocationID_t"},
                {"left_col": "DOLocationID_s", "operator": "EQUAL", "right_col": "DOLocationID_t"},
                {"left_col": "tolls_amount_s", "operator": "LESS_EQUAL", "right_col": "tolls_amount_t"},
            ],
        },
    ]


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


def setup_pane(pipeline, events_and_points, wcfg):
    """Insert events, close pane, return (pane_id, pane)."""
    pane = None
    pane_id = None
    for event, point in events_and_points:
        pane = pipeline._pane_forest.pane_insert(
            event.event_time, event.event_id, point,
            event.window_id or "w_test", wcfg
        )
        pane_id = pane.pane_id
    if pane is not None and pane.buffer:
        dim_count = len(events_and_points[0][1]) if events_and_points else 2
        lo = tuple(0.0 for _ in range(dim_count))
        hi = tuple(1000.0 for _ in range(dim_count))
        pipeline._pane_forest.pane_close(pane_id, dim_count, lo, hi)
    pane = pipeline._pane_forest.get_pane_by_id(pane_id)
    return pane_id, pane


# ─── DC1: Fare-Distance Dominance ────────────────────────────────────────────

class TestDC1FareDistanceDominance:

    def test_dc1_provisional_alert_created(self, dc_rules):
        """Same distance, cheaper trip -> DC1 fires."""
        config = PipelineConfig(k_max=4)
        pipeline = WavePipeline(config)
        pipeline.load_dc_rules(dc_rules)

        t = datetime.now(timezone.utc)
        e1 = make_event("e1", {"trip_distance": 5.0, "fare_amount": 20.0}, event_time=t)
        e2 = make_event("e2", {"trip_distance": 5.0, "fare_amount": 10.0}, event_time=t)

        wcfg = pipeline.config.window_config()
        pane_id, pane = setup_pane(pipeline, [(e1, (5.0, 20.0, 0.0)), (e2, (5.0, 10.0, 0.0))], wcfg)
        assert pane is not None

        candidates = pipeline._traverse_pane(pane, e2)
        assert len(candidates) >= 1
        assert candidates[0].dc_id == "DC1"
        assert "e1" in candidates[0].matched_ids


# ─── DC3: Toll Route Anomaly ─────────────────────────────────────────────────

class TestDC3TollRouteAnomaly:

    def test_dc3_fires_on_high_toll(self, dc_rules):
        """Same route, higher toll -> DC3 fires (not DC1)."""
        config = PipelineConfig(k_max=4)
        pipeline = WavePipeline(config)
        pipeline.load_dc_rules(dc_rules)

        t = datetime.now(timezone.utc)
        e1 = make_event("e1", {"PULocationID": 1.0, "DOLocationID": 2.0, "tolls_amount": 2.0}, event_time=t)
        e2 = make_event("e2", {"PULocationID": 1.0, "DOLocationID": 2.0, "tolls_amount": 5.0}, event_time=t)

        wcfg = pipeline.config.window_config()
        pane_id, pane = setup_pane(pipeline, [(e1, (1.0, 2.0, 2.0)), (e2, (1.0, 2.0, 5.0))], wcfg)
        assert pane is not None

        candidates = pipeline._traverse_pane(pane, e2)
        assert len(candidates) >= 1
        assert any(c.dc_id == "DC3" for c in candidates)


# ─── Shared Indexing / Batched Traversal ─────────────────────────────────────

class TestSharedIndexing:

    def test_batched_traversal_dc1_dc3(self, dc_rules):
        """DC1 and DC3 both fire on the same event pair."""
        config = PipelineConfig(k_max=4)
        pipeline = WavePipeline(config)
        pipeline.load_dc_rules(dc_rules)

        t = datetime.now(timezone.utc)
        e1 = make_event("e1", {
            "trip_distance": 5.0, "fare_amount": 20.0,
            "PULocationID": 1.0, "DOLocationID": 2.0, "tolls_amount": 2.0,
        }, event_time=t)
        e2 = make_event("e2", {
            "trip_distance": 5.0, "fare_amount": 10.0,
            "PULocationID": 1.0, "DOLocationID": 2.0, "tolls_amount": 5.0,
        }, event_time=t)

        wcfg = pipeline.config.window_config()
        pane_id, pane = setup_pane(
            pipeline,
            [(e1, (5.0, 20.0, 1.0, 2.0, 2.0)), (e2, (5.0, 10.0, 1.0, 2.0, 5.0))],
            wcfg,
        )
        assert pane is not None

        candidates = pipeline._traverse_pane(pane, e2)
        dc_ids = {c.dc_id for c in candidates}
        assert "DC1" in dc_ids
        assert "DC3" in dc_ids


# ─── Alert State Lifecycle ────────────────────────────────────────────────────

class TestAlertStateLifecycle:

    def test_provisional_alert_created_and_stored(self, dc_rules):
        """Provisional alert created, stored in AlertStateStore."""
        config = PipelineConfig(k_max=4)
        pipeline = WavePipeline(config)
        pipeline.load_dc_rules(dc_rules)

        t = datetime.now(timezone.utc)
        e1 = make_event("e1", {"trip_distance": 5.0, "fare_amount": 20.0}, event_time=t)
        e2 = make_event("e2", {"trip_distance": 5.0, "fare_amount": 10.0}, event_time=t)

        wcfg = pipeline.config.window_config()
        pane_id, pane = setup_pane(pipeline, [(e1, (5.0, 20.0, 0.0)), (e2, (5.0, 10.0, 0.0))], wcfg)
        assert pane is not None

        candidates = pipeline._traverse_pane(pane, e2)
        assert len(candidates) >= 1

        from waves.decision.decision import process_candidate
        decisions = process_candidate(candidates[0], pipeline._alert_store, pipeline._tombstone_mgr)
        assert len(decisions) == 1
        assert decisions[0].decision_type == "provisional"

        alert = decisions[0].alert
        assert alert.status == AlertStatus.PROVISIONAL
        stored = pipeline._alert_store.get(alert.alert_id)
        assert stored is not None
        assert stored.status == AlertStatus.PROVISIONAL

    def test_finalize_by_window_id(self):
        """Finalize alerts for a specific window_id."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord
        # CandidateViolation creates alert with window_id=""
        alert = AlertRecord(
            alert_id="alert_direct",
            dc_id="DC1",
            window_id="w_direct",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            all_event_ids=["e1"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        decisions = pipeline.finalize_window("w_direct")
        assert len(decisions) == 1
        assert decisions[0].decision_type == "final"
        assert decisions[0].alert.status == AlertStatus.FINAL

    def test_alert_indexed_by_event_id(self):
        """Alert retrieved by event_id via get_by_event_id."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        from waves.decision.alert_store import AlertRecord
        alert = AlertRecord(
            alert_id="alert_ev",
            dc_id="DC1",
            window_id="w_ev",
            pane_id="p1",
            status=AlertStatus.PROVISIONAL,
            all_event_ids=["e1", "e2"],
            created_at=datetime.now(timezone.utc),
        )
        pipeline._alert_store.put(alert)

        by_event = pipeline._alert_store.get_by_event_id("e1")
        assert len(by_event) >= 1
        assert by_event[0].alert_id == "alert_ev"


# ─── Event Store + Pane Forest ───────────────────────────────────────────────

class TestEventStoreAndPaneForest:

    def test_event_store_pane_id_lookup(self):
        """event_store.get_pane_id() used by Rapidash."""
        store = EventStore()
        e1 = make_event("e1", {"x": 1.0})
        e1.pane_id = "p_manual"
        e1.window_id = "w_manual"
        store.put(e1)

        assert store.get_pane_id("e1") == "p_manual"
        assert store.get_window_id("e1") == "w_manual"

    def test_pane_forest_insert_returns_pane(self):
        """pane_insert returns pane, pane_close builds kdtree."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)

        t = datetime.now(timezone.utc)
        wcfg = pipeline.config.window_config()

        e1 = make_event("e1", {"x": 1.0, "y": 2.0}, event_time=t)
        e2 = make_event("e2", {"x": 3.0, "y": 4.0}, event_time=t)

        pane1 = pipeline._pane_forest.pane_insert(t, "e1", (1.0, 2.0), "w1", wcfg)
        pane2 = pipeline._pane_forest.pane_insert(t, "e2", (3.0, 4.0), "w1", wcfg)

        assert pane1.pane_id == pane2.pane_id
        assert len(pane1.buffer) == 2
        assert pane1.is_active is True

        pipeline._pane_forest.pane_close(pane1.pane_id, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 100.0))
        assert pane1.is_active is False
        assert pane1.kdtree is not None


# ─── Tombstone Prevents Duplicate Matches ─────────────────────────────────

class TestTombstonePreventsDuplicates:

    def test_tombstoned_event_excluded_from_traversal(self):
        """Tombstoned event is pruned during kdtree traversal."""
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


# ─── Elastic Box / EMA ─────────────────────────────────────────────────────

class TestElasticBoxEMA:

    def test_ema_adapts_mean_variance(self):
        """EMA mean increases as higher duration values are observed."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)
        engine = pipeline._logical_engine

        dc = DenialConstraint(
            dc_id="DC2",
            rule_group="duration_context",
            dim_map={"trip_duration": 0, "PULocationID": 1},
            static_bounds={"trip_duration": (60.0, 10800.0), "PULocationID": (1.0, 265.0)},
            feature_columns=["trip_duration"],
        )

        t = datetime.now(timezone.utc)
        means = []
        for i, dur in enumerate([100.0, 200.0, 300.0, 400.0, 500.0]):
            engine.process_event({"trip_duration": dur, "PULocationID": 1.0}, [dc])
            state = engine.get_state("duration_context", "trip_duration")
            if state is not None:
                means.append(state.mean)

        assert len(means) >= 1
        assert means[-1] > means[0]

    def test_process_event_returns_elastic_boxes(self):
        """process_event updates EMA and returns ElasticBoxes."""
        config = PipelineConfig()
        pipeline = WavePipeline(config)
        engine = pipeline._logical_engine

        dc = DenialConstraint(
            dc_id="DC2",
            rule_group="dur",
            dim_map={"trip_duration": 0},
            static_bounds={"trip_duration": (60.0, 10800.0)},
            feature_columns=["trip_duration"],
        )

        boxes = engine.process_event({"trip_duration": 300.0}, [dc])
        assert len(boxes) == 1
        assert boxes[0].dc_id == "DC2"


# ─── Meta Stream Output ──────────────────────────────────────────────────────

class TestMetaStreamOutput:

    def test_window_meta_built_from_dq_checker(self):
        """WindowMeta accumulates from DQ checks."""
        from waves.basic_dq import BasicDQRule

        config = PipelineConfig()
        pipeline = WavePipeline(config)
        pipeline._dq_checker.rules = {
            "null_x": BasicDQRule(rule_id="null_x", rule_type="null", field="x")
        }
        pipeline._dq_checker._counters["null_x"] = 0
        pipeline._dq_checker._total["null_x"] = 0

        t = datetime.now(timezone.utc)
        for i in range(5):
            e = make_event(f"dq{i}", {"x": None}, event_time=t)
            results = pipeline._dq_checker.check_event(e)
            assert results is not None

        meta = pipeline.build_window_meta("w_test")
        assert meta.total_count > 0

    def test_emit_meta_to_output(self):
        """emit_meta sends WindowMeta to meta_sink."""
        received = []

        def meta_sink(e):
            received.append(e)

        config = PipelineConfig(meta_sink=meta_sink)
        pipeline = WavePipeline(config)

        meta = pipeline.emit_meta("w_meta_test")
        assert len(received) == 1
        assert received[0].window_id == "w_meta_test"
        assert meta.window_id == "w_meta_test"
