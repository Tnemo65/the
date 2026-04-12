"""Unit tests cho Module 2.4 — LogicalEngine."""
import pytest
from waves.logical_engine import (
    LogicalEngine,
    StatisticalState,
    ElasticBox,
    ElasticBoxConfig,
    DenialConstraint,
)


# ─── Helpers ───────────────────────────────────────────────────────────────────

def make_engine(**overrides) -> LogicalEngine:
    defaults = dict(
        alpha=0.05,
        k=3.0,
        delta_min=1.0,
        delta_max=50.0,
        warmup=50,
        stale_timeout_sec=300.0,
        partition_by=None,
    )
    defaults.update(overrides)
    cfg = ElasticBoxConfig(**defaults)
    return LogicalEngine(cfg)


def make_dc(
    dc_id="DC1",
    rule_group="fare_distance",
    dim_map=None,
    static_bounds=None,
    feature_columns=None,
) -> DenialConstraint:
    return DenialConstraint(
        dc_id=dc_id,
        rule_group=rule_group,
        dim_map=dim_map or {"trip_distance": 0, "fare_amount": 1},
        static_bounds=static_bounds or {"trip_distance": (0.0, 100.0), "fare_amount": (0.0, 500.0)},
        feature_columns=feature_columns or list(dim_map or {"trip_distance": 0, "fare_amount": 1}.keys()),
    )


# ─── ElasticBoxConfig ──────────────────────────────────────────────────────────

class TestElasticBoxConfig:
    def test_defaults(self):
        cfg = ElasticBoxConfig()
        assert cfg.alpha == 0.05
        assert cfg.k == 3.0
        assert cfg.delta_min == 1.0
        assert cfg.delta_max == 50.0
        assert cfg.warmup == 50

    def test_alpha_zero_rejected(self):
        with pytest.raises(ValueError):
            ElasticBoxConfig(alpha=0.0)

    def test_alpha_one_accepted(self):
        cfg = ElasticBoxConfig(alpha=1.0)
        assert cfg.alpha == 1.0

    def test_k_negative_rejected(self):
        with pytest.raises(ValueError):
            ElasticBoxConfig(k=-1.0)

    def test_delta_min_zero_rejected(self):
        with pytest.raises(ValueError):
            ElasticBoxConfig(delta_min=0.0)

    def test_warmup_negative_rejected(self):
        with pytest.raises(ValueError):
            ElasticBoxConfig(warmup=0)

    def test_partition_by(self):
        cfg = ElasticBoxConfig(partition_by="PULocationID")
        assert cfg.partition_by == "PULocationID"


# ─── StatisticalState ─────────────────────────────────────────────────────────

class TestStatisticalState:
    def test_std_zero_variance(self):
        state = StatisticalState(mean=10.0, variance=0.0, sample_count=10)
        assert state.std > 0  # floor at 1e-12

    def test_std_nonzero_variance(self):
        state = StatisticalState(mean=10.0, variance=4.0, sample_count=10)
        assert abs(state.std - 2.0) < 1e-9


# ─── DenialConstraint ─────────────────────────────────────────────────────────

class TestDenialConstraint:
    def test_from_dict(self):
        data = {
            "dc_id": "DC1",
            "rule_group": "fare_distance",
            "dim_map": {"trip_distance": 0, "fare_amount": 1},
            "static_bounds": {"trip_distance": (0.0, 100.0), "fare_amount": (0.0, 500.0)},
        }
        dc = DenialConstraint.from_dict(data)
        assert dc.dc_id == "DC1"
        assert dc.rule_group == "fare_distance"
        assert dc.dim_map["trip_distance"] == 0

    def test_from_dict_minimal(self):
        data = {"dc_id": "DCx", "rule_group": "rg1"}
        dc = DenialConstraint.from_dict(data)
        assert dc.feature_columns == []  # fallback empty when no dim_map


# ─── EMA Correctness ─────────────────────────────────────────────────────────

class TestEMACorrectness:
    def test_mean_converges(self):
        """After many identical values, EMA mean should converge to that value."""
        engine = make_engine(alpha=0.05, warmup=5)
        for _ in range(200):
            engine.update_statistical_state("rg", "f", 100.0)
        state = engine.get_state("rg", "f")
        assert abs(state.mean - 100.0) < 0.01

    def test_variance_tracks_series_volatility(self):
        """Variance should be high for volatile series, low for stable series."""
        # High variance: random-ish values
        engine_hi = make_engine(alpha=0.05)
        for i in range(100):
            engine_hi.update_statistical_state("rg", "f", float(i % 20))
        state_hi = engine_hi.get_state("rg", "f")
        # Low variance: near-constant values
        engine_lo = make_engine(alpha=0.05)
        for _ in range(100):
            engine_lo.update_statistical_state("rg", "f", 50.0)
        state_lo = engine_lo.get_state("rg", "f")
        # The near-constant series should have much lower variance
        assert state_lo.variance < state_hi.variance

    def test_sample_count_increments(self):
        engine = make_engine()
        for i in range(10):
            engine.update_statistical_state("rg", "f", float(i))
        state = engine.get_state("rg", "f")
        assert state.sample_count == 10

    def test_alpha_effect_fast_convergence(self):
        """Larger alpha → faster convergence to latest value."""
        fast = make_engine(alpha=0.5)
        slow = make_engine(alpha=0.01)
        for i in range(5):
            fast.update_statistical_state("rg", "f", 100.0)
            slow.update_statistical_state("rg", "f", 100.0)
        # Both should converge, but fast more so
        assert fast.get_state("rg", "f").mean > slow.get_state("rg", "f").mean

    def test_ema_known_values(self):
        """Manual check with α=0.5: x=[1,3,5] → μ=[0.5,1.75,3.375]"""
        engine = make_engine(alpha=0.5, warmup=1)
        engine.update_statistical_state("rg", "f", 1.0)
        s1 = engine.get_state("rg", "f")
        assert abs(s1.mean - 0.5) < 1e-9  # α·1 + (1-α)·0 = 0.5

        engine.update_statistical_state("rg", "f", 3.0)
        s2 = engine.get_state("rg", "f")
        assert abs(s2.mean - 1.75) < 1e-9  # 0.5·3 + 0.5·0.5 = 1.75

        engine.update_statistical_state("rg", "f", 5.0)
        s3 = engine.get_state("rg", "f")
        assert abs(s3.mean - 3.375) < 1e-9  # 0.5·5 + 0.5·1.75 = 3.375


# ─── Edge Cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_zero_variance_delta_min_used(self):
        """When variance is ~0, delta should clamp to delta_min."""
        engine = make_engine(alpha=0.05, k=3.0, delta_min=1.0, delta_max=50.0, warmup=1)
        # Feed constant to drive variance to ~0
        for _ in range(200):
            engine.update_statistical_state("rg", "f", 10.0)
        dc = make_dc(static_bounds={"trip_distance": (0.0, 100.0)})
        box = engine.build_elastic_box(dc)
        # delta = max(k*σ, delta_min) = max(k*0, 1.0) = 1.0
        assert box.delta == pytest.approx(1.0, abs=0.1)

    def test_cold_start_fallback_to_static_bounds(self):
        """Below warmup, delta_min is used (static bound ± delta_min)."""
        engine = make_engine(alpha=0.05, k=3.0, delta_min=2.5, warmup=50)
        # Only feed a few samples (below warmup)
        for i in range(5):
            engine.update_statistical_state("rg", "f", float(i))
        state = engine.get_state("rg", "f")
        assert state.sample_count < 50  # still cold

        dc = make_dc(static_bounds={"trip_distance": (0.0, 100.0)})
        box = engine.build_elastic_box(dc)
        # Since warmup not reached, padded_bounds should be (0-2.5, 100+2.5) = (-2.5, 102.5)
        dim = dc.dim_map["trip_distance"]
        lo, hi = box.padded_bounds[dim]
        assert lo == pytest.approx(-2.5, abs=0.01)
        assert hi == pytest.approx(102.5, abs=0.01)

    def test_missing_feature_uses_static_bound(self):
        """Missing feature → padded with delta_min (no infer)."""
        engine = make_engine(alpha=0.05, warmup=100)
        # No state for "missing_col"
        dc = make_dc(
            static_bounds={"missing_col": (10.0, 20.0)},
            dim_map={"missing_col": 0},
            feature_columns=["missing_col"],
        )
        box = engine.build_elastic_box(dc)
        dim = dc.dim_map["missing_col"]
        lo, hi = box.padded_bounds[dim]
        assert lo == pytest.approx(9.0, abs=0.01)   # 10 - delta_min(1.0)
        assert hi == pytest.approx(21.0, abs=0.01)  # 20 + delta_min(1.0)

    def test_null_value_skipped(self):
        """None/non-numeric values are skipped, don't affect state."""
        engine = make_engine()
        engine.update_statistical_state("rg", "f", None)
        engine.update_statistical_state("rg", "f", "not a number")
        state = engine.get_state("rg", "f")
        assert state is None  # skipped entirely

    def test_partition_isolation(self):
        """Different partition keys → separate states."""
        engine = make_engine(partition_by="zone")
        engine.update_statistical_state("rg", "f", 10.0, partition_key="zone_A")
        engine.update_statistical_state("rg", "f", 20.0, partition_key="zone_B")
        engine.update_statistical_state("rg", "f", 30.0, partition_key="zone_A")
        s_a = engine.get_state("rg", "f", partition_key="zone_A")
        s_b = engine.get_state("rg", "f", partition_key="zone_B")
        assert s_a.sample_count == 2
        assert s_b.sample_count == 1
        # zone_A mean: α·30 + (1-α)·10 ≈ converges toward 30
        assert s_a.mean > s_b.mean

    def test_state_key_without_partition(self):
        """When partition_by=None, key is (rule_group, feature)."""
        engine = make_engine(partition_by=None)
        engine.update_statistical_state("rg1", "f", 5.0)
        engine.update_statistical_state("rg1", "f", 10.0)
        state = engine.get_state("rg1", "f")
        assert state.sample_count == 2


# ─── ElasticBox ───────────────────────────────────────────────────────────────

class TestElasticBox:
    def test_extract_point(self):
        dc = make_dc(dim_map={"trip_distance": 0, "fare_amount": 1})
        box = ElasticBox(
            dc_id="DC1", rule_group="rg", dim_map=dc.dim_map,
            padded_bounds={0: (0.0, 100.0), 1: (0.0, 500.0)},
            delta=2.0, sigma=1.0,
        )
        point = box.extract_point({"trip_distance": 5.5, "fare_amount": 15.0})
        assert point == (5.5, 15.0)

    def test_extract_point_missing_field(self):
        box = ElasticBox(
            dc_id="DC1", rule_group="rg", dim_map={"x": 0},
            padded_bounds={0: (0.0, 100.0)},
            delta=1.0, sigma=0.0,
        )
        point = box.extract_point({})  # missing x
        assert point == (0.0,)

    def test_padded_bounds_structure(self):
        engine = make_engine(warmup=100)
        dc = make_dc(
            dim_map={"a": 0, "b": 1},
            static_bounds={"a": (0.0, 10.0), "b": (5.0, 15.0)},
        )
        box = engine.build_elastic_box(dc)
        assert 0 in box.padded_bounds
        assert 1 in box.padded_bounds

    def test_sigma_field(self):
        engine = make_engine(warmup=100)
        dc = make_dc(static_bounds={"f": (0.0, 100.0)})
        box = engine.build_elastic_box(dc)
        assert box.sigma >= 0.0

    def test_created_at_ms_set(self):
        engine = make_engine()
        dc = make_dc()
        box = engine.build_elastic_box(dc)
        assert box.created_at_ms > 0


# ─── process_event ────────────────────────────────────────────────────────────

class TestProcessEvent:
    def test_returns_list_of_boxes(self):
        engine = make_engine(warmup=100)
        dc1 = make_dc(dc_id="DC1", rule_group="rg1")
        dc2 = make_dc(dc_id="DC2", rule_group="rg2", dim_map={"x": 0})
        boxes = engine.process_event({"trip_distance": 5.0, "fare_amount": 15.0}, [dc1, dc2])
        assert isinstance(boxes, list)
        assert len(boxes) == 2

    def test_full_pipeline_updates_state(self):
        engine = make_engine(warmup=1)
        dc = make_dc(feature_columns=["trip_distance"])
        engine.process_event({"trip_distance": 7.0}, [dc])
        state = engine.get_state("fare_distance", "trip_distance")
        assert state is not None
        assert state.sample_count == 1
        # α=0.05, old_mean=0 → new_mean = 0.05*7 = 0.35
        assert abs(state.mean - 0.35) < 0.01

    def test_multiple_dcs_multiple_features(self):
        engine = make_engine(warmup=1)
        dc = make_dc(feature_columns=["trip_distance", "fare_amount"])
        engine.process_event({"trip_distance": 5.0, "fare_amount": 20.0}, [dc])
        s1 = engine.get_state("fare_distance", "trip_distance")
        s2 = engine.get_state("fare_distance", "fare_amount")
        assert s1.sample_count == 1
        assert s2.sample_count == 1

    def test_boxes_built_after_stat_update(self):
        """Boxes reflect state AFTER the same event updated it."""
        engine = make_engine(warmup=1, k=3.0, delta_min=1.0)
        dc = make_dc(feature_columns=["trip_distance"], static_bounds={"trip_distance": (0.0, 100.0)})
        # Feed 200 identical values → variance → 0 → delta clamped to delta_min
        for _ in range(200):
            engine.update_statistical_state("fare_distance", "trip_distance", 50.0)
        box = engine.build_elastic_box(dc)
        # k*σ ≈ 0 → clamp to delta_min = 1.0
        assert box.delta == pytest.approx(1.0, abs=0.1)

    def test_partition_key_passed_through(self):
        engine = make_engine(partition_by="zone", warmup=1)
        dc = make_dc(feature_columns=["trip_distance"])
        engine.process_event({"trip_distance": 5.0}, [dc], partition_key="zone_1")
        state = engine.get_state("fare_distance", "trip_distance", partition_key="zone_1")
        assert state is not None
        assert state.sample_count == 1

    def test_empty_dc_rules(self):
        engine = make_engine()
        boxes = engine.process_event({"trip_distance": 5.0}, [])
        assert boxes == []

    def test_dc_id_and_rule_group_preserved(self):
        engine = make_engine(warmup=100)
        dc = make_dc(dc_id="DC_XYZ", rule_group="my_group")
        boxes = engine.process_event({"trip_distance": 5.0, "fare_amount": 15.0}, [dc])
        assert boxes[0].dc_id == "DC_XYZ"
        assert boxes[0].rule_group == "my_group"


# ─── Reset ────────────────────────────────────────────────────────────────────

class TestReset:
    def test_reset_clears_all_state(self):
        engine = make_engine(warmup=1)
        engine.update_statistical_state("rg", "f", 10.0)
        assert engine.get_state("rg", "f") is not None
        engine.reset_state()
        assert engine.get_state("rg", "f") is None
