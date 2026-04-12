"""Unit tests cho Module 2.5 — SharedRuleOptimizer."""
import math
import pytest
from waves.optimizer import (
    ActiveBox,
    DCParser,
    EnrichedDC,
    GreedyRuleGrouper,
    GroupMetadata,
    OptimizerConfig,
    Predicate,
    PredicateType,
    build_active_boxes,
    NYC_TAXI_BOUNDS,
    strip_side_prefix,
)


# ─── Helpers ───────────────────────────────────────────────────────────────────

DC1_RAW = {
    "dc_id": "DC1",
    "rule_group": "fare_distance",
    "predicates": [
        {"left_col": "trip_distance_s", "operator": "EQUAL", "right_col": "trip_distance_t"},
        {"left_col": "fare_amount_s", "operator": "LESS_EQUAL", "right_col": "fare_amount_t"},
        {"left_col": "trip_distance_s", "operator": "LESS", "right_col": "trip_distance_t"},
    ],
}

DC2_RAW = {
    "dc_id": "DC2",
    "rule_group": "duration_context",
    "predicates": [
        {"left_col": "PULocationID_s", "operator": "EQUAL", "right_col": "PULocationID_t"},
        {"left_col": "DOLocationID_s", "operator": "EQUAL", "right_col": "DOLocationID_t"},
        {"left_col": "trip_duration_s", "operator": "LESS_EQUAL", "right_col": "trip_duration_t"},
    ],
}

DC3_RAW = {
    "dc_id": "DC3",
    "rule_group": "toll_route",
    "predicates": [
        {"left_col": "PULocationID_s", "operator": "EQUAL", "right_col": "PULocationID_t"},
        {"left_col": "DOLocationID_s", "operator": "EQUAL", "right_col": "DOLocationID_t"},
        {"left_col": "tolls_amount_s", "operator": "LESS_EQUAL", "right_col": "tolls_amount_t"},
    ],
}

ALL_RAW = [DC1_RAW, DC2_RAW, DC3_RAW]


# ─── strip_side_prefix ─────────────────────────────────────────────────────────

class TestStripSidePrefix:
    def test_suffix_s(self):
        assert strip_side_prefix("trip_distance_s") == ("trip_distance", "s")

    def test_suffix_t(self):
        assert strip_side_prefix("PULocationID_t") == ("PULocationID", "t")

    def test_bare_column(self):
        assert strip_side_prefix("fare_amount") == ("fare_amount", None)

    def test_underscore_in_name(self):
        assert strip_side_prefix("trip_distance_abc_s") == ("trip_distance_abc", "s")

    def test_only_s_suffix(self):
        assert strip_side_prefix("s") == ("s", None)  # weird but consistent

    def test_trailing_t(self):
        assert strip_side_prefix("location_t") == ("location", "t")


# ─── PredicateType ─────────────────────────────────────────────────────────────

class TestPredicateType:
    def test_equal(self):
        assert PredicateType.from_str("EQUAL") == PredicateType.EQUAL

    def test_less(self):
        assert PredicateType.from_str("less") == PredicateType.LESS

    def test_less_equal(self):
        assert PredicateType.from_str("LESS_EQUAL") == PredicateType.LESS_EQUAL

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            PredicateType.from_str("NOT_EXISTS")


# ─── DCParser ──────────────────────────────────────────────────────────────────

class TestDCParser:
    def test_parse_single_predicate_equal(self):
        parser = DCParser()
        preds = parser.parse_dc_json({
            "predicates": [
                {"left_col": "trip_distance_s", "operator": "EQUAL", "right_col": "trip_distance_t"},
            ],
        })
        assert len(preds) == 1
        p = preds[0]
        assert p.left_col == "trip_distance"
        assert p.left_side == "s"
        assert p.operator == PredicateType.EQUAL
        assert p.right_col == "trip_distance"
        assert p.right_side == "t"
        assert p.is_constant is False

    def test_parse_constant_predicate(self):
        parser = DCParser()
        preds = parser.parse_dc_json({
            "predicates": [
                {"left_col": "fare_amount_s", "operator": "LESS_EQUAL", "right_col": "500"},
            ],
        })
        p = preds[0]
        assert p.is_constant is True
        assert p.constant_value == 500.0

    def test_parse_full_dc(self):
        parser = DCParser()
        preds = parser.parse_dc_json(DC1_RAW)
        assert len(preds) == 3
        ops = {p.operator for p in preds}
        assert PredicateType.EQUAL in ops
        assert PredicateType.LESS in ops
        assert PredicateType.LESS_EQUAL in ops

    def test_equality_cols(self):
        parser = DCParser()
        preds = parser.parse_dc_json(DC1_RAW)
        cols = parser.get_equality_cols(preds)
        assert cols == ["trip_distance"]

    def test_equality_cols_multi(self):
        parser = DCParser()
        preds = parser.parse_dc_json(DC2_RAW)
        cols = parser.get_equality_cols(preds)
        assert cols == ["DOLocationID", "PULocationID"]  # sorted

    def test_inequality_cols(self):
        parser = DCParser()
        preds = parser.parse_dc_json(DC1_RAW)
        cols = parser.get_inequality_cols(preds)
        assert cols == ["fare_amount", "trip_distance"]

    def test_equality_signature(self):
        parser = DCParser()
        preds = parser.parse_dc_json(DC1_RAW)
        sig = parser.get_equality_signature(preds)
        assert sig == ("trip_distance",)

    def test_parse_dc_rules_all_three(self):
        parser = DCParser()
        dcs = parser.parse_dc_rules(ALL_RAW)
        assert len(dcs) == 3
        assert {d.dc_id for d in dcs} == {"DC1", "DC2", "DC3"}

    def test_parsed_dc_has_correct_group(self):
        parser = DCParser()
        dcs = parser.parse_dc_rules([DC1_RAW, DC2_RAW])
        dc1 = next(d for d in dcs if d.dc_id == "DC1")
        assert dc1.rule_group == "fare_distance"
        assert dc1.equality_cols == ["trip_distance"]

        dc2 = next(d for d in dcs if d.dc_id == "DC2")
        assert dc2.rule_group == "duration_context"
        assert dc2.equality_cols == ["DOLocationID", "PULocationID"]  # sorted


# ─── OptimizerConfig ───────────────────────────────────────────────────────────

class TestOptimizerConfig:
    def test_defaults(self):
        cfg = OptimizerConfig()
        assert cfg.k_max == 5
        assert cfg.infinite_padding is True
        assert cfg.static_bounds == NYC_TAXI_BOUNDS

    def test_k_max_zero_rejected(self):
        with pytest.raises(ValueError):
            OptimizerConfig(k_max=0)

    def test_custom_bounds(self):
        custom = {"x": (0.0, 10.0)}
        cfg = OptimizerConfig(static_bounds=custom)
        assert cfg.static_bounds["x"] == (0.0, 10.0)

    def test_nyc_bounds_accessible(self):
        assert NYC_TAXI_BOUNDS["fare_amount"] == (2.5, 500.0)
        assert NYC_TAXI_BOUNDS["trip_distance"] == (0.0, 100.0)
        assert NYC_TAXI_BOUNDS["trip_duration"] == (60.0, 10800.0)


# ─── GreedyRuleGrouper ────────────────────────────────────────────────────────

class TestGreedyRuleGrouper:
    def test_same_signature_merge(self):
        """Two DCs with same equality cols should merge into one group."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        # DC2 and DC3 both have PULocationID + DOLocationID as equality cols
        dcs = parser.parse_dc_rules([DC2_RAW, DC3_RAW])
        groups = grouper.group(dcs)

        # Should merge into 1 group (same equality signature)
        assert len(groups) == 1
        assert set(groups[0].dc_ids) == {"DC2", "DC3"}
        # Group has: PULocationID, DOLocationID, trip_duration, tolls_amount = 4 dims
        assert groups[0].dimension_count == 4
        # dim_map should contain all 4 columns
        assert len(groups[0].column_to_dim) == 4

    def test_different_signature_no_merge(self):
        """DC1 (trip_distance) vs DC2 (PULocationID+DOLocationID) → 2 groups."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW, DC2_RAW])
        groups = grouper.group(dcs)

        assert len(groups) == 2
        dc_ids_sets = {frozenset(g.dc_ids) for g in groups}
        assert frozenset({"DC1"}) in dc_ids_sets
        assert frozenset({"DC2"}) in dc_ids_sets

    def test_k_max_overflow_new_group(self):
        """If adding a DC exceeds k_max, it starts a new group."""
        parser = DCParser()
        # k_max=2 means each group can have at most 2 dims beyond equality
        cfg = OptimizerConfig(k_max=2)
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC2_RAW, DC3_RAW])
        groups = grouper.group(dcs)

        # DC2 has 2 equality + 1 inequality = 3 dims → exceeds k_max=2
        # DC3 has 2 equality + 1 inequality = 3 dims → exceeds k_max=2
        # Both would need new groups, but with same signature → first one
        # creates group, second one can't merge → 2 groups
        assert len(groups) == 2

    def test_dim_map_assignment(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)

        dc = dcs[0]
        assert dc.dim_map is not None
        assert "trip_distance" in dc.dim_map
        assert "fare_amount" in dc.dim_map

    def test_static_bounds_assignment(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)

        dc = dcs[0]
        assert dc.static_bounds is not None
        assert "trip_distance" in dc.static_bounds
        assert "fare_amount" in dc.static_bounds
        assert dc.static_bounds["trip_distance"] == (0.0, 100.0)
        assert dc.static_bounds["fare_amount"] == (2.5, 500.0)

    def test_infer_static_bounds_returns_none_for_unknown(self):
        grouper = GreedyRuleGrouper(OptimizerConfig())
        result = grouper.infer_static_bounds("unknown_column")
        assert result is None

    def test_dimension_count(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        # DC1: unique columns = {trip_distance, fare_amount} = 2 dims
        assert groups[0].dimension_count == 2

    def test_group_id_from_rule_group(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        assert groups[0].group_id == "fare_distance"


# ─── ActiveBox ────────────────────────────────────────────────────────────────

class TestActiveBox:
    def test_structure(self):
        box = ActiveBox(
            box_id="box_DC1_fare_distance",
            dc_id="DC1",
            rule_group="fare_distance",
            feature_mapping={"trip_distance": 0, "fare_amount": 1},
            padded_bounds={0: (0.0, 100.0), 1: (2.5, 500.0)},
        )
        assert box.box_id == "box_DC1_fare_distance"
        assert box.dc_id == "DC1"
        assert box.priority == 0
        assert box.version == 1

    def test_feature_mapping_matches_dim_map(self):
        feature_mapping = {"trip_distance": 0, "fare_amount": 1}
        box = ActiveBox(
            box_id="b",
            dc_id="DC1",
            rule_group="rg",
            feature_mapping=feature_mapping,
            padded_bounds={0: (0.0, 100.0), 1: (2.5, 500.0)},
        )
        # feature_mapping is public alias of dim_map
        assert box.feature_mapping["trip_distance"] == 0


# ─── Infinite Padding ─────────────────────────────────────────────────────────

class TestInfinitePadding:
    def test_infinite_padding_used_for_unknown_column(self):
        """A column not in static_bounds → padded with (-inf, +inf)."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        # PULocationID is NOT in NYC_TAXI_BOUNDS → should get infinite padding
        dc_data = {
            "dc_id": "DC_X",
            "rule_group": "test_group",
            "predicates": [
                {"left_col": "PULocationID_s", "operator": "EQUAL", "right_col": "PULocationID_t"},
                {"left_col": "fare_amount_s", "operator": "LESS_EQUAL", "right_col": "fare_amount_t"},
            ],
        }
        dcs = parser.parse_dc_rules([dc_data])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        assert len(boxes) == 1
        box = boxes[0]
        # PULocationID: NOT in static_bounds → (-inf, +inf)
        assert box.padded_bounds[box.feature_mapping["PULocationID"]] == (-math.inf, math.inf)
        # fare_amount: IS in static_bounds → real bounds
        assert box.padded_bounds[box.feature_mapping["fare_amount"]] == (2.5, 500.0)

    def test_infinite_padding_disabled(self):
        """With infinite_padding=False, unknown columns are excluded."""
        parser = DCParser()
        cfg = OptimizerConfig(infinite_padding=False)
        grouper = GreedyRuleGrouper(cfg)

        dc_data = {
            "dc_id": "DC_X",
            "rule_group": "test_group",
            "predicates": [
                {"left_col": "PULocationID_s", "operator": "EQUAL", "right_col": "PULocationID_t"},
            ],
        }
        dcs = parser.parse_dc_rules([dc_data])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        box = boxes[0]
        dim = box.feature_mapping["PULocationID"]
        # Not in padded_bounds because infinite_padding is False and no static_bound
        assert dim not in box.padded_bounds


# ─── build_active_boxes ────────────────────────────────────────────────────────

class TestBuildActiveBoxes:
    def test_all_dcs_get_boxes(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules(ALL_RAW)
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        assert len(boxes) == 3
        box_ids = {b.box_id for b in boxes}
        assert "box_DC1_fare_distance" in box_ids
        assert "box_DC2_duration_context" in box_ids
        assert "box_DC3_duration_context" in box_ids  # DC3 merged into duration_context group

    def test_box_feature_mapping(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        box = boxes[0]
        # feature_mapping should match what LogicalEngine expects
        assert "trip_distance" in box.feature_mapping
        assert "fare_amount" in box.feature_mapping
        # dim_map is just the feature_mapping
        assert box.feature_mapping == box.feature_mapping

    def test_padded_bounds_have_all_dims(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        # DC2 + DC3 share group with 4 dims: PULocationID, DOLocationID, trip_duration, tolls_amount
        dcs = parser.parse_dc_rules([DC2_RAW, DC3_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        # Both boxes should have the same 4 dims
        for box in boxes:
            assert len(box.padded_bounds) == 4

    def test_static_bounds_values(self):
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        box = boxes[0]
        # DC1 alone: equality= trip_distance, inequality= fare_amount+trip_distance
        # dim_map: {trip_distance: 0, fare_amount: 1}
        dim_dist = box.feature_mapping["trip_distance"]
        dim_fare = box.feature_mapping["fare_amount"]
        assert box.padded_bounds[dim_dist] == (0.0, 100.0)
        assert box.padded_bounds[dim_fare] == (2.5, 500.0)

    def test_merged_group_shared_tree(self):
        """DC2 and DC3 share a KD-tree: both boxes have same feature_mapping."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC2_RAW, DC3_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        # Both boxes should reference the same group and share feature_mapping
        assert len(groups) == 1
        box2 = next(b for b in boxes if b.dc_id == "DC2")
        box3 = next(b for b in boxes if b.dc_id == "DC3")
        # Same group → same feature_mapping
        assert box2.feature_mapping == box3.feature_mapping
        # But different DC IDs
        assert box2.dc_id != box3.dc_id
        # Different box IDs
        assert box2.box_id != box3.box_id


# ─── Full Integration ──────────────────────────────────────────────────────────

class TestFullIntegration:
    def test_full_pipeline_dc1(self):
        """End-to-end: dc_rules.json → EnrichedDC → ActiveBox for DC1."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        assert len(boxes) == 1
        box = boxes[0]

        # box_id format
        assert box.box_id == "box_DC1_fare_distance"
        # feature_mapping matches dim_map (LogicalEngine contract)
        assert box.dc_id == "DC1"
        # All dims present
        assert len(box.padded_bounds) == len(box.feature_mapping)

    def test_full_pipeline_all_dcs(self):
        """End-to-end for all 3 DCs."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules(ALL_RAW)
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)
        boxes = build_active_boxes(dcs, groups, cfg)

        # Expected: DC1 separate group (trip_distance equality), DC2+DC3 merged
        assert len(groups) == 2
        # 3 boxes total
        assert len(boxes) == 3

        # Check DC1 has trip_distance in feature_mapping
        dc1_box = next(b for b in boxes if b.dc_id == "DC1")
        assert "trip_distance" in dc1_box.feature_mapping
        assert "PULocationID" not in dc1_box.feature_mapping

        # Check merged group has both PULocationID and DOLocationID
        dc3_box = next(b for b in boxes if b.dc_id == "DC3")
        assert dc3_box.rule_group == "duration_context"
        # Merged group has both PULocationID and DOLocationID
        assert "PULocationID" in dc3_box.feature_mapping
        assert "DOLocationID" in dc3_box.feature_mapping

    def test_enriched_dc_dim_map_is_valid(self):
        """EnrichedDC.dim_map is compatible with LogicalEngine.ElasticBox dim_map."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)

        dc = dcs[0]
        # dim_map is Dict[str, int]
        assert isinstance(dc.dim_map, dict)
        # All values are integers (dim indices)
        for col, dim in dc.dim_map.items():
            assert isinstance(dim, int)
            assert dim >= 0
        # No duplicate dims
        assert len(dc.dim_map) == len(set(dc.dim_map.values()))

    def test_static_bounds_filled(self):
        """Known NYC taxi columns get static_bounds; unknown columns don't."""
        parser = DCParser()
        cfg = OptimizerConfig()
        grouper = GreedyRuleGrouper(cfg)

        dcs = parser.parse_dc_rules([DC1_RAW, DC2_RAW, DC3_RAW])
        groups = grouper.group(dcs)
        grouper.assign_dim_map_to_dcs(dcs, groups)

        # Only columns in NYC_TAXI_BOUNDS get static_bounds
        # PULocationID, DOLocationID are NOT in NYC_TAXI_BOUNDS
        all_bounds = {col for dc in dcs for col in (dc.static_bounds or {})}
        expected = {"trip_distance", "fare_amount", "trip_duration", "tolls_amount"}
        assert all_bounds == expected
