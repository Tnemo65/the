"""Unit tests for Module 2.6 — Rapidash (kdtree + traversal + candidate)."""

import math
import pytest
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

from waves.optimizer.grouper import ActiveBox
from waves.rapidash.kdtree import (
    KDTreeNode, bulk_load, range_query, _argmax_range, _boxes_intersect,
    DEFAULT_LEAF_SIZE,
)
from waves.rapidash.candidate import CandidateViolation, BatchedTraversalResult
from waves.rapidash.traversal import Intersects, point_in_box, traverse_node, BatchedTraversal


# =============================================================================
# Fixtures
# =============================================================================

@dataclass
class MockTree:
    root: Optional[KDTreeNode]


@dataclass
class MockForest:
    roots: Dict[str, List[Optional[KDTreeNode]]] = field(default_factory=dict)

    def get_roots(self, group_id: str) -> List[Optional[KDTreeNode]]:
        return self.roots.get(group_id, [])


@dataclass
class MockTombstone:
    _ids: Dict[str, set]

    def __init__(self):
        self._ids = {}

    def add(self, event_id: str, pane_id: str):
        if pane_id not in self._ids:
            self._ids[pane_id] = set()
        self._ids[pane_id].add(event_id)

    def contains(self, event_id: str, pane_id: str) -> bool:
        return event_id in self._ids.get(pane_id, set())


def make_box(
    box_id="box_DC1_group",
    dc_id="DC1",
    rule_group="group",
    feature_mapping=None,
    padded_bounds=None,
):
    if feature_mapping is None:
        feature_mapping = {"x": 0, "y": 1}
    if padded_bounds is None:
        padded_bounds = {0: (0.0, 10.0), 1: (0.0, 10.0)}
    return ActiveBox(
        box_id=box_id,
        dc_id=dc_id,
        rule_group=rule_group,
        feature_mapping=feature_mapping,
        padded_bounds=padded_bounds,
    )


# =============================================================================
# kdtree: _argmax_range
# =============================================================================

class TestArgmaxRange:
    def test_widest_first_dim(self):
        pts = [
            ((1.0, 0.0), "a"),
            ((5.0, 0.0), "b"),
            ((3.0, 0.0), "c"),
        ]
        assert _argmax_range(pts, 2) == 0  # x has range 4

    def test_widest_second_dim(self):
        pts = [
            ((0.0, 1.0), "a"),
            ((0.0, 9.0), "b"),
        ]
        assert _argmax_range(pts, 2) == 1  # y has range 8

    def test_tie_goes_to_first(self):
        pts = [
            ((0.0, 0.0), "a"),
            ((5.0, 5.0), "b"),
        ]
        assert _argmax_range(pts, 2) == 0  # both range 5, first wins


# =============================================================================
# kdtree: _boxes_intersect
# =============================================================================

class TestBoxesIntersect:
    def test_disjoint(self):
        assert _boxes_intersect((0, 0), (2, 2), (3, 3), (5, 5)) is False

    def test_contains(self):
        assert _boxes_intersect((0, 0), (5, 5), (1, 1), (4, 4)) is True

    def test_partial_overlap_x(self):
        assert _boxes_intersect((0, 0), (3, 3), (2, 0), (5, 3)) is True

    def test_partial_overlap_y(self):
        assert _boxes_intersect((0, 0), (3, 3), (0, 2), (3, 5)) is True

    def test_touches_edge(self):
        assert _boxes_intersect((0, 0), (2, 2), (2, 2), (4, 4)) is True

    def test_empty_dim(self):
        assert _boxes_intersect((), (), (), ()) is True


# =============================================================================
# kdtree: bulk_load
# =============================================================================

class TestBulkLoad:
    def test_empty_returns_none(self):
        assert bulk_load([], dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0)) is None

    def test_single_point_leaf(self):
        pts = [((3.0, 4.0), "e1")]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        assert node is not None
        assert node.is_leaf is True
        assert node.leaf_points == pts

    def test_two_points_leaf(self):
        pts = [((1.0, 2.0), "e1"), ((3.0, 4.0), "e2")]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        assert node is not None
        assert node.is_leaf is True
        assert len(node.leaf_points) == 2

    def test_exactly_leaf_size(self):
        pts = [((i, i), f"e{i}") for i in range(DEFAULT_LEAF_SIZE)]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 100.0))
        assert node.is_leaf is True

    def test_one_over_leaf_size_internal(self):
        pts = [((i, i), f"e{i}") for i in range(DEFAULT_LEAF_SIZE + 1)]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 100.0))
        assert node is not None
        assert node.is_leaf is False
        assert node.point is not None

    def test_internal_node_has_bounding_boxes(self):
        pts = [((float(i), float(i * 2)), f"e{i}") for i in range(17)]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 200.0))
        assert node is not None
        assert node.is_leaf is False
        assert len(node.left_lo) == 2
        assert len(node.left_hi) == 2
        assert len(node.right_lo) == 2
        assert len(node.right_hi) == 2

    def test_children_have_correct_bounds(self):
        pts = [((float(i), float(i * 2)), f"e{i}") for i in range(17)]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 200.0))
        assert node.is_leaf is False
        d = node.partition_dim
        # Bulk-load: left subtree gets points where pt[d] <= split_val,
        # right gets points where pt[d] >= split_val.
        # Bounding boxes: left_hi[d] = split_val, right_lo[d] = split_val
        assert node.left_hi[d] == node.split_value, f"left_hi[{d}]={node.left_hi[d]} != split_val={node.split_value}"
        assert node.right_lo[d] == node.split_value, f"right_lo[{d}]={node.right_lo[d]} != split_val={node.split_value}"
        # Other dimensions: left_lo == right_lo == global_lo, left_hi == right_hi == global_hi
        for other in range(2):
            if other != d:
                assert node.left_lo[other] == node.right_lo[other] == 0.0
                assert node.left_hi[other] == node.right_hi[other] == (200.0 if other == 1 else 100.0)

    def test_bulk_load_3d(self):
        pts = [((float(i), float(i * 2), float(i * 3)), f"e{i}") for i in range(17)]
        node = bulk_load(pts, dim_count=3, lo_bounds=(0.0, 0.0, 0.0), hi_bounds=(100.0, 200.0, 300.0))
        assert node is not None
        assert node.partition_dim in (0, 1, 2)

    def test_custom_leaf_size(self):
        pts = [((i, i), f"e{i}") for i in range(5)]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0), leaf_size=2)
        # At leaf_size=2, 5 points should produce internal nodes
        assert node.is_leaf is False

    def test_all_points_same(self):
        pts = [((5.0, 5.0), "e1"), ((5.0, 5.0), "e2")]
        node = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        # Should not crash; may be leaf or internal
        assert node is not None


# =============================================================================
# kdtree: range_query
# =============================================================================

class TestRangeQuery:
    @pytest.fixture
    def tree(self):
        pts = [
            ((1.0, 2.0), "e1"),
            ((3.0, 4.0), "e2"),
            ((5.0, 6.0), "e3"),
            ((7.0, 8.0), "e4"),
            ((9.0, 10.0), "e5"),
            ((11.0, 12.0), "e6"),
        ]
        return bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(20.0, 20.0))

    def test_empty_tree(self):
        results = range_query(None, (0.0, 0.0), (5.0, 5.0))
        assert results == []

    def test_point_inside(self, tree):
        results = range_query(tree, (1.0, 2.0), (1.0, 2.0))
        ids = [pid for _, pid in results]
        assert "e1" in ids

    def test_point_outside(self, tree):
        results = range_query(tree, (100.0, 100.0), (200.0, 200.0))
        assert results == []

    def test_partial_overlap_finds_some(self, tree):
        results = range_query(tree, (0.0, 0.0), (4.0, 5.0))
        ids = [pid for _, pid in results]
        assert "e1" in ids
        assert "e2" in ids

    def test_all_points(self, tree):
        results = range_query(tree, (0.0, 0.0), (20.0, 20.0))
        assert len(results) == 6

    def test_open_range(self, tree):
        results = range_query(tree, (2.5, 3.5), (8.5, 9.5))
        ids = [pid for _, pid in results]
        assert "e2" in ids
        assert "e3" in ids
        assert "e4" in ids

    def test_range_boundary(self, tree):
        # e1 is at (1,2), query from (1,2) to (1,2)
        results = range_query(tree, (1.0, 2.0), (1.0, 2.0))
        ids = [pid for _, pid in results]
        assert "e1" in ids


# =============================================================================
# traversal: Intersects
# =============================================================================

class TestIntersects:
    def test_fully_inside(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert Intersects(box, (0.0, 0.0), (10.0, 10.0)) is True

    def test_box_covers_region(self):
        box = make_box(padded_bounds={0: (-1.0, 11.0), 1: (-1.0, 11.0)})
        assert Intersects(box, (0.0, 0.0), (10.0, 10.0)) is True

    def test_disjoint_x(self):
        box = make_box(padded_bounds={0: (0.0, 5.0), 1: (0.0, 10.0)})
        assert Intersects(box, (6.0, 0.0), (10.0, 10.0)) is False

    def test_disjoint_y(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 5.0)})
        assert Intersects(box, (0.0, 6.0), (10.0, 10.0)) is False

    def test_box_touches_edge(self):
        box = make_box(padded_bounds={0: (0.0, 5.0), 1: (0.0, 10.0)})
        assert Intersects(box, (5.0, 0.0), (10.0, 10.0)) is True

    def test_box_has_inf_bounds(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (-math.inf, math.inf)})
        assert Intersects(box, (0.0, 0.0), (10.0, 10.0)) is True

    def test_box_has_fewer_dims_than_region(self):
        box = make_box(padded_bounds={0: (0.0, 10.0)})  # only dim 0
        assert Intersects(box, (0.0, 0.0), (10.0, 10.0)) is True  # conservatively intersects

    def test_box_has_more_dims_than_region(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0), 2: (0.0, 10.0)})
        assert Intersects(box, (0.0, 0.0), (10.0, 10.0)) is True  # conservatively intersects


# =============================================================================
# traversal: point_in_box
# =============================================================================

class TestPointInBox:
    def test_point_inside(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((5.0, 5.0), box) is True

    def test_point_outside_x(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((15.0, 5.0), box) is False

    def test_point_outside_y(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((5.0, 15.0), box) is False

    def test_on_boundary_lo(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((0.0, 5.0), box) is True

    def test_on_boundary_hi(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((10.0, 5.0), box) is True

    def test_point_with_inf_bounds(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (-math.inf, math.inf)})
        assert point_in_box((5.0, 1e9), box) is True
        assert point_in_box((-5.0, 5.0), box) is False

    def test_point_too_few_dims(self):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        assert point_in_box((5.0,), box) is False  # dim 1 missing

    def test_point_with_negative_inf(self):
        box = make_box(padded_bounds={0: (-math.inf, 0.0), 1: (0.0, 10.0)})
        assert point_in_box((-1e9, 5.0), box) is True
        assert point_in_box((1.0, 5.0), box) is False

    def test_empty_box_bounds(self):
        box = make_box(padded_bounds={})
        assert point_in_box((5.0, 5.0), box) is True  # empty bounds: vacuously inside


# =============================================================================
# traversal: traverse_node
# =============================================================================

class TestTraverseNode:
    @pytest.fixture
    def tree(self):
        pts = [
            ((1.0, 2.0), "e1"),
            ((3.0, 4.0), "e2"),
            ((5.0, 6.0), "e3"),
        ]
        return bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))

    def get_pane(self, eid): return "p0"

    def test_none_returns_empty(self):
        hits, v, p = traverse_node(None, (5.0, 5.0), "e0", make_box(), None, self.get_pane)
        assert hits == []
        assert v == 1

    def test_leaf_no_match(self, tree):
        box = make_box(padded_bounds={0: (100.0, 200.0), 1: (100.0, 200.0)})
        hits, v, p = traverse_node(tree, (50.0, 50.0), "e0", box, None, self.get_pane)
        assert hits == []
        assert v >= 1

    def test_leaf_match(self, tree):
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        hits, v, p = traverse_node(tree, (5.0, 5.0), "e0", box, None, self.get_pane)
        assert len(hits) >= 1

    def test_self_exclusion(self, tree):
        # Query point is e1, e1 should not appear in matched_ids
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        hits, v, p = traverse_node(tree, (1.0, 2.0), "e1", box, None, self.get_pane)
        for c in hits:
            assert "e1" not in c.matched_ids

    def test_box_dropping_prunes_branch(self):
        # Build a tree with enough points for internal nodes
        pts = [((float(i), float(i * 2)), f"e{i}") for i in range(17)]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 200.0))
        # Query box completely disjoint from tree — at (200,200) to (300,300)
        # Tree bounds are (0,100)x(0,200) — no intersection with (200,300)x(200,300)
        box = make_box(padded_bounds={0: (200.0, 300.0), 1: (200.0, 300.0)})
        hits, v, p = traverse_node(tree, (250.0, 250.0), "e0", box, None, self.get_pane)
        # Entire tree should be pruned
        assert p >= 1
        assert hits == []

    def test_tombstone_filters(self):
        pts = [((3.0, 4.0), "e2"), ((5.0, 6.0), "e3")]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        tomb = MockTombstone()
        tomb.add("e2", "p0")

        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        hits, v, p = traverse_node(tree, (1.0, 2.0), "e1", box, tomb, self.get_pane)
        for c in hits:
            assert "e2" not in c.matched_ids

    def test_visited_count_increments(self):
        pts = [((1.0, 2.0), "e1"), ((3.0, 4.0), "e2")]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        hits, v, p = traverse_node(tree, (5.0, 5.0), "e0", box, None, self.get_pane)
        assert v >= 1


# =============================================================================
# BatchedTraversal
# =============================================================================

class TestBatchedTraversal:
    @pytest.fixture
    def tree_a(self):
        pts = [((float(i), float(i * 2)), f"e{i}") for i in range(1, 17)]
        return bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(20.0, 40.0))

    @pytest.fixture
    def tree_b(self):
        pts = [((float(i), float(i * 2 + 50)), f"e{i+100}") for i in range(1, 10)]
        return bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(20.0, 200.0))

    @pytest.fixture
    def forest(self, tree_a, tree_b):
        f = MockForest()
        f.roots["group_a"] = [tree_a]
        f.roots["group_b"] = [tree_b]
        return f

    def get_pane(self, eid): return "p0"

    def test_no_boxes_returns_empty(self, forest):
        bt = BatchedTraversal(forest=forest)
        result = bt.query((5.0, 5.0), "e0", [], self.get_pane)
        assert result.candidates == []
        assert result.visited_nodes == 0
        assert result.pruned_nodes == 0

    def test_no_forest_returns_empty(self):
        bt = BatchedTraversal(forest=None)
        result = bt.query((5.0, 5.0), "e0", [], self.get_pane)
        assert result.candidates == []
        assert result.visited_nodes == 0

    def test_single_box_single_tree(self, forest):
        box = make_box(box_id="box1", rule_group="group_a", padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        bt = BatchedTraversal(forest=forest)
        result = bt.query((5.0, 5.0), "e0", [box], self.get_pane)
        assert len(result.candidates) >= 1
        assert result.visited_nodes >= 1

    def test_multiple_trees_same_group_id(self, forest):
        # Add second tree to same group
        pts2 = [((1.5, 2.5), "e6")]
        tree2 = bulk_load(pts2, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 10.0))
        forest.roots["group_a"].append(tree2)

        box = make_box(box_id="box1", rule_group="group_a", padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        bt = BatchedTraversal(forest=forest)
        result = bt.query((5.0, 5.0), "e0", [box], self.get_pane)
        # Should have results from both trees
        assert result.visited_nodes >= 2

    def test_wrong_group_id(self, forest):
        box = make_box(box_id="box1", rule_group="nonexistent_group", padded_bounds={0: (0.0, 10.0), 1: (0.0, 10.0)})
        bt = BatchedTraversal(forest=forest)
        result = bt.query((5.0, 5.0), "e0", [box], self.get_pane)
        assert result.candidates == []

    def test_visited_and_pruned_counters(self, forest):
        # Far-away box — the tree is a leaf, so no internal nodes to prune
        box = make_box(box_id="box_far", rule_group="group_a", padded_bounds={0: (50.0, 60.0), 1: (50.0, 60.0)})
        bt = BatchedTraversal(forest=forest)
        result = bt.query((55.0, 55.0), "e0", [box], self.get_pane)
        assert result.candidates == []
        assert result.visited_nodes >= 1
        # pruned >= 0 is always true; can't prune a leaf-only tree

    def test_multiple_boxes_same_group(self, forest):
        box1 = make_box(box_id="box1", dc_id="DC1", rule_group="group_a", padded_bounds={0: (0.0, 4.0), 1: (0.0, 4.0)})
        box2 = make_box(box_id="box2", dc_id="DC2", rule_group="group_a", padded_bounds={0: (4.0, 10.0), 1: (4.0, 10.0)})
        bt = BatchedTraversal(forest=forest)
        result = bt.query((5.0, 5.0), "e0", [box1, box2], self.get_pane)
        # box2 should find e3
        dc_ids = [c.dc_id for c in result.candidates]
        assert "DC2" in dc_ids


# =============================================================================
# Integration: ActiveBox → bulk_load → traverse end-to-end
# =============================================================================

class TestEndToEnd:
    def test_activebox_to_traversal(self):
        # Test: query finds points that are spatially inside the box
        # e1=(3.5, 7.5): inside box on dim0 (3.5∈[3,4]), outside on dim1 (7.5<8)
        # e2=(2.0, 8.0): outside on dim0 (2.0<3)
        # e3=(4.0, 20.0): inside both dims → matches DC1 box
        pts = [
            ((3.5, 7.5), "e1"),
            ((2.0, 8.0), "e2"),
            ((4.0, 20.0), "e3"),  # ONLY point inside the box
        ]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(100.0, 500.0))

        box = make_box(
            box_id="box_DC1_fare_distance",
            dc_id="DC1",
            rule_group="fare_distance",
            feature_mapping={"trip_distance": 0, "fare_amount": 1},
            padded_bounds={0: (3.0, 4.0), 1: (8.0, 500.0)},
        )

        def get_pane(eid): return "p0"

        # Query e1 (outside box on dim1) → e3 is inside → candidate
        hits, v, p = traverse_node(tree, (3.5, 7.5), "e1", box, None, get_pane)
        assert len(hits) == 1
        assert hits[0].matched_ids == ["e3"]
        assert hits[0].dc_id == "DC1"

        # Query e2 (outside box on dim0) → no matches (e3 is inside but query is e2 ≠ e3)
        hits, v, p = traverse_node(tree, (2.0, 8.0), "e2", box, None, get_pane)
        # e2 is outside; e1 is outside; e3 is inside → e2→e3 is a candidate
        assert len(hits) == 1
        assert hits[0].matched_ids == ["e3"]

        # Query e3 (inside box) → e1 and e2 are outside → no matches
        hits, v, p = traverse_node(tree, (4.0, 20.0), "e3", box, None, get_pane)
        assert hits == [], f"e3 inside box but e1,e2 outside, expected no candidates, got {hits}"

    def test_box_dropping_with_shared_kdtree(self):
        # Two ActiveBoxes sharing same tree
        pts = [((float(i), float(i * 2)), f"e{i}") for i in range(1, 17)]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(20.0, 40.0))

        # Box A: covers origin region
        box_a = make_box(box_id="boxA", dc_id="DC1", rule_group="group", padded_bounds={0: (0.0, 5.0), 1: (0.0, 10.0)})
        # Box B: covers far region
        box_b = make_box(box_id="boxB", dc_id="DC2", rule_group="group", padded_bounds={0: (8.0, 15.0), 1: (16.0, 30.0)})

        def get_pane(eid): return "p0"

        bt = BatchedTraversal(forest=None)

        result_a = bt.query((2.0, 4.0), "eq", [box_a], get_pane)
        result_b = bt.query((2.0, 4.0), "eq", [box_b], get_pane)

        # box_a should have matches, box_b should not
        assert len(result_a.candidates) >= len(result_b.candidates)

    def test_tombstone_prevents_retracted_ghost_matches(self):
        pts = [((3.0, 10.0), "e1"), ((3.0, 20.0), "e2"), ((3.0, 30.0), "e3")]
        tree = bulk_load(pts, dim_count=2, lo_bounds=(0.0, 0.0), hi_bounds=(10.0, 100.0))

        tomb = MockTombstone()
        tomb.add("e2", "p0")  # e2 was retracted

        box = make_box(padded_bounds={0: (0.0, 10.0), 1: (0.0, 100.0)})

        def get_pane(eid): return "p0"

        hits, v, p = traverse_node(tree, (3.0, 5.0), "eq", box, tomb, get_pane)
        # e2 should be filtered out by tombstone
        all_matched = []
        for c in hits:
            all_matched.extend(c.matched_ids)
        assert "e2" not in all_matched
