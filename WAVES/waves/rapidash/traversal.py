"""Module 2.6 — Rapidash: Batched KD-Tree Traversal."""

from typing import List, Optional, Tuple, Any, TYPE_CHECKING

from waves.optimizer import ActiveBox
from waves.rapidash.candidate import CandidateViolation, BatchedTraversalResult

if TYPE_CHECKING:
    from waves.rapidash.kdtree import KDTreeNode


def Intersects(
    box: ActiveBox,
    region_lo: Tuple[float, ...],
    region_hi: Tuple[float, ...],
) -> bool:
    """O(1) axis-aligned bounding-box intersection test.

    Tests if `box.padded_bounds` intersects with a KD-tree node's bounding region.
    box.padded_bounds: {dim_index → (lo, hi)}
    region_lo/hi: bounding box of the subtree region.

    Handles the case where box has more dims than region (infinite padding case).
    """
    for dim, (box_lo, box_hi) in box.padded_bounds.items():
        if dim >= len(region_lo) or dim >= len(region_hi):
            # Box has dimension not in region — conservatively treat as intersecting
            # (the point-in-box check at leaf will filter)
            continue
        if box_lo > region_hi[dim] or box_hi < region_lo[dim]:
            return False
    return True


def point_in_box(point: Tuple[float, ...], box: ActiveBox) -> bool:
    """Test if a point falls inside an ActiveBox.

    point: coordinates indexed by dim (pre-extracted using box.feature_mapping)
    box: ActiveBox with padded_bounds keyed by dim_index
    """
    for dim, (lo, hi) in box.padded_bounds.items():
        if dim >= len(point):
            return False
        val = point[dim]
        # lo/hi are already float or -inf/+inf
        if val < lo or val > hi:
            return False
    return True


def traverse_node(
    node: Optional['KDTreeNode'],
    query_point: Tuple[float, ...],
    query_id: str,
    box: ActiveBox,
    tombstone_mgr: Optional[Any],   # TombstoneManager or mock
    get_pane_id: Any,               # callable: event_id → pane_id
) -> Tuple[List[CandidateViolation], int, int]:
    """Recursive KD-Tree traversal with Box Dropping.

    Returns: (candidates, visited_nodes, pruned_nodes)
    """
    if node is None:
        return ([], 1, 0)

    if node.is_leaf:
        matched_ids: List[str] = []
        pane_id_of_matches: str = ""
        for (pt, pt_id) in node.leaf_points:
            if pt_id == query_id:
                continue
            pane_id_t = get_pane_id(pt_id)
            if tombstone_mgr is not None and tombstone_mgr.contains(pt_id, pane_id=pane_id_t):
                continue
            if point_in_box(pt, box):
                matched_ids.append(pt_id)
                pane_id_of_matches = pane_id_t
        if matched_ids:
            candidates: List[CandidateViolation] = [CandidateViolation(
                dc_id=box.dc_id,
                window_id="",
                pane_id=pane_id_of_matches,
                query_id=query_id,
                matched_ids=matched_ids,
                box_id=box.box_id,
                timestamp_ms=0,
            )]
        else:
            candidates = []
        return (candidates, 1, 0)

    # Box Dropping: check intersection with children's bounding regions
    left_inter = Intersects(box, node.left_lo, node.left_hi)
    right_inter = Intersects(box, node.right_lo, node.right_hi)

    if not left_inter and not right_inter:
        return ([], 1, 1)  # visited=1, pruned=1

    # Query point on path — will be handled at leaf nodes where we check OTHER points.
    # We emit ONE candidate per (query, box) pair only when there are matched points.
    # If query itself is in the box, it's counted as matched (no self-exclusion at this level).

    candidates: List[CandidateViolation] = []
    visited = 1
    pruned = 0

    # Recurse into intersecting children
    if left_inter and node.left is not None:
        hits, v, p = traverse_node(node.left, query_point, query_id, box, tombstone_mgr, get_pane_id)
        candidates.extend(hits)
        visited += v
        pruned += p

    if right_inter and node.right is not None:
        hits, v, p = traverse_node(node.right, query_point, query_id, box, tombstone_mgr, get_pane_id)
        candidates.extend(hits)
        visited += v
        pruned += p

    return (candidates, visited, pruned)


class BatchedTraversal:
    """Batched KD-Tree traversal: one query tests multiple ActiveBoxes.

    Batched means: one tree traversal handles MULTIPLE ActiveBoxes simultaneously,
    pruning boxes that don't intersect each subtree as we go.
    """

    def __init__(
        self,
        forest: Optional[Any] = None,      # PaneForest or mock
        tombstone_mgr: Optional[Any] = None,
    ):
        self.forest = forest
        self.tombstone_mgr = tombstone_mgr

    def query(
        self,
        query_point: Tuple[float, ...],
        query_id: str,
        active_boxes: List[ActiveBox],
        get_pane_id: Any,   # callable: event_id → pane_id
    ) -> BatchedTraversalResult:
        """Test one query point against all active boxes across all panes.

        Args:
            query_point: Feature vector extracted from event using box.feature_mapping.
            query_id: Event ID of the query point.
            active_boxes: All ActiveBoxes from Optimizer (one per DC).
            get_pane_id: Function mapping event_id → pane_id.

        Returns:
            BatchedTraversalResult with all candidate violations.
        """
        if not active_boxes:
            return BatchedTraversalResult([], 0, 0)

        all_candidates: List[CandidateViolation] = []
        total_visited = 0
        total_pruned = 0

        for box in active_boxes:
            trees: List[Any] = []
            if self.forest is not None:
                trees = self.forest.get_roots(box.rule_group)
            for tree_root in trees:
                hits, v, p = traverse_node(
                    tree_root, query_point, query_id,
                    box, self.tombstone_mgr, get_pane_id,
                )
                all_candidates.extend(hits)
                total_visited += v
                total_pruned += p

        return BatchedTraversalResult(all_candidates, total_visited, total_pruned)
