"""Module 2.6 — Rapidash: KD-Tree Node and Bulk Load."""

from dataclasses import dataclass, field
from typing import List, Tuple, Optional

DEFAULT_LEAF_SIZE = 16


@dataclass
class KDTreeNode:
    """A node in a KD-Tree with pre-computed bounding boxes for Box Dropping.

    Design:
    - Internal nodes store ONE point (the median) plus split metadata.
    - Leaf nodes store up to `leaf_size` points as a flat list.
    - Bounding boxes (left_lo/hi, right_lo/hi) are pre-computed at bulk-load
      time for O(1) Box Dropping during traversal.
    """
    point: Optional[Tuple[float, ...]] = None
    point_id: Optional[str] = None

    partition_dim: int = -1
    split_value: float = 0.0

    left_lo: Tuple[float, ...] = field(default_factory=lambda: ())
    left_hi: Tuple[float, ...] = field(default_factory=lambda: ())
    right_lo: Tuple[float, ...] = field(default_factory=lambda: ())
    right_hi: Tuple[float, ...] = field(default_factory=lambda: ())

    left: Optional['KDTreeNode'] = None
    right: Optional['KDTreeNode'] = None

    is_leaf: bool = False
    leaf_points: List[Tuple[Tuple[float, ...], str]] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return self.point is None and not self.is_leaf


def _argmax_range(
    points: List[Tuple[Tuple[float, ...], str]], dim_count: int
) -> int:
    """Find dimension with widest spread (max range)."""
    lo = [min(p[0][d] for p in points) for d in range(dim_count)]
    hi = [max(p[0][d] for p in points) for d in range(dim_count)]
    ranges = [hi[d] - lo[d] for d in range(dim_count)]
    return ranges.index(max(ranges))


def _boxes_intersect(
    lo1: Tuple[float, ...], hi1: Tuple[float, ...],
    lo2: Tuple[float, ...], hi2: Tuple[float, ...],
) -> bool:
    """Axis-aligned bounding-box intersection test. O(dim_count)."""
    return all(lo1[d] <= hi2[d] and lo2[d] <= hi1[d] for d in range(len(lo1)))


def bulk_load(
    points: List[Tuple[Tuple[float, ...], str]],
    dim_count: int,
    lo_bounds: Tuple[float, ...],
    hi_bounds: Tuple[float, ...],
    leaf_size: int = DEFAULT_LEAF_SIZE,
) -> Optional[KDTreeNode]:
    """Bulk-load a balanced KD-Tree using sort-based median split.

    Pre-computes bounding boxes at each node to enable O(1) Box Dropping
    during BatchedTraversal. Runs in O(N log N) time.

    Args:
        points: List of (point_tuple, event_id).
        dim_count: Number of dimensions per point.
        lo_bounds: Global lower bound per dimension.
        hi_bounds: Global upper bound per dimension.
        leaf_size: Max points per leaf node.
    """
    if not points:
        return None

    if len(points) <= leaf_size:
        return KDTreeNode(
            point=None, point_id=None,
            is_leaf=True,
            leaf_points=points,
            left_lo=lo_bounds, left_hi=hi_bounds,
            right_lo=lo_bounds, right_hi=hi_bounds,
        )

    split_dim = _argmax_range(points, dim_count)
    sorted_pts = sorted(points, key=lambda x: x[0][split_dim])
    mid = len(sorted_pts) // 2
    split_val = sorted_pts[mid][0][split_dim]

    left_pts = sorted_pts[:mid]
    right_pts = sorted_pts[mid:]

    left_lo = list(lo_bounds); left_hi = list(hi_bounds); left_hi[split_dim] = split_val
    right_lo = list(lo_bounds); right_lo[split_dim] = split_val; right_hi = list(hi_bounds)

    return KDTreeNode(
        point=sorted_pts[mid][0],
        point_id=sorted_pts[mid][1],
        partition_dim=split_dim,
        split_value=split_val,
        left_lo=tuple(left_lo), left_hi=tuple(left_hi),
        right_lo=tuple(right_lo), right_hi=tuple(right_hi),
        left=bulk_load(left_pts, dim_count, tuple(left_lo), tuple(left_hi), leaf_size),
        right=bulk_load(right_pts, dim_count, tuple(right_lo), tuple(right_hi), leaf_size),
    )


def range_query(
    node: Optional[KDTreeNode],
    query_lo: Tuple[float, ...],
    query_hi: Tuple[float, ...],
) -> List[Tuple[Tuple[float, ...], str]]:
    """Return all (point, event_id) inside an axis-aligned bounding box."""
    if node is None:
        return []

    results: List[Tuple[Tuple[float, ...], str]] = []

    if node.is_leaf:
        for pt, pid in node.leaf_points:
            if all(query_lo[d] <= pt[d] <= query_hi[d] for d in range(len(pt))):
                results.append((pt, pid))
        return results

    if node.point is not None:
        if all(query_lo[d] <= node.point[d] <= query_hi[d] for d in range(len(node.point))):
            results.append((node.point, node.point_id))

    if node.left is not None:
        if _boxes_intersect(query_lo, query_hi, node.left_lo, node.left_hi):
            results.extend(range_query(node.left, query_lo, query_hi))
    if node.right is not None:
        if _boxes_intersect(query_lo, query_hi, node.right_lo, node.right_hi):
            results.extend(range_query(node.right, query_lo, query_hi))

    return results
