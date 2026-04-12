"""Module 2.5 — Shared Rule Optimizer: greedy grouper + ActiveBox builder."""

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from waves.optimizer.config import OptimizerConfig


@dataclass
class GroupMetadata:
    """Metadata for one group of DCs that share the same KD-tree."""
    group_id: str
    equality_signature: Tuple[str, ...]          # tuple(sorted(equality_cols)) — group key
    column_to_dim: Dict[str, int]               # col → dim_index
    dc_ids: List[str]
    dimension_count: int


class GreedyRuleGrouper:
    """Greedy grouping of DCs by equality signature + dimension-cap enforcement."""

    def __init__(self, config: OptimizerConfig):
        self.config = config

    def infer_static_bounds(self, col: str) -> Optional[Tuple[float, float]]:
        """Look up static bounds from config, return None if unknown."""
        return self.config.static_bounds.get(col)

    def _find_group(self, groups: List[GroupMetadata], sig: Tuple[str, ...]) -> Optional[GroupMetadata]:
        for g in groups:
            if g.equality_signature == sig:
                return g
        return None

    def group(self, enriched_dcs: List["EnrichedDC"]) -> List[GroupMetadata]:
        """Greedy grouping: merge DCs with same equality signature if dimension count <= k_max.

        Steps per DC:
          1. Compute equality_signature = tuple(sorted(dc.equality_cols))
          2. Try to merge into existing group with same signature
          3. Check: new_dim_count = group.dimension_count + len(dc.inequality_cols) <= k_max
          4. If merge: assign new columns to next dim indices; if not: start new group
        """
        groups: List[GroupMetadata] = []
        for dc in enriched_dcs:
            sig = tuple(sorted(dc.equality_cols))
            merged = False

            # Try to merge into existing group with same equality signature
            for g in groups:
                if g.equality_signature == sig:
                    new_dim_count = g.dimension_count + len(dc.inequality_cols)
                    if new_dim_count <= self.config.k_max:
                        g.dc_ids.append(dc.dc_id)
                        for col in dc.inequality_cols:
                            if col not in g.column_to_dim:
                                g.column_to_dim[col] = len(g.column_to_dim)
                                g.dimension_count += 1
                        merged = True
                        break

            # No merge possible → start a new group
            if not merged:
                # Build dim_map from unique columns (equality first, then inequality)
                seen: set = set()
                dim_map: Dict[str, int] = {}
                for col in dc.equality_cols + dc.inequality_cols:
                    if col not in seen:
                        seen.add(col)
                        dim_map[col] = len(dim_map)
                groups.append(GroupMetadata(
                    group_id=dc.rule_group,
                    equality_signature=sig,
                    column_to_dim=dim_map,
                    dc_ids=[dc.dc_id],
                    dimension_count=len(dim_map),
                ))

        return groups

    def assign_dim_map_to_dcs(
        self, dcs: List["EnrichedDC"], groups: List[GroupMetadata]
    ) -> None:
        """Assign dim_map and static_bounds from groups back to each EnrichedDC."""
        # Build dc_id → group lookup
        dc_to_group: Dict[str, GroupMetadata] = {}
        for g in groups:
            for dc_id in g.dc_ids:
                dc_to_group[dc_id] = g

        for dc in dcs:
            g = dc_to_group.get(dc.dc_id)
            if g is not None:
                dc.dim_map = dict(g.column_to_dim)
            else:
                # Fallback: build dim_map from equality + inequality cols
                all_cols = dc.equality_cols + dc.inequality_cols
                dc.dim_map = {col: idx for idx, col in enumerate(all_cols)}

            # Infer static_bounds from config
            bounds: Dict[str, Tuple[float, float]] = {}
            for col in dc.equality_cols + dc.inequality_cols:
                b = self.infer_static_bounds(col)
                if b is not None:
                    bounds[col] = b
            dc.static_bounds = bounds


@dataclass
class ActiveBox:
    """Padded box for one DC, ready for Rapidash KD-Tree construction.

    Unlike ElasticBox (runtime, EMA-adaptive), this is a pre-materialized
    static box that Rapidash uses as the starting seed for the KD-tree.
    """
    box_id: str                                  # f"box_{dc_id}_{group_id}"
    dc_id: str
    rule_group: str
    feature_mapping: Dict[str, int]              # col → dim_index (= dim_map, public alias)
    padded_bounds: Dict[int, Tuple[float, float]]  # dim_index → (lo, hi)
    priority: int = 0
    version: int = 1


def build_active_boxes(
    enriched_dcs: List["EnrichedDC"],
    groups: List[GroupMetadata],
    config: OptimizerConfig,
) -> List[ActiveBox]:
    """Materialize ActiveBox for every EnrichedDC.

    For each DC, build padded_bounds over ALL dimensions in its group:
      - If column has a static_bound: use (s_lo, s_hi)
      - Else if infinite_padding enabled: use (-inf, +inf)

    This ensures every box covers the full group dimension space, which is
    what allows Rapidash to traverse a single shared KD-tree for all DCs
    in the same group.
    """
    # Build dc_id → group lookup
    dc_to_group: Dict[str, GroupMetadata] = {}
    for g in groups:
        for dc_id in g.dc_ids:
            dc_to_group[dc_id] = g

    boxes: List[ActiveBox] = []
    for dc in enriched_dcs:
        g = dc_to_group.get(dc.dc_id)
        if g is None:
            continue  # skip if DC wasn't assigned to any group

        padded_bounds: Dict[int, Tuple[float, float]] = {}
        for col, dim in g.column_to_dim.items():
            if dc.static_bounds and col in dc.static_bounds:
                padded_bounds[dim] = dc.static_bounds[col]  # key = dim INDEX
            elif config.infinite_padding:
                padded_bounds[dim] = (-math.inf, math.inf)  # key = dim INDEX
            # else: dimension excluded from box entirely

        boxes.append(ActiveBox(
            box_id=f"box_{dc.dc_id}_{g.group_id}",
            dc_id=dc.dc_id,
            rule_group=g.group_id,
            feature_mapping=dict(g.column_to_dim),
            padded_bounds=padded_bounds,
        ))

    return boxes
