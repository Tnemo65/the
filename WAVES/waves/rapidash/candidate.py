"""Module 2.6 — Rapidash: Candidate Violation Data Structures."""

from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class CandidateViolation:
    """A potential DC violation found by Rapidash traversal.

    "Candidate" means it passed spatial indexing (KD-Tree + Box Dropping)
    and tombstone filtering, but has not yet been confirmed by Decision layer.
    """
    dc_id: str
    window_id: str
    pane_id: str
    query_id: str           # event_id of the query point (s)
    matched_ids: List[str]  # ALL historical event_ids (t) inside the box
    box_id: str
    timestamp_ms: int
    detail: Dict = field(default_factory=dict)


@dataclass
class BatchedTraversalResult:
    """Result of a BatchedTraversal.query() call."""
    candidates: List[CandidateViolation]
    visited_nodes: int
    pruned_nodes: int
