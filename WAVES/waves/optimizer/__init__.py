"""Module 2.5 — Shared Rule Optimizer."""

from waves.optimizer.config import OptimizerConfig, NYC_TAXI_BOUNDS
from waves.optimizer.dc_parser import (
    DCParser,
    EnrichedDC,
    Predicate,
    PredicateType,
    strip_side_prefix,
)
from waves.optimizer.grouper import (
    ActiveBox,
    GroupMetadata,
    GreedyRuleGrouper,
    build_active_boxes,
)

__all__ = [
    # config
    "OptimizerConfig",
    "NYC_TAXI_BOUNDS",
    # dc_parser
    "DCParser",
    "EnrichedDC",
    "Predicate",
    "PredicateType",
    "strip_side_prefix",
    # grouper
    "ActiveBox",
    "GroupMetadata",
    "GreedyRuleGrouper",
    "build_active_boxes",
]
