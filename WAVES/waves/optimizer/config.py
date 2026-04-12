"""Module 2.5 — Shared Rule Optimizer: config."""

from dataclasses import dataclass, field
from typing import Dict, Tuple


NYC_TAXI_BOUNDS: Dict[str, Tuple[float, float]] = {
    "fare_amount":    (2.5,  500.0),
    "trip_distance": (0.0,  100.0),
    "trip_duration": (60.0, 10800.0),
    "tolls_amount":  (0.0,  50.0),
}


@dataclass(frozen=True)
class OptimizerConfig:
    """System-wide config for the SharedRuleOptimizer."""
    k_max: int = 5                          # max dimensions per group (dimension cap)
    infinite_padding: bool = True           # enable (-inf, +inf) padding for unused dims
    static_bounds: Dict[str, Tuple[float, float]] = field(
        default_factory=lambda: dict(NYC_TAXI_BOUNDS)
    )

    def __post_init__(self):
        if self.k_max <= 0:
            raise ValueError(f"k_max must be positive, got {self.k_max}")
