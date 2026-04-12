"""Module 2.4 — Logical Engine.

GỘP 3 sub-components vào 1 file theo thiết kế:
  - StatisticalContext  (EMA mean / variance)
  - ElasticBoxGenerator (padding static bounds)
  - Coordinator          (update → build → emit)

Interface:
  Input  : DataEvent (from WindowManager) + dc_rules list + ElasticBoxConfig
  Output : List[ElasticBox]  →  Rapidash (2.6)
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple, List, Any
import math
import time


# ─── Config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ElasticBoxConfig:
    """System-wide config for LogicalEngine."""
    alpha: float = 0.05           # EMA smoothing factor
    k: float = 3.0               # multiplier on sigma for delta
    delta_min: float = 1.0       # minimum padding
    delta_max: float = 50.0      # maximum padding
    warmup: int = 50             # min samples before EMA box kicks in
    stale_timeout_sec: float = 300.0  # reset state after N seconds of silence
    partition_by: Optional[str] = None  # field name to partition state; None = global

    def __post_init__(self):
        if not 0 < self.alpha <= 1:
            raise ValueError(f"alpha must be in (0, 1], got {self.alpha}")
        if self.k <= 0:
            raise ValueError(f"k must be positive, got {self.k}")
        if self.delta_min <= 0:
            raise ValueError(f"delta_min must be positive, got {self.delta_min}")
        if self.warmup <= 0:
            raise ValueError(f"warmup must be positive, got {self.warmup}")


# ─── State ─────────────────────────────────────────────────────────────────────

@dataclass
class StatisticalState:
    """EMA statistics for one (rule_group, feature) key."""
    mean: float = 0.0
    variance: float = 0.0
    sample_count: int = 0
    last_update_ms: int = 0

    @property
    def std(self) -> float:
        return math.sqrt(max(self.variance, 1e-12))


# ─── ElasticBox ────────────────────────────────────────────────────────────────

@dataclass
class ElasticBox:
    """Padded box for one DC, used by Rapidash KD-Tree traversal."""
    dc_id: str
    rule_group: str
    dim_map: Dict[str, int]          # col_name → dim_index
    padded_bounds: Dict[int, Tuple[float, float]]  # dim_index → (lo, hi)
    delta: float                      # total padding applied (sum of per-dim deltas)
    sigma: float                      # max std across dimensions
    created_at_ms: int = 0

    def extract_point(self, attrs: Dict[str, Any]) -> Tuple[float, ...]:
        """Extract coordinate vector from event attributes using dim_map."""
        return tuple(attrs.get(col, 0.0) for col in sorted(self.dim_map, key=self.dim_map.get))


# ─── DenialConstraint (input schema from Optimizer) ───────────────────────────

@dataclass
class DenialConstraint:
    """DC descriptor parsed from dc_rules.json + enriched by Optimizer."""
    dc_id: str
    rule_group: str
    dim_map: Dict[str, int]              # col → dim_index
    static_bounds: Dict[str, Tuple[float, float]]  # col → (lo, hi)  — from optimizer
    feature_columns: List[str]            # which columns feed into EMA state

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DenialConstraint":
        return cls(
            dc_id=data["dc_id"],
            rule_group=data["rule_group"],
            dim_map=data.get("dim_map", {}),
            static_bounds=data.get("static_bounds", {}),
            feature_columns=data.get("feature_columns", list(data.get("dim_map", {}).keys())),
        )


# ─── LogicalEngine ────────────────────────────────────────────────────────────

class LogicalEngine:
    """Coordinator: update EMA stats → generate ElasticBoxes → return for Rapidash."""

    def __init__(self, config: ElasticBoxConfig):
        self.config = config
        self._state: Dict[tuple, StatisticalState] = {}

    # ── helpers ──────────────────────────────────────────────────────────────

    def _state_key(
        self,
        rule_group: str,
        feature: str,
        partition_key: Optional[str],
    ) -> tuple:
        if self.config.partition_by and partition_key is not None:
            return (partition_key, rule_group, feature)
        return (rule_group, feature)

    def _is_stale(self, state: StatisticalState) -> bool:
        if state.sample_count == 0:
            return False
        now_ms = _now_ms()
        return (now_ms - state.last_update_ms) > (self.config.stale_timeout_sec * 1000)

    def _ensure_not_stale(self, state: StatisticalState) -> StatisticalState:
        if self._is_stale(state):
            return StatisticalState(last_update_ms=_now_ms())
        return state

    # ── public API ───────────────────────────────────────────────────────────

    def update_statistical_state(
        self,
        rule_group: str,
        feature: str,
        value: float,
        partition_key: Optional[str] = None,
    ) -> Optional[StatisticalState]:
        """Update EMA mean/variance for one (rule_group, feature) key.

        Uses μ_{t-1} for variance to avoid look-ahead bias.
        Formula (Welford-style online EMA):
          μ_t = α·x_t + (1-α)·μ_{t-1}
          σ²_t = (1-α)·σ²_{t-1} + α·(x_t - μ_{t-1})²
        Returns None if value is None or non-numeric.
        """
        if value is None or not isinstance(value, (int, float)):
            return None
        key = self._state_key(rule_group, feature, partition_key)
        state = self._state.get(key)
        if state is None:
            state = StatisticalState(last_update_ms=_now_ms())
        else:
            state = self._ensure_not_stale(state)

        alpha = self.config.alpha
        old_mean = state.mean
        old_var = state.variance

        # Update mean: μ_t = α·x + (1-α)·μ_{t-1}
        new_mean = alpha * value + (1 - alpha) * old_mean
        # Update variance using d = x - μ_{t-1} (old mean, not yet updated)
        d = value - old_mean
        new_var = (1 - alpha) * old_var + alpha * d * d

        state.mean = new_mean
        state.variance = new_var
        state.sample_count += 1
        state.last_update_ms = _now_ms()
        self._state[key] = state
        return state

    def build_elastic_box(
        self,
        dc: DenialConstraint,
        partition_key: Optional[str] = None,
    ) -> ElasticBox:
        """Generate ElasticBox for one DC with EMA-padded bounds."""
        now_ms = _now_ms()
        k = self.config.k
        delta_min = self.config.delta_min
        delta_max = self.config.delta_max
        warmup = self.config.warmup

        padded_bounds: Dict[int, Tuple[float, float]] = {}
        total_delta = 0.0
        max_sigma = 0.0

        for col, (s_lo, s_hi) in dc.static_bounds.items():
            key = self._state_key(dc.rule_group, col, partition_key)
            state = self._state.get(key)
            if state is not None:
                state = self._ensure_not_stale(state)

            if state is not None and state.sample_count >= warmup:
                sigma = state.std
                delta = max(min(k * sigma, delta_max), delta_min)
                max_sigma = max(max_sigma, sigma)
            else:
                delta = delta_min
                sigma = 0.0
                max_sigma = max(max_sigma, sigma)

            dim = dc.dim_map.get(col, -1)
            if dim >= 0:
                padded_bounds[dim] = (s_lo - delta, s_hi + delta)
            total_delta += delta

        return ElasticBox(
            dc_id=dc.dc_id,
            rule_group=dc.rule_group,
            dim_map=dc.dim_map,
            padded_bounds=padded_bounds,
            delta=total_delta,
            sigma=max_sigma,
            created_at_ms=now_ms,
        )

    def process_event(
        self,
        event_attrs: Dict[str, Any],
        dc_rules: List[DenialConstraint],
        partition_key: Optional[str] = None,
    ) -> List[ElasticBox]:
        """Full pipeline: update EMA stats → build ElasticBoxes → return list."""
        # Step 1: update stats for ALL (rule_group, feature) pairs
        for dc in dc_rules:
            for col in dc.feature_columns:
                val = event_attrs.get(col)
                if val is not None and isinstance(val, (int, float)):
                    self.update_statistical_state(dc.rule_group, col, float(val), partition_key)

        # Step 2: build boxes AFTER stats updated
        boxes = []
        for dc in dc_rules:
            box = self.build_elastic_box(dc, partition_key)
            boxes.append(box)

        return boxes

    def get_state(self, rule_group: str, feature: str, partition_key: Optional[str] = None) -> Optional[StatisticalState]:
        """Expose state for testing / introspection."""
        return self._state.get(self._state_key(rule_group, feature, partition_key))

    def reset_state(self) -> None:
        """Clear all EMA state (e.g., on window close)."""
        self._state.clear()


# ─── Module-level convenience ─────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)
