"""Module 2.5 — Shared Rule Optimizer: DC parser.

Parse raw DC rules JSON → EnrichedDC objects with predicates, equality/inequality columns.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any


class PredicateType(Enum):
    EQUAL = "EQUAL"
    LESS = "LESS"
    LESS_EQUAL = "LESS_EQUAL"
    GREATER = "GREATER"
    GREATER_EQUAL = "GREATER_EQUAL"

    @classmethod
    def from_str(cls, s: str) -> "PredicateType":
        try:
            return cls(s.upper())
        except ValueError:
            raise ValueError(f"Unknown predicate operator: {s!r}")


@dataclass
class Predicate:
    """A single predicate from a DC rule."""
    left_col: str           # column name without suffix
    left_side: Optional[str]  # "s" or "t", None if bare column
    operator: PredicateType
    right_col: str          # column name without suffix, or constant string
    right_side: Optional[str]  # "s", "t", or None (constant)
    is_constant: bool        # True if right_col is a literal constant
    constant_value: Optional[float] = None  # parsed if is_constant

    @property
    def is_equality(self) -> bool:
        return self.operator == PredicateType.EQUAL

    @property
    def is_inequality(self) -> bool:
        return not self.is_equality


def strip_side_prefix(col: str) -> Tuple[str, Optional[str]]:
    """Strip _s or _t suffix from a column name.

    'trip_distance_s' -> ('trip_distance', 's')
    'PULocationID_t'   -> ('PULocationID', 't')
    'fare_amount'      -> ('fare_amount', None)  # bare column (constant)
    """
    if col.endswith("_s"):
        return col[:-2], "s"
    if col.endswith("_t"):
        return col[:-2], "t"
    return col, None


def _parse_constant(value: Any) -> Optional[float]:
    """Try to parse a numeric constant. Return None if not numeric."""
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class DCParser:
    """Parse raw DC rules JSON into EnrichedDC objects."""

    def parse_dc_json(self, raw: Dict[str, Any]) -> List[Predicate]:
        """Parse one DC dict (from JSON) → list of Predicates."""
        predicates = []
        for p in raw.get("predicates", []):
            lcol = p["left_col"]
            rcol = p["right_col"]
            op_str = p["operator"]

            left_name, left_side = strip_side_prefix(lcol)
            right_name, right_side = strip_side_prefix(rcol)

            const_val = _parse_constant(right_name) if right_side is None else None
            is_const = const_val is not None

            predicates.append(Predicate(
                left_col=left_name,
                left_side=left_side,
                operator=PredicateType.from_str(op_str),
                right_col=right_name,
                right_side=right_side,
                is_constant=is_const,
                constant_value=const_val,
            ))
        return predicates

    def get_equality_cols(self, predicates: List[Predicate]) -> List[str]:
        """Extract unique column names from EQUAL predicates (left_col only)."""
        return sorted({p.left_col for p in predicates if p.is_equality})

    def get_inequality_cols(self, predicates: List[Predicate]) -> List[str]:
        """Extract unique column names from non-EQUAL predicates (left_col only)."""
        return sorted({p.left_col for p in predicates if p.is_inequality})

    def get_equality_signature(self, predicates: List[Predicate]) -> Tuple[str, ...]:
        """Sorted tuple of equality column names — used as group key."""
        return tuple(self.get_equality_cols(predicates))

    def parse_dc_rules(self, rules_json: List[Dict[str, Any]]) -> List["EnrichedDC"]:
        """Parse full rules JSON → list of EnrichedDC objects."""
        dcs = []
        for raw in rules_json:
            predicates = self.parse_dc_json(raw)
            dcs.append(EnrichedDC(
                dc_id=raw["dc_id"],
                rule_group=raw["rule_group"],
                predicates=predicates,
                equality_cols=self.get_equality_cols(predicates),
                inequality_cols=self.get_inequality_cols(predicates),
            ))
        return dcs


@dataclass
class EnrichedDC:
    """A DC enriched with parsed predicates and column classification."""
    dc_id: str
    rule_group: str
    predicates: List[Predicate]
    equality_cols: List[str]
    inequality_cols: List[str]
    # dim_map and static_bounds are assigned by GreedyRuleGrouper
    dim_map: Optional[Dict[str, int]] = None
    static_bounds: Optional[Dict[str, Tuple[float, float]]] = None
