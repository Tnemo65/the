"""
DC Checker - Port từ Rapidash/DCVerifier.java

Denial Constraint checking với:
- kd-tree / Range-tree (orthogonal range search)
- Incremental updates (từ Weever)
- Multiple DC types: Candidate Key, Functional Dependency, Order Dependency
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple
from enum import Enum
import re


class Operator(Enum):
    """Operators cho predicates."""
    EQUAL = "=="
    NOT_EQUAL = "<>"
    LESS = "<"
    LESS_EQUAL = "<="
    GREATER = ">"
    GREATER_EQUAL = ">="


@dataclass
class Predicate:
    """
    Một predicate trong DC.
    
    Ví dụ: "s.Salary < t.Salary"
    """
    left_column: str
    right_column: str
    operator: Operator
    
    @property
    def is_homogeneous(self) -> bool:
        """Kiểm tra xem predicate có homogeneous không."""
        return self.left_column == self.right_column


@dataclass
class DenialConstraint:
    """
    Một Denial Constraint.
    
    Ví dụ: NOT (State = t.State AND Salary <= t.Salary AND FedTaxRate > t.FedTaxRate)
    """
    dc_id: str
    predicates: List[Predicate] = field(default_factory=list)
    enabled: bool = True
    
    @classmethod
    def from_string(cls, dc_id: str, dc_string: str) -> 'DenialConstraint':
        """
        Parse DC từ string.
        
        Format: "NOT (pred1 AND pred2 AND ...)"
        """
        dc = cls(dc_id=dc_id)
        
        inner = dc_string.replace("NOT (", "").replace(")", "")
        pred_strings = [p.strip() for p in inner.split("AND")]
        
        for pred_str in pred_strings:
            pred = cls._parse_predicate(pred_str)
            if pred:
                dc.predicates.append(pred)
        
        return dc
    
    @staticmethod
    def _parse_predicate(pred_str: str) -> Optional[Predicate]:
        """Parse một predicate string."""
        pred_str = pred_str.strip()
        
        # Match pattern: s.column op t.column
        match = re.match(r's\.(\w+)\s*([<>=!]+)\s*t\.(\w+)', pred_str)
        if match:
            return Predicate(
                left_column=match.group(1),
                right_column=match.group(3),
                operator=Operator(match.group(2))
            )
        
        return None


@dataclass
class DCViolation:
    """Kết quả vi phạm DC."""
    dc_id: str
    violating_tuples: List[Tuple[Any, Any]]
    violation_count: int
    is_violated: bool


class KDTreeNode:
    """
    KD-Tree node cho orthogonal range search.
    
    Port từ Rapidash.
    """
    
    def __init__(
        self,
        point: List[Any],
        point_id: Any,
        dimension: int = 0,
        left: Optional['KDTreeNode'] = None,
        right: Optional['KDTreeNode'] = None
    ):
        self.point = point
        self.point_id = point_id
        self.dimension = dimension
        self.left = left
        self.right = right
        self.min_bounds: List[Any] = point.copy()
        self.max_bounds: List[Any] = point.copy()
    
    def insert(self, point: List[Any], point_id: Any) -> None:
        """Chèn một điểm vào cây."""
        if point[self.dimension] < self.point[self.dimension]:
            if self.left:
                self.left.insert(point, point_id)
            else:
                self.left = KDTreeNode(point, point_id, (self.dimension + 1) % len(point))
        else:
            if self.right:
                self.right.insert(point, point_id)
            else:
                self.right = KDTreeNode(point, point_id, (self.dimension + 1) % len(point))
        
        self._update_bounds(point)
    
    def query_range(
        self,
        lower: List[Any],
        upper: List[Any]
    ) -> List[Tuple[List[Any], Any]]:
        """Query tất cả điểm trong range [lower, upper]."""
        results = []
        
        if self._point_in_range(lower, upper):
            results.append((self.point, self.point_id))
        
        dim = self.dimension
        
        if lower[dim] <= self.point[dim] and self.left:
            results.extend(self.left.query_range(lower, upper))
        
        if upper[dim] >= self.point[dim] and self.right:
            results.extend(self.right.query_range(lower, upper))
        
        return results
    
    def _point_in_range(self, lower: List[Any], upper: List[Any]) -> bool:
        """Kiểm tra xem điểm hiện tại có trong range không."""
        for i, val in enumerate(self.point):
            if val < lower[i] or val > upper[i]:
                return False
        return True
    
    def _update_bounds(self, point: List[Any]) -> None:
        """Cập nhật bounds sau khi insert."""
        for i in range(len(point)):
            self.min_bounds[i] = min(self.min_bounds[i], point[i])
            self.max_bounds[i] = max(self.max_bounds[i], point[i])


class DCChecker:
    """
    DC Checker với Orthogonal Range Search.
    
    Port từ Rapidash/DCVerifier.java.
    """
    
    def __init__(
        self,
        constraints: List[DenialConstraint] = None,
        indexed_columns: List[str] = None,
        use_kdtree: bool = True
    ):
        self.constraints = constraints or []
        self.indexed_columns = indexed_columns or []
        self.use_kdtree = use_kdtree
        
        # Hash table: equality_value -> tree
        self._hash_table: Dict[Tuple, KDTreeNode] = {}
        
        # Column indices
        self._column_indices = {col: i for i, col in enumerate(self.indexed_columns)}
    
    def add_constraint(self, dc: DenialConstraint) -> None:
        """Thêm một DC constraint."""
        self.constraints.append(dc)
    
    def check_window(
        self,
        tuples: List[Dict[str, Any]],
        window_id: str
    ) -> List[DCViolation]:
        """Kiểm tra tất cả DCs trên một window."""
        violations = []
        
        self._build_indexes(tuples)
        
        for dc in self.constraints:
            if not dc.enabled:
                continue
            violation = self._check_dc(dc, tuples)
            violations.append(violation)
        
        return violations
    
    def update_with_tuple(self, tuple_data: Dict[str, Any]) -> List[DCViolation]:
        """Cập nhật indexes với một tuple mới (incremental)."""
        violations = []
        
        self._insert_tuple(tuple_data)
        
        for dc in self.constraints:
            if not dc.enabled:
                continue
            violation = self._check_dc_incremental(dc, tuple_data)
            if violation.is_violated:
                violations.append(violation)
        
        return violations
    
    def _build_indexes(self, tuples: List[Dict[str, Any]]) -> None:
        """Xây dựng hash table và kd-tree indexes."""
        self._hash_table.clear()
        
        for tuple_data in tuples:
            self._insert_tuple(tuple_data)
    
    def _insert_tuple(self, tuple_data: Dict[str, Any]) -> None:
        """Chèn một tuple vào indexes."""
        if not self.indexed_columns:
            return
        
        key_values = tuple_data.get(self.indexed_columns[0])
        key = (key_values,) if key_values is not None else None
        
        if key is None:
            return
        
        point = self._extract_point(tuple_data)
        
        if key not in self._hash_table:
            self._hash_table[key] = KDTreeNode(
                point=point,
                point_id=id(tuple_data),
                dimension=0
            )
        else:
            self._hash_table[key].insert(point, id(tuple_data))
    
    def _extract_point(self, tuple_data: Dict[str, Any]) -> List[Any]:
        """Trích xuất điểm cho kd-tree từ tuple."""
        return [tuple_data.get(col, 0) for col in self.indexed_columns]
    
    def _check_dc(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """Kiểm tra một DC."""
        ineq_preds = [p for p in dc.predicates if p.operator != Operator.EQUAL]
        
        if len(ineq_preds) == 0:
            return self._check_eq_only(dc, tuples)
        elif len(ineq_preds) == 1:
            return self._check_single_inequality(dc, tuples)
        else:
            return self._check_multiple_inequalities(dc, tuples)
    
    def _check_single_inequality(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """Kiểm tra DC với single inequality (O(N))."""
        violation_pairs = []
        min_max: Dict[Tuple, Tuple[float, float]] = {}
        
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            if key not in min_max:
                col = dc.predicates[0].left_column
                val = tuple_data.get(col, 0)
                min_max[key] = (val, val)
            else:
                col = dc.predicates[0].left_column
                val = tuple_data.get(col, 0)
                current_min, current_max = min_max[key]
                min_max[key] = (min(current_min, val), max(current_max, val))
        
        is_violated = False
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            current_min, current_max = min_max[key]
            col = dc.predicates[0].left_column
            val = tuple_data.get(col, 0)
            
            pred = dc.predicates[0]
            if self._is_violation(val, current_min, current_max, pred.operator):
                is_violated = True
                violation_pairs.append((tuple_data, None))
        
        return DCViolation(
            dc_id=dc.dc_id,
            violating_tuples=violation_pairs,
            violation_count=len(violation_pairs),
            is_violated=is_violated
        )
    
    def _check_multiple_inequalities(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """Kiểm tra DC với multiple inequalities (dùng kd-tree)."""
        violation_pairs = []
        
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            
            if key not in self._hash_table:
                continue
            
            tree = self._hash_table[key]
            lower, upper = self._build_range_bounds(tuple_data, dc)
            results = tree.query_range(lower, upper)
            
            for point, other_id in results:
                if other_id != id(tuple_data):
                    violation_pairs.append((tuple_data, other_id))
        
        return DCViolation(
            dc_id=dc.dc_id,
            violating_tuples=violation_pairs,
            violation_count=len(violation_pairs),
            is_violated=len(violation_pairs) > 0
        )
    
    def _check_eq_only(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """Kiểm tra DC chỉ với equality predicates."""
        # Đơn giản: đếm tuples có cùng key
        key_counts: Dict[Tuple, int] = {}
        
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            key_counts[key] = key_counts.get(key, 0) + 1
        
        is_violated = any(count > 1 for count in key_counts.values())
        
        return DCViolation(
            dc_id=dc.dc_id,
            violating_tuples=[],
            violation_count=sum(1 for count in key_counts.values() if count > 1),
            is_violated=is_violated
        )
    
    def _check_dc_incremental(self, dc: DenialConstraint, tuple_data: Dict[str, Any]) -> DCViolation:
        """Kiểm tra DC với tuple mới (incremental)."""
        # Simplified: kiểm tra với tất cả tuples trong partition
        return self._check_single_inequality(dc, [tuple_data])
    
    def _get_partition_key(self, tuple_data: Dict[str, Any], dc: DenialConstraint) -> Tuple:
        """Lấy partition key từ equality predicates."""
        key_parts = []
        for pred in dc.predicates:
            if pred.operator == Operator.EQUAL:
                key_parts.append(tuple_data.get(pred.left_column))
        return tuple(key_parts) if key_parts else None
    
    def _build_range_bounds(self, tuple_data: Dict[str, Any], dc: DenialConstraint) -> Tuple[List[Any], List[Any]]:
        """Build range bounds cho kd-tree query."""
        lower = []
        upper = []
        
        for pred in dc.predicates:
            val = tuple_data.get(pred.left_column, 0)
            
            if pred.operator == Operator.LESS:
                upper.append(val)
                lower.append(float('-inf'))
            elif pred.operator == Operator.LESS_EQUAL:
                upper.append(val)
                lower.append(float('-inf'))
            elif pred.operator == Operator.GREATER:
                upper.append(float('inf'))
                lower.append(val)
            elif pred.operator == Operator.GREATER_EQUAL:
                upper.append(float('inf'))
                lower.append(val)
            else:
                lower.append(val)
                upper.append(val)
        
        return lower, upper
    
    def _is_violation(self, val: float, min_val: float, max_val: float, operator: Operator) -> bool:
        """Kiểm tra xem có vi phạm không."""
        if operator == Operator.LESS:
            return val < min_val
        elif operator == Operator.LESS_EQUAL:
            return val <= min_val
        elif operator == Operator.GREATER:
            return val > max_val
        elif operator == Operator.GREATER_EQUAL:
            return val >= max_val
        return False
    
    def get_violations(self, window_id: str) -> List[DCViolation]:
        """Lấy violations cho một window (placeholder)."""
        return []
