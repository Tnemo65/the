"""
Weever LT-Tree - Incremental Less-Than indexing

Port từ Weever/WeeverSequential.java và các schedules.

LT-Tree là AVL Tree với lt_aggregate (tất cả TIDs nhỏ hơn node).
Operations:
- Insert: O(log N)
- Delete: O(log N)
- Query less than: O(log N) + |result|
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Generic, TypeVar
import bisect


T = TypeVar('T')


@dataclass
class AVLNode(Generic[T]):
    """AVL Tree node với LT aggregate."""
    key: T
    tuple_data: Any
    left: Optional['AVLNode[T]'] = None
    right: Optional['AVLNode[T]'] = None
    height: int = 1
    
    # CRITICAL: Less-Than aggregate
    lt_aggregate: Set[Any] = field(default_factory=set)
    lt_count: int = 1


class LTTree(Generic[T]):
    """
    Less-Than Tree - Weever's core data structure.
    
    Mỗi node lưu trữ:
    1. Key (giá trị column)
    2. Tuple data
    3. lt_aggregate: tất cả tuples nhỏ hơn key
    
    Operations:
    - Insert: O(log N)
    - Delete: O(log N)
    - Query less than: O(log N) + |result|
    """
    
    def __init__(
        self,
        column: str,
        column_type: str = "int",
        config: Optional[Dict] = None
    ):
        self.column = column
        self.column_type = column_type
        self.config = config or {}
        self.root: Optional[AVLNode[T]] = None
        self.size: int = 0
    
    def insert(self, key: T, tuple_data: Any) -> None:
        """Chèn một tuple vào tree."""
        if self.root is None:
            self.root = AVLNode(key=key, tuple_data=tuple_data)
            self.root.lt_aggregate = {id(tuple_data)}
        else:
            self.root = self._insert_recursive(self.root, key, tuple_data)
        
        self.size += 1
        self._update_heights(self.root)
        self.root = self._rebalance(self.root)
    
    def delete(self, key: T, tuple_data: Any) -> bool:
        """Xóa một tuple khỏi tree."""
        if self.root is None:
            return False
        
        deleted = self._delete_recursive(self.root, key, tuple_data)
        if deleted:
            self.size -= 1
            self._update_heights(self.root)
            self.root = self._rebalance(self.root)
        
        return deleted
    
    def query_less_than(self, key: T) -> List[Any]:
        """
        Query tất cả tuples có key < given key.
        
        CRITICAL: Sử dụng lt_aggregate để query nhanh O(log N).
        """
        if self.root is None:
            return []
        
        results = []
        self._query_recursive(self.root, key, results)
        return results
    
    def query_less_equal(self, key: T) -> List[Any]:
        """Query tất cả tuples có key <= given key."""
        results = self.query_less_than(key)
        node = self._find_node(self.root, key)
        if node:
            results.append(node.tuple_data)
        return results
    
    def query_between(self, lower: T, upper: T) -> List[Any]:
        """Query tất cả tuples có lower < key < upper."""
        results = []
        
        # Lấy tất cả < upper
        all_upper = self.query_less_than(upper)
        
        # Loại bỏ những cái < lower
        all_lower = self.query_less_than(lower)
        lower_set = set(id(t) for t in all_lower)
        
        for t in all_upper:
            if id(t) not in lower_set:
                results.append(t)
        
        return results
    
    def _insert_recursive(self, node: AVLNode[T], key: T, tuple_data: Any) -> AVLNode[T]:
        """Chèn đệ quy."""
        if key < node.key:
            if node.left is None:
                node.left = AVLNode(key=key, tuple_data=tuple_data)
                node.left.lt_aggregate = {id(tuple_data)}
            else:
                node.left = self._insert_recursive(node.left, key, tuple_data)
        else:
            if node.right is None:
                node.right = AVLNode(key=key, tuple_data=tuple_data)
                node.right.lt_aggregate = {id(tuple_data)}
            else:
                node.right = self._insert_recursive(node.right, key, tuple_data)
        
        # Update lt_aggregate
        node.lt_aggregate.add(id(tuple_data))
        node.lt_count = len(node.lt_aggregate)
        
        return self._rebalance(node)
    
    def _delete_recursive(
        self,
        node: Optional[AVLNode[T]],
        key: T,
        tuple_data: Any
    ) -> Optional[AVLNode[T]]:
        """Xóa đệ quy."""
        if node is None:
            return None
        
        if key < node.key:
            node.left = self._delete_recursive(node.left, key, tuple_data)
            if node.left:
                node.left.lt_aggregate.discard(id(tuple_data))
                node.left.lt_count = len(node.left.lt_aggregate)
        elif key > node.key:
            node.right = self._delete_recursive(node.right, key, tuple_data)
        else:
            # Found node to delete
            if node.left is None:
                return node.right
            elif node.right is None:
                return node.left
            else:
                # Replace with inorder successor
                successor = self._find_min(node.right)
                node.key = successor.key
                node.tuple_data = successor.tuple_data
                node.right = self._delete_recursive(node.right, successor.key, successor.tuple_data)
        
        self._update_heights(node)
        return self._rebalance(node)
    
    def _query_recursive(
        self,
        node: Optional[AVLNode[T]],
        key: T,
        results: List[Any]
    ) -> None:
        """Query đệ quy."""
        if node is None:
            return
        
        if key < node.key:
            self._query_recursive(node.left, key, results)
        elif key > node.key:
            # Thêm current node và tất cả lt_aggregate của nó
            results.append(node.tuple_data)
            results.extend([
                t for t in self._collect_lt_aggregate(node.left)
                if t != node.tuple_data
            ])
            self._query_recursive(node.right, key, results)
        else:
            # Key == current node
            results.extend(self._collect_lt_aggregate(node.left))
    
    def _collect_lt_aggregate(self, node: Optional[AVLNode[T]]) -> List[Any]:
        """Thu thập tất cả tuples trong lt_aggregate của subtree."""
        if node is None:
            return []
        results = [node.tuple_data]
        if node.left:
            results.extend(self._collect_lt_aggregate(node.left))
        return results
    
    def _find_node(self, node: Optional[AVLNode[T]], key: T) -> Optional[AVLNode[T]]:
        """Tìm node có key."""
        if node is None:
            return None
        if key < node.key:
            return self._find_node(node.left, key)
        elif key > node.key:
            return self._find_node(node.right, key)
        return node
    
    def _find_min(self, node: AVLNode[T]) -> AVLNode[T]:
        """Tìm node có key nhỏ nhất trong subtree."""
        while node.left:
            node = node.left
        return node
    
    def _update_heights(self, node: Optional[AVLNode[T]]) -> None:
        """Cập nhật heights."""
        if node:
            node.height = 1 + max(
                self._height(node.left),
                self._height(node.right)
            )
    
    def _height(self, node: Optional[AVLNode[T]]) -> int:
        """Lấy height của node."""
        return node.height if node else 0
    
    def _balance_factor(self, node: Optional[AVLNode[T]]) -> int:
        """Tính balance factor."""
        return self._height(node.left) - self._height(node.right) if node else 0
    
    def _rebalance(self, node: Optional[AVLNode[T]]) -> Optional[AVLNode[T]]:
        """Rebalance cây AVL."""
        if node is None:
            return None
        
        node.lt_aggregate = self._collect_lt_aggregate(node)
        node.lt_count = len(node.lt_aggregate)
        
        bf = self._balance_factor(node)
        
        # Left heavy
        if bf > 1:
            if self._balance_factor(node.left) < 0:
                node.left = self._rotate_left(node.left)
            return self._rotate_right(node)
        
        # Right heavy
        if bf < -1:
            if self._balance_factor(node.right) > 0:
                node.right = self._rotate_right(node.right)
            return self._rotate_left(node)
        
        return node
    
    def _rotate_right(self, y: AVLNode[T]) -> AVLNode[T]:
        """Rotate right."""
        x = y.left
        T2 = x.right
        
        x.right = y
        y.left = T2
        
        self._update_heights(y)
        self._update_heights(x)
        
        x.lt_aggregate = self._collect_lt_aggregate(x)
        x.lt_count = len(x.lt_aggregate)
        y.lt_aggregate = self._collect_lt_aggregate(y)
        y.lt_count = len(y.lt_aggregate)
        
        return x
    
    def _rotate_left(self, x: AVLNode[T]) -> AVLNode[T]:
        """Rotate left."""
        y = x.right
        T2 = y.left
        
        y.left = x
        x.right = T2
        
        self._update_heights(x)
        self._update_heights(y)
        
        x.lt_aggregate = self._collect_lt_aggregate(x)
        x.lt_count = len(x.lt_aggregate)
        y.lt_aggregate = self._collect_lt_aggregate(y)
        y.lt_count = len(y.lt_aggregate)
        
        return y


class PredicateScheduler:
    """
    Schedule predicates để optimize execution.
    
    Port từ Weever/PredicateScheduler.java.
    """
    
    def __init__(
        self,
        use_selectivity: bool = True,
        use_prefix_sharing: bool = True
    ):
        self.use_selectivity = use_selectivity
        self.use_prefix_sharing = use_prefix_sharing
    
    def schedule(self, predicates: List) -> List:
        """
        Sắp xếp predicates theo thứ tự tối ưu.
        
        Strategy:
        1. Equality predicates trước (ít selectivity)
        2. Inequality predicates có selectivity thấp trước
        3. Sử dụng prefix sharing nếu có
        """
        sorted_preds = list(predicates)
        
        if self.use_selectivity:
            sorted_preds.sort(key=self._estimate_selectivity)
        
        return sorted_preds
    
    def _estimate_selectivity(self, pred) -> float:
        """
        Ước lượng selectivity của predicate.
        
        Returns:
            0.0 = very selective (ít kết quả)
            1.0 = not selective (nhiều kết quả)
        """
        from .dc_checker import Operator
        
        if hasattr(pred, 'operator'):
            if pred.operator == Operator.EQUAL:
                return 0.1  # Rất selective
            elif pred.operator in (Operator.NOT_EQUAL,):
                return 0.9  # Không selective
            else:
                return 0.5  # Trung bình
        
        return 0.5