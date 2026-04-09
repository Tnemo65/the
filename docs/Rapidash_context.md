# Rapidash - Documentation Context

## 1. Tong quan

**Paper:** Rapidash: Efficient Detection of Constraint Violations (VLDB 2024)
**Repository:** `/home/dtl/Documents/thesis/base/Rapidash/`
**Language:** Java
**GitHub:** Internal repository (không có public link trong README)

## 2. Muc tieu cua Paper

Phát hiện nhanh các vi phạm **Denial Constraints (DCs)** trên datasets lớn:
- DC: Ràng buộc dạng `NOT (predicate1 AND predicate2 AND ...)`
- Ví dụ: `NOT (State=Wisconsin AND Salary <= 4000 AND Rate > 22)`
- Đạt **84x nhanh hơn** state-of-the-art (Facet)

## 3. Phuong phap

### 3.1 Orthogonal Range Search

- Mỗi tuple = 1 điểm trong không gian k-chiều
- Vi phạm DC = điểm nằm trong vùng truy vấn (axis-aligned hypercube)
- Dùng **Range Tree** hoặc **KD-Tree** để tìm nhanh

### 3.2 Hash Table + Tree Structure

```java
// Hash partition theo equality predicates
Map<List<Integer>, RangeTreeHelper> treesMap = new HashMap<>();

// Moi partition = 1 range tree
// Tuong tu voi "tu nhieu ngan, moi ngan = 1 cay"
```

### 3.3 Time Complexity

| Scenario | Complexity |
|----------|-----------|
| General case | O(|R| · log^k |R|) |
| Single inequality | **O(|R|)** - Linear! |
| Facet (baseline) | O(|R|²) |

## 4. Predicate Types

### 4.1 Homogeneous Predicates
- `s.A op t.B` với A = B (cùng column)
- Ví dụ: `s.Salary < t.Salary`

### 4.2 Heterogeneous Predicates
- `s.A op t.B` với A ≠ B (khác column)
- Ví dụ: `s.Salary < t.FedTaxRate`

### 4.3 Supported Operators
- Equality: `=`, `==`
- Disequality: `≠`, `!=`, `<>`
- Inequality: `<`, `>`, `<=`, `>=`

## 5. Optimization Techniques

### 5.1 Early Termination
- Khi `earlyStop = true`: Trả về boolean ngay khi tìm thấy 1 violation
- Complexity O(1) trong best case

### 5.2 Single Inequality Optimization
```java
// Chi can track min/max value
Map<List<Integer>, Integer> extremeAsLeftSide = new HashMap<>();
Map<List<Integer>, Integer> extremeAsRightSide = new HashMap<>();
```

### 5.3 Sort-Based Enumeration
```java
// Sort tuples theo predicate column
Collections.sort(e.getValue(), comparator);
// Chi can query tu items nho hon
```

## 6. Data Structures

### 6.1 RangeTree
- Multi-dimensional range tree
- Insert + rangeCount operations
- File: `rangetree/RangeTreeCount.java`

### 6.2 KDTree
- Adapted from Gonnet & Baeza-Yates
- Efficient pruning
- File: `kdrange/KDTreeHelper.java`

### 6.3 AVLTree
- Cho single inequality optimization
- File: `trees/AVLTree.java`

### 6.4 RoaringBitmap
- Efficient tuple ID storage
- File: External dependency

## 7. Loi moi can bo sung (theo WAVES)

### 7.1 Streaming Adaptation
- Hiện tại chỉ hoạt động trên **static data**
- Cần adaptation cho **incremental updates**:
  - Insert new tuples → add vao tree
  - Delete tuples → remove khoi tree
  - Time window → maintain multiple trees

### 7.2 Window-Aware Range Search
- Partition data theo time windows
- Query chỉ trong window hiện tại
- Delete expired tuples khi window expired

### 7.3 Integration voi Stream DaQ
```java
// Pseudocode cho integration
class WindowedRapidash {
    Map<Long, RangeTree> windowTrees; // window_id -> tree
    
    void process(Tuple t, long windowId) {
        windowTrees[windowId].insert(t);
    }
    
    void expireWindow(long windowId) {
        windowTrees.remove(windowId);
    }
    
    long countViolations(long windowId, Constraint dc) {
        return windowTrees[windowId].rangeCount(dc);
    }
}
```

## 8. Vi du DC

### 8.1 Tax Dataset
```
DC1: NOT (AreaCode = t.AreaCode AND Phone = t.Phone)
     → Candidate key check

DC2: NOT (ZipCode = t.ZipCode AND City ≠ t.City)
     → Functional dependency

DC3: NOT (State = t.State AND Salary > t.Salary AND Rate < t.Rate)
     → Order dependency
```

### 8.2 TPC-H Dataset
```
DC5: NOT (Customer = t.Supplier AND Supplier = t.Customer)
     → Unique constraint

DC6: NOT (Receiptdate >= Shipdate AND Shipdate <= Receiptdate)
     → Heterogeneous constraint
```

## 9. Noi dung trong Repository

```
Rapidash/
├── src/main/java/org/dc/
│   ├── DCVerifier.java       # Main algorithm
│   ├── Constraint.java     # DC parsing
│   ├── Predicate.java      # Predicate representation
│   ├── Main.java          # Entry point
│   ├── Tax.java           # Tax dataset experiments
│   ├── TPCH.java          # TPC-H experiments
│   └── NCVoter.java       # NCVoter experiments
├── rangetree/             # Range tree implementation
├── kdrange/              # KD tree implementation
├── kdrangeDouble/        # KD tree with doubles
├── rangetreeboolean/      # Boolean range tree
└── trees/                # AVL tree
```

## 10. Performance Results

| Dataset | Size | Speedup vs Facet |
|---------|------|------------------|
| Tax | 1M rows | 2-84x |
| TPC-H | 1M rows | 2-84x |
| NCVoter | 1M rows | 2-200x |
| Production D1 | 50M rows | 10-40x |
| Production D2 | 25M rows | 10-40x |
