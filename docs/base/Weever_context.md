# Weever - Documentation Context

## 1. Tong quan

**Paper:** Incremental Denial Constraint Checking (Academic research)
**Repository:** `/home/dtl/Documents/thesis/base/Weever/`
**Language:** Java
**Author:** Hasso Plattner Institute

## 2. Muc tieu cua Research

Phát hiện vi phạm Denial Constraints một cách **incremental**:
- Khi có tuple mới được insert/delete
- Không cần quét lại toàn bộ dataset
- Sử dụng **index structures** để tối ưu

## 3. Bai toan goc

### 3.1 Rapidash Limitation
- Rapidash xây dựng tree từ đầu cho mỗi window
- Khi window trượt 1 phút:
  - 90% dữ liệu cũ vẫn hợp lệ
  - Phải rebuild tree = O(N) lãng phí

### 3.2 Weever Solution
- Duy trì **LT-Tree** (Less-Than Tree)
- Insert/Delete chỉ cập nhật phần liên quan
- Không cần rebuild

## 4. Cac thanh phan chinh

### 4.1 LT-Tree (Less-Than Tree)

Mở rộng của sorted tree (AVL/Red-Black):
- Mỗi node lưu trữ **ltAggregate** = tất cả TIDs nhỏ hơn node
- Query: `Find all tuples where column < value`
  - Đi xuống cây O(log N)
  - Trả về ltAggregate + some nodes

```java
// Ví dụ: Tìm tất cả tuple có Salary < 4000
LTNode result = tree.findLessThan(salaryIndex, 4000);
// Trả về nhanh chóng không cần scan
```

### 4.2 Predicate Scheduling

Tối ưu thứ tự thực hiện predicates:

```java
// Ưu tiên predicate có selectivity thấp (ít kết quả hơn)
List<Predicate> schedule = predicateScheduler.optimize(dc);
// Thực hiện predicate ít kết quả trước
// Nếu predicate đầu tiên fail → skip các predicate còn lại
```

### 4.3 Prefix Optimization

Phát hiện và tái sử dụng prefix chung giữa các DCs:

```java
// DC1: NOT (A = x AND B < y AND C > z)
// DC2: NOT (A = x AND B < y AND D > w)
// → Prefix chung: A = x AND B < y
// → Chỉ tính một lần, tái sử dụng
```

### 4.4 TIdSet Implementations

Lưu trữ tập hợp Tuple IDs:

| Implementation | Best For | Library |
|---------------|----------|--------|
| `RoaringTidSet` | Sparse sets | RoaringBitmap |
| `BitTidSet` | Medium density | Java BitSet |
| `HashTIdSet` | Very sparse | fastutil |

## 5. Incremental Operations

### 5.1 Insert Tuple
```java
public void insert(Tuple t) {
    // 1. Check violations với existing data
    for (DC dc : constraints) {
        checkViolation(t, dc);
    }
    
    // 2. Insert vào các indexes
    for (Index idx : indexes) {
        idx.insert(t);
    }
}
```

### 5.2 Delete Tuple
```java
public void delete(Tuple t) {
    // 1. Xóa khỏi indexes
    for (Index idx : indexes) {
        idx.delete(t);
    }
    
    // 2. Update intermediate state
    intermediateMaintainer.remove(t);
}
```

## 6. Loi moi can bo sung (theo WAVES)

### 6.1 Streaming Adaptation
- Hiện tại chỉ xử lý **batch inserts/deletes**
- Cần adaptation cho **streaming**:
  - Time-based window expiration
  - Late data handling
  - State cleanup

### 6.2 Watermark Integration
- Khi watermark advance:
  - Mark window là "complete"
  - Xóa expired intermediate state
  - Không cần giữ provisional results

### 6.3 Integration voi Rapidash
```
┌─────────────────────────────────────────────────────────────┐
│                    WAVES Architecture         │
├─────────────────────────────────────────────────────────────┤
│  Stream DaQ: Windowing + Basic Checks           │
│         ↓ (push complex checks)                   │
│  Rapidash: kd-tree + Orthogonal Range Search    │
│         ↓ (incremental updates)                   │
│  Weever: LT-Tree + Predicate Scheduling          │
│         ↓ (late data handling)                   │
│  Watermark: Late data + Retraction              │
└─────────────────────────────────────────────────────────────┘
```

## 7. Vi du Configuration

```json
{
  "name": "tax_dc1",
  "dataPath": "data/tax.csv",
  "delimiter": ",",
  "constraints": [
    {
      "predicate": "State = t.State",
      "type": "EQUAL"
    },
    {
      "predicate": "Salary <= t.Salary",
      "type": "LESS_EQUAL"
    }
  ],
  "keyColumns": ["SSN"],
  "scheduling": "SELECTIVITY"
}
```

## 8. Noi dung trong Repository

```
Weever/
├── src/main/java/de/hpi/isg/
│   ├── Weever.java              # Abstract base class
│   ├── WeeverSequential.java    # Sequential implementation
│   ├── WeeverSingleIndex.java   # Single-index optimization
│   ├── Predicate.java           # Predicate class
│   ├── DenialConstraint.java    # DC class
│   ├── Operator.java            # Operators enum
│   ├── PredicateScheduler.java   # Scheduling logic
│   ├── schedules/                # Scheduling classes
│   │   ├── ScheduledPredicate.java
│   │   ├── IntermediateMaintainer.java
│   │   └── PossiblePrefix.java
│   ├── tidSets/                 # TIdSet implementations
│   │   ├── RoaringTidSet.java
│   │   ├── BitTidSet.java
│   │   └── HashTIdSet.java
│   └── dataStructures/ltAggregateMap/
│       ├── LTAggregateMap.java
│       ├── Int2TidSetLTTreeAggregateMap.java
│       └── Float2TidSetLTTreeAggregateMap.java
├── configurations/            # 15+ JSON configs
│   ├── config_tax_*.json
│   ├── config_lineitem_*.json
│   └── config_flights_*.json
└── pom.xml                     # Maven config
```

## 9. So sanh voi Rapidash

| Aspect | Rapidash | Weever |
|--------|---------|--------|
| **Data Model** | Static | Incremental |
| **Time Complexity** | O(N log^k N) | Amortized O(log N) per update |
| **Space** | O(N log^k N) | O(N) |
| **Updates** | Full rebuild | In-place |
| **Predicate Types** | All | All |
| **Scheduling** | None | Selectivity-based |

## 10. Dependencies

- Apache Commons CSV 1.9.0
- RoaringBitmap 0.9.38
- fastutil 8.3.0
- SQLite JDBC 3.43.0.0
- JUnit 4.13.2
