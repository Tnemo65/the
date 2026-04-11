# Stream DaQ - Documentation Context

## 1. Tong quan

**Paper:** Stream DaQ: Stream-First Data Quality Monitoring
**Repository:** `/home/dtl/Documents/thesis/base/stream-DaQ/`
**Language:** Python (Built on Pathway)
**GitHub:** https://github.com/Bilpapster/Stream-DaQ

## 2. Muc tieu cua Paper

Thiết kế một mô hình giám sát chất lượng dữ liệu mới cho **unbounded data streams**:
- Cơ chế chia cửa sổ có thể cấu hình
- Đánh giá ngữ cảnh động
- Đầu ra là **quality meta-stream**

## 3. Cac thanh phan chinh

### 3.1 Quality Checks (30+ built-in)

| Category | Checks |
|----------|--------|
| **Tuple-at-a-time** | valid_range, accepted_value_sets, pattern_matching, row-wise_conformance, value_ordering, cross-interval_validation |
| **Window Context** | stream_freshness, dead_stream_detection, frozen_stream_detection, out-of-order_arrival_detection, element_length_statistics, distribution_analysis, window_statistics, volume_monitoring |
| **Aggregation** | distinct_element_counting, uniqueness_validation, heavy_hitters_identification |
| **Completeness** | missing_stream_elements, placeholder_consistency |
| **Relationship** | correlation_analysis |
| **Schema** | schema_validation, data_type_validation |

### 3.2 Windowing

```python
# Tumbling windows
streamdaQ.configure(window=tumbling(duration=timedelta(hours=1)))

# Sliding windows
streamdaQ.configure(window=sliding(hop=timedelta(minutes=1), duration=timedelta(hours=1)))

# Session windows
streamdaQ.configure(window=session(max_gap=timedelta(minutes=30)))
```

### 3.3 Dynamic Context

```python
# Su dung lambda/callable de thay doi threshold theo context
daq.add(dqm.most_frequent('location'), lambda loc: is_tampere_frequent(loc))
```

### 3.4 Quality Meta-Stream

```python
# Output format
meta_stream = daq.watch_out()
# Columns: user_id, window_start, window_end, measure_value, pass/fail
```

## 4. Diem manh

- Hiệu suất vượt trội: **13.8x nhanh hơn** Deequ tren window nho
- Hơn 30 built-in checks
- Python-native, tích hợp tốt với Python ecosystem
- Hỗ trợ Kafka, CSV, JSON, Python pipeline

## 5. Diem yeu

- Không có cơ chế **watermark** để xử lý late arrivals
- Không có **denial constraint checking** phức tạp (chỉ kiểm tra cơ bản)
- Hiệu suất giảm khi sliding windows có overlap > 90%

## 6. Loi moi can bo sung (theo WAVES)

### 6.1 Watermark-Aware Late Data Handling
- Thêm `wait_for_late` parameter trong configure()
- Lưu trữ provisional results
- Retraction mechanism khi late data đến

### 6.2 Denial Constraint Checks (Rapidash integration)
- Tích hợp orthogonal range search
- Kiểm tra ràng buộc logic phức tạp: `NOT (A AND B AND C)`
- Tránh O(N²) brute-force scanning

### 6.3 Incremental Indexing (Weever integration)
- LT-Tree index cho inequality predicates
- Insert/delete tuples nhanh
- Không cần rebuild tree mỗi window

## 7. Vi du usage

```python
from streamdaq import StreamDaQ, DaQMeasures as dqm
from datetime import timedelta

# Configure voi windowing
daq = StreamDaQ().configure(
    window=tumbling(duration=timedelta(hours=1)),
    wait_for_late=timedelta(minutes=5)
)

# Add quality checks
daq.add(dqm.count('id'), "> 10")
daq.add(dqm.distinct_count_approx('category'), ...)

# Monitor output
meta_stream = daq.watch_out()
```

## 8. Dependencies

- Pathway 0.16+
- Python 3.10+
- Kafka (optional)
- Pydantic (for schema validation)

## 9. Noi dung trong Repository

```
stream-DaQ/
├── streamdaq/                 # Core library
│   ├── StreamDaQ.py         # Main class
│   ├── DaQMeasures.py       # 50+ measures
│   ├── Windows.py           # Windowing
│   ├── Task.py              # Multi-task
│   ├── anomaly_detectors/   # Bonus: Anomaly detection
│   └── SchemaValidator.py   # Pydantic validation
├── examples/                 # 9 examples
└── docs/                     # Documentation
```
