# Icewafl - Documentation Context

## 1. Tong quan

**Paper:** Icewafl: A Configurable Data Stream Polluter (EDBT 2025)
**Repository:** `/home/dtl/Documents/thesis/base/Icewafl/`
**Language:** Python (Built on Apache Flink 1.17.0)
**GitHub:** https://github.com/chri-schi/Icewafl

## 2. Muc tieu cua Paper

Tạo **benchmark datasets** cho data quality bằng cách inject errors vào data streams:
- **Data polluter**: Inject errors vào existing data
- Khác với **data synthesizer**: Tạo data từ đầu
- Mục đích: Đánh giá DQ tools và ML models

## 3. Bai toan goc

Khi đánh giá DQ tools hoặc ML models:
1. Cần **ground truth** - biết trước có bao nhiêu lỗi
2. Cần **temporal errors** - lỗi theo thời gian (không có trong static tools)
3. Cần **reproducible** - có thể lặp lại

→ Icewafl giải quyết bằng cách pollute data có kiểm soát

## 4. Error Types

### 4.1 Static Errors (Không phụ thuộc thời gian)

| Error Type | Description | Implementation |
|-----------|-------------|----------------|
| **Gaussian Noise** | Thêm noise Gaussian vào giá trị | `gaussian_noise_attribute_polluter.py` |
| **Scaled by Factor** | Nhân giá trị với hệ số | `multiply_with_constant_attribute_polluter.py` |
| **Incorrect Category** | Thay giá trị bằng giá trị sai | `replace_value_attribute_polluter.py` |
| **Missing Value** | Đặt giá trị thành NULL | `semi_empty_tuple_attribute_polluter.py` |

### 4.2 Temporal Errors (Theo thời gian)

| Error Type | Description | Implementation | Status |
|-----------|-------------|----------------|--------|
| **Delayed Tuple** | Tuple đến muộn | `delay_tuple_stream_temporal_polluter.py` | ✅ Implemented |
| **Frozen Value** | Giá trị đứng yên theo thời gian | NOT FOUND | ❌ Missing |
| **Timestamp Error** | Timestamp bị sai | NOT FOUND | ❌ Missing |

### 4.3 Derived Temporal Errors

Static errors + temporal patterns:
```python
# Ví dụ: Noise tăng dần theo thời gian
noise_factor = 0.25 * cos(π/12 * hour) + 0.25  # 0-0.5 probability
```

## 5. Pollution Model

### 5.1 Polluter Definition

```python
class Polluter:
    def __init__(self, error_function, condition, target_attributes):
        # error_function: hàm tạo lỗi
        # condition: điều kiện kích hoạt
        # target_attributes: thuộc tính bị ảnh hưởng
```

### 5.2 Condition Types

| Condition | Description | File |
|-----------|-------------|------|
| **Probability** | Ngẫu nhiên với xác suất | `probability_condition.py` |
| **Function** | Phụ thuộc giá trị tuple | `function_condition.py` |
| **Temporal** | Phụ thuộc thời gian | `cosine_temporal_condition.py` |
| **Composite** | AND/OR của nhiều conditions | `composite_condition.py` |
| **Mixed** | Probability + Function | `mixed_condition.py` |

### 5.3 Composite Polluters

```python
class SoftwareUpdatePolluter(CompositeAttributePolluter):
    """
    Composite polluter cho software update scenario:
    - Distance: km → cm
    - CaloriesBurned: precision = 2
    - BPM: > 100 → NULL (20%)
    """
    def __init__(self):
        super().__init__([
            UnitConversionPolluter("Distance", multiplier=100),
            PrecisionPolluter("CaloriesBurned", decimals=2),
            CompositeAttributePolluter([
                ReplacePolluter("BPM", value=0),
                NullPolluter("BPM", probability=0.2)
            ])
        ])
```

## 6. Integration Scenarios

### 6.1 Stream Splitting
```python
# Tách stream theo sub-pipelines
polluter_builder.split(num_substreams=2)

# Mỗi sub-stream có polluters riêng
polluter_builder.for_substream(0).add(GaussianNoise("temperature"))
polluter_builder.for_substream(1).add(MissingValue("humidity"))
```

### 6.2 Stream Merging
```python
# Merge sau khi pollute
polluter_builder.merge(substream_ids=[0, 1])
```

## 7. Flink Implementation

### 7.1 Architecture
```
CSV/JSON → Source → Polluter Operators → Sink
```

### 7.2 Key Classes

| Class | Description |
|-------|-------------|
| `PolluterBuilder` | Build polluter pipeline |
| `StreamPolluter` | Non-keyed stream polluter |
| `KeyedStreamPolluter` | Keyed stream polluter |
| `AttributePolluter` | Pollute attributes |

### 7.3 Flink Operations

```python
# Tạo Flink DataStream
stream = env.from_source(...)

# Apply polluters
polluted_stream = PolluterBuilder(polluter).build(stream)

# Output với ground truth
clean_stream, polluted_stream, ground_truth = polluted_stream
```

## 8. Loi moi can bo sung (theo WAVES)

### 8.1 Missing Temporal Errors
- ❌ **Frozen Value**: Giá trị "đóng băng" theo thời gian
- ❌ **Timestamp Error**: Thay đổi timestamp của tuple

### 8.2 Suggested Implementation

```python
class FrozenValuePolluter(StreamPolluter):
    """
    Giá trị không thay đổi trong khoảng thời gian,
    sau đó được cập nhật đột ngột.
    
    Ví dụ: Sensor đóng băng ở 25°C trong 30 phút,
    sau đó nhảy lên 35°C
    """
    def __init__(self, attribute, freeze_duration_ms, frozen_value):
        self._freeze_duration = freeze_duration_ms
        self._frozen_value = frozen_value
        self._last_update_time = {}
        
    def process_element(self, value, ctx):
        current_time = ctx.timestamp()
        last_time = self._last_update_time.get(value['id'], 0)
        
        if current_time - last_time > self._freeze_duration:
            # Thoát khỏi trạng thái đóng băng
            self._last_update_time[value['id']] = current_time
            return value
        else:
            # Vẫn đóng băng
            value[self._attribute] = self._frozen_value
            return value


class TimestampErrorPolluter(StreamPolluter):
    """
    Thay đổi timestamp của tuple.
    
    Ví dụ: Sensor bị trễ 1 giờ
    """
    def __init__(self, timestamp_attribute, delay_ms):
        self._delay = delay_ms
        
    def process_element(self, value, ctx):
        # Thay đổi timestamp
        value[self._timestamp_attr] = value[self._timestamp_attr] + self._delay
        
        # Emit với timestamp mới
        ctx.emit_with_timestamp(value, value[self._timestamp_attr])
        return value
```

## 9. Evaluation Results

### 9.1 DQ Tool Evaluation (Great Expectations)
| Scenario | Expected Errors | Measured Errors |
|-----------|----------------|-----------------|
| Random temporal (null values) | 259.6 avg | 259.6 avg |
| Software update (BPM=0) | 26.4 | 28 |
| Bad network (delayed) | 17.6 | 17.02 |

### 9.2 Forecasting Robustness
| Method | Noise Robustness | Scale Error Robustness |
|--------|------------------|------------------------|
| ARIMA | Low | Medium |
| Holt-Winters | Low | Medium |
| ARIMAX | **High** | Medium |

## 10. Noi dung trong Repository

```
Icewafl/
├── src/
│   ├── Builder/
│   │   └── polluter_builder.py
│   ├── Conditions/
│   │   ├── polluter_condition.py
│   │   ├── probability_condition.py
│   │   ├── function_condition.py
│   │   ├── composite_condition.py
│   │   ├── mixed_condition.py
│   │   └── temporal/
│   │       ├── cosine_temporal_condition.py
│   │       └── defaul_temporal_condition.py
│   ├── Polluters/
│   │   ├── StreamPolluters/
│   │   │   ├── stream_polluter.py
│   │   │   ├── delay_tuple_stream_temporal_polluter.py
│   │   │   └── duplicate_tuple_polluter.py
│   │   ├── KeyedStreamPolluters/
│   │   │   └── keyed_stream_polluter.py
│   │   └── AttributePolluters/
│   │       ├── gaussian_noise_attribute_polluter.py
│   │       ├── replace_value_attribute_polluter.py
│   │       ├── semi_empty_tuple_attribute_polluter.py
│   │       └── composite_attribute_polluter.py
│   └── Utils/
│       └── sort_keyed_stream.py
├── eval_*.py                    # Evaluation scripts
├── prepare_evaluation.py
└── readme.md
```

## 11. Limitations

1. **Không có Frozen Value polluter**
2. **Không có Timestamp Error polluter** 
3. **Chỉ Python** - Paper đề cập Java nhưng không implement
4. **Chưa test đầy đủ** overlapping sub-streams
