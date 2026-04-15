# WAVES Project — Context for Claude Agent (Next Session)

**File này do agent tạo để user đưa cho Claude session mới trên server mạnh.**
**User muốn: cải thiện precision/recall từ ~35%/~50% lên cao hơn (mục tiêu 80%+).**

---

## 1. Project Overview

- **Repo**: `/home/dtl/Documents/the/WAVES/`
- **Language**: Python 3.11 (`./WAVES/.venv/bin/python3`)
- **Data**: `/home/dtl/Documents/the/WAVES/data/benchmark.parquet` (2.7M events NYC Taxi)
- **Ground truth**: `/home/dtl/Documents/the/WAVES/data/ground_truth.jsonl`

## 2. Current Benchmark Results

| Batch | Events | Speed | Precision | Recall | F1 | RAM |
|-------|--------|-------|-----------|--------|-----|-----|
| 1 | 10K | 201 ev/s | 23.3% | 78.7% | 0.36 | 16MB |
| 2 | 50K | 289 ev/s | 31.1% | 53.8% | 0.39 | 57MB |
| 3 | 100K | 305 ev/s | 34.8% | 50.6% | 0.41 | 104MB |

**Recall breakdown (100K):**
- DC1 (Fare-Distance Dominance): 17.7% (296/1677)
- DC2 (Duration Anomaly): 14.4% (130/900)
- DC3 (Toll Anomaly): 16.0% (199/1244)
- Late data: ~65%

**Nguyên nhân recall thấp KHÔNG phải do RAM** — RAM stable, no leak.

---

## 3. Root Causes Đã Xác Định

### Recall thấp — 3 lý do:

**A. Pane scope quá hẹp (HARD LIMIT)**
- DC2/DC3 violations có thể cách nhau **60–300 giây** (tùy pane size)
- Buffer scan hiện tại chỉ check **5 panes gần nhất** (~5 phút)
- Violations ngoài phạm vi → **bỏ sót hoàn toàn**
- Fix đã thử: tăng lên 20 panes → **chạy quá chậm** (stuck, >10 phút cho 10K events)
- **Cần giải pháp khác: location-partitioned traversal**

**B. Không có location-aware indexing (STRUCTURAL)**
- DC2/DC3 require `PULocationID EQUAL PULocationID AND DOLocationID EQUAL DOLocationID`
- Nhưng KD-Tree index theo **trip_duration / tolls_amount** — không phải location
- Traversal phải duyệt TẤT CẢ candidates rồi mới filter bằng predicates
- Violations ở pane khác nhau → không tìm thấy

**C. EMA bounds quá rộng**
- Outliers từ fraud làm tăng sigma → ElasticBox mở rộng
- Kết quả: candidates tăng nhanh nhưng TPs không tăng tương ứng

### Precision thấp — 1 lý do chính:

**Spatial proximity ≠ Violation condition**
- KD-Tree tìm events **gần nhau trong feature space** (distance dimensions)
- DC1 violation là `|dist_s - dist_t| ≤ 0.5 AND fare_s > fare_t`
- KD-Tree trả về 10–50 candidates gần đúng → predicates lọc ra 1 true + 9 false
- **Root cause: không có index trên `|dist_s - dist_t|`** — chỉ có index trên giá trị tuyệt đối

---

## 4. DC Rules (Denial Constraints)

```python
DC1: Fare–Distance Dominance
  Violation: |trip_distance_s - trip_distance_t| ≤ 0.5 AND fare_s > fare_t
  (same distance, higher fare)

DC2: Context-Aware Duration Anomaly
  Violation: PUL_s == PUL_t AND DOL_s == DOL_t AND duration_s > 2.5 × median
  (same route, duration > 2.5× context median + random(0,300))

DC3: Toll Route Anomaly
  Violation: PUL_s == PUL_t AND DOL_s == DOL_t AND tolls_s > tolls_t + 2.0
  (same route, toll discrepancy)
```

---

## 5. Code Architecture (Key Files)

```
waves/pipeline/pipeline.py        # MAIN — WavePipeline orchestrator
  ├─ _traverse_pane_elastic()    # KD-Tree traversal + buffer scan (BOTTLENECK)
  ├─ _evaluate_predicates()       # Predicate evaluation for violations
  ├─ _seal_all_windows()          # Finalize PROVISIONAL → FINAL alerts
  └─ _close_old_panes()           # Close + drop panes

waves/logical_engine/engine.py     # EMA state + ElasticBox builder
  └─ build_elastic_box()          # Uses EMA sigma to pad bounds

waves/weever/pane_forest.py        # Pane-based forest
waves/rapidash/traversal.py       # KD-Tree traversal + point_in_box

waves/decision/decision.py         # Alert decision logic
waves/output/alert_output.py      # AlertEvent output
```

### Key Fixes Đã Applied (trong `pipeline.py`):

1. **`_evaluate_predicates`**: EQUAL operators KHÔNG được invert cho DC2/DC3 (predicate là violation condition, không phải normal case)
2. **`_evaluate_predicates`**: Strip `_s`/`_t` suffix khi lookup column trong event attributes (event chỉ có `trip_distance`, không có `trip_distance_s`)
3. **DC1 predicates**: 2 predicates: `LESS_EQUAL(trip_distance_s ≤ trip_distance_t + 0.5)` + `LESS_EQUAL(fare_amount_s ≤ fare_amount_t)` (inverted = violation when fare_s > fare_t)
4. **Buffer scan**: Scan last 5 panes (active + recently closed) thay vì chỉ current pane
5. **Alert finalization**: `_seal_all_windows()` emit FINAL alerts to sink

---

## 6. Các Solutions Đề Xuất (Để Implement)

### Solution A — Location-Partitioned KD-Tree (RECOMMENDED)

```
Thay vì 1 KD-Tree global, dùng HashMap:
  route_key = (PULocationID, DOLocationID)  →  KD-Tree của các events cùng route

Khi event S được xử lý:
  1. Xác định route_key của S
  2. Lấy KD-Tree cho route_key đó
  3. Traverse tree với bounds phù hợp
  4. Emit violations

→ Violations cùng route chỉ tìm trong 1 subtree
→ Recall + precision đều tăng mạnh
→ Khắc phục root cause B ở trên
```

**Effort ước tính**: 4–6 giờ
**Expected precision**: ~60–70%
**Expected recall**: ~65–75%

### Solution B — Tighter Box Bounds (Quick Fix)

```
Thay vì dùng EMA bounds cho kdtree traversal,
dùng query-centric bounds quanh query point:
  DC1: |dist_s - dist_t| ≤ 0.5 → dist_t ∈ [dist_s-0.5, dist_s+0.5]
  DC2: duration_t ∈ [duration_s, +inf]
  DC3: tolls_t ∈ [tolls_s, +inf]

Nhưng CẨN THẬN: query-centric bounds đã thử và làm recall tụt từ 50% → 10%
  → Chỉ dùng nếu kết hợp với Solution A
```

**Effort ước tính**: 1–2 giờ
**Rủi ro**: Cao nếu dùng một mình

### Solution C — Incremental Nested-Loop với Cửa Sổ Hẹp

```
Cho mỗi event S:
  1. Tìm T trong same window (không phải pane)
  2. Filter: same-route → check violation
  3. Emit alert

→ 80%+ precision/recall nhưng O(window_size) per event
→ Cần spatial index để giới hạn search space
→ Phù hợp với window nhỏ (60s) và có partition
```

**Effort ước tính**: 8–12 giờ
**Expected**: ~80%+ precision/recall

---

## 7. Cách Chạy Benchmark

### Benchmark nhỏ (trên server):
```bash
cd /home/dtl/Documents/the/WAVES

# 100K events
/home/dtl/Documents/the/WAVES/.venv/bin/python3 << 'PYEOF'
import json, sys, tracemalloc, time
sys.path.insert(0, '/home/dtl/Documents/the/WAVES')
import pandas as pd
from pathlib import Path
import waves._benchmark_patch
from waves.pipeline import PipelineConfig, WavePipeline
from waves.ingestion.schema import parse_and_normalize, ParseError, Schema

NYC_SCHEMA = Schema(
    event_time_field="event_time", event_time_format=None,
    ingestion_time_field="ingestion_time", ingestion_time_format=None,
    partition_key_field="PULocationID",
    required_fields=["trip_distance", "fare_amount"],
    field_types={
        "PULocationID": int, "DOLocationID": int,
        "trip_distance": float, "fare_amount": float,
        "tolls_amount": float, "trip_duration": float, "total_amount": float,
    },
)

gt = {}
with open('data/ground_truth.jsonl') as f:
    for line in f:
        r = json.loads(line)
        gt[r['event_id']] = r

df = pd.read_parquet('data/benchmark.parquet')
dc_rules = json.loads(Path('configs/dc_rules.json').read_text())['dc_rules']
config = PipelineConfig(
    window_width_seconds=600.0, slide_step_seconds=120.0, pane_size_seconds=60.0,
    k_max=5, alpha_ema=0.05, ema_k=3.0, ema_delta_min=1.0, ema_delta_max=50.0,
    ema_warmup=50, alert_ttl_seconds=3600.0, wait_for_late_seconds=300.0,
)
pipeline = WavePipeline(config)
pipeline.load_dc_rules(dc_rules)

tracemalloc.start()
t0 = time.perf_counter()
events_processed = 0
for i, row in enumerate(df.itertuples(index=False)):
    raw = row._asdict()
    try:
        event = parse_and_normalize(raw, NYC_SCHEMA)
    except ParseError:
        continue
    pipeline.process(event)
    events_processed += 1
    if events_processed >= 100000:
        break

_ = pipeline._seal_all_windows()
elapsed = time.perf_counter() - t0
current, peak = tracemalloc.get_traced_memory()
print(f"Speed: {events_processed/elapsed:.0f} ev/s, RAM: {current/1024/1024:.0f}MB")

# Accuracy
eids = set(df.head(100000)['event_id'].values)
gt_filtered = {eid: r for eid, r in gt.items() if eid in eids}
dc_gt = {'dc1': 0, 'dc2': 0, 'dc3': 0, 'late': 0}
for eid, r in gt_filtered.items():
    nt = r.get('noise_type', '')
    if nt in dc_gt:
        dc_gt[nt] += 1

final_eids = set()
for evt in pipeline._output._alert_history:
    if evt.event_type == 'final':
        final_eids.add(evt.event_id)

tp_dc = {k: 0 for k in dc_gt}
for eid, r in gt_filtered.items():
    if eid in final_eids:
        nt = r.get('noise_type', '')
        if nt in tp_dc:
            tp_dc[nt] += 1

total_tp = sum(tp_dc.values())
total_fn = sum(dc_gt.values()) - total_tp
fp = max(0, len(final_eids) - total_tp)
prec = total_tp / (total_tp + fp) if (total_tp + fp) > 0 else 0
rec = total_tp / sum(dc_gt.values()) if sum(dc_gt.values()) > 0 else 0
f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0

print(f"Precision={prec:.2%}  Recall={rec:.2%}  F1={f1:.4f}")
for k in ['dc1', 'dc2', 'dc3']:
    g = dc_gt[k]; t = tp_dc[k]
    print(f"  {k}: {t}/{g} = {t/g*100:.1f}% recall" if g else f"  {k}: 0/0")

tracemalloc.stop()
PYEOF
```

---

## 8. Những Điều KHÔNG ĐƯỢC LÀM

1. **Không tự tạo "simplified" version** — implement phải đúng kiến trúc
2. **Không đổi config ngẫu nhiên** (pane_size, window_width) mà không hiểu impact
3. **Không commit code mà không verify** bằng benchmark 100K
4. **Không dùng dữ liệu giả** thay cho ground truth thật

## 9. Milestones Để Verify

| Milestone | Metric | Threshold |
|-----------|--------|-----------|
| M1 | Precision | ≥ 50% |
| M2 | Recall (DC1+DC2+DC3) | ≥ 65% |
| M3 | Throughput | ≥ 200 ev/s |
| M4 | RAM (100K) | ≤ 200MB |
| M5 | Full 2.7M chạy thành công | ✓ |

**Mỗi lần modify code → chạy M1–M4 trước khi commit.**

---

## 10. Reference Documents

- `docs/project.md` — Project changelog
- `docs/MASTER_AGENT_CHECKLIST.md` — Full checklist
- `WAVES/waves/pipeline/pipeline.py` — Main code (đã fix nhiều bugs)
- `WAVES/waves/benchmark_runner.py` — Benchmark runner reference
