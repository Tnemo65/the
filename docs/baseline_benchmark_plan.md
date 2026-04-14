# WAVES — Baseline Benchmark Plan

> Kế hoạch benchmark để so trực tiếp với baseline chính `Ada-Context` và các benchmark phụ để chứng minh phần mở rộng của WAVES.

---

## 1. Mục tiêu

- **Baseline chính:** `paper_to_baseline/s10618-025-01095-6.pdf`
  - **Ada-Context: adaptive context-aware grid-based approach for curation of data streams**
  - Venue: *Data Mining and Knowledge Discovery* (2025)
- **Nguyên tắc claim**
  - Chỉ nói **“hơn Ada-Context”** ở những benchmark mà paper Ada-Context đã công bố kết quả.
  - Những benchmark mở rộng của WAVES chỉ dùng để nói **“WAVES đánh giá được những khía cạnh ngoài phạm vi Ada-Context”**.

---

## 2. Biến thể WAVES cần chạy

| Biến thể | Mục tiêu |
|---|---|
| `WAVES-Context` | So trực tiếp với Ada-Context trên context-aware quality assessment/curation |
| `WAVES-Full` | Chứng minh phần mở rộng của WAVES: DC checking, watermark, late handling, retraction |

**Quy tắc dùng biến thể**
- `WAVES-Context` dùng cho **benchmark chính** với Ada-Context.
- `WAVES-Full` dùng cho **benchmark phụ / benchmark mở rộng**.

---

## 3. Benchmark chính: so trực tiếp với published result của Ada-Context

| Benchmark | Dataset cần dùng | Setup cần bám paper | Metric cần đo | Published Ada-Context result cần vượt | Biến thể WAVES cần chạy | Điều kiện thắng tối thiểu | Claim hợp lệ |
|---|---|---|---|---|---|---|---|
| **Main-1: Traffic scenario** | Chicago Traffic Tracker, Chicago Traffic Crashes, Chicago Park District Event Permits, Chicago Weather, Chicago Crimes, Park Location, Chicago Segments | `context key = latitude + longitude`, online stream, rolling-window validation, `window = 5 giây`, `train/test = 80/20`, inject `5%` poor-quality data | `accuracy`, `precision`, `recall`, `F-score`, `cleansing accuracy`, `runtime` | `CAg = 56.396% accuracy`, `SemCA = 74.831%`, `CA = 81.496%`, `Ada-Context = 91.846%`; runtime của Ada-Context nhanh khoảng `1/20` so với `CA` | `WAVES-Context` | Ưu tiên: `accuracy > 91.846%`; nếu không, thì `accuracy >= 91.846%` và runtime thấp hơn bản tái lập Ada-Context trên cùng máy | “Trên traffic benchmark của Ada-Context, WAVES-Context đạt X so với 91.846% của Ada-Context” |
| **Main-2: Stroke scenario** | Stroke Predictions dataset từ Kaggle | Chạy đúng bài toán classification như paper | `accuracy`, `precision`, `recall`, `F-score` | `accuracy = 82%`, `precision = 88.5%`, `recall = 81%`, `F-score = 85%` | `WAVES-Context` | Vượt cả 4 metric nếu có thể; tối thiểu `accuracy > 82%` và `F-score > 85%` | “Trên stroke benchmark của Ada-Context, WAVES-Context vượt các chỉ số classification đã công bố” |

---

## 4. Tóm tắt Baseline Benchmark — WAVES vs Ada-Context

### 4.1. Kết quả Ada-Context đã publish và target của WAVES

| Metric | Ada-Context đạt được | WAVES cần đạt |
|---|---|---|
| Accuracy (traffic) | `91.846%` | `>= 91.846%` |
| Precision (so với 7 baselines) | Cao nhất trong biểu đồ paper | Cao nhất |
| Recall (so với 7 baselines) | Cao nhất trong biểu đồ paper | Cao nhất |
| F-score (so với 7 baselines) | Cao nhất trong biểu đồ paper | Cao nhất |
| Curation accuracy | Khoảng `83%` | `>= 83%` |
| Runtime (per 5s window) | Khoảng `1.6s` | `<= 1.6s` |
| Runtime speedup vs CA | Khoảng `20x` | `>= 20x` |

**Ghi chú**
- Các số `91.846%`, `83%`, `1.6s`, `20x` là mốc đọc từ nội dung/thí nghiệm paper.
- Các ô “Cao nhất” là cách tóm tắt từ biểu đồ so sánh của paper; khi viết paper WAVES, cần thay bằng số đo head-to-head thực tế từ lần tái lập benchmark.

### 4.2. Dataset để so sánh với Ada-Context

Ta dùng đúng dataset của Ada-Context, không đổi benchmark gốc.

| Dataset | Chi tiết | Dùng cho |
|---|---|---|
| Chicago Traffic Tracker | `119M` records, `22` features, enrich thêm weather/crimes/events | Scenario 1 (chính) |
| Stroke Prediction | `172K` records, `22` features, internal context only | Scenario 2 (phụ) |

**Nguồn public do tác giả paper công bố**
- https://github.com/mostafamirzaie/Traffic_Data_Quality

### 4.3. Cách đảm bảo WAVES vượt Ada-Context

#### Nhóm A: cùng task, cùng metric, so sánh trực tiếp được

| Metric | Ada-Context | WAVES cách làm |
|---|---|---|
| Accuracy | `91.846%` | Chạy WAVES trên Chicago Traffic, đo accuracy trên cùng injected faults |
| Precision | Cao nhất | Đo precision trên cùng injected faults |
| Recall | Cao nhất | Đo recall trên cùng injected faults |
| F-score | Cao nhất | Đo F1 trên cùng injected faults |
| Runtime | `~1.6s/window` | Đo runtime per window và throughput `events/s` trên cùng setup |

**Fault injection phải giữ giống baseline**
- Inject `5%` poor-quality data.
- Các loại lỗi: `inaccurate`, `invalid`, `incomplete`.

#### Nhóm B: task mới, metric mới hoàn toàn, Ada-Context không có

| Metric | Ada-Context | WAVES |
|---|---|---|
| DC Violation F1 / Precision / Recall | Không có | Có |
| Retraction Rate | Không có | Có |
| False Alert Rate under drift | Không đo | Có |

**Ghi chú**
- Đây là **contribution mở rộng** của WAVES, không phải benchmark đối đầu trực tiếp với published result của Ada-Context.
- Không dùng các hàng này để viết claim kiểu “vượt Ada-Context ở metric X” vì paper baseline không công bố metric tương ứng.

### 4.4. Benchmark chính (Primary)

**Task**
- Giám sát chất lượng trên Chicago Traffic Tracker.

**Data**
- `119M` records + external context (`weather`, `crime`, `events`).

**Fault injection**
- `5%` poor-quality data, giữ cùng protocol với Ada-Context.

**Baseline setup**
- `Ada-Context`: Grid + ML regression + multi-level adaptive model.
- `WAVES-Context`: context-aware scoring/bucketing để so trực tiếp.
- `WAVES-Full`: KD-Tree + EMA + watermark + retraction, dùng cho benchmark mở rộng.

**So sánh ở benchmark chính**
- `accuracy`, `precision`, `recall`, `F1` trên cùng injected faults
- `runtime per window`
- `throughput (events/s)` như metric bổ sung của WAVES
- `retraction rate` như metric mở rộng riêng của WAVES
- `false alert rate under concept drift` như metric mở rộng riêng của WAVES

### 4.5. Benchmark phụ (Secondary) — Ablation studies

| # | Benchmark phụ | Mục đích |
|---|---|---|
| B1 | `NL-Stream (O(N^2))` | Chứng minh bottleneck của nested-loop; WAVES phải nhanh hơn rõ rệt |
| B2 | `Static-Box-DaQ (EMA OFF)` | Chứng minh EMA có tác dụng |
| B3 | `Buffer-Wait-DaQ (Retraction OFF)` | Chứng minh retraction có tác dụng |
| B4 | `WAVES-SingleRule` vs `WAVES-Full` | Chứng minh shared optimizer có tác dụng khi số luật tăng |
| B5 | Throughput vs `StreamDaQ` | Chứng minh WAVES không chậm hơn StreamDaQ trên DC-heavy tasks |

### 4.6. Tổng kết — 3 con số cần show trong paper

**Claim 1**
- `WAVES-Context` hoặc `WAVES-Full` đạt `F1/accuracy >= 91.846%` trên Chicago Traffic, tức là ngang hoặc hơn Ada-Context trên benchmark chính.
- Đồng thời báo thêm `DC Violation F1` như phần mở rộng mà Ada-Context không có.

**Claim 2**
- WAVES báo được `Retraction Rate` và `False Alert Rate under drift`, là hai metric ngoài phạm vi paper Ada-Context.

**Claim 3**
- WAVES báo `throughput (events/s)` và `runtime per window` trên cùng benchmark, để chứng minh không đánh đổi semantics lấy hiệu năng.

**Lưu ý an toàn khi viết paper**
- Các ngưỡng như `Retraction Rate = 3-8%`, `False Alert Rate < 5%`, hay `throughput >= 1000 events/s` hiện chỉ nên xem là **target nội bộ**, không được ghi như fact trước khi có số đo thực nghiệm.

---

## 5. Cách triển khai để có cơ hội hơn Ada-Context

| Hạng mục nâng cấp | Làm gì trong WAVES | Lý do |
|---|---|---|
| **External context ingestion** | Thêm context streams ngoài nguồn chính vào `waves/ingestion` và enrich theo `context_key` | Ada-Context mạnh ở internal + external context; nếu không thêm lớp này thì benchmark trực tiếp sẽ yếu |
| **Context-aware scoring** | Thêm `context bucket` / `context-aware pane statistics` trong `waves/logical_engine` | Để thay global threshold bằng adaptive threshold theo ngữ cảnh |
| **Parent fallback** | Khi bucket ít dữ liệu, fallback lên mức cha / context rộng hơn | Bắt đúng tinh thần adaptive multi-level của Ada-Context nhưng vẫn giữ implement gọn |
| **Tách benchmark chính khỏi benchmark mở rộng** | Không bật toàn bộ DC + retraction trong benchmark chính nếu nó làm tăng latency không cần thiết | So trực tiếp với Ada-Context phải công bằng, đúng lớp bài toán context-aware assessment |

---

## 6. Benchmark phụ: dùng để chứng minh phần mở rộng của WAVES

| Benchmark phụ | Dataset / nguồn | Metric | Biến thể WAVES | Mục đích |
|---|---|---|---|---|
| **Aux-1: Late / out-of-order** | Dataset traffic hoặc benchmark nội bộ có inject late events | `precision`, `recall`, `F1 after retraction`, `detection latency`, `retraction rate` | `WAVES-Full` | Chứng minh WAVES xử lý được phần Ada-Context không có: watermark, late data, retraction |
| **Aux-2: Temporal error benchmark** | Dùng `Icewafl` để inject delayed tuples, temporally increasing noise, scale errors, composite temporal scenarios | `precision`, `recall`, `F1`, `runtime` | `WAVES-Full` | Chứng minh WAVES bền vững với temporal errors thực thụ |
| **Aux-3: DC benchmark** | Bộ benchmark DC1–DC3 của WAVES | `precision`, `recall`, `F1`, `throughput`, `P99 latency`, `memory footprint` | `WAVES-Full` | Chứng minh lợi thế của Rapidash + Weever so với baseline context-aware thuần scoring |
| **Aux-4: Rule scaling** | Tăng số luật DC từ ít đến nhiều | `throughput`, `P99 latency`, `RAM` | `WAVES-Full` | Chứng minh shared indexing / batched traversal |
| **Aux-5: System benchmark** | Cùng luồng dữ liệu nhưng tăng tốc độ input / kích thước window | `throughput`, `avg latency`, `P99 latency`, `memory` | `WAVES-Full` | Chứng minh WAVES vẫn giữ được streaming semantics dưới tải cao |

---

## 7. Cách viết claim trong paper

### Claim được phép

- “Trên traffic scenario của Ada-Context, `WAVES-Context` đạt `X` accuracy so với `91.846%` của Ada-Context.”
- “Trên stroke scenario của Ada-Context, `WAVES-Context` đạt `X/Y/Z/W` so với `82/88.5/81/85`.”
- “Trên các benchmark mở rộng về `late/out-of-order`, `retraction`, và `DC violations`, WAVES đánh giá được những khía cạnh ngoài phạm vi của Ada-Context.”

### Claim không được phép

- “WAVES thắng Ada-Context về watermark/retraction” nếu Ada-Context không công bố metric đó.
- “WAVES tốt hơn toàn diện” nếu chỉ mới thắng ở benchmark phụ ngoài phạm vi paper baseline.

---

## 8. Chốt hành động

1. Tái dựng **Traffic scenario** của Ada-Context.
2. Tái dựng **Stroke scenario** của Ada-Context.
3. Chạy `WAVES-Context` trên cả 2 benchmark chính.
4. Sau đó chạy `WAVES-Full` trên benchmark phụ để mở rộng claim.

---

---

## 10. WAVES Module Analysis — NEW vs PORTED

> Phần này phân tích codebase WAVES để xác định đâu là **PORT** từ base paper, đâu là **NEW** do WAVES viết. Căn cứ để xác định chiến lược benchmark hợp lệ.

### 10.1. Tổng quan loại module

| Module | File(s) | Loại | Base paper | Chi tiết |
|--------|---------|------|-----------|---------|
| **Rapidash kd-tree** | `waves/rapidash/kdtree.py` | **PORT** | Rapidash (VLDB 2021) | Bulk-load kd-tree, faithful port |
| **Rapidash traversal** | `waves/rapidash/traversal.py` | **PORT+** | Rapidash | Batched by `rule_group` — đây là **shared indexing** |
| **Weever PaneForest** | `waves/wever/pane_forest.py` | **PORT** | Weever (SIGMOD 2012) | Pane-based forest, simplified (dict thay B-Tree) |
| **Pipeline orchestration** | `waves/pipeline/pipeline.py` | **NEW** | — | Điều phối 11 module end-to-end |
| **WatermarkClock** | `waves/windowing/watermark.py` | **NEW** | — | Non-blocking event-time clock |
| **Shared Rule Optimizer** | `waves/optimizer/*.py` | **NEW** | — | Greedy grouping + dimension cap + infinite padding |
| **EMA + ElasticBox** | `waves/logical_engine/engine.py` | **NEW** | — | Box động theo σ, không có trong paper nào |
| **Tombstone + Retraction** | `waves/tombstone/*.py`, `waves/decision/decision.py` | **NEW** | Stream DaQ | Pane-scoped O(1) tombstones |
| **Late Handler** | `waves/late_handler/handler.py` | **NEW** | Stream DaQ | Retract false-positive + re-check |
| **Windowing + EventStore + DQ + Output** | `waves/windowing/`, `waves/store/`, `waves/basic_dq/`, `waves/output/` | **NEW** | — | Infrastructure WAVES |

### 10.2. Ba điểm MỚI hoàn toàn của WAVES (không có trong base paper nào)

Đây là **contribution thật sự** của WAVES — mỗi điểm có thể benchmark bằng ablation trên cùng dataset.

#### [NEW-1] Shared Rule Optimizer

**Mô tả:** WAVES group các DC rules có chung equality columns vào cùng một kd-tree traversal. Thay vì K rules → K lần traverse cây, WAVES traverse 1 lần cho N rules cùng group.

**Cơ chế:** `optimizer/grouper.py` — `GreedyRuleGrouper` group theo `tuple(sorted(equality_cols))`, sau đó `build_active_boxes()` gán infinite padding cho dimensions không dùng.

**Rapidash gốc:** Mỗi DC → traverse cây riêng → O(K × N log N)  
**WAVES:** Group rules → traverse chung → O(N log N + overhead group)  
**Benchmark:** So `WAVES-Shared` vs `WAVES-Unshared` (1 tree per DC) trên NYC Taxi, K = 1, 10, 50, 100

#### [NEW-2] EMA + ElasticBox

**Mô tả:** Thay vì static padding cố định (như Rapidash), WAVES dùng EMA để track mean và variance theo thời gian, sau đó tính padding = `k × σ`. Box tự co giãn khi concept drift xảy ra.

**Cơ chế:** `logical_engine/engine.py` — `LogicalEngine.process_event()` update EMA stats (Welford online), sau đó `ElasticBox` tính `padded_bounds` dựa trên `max(min(k·σ, delta_max), delta_min)`.

**Base papers (Rapidash/Stream DaQ):** Static box → concept drift → false positive  
**WAVES:** Adaptive box → tự co giãn → giảm false positive  
**Benchmark:** So `WAVES-EMA` vs `WAVES-StaticBox` trên dataset có injected drift

#### [NEW-3] Late Event + Retraction + Tombstone

**Mô tả:** Khi event đến muộn, WAVES retract các PROVISIONAL alerts liên quan, gắn event ID vào pane-scoped tombstone filter, rồi re-check pane để tìm violations mới.

**Cơ chế:**
- `decision/decision.py` — `retract_alert()` marks alert RETRACTED
- `tombstone/filter.py` — `TombstoneManager` tạo O(1) filter per pane
- `late_handler/handler.py` — `handle_late_event()` chạy 5-step flow: DROP check → find alerts → retract → pane_insert → re-check

**Base paper (Stream DaQ):** Blocking `wait_for_late` — giữ events trong buffer  
**WAVES:** Non-blocking — retract sai, tombstone, re-check  
**Benchmark:** So `WAVES-Full` vs `WAVES-NoRetract` trên dataset với 10%, 20%, 30% late-arriving events

---

## 11. Benchmark Strategy — Cách so sánh HỢP LỆ

### 11.1. Nguyên tắc cốt lõi

```
"Để so sánh công bằng: cùng bài toán, cùng dataset, chỉ thay đổi 1 feature"
```

**KHÔNG** so sánh WAVES vs Rapidash gốc vì:
- Dataset khác nhau (Rapidash: static tabular, WAVES: streaming)
- Bài toán khác nhau (Rapidash: batch DC detection, WAVES: streaming DC monitoring)

**SO SÁNH đúng cách:** WAVES ablation trên cùng dataset, chỉ bật/tắt 1 feature.

### 11.2. Benchmark matrix cho 3 điểm NEW

#### [NEW-1] Shared Rule Optimizer

| Biến thể | Shared Optimizer | Trees | Dataset | K DC rules |
|---|---|---|---|---|
| `WAVES-Shared-K1` | ON | 1 tree per group | NYC Taxi | 1 |
| `WAVES-Shared-K10` | ON | 1 tree per group | NYC Taxi | 10 |
| `WAVES-Shared-K50` | ON | 1 tree per group | NYC Taxi | 50 |
| `WAVES-Shared-K100` | ON | 1 tree per group | NYC Taxi | 100 |
| `WAVES-Unshared-K1` | OFF | 1 tree per DC | NYC Taxi | 1 |
| `WAVES-Unshared-K10` | OFF | 1 tree per DC | NYC Taxi | 10 |
| `WAVES-Unshared-K50` | OFF | 1 tree per DC | NYC Taxi | 50 |
| `WAVES-Unshared-K100` | OFF | 1 tree per DC | NYC Taxi | 100 |

**Metrics:** Throughput (events/s), Memory (MB), P99 latency (ms)  
**Baseline so sánh:** Unshared = ON/OFF ratio trên cùng K  
**Claim hợp lệ:** "Shared Rule Optimizer cải thiện throughput X lần với K DC rules so với 1-tree-per-DC"

#### [NEW-2] EMA + ElasticBox

| Biến thể | EMA | ElasticBox | Dataset | Drift injected |
|---|---|---|---|---|
| `WAVES-EMA` | ON | Adaptive (σ×k) | NYC Taxi + drift | Có |
| `WAVES-StaticBox` | OFF | Fixed padding | NYC Taxi + drift | Có |

**Metrics:** Precision, Recall, F1, False Alert Rate  
**Baseline so sánh:** StaticBox = điểm thắng của EMA trên cùng drifted dataset  
**Claim hợp lệ:** "EMA ElasticBox cải thiện F1 Y% trong điều kiện concept drift so với static padding"

#### [NEW-3] Late Event + Retraction

| Biến thể | Retraction | Tombstone | Late% | Dataset |
|---|---|---|---|---|
| `WAVES-Full` | ON | ON | 10%, 20%, 30% | NYC Taxi |
| `WAVES-NoRetract` | OFF | OFF | 10%, 20%, 30% | NYC Taxi |

**Metrics:** Precision, Recall, F1 (after retraction), False Positive Rate, Retraction Rate  
**Baseline so sánh:** NoRetract = điểm thắng của retraction trên cùng late%  
**Claim hợp lệ:** "Retraction mechanism cải thiện F1 Z% khi có 20% late-arriving events"

### 11.3. Baseline end-to-end (không có paper so sánh)

| Biến thể | Mô tả | Mục đích |
|---|---|---|
| `NL-Stream` | O(N²) nested-loop, không kd-tree, không pane | Prove bottleneck |
| `WAVES-Single` | Mỗi DC 1 cây riêng, không shared optimizer | Prove shared optimizer value |
| `WAVES-Buffer` | Bật watermark, không retraction | Prove retraction value |
| `WAVES-Full` | Tất cả bật | End-to-end performance |

---

## 12. WAVES vs Base Papers — Phạm vi so sánh hợp lệ

### 12.1. Không so sánh trực tiếp được

| Base paper | Lý do |
|---|---|
| Rapidash | Batch static data, không streaming, không EMA, không retraction |
| Weever | Incremental index nhưng không DC checking, không EMA |
| Stream DaQ | Basic DQ checks, không kd-tree, không shared optimizer |
| Ada-Context | Grid + ML regression, không streaming, không kd-tree, không DC monitoring |

**Giải thích:** Mỗi paper giải quyết 1 sub-problem; WAVES tích hợp để giải quyết bài toán end-to-end. Dataset, bài toán, và evaluation setup khác nhau hoàn toàn → không head-to-head được.

### 12.2. So sánh gián tiếp được

| Cách | Chi tiết |
|---|---|
| **Ablation on WAVES** | Bật/tắt 1 module → measure impact (Section 11) |
| **Code review** | WAVES port đúng Rapidash kd-tree (Section 10.1), điểm mới là integration + optimizer + EMA + retraction |
| **Scalability claim** | WAVES-Shared với 100 DC rules vs literature đã công bố (Rapidash: 84× faster than nested-loop) |

### 12.3. Phân biệt contribution trong paper

```
WAVES paper structure:

Phần 1 — Problem: Streaming DC Monitoring with Concept Drift + Late Data
  → Không có paper nào cover đầy đủ bài toán này

Phần 2 — Approach: WAVES architecture (integration of modules + 3 new contributions)
  → [NEW-1] Shared Rule Optimizer
  → [NEW-2] EMA + ElasticBox
  → [NEW-3] Late Event + Retraction + Tombstone

Phần 3 — Experiments:
  → Ablation studies (Section 11): prove each NEW contribution
  → System benchmark (Aux-1..5): prove WAVES handles full scenario
  → (Optional) Ada-Context comparison: if reproduce được

Phần 4 — Related Work: discuss how WAVES relates to Rapidash, Weever, Stream DaQ, Ada-Context
```

---

## 13. Ablation Variants — Phiên bản đầy đủ (bổ sung Section 4.5)

### 13.1. Ablation cho 3 điểm NEW

| Ablation ID | Biến thể | Feature bật | Feature tắt | Module bị ảnh hưởng | Metrics |
|---|---|---|---|---|---|
| **A1-Shared** | `WAVES-Shared` | Shared Rule Optimizer | — | optimizer, rapidash traversal | Throughput, Memory, P99 (K=1..100) |
| **A1-Unshared** | `WAVES-Unshared` | — | Shared Rule Optimizer | optimizer, rapidash traversal | Throughput, Memory, P99 (K=1..100) |
| **A2-EMA** | `WAVES-EMA` | EMA + ElasticBox | — | logical_engine | Precision, Recall, F1 (drift) |
| **A2-Static** | `WAVES-StaticBox` | — | EMA + ElasticBox | logical_engine | Precision, Recall, F1 (drift) |
| **A3-Retract** | `WAVES-Retract` | Retraction + Tombstone | — | decision, tombstone, late_handler | Precision, Recall, F1 (late 10/20/30%) |
| **A3-NoRetract** | `WAVES-NoRetract` | — | Retraction + Tombstone | decision, tombstone, late_handler | Precision, Recall, F1 (late 10/20/30%) |

### 13.2. Integration + System Ablations

| Ablation ID | Biến thể | Mô tả | Metrics |
|---|---|---|---|
| **A4-NL** | `NL-Stream` | O(N²) nested-loop, không kd-tree, không pane, không EMA, không retraction | Throughput, P99 (small dataset) |
| **A5-Single** | `WAVES-Single` | 1 kd-tree per DC, không shared optimizer, không EMA, có retraction | Throughput, Memory (K=1..100) |
| **A6-Buffer** | `WAVES-Buffer` | Shared optimizer, EMA, watermark, không retraction | Precision, Recall, F1 (late data) |
| **A7-Full** | `WAVES-Full` | Tất cả bật | All metrics |

### 13.3. Mapping RQ → Ablation

| RQ | Ablation(s) | Claim |
|---|---|---|
| **RQ1** (Throughput) | A1-Shared vs A1-Unshared, A4-NL | Shared optimizer + kd-tree vượt nested-loop |
| **RQ2** (EMA + Drift) | A2-EMA vs A2-Static | EMA ElasticBox giữ F1 khi concept drift |
| **RQ3** (Scalability) | A1-Shared vs A1-Unshared (K=1..100) | Shared optimizer scales to 100+ rules |
| **RQ4** (Trade-off) | A5-Single vs A7-Full, A3-Retract vs A3-NoRetract | Retraction + EMA trade-off latency vs accuracy |
| **Extended** (Late data) | A3-Retract vs A3-NoRetract, A6-Buffer | Late event + retraction cải thiện accuracy |

### 13.4. Chiến lược claim theo kết quả thực nghiệm

```
Nếu Shared Optimizer ≥ 5× throughput với K=100:
  → Claim: "Shared Rule Optimizer enables WAVES to scale to 100+ DC rules"

Nếu EMA ElasticBox F1 > StaticBox F1 (có drift):
  → Claim: "EMA ElasticBox maintains F1 under concept drift, reducing false positives by X%"

Nếu Retraction cải thiện F1 khi late > 20%:
  → Claim: "Retraction mechanism improves accuracy by X% when 20% events arrive late"

Nếu TẤT CẢ đều positive:
  → Claim: "WAVES is the first streaming DC monitoring system that combines
    shared spatial indexing, adaptive bounds, and provenance-aware retraction"
```

---

## 14. Nguồn tham chiếu cục bộ

- `paper_to_baseline/s10618-025-01095-6.pdf` — Ada-Context (DMKD 2025)
- `paper_to_baseline/Icewafl.pdf` — Icewafl (EDBT 2025)
- `paper_to_baseline/Rapidash.pdf` — Rapidash (VLDB 2021)
- `paper_to_baseline/2106.03837v2.pdf` — Stream Cleaning (TODS 2021)
- `paper_to_baseline/Weever.pdf` — Weever (SIGMOD 2012)
- `paper_to_baseline/1-s2.0-S0306437924000930-main.pdf` — Stream DaQ (ICDE 2021)
- `base_repo/Rapidash/` — Rapidash code (Java, Maven)
- `base/Icewafl/` — Icewafl code (Python, Flink)
- `[original]stream-DaQ/` — Stream DaQ code (Python)
