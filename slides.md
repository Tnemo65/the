# WAVES Presentation Slides
# Chi tiết nội dung từng slide — Thesis Defense

> Nguồn tổng hợp: `slides_merged.txt`, `modules.tex`, `intro_background.tex`, `method_eval.tex`, `references.bib`
> Last updated: April 2026

---

## PHẦN 1: BỐI CẢNH & KHOẢNG TRỐNG

---

### SLIDE 1 — Tiêu đề

#### Nội dung trình bày

**Tiêu đề chính:**
WAVES: Window-based Adaptive Violation Detection with Elastic Streaming

**Tiêu đề phụ:**
Khung giám sát chất lượng dữ liệu luồng có nhận thức độ trễ bằng Ràng buộc Không gian

**Thông tin:**
- Sinh viên thực hiện: [Tên SV]
- GVHD: [Tên GVHD]
- Thời gian: [Ngày bảo vệ]

**Nền tảng nghiên cứu:**
Stream DaQ | Rapidash | Weever | Watermark (Google Dataflow)

---

### SLIDE 2 — Bối cảnh nghiên cứu

#### Nội dung trình bày

**Tiêu đề:** DQ đã tiến hóa qua ba lớp — nhưng vẫn còn khoảng trống

---

**Ba lớp tiến hóa của Data Quality:**

| Lớp | Mô tả | Đặc điểm | Ví dụ |
|------|--------|-----------|-------|
| **Lớp 1: Static DQ** | Kiểm tra một lần trên dữ liệu đã ổn định | Batch, hậu kiểm, không real-time | Great Expectations, Soda Core |
| **Lớp 2: Incremental DQ** | Cộng dồn trạng thái theo luồng | Cập nhật gia tăng, watermark đơn giản | Stream DaQ, Spark Structured Streaming |
| **Lớp 3: Stream-first DQ** | Giám sát ngay bên trong pipeline | Watermark-aware, delay-sensitive | WAVES hướng tới |

---


**Phần 1 — Batch DQ Tools (Tham khảo cách viết checks)**

| Tool | Link | Checks được hỗ trợ | Giúp ta làm gì |
|------|------|---------------------|-----------------|
| **Great Expectations** | [GitHub](https://github.com/great-expectations/great_expectations) | Null, uniqueness, value range, type, regex, cross-column, row count, completeness, custom Python expectations. Batch scan trên DataFrame. Tạo data docs interactive. | Tham khảo cách viết Expectation DSL. Không dùng trực tiếp (batch-only). |
| **Soda Core** | [GitHub](https://github.com/sodadata/soda-core) | Metric-based checks qua YAML: row count, null%, sum, avg, min/max, distinct count, regex, schema, freshness. Scan SQL trên warehouse. 18+ nguồn. | Tham khảo cách tổ chức metric-based checks. Không dùng trực tiếp (batch-only). |
| **AWS Deequ** | [VLDB 2018](https://dl.acm.org/doi/10.1145/3213881.3228209) | Metrics phân tán trên Spark: completeness, uniqueness, key integrity, row-level constraints. Tự động sinh checks từ schema. Tích hợp AWS Glue, S3, Redshift. | Tham khảo cách auto-generate checks từ schema. Không dùng trực tiếp (batch-only). |
| **Apache Griffin** | [GitHub](https://github.com/apache/griffin) | 30+ built-in checks: accuracy, completeness, timeliness, profiling, uniqueness. Kafka/Kinesis connector. Dựa trên Spark Streaming. | Tham khảo cách kết nối Kafka cho streaming checks. Dự án archived (2018). |

---

**Phần 2 — Streaming Papers (Xây dựng WAVES trực tiếp)**

| Paper | Link | Checks được hỗ trợ | Giúp ta làm gì |
|-------|------|---------------------|-----------------|
| **Stream DaQ** | [arXiv:2506.06147](https://arxiv.org/abs/2506.06147) | 30+ checks trên luồng: null, range, type, regex, count, availability, pattern. Quality meta-stream phát metrics song song dữ liệu. Windowing: tumbling/sliding/session. Xử lý out-of-order nhẹ. | Dùng trực tiếp: runtime pattern, cách tổ chức luồng, windowing, quality meta-stream. |
| **Rapidash** | [arXiv:2309.12436](https://arxiv.org/abs/2309.12436) (VLDB 2024) | Denial constraints (DC): multi-column equality (=, !=), inequality (<, <=, >, >=). KD-Tree + orthogonal range search. Boolean range search. 84x nhanh hơn Facet. | Dùng trực tiếp: DC engine bằng KD-Tree, box dropping, orthogonal range search cho WAVES core. |
| **Weever** | [VLDB 2021](https://dl.acm.org/doi/10.14778/3717755.3717761) / [GitHub](https://github.com/HPI-Information-Systems/Weever) | Denial constraints incremental. LT-Tree (AVL/Red-Black variant). Insert/delete O(log N). Predicate scheduling ưu tiên selectivity thấp. | Dùng trực tiếp: incremental state management, pane-based forest thay LT-Tree cho WAVES. |
| **The Dataflow Model** | [VLDB 2015](https://vldb.org/pvldb/vol8/p1792-Akidau.pdf) | Event-time semantics. Watermark là tín hiệu completeness. Window, trigger, accumulation (discarding/accumulating/retracting). | Dùng trực tiếp: nền tảng watermark, event-time, provisional vs final alerts cho WAVES. |

---

**Phần 3 — Các hướng bổ sung (Xử lý vấn đề riêng)**

| Paper | Link | Checks được hỗ trợ | Giúp ta làm gì |
|-------|------|---------------------|-----------------|
| **Watermarks in Stream Processing** | [VLDB 2017](https://dl.acm.org/doi/10.14778/3476311.3476389) | Cơ sở lý thuyết watermark: event-time, watermark generation, stragglers, watermark latency. Completeness, correctness, .emitOnMissedWatermark. | Dùng trực tiếp: watermark semantics, cách xử lý stragglers, watermark lag. |
| **ProbSlack** | [SIGMOD 2018](https://dl.acm.org/doi/10.1145/3210284.3210293) | Mô hình hóa độ trễ động bằng probabilistic bounds. Tính expected slack, watermark confidence. | Tham khảo: mô hình probabilistic bounds cho watermark. WAVES dùng cấu hình thực nghiệm (fixed wait_for_late). |
| **Icewafl** | [EDBT 2025](https://openproceedings.org/2025/conf/edbt/paper-240.pdf) | Fault injection: inject delayed, out-of-order, drifted events vào stream. Benchmark pipeline B1→B4. Tạo ground truth cho DQ evaluation. | Dùng trực tiếp: benchmark protocol cho WAVES experiments (fault injection pipeline). |
| **Klink** | [SIGMOD 2021](https://doi.org/10.1145/3448016.3452794) | Progress-aware scheduling giảm watermark lag 60%. Root causes: stragglers, network delay, idle sources. | Tham khảo: root causes của watermark lag để cấu hình hệ thống. |
| **RTClean** | [InfoSci 2023](https://export.arxiv.org/pdf/2302.04726v1.pdf) | Real-time cleaning bằng operational FDs. Học FD từ live context, detect và fix anomalies trên luồng. | Tham khảo: cách dùng FDs cho streaming DQ. WAVES tập trung DC (mạnh hơn FD). |
| **Soda AI** | [soda.io](https://www.soda.io/product/soda-ai) | ML anomaly detection trên time-series metrics. 70% ít false positive hơn Prophet. | Tham khảo: ML cho DQ. WAVES dùng EMA (đơn giản, deterministic). |
| **Monte Carlo / Bigeye** | — | ML-driven anomaly detection, lineage tracking, root cause analysis. Giám sát ở mức bảng. | Tham khảo: ML observability. Không dùng trực tiếp (batch-table-level). |
| **LLMatch** | [arXiv:2507.10897](https://arxiv.org/abs/2507.10897) (APWeb 2025) | LLM cho schema matching tự động. Rollup/drilldown detection bằng transformer. | Tham khảo: LLM cho rule discovery. WAVES dùng DC thủ công nhưng tương thích với LLM-generated rules. |

#### Citations

---

### SLIDE 3 — Related Works: 4 trụ cột nền tảng

#### Nội dung trình bày

**Tiêu đề:** Bốn trụ cột nghiên cứu nền tảng

**Bảng so sánh 4 base papers:**

| | Stream DaQ | Rapidash | Weever | Watermark/Dataflow |
|---|---|---|---|---|
| **Ngôn ngữ** | Python | Java | Java | Lý thuyết |
| **Năm** | 2025 | 2024 | 2021 | 2015–2017 |
| **Phương pháp** | Stream-first framework | KD-Tree + orthogonal range search | LT-Tree incremental | Event-time semantics |
| **Điểm mạnh** | 30+ checks, quality meta-stream | 84x nhanh hơn Facet | O(log N) per update | Provisional vs Final alerts |
| **Điểm yếu** | Không DC, không watermark | Static snapshot, rebuild O(N) | Chưa gắn watermark | Chưa tích hợp DC engine |
| **Ý nghĩa với WAVES** | Runtime tư duy, windowing | Lõi phát hiện vi phạm | Incremental state management | Thành phần trung tâm |

**Kết luận thị trường (12 nền tảng):**

| Nền tảng | DC Check | Watermark | Late Data | Incremental | Streaming |
|---|---|---|---|---|---|
| Apache Griffin | ❌ | ❌ | ❌ | ⚠️ | ⚠️ |
| Great Expectations | ❌ | ❌ | ❌ | ❌ | ❌ |
| Soda Core | ❌ | ❌ | ❌ | ❌ | ❌ |
| Spark Struct. Streaming | ❌ | ✅ | ⚠️ | ⚠️ | ✅ |
| AWS Deequ | ❌ | ❌ | ❌ | ⚠️ | ❌ |
| GCP Dataplex | ❌ | ❌ | ❌ | ❌ | ❌ |
| Monte Carlo / Bigeye | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Stream DaQ** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Rapidash** | ✅ | ⚠️ | ⚠️ | ✅ | ✅ |
| **WAVES (đề xuất)** | ✅ | ✅ | ✅ | ✅ | ✅ |

> **Tất cả 12 nền tảng đều THIẾU đồng thời: Stream-first + DC + Watermark + Retraction.**
> WAVES là khung ĐẦU TIÊN tích hợp đủ 4 thành phần.

#### Citations

- Stream DaQ: [arXiv:2506.06147](https://arxiv.org/abs/2506.06147)
- Rapidash: [arXiv:2309.12436](https://arxiv.org/abs/2309.12436) (VLDB 2024)
- Weever: [ACM VLDB 2021](https://dl.acm.org/doi/10.14778/3717755.3717761)
- Watermarks in Stream Processing: [VLDB 2017](https://dl.acm.org/doi/10.14778/3476311.3476389)
- The Dataflow Model: [VLDB 2015](https://vldb.org/pvldb/vol8/p1792-Akidau.pdf)

---

### SLIDE 4 — Research Gaps + Research Questions

#### Nội dung trình bày

**Tiêu đề:** Từ khoảng trống nghiên cứu đến câu hỏi nghiên cứu

---

**Bảng 3 Research Gaps (có trích dẫn):**

| Gap | Mô tả khoa học | Bằng chứng từ tài liệu |
|-----|----------------|------------------------|
| **Gap 1: Chi phí phát hiện DC trên luồng** | Phát hiện vi phạm denial constraints yêu cầu đối chiếu đa bản ghi trong cùng cửa sổ thời gian. Cách tiếp cận brute-force có độ phức tạp O(N²), không scale khi kích thước cửa sổ tăng. Thêm vào đó, đến 95% constraints được tự động suy luận là FALSE khi thiếu ngữ cảnh, dẫn đến chi phí xử lý phí không cần thiết. | VLDB 2025 Martin et al.: "đến 95% constraints được suy luận là FALSE" ([link](https://www.vldb.org/pvldb/vol18/p3477-martin.pdf)) |
| **Gap 2: Luật tĩnh không thích nghi với concept drift** | Các luật kiểm tra được định nghĩa bằng ngưỡng cố định (static thresholds). Khi phân phối dữ liệu thay đổi theo thời gian (kẹt xe, bão tuyết, sự kiện đặc biệt), ngưỡng cố định không phản ánh ngữ cảnh thực tế, dẫn đến tỷ lệ false positive cao bất thường trong thời gian drift. | Rapidash (VLDB 2024): không có cơ chế thích nghi theo context |
| **Gap 3: Thiếu cơ chế provisional–retract trong môi trường watermark** | Trong môi trường streaming thực, dữ liệu đến muộn (late arrivals) và out-of-order arrivals rất phổ biện do stragglers, độ trễ mạng biến thiên, và các partition rỗng không produce. Nếu hệ thống thiếu cơ chế provisional alert và retraction, mọi cảnh báo đều bị coi là chính thức ngay khi phát hiện, dẫn đến false alert tăng mạnh khi watermark bị lag. | Klink (SIGMOD 2021): root causes của watermark lag được xác định rõ — stragglers, network delay, idle sources ([link](https://doi.org/10.1145/3448016.3452794)) |
| **Gap 4: Curse of dimensionality trong box dropping** | Khi số chiều k tăng, hypercube volume tăng theo k, bounding boxes giao với hầu hết query regions, và box dropping trở nên kém hiệu quả — gần O(N) trong không gian cao chiều. Các giải pháp hiện tại (hash partition, k_max cap) chưa được tích hợp trong kiến trúc streaming. | Rapidash (VLDB 2024): đề xuất k_max cap nhưng chưa có incremental streaming adaptation |

---

**Bảng 4 Research Questions (bài toán chung):**

| RQ | Bài toán nghiên cứu | Metrics | Tại sao khó |
|----|---------------------|---------|------------|
| **RQ1** | **Hiệu suất phát hiện DC trên luồng:** Làm thế nào để phát hiện vi phạm denial constraints trên dữ liệu luồng với chi phí thấp hơn O(N²), trong khi vẫn duy trì throughput ổn định khi kích thước cửa sổ tăng và dữ liệu liên tục được thêm/xóa theo cửa sổ trượt? | Throughput (events/s), P99 Latency, Detection Latency (event_time → output_time) | Brute-force O(N²) không scale; cấu trúc indexing tĩnh (KD-Tree trên database) không phù hợp với cửa sổ liên tục thay đổi |
| **RQ2** | **Chất lượng cảnh báo dưới drift và late arrivals:** Làm thế nào để duy trì độ chính xác (Precision/Recall/F1) của cảnh báo vi phạm logic trong môi trường streaming khi: (1) phân phối dữ liệu thay đổi theo thời gian (concept drift), và (2) dữ liệu đến không đúng thứ tự hoặc đến muộn (out-of-order / late arrivals)? | Precision, Recall, F1 (sau retraction), FP Reduction Rate | Luật tĩnh (static thresholds) không thích nghi với drift → FP tăng; thiếu provisional-retract mechanism → báo động oan khi watermark lag |
| **RQ3** | **Khả năng mở rộng khi số luật tăng:** Khi số lượng ràng buộc logic tăng từ 3 lên 50–100+, làm thế nào để hệ thống giám sát DQ trên luồng duy trì memory footprint và throughput ở mức chấp nhận được mà không phải xây dựng lại index từ đầu sau mỗi cửa sổ? | Memory Footprint (active/cache/tombstone), Throughput (events/s), Scale ratio (100 rules vs 3 rules) | Mỗi luật DC có thể cần 1 index riêng → memory tăng tuyến tính; curse of dimensionality khi nhiều luật cùng dùng range search đa chiều |
| **RQ4** | **Sensitivity analysis cho hệ thống DQ trên luồng:** Trong hệ thống giám sát DQ trên luồng có các siêu tham số (window/pane size, adaptation rate, dimension cap), chúng ảnh hưởng đến hiệu năng và chất lượng cảnh báo như thế nào, và làm thế nào để xác định vùng tham số tối ưu cho từng tập dữ liệu và kịch bản vận hành? | P99/avg Latency, F1, RAM, Heatmap trade-off | Mỗi siêu tham số có trade-off không trực quan: pane nhỏ → overhead, pane lớn → tree lớn; α nhỏ → adapt chậm, α lớn → overfit; sweet spot phụ thuộc workload |

> **Lưu ý:** 4 RQ trên là bài toán chung của lĩnh vực streaming DQ — không phụ thuộc vào kiến trúc cụ thể nào. WAVES đề xuất một cách tiếp cận cụ thể cho từng RQ, sẽ được trình bày ở các slide tiếp theo.

---

**Mối quan hệ Gap → RQ:**

```
Gap 1 (O(N²) bottleneck khi phát hiện DC trên luồng)
    └───▶ RQ1: Tìm cách giảm chi phí xuống dưới O(N²)

Gap 2 (Luật tĩnh không thích nghi với concept drift)
    └───▶ RQ2: Cần cơ chế adaptive context cho precision/recall/F1

Gap 3 (Late arrivals gây false alerts khi watermark lag)
    └───▶ RQ2: Cần provisional-retract mechanism cho FP reduction

Gap 4 (Curse of dimensionality khi nhiều luật DC)
    └───▶ RQ3: Cần shared indexing để scale memory + throughput
    └───▶ RQ4: Sensitivity analysis xác định vùng tham số vận hành
```

#### Ghi chú cho presenter

- "4 RQ này là bài toán mở của cộng đồng nghiên cứu — không phải câu hỏi về WAVES. Hội đồng có thể hỏi: tại sao không dùng approach X? WAVES là một cách tiếp cận cụ thể, sẽ trình bày ở phần sau."
- "RQ1 hỏi về hiệu năng. RQ2 hỏi về chất lượng cảnh báo. RQ3 hỏi về khả năng mở rộng. RQ4 hỏi về sensitivity và vùng vận hành an toàn."
- "WAVES đề xuất: (RQ1) pane-based KD-Tree forest, (RQ2) elastic box + EMA + provisional-retract, (RQ3) shared rule optimizer, (RQ4) sensitivity analysis protocol — sẽ chi tiết ở Slides 6-9."

#### Citations

- VLDB 2025 Martin et al. (95% constraints false): [VLDB 2025](https://www.vldb.org/pvldb/vol18/p3477-martin.pdf)
- Klink — Progress-aware Scheduling (SIGMOD 2021): [doi.org](https://doi.org/10.1145/3448016.3452794)
- Incremental DC Discovery (Springer VLDB Journal 2024): [link.springer.com](https://link.springer.com/article/10.1007/s00778-023-00788-y)
- Rapidash (VLDB 2024): [arXiv:2309.12436](https://arxiv.org/abs/2309.12436)
- Watermarks in Stream Processing (VLDB 2017): [ACM](https://dl.acm.org/doi/10.14778/3476311.3476389)

---

## PHẦN 2: KIẾN TRÚC ĐỀ XUẤT

---

### SLIDE 5 — Kiến trúc tổng thể WAVES

#### Nội dung trình bày

**Tiêu đề:** WAVES: Stream-first + DC + Watermark + Retraction

```
┌──────────────────────────────────────────────────────────────┐
│                    StreamIngestion                           │
│         CSV / JSON / Kafka → DataEvent                       │
│         Gán EventTime + IngestionTime                        │
└────────────────────┬─────────────────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────────────────┐
│                  WindowManager                               │
│         Sliding Window + Pane gán                           │
│         Tính delta insert/delete khi slide                  │
└────────────────────┬─────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
┌───────▼───────┐      ┌─────────▼─────────┐
│  BasicDQ      │      │  Logical Engine  │
│  Checks       │      │  Statistical     │
│  (inline)     │      │  Context (EMA)   │
└───────┬───────┘      │  Elastic Box Gen │
        │              └─────────┬─────────┘
        │                        │
┌───────▼────────────────────────▼─────────────────────────────┐
│                     Rapidash                                 │
│         Batched KD-Tree Traversal                            │
│         Box Dropping + Candidate Violations                  │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                  Decision Layer                               │
│         Provisional / Final / Retraction                     │
│         AlertStateStore + TombstoneFilter                   │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│           Watermark + AlertOutput                           │
│         WatermarkClock + MetaStream                         │
│                                                              │
│         ┌─────────────────────────────────────┐             │
│         │           Weever (Pane Forest)        │             │
│         │  Pane close → Bulk Load KD-Tree     │             │
│         │  O(1) DROP pane khi expire           │             │
│         └─────────────────────────────────────┘             │
└──────────────────────────────────────────────────────────────┘
```

**Bảng 11 Modules (2.1–2.11):**

| Module | ID | Chức năng chính |
|--------|----|-----------------|
| **StreamIngestion** | 2.1 | Nhận luồng CSV/JSON/Kafka, gán EventTime/IngestionTime, chuẩn hóa schema → DataEvent. DLQ cho malformed events. |
| **WindowManager** | 2.2 | Gán bản ghi vào sliding window (tumbling/sliding). Tính delta insert/delete khi slide. Gán pane_id vào event. |
| **BasicDQChecks** | 2.3 | Completeness, validity, range checks. Inline trên luồng chính, O(1) per event. Output: QualityMetaStream. |
| **LogicalEngine** | 2.4 | Statistical Context (EMA) — cập nhật liên tục μ, σ². Elastic Box Generator — padding tự động theo context. |
| **SharedRuleOptimizer** | 2.5 | DC Parser — parse JSON → Predicate list. Greedy Grouping — ghép luật có equality cols chung. Infinite Padding — (-∞,+∞) cho chiều không dùng. Sinh ActiveBox metadata. |
| **Rapidash** | 2.6 | KD-Tree Bulk Load per pane (khi pane close). Batched Traversal — duyệt nhiều box cùng lúc. Box Dropping — prune nhánh không giao box. |
| **Weever** | 2.7 | Pane Forest — flat list + O(1) lookup. DROP pane khi expire = O(1). TombstoneManager drop cùng lúc. |
| **DecisionLayer** | 2.8 | AlertStateStore — key = alert_id. Provisional / Final / Retraction alerts. TombstoneManager add khi retract. |
| **TombstoneFilter** | 2.9 | O(1) lookup filter per pane. Gắn vòng đời với pane_id. drop_pane = O(1) clear. Ưu tiên Bloom/Cuckoo filter. |
| **LateHandler** | 2.10 | Kiểm tra ngưỡng lateness. Retract alerts liên quan. Re-check với pane đã close. Late event upsert vào store. |
| **AlertOutput** | 2.11 | AlertSink — provisional/final/retraction. MetaSink — quality meta-stream. Detection latency = event_time → output_time. |

**Sự phụ thuộc quan trọng:**

- Rapidash đọc snapshot forest từ Weever (stateless về index)
- Decision nhận candidate từ Rapidash + watermark → cập nhật Weever
- WindowManager cung cấp ranh giới cho watermark seal
- TombstoneManager được khởi tạo tại pipeline level

#### Ghi chú cho presenter

- "Tôi sẽ dẫn hội đồng đi qua hành trình của một gói tin: từ lúc nhận diện Watermark, đi qua bộ đo Ngữ cảnh EMA, chui vào Rừng cây cắt tỉa đa chiều, và cuối cùng là ra quyết định Cảnh báo hoặc Rút lại."

---

### SLIDE 6 — Giải quyết Gap 1: KD-Tree + Pane-based Forest

#### Nội dung trình bày

**Tiêu đề:** Phá vỡ O(N²) bằng Orthogonal Range Search và Pane-based Forest

**DC1 — Fare-Distance Dominance**

> **Ý nghĩa nghiệp vụ:** Hai xe cùng tuyến, quãng đường gần nhau → xe ngắn không fare cao bất thường.

**Công thức:**
```
NOT (
  PULocationID_s = PULocationID_t
  AND trip_distance_s ≈ trip_distance_t  (|dist_s - dist_t| < ε)
  AND fare_amount_s > fare_amount_t + Δ
)
```

**Mapping sang không gian:**
- Mỗi tuple = 1 điểm trong không gian k-chiều
- DC violation = điểm nằm trong vùng truy vấn (axis-aligned hypercube)
- Equality columns (PULocationID) → Hash partition
- Inequality columns (distance, fare) → Range search 2 chiều

**KD-Tree Bulk Load (khi pane close):**

```
1. Sắp xếp points theo chiều có range lớn nhất (argmax_range)
2. Tách điểm tại median → left/right subtree
3. Mỗi node lưu: bounding box (left_lo, left_hi, right_lo, right_hi)
   → DÙNG CHO BOX DROPPING
```

**Box Dropping Algorithm:**

```
Intersects(box, region_lo, region_hi):
  for dim in range(len(region_lo)):
      box_lo, box_hi = box.padded_bounds.get(dim, (-INF, +INF))
      overlap_lo = max(region_lo[dim], box_lo)
      overlap_hi = min(region_hi[dim], box_hi)
      if overlap_lo > overlap_hi: return False  # PRUNE!
  return True  # Cần duyệt tiếp
```

**Pane-based Forest — giải quyết cây phình to:**

| Vấn đề | Giải pháp |
|---------|-----------|
| 1 tree lớn cho toàn window → fragmentation | Nhiều tree nhỏ (mỗi pane = 1 tree) |
| Insert/delete từng điểm → O(log N) × N | Bulk load khi pane close |
| Rebalance liên tục → spike latency | O(1) DROP pane khi expire |
| Memory leak | TombstoneManager drop cùng lúc |

**So sánh độ phức tạp:**

| | Rapidash gốc | WAVES |
|---|---|---|
| Data model | Static snapshot | Streaming / pane-based |
| Build per window | O(N) full rebuild | O(N) bulk load per pane |
| Delete per slide | O(N) | O(1) DROP pane |
| Space | O(N log^k N) | O(N) — cap by pane_size |
| Watermark-aware | Không | Có |

**Worst case cho Box Dropping:**
- Curse of dimensionality (k > 10)
- Uniform data distribution
- Query region lớn (padding không phù hợp)

#### Citations

- Rapidash (KD-Tree + orthogonal range search): [arXiv:2309.12436](https://arxiv.org/abs/2309.12436) (VLDB 2024)
- Bentley & Friedman (1979): data structures for range searching

---

### SLIDE 7 — Giải quyết Gap 2: Elastic Box + EMA

#### Nội dung trình bày

**Tiêu đề:** Luật tĩnh → Luật động: Elastic Box tự thích nghi với Concept Drift

**DC2 — Context-Aware Duration Anomaly**

> **Ý nghĩa nghiệp vụ:** Cùng tuyến, duration chênh ngoài biên độ giao thông hiện tại.

**Công thức:**
```
NOT (
  PULocationID_s = PULocationID_t
  AND DOLocationID_s = DOLocationID_t
  AND |trip_duration_s - trip_duration_t| > Δ_t
)
```

> **Điểm khác biệt với DC1:** Δ_t không cố định, thay đổi theo concept drift. Luật tĩnh sẽ báo lỗi oan khi kẹt xe, bão tuyết, hoặc sự kiện đặc biệt.

**EMA — Exponential Moving Average (Statistical Context Engine):**

```
Cập nhật trung bình động:
  μ_t = (1 - α) · μ_{t-1} + α · X_t

Cập nhật phương sai động:
  σ_t² = (1 - α) · σ_{t-1}² + α · (X_t - μ_{t-1})²

Lấy độ lệch chuẩn:
  σ_t = √(max(σ_t², 0))
```

Trong đó:
- α ∈ (0, 1) = learning rate (tham số cần tune trong E2)
- X_t = giá trị mới (trip_duration mới nhất)

**Elastic Box Generator:**

```
Tính độ nới lỏng:
  Δ_t = k · σ_t

Clamp để tránh cực đoan:
  Δ_t = min(max(Δ_t, Δ_min), Δ_max)

Sinh ElasticBoundingBox:
  effective_bounds = static_bounds ± Δ_t
```

**Hành vi khi concept drift:**

```
Normal traffic:
  μ = 20 min, σ = 3 min → Δ = k·σ = 2·3 = 6 min
  → Box: [14, 26] min

Traffic jam / bad weather:
  μ = 45 min, σ = 15 min → Δ = 2·15 = 30 min
  → Box: [15, 75] min  (tự nới rộng)
  → KHÔNG báo false positive khi mọi xe đều chậm
```

**Edge cases:**

| Trường hợp | Xử lý |
|-------------|--------|
| Zero variance (σ = 0) | Luôn áp dụng Δ_min > 0 để tránh box co về điểm |
| Drift đột ngột | Dùng clamp bằng Δ_max, có thể bỏ qua outlier cực đoan trước khi update EMA |
| Cold start | Fallback về static box gốc (chưa đủ mẫu) |
| State stale theo partition | Timeout để reset state hoặc đánh dấu degraded mode |

#### Citations

- Stream DaQ (EMA concept): [arXiv:2506.06147](https://arxiv.org/abs/2506.06147)
- Rapidash (elastic box analogy): [VLDB 2024](https://arxiv.org/abs/2309.12436)

---

### SLIDE 8 — Giải quyết Gap 3: Watermark + Tombstone + Retraction

#### Nội dung trình bày

**Tiêu đề:** Bia mộ & Sửa sai: Delay-aware Alerting với Watermark semantics

**DC3 — Toll Route Anomaly**

> **Ý nghĩa nghiệp vụ:** Cùng tuyến, tolls chênh bất thường (có thể hack GPS hoặc gian lận tuyến).

**Công thức:**
```
NOT (
  PULocationID_s = PULocationID_t
  AND DOLocationID_s = DOLocationID_t
  AND tolls_amount_s ≫ tolls_amount_t + Δ
)
```

> **Lưu ý:** DC2 và DC3 có thể group chung (cùng PULocationID + DOLocationID = equality keys) → Shared Rule Optimizer gom thành 1 tree.

**Alert State Machine:**

```
┌───────────────┐  WM passes, no retraction  ┌───────────────┐
│ PROVISIONAL  │ ─────────────────────────────▶│    FINAL     │
│    [+1]      │                             │   [FINAL]    │
└───────────────┘                             └───────────────┘
       │
       │ retraction arrives
       ▼
┌───────────────┐
│ RETRACTED    │
│   [-1]       │
└───────────────┘
```

**TombstoneFilter — O(1) lookup:**

- Mỗi retracted event_id được ghi vào filter
- Gắn vòng đời với pane_id (pane hết hạn → toàn bộ tombstone của pane được clear)
- Ưu tiên Bloom Filter hoặc Cuckoo Filter cho O(1) lookup và tiết kiệm RAM
- Tại lá KD-Tree, kiểm tra tombstone trước khi report violation

**LateHandler Flow:**

```
1. Late event đến (event_time < current_watermark)
2. Kiểm tra ngưỡng: watermark > window_end + wait_for_late?
   → YES: DROP event, log
   → NO: LateHandler xử lý
3. Tìm provisional alerts liên quan (AlertStateStore.get_by_event_id)
4. Kiểm tra late_event có invalidate alert không
5. Nếu invalidate → RETRACT alert + add TẤT CẢ event_ids vào Tombstone
6. Re-check: late event có tạo violation mới không?
7. Insert vào pane đã close (re-check KD-Tree traversal)
```

**Watermark không block luồng chính:**

- Watermark chỉ advance khi có dữ liệu mới
- Luồng chính vẫn phát provisional alert ngay khi phát hiện
- Watermark phụ trách: (1) đóng sổ, (2) dọn rác, (3) chuyển provisional → final

**Cấu hình hệ thống:**

| Tham số | Giá trị | Ý nghĩa |
|---------|---------|---------|
| `wait_for_late_seconds` | 300 (5 phút) | Sau window close, chờ thêm 5 phút cho late data |
| `watermark_advance_interval` | 1 (giây) | Kiểm tra watermark mỗi giây |
| `alert_ttl_seconds` | 3600 (1 giờ) | Xóa alert sau 1 giờ |
| `pane_size` | 60s (1 phút) | Kích thước pane mặc định (tune trong E1) |

**Trade-off:**

| Cấu hình | Ưu điểm | Nhược điểm |
|-----------|---------|-------------|
| Watermark bảo thủ | Ít false positive | Phản ứng chậm |
| Watermark gắt | Phản ứng nhanh | Late data bị xem là lỗi → false alert tăng |

#### Citations

- Watermarks in Stream Processing Systems: [VLDB 2017](https://dl.acm.org/doi/10.14778/3476311.3476389)
- The Dataflow Model (Google): [VLDB 2015](https://vldb.org/pvldb/vol8/p1792-Akidau.pdf)
- ProbSlack (watermark probabilistic): [SIGMOD 2018](https://dl.acm.org/doi/10.1145/3210284.3210293)

---

### SLIDE 9 — Shared Rule Optimizer: Mở rộng 50–100 luật

#### Nội dung trình bày

**Tiêu đề:** Từ 3 DC đến 100 DC: Shared Rule Optimizer

**Ba DC benchmark và cấu trúc không gian:**

| DC | Equality columns | Range search columns | Tree |
|----|----------------|---------------------|------|
| **DC1** (Fare-Distance) | PULocationID | (trip_distance, fare_amount) | Tree A |
| **DC2** (Duration) | PULocationID, DOLocationID | (trip_duration) | Tree A (chung với DC1) |
| **DC3** (Toll Route) | DOLocationID | (tolls_amount) | Tree B |

> **Shared Optimizer nhận ra:** DC1 + DC2 có PULocationID chung → ghép vào cùng group → **1 tree** (range search 2 chiều: distance + fare, bỏ qua duration bằng filter). DC3 riêng DOLocationID → **Tree B** (range search 1 chiều: tolls).

**So sánh: Không shared vs Có shared:**

| | Không Shared | Có Shared (WAVES) |
|---|---|---|
| Số trees cho 3 DC | 3 trees | 2 trees |
| Số traversals | 3 traversals | 2 traversals |
| RAM overhead | 3x metadata | Tối ưu chung |
| Scale khi 50–100 DC | O(N × R) | O(N × G) với G << R |

**Kỹ thuật tối ưu:**

**1. Infinite Padding:**
- Nếu luật không dùng chiều nào, chiều đó được gán `(-∞, +∞)`
- Nhờ vậy nhiều luật khác nhau vẫn cùng sống trong một KD-tree mà không làm sai logic

**2. k_max cap (dimension cap):**
- Giới hạn số chiều cho range search (mặc định k_max = 5)
- Các chiều còn lại được xử lý bằng filter sau traversal
- DC1–DC3 chỉ cần k = 2

**3. Batched Traversal:**
- Một traversal duyệt tất cả boxes trong group cùng lúc
- Box nào không giao với region → prune sớm
- ActiveBox metadata loại bỏ parsing DC trên hot path

**Thumb rules cho WAVES:**

```
1. k_max = 5 cho range search dimensions
   → Đủ cho hầu hết DCs (thường chỉ cần 1-3)
   → Tránh curse of dimensionality

2. Equality columns → Hash partition
   → Luôn dùng cho grouping
   → Không tăng search space

3. Inequality columns ưu tiên:
   → Columns có cardinality CAO (nhiều giá trị)
   → Columns có HIGH selectivity (giảm candidate count)

4. Boolean flags / columns có cardinality thấp:
   → Không cần index
   → Filter sau traversal
```

#### Citations

- Rapidash (dimension cap, hash partition): [VLDB 2024](https://arxiv.org/abs/2309.12436)
- Incremental DC Discovery: [Springer 2024](https://link.springer.com/article/10.1007/s00778-023-00788-y)

---

### SLIDE 10 — Tại sao backend không đủ?

#### Nội dung trình bày

**Tiêu đề:** Backend chỉ thực thi local invariants — WAVES thực thi global temporal invariants

**Câu hỏi phản biện:** "Tại sao không viết hàm IF/ELSE ở Backend?"

**Trả lời ngắn gọn:**
Backend CHỈ đủ với 3 loại luật: (1) Luật đơn bản ghi, (2) Luật đồng bộ trong 1 request, (3) Luật không có late/out-of-order data.

**5 nhóm luật backend KHÔNG thể xử lý:**

| Nhóm | Ví dụ | Tại sao backend thất bại |
|------|-------|-------------------------|
| **Cross-record** | 2 chuyến chồng thời gian cùng driver | Phải nhìn nhiều record cùng lúc → không scale |
| **Cross-service** | Tài khoản đóng băng nhưng vẫn thanh toán | Dữ liệu nằm ở nhiều microservice riêng |
| **Cross-time** | Parcel ở 2 hub cách 200km trong 20 phút | Phải nhìn cửa sổ thời gian / lịch sử gần |
| **Late-data-aware** | Chuyến kết thúc đến muộn do mất 3G/4G | Không có retract mechanism |
| **High-rate/low-latency** | E-commerce oversell nhiều kênh cùng lúc | Không scale khi triệu events/ngày |

**So sánh:**

| | Backend | WAVES |
|---|---|---|
| Scope | 1 request đơn lẻ | Global, cross-record, cross-time |
| Late data | Không handle được | Provisional → Retract |
| State | Request-scoped | Pane-based, persistent |
| Cross-service | Phải gọi chéo (coupling) | Control plane độc lập |
| Alert quality | Tức thời, không phân biệt provisional/final | Delay-aware, two-phase verdict |

**Câu "đinh" cho hội đồng:**

> "Nếu IF/ELSE là luật giao thông tại một ngã tư, thì WAVES là trung tâm điều phối giao thông của cả thành phố."

> "Backend kiểm tra được 'stock hiện tại'. WAVES kiểm tra được 'stock consistency theo thời gian thực giữa nhiều dòng sự kiện'."

> "Khi doanh nghiệp còn nhỏ, luật nằm trong code. Khi doanh nghiệp lớn, luật phải được tách khỏi code và đặt vào một control plane độc lập. WAVES chính là control plane đó cho denial constraints thời gian thực."

---

## PHẦN 3: THỰC NGHIỆM

---

### SLIDE 11 — Ví dụ thực tiễn

#### Nội dung trình bày

**Tiêu đề:** Hai ví dụ thực tiễn: Ride-hailing và Logistics

---

**Example 1 — Ride-hailing: Gian lận GPS (DC cross-record)**

**DC:** Một driver không thể có hai chuyến đang active cùng lúc.

**Kịch bản vi phạm:**
- Event A: Driver D bắt đầu chuyến tại Location X, timestamp 10:00
- Event B: Driver D bắt đầu chuyến tại Location Y (cách X 10km), timestamp 10:01
- Event kết thúc A đến muộn (do mất 3G/4G) → event_time = 09:59:58, nhưng ingestion_time = 10:03

**Backend phải làm mỗi request mới:**
- Gọi chéo Trip Service + Driver Session Service + Telemetry → **coupling**
- Query lịch sử gần nhất → **stale read**
- Race condition giữa 2 services → **kết quả không nhất quán**
- Không phân biệt event_time vs processing_time → **báo động oan**
- Không có retract → **action đã taken trước khi biết oan**

**WAVES xử lý:**
- Theo dõi trên luồng sự kiện, tổng hợp từ nhiều service
- Provisional alert ngay lập tức [+1] tại 10:01 (phát hiện overlap)
- Late event đến tại 10:03 → Tombstone O(1) → Retraction [-1]
- Không action trên provisional → **đúng khi retract**

**Điểm độc quyền:** DÁM RA QUYẾT ĐỊNH SỚM RỒI TỰ SỬA SAI SAU.

---

**Example 2 — Logistics: Parcel hiện diện ở hai hub phi vật lý (DC space-time)**

**DC:** Một kiện hàng không thể được scan ở Hub A lúc 10:00 và ở Hub B cách 200km lúc 10:20, nếu thời gian vận chuyển tối thiểu là 3 giờ.

**Đây là DC kiểu:**
- Cross-record (so sánh nhiều scan events)
- Có ngữ cảnh không gian-thời gian
- Cần lookup lịch sử gần nhất
- Cần xử lý out-of-order scan (handheld device)

**Backend phải làm mỗi scan mới:**
- Lục lịch sử gần nhất → query DB
- Đối chiếu tuyến → tính khoảng cách
- Đối chiếu hub → kiểm tra transit time
- Xử lý late scan từ handheld device
- → **O(N) mỗi scan × triệu scans/ngày → không scale**

**WAVES — Rapidash + Elastic Boxes:**
- Luật logic được biến thành truy vấn không gian-thời gian
- (hub_A, hub_B, timestamp_A, timestamp_B, distance) → 5 chiều
- KD-Tree tìm candidate violations hiệu quả hơn brute-force
- O(log N) per scan thay vì O(N)

**Câu chốt:** Với loại luật này, backend không chỉ CHẬM — mà còn SAI MÔ HÌNH TÍNH TOÁN.

---

### SLIDE 12 — Fault Injection Pipeline

#### Nội dung trình bày

**Tiêu đề:** Tại sao cần Fault Injection? Và pipeline tiêm nhiễu như thế nào?

**Tại sao cần fault injection?**

> Dữ liệu thật (NYC Taxi) rất sạch → không có ground truth cho violations → không thể đo Precision/Recall.
>
> Cần tạo ra các tình huống: delayed, out-of-order, drift — để đo F1 thực sự.

**Nguồn tham chiếu:** Icewafl (EDBT 2025) — "A Configurable Data Stream Polluter"
DOI: [10.48786/edbt.2025.64](https://openproceedings.org/2025/conf/edbt/paper-240.pdf)

**Pipeline 4 bước:**

```
B1 (Sạch) ──▶ B2 (Drift) ──▶ B3 (Fraud) ──▶ B4 (Late/OoO)
NYC Taxi     Tiêm kẹt xe    Inject DC1-3    Tiêm trễ mạng
```

**Bảng chi tiết từng bước:**

| Bước | Nội dung | Kỹ thuật | Output |
|------|----------|----------|--------|
| **B1 — Base Prep** | Chuẩn hóa schema, loại lỗi vật lý rõ ràng | Filter null, format, range vượt ngưỡng vật lý | Tập dữ liệu sạch |
| **B2 — Drift Injection** | Mô phỏng concept drift (kẹt xe, bão tuyết, sự kiện đặc biệt) | Tăng trip_duration theo time-of-day pattern. Cấu hình: α = 0.05 | Tập có drift → test EMA |
| **B3 — Fraud Injection** | Inject DC1–DC3 trên tỷ lệ nhỏ bản ghi (1–5%) | Mutate fare_amount, trip_duration, tolls_amount. Đánh dấu ground_truth_violation = True | Ground truth positives → đo Precision/Recall |
| **B4 — Late/OoO Injection** | 90% on-time, 10% late 120–300s | Sinh ingestion_time khác event_time, sort theo ingestion | Benchmark stream → test Retraction |

**Dataset:**

- NYC Taxi Yellow Cab (CSV/Parquet)
- Các trường ưu tiên: `tpep_pickup/dropoff_datetime`, `PULocationID`, `DOLocationID`, `trip_distance`, `fare_amount`, `total_amount`, `trip_duration`, `tolls_amount`, `RatecodeID`

**Lưu ý quan trọng:**

> Sort toàn bộ DataFrame theo **IngestionTime** ở bước CUỐI CÙNG.
> Giữ đúng thứ tự ingestion trong file replay.

#### Citations

- Icewafl (EDBT 2025): [OpenProceedings](https://openproceedings.org/2025/conf/edbt/paper-240.pdf)

---

### SLIDE 13 — Ablation Baselines

#### Nội dung trình bày

**Tiêu đề:** Phương pháp đối chuẩn: Ablation Study — không so sánh với tool ngoài, mà tắt từng tính năng của WAVES

**Lý do dùng Ablation thay vì so sánh với tool ngoài:**

- Rapidash là static, không phải streaming → không công bằng để so sánh trực tiếp
- Stream DaQ thiếu DC → không cùng benchmark surface
- Ablation chứng minh giá trị từng dòng code: tắt X → hệ thống sụp đổ như thế nào

**Bảng 6 hệ thống đối chuẩn:**

| Baseline | KD-Tree | Pane | EMA | Retraction | SharedOpt | Mục đích |
|----------|---------|------|-----|------------|-----------|-----------|
| **NL-Stream** | ❌ | ❌ | ❌ | ❌ | ❌ | Chứng minh O(N²) là nút thắng cổ chai |
| **Single-Tree** | ✅ | ❌ | ❌ | ❌ | ❌ | Chứng minh 1 tree lớn → fragmentation/spike latency |
| **Static-Box** | ✅ | ✅ | ❌ | ✅ | ❌ | Chứng minh concept drift → false positives (box không thích nghi) |
| **WAVES-SingleRule** | ✅ | ✅ | ✅ | ✅ | ❌ | Tắt shared optimizer — đo overhead khi không share indexing |
| **Buffer-Wait** | ✅ | ✅ | ✅ | ❌ | ❌ | Chứng minh detection latency cao khi watermark như barrier |
| **WAVES-Full** | ✅ | ✅ | ✅ | ✅ | ✅ | Performance ceiling |

**Các biến thể đối chuẩn khác:**

| Baseline | Điểm khác với WAVES-Full |
|----------|--------------------------|
| NL-Stream | Brute-force O(N²) — nested loop |
| Single-Tree | 1 KD-Tree cho toàn window, không pane, không watermark |
| Static-Box | Box có kích thước cố định, không EMA, không adaptive padding |
| WAVES-SingleRule | Mỗi rule = tree riêng, không group |
| Buffer-Wait | Watermark như barrier chặn, không có Retraction, provisional = final |

---

### SLIDE 14 — Metrics + Sensitivity Analysis

#### Nội dung trình bày

**Tiêu đề:** Hai nhóm metrics: Hiệu năng và Chất lượng cảnh báo

---

**Nhóm 1: HIỆU NĂNG HỆ THỐNG**

| Metric | Định nghĩa | Công thức / Cách đo | Dùng trả lời |
|--------|-----------|---------------------|--------------|
| **Throughput** | Số bản ghi xử lý được mỗi giây | Đếm events trong sliding window, chia thời gian | RQ1, RQ3 |
| **P99 Latency** | Latency ở percentile 99 | Sort all latencies, take 99th percentile | RQ1, RQ4 |
| **Avg Latency** | Latency trung bình | Tổng latencies / N | RQ4 |
| **Detection Latency** | Thời gian từ violation xảy ra đến alert được emit | `alert_time - violation_event_time` | RQ2 |
| **Memory Footprint** | RAM phân biệt: Active (pane đang hoạt động), Cached (pane đã close chưa drop), Tombstone (retracted event IDs) | Monitoring RAM per component | RQ3, RQ4 |

---

**Nhóm 2: CHẤT LƯỢNG CẢNH BÁO**

> **Lưu ý quan trọng:** Không chốt Precision/Recall chỉ dựa trên provisional alerts [+1]. Một cảnh báo chỉ được xem là còn tồn tại nếu sau giai đoạn quan sát nó không bị triệt tiêu bởi tín hiệu retraction [--1].

| Metric | Định nghĩa | Công thức | Dùng trả lời |
|--------|-----------|---------|--------------|
| **Precision** | TP / (TP + FP) sau retraction | TP: đúng + ground truth có; FP: báo sai + ground truth không | RQ2, RQ4 |
| **Recall** | TP / (TP + FN) | FN: không báo + ground truth có vi phạm | RQ2, RQ4 |
| **F1 Score** | 2 × Precision × Recall / (Precision + Recall) | `2PR/(P+R)` | RQ2, RQ4 |
| **FP Reduction Rate** | Tỷ lệ FP giảm nhờ delay-aware | `(FP_without_delay - FP_with_delay) / FP_without_delay` | RQ2 |

---

**Sensitivity Analysis (E1–E3):**

| Exp | Tham số | Giá trị test | Giữ cố định | Metrics | Mong đợi |
|-----|---------|-------------|-------------|---------|-----------|
| **E1** | Pane size | 30s / 1m / 5m / 10m | Window 10 phút | P99, avg latency, throughput | Pane nhỏ: nhiều overhead; pane lớn: tree lớn, box dropping kém. **Sweet spot: 1–5 phút** |
| **E2** | α (EMA learning rate) | 0.01 / 0.05 / 0.1 / 0.2 | Tập có drift | Precision, Recall, F1 | α nhỏ: thích nghi chậm → false positives khi drift; α lớn: over-adapt. **Sweet spot: 0.05** |
| **E3** | k_max (dimension cap) | 2 / 3 / 5 / 10 | 100 luật DC | RAM, throughput, precision/recall | k_max lớn: curse of dimensionality; DC1–DC3 chỉ cần k=2. **Sweet spot: k_max = 5** |

---

**Kết quả kỳ vọng:**

| RQ | Kết quả kỳ vọng | Dấu hiệu thành công |
|----|----------------|---------------------|
| RQ1 | Throughput cao và phẳng; P99 ổn định khi window tăng | Đường throughput WAVES cao hơn NL-Stream; ít spikes |
| RQ2 | Precision và F1 cao hơn Static-Box trong dữ liệu có drift + late | False positives giảm mạnh; detection latency thấp |
| RQ3 | RAM tăng chậm khi tăng số luật lên 50–100 | Memory WAVES-SingleRule cao hơn WAVES-Full |
| RQ4 | Tồn tại vùng tham số tối ưu thay vì một cực trị duy nhất | Biểu đồ sensitivity thể hiện sweet spot rõ ràng |

---

## PHẦN 4: KẾT LUẬN

---

### SLIDE 15 — Kết luận + Timeline + Risk Assessment

#### Nội dung trình bày

**Tiêu đề:** WAVES — Tổng hợp và Lộ trình

---

**Ba đóng góp chính:**

| # | Đóng góp | Chi tiết |
|---|---------|---------|
| 1 | **Stream-first architecture đầu tiên tích hợp đủ 4 thành phần** | DC checking (Rapidash-style) + Watermark semantics + Retraction mechanism + Quality meta-stream |
| 2 | **Elastic Box với EMA tự thích nghi concept drift** | Padding tự động theo ngữ cảnh thống kê → giảm false alerts mà không cần con người can thiệp |
| 3 | **Fault injection benchmark protocol cho delay-aware DQ systems** | B1→B2→B3→B4 pipeline + Ablation baselines → đánh giá đồng thời hiệu năng và chất lượng cảnh báo |

---

**Tổng hợp RQ vs Modules:**

| RQ | Modules cốt lõi |
|----|----------------|
| RQ1 Throughput | Rapidash + Weever + Window + Ingestion |
| RQ2 F1 (drift+late) | LogicalEngine + Decision + Tombstone |
| RQ3 Scale (50–100 rules) | SharedOptimizer + Rapidash batched |
| RQ4 Trade-off | ElasticBox + Weever + Optimizer |

---

**Timeline thực nghiệm:**

```
Tháng 1–2     │ Tháng 3        │ Tháng 4         │ Tháng 5
──────────────┼────────────────┼─────────────────┼──────────
Fault         │ Chạy 6         │ Sensitivity      │ Viết
Injection     │ baselines       │ analysis         │ báo cáo
Pipeline      │ trên NYC Taxi   │ (E1, E2, E3)     │ + nộp
(B1→B4)      │ Thu metrics     │ Phân tích        │
Baseline     │                │ trade-off        │
impl.        │                │                  │
```

---

**Risk Assessment:**

| Risk | Mô tả | Mitigation |
|------|-------|-----------|
| **Risk 1** | Watermark lag → pane close chậm → detection latency tăng | B4 late injection đo FP reduction rate; watermark per pane (không toàn cục) |
| **Risk 2** | k_max > 5 → curse of dimensionality → Box Dropping kém | E3 sensitivity analysis xác định sweet spot; k_max configurable |
| **Risk 3** | Overhead shared optimizer với luật không tương thích → perf giảm | Fallback: tree riêng per rule khi không group được |
| **Risk 4** | EMA cold start → false alerts cao trong warm-up period | Fallback về static box trong warm-up; đo warm-up period trong experiments |

---

**Câu "đinh" cuối cùng cho hội đồng:**

> "WAVES không cạnh tranh với backend ở bài toán validation cục bộ.
> WAVES giải quyết lớp Denial Constraints mà backend không thể gánh một cách bền vững:
> luật chéo bản ghi, chéo dịch vụ, chéo thời gian,
> có dữ liệu đến muộn, và cần phản ứng ngay
> nhưng vẫn phải có cơ chế sửa sai."

---

## PHỤ LỤC: Tổng hợp Citations

| # | Paper / Nguồn | Link |
|---|---------------|------|
| 1 | Stream DaQ (arXiv:2506.06147) | [https://arxiv.org/abs/2506.06147](https://arxiv.org/abs/2506.06147) |
| 2 | Rapidash (VLDB 2024, arXiv:2309.12436) | [https://arxiv.org/abs/2309.12436](https://arxiv.org/abs/2309.12436) |
| 3 | Weever (VLDB 2021) | [https://dl.acm.org/doi/10.14778/3717755.3717761](https://dl.acm.org/doi/10.14778/3717755.3717761) |
| 4 | Watermarks in Stream Processing (VLDB 2017) | [https://dl.acm.org/doi/10.14778/3476311.3476389](https://dl.acm.org/doi/10.14778/3476311.3476389) |
| 5 | The Dataflow Model (VLDB 2015) | [https://vldb.org/pvldb/vol8/p1792-Akidau.pdf](https://vldb.org/pvldb/vol8/p1792-Akidau.pdf) |
| 6 | Icewafl (EDBT 2025, DOI: 10.48786/edbt.2025.64) | [https://openproceedings.org/2025/conf/edbt/paper-240.pdf](https://openproceedings.org/2025/conf/edbt/paper-240.pdf) |
| 7 | Klink — Progress-aware Scheduling (SIGMOD 2021) | [https://doi.org/10.1145/3448016.3452794](https://doi.org/10.1145/3448016.3452794) |
| 8 | Incremental DC Discovery (Springer 2024) | [https://link.springer.com/article/10.1007/s00778-023-00788-y](https://link.springer.com/article/10.1007/s00778-023-00788-y) |
| 9 | VLDB 2025 Martin et al. (95% constraints false) | [https://www.vldb.org/pvldb/vol18/p3477-martin.pdf](https://www.vldb.org/pvldb/vol18/p3477-martin.pdf) |
| 10 | ProbSlack (SIGMOD 2018) | [https://dl.acm.org/doi/10.1145/3210284.3210293](https://dl.acm.org/doi/10.1145/3210284.3210293) |
| 11 | Apache Griffin | [https://github.com/apache/griffin](https://github.com/apache/griffin) |
| 12 | Great Expectations | [https://github.com/great-expectations/great_expectations](https://github.com/great-expectations/great_expectations) |
| 13 | Soda Core | [https://github.com/sodadata/soda-core](https://github.com/sodadata/soda-core) |
| 14 | Spark Structured Streaming | [https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) |
| 15 | AWS Deequ + Sagemaker Model Monitor | [https://docs.aws.amazon.com/sagemaker/latest/dg/model-monitor-data-quality.html](https://docs.aws.amazon.com/sagemaker/latest/dg/model-monitor-data-quality.html) |
| 16 | GCP Dataplex Data Quality | [https://cloud.google.com/dataplex/docs/auto-data-quality-overview](https://cloud.google.com/dataplex/docs/auto-data-quality-overview) |
