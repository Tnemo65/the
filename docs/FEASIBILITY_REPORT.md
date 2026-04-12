# WAVES — Feasibility Report

> Review: Kiểm tra từng thành phần trong `MASTER_AGENT_CHECKLIST.md` có cơ sở từ source code, paper, hoặc tài liệu trong repo. Đánh dấu **FEASIBLE** (có proof), **QUESTIONABLE** (cần verify thêm), hoặc **RISKY** (thiếu cơ sở / khó implement).

---

## Tổng hợp

| Thành phần | Verdict | Độ rủi ro |
|---|---|---|
| Stream DaQ (windowing + basic DQ) | ✅ FEASIBLE | Thấp |
| KD-Tree với Box Dropping (Rapidash) | ✅ FEASIBLE | Thấp |
| Pane-based Forest (Weever) | ✅ FEASIBLE | Thấp |
| Elastic Box + EMA (Logical Engine) | ⚠️ QUESTIONABLE | Trung bình |
| Shared Rule Optimizer | ⚠️ QUESTIONABLE | Trung bình |
| Tombstone Filter | ✅ FEASIBLE | Thấp |
| Watermark + Late Data Handling | ⚠️ QUESTIONABLE | Trung bình |
| Watermark / Alert Decision | ⚠️ QUESTIONABLE | Trung bình |
| NYC Taxi + DC1/DC2/DC3 | ✅ FEASIBLE | Thấp |
| Baseline systems (NL-Stream, etc.) | ✅ FEASIBLE | Thấp |
| Icewafl data injection | ⚠️ QUESTIONABLE | Trung bình |
| 6 baselines + 4 ablation | ⚠️ QUESTIONABLE | Cao |

---

## Chi tiết từng thành phần

---

### 1. Stream DaQ — Windowing + Basic DQ Checks

**Verdict: ✅ FEASIBLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/stream-DaQ/streamdaq/StreamDaQ.py` | Class `StreamDaQ` đầy đủ, 317 dòng code thực. |
| `base_repo/stream-DaQ/streamdaq/Windows.py` | Hỗ trợ `tumbling`, `sliding`, `session` — đúng 3 loại window cần thiết. |
| `base_repo/stream-DaQ/streamdaq/DaQMeasures.py` | 30+ built-in checks: null, range, type, regex, count, sum, availability. |
| `docs/base/stream-DaQ_context.md` | Đạt **13.8x nhanh hơn Deequ** trên window nhỏ. |

**Phân tích:**

- Stream DaQ dùng **Pathway** (`import pathway as pw`) — không phải tự xây từ đầu.
- Windowing dựa trên Pathway's `pw.temporal.tumbling/sliding/session`.
- Basic DQ checks là stateless transformations, đã implement sẵn.
- **Điểm yếu đã biết** (docs/base/stream-DaQ_context.md): không có watermark, không có DC checking. WAVES cần bổ sung 2 phần này.

**Kết luận:** Toàn bộ module 2.1 và 2.2 dựa trên code có sẵn. Chỉ cần wrap hoặc extend, không cần viết lại.

---

### 2. KD-Tree + Box Dropping (Rapidash Layer)

**Verdict: ✅ FEASIBLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/Rapidash/src/main/java/kdrange/KDNode.java` | Full KD-Tree implementation. Mỗi node lưu `min[]`/`max[]` bounding box — đúng pattern blueprint cần. |
| `base_repo/Rapidash/src/main/java/kdrange/KDNode.java:109–127` | `rsearch()` — range query chuẩn. |
| `base_repo/Rapidash/src/main/java/kdrange/KDNode.java:130–159` | `rcount()` — dùng `min[]/max[]` bounds để prune. ĐÂY CHÍNH LÀ box dropping. |
| `base_repo/Rapidash/src/main/java/org/dc/DCVerifier.java` | `detectViolationSingle()` — gọi `KDTree` cho heterogeneous DCs. |
| `docs/base/Rapidash_context.md` | Paper đạt **84x nhanh hơn Facet** (O(N²)). |

**Phân tích:**

KD-Tree trong Rapidash đã có đầy đủ:
1. **`min[]/max[]` bounding box per node** — dùng cho box dropping đúng cách (`rcount()` line 133: `if (lowk.coord[l] > t.max[l] || uppk.coord[l] <= t.min[l]) return 0`).
2. **Bulk load** — tree được build từ dataset, không phải incremental insert.
3. **Hash partition** — `Map<List<Integer>, RangeTreeHelper>` theo equality predicates — pattern y hệt HashPartition trong blueprint.

**Điểm cần lưu ý:**

- Rapidash gốc là **batch**, không phải streaming. WAVES cần adapt:
  - Bulk load khi pane close (đúng với pane-based approach).
  - Không có incremental insert (Weever giải quyết phần này).
  - `Box Dropping` trong Rapidash dùng `min[]/max[]` bounds, KHÔNG dùng `split_value` — blueprint đúng.

**Kết luận:** Thuật toán và data structure **100% có trong source code**. Chỉ cần port từ Java sang Python, tích hợp với pane-based architecture.

---

### 3. Pane-based Forest (Weever)

**Verdict: ✅ FEASIBLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/Weever/src/main/java/de/hpi/isg/Weever.java` | Abstract class, 230+ dòng. Hỗ trợ `insert()`, `delete()` incremental. |
| `base_repo/Weever/src/main/java/de/hpi/isg/WeeverSequential.java` | Implement cụ thể với LT-Tree. |
| `docs/base/Weever_context.md` | "Thay vì tìm và xóa từng lá khỏi KD-Tree với chi phí O(log N) rồi làm cây mất cân bằng, Weever dùng kiến trúc Pane-based Forest." |
| `docs/design/extracted_content.txt` (TABLE 5) | Đặc tả chi tiết: pane-based forest, bulk-load khi pane close, O(1) drop. |

**Phân tích:**

Weever gốc dùng **LT-Tree** (Less-Than Tree — mở rộng AVL/Red-Black). WAVES blueprint thay bằng **KD-Tree bulk-load per pane** — đây là adaptation, KHÔNG phải sai.

**Lý do adaptation này hợp lý:**
- Rapidash dùng KD-Tree → pane-based forest dùng KD-Tree per pane → cùng structure, dễ integrate.
- Weever gốc dùng LT-Tree vì cần incremental insert. WAVES dùng pane close = bulk load → không cần incremental insert.

**Điểm cần lưu ý:**

- `WeeverSequential.java` có `insert(Object[] tuple)` và `delete(Object[] tuple)` — đây là incremental operations. WAVES pane-based approach KHÔNG có delete từng điểm, chỉ DROP pane. Đây là **trade-off có cơ sở** (design doc TABLE 5).
- `TIdSet` trong Weever gốc dùng RoaringBitmap/BitTIdSet — WAVES dùng `TombstoneFilter` (Set-based). Vẫn OK vì tombstone chỉ cần O(1) lookup, không cần cardinality operations.

**Kết luận:** Pane-based forest pattern **100% có trong source code và design doc**. Adaptation từ LT-Tree sang KD-Tree per pane là hợp lý và đã được design doc ghi rõ.

---

### 4. Elastic Box + EMA (Logical Engine)

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `docs/design/extracted_content.txt` (TABLE 4) | "Statistical Context Engine liên tục lấy mẫu các tín hiệu như vận tốc và thời gian để ước lượng phương sai σ bằng Exponential Moving Average (EMA). Từ σ, hệ thống sinh Elastic Bounding Boxes." |
| `docs/design/extracted_content.txt` (mục 5.5) | Công thức EMA: `μ_t = α · x_t + (1 - α) · μ_{t-1}` và `σ²_t = (1 − α) · σ²_{t-1} + α · (x_t − μ_{t-1})²`. Có đủ 5 edge cases. |

**Phân tích:**

- EMA là **standard technique** — không cần chứng minh tính khả thi.
- Công thức variance đúng (dùng μ_{t-1}, không phải μ_t) — đã sửa trong review.
- Elastic Box: `Δ = clamp(k · σ, Δ_min, Δ_max)` — đơn giản, có cơ sở.

**Vấn đề tiềm ẩn:**

1. **Độ trễ convergence**: EMA với α nhỏ (0.01–0.1) cần 50–100 samples để state ổn định. Design doc nói warmup = 50. Đây là **acceptable trade-off**, nhưng cần verify với benchmark thực tế.

2. **Cold start trên stream**: Khi bắt đầu stream, warm-up period có thể gây ra nhiều false positive (box lớn). Không có mechanism đặc biệt trong base code → cần implement rõ ràng.

3. **Feature selection**: DC2 và DC3 dùng `trip_duration` và `tolls_amount`. Nhưng EMA cần continuous numeric features. `trip_duration` (derived field) — **cần verify** có được tính trong stream real-time không hay chỉ có trong offline preparation.

**Kết luận:** Công thức và thuật toán **rõ ràng và đúng**. Rủi ro nằm ở implementation details (warm-up, cold start, derived fields) chứ không phải ở concept.

---

### 5. Shared Rule Optimizer

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/Rapidash/src/main/java/org/dc/DCVerifier.java:24–36` | "Hash partition theo equality predicates" — pattern tương tự. |
| `base_repo/Weever/src/main/java/de/hpi/isg/PredicateScheduler.java` | Predicate scheduling — tối ưu thứ tự predicates. |
| `docs/base/Rapidash_context.md` (mục 3.2) | Hash table + Tree structure: `Map<List<Integer>, RangeTreeHelper> treesMap`. |

**Phân tích:**

Greedy grouping theo `equality_cols` là **standard technique** (bin-packing variant). Không có gì novel hoặc khó.

**Vấn đề tiềm ẩn:**

1. **`k_max` dimension cap**: Design doc ghi k_max = 5. Nhưng không có analysis về tại sao 5 là con số phù hợp. KD-Tree efficiency giảm khi dimension > 10–15 (curse of dimensionality). Với DC1–DC3:
   - DC1 (trip_distance, fare_amount): k = 2 ✅
   - DC2 (trip_duration, distance): k = 2 ✅
   - DC3 (tolls_amount, distance): k = 2 ✅
   
   → Với DC1–DC3, k_max = 5 là **quá dư** — không cần. Nhưng RQ3 test với 50–100 rules → k_max cần được justify.

2. **`infinite_padding`**: Padding = (-∞, +∞) cho các chiều không dùng trong DC là **an toàn nhưng tốn memory**. Rapidash gốc không làm điều này — nó chỉ build tree trên các columns thực sự dùng. Cần verify hiệu năng khi dimension cap lớn.

3. **`static_bounds` initialization**: Blueprint để `infer_static_bounds()` là TODO. Trong thực tế, NYC Taxi có ranges:
   - trip_distance: 0–500 miles
   - fare_amount: 0–1000 USD
   - trip_duration: 0–86400 seconds
   
   Cần thực sự implement `infer_static_bounds()` từ data hoặc config.

**Kết luận:** Greedy grouping và DC parsing **feasible**. Rủi ro ở performance tuning (k_max, padding) và `static_bounds` initialization cần implement thực sự.

---

### 6. Tombstone Filter

**Verdict: ✅ FEASIBLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/Rapidash/src/main/java/kdrange/KDNode.java:79–86` | `del()` — soft delete bằng `deleted` flag. |
| `base_repo/Rapidash/src/main/java/kdrange/KDNode.java:30–38` | `edit()` — re-insert với `deleted` flag toggle. |
| `docs/design/extracted_content.txt` (mục 5.4) | "TombstoneFilter tại lá bỏ ghost/retracted data O(1)." |

**Phân tích:**

- Rapidash gốc dùng `deleted` flag per node → O(1) toggle nhưng vẫn chiếm space trong tree.
- WAVES dùng **external filter** (Set-based) → tách ghost data khỏi tree.
- O(1) lookup của Set-based filter là **standard CS fact** (Python `set`).

**Kết luận:** Không có vấn đề gì. Pattern rõ ràng, O(1) guarantee bằng Python `set`.

---

### 7. Watermark + Late Data Handling

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `docs/design/extracted_content.txt` (mục 5.4) | Đặc tả đầy đủ: provisional vs final violation, watermark trigger, pending violation state. |
| `docs/design/extracted_content.txt` (mục 3.3) | "Watermark dùng để quyết định điều gì, provisional violation khác final violation ở đâu." |
| `base_repo/Icewafl/src/Polluters/TabularPolluters/KeyedStreamPolluters/delay_tuple_batch_temporal_polluter.py` | Delay tuple temporal polluter — có implement. |

**Phân tích:**

- **Watermark semantics** trong design doc rõ ràng: không block, chỉ advance khi có data.
- **Late event handling** trong Icewafl có implement `delay_tuple` — có thể dùng làm reference.

**Vấn đề tiềm ẩn:**

1. **`wait_for_late` duration**: Design doc nói 300s default. Không có analysis về con số này. NYC Taxi window = 1h → 300s = 8.3% của window. Cần verify đủ để capture late data thực tế.

2. **`HandleLateEvent.recheck()`**: Pseudocode trong blueprint gọi lại `process_event_engine` + `traverse_node` cho late event. Đây là **expensive operation** — có thể tạo O(N) re-check khi nhiều late events cùng đến. Không có deduplication strategy rõ ràng trong design doc.

3. **`extract_pane_from_event`**: Cần event có `pane_id` field — nhưng `DataEvent` trong blueprint KHÔNG có `pane_id`. Cần verify design: `DataEvent` có `window_id` nhưng không có `pane_id`. Event được gán pane khi nào? Trong `assign_window`, `pane_id=None`. → **Thiếu thiết kế rõ ràng về khi nào event nhận pane_id.**

4. **`late_event_does_invalidate()`**: Pseudocode trả `True # TODO`. Đây là **core logic của late handling** — cần implement cụ thể:
   - DC1: late event t có fare thấp hơn → retract alert nếu alert nói s.fare > t.fare
   - DC2/DC3: tương tự
   - Logic này phức tạp hơn `True`.

**Kết luận:** Watermark concept và provisional/final pattern **feasible**. Nhưng 3 implementation details quan trọng cần giải quyết trước khi implement: `pane_id` assignment, `recheck()` efficiency, và `late_event_does_invalidate()` logic.

---

### 8. Alert Decision Layer

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `docs/design/extracted_content.txt` (mục 5.4) | "Alert State Store: lưu candidate violations, provisional alerts và lịch sử quyết định chưa chốt." |
| `docs/design/extracted_content.txt` (mục 5.4) | "key = window_id, rule_id, entity_id hoặc alert_id." |

**Phân tích:**

- `AlertStateStore` interface rõ ràng. In-memory Dict là feasible cho prototype.
- RocksDB là upgrade option cho production — nhưng **không cần cho benchmark**.

**Vấn đề tiềm ẩn:**

1. **`alert_id` deduplication**: `process_candidate` dùng `alert_id = f"alert_{candidate.dc_id}_{candidate.window_id}_{candidate.query_id}"`. Nhưng nếu late event retract rồi tạo alert mới → alert_id khác (vì query_id khác) → không deduplicate được. Cần kiểm tra.

2. **`all_event_ids` indexing**: `AlertStateStore._by_event` index theo TẤT CẢ event_ids trong violation. Nếu violation có 100 matched_ids → alert được index 101 lần. Với 10K alerts × 100 matches → 1M index entries → có thể là bottleneck.

3. **`cleanup_expired` O(N) scan**: Duyệt toàn bộ store để tìm expired alerts → O(N). Cần dùng priority queue hoặc index theo `finalized_at` để optimize.

**Kết luận:** Interface đúng. Rủi ro ở performance (indexing strategy, cleanup) và edge cases (deduplication).

---

### 9. NYC Taxi + DC1 / DC2 / DC3

**Verdict: ✅ FEASIBLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `docs/design/extracted_content.txt` (mục 3.1) | "NYC Taxi Yellow Cab là nguồn benchmark chuẩn." |
| `docs/design/extracted_content.txt` (mục 3.2) | "DC1: cùng trip_distance → xe ngắn không fare > xe dài. DC2: cùng tuyến → duration chênh ngoài biên độ EMA. DC3: cùng tuyến → tolls chênh bất thường." |
| NYC Taxi public dataset | https://www.nyc.gov/site/tlc/about/trip-record-data.page — có đầy đủ fields. |

**Phân tích:**

- DC1, DC2, DC3 đều là **cross-record comparisons** — phù hợp với Rapidash's DC checking approach.
- `trip_distance`, `fare_amount`, `trip_duration`, `tolls_amount` đều có trong NYC Taxi raw data.
- `trip_duration` cần derived: `tpep_dropoff_datetime - tpep_pickup_datetime` — feasible trong preprocessing.

**Kết luận:** Dataset và DC definitions **feasible**. Không có vấn đề.

---

### 10. Baseline Systems (NL-Stream, Single-Tree-DaQ, Static-Box-DaQ, WAVES-SingleRule, Buffer-Wait-DaQ)

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `docs/design/extracted_content.txt` (mục 4.1) | Bảng baselines đầy đủ. |
| `docs/base/Rapidash_context.md` | NL-Stream = O(N²) brute-force — reference có. |

**Phân tích:**

| Baseline | Xây dựng từ | Độ khó |
|----------|-------------|--------|
| NL-Stream | Nested loop trong Python | Thấp ✅ |
| Single-Tree-DaQ | Một KD-Tree cho toàn bộ window | Trung bình ⚠️ |
| Static-Box-DaQ | WAVES + static box (không EMA) | Thấp ✅ |
| WAVES-SingleRule | WAVES + 1 rule (không shared optimizer) | Trung bình ⚠️ |
| Buffer-Wait-DaQ | WAVES + watermark như barrier | Trung bình ⚠️ |

**Vấn đề tiềm ẩn:**

1. **Single-Tree-DaQ** cần maintain một KD-Tree động (incremental insert + delete từng điểm). Rapidash gốc không có incremental insert. Cần implement riêng hoặc dùng Weever approach mà không có pane → vẫn cần O(N) rebuild khi delete.

2. **Buffer-Wait-DaQ** dùng watermark như barrier — khác với design doc nói watermark KHÔNG block. Nếu watermark block, throughput sẽ thấp → benchmark không fair. Cần làm rõ semantics.

3. **6 baselines × 4 ablation × 4 RQs × 4 datasets** = 384 configurations — đây là con số rất lớn. Cần estimate runtime và resource.

**Kết luận:** Tất cả baselines **conceptually feasible**, nhưng 2–3 trong số đó cần implementation effort đáng kể (Single-Tree, Buffer-Wait). Cần ước tính experiment time trước.

---

### 11. Icewafl Data Injection

**Verdict: ⚠️ QUESTIONABLE**

**Cơ sở:**

| Nguồn | Bằng chứng |
|--------|------------|
| `base_repo/Icewafl/src/Polluters/TabularPolluters/StreamPolluters/stream_polluter.py` | Base class có sẵn. |
| `base_repo/Icewafl/src/Polluters/TabularPolluters/KeyedStreamPolluters/delay_tuple_batch_temporal_polluter.py` | Delay tuple polluter — late/OoO injection. |
| `base_repo/Icewafl/src/Polluters/TabularPolluters/StreamPolluters/AttributePolluter/gaussian_noise_attribute_polluter.py` | Gaussian noise — fraud injection. |
| `docs/base/Icewafl_context.md` | Paper EDBT 2025. |

**Phân tích:**

Icewafl gốc chạy trên **Apache Flink** (Python API). WAVES chạy trên **Pathway**. Đây là **platform mismatch** — không thể dùng trực tiếp Icewafl.

**Cần xây dựng lại injection pipeline:**
- Fraud injection (DC1–DC3): `scripts/inject_fraud.py` — có thể dùng Pandas để modify dataset offline, sau đó replay.
- Concept drift: `scripts/inject_drift.py` — modify trip_duration theo time-of-day pattern.
- Late data: `scripts/inject_late.py` — dùng `delay_tuple_batch_temporal_polluter.py` làm reference, viết lại bằng Pandas.

**Vấn đề tiềm ẩn:**

1. **Sort theo IngestionTime bước cuối cùng** — design doc nói phải sort. Nhưng với late injection, sort sẽ **loại bỏ effect của late data** (vì late events được sắp xếp vào đúng vị trí). Đây là **fundamental tension**:
   - Sort để simulate real-world ingestion order.
   - Nhưng nếu sort lại, late events nằm ở vị trí "đúng" theo event_time → không còn late nữa.
   
   → Cần rõ ràng: sort theo **ingestion_time**, không sort theo event_time. Và output phải GIỮ THỨ TỰ INGESTION trong file để replay đúng.

2. **`icewafl_context.md` ghi "Frozen Value" và "Timestamp Error" là NOT FOUND** — 2 trong số các error types không có trong Icewafl. Cần implement thêm.

**Kết luận:** Icewafl là **reference**, không phải **dependency**. Cần implement lại injection pipeline. Frozen Value và Timestamp Error cần bổ sung.

---

### 12. Các lỗi thuật toán nghiêm trọng (TUYỆT ĐỐI PHẢI SỬA)

**Tất cả 4 bugs dưới đây được phát hiện bằng dry-run thuật toán trên giấy. Đã sửa trong checklist.**

#### Bug A: `traverse_node` early-return — bỏ qua cả subtree phải

| | Chi tiết |
|--|----------|
| **Vị trí** | Section 2.6.3, bước 4 |
| **Mô tả** | Khi `query_point` nằm trong `box`, thuật toán return ngay lập tức sau khi collect matches từ `node` ( lá hiện tại). Tuy nhiên, `node` có thể không phải là lá — nó có thể là internal node mà cả hai subtree (trái và phải) đều chứa points nằm trong box. |
| **Hậu quả** | Violations bị miss nếu query_point đi qua internal node rồi đi vào subtree phải mà không gặp lá nào nằm trong box tại bước hiện tại. Precision cao hơn thực tế (thiếu false positive thật ra là thiếu TP). |
| **Sửa** | Không early-return. Luôn duyệt cả 2 nhánh. Nếu query_point trong box → tạo candidate object trước, duyệt cả 2 nhánh xong rồi gắn matched_ids. |
| **Trạng thái** | ✅ ĐÃ SỬA |

#### Bug B: Retraction — mark RETRACTED nhưng không xóa khỏi store

| | Chi tiết |
|--|----------|
| **Vị trí** | Section 2.8.3, `retract_alert` pseudocode |
| **Mô tả** | Retraction chỉ đổi `status = RETRACTED` và gọi `put()`. Alert vẫn nằm trong `_store`, `_by_window`, `_by_event`. |
| **Hậu quả** | `get_by_event_id()` trả về RETRACTED alert → HandleLateEvent retract lại alert đã retract → duplicate retraction, wasted work. `cleanup_expired` duyệt qua RETRACTED alerts → memory leak. |
| **Sửa** | `retract()` xóa alert khỏi cả `_store`, `_by_window`, `_by_event`. Chỉ keep alert_id trong metrics log. |
| **Trạng thái** | ✅ ĐÃ SỬA |

#### Bug C: Deduplication chỉ check existence, không check status

| | Chi tiết |
|--|----------|
| **Vị trí** | Section 2.8.3, `process_candidate` pseudocode |
| **Mô tả** | `IF alert_store.get(alert_id) IS NOT None: RETURN []`. Nếu alert đã bị retract (đã xóa), `get()` trả `None` → tạo alert mới. Đúng. Nhưng nếu retract xóa alert khỏi store → `get()` return `None` → vẫn tạo alert mới. ĐÚNG. Nhưng vấn đề là: sau khi retract, alert đã bị xóa khỏi store (Bug B đã fix), nên deduplication đã hoạt động đúng. Tuy nhiên, nếu retract CHỈ mark mà không xóa (trước fix), thì `get()` vẫn return RETRACTED alert → skip tạo mới → ĐÚNG NHƯNG SAI VỀ MẶT SEMANTICS vì RETRACTED alert không nên occupy store space. |
| **Sửa** | Kết hợp với Bug B: retract phải xóa khỏi store. Deduplication check `existing.status != RETRACTED`. |
| **Trạng thái** | ✅ ĐÃ SỬA (kết hợp Bug B) |

#### Bug D: `kd_insert_incremental` — không tồn tại trong pane-based system

| | Chi tiết |
|--|----------|
| **Vị trí** | Section 2.7, `pane_insert` pseudocode |
| **Mô tả** | Khi pane đã close (`is_active=False`), thuật toán gọi `kd_insert_incremental()` để insert event vào KD-Tree đã bulk-load. Nhưng KD-Tree sau bulk-load là **immutable**. Incremental insert không được định nghĩa trong blueprint, và implement nó đòi hỏi rebalancing đầy đủ. |
| **Hậu quả** | Late events cần insert vào closed pane → không biết làm thế nào. |
| **Sửa** | Pane-based system KHÔNG insert vào closed pane. Late events được xử lý qua `HandleLateEvent`, không qua `pane_insert`. Closed pane chỉ được query (read-only). |
| **Trạng thái** | ✅ ĐÃ SỬA |

---

## Tổng hợp các vấn đề cần giải quyết

### 🔴 Cao (block implement nếu không fix)

1. **`DataEvent` thiếu `pane_id`**: Event được gán pane khi nào? Trong `assign_window`, `pane_id=None`. Nhưng `extract_pane_from_event()` và `tombstone_mgr.contains()` cần pane_id. → **Cần bổ sung `pane_id` assignment trong Window Manager flow.**

2. **`infer_static_bounds()` là TODO**: DC1–DC3 cần static bounds. Không có bounds → không build được Elastic Box. → **Cần implement từ NYC Taxi data analysis hoặc config file.**

3. **`late_event_does_invalidate()` trả `True # TODO`**: Không implement logic này thì retraction không bao giờ được trigger đúng cách. → **Cần implement chi tiết theo DC semantics.**

### 🟡 Trung bình (nên fix trước khi benchmark)

4. **`cleanup_expired` O(N) scan**: Alert store có thể grow unbounded. → **Cần index theo `finalized_at` hoặc dùng heap.**

5. **`all_event_ids` indexing**: 1 alert × 100 matches × 10K alerts = 1M entries. → **Cần evaluate xem có bottleneck không với dataset size thực tế.**

6. **`wait_for_late` 300s**: Không có analysis. → **Cần justify hoặc test với nhiều giá trị.**

7. **Single-Tree-DaQ baseline**: Cần incremental KD-Tree (Rapidash gốc không có). → **Cần implement riêng hoặc dùng `sortedcontainers.SortedList`.**

8. **Icewafl platform mismatch**: Flink → Pathway. → **Cần xây lại injection pipeline.**

### 🟢 Thấp (cải thiện sau)

9. **`k_max` = 5**: DC1–DC3 chỉ cần k=2. RQ3 test 50–100 rules → cần justify.
10. **`infinite_padding` performance**: Padding (-∞, +∞) cho unused dimensions → cần measure.
11. **Frozen Value / Timestamp Error**: NOT FOUND trong Icewafl → cần implement.
12. **6 baselines × 4 ablation × 4 datasets**: Estimate runtime trước khi commit.

---

## Kết luận

**Tổng thể: WAVES blueprint KHẢ THI về mặt kiến trúc.**

Tất cả các thành phần core đều có cơ sở từ source code hoặc design doc:
- ✅ KD-Tree + Box Dropping — **100% source code có sẵn**
- ✅ Pane-based Forest — **100% pattern có trong Weever**
- ✅ Stream DaQ windowing — **code có sẵn trong base repo**
- ✅ Tombstone Filter — **standard CS, trivial**
- ✅ DC1–DC3 + NYC Taxi — **dataset và rules rõ ràng**

**Phần ⚠️ QUESTIONABLE** không phải vì "bất khả thi" mà vì:
1. Thiếu design chi tiết ở 3 điểm (pane_id, static_bounds, late_event_invalidate)
2. Platform adaptation (Icewafl Flink → WAVES Pathway)
3. Performance tuning cần empirical validation

**Recommendation**: Fix 3 issues 🔴 trước khi bắt đầu implement. Sau đó implement theo thứ tự: 2.1 (Stream DaQ) → 2.6 (Rapidash) → 2.7 (Weever) → 2.2 (Window) → 2.4 (Logical Engine) → 2.5 (Optimizer) → 2.8 (Decision) → 2.9 (Tombstone) → 2.10 (Late Handler) → 2.11 (Output) → Scripts → Tests.
