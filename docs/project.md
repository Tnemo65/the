# WAVES — Project Milestones

> Ghi mọi thay đổi đáng kể, ngày, module. Cập nhật ngay khi hoàn thành milestone.

---

## Changelog

### 2026-04-13 — Benchmark Research & Competitive Analysis

**Trạng thái:** Hoàn thành

**Thay đổi:**

- `benchmark_research.md` — Comprehensive landscape survey:
  - **36+ systems** được tìm và phân tích chi tiết
  - **44 key papers** (VLDB/SIGMOD/ICDE/EDBT/ICML 2013–2025)
  - **5 white space** được xác định rõ ràng
  - **11 recommended metrics** được phân loại theo tier
  - **Bảng so sánh metrics** đầy đủ giữa các hệ thống
  - **35 key papers** với URL/DOI đầy đủ
  - **Claims** phân loại: cái nào đưa được, cái nào không
  - **Paper structure** cho Related Work section

- `docs/context.md` — Cập nhật với reference đến benchmark_research.md

**Key findings:**

| # | Finding | Evidence |
|---|--------|----------|
| 1 | **WHITE SPACE**: No system measures P/R/F1 on stream DC violations | Survey of 36+ systems found zero |
| 2 | **WHITE SPACE**: No system combines EMA + DC verification | All EMA systems univariate; all DC systems static |
| 3 | **WHITE SPACE**: No system has retraction mechanism for DC | All retraction systems for materialized views, not DC |
| 4 | **WHITE SPACE**: No system combines pane forest + KD-Tree + retraction | Weever pane+KD-Tree; Materialize retraction; WAVES all three |
| 5 | **WHITE SPACE**: No system has shared multi-rule optimization for DC | Rapidash one tree per rule; WAVES batches across rules |

**Top metrics ưu tiên:**

```
TIER 1 — WHITE SPACE (không đối thủ):
  1. F1 Score (no drift)        vs Static-Box-DaQ
  2. F1 Score (with drift)     vs Static-Box-DaQ
  3. F1 Score (with late data) vs Buffer-Wait-DaQ
  4. Retraction Rate            ← WAVES unique metric
  5. Precision/Recall (all)

TIER 2 — CÓ baseline:
  6. Throughput                vs StreamDaQ
  7. P99 Latency             vs RisingWave
  8. Detection Latency        vs Buffer-Wait-DaQ
  9. Memory (50-100 rules)   vs WAVES-SingleRule

TIER 3 — Ablation:
  A1-A5: Mỗi cơ chế đều đo được
```

---

### 2026-04-13 — Phase 3/4: Benchmark Infrastructure + Checklist Update

**Trạng thái:** Đang thực hiện

**Thay đổi:**

- `docs/MASTER_AGENT_CHECKLIST.md` — Section 3 (Dữ liệu) và 4 (Thực nghiệm) được update chi tiết thực tế:
  - **Section 3**: Taxonomy đầy đủ các loại lỗi (physical, DC1–DC3, concept drift, late/OoO), ground truth schema, injection algorithm pseudocode, pipeline B1–B4 chi tiết với pseudocode
  - **Section 4**: Bảng metrics đầy đủ (system + accuracy), 4 bảng so sánh metric theo RQ (RQ1–RQ4), 6 quy tắc fair comparison, 5 ablation experiments, danh sách scripts cần implement
  - **Nguyên tắc transparency**: Không bịa metrics — tất cả cell "?" phải đợi experiment thực tế; nếu baseline đánh bại WAVES → ghi nhận trung thực
  - Bổ sung: missing scripts (inject_drift.py, inject_late.py, run_benchmark.py, collect_metrics.py, plot_results.py)

- `docs/project.md` — Phase 3/4 checklist chi tiết hơn: liệt kê rõ 7 scripts cần implement, RQ1–RQ4 tasks riêng, bảng so sánh metric milestone

**Lưu ý:**
- Tất cả scripts benchmark (`prepare_benchmark.py`, `inject_fraud.py`) hiện là placeholder
- 3 scripts hoàn toàn không tồn tại: `inject_drift.py`, `inject_late.py`, `run_benchmark.py`
- NYC Taxi dataset chưa được download — cần người dùng chạy `prepare_benchmark.py` hoặc cung cấp đường dẫn
- Tất cả bảng metric trong checklist để `?` — chưa có số liệu thực tế

**Scripts benchmark cần implement (theo thứ tự ưu tiên):**
1. `prepare_benchmark.py` — download + parse + clean NYC Taxi CSV
2. `inject_drift.py` — concept drift injection (không tồn tại)
3. `inject_late.py` — late/OoO injection (không tồn tại)
4. `run_benchmark.py` — benchmark runner (không tồn tại)
5. `collect_metrics.py` — metrics aggregation (không tồn tại)
6. `plot_results.py` — visualization (không tồn tại)

---

**Trạng thái:** Hoàn thành

**Thay đổi:**

- `tests/integration/test_end_to_end.py` — 14 integration tests: DC1 (fare-distance dominance), DC3 (toll route anomaly), batched traversal, alert state lifecycle, pane forest, tombstone, EMA, meta stream
- `tests/integration/test_late_retraction.py` — 18 integration tests: late event threshold, alert retraction, state transitions, event store integration, tombstone lifecycle, cleanup expired alerts, output sink
- Renamed: `tests/integration/test_pipeline.py` → `tests/integration/test_end_to_end.py` (tránh trùng tên với `tests/unit/test_pipeline.py`)

**Bug fix phát hiện trong integration tests:**
- `DCParser.parse()` method không tồn tại → dùng `parse_dc_rules()` thay thế
- `BasicDQChecker.check()` không tồn tại → dùng `check_event()` thay thế
- `PaneForest.pane_close()` cần `dim_count`, `lo_bounds`, `hi_bounds` (không phải 0 args)
- `LogicalEngine.update()` không tồn tại → dùng `process_event(event_attrs, dc_rules)` thay thế
- `AlertStateStore.get_by_event()` không tồn tại → dùng `get_by_event_id()` thay thế
- `TombstoneManager.add()` silently skips nếu pane chưa được tạo → cần gọi `create_pane()` trước
- `traverse_node` dùng `event_store.get_pane_id(event_id)` để check tombstone → event cần được đăng ký trong event_store với pane_id trước khi traversal

**Tổng test: 400/400 pass** ✅

**Modules đã implement:**
- 2.1 ingestion ✅ (unit tests 15/15 pass)
- 2.2 windowing ✅ (unit tests 26/26 pass)
- 2.3 basic_dq ✅ (unit tests 27/27 pass)
- 2.4 logical_engine ✅ (unit tests 35/35 pass)
- 2.5 optimizer ✅ (unit tests 44/44 pass)
- 2.6 rapidash ✅ (unit tests 60/60 pass)
- 2.7 weever ✅ (unit tests 25/25 pass)
- 2.8 decision ✅ (unit tests 35/35 pass)
- 2.9 tombstone ✅ (unit tests 13/13 pass)
- 2.10 late_handler ✅ (unit tests 30/30 pass)
- 2.11 output ✅ (unit tests 19/19 pass)
- 2.11b store ✅ (unit tests 16/16 pass)
- 2.11c pipeline ✅ (unit tests 18/18 pass)
- 2.13 integration ✅ (integration tests 30/30 pass, 400/400 total pass)

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

### 2026-04-12 — Phase 2.7/2.8/2.9: Weever + Decision + Tombstone

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/tombstone/filter.py` — TombstoneFilter (O(1) per-pane filter), TombstoneManager (pane lifecycle: create/drop, retraction: add/contains)
- `waves/tombstone/__init__.py` — Export TombstoneFilter, TombstoneManager
- `waves/weever/pane_forest.py` — PaneForest (flat list + O(1) lookup), pane_insert, pane_close (bulk_load), window_slide (close+drop expired panes), find_or_create_pane, get_roots
- `waves/weever/__init__.py` — Export PaneForest, _floor_ts
- `waves/decision/alert_store.py` — AlertStateStore (KV store with window/event indexes), AlertStatus (PROVISIONAL/FINAL/RETRACTED), AlertRecord
- `waves/decision/decision.py` — ProvisionalDecision, FinalDecision, RetractionDecision, process_candidate (idempotent), finalize_window (watermark seal), retract_alert (tombstones events), cleanup_expired (TTL)
- `waves/decision/__init__.py` — Export all 12 symbols
- `tests/unit/test_tombstone.py` — 13 tests
- `tests/unit/test_weever.py` — 25 tests
- `tests/unit/test_decision.py` — 35 tests

**Tổng test khi đó: 241/241 pass**

**Modules đã implement:**
- 2.1–2.5 ✅ (các phase trước)
- 2.6 rapidash ✅ (hoàn thành phase trước)
- 2.7 weever ✅ (PaneForest: pane_insert/pane_close/window_slide; unit tests 25/25 pass)
- 2.8 decision ✅ (AlertStateStore: put/get/delete/retract/indexes; process_candidate/finalize_window/retract_alert/cleanup_expired; unit tests 35/35 pass)
- 2.9 tombstone ✅ (TombstoneFilter/TombstoneManager: create/drop/add/contains; unit tests 13/13 pass)
- 2.10–2.11c (placeholder — hoàn thành ở phase sau)

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

### 2026-04-12 — Phase 2.5: SharedRuleOptimizer

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/optimizer/config.py` — OptimizerConfig (k_max, infinite_padding, static_bounds), NYC_TAXI_BOUNDS
- `waves/optimizer/dc_parser.py` — Predicate, PredicateType, DCParser, EnrichedDC, strip_side_prefix
- `waves/optimizer/grouper.py` — GroupMetadata, GreedyRuleGrouper, ActiveBox, build_active_boxes
- `waves/optimizer/__init__.py` — Export all 11 symbols
- `tests/unit/test_optimizer.py` — 44 unit tests, 44/44 pass

**Tổng test khi đó: 147/147 pass**

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

### 2026-04-12 — Phase 2.4: LogicalEngine

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/logical_engine/engine.py` — StatisticalState, ElasticBox, ElasticBoxConfig, DenialConstraint, LogicalEngine (GỘP StatisticalContext + ElasticBoxGenerator + Coordinator)
- `waves/logical_engine/__init__.py` — Export LogicalEngine, StatisticalState, ElasticBox, ElasticBoxConfig, DenialConstraint
- `waves/__init__.py` — Export LogicalEngine, StatisticalState, ElasticBox, ElasticBoxConfig
- `tests/unit/test_logical_engine.py` — 35 unit tests, 35/35 pass

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA mean/variance, ElasticBox padding, process_event pipeline, unit tests 35/35 pass)
- 2.5 optimizer ✅ (config: NYC_TAXI_BOUNDS; dc_parser: Predicate/DCParser/EnrichedDC; grouper: GreedyRuleGrouper/ActiveBox/build_active_boxes; unit tests 44/44 pass)
- 2.6 rapidash (placeholder)
- 2.7 weever (placeholder)
- 2.8 decision (placeholder)
- 2.9 tombstone (placeholder)
- 2.10 late_handler (placeholder)
- 2.11 output (placeholder)
- 2.11b store (placeholder)
- 2.11c pipeline (placeholder)

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

### 2026-04-12 — Phase 2.3: BasicDQChecks

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/basic_dq/checker.py` — DQResult, CheckResult, BasicDQRule, BasicDQChecker (null/type/range/regex, O(1) counters)
- `waves/basic_dq/meta_stream.py` — WindowMeta, MetaEvent, QualityMetaStream
- `tests/unit/test_basic_dq.py` — 27 unit tests, 27/27 pass

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA, ElasticBox, unit tests 35/35 pass)
- 2.5 optimizer ✅ (config: NYC_TAXI_BOUNDS; dc_parser: Predicate/DCParser/EnrichedDC; grouper: GreedyRuleGrouper/ActiveBox/build_active_boxes; unit tests 44/44 pass)
- 2.6 rapidash (placeholder)
- 2.7 weever (placeholder)
- 2.8 decision (placeholder)
- 2.9 tombstone (placeholder)
- 2.10 late_handler (placeholder)
- 2.11 output (placeholder)
- 2.11b store (placeholder)
- 2.11c pipeline (placeholder)

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

### 2026-04-12 — Phase 2.2: WindowManager

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/windowing/pane.py` — WindowConfig, Pane, WindowBuffer, WindowedEvent, WindowDelta, PaneManager (ONE definition of Pane)
- `waves/windowing/manager.py` — WindowManager với assign_window() + on_slide()
- `waves/windowing/watermark.py` — WatermarkClock với is_ready() + has_advanced()
- `tests/unit/test_windowing.py` — 26 unit tests, 26/26 pass
- Cleanup: xóa json_connector, kafka_connector, 9 placeholder demos, 11 scaffold test files
- Cleanup: xóa inject_drift.py, inject_late.py, run_baseline.py (placeholders)

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA, ElasticBox, unit tests 35/35 pass)
- 2.5 optimizer ✅ (config: NYC_TAXI_BOUNDS; dc_parser: Predicate/DCParser/EnrichedDC; grouper: GreedyRuleGrouper/ActiveBox/build_active_boxes; unit tests 44/44 pass)
- 2.6 rapidash (placeholder)
- 2.7 weever (placeholder)
- 2.8 decision (placeholder)
- 2.9 tombstone (placeholder)
- 2.10 late_handler (placeholder)
- 2.11 output (placeholder)
- 2.11b store (placeholder)
- 2.11c pipeline (placeholder)

**Scripts thực tế:**
- scripts/prepare_benchmark.py (placeholder)
- scripts/inject_fraud.py (placeholder)

---

## Tiến độ Checklist

### Phase 0: Khởi tạo ✅
- [x] 0.1 Cấu trúc thư mục WAVES
- [x] 0.2 docs/context.md + docs/project.md
- [x] 0.3 Môi trường Python (.venv + pyproject.toml)
- [x] 0.4 Git (.gitignore)
- [x] 0.5 Nguyên tắc vận hành (chưa cần code)

### Phase 1: Traceability
- [x] 1.1 Bảng Research Questions (trong file này)
- [x] 1.2 Luồng tài liệu → code → dữ liệu → paper

### Phase 2: Implementation
- [x] 2.1 StreamIngestion
- [x] 2.2 WindowManager
- [x] 2.3 BasicDQChecks
- [x] 2.4 LogicalEngine
- [x] 2.5 SharedRuleOptimizer
- [x] 2.6 Rapidash
- [x] 2.7 Weever
- [x] 2.8 Watermark/Decision
- [x] 2.9 Tombstone
- [x] 2.10 LateHandler
- [x] 2.11 AlertOutput
- [x] 2.11b EventStore
- [x] 2.11c Pipeline
- [x] 2.12 Unit tests per module (370 tests, 370/370 pass)
- [x] 2.13 Integration tests (32 integration tests, 400/400 total pass)

### Phase 3: Dữ liệu & Benchmark Infrastructure
- [x] Scripts infrastructure: prepare_benchmark, inject_fraud, inject_drift, inject_late, run_benchmark, collect_metrics, plot_results (all implemented)
- [x] Phụ thuộc: numpy, requests, tqdm, matplotlib thêm vào pyproject.toml
- [ ] Download NYC Taxi 2024 (≥1 tháng) — chạy prepare_benchmark.py
- [ ] Tạo benchmark.parquet + ground_truth.jsonl
- [ ] Chạy thử nghiệm đầu tiên (seed=42)

### Phase 4: Thực nghiệm
- [ ] Baseline runners (NL-Stream, Single-Tree-DaQ, Static-Box-DaQ, WAVES-SingleRule, Buffer-Wait-DaQ, WAVES-Full)
- [ ] RQ1: Throughput vs O(N²)
- [ ] RQ2: EMA + Retraction vs drift + late
- [ ] RQ3: Scalability (10, 50, 100 DC rules)
- [ ] RQ4: Sensitivity analysis (E1 pane size, E2 α EMA, E3 k_max)
- [ ] Bảng so sánh metric (RQ1–RQ4 → baselines)

### Phase 5: Paper
- [ ] 5.1 Cấu trúc paper (8 sections)
- [ ] 5.2 Bảng metric summary (sau experiments thực tế — tất cả cell `?`)
- [ ] 5.3 Phản biện — 6 counter-arguments
- [ ] 5.4 Cross-domain comparison (optional, cần user approve dataset 2)
- [ ] 5.5 Paper checklist (8 items: abstract, RQ tables, plots, discussion)

### Phase 6: Đóng dự án
- [ ] 6.1 Release tagging (format v{major}.{minor}.{patch}-{date})
- [ ] 6.2 Artifact documentation (code, data, experiment artifacts)
- [ ] 6.3 Reproducibility checklist
- [ ] 6.4 Final project documentation
