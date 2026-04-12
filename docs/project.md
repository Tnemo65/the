# WAVES — Project Milestones

> Ghi mọi thay đổi đáng kể, ngày, module. Cập nhật ngay khi hoàn thành milestone.

---

## Changelog

### 2026-04-13 — Phase 2.10/2.11/2.11b/2.11c: LateHandler + EventStore + AlertOutput + Pipeline

**Trạng thái:** Hoàn thành

**Thay đổi:**

Phase 2.11b — EventStore:
- `waves/store/event_store.py` — EventStore (event_id→DataEvent, pane_id, window_id), put/get/get_pane_id/get_window_id/has/count/clear
- `waves/store/__init__.py` — Export EventStore
- `tests/unit/test_store.py` — 16 tests

Phase 2.10 — LateHandler:
- `waves/late_handler/handler.py` — LateHandlerConfig, handle_late_event (5-step), late_event_invalidate_check, _parse_window_end, _extract_point, _evaluate_predicate
- `waves/late_handler/__init__.py` — Export LateHandlerConfig, handle_late_event, late_event_invalidate_check
- `tests/unit/test_late_handler.py` — 30 tests

Phase 2.11 — AlertOutput:
- `waves/output/alert_output.py` — AlertEvent (event_type: provisional/final/retraction), AlertOutput (emit + emit_meta + sinks + history)
- `waves/output/__init__.py` — Export AlertEvent, AlertOutput
- `waves/__init__.py` — Add AlertEvent, AlertOutput
- `tests/unit/test_output.py` — 19 tests

Phase 2.11c — Pipeline:
- `waves/pipeline/pipeline.py` — PipelineConfig (window, late, optimizer, sink), WavePipeline (wires all modules: process/DataEvent, process_late, finalize_window, cleanup, emit_meta, build_window_meta)
- `waves/pipeline/__init__.py` — Export PipelineConfig, WavePipeline
- `tests/unit/test_pipeline.py` — 18 tests

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA mean/variance, ElasticBox padding, unit tests 35/35 pass)
- 2.5 optimizer ✅ (config: NYC_TAXI_BOUNDS; dc_parser; grouper; unit tests 44/44 pass)
- 2.6 rapidash ✅ (kdtree: KDTreeNode/bulk_load; traversal: traverse_node/BatchedTraversal; unit tests 60/60 pass)
- 2.7 weever ✅ (PaneForest: pane_insert/pane_close/window_slide; unit tests 25/25 pass)
- 2.8 decision ✅ (AlertStateStore; process_candidate/finalize_window/retract_alert/cleanup_expired; unit tests 35/35 pass)
- 2.9 tombstone ✅ (TombstoneFilter/TombstoneManager; unit tests 13/13 pass)
- 2.10 late_handler ✅ (handle_late_event 5-step; unit tests 30/30 pass)
- 2.11 output ✅ (AlertOutput: emit/emit_meta/sinks/history; unit tests 19/19 pass)
- 2.11b store ✅ (EventStore; unit tests 16/16 pass)
- 2.11c pipeline ✅ (WavePipeline: wires all modules; unit tests 18/18 pass)

**Tổng test: 370/370 pass** ✅

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
- [ ] 0.5 Nguyên tắc vận hành (chưa cần code)

### Phase 1: Traceability
- [ ] 1.1 Bảng Research Questions
- [ ] 1.2 Luồng tài liệu → code → dữ liệu → paper

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
- [ ] 2.13 Integration tests

### Phase 3: Dữ liệu
- [ ] B1: NYC Taxi base prep
- [ ] B2: Drift injection
- [ ] B3: Fraud injection (DC1–DC3)
- [ ] B4: Late/OoO injection

### Phase 4: Thực nghiệm
- [ ] Baseline runners
- [ ] Metrics collection
- [ ] Sensitivity analysis (E1, E2, E3)

### Phase 5: Paper
- [ ] Viết paper

### Phase 6: Đóng dự án
- [ ] Tag release
- [ ] Reproducibility documentation
