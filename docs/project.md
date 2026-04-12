# WAVES — Project Milestones

> Ghi mọi thay đổi đáng kể, ngày, module. Cập nhật ngay khi hoàn thành milestone.

---

## Changelog

### 2026-04-12 — Phase 2.6: Rapidash

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/rapidash/candidate.py` — CandidateViolation, BatchedTraversalResult
- `waves/rapidash/kdtree.py` — KDTreeNode, bulk_load, range_query, _argmax_range, _boxes_intersect (DEFAULT_LEAF_SIZE=16)
- `waves/rapidash/traversal.py` — Intersects, point_in_box, traverse_node, BatchedTraversal
- `waves/rapidash/__init__.py` — Export all 9 symbols (added bulk_load, range_query)
- `tests/unit/test_rapidash.py` — 60 unit tests, 60/60 pass

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA mean/variance, ElasticBox padding, unit tests 35/35 pass)
- 2.5 optimizer ✅ (config: NYC_TAXI_BOUNDS; dc_parser: Predicate/DCParser/EnrichedDC; grouper: GreedyRuleGrouper/ActiveBox/build_active_boxes; unit tests 44/44 pass)
- 2.6 rapidash ✅ (kdtree: KDTreeNode/bulk_load/range_query; traversal: BatchedTraversal/traverse_node/Intersects/point_in_box; candidate: CandidateViolation/BatchedTraversalResult; unit tests 60/60 pass)
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

### 2026-04-12 — Phase 2.5: SharedRuleOptimizer

**Trạng thái:** Hoàn thành

**Thay đổi:**
- `waves/optimizer/config.py` — OptimizerConfig (k_max, infinite_padding, static_bounds), NYC_TAXI_BOUNDS
- `waves/optimizer/dc_parser.py` — Predicate, PredicateType, DCParser, EnrichedDC, strip_side_prefix
- `waves/optimizer/grouper.py` — GroupMetadata, GreedyRuleGrouper, ActiveBox, build_active_boxes
- `waves/optimizer/__init__.py` — Export all 11 symbols
- `tests/unit/test_optimizer.py` — 44 unit tests, 44/44 pass

**Modules đã implement:**
- 2.1 ingestion ✅ (schema, connectors, unit tests 15/15 pass)
- 2.2 windowing ✅ (pane, manager, watermark, unit tests 26/26 pass)
- 2.3 basic_dq ✅ (checker, meta_stream, unit tests 27/27 pass)
- 2.4 logical_engine ✅ (engine: EMA mean/variance, ElasticBox padding, unit tests 35/35 pass)
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
- [ ] 2.6 Rapidash
- [ ] 2.7 Weever
- [ ] 2.8 Watermark/Decision
- [ ] 2.9 Tombstone
- [ ] 2.10 LateHandler
- [ ] 2.11 AlertOutput
- [ ] 2.11b EventStore
- [ ] 2.11c Pipeline
- [ ] 2.12 Unit tests per module
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
