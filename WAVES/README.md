# WAVES

**WAVES: Window-based Adaptive Violation Detection with Elastic Streaming**

Hệ thống phát hiện vi phạm logical data quality trên data stream, kết hợp KD-Tree range search, pane-based forest, EMA-based elastic bounding boxes, và watermark-aware alerting.

## Kiến trúc

```
StreamIngestion → WindowManager → BasicDQ → LogicalEngine → Rapidash → Decision → AlertOutput
                                     ↓                              ↑
                              Weever (PaneForest) ←──────────────┘
```

## Modules

| Module | Mô tả |
|--------|--------|
| 2.1 StreamIngestion | Nhận luồng CSV/JSON/Kafka, gán EventTime/IngestionTime |
| 2.2 WindowManager | Gán bản ghi vào sliding window, duy trì buffer |
| 2.3 BasicDQ | Completeness, validity, range checks |
| 2.4 LogicalEngine | Statistical context (EMA), Elastic Box generator |
| 2.5 Optimizer | DC parser, greedy grouping, infinite padding |
| 2.6 Rapidash | KD-Tree bulk-load, batched traversal, box dropping |
| 2.7 Weever | Pane-based forest, O(1) pane drop |
| 2.8 Decision | Provisional/final/retraction alerts |
| 2.9 Tombstone | O(1) ghost/retracted data filter |
| 2.10 LateHandler | Late data re-check, retraction |
| 2.11 Output | Alert sink, quality meta-stream |
| 2.11b EventStore | Shared event_id → (DataEvent, pane_id, window_id) |
| 2.11c Pipeline | Orchestrator điều phối toàn bộ luồng |

## Cài đặt

```bash
# Tạo virtual environment (một lần)
python -m venv .venv

# Kích hoạt
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# Cài package (editable mode)
pip install -e .

# Hoặc dùng uv:
uv pip install -e .
```

## Phát triển

```bash
# Chạy tests
pytest

# Format code
ruff check . --fix
ruff format .

# Type check
mypy waves/
```

## Cấu trúc thư mục

```
WAVES/
├── waves/               # Package chính
│   ├── ingestion/       # Module 2.1
│   ├── windowing/       # Module 2.2
│   ├── basic_dq/        # Module 2.3
│   ├── logical_engine/  # Module 2.4
│   ├── optimizer/       # Module 2.5
│   ├── rapidash/        # Module 2.6
│   ├── weever/          # Module 2.7
│   ├── decision/        # Module 2.8
│   ├── tombstone/       # Module 2.9
│   ├── late_handler/    # Module 2.10
│   ├── output/          # Module 2.11
│   └── store/           # Module 2.11b (shared)
├── configs/             # DC rules, system config
├── scripts/             # Benchmark, injection
├── examples/            # Demo scripts
└── tests/               # Unit & integration tests
```

## Research Questions

| RQ | Câu hỏi | Modules |
|----|----------|---------|
| RQ1 | Throughput vượt nested-loop? | Rapidash, Weever |
| RQ2 | EMA + Retraction giữ F1 khi drift + late? | LogicalEngine, Decision, Tombstone |
| RQ3 | Mở rộng khi 50–100 luật? | Optimizer, Rapidash batched |
| RQ4 | Trade-off siêu tham số? | ElasticBox, Weever, Optimizer |

## Baseline Systems

- NL-Stream: O(N²) brute-force
- Single-Tree-DaQ: một KD-Tree lớn
- Static-Box-DaQ: không EMA
- WAVES-SingleRule: không shared optimizer
- Buffer-Wait-DaQ: không retraction
- WAVES-Full: cấu hình đầy đủ

## Denial Constraints (DC1–DC3)

- **DC1**: Fare–Distance Dominance
- **DC2**: Context-Aware Duration Anomaly
- **DC3**: Toll Route Anomaly

## Benchmark

Dataset: NYC Taxi Yellow Cab

Pipeline chuẩn bị: B1 (base) → B2 (drift) → B3 (fraud) → B4 (late data)
