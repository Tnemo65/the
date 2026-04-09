# WAVES - Watermark-Aware Violation Detection for Streaming

## Giới thiệu

WAVES là một mở rộng của [Stream DaQ](https://github.com/Bilpapster/stream-DaQ), được phát triển cho luận văn thesis.

### Tính năng mới

- **DC Checking**: Tích hợp Denial Constraint checking từ Rapidash
- **Watermark Layer**: Xử lý late arrivals với retraction mechanism
- **Weever Integration**: Incremental LT-Tree indexing cho hiệu suất cao
- **Meta-Stream Output**: Output chứa cả basic checks và DC violations

## Yêu cầu hệ thống

```
Python >= 3.11 (BẮT BUỘC)
```

## Cài đặt

```bash
cd WAVES

# Tạo virtual environment với Python 3.11+
python3.11 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate     # Windows

# Cài đặt
pip install -e .
```

## Cấu trúc project

```
WAVES/
├── pyproject.toml           # Dependencies (pyproject.toml!)
├── README.md               # File này
├── waves/                  # Source code chính
│   ├── __init__.py         # Main API
│   ├── watermark.py         # WatermarkHandler
│   ├── windowing.py         # Window extensions
│   ├── dc_checker.py        # DC Checker (port từ Rapidash)
│   ├── wever.py             # LT-Tree (port từ Weever)
│   └── meta_stream.py       # Meta-stream output
└── examples/               # Ví dụ (sắp tới)
```

## Sử dụng cơ bản

```python
from waves import WAVES, DaQMeasures as dqm
from datetime import timedelta

waves = WAVES()

# Cấu hình cơ bản - như StreamDaQ
waves.configure(
    window=Windows.tumbling(3),
    instance="user_id",
    time_column="timestamp"
)

# Stream DaQ checks - y hệt như cũ
waves.add_check(dqm.null_count("destination"), threshold=0.1)

# === WAVES NEW FEATURES ===

# 1. Watermark cho late data
waves.configure_watermark(
    watermark_interval=timedelta(minutes=1),
    max_lateness=timedelta(minutes=5)
)

# 2. DC Checks (từ Rapidash)
waves.add_dc(
    "dc1",
    "NOT (s.State = t.State AND s.Salary < t.Salary)"
)

# Meta stream - giờ có thêm DC violations!
for result in waves.watch():
    print(result.window_id)
    print(result.basic_check_results)  # StreamDaQ
    print(result.dc_violations)        # WAVES
```

## So sánh với Stream DaQ

| Tính năng | StreamDaQ | WAVES |
|-----------|-----------|-------|
| Windowing | ✅ | ✅ Giữ nguyên |
| Basic Checks (50+) | ✅ | ✅ Giữ nguyên |
| DC Checking | ❌ | ✅ Mới |
| Watermark | ❌ | ✅ Mới |
| LT-Tree Indexing | ❌ | ✅ Mới |
| Retraction | ❌ | ✅ Mới |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WAVES                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        INPUT LAYER                                 │   │
│  │  CSV | JSON | Kafka | IoT | ...                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      WATERMARK LAYER (MỚI)                         │   │
│  │  • Late data detection                                              │   │
│  │  • Buffer & retraction                                              │   │
│  │  • Provisional → Final                                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                 WINDOWING ENGINE (StreamDaQ Core)                 │   │
│  │  Tumbling | Sliding | Session                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    CHECK PIPELINE                                    │   │
│  │                                                                      │   │
│  │  ┌──────────────────────┐  ┌──────────────────────────────────┐   │   │
│  │  │  BASIC CHECKS       │  │  DC CHECKS (Rapidash)           │   │   │
│  │  │  (StreamDaQ)        │  │  • Denial Constraints           │   │   │
│  │  │  • null_count      │  │  • kd-tree / Range-tree        │   │   │
│  │  │  • range_check     │  │  • Incremental updates          │   │   │
│  │  │  • freshness       │  │                                  │   │   │
│  │  │  • ...50+         │  │  ┌────────────────────────────┐  │   │   │
│  │  └──────────────────────┘  │  │ WEVER (LT-Tree Index)   │  │   │   │
│  │                            │  │ Incremental updates     │  │   │   │
│  │                            │  └────────────────────────────┘  │   │   │
│  │                            └──────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      META-STREAM OUTPUT                             │   │
│  │  • Basic check results                                               │   │
│  │  • DC violations                                                     │   │
│  │  • Watermark status (PROVISIONAL / FINAL)                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Tài liệu tham khảo

- [Stream DaQ](https://github.com/Bilpapster/stream-DaQ) - Base project
- [Rapidash](base/Rapidash/) - DC Checking
- [Weever](base/Weever/) - Incremental Indexing
- [WAVES Architecture](../docs/WAVES_FULL_Architecture.md) - Chi tiết kiến trúc

## Nguyên tắc phát triển

Xem [AGENTS.md](../docs/AGENTS.md) để biết chi tiết.

```
1. KHÔNG LÀM MẤT BẢN CHẤT của StreamDaQ
2. Sử dụng relative paths cho cross-platform
3. Dùng pyproject.toml cho dependencies
4. Research kỹ trước khi implement
```
