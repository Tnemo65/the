# WAVES FULL (A) - Complete Architecture Specification

## ⚠️ NGUYÊN TẮC QUAN TRỌNG

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│   MỤC TIÊU: KHÔNG LÀM MẤT BẢN CHẤT + GIỮ NHỮNG GÌ StreamDaQ LÀM ĐƯỢC          │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ❌ KHÔNG CẦN: "Chỉ thêm, không xóa" (quá máy móc)                           │
│   ✅ CẦN: "Không làm mất những gì StreamDaQ làm được"                         │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   Trong quá trình nâng cấp lên WAVES, ta CÓ THỂ phải:                         │
│                                                                                 │
│   • Thay đổi internal structure (data flow, state management)                  │
│   • Refactor code để tích hợp watermark, DC checker sâu hơn                   │
│   • Sửa đổi cách window state được quản lý                                   │
│   • Thậm chí override/rewrite một số internal methods                          │
│                                                                                 │
│   NHƯNG vẫn PHẢI ĐẢM BẢO:                                                      │
│                                                                                 │
│   ✅ StreamDaQ vẫn hoạt động như một hệ thống độc lập                         │
│   ✅ Tất cả 50+ quality checks vẫn chạy đúng                                  │
│   ✅ Meta-stream output vẫn xuất ra đúng format                                │
│   ✅ API cũ vẫn gọi được (backward compatibility hoặc migration guide)        │
│   ✅ Windowing logic vẫn đúng                                                 │
│   ✅ Dynamic context evaluation vẫn hoạt động                                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Stream DaQ Essence - Những thứ PHẢI giữ

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BẢN CHẤT StreamDaQ (Essence)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. WINDOWING                                                               │
│     ├── Tumbling/Sliding/Session windows                                    │
│     ├── Window assignment theo timestamp                                    │
│     └── Window state management                                             │
│                                                                             │
│  2. QUALITY CHECKS (50+ built-in)                                           │
│     ├── Tuple-at-a-time checks (null, range, pattern...)                   │
│     ├── Window context checks (freshness, distribution...)                  │
│     ├── Aggregation checks (count, distinct, mean...)                       │
│     └── Dynamic threshold evaluation                                        │
│                                                                             │
│  3. META-STREAM OUTPUT                                                      │
│     ├── watch_out() / watch()                                              │
│     ├── Quality results per window                                          │
│     └── Structured output format                                           │
│                                                                             │
│  4. DYNAMIC CONTEXT                                                         │
│     ├── Lambda-based threshold evaluation                                   │
│     └── Context-aware checks                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

                         ┌─────────────────────────┐
                         │   WAVES Enhancement    │
                         │   (Có thể refactor)    │
                         ├─────────────────────────┤
                         │ • Watermark handling   │
                         │ • DC checking          │
                         │ • LT-Tree indexing     │
                         │ • Retraction mechanism │
                         └───────────┬─────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  WAVES = StreamDaQ Essence (GIỮ) + WAVES Features (THÊM/CẦN SỬA)           │
│                                                                             │
│  ┌─────────────────────┐      ┌─────────────────────┐                      │
│  │  StreamDaQ Essence  │  +   │  WAVES Extensions   │                      │
│  │  (BẮT BUỘC GIỮ)     │      │  (MỚI - CÓ THỂ     │                      │
│  │                     │      │   REFACTOR)         │                      │
│  │  • Windowing        │      │                     │                      │
│  │  • 50+ Checks       │      │  • Watermark        │                      │
│  │  • Meta-stream     │  ──► │  • DC Checker       │                      │
│  │  • Dynamic Context  │      │  • LT-Tree          │                      │
│  │                     │      │  • Retraction       │                      │
│  └─────────────────────┘      └─────────────────────┘                      │
│                                                                             │
│  CÓ THỂ THAY ĐỔI: Internal implementation, data flow, structure           │
│  KHÔNG ĐƯỢC THAY ĐỔI: API contract, output format, check behavior         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Mục lục

1. [Nguyên tắc: Giữ Bản chất, không phải Code](#-nguyên-tắc-quan-trọng)
2. [Stream DaQ Essence - Những thứ PHẢI giữ](#stream-daq-essence---những-thứ-phải-giữ)
3. [Tổng quan Kiến trúc](#3-tổng-quan-kiến-trúc)
4. [Data Flow Tổng thể](#4-data-flow-tổng-thể)
5. [Component Specifications](#5-component-specifications)
6. [Integration Strategy - Refactor vs Extend](#6-integration-strategy---refactor-vs-extend)
7. [API Design](#7-api-design)
8. [Porting Strategy: Java → Python](#8-porting-strategy-java--python)
9. [Implementation Phases](#9-implementation-phases)
10. [Testing Strategy](#10-testing-strategy)
11. [Stream DaQ Gốc - Giữ & Migration](#11-stream-daq-gốc---giữ--migration)

---

## 1. Tổng quan Kiến trúc

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    WAVES FULL (A)                                       │
│                    Watermark-Aware Violation Detection for Streaming                    │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                              INPUT LAYER                                         │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐         │   │
│  │  │  CSV    │  │  JSON   │  │  Kafka  │  │ Python  │  │  IoT    │  ...   │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘         │   │
│  │       └─────────────┴─────────────┴─────────────┴─────────────┘               │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│                                            │                                              │
│                                            ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           WATERMARK LAYER (Lớp nền tảng)                       │   │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐   │   │
│  │  │  • Timestamp Assignment & Validation                                    │   │   │
│  │  │  • Late Data Buffering (wait_for_late)                                 │   │   │
│  │  │  • Watermark Progress Tracking                                          │   │   │
│  │  │  • Provisional → Final Result Transition                                │   │   │
│  │  └─────────────────────────────────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│                                            │                                              │
│                                            ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           WINDOWING ENGINE (Stream DaQ Core)                    │   │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                   │   │
│  │  │   Tumbling    │  │    Sliding     │  │    Session     │                   │   │
│  │  │   Windows     │  │    Windows     │  │    Windows     │                   │   │
│  │  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                   │   │
│  │          │                    │                    │                            │   │
│  │          └────────────────────┴────────────────────┘                            │   │
│  │                                │                                                  │   │
│  │                    ┌───────────┴───────────┐                                       │   │
│  │                    │   Window Manager      │                                       │   │
│  │                    │   • Window State    │                                       │   │
│  │                    │   • Expiration      │                                       │   │
│  │                    │   • Overlap Mgmt    │                                       │   │
│  │                    └─────────────────────┘                                       │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│                                            │                                              │
│                                            ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           CHECK PIPELINE (Parallel Execution)                    │   │
│  │                                                                                  │   │
│  │  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐     │   │
│  │  │     BASIC CHECKS                │  │     DC CHECKS (Rapidash)       │     │   │
│  │  │     (Stream DaQ)               │  │     (Orthogonal Range Search)   │     │   │
│  │  │                                 │  │                                 │     │   │
│  │  │  • null_count                  │  │  • Candidate Key                 │     │   │
│  │  │  • range_conformance           │  │  • Functional Dependency        │     │   │
│  │  │  • distinct_count               │  │  • Order Dependency            │     │   │
│  │  │  • freshness                   │  │  • Denial Constraint           │     │   │
│  │  │  • ...30+ checks              │  │  • kd-tree / Range-tree       │     │   │
│  │  │                                 │  │                                 │     │   │
│  │  └───────────────┬─────────────────┘  └───────────────┬─────────────────┘     │   │
│  │                  │                                      │                        │   │
│  │                  │      ┌─────────────────────┐       │                        │   │
│  │                  └──────►│   WEVER LAYER     │◄─────┘                        │   │
│  │                             │ (Incremental Index)│                               │   │
│  │                             │                   │                                │   │
│  │                             │  • LT-Tree Index │                                │   │
│  │                             │  • Incremental   │                                │   │
│  │                             │    Updates       │                                │   │
│  │                             │  • Predicate     │                                │   │
│  │                             │    Scheduling    │                                │   │
│  │                             └────────┬─────────┘                                │   │
│  │                                      │                                           │   │
│  └──────────────────────────────────────┼───────────────────────────────────────────┘   │
│                                           │                                            │
│                                           ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                         META-STREAM OUTPUT LAYER                               │   │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Unified Quality Meta-Stream                                          │   │   │
│  │  │                                                                          │   │   │
│  │  │  window_id | window_start | window_end | watermark_status |            │   │   │
│  │  │  ├─ Basic Check Results ────────────────────────────────────────────  │   │   │
│  │  │  │   check_type | column | value | pass/fail | severity | ...        │   │   │
│  │  │  ├─ DC Check Results ───────────────────────────────────────────────  │   │   │
│  │  │  │   dc_id | predicate | violation_pairs | count | ...                 │   │   │
│  │  │  └─ Watermark Status ───────────────────────────────────────────────  │   │   │
│  │  │      provisional | retracted | final | ...                               │   │   │
│  │  └─────────────────────────────────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Flow Tổng thể

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW - TUẦN HOÀN                                       │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  TUPLE ARRIVES                                                                          │
│       │                                                                                  │
│       ▼                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐     │
│  │ 1. WATERMARK LAYER                                                               │     │
│  │    ├─ Validate timestamp                                                           │     │
│  │    ├─ Check if tuple is late (behind watermark)                                   │     │
│  │    ├─ If late: Buffer for potential retraction                                     │     │
│  │    └─ If on-time: Forward to windowing                                            │     │
│  └─────────────────────────────────────────────────────────────────────────────────┘     │
│       │                                                                                  │
│       ▼                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐     │
│  │ 2. WINDOWING ENGINE                                                              │     │
│  │    ├─ Assign tuple to window(s) based on timestamp                                  │     │
│  │    ├─ Update window state                                                         │     │
│  │    └─ Emit window trigger when window completes                                   │     │
│  └─────────────────────────────────────────────────────────────────────────────────┘     │
│       │                                                                                  │
│       ▼                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐     │
│  │ 3. CHECK PIPELINE (Parallel)                                                      │     │
│  │                                                                                  │     │
│  │    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐       │     │
│  │    │   BASIC CHECKS   │    │    DC CHECKS     │    │  INDEX UPDATE   │       │     │
│  │    │   (30+ types)    │    │   (Rapidash)     │    │   (Weever)      │       │     │
│  │    │                  │    │                  │    │                  │       │     │
│  │    │  • null_count    │    │  • Parse DC     │    │  • LT-Tree      │       │     │
│  │    │  • range_check   │───►│  • kd-tree      │───►│  • Insert      │       │     │
│  │    │  • freshness     │    │  • Range Query   │    │  • Update       │       │     │
│  │    │  • ...          │    │  • Count        │    │  • Delete       │       │     │
│  │    └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘       │     │
│  │             │                        │                        │                   │     │
│  └─────────────┼────────────────────────┼────────────────────────┼───────────────────┘     │
│                │                        │                        │                            │
│                ▼                        ▼                        ▼                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐     │
│  │ 4. RESULT AGGREGATION                                                            │     │
│  │    ├─ Merge basic check results                                                    │     │
│  │    ├─ Merge DC violation results                                                   │     │
│  │    ├─ Determine provisional/final status (watermark dependent)                     │     │
│  │    └─ Generate unified result tuple                                               │     │
│  └─────────────────────────────────────────────────────────────────────────────────┘     │
│       │                                                                                  │
│       ▼                                                                                 │
│  META-STREAM OUTPUT                                                                     │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Specifications

### 3.1 Watermark Layer

```python
# =============================================================================
# WAVES/watermark.py
# =============================================================================

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import threading
from collections import defaultdict


class WatermarkStatus(Enum):
    """Trạng thái của một kết quả liên quan đến watermark."""
    PROVISIONAL = "provisional"      # Chưa final, có thể bị retract
    FINAL = "final"                  # Đã final, không thể retract
    RETRACTED = "retracted"          # Đã bị retract


@dataclass
class WatermarkEvent:
    """Sự kiện watermark."""
    timestamp: float                   # Watermark timestamp mới
    event_time: datetime
    is_aligned: bool                 # True = conformant, False = heuristic
    source_id: str                    # Nguồn phát sinh watermark


@dataclass
class LateTuple:
    """Tuple đến muộn."""
    tuple_data: Any
    window_id: str                    # Window mà tuple thuộc về
    arrival_time: float               # Thời gian đến
    original_timestamp: float          # Timestamp gốc của tuple
    

class WatermarkHandler:
    """
    Xử lý watermark cho toàn bộ hệ thống WAVES.
    
    RESPONSIBILITIES:
    1. Theo dõi watermark progress
    2. Buffer late tuples cho potential retraction
    3. Emit provisional → final transitions
    4. Quản lý watermark alignment
    
    CRITICAL: Watermark layer này XUYÊN SUỐT toàn bộ kiến trúc,
    không phải wrapper riêng biệt.
    """
    
    def __init__(
        self,
        watermark_interval: timedelta = timedelta(minutes=1),
        max_lateness: timedelta = timedelta(minutes=5),
        alignment_mode: str = "event_time"  # "event_time" | "processing_time" | "heuristic"
    ):
        self.watermark_interval = watermark_interval
        self.max_lateness = max_lateness
        
        # Watermark state
        self._current_watermark: float = 0.0
        self._watermark_sources: Dict[str, float] = {}  # source_id -> watermark
        self._lock = threading.Lock()
        
        # Late tuple buffer: window_id -> List[LateTuple]
        self._late_buffers: Dict[str, List[LateTuple]] = defaultdict(list)
        
        # Provisional results: window_id -> result_data
        self._provisional_results: Dict[str, Any] = {}
        
        # Callbacks
        self._on_watermark_advance: Optional[Callable] = None
        self._on_late_tuple: Optional[Callable] = None
        self._on_retraction: Optional[Callable] = None
        
    # -------------------------------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------------------------------
    
    def register_source(self, source_id: str) -> None:
        """Đăng ký một nguồn dữ liệu mới."""
        with self._lock:
            self._watermark_sources[source_id] = 0.0
    
    def update_watermark(self, source_id: str, watermark: float) -> WatermarkEvent:
        """
        Cập nhật watermark cho một nguồn.
        
        Returns:
            WatermarkEvent nếu watermark advance
        """
        with self._lock:
            old_watermark = self._watermark_sources.get(source_id, 0.0)
            
            if watermark <= old_watermark:
                return None  # Không advance
            
            self._watermark_sources[source_id] = watermark
            
            # Compute global watermark = min của tất cả sources
            new_global_watermark = min(self._watermark_sources.values())
            
            if new_global_watermark > self._current_watermark:
                old_watermark = self._current_watermark
                self._current_watermark = new_global_watermark
                
                event = WatermarkEvent(
                    timestamp=new_global_watermark,
                    event_time=datetime.fromtimestamp(new_global_watermark),
                    is_aligned=True,
                    source_id=source_id
                )
                
                # Emit callbacks
                self._emit_watermark_advance(old_watermark, new_global_watermark)
                
                return event
        
        return None
    
    def process_tuple(
        self,
        tuple_data: Any,
        tuple_timestamp: float,
        window_id: str,
        source_id: str
    ) -> TupleProcessResult:
        """
        Xử lý một tuple, quyết định xem nó có late hay không.
        
        Returns:
            TupleProcessResult với quyết định và action cần thực hiện
        """
        with self._lock:
            # Kiểm tra xem tuple có late không
            if tuple_timestamp <= self._current_watermark - self.max_lateness.total_seconds():
                # Tuple quá muộn, bỏ qua hoặc đánh dấu đặc biệt
                return TupleProcessResult(
                    action=TupleAction.DISCARD,
                    is_late=True,
                    tuple_data=tuple_data,
                    window_id=window_id
                )
            
            elif tuple_timestamp < self._current_watermark:
                # Tuple late nhưng còn trong ngưỡng cho phép
                # Buffer để xử lý retraction
                late_tuple = LateTuple(
                    tuple_data=tuple_data,
                    window_id=window_id,
                    arrival_time=time.time(),
                    original_timestamp=tuple_timestamp
                )
                self._late_buffers[window_id].append(late_tuple)
                
                return TupleProcessResult(
                    action=TupleAction.BUFFER_LATE,
                    is_late=True,
                    tuple_data=tuple_data,
                    window_id=window_id,
                    late_tuple=late_tuple
                )
            
            else:
                # Tuple đúng giờ
                return TupleProcessResult(
                    action=TupleAction.PROCESS,
                    is_late=False,
                    tuple_data=tuple_data,
                    window_id=window_id
                )
    
    def on_window_complete(
        self,
        window_id: str,
        window_data: Any,
        results: CheckResults
    ) -> WindowResult:
        """
        Xử lý khi window hoàn thành.
        
        Returns:
            WindowResult với trạng thái provisional/final
        """
        with self._lock:
            window_start = self._extract_window_start(window_id)
            
            if window_start <= self._current_watermark:
                # Window đã được watermark cover = FINAL
                return WindowResult(
                    window_id=window_id,
                    results=results,
                    status=WatermarkStatus.FINAL,
                    watermark=self._current_watermark
                )
            else:
                # Window chưa được cover = PROVISIONAL
                self._provisional_results[window_id] = results
                return WindowResult(
                    window_id=window_id,
                    results=results,
                    status=WatermarkStatus.PROVISIONAL,
                    watermark=None
                )
    
    def retract_window(self, window_id: str, new_results: CheckResults) -> RetractionEvent:
        """
        Retract một provisional window khi late tuple đến.
        
        CRITICAL: Đây là chức năng KHÔNG CÓ trong WAVES-lite
        """
        with self._lock:
            old_results = self._provisional_results.pop(window_id, None)
            
            if old_results is None:
                return None
            
            # Cập nhật provisional results
            self._provisional_results[window_id] = new_results
            
            return RetractionEvent(
                window_id=window_id,
                old_results=old_results,
                new_results=new_results,
                timestamp=time.time()
            )
    
    # -------------------------------------------------------------------------
    # INTERNAL METHODS
    # -------------------------------------------------------------------------
    
    def _emit_watermark_advance(self, old_wm: float, new_wm: float) -> None:
        """Emit watermark advance event và finalize provisional windows."""
        # Find windows that are now finalized
        finalized_windows = []
        
        for window_id in list(self._provisional_results.keys()):
            window_start = self._extract_window_start(window_id)
            if window_start <= new_wm:
                finalized_windows.append(window_id)
        
        # Remove from provisional
        for window_id in finalized_windows:
            del self._provisional_results[window_id]
        
        # Emit callback
        if self._on_watermark_advance:
            self._on_watermark_advance(old_wm, new_wm, finalized_windows)
    
    def _extract_window_start(self, window_id: str) -> float:
        """Trích xuất window start timestamp từ window_id."""
        # Format: "window_{start_ts}_{end_ts}"
        parts = window_id.split("_")
        return float(parts[1])
    
    @property
    def current_watermark(self) -> float:
        """Lấy watermark hiện tại."""
        with self._lock:
            return self._current_watermark
    
    @property
    def pending_provisional(self) -> Dict[str, Any]:
        """Lấy tất cả provisional windows."""
        with self._lock:
            return dict(self._provisional_results)


# =============================================================================
# Supporting Classes
# =============================================================================

from enum import Enum


class TupleAction(Enum):
    """Action cần thực hiện với tuple."""
    PROCESS = "process"              # Xử lý bình thường
    BUFFER_LATE = "buffer_late"    # Buffer để retract sau
    DISCARD = "discard"             # Bỏ qua hoàn toàn


@dataclass
class TupleProcessResult:
    """Kết quả xử lý tuple."""
    action: TupleAction
    is_late: bool
    tuple_data: Any
    window_id: str
    late_tuple: Optional[LateTuple] = None


@dataclass
class WindowResult:
    """Kết quả khi window hoàn thành."""
    window_id: str
    results: Any  # CheckResults
    status: WatermarkStatus
    watermark: Optional[float]


@dataclass
class RetractionEvent:
    """Sự kiện retraction."""
    window_id: str
    old_results: Any
    new_results: Any
    timestamp: float
```

### 3.2 Windowing Engine (Stream DaQ Core - Extended)

```python
# =============================================================================
# WAVES/windowing.py
# =============================================================================

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Generic, TypeVar
from datetime import datetime, timedelta
from enum import Enum
import time
import threading


T = TypeVar('T')


class WindowType(Enum):
    """Loại window."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    """
    Định nghĩa một window.
    
    CRITICAL: Window phải QUẢN LÝ STATE cho Weever integration.
    """
    window_type: WindowType
    size: timedelta
    hop: Optional[timedelta] = None  # Chỉ cho sliding
    max_gap: Optional[timedelta] = None  # Chỉ cho session
    origin: Optional[datetime] = None
    
    # CRITICAL: Window state để tích hợp Weever
    # Key = window_id, Value = window state
    _window_states: Dict[str, 'WindowState'] = field(default_factory=dict)
    _lock = field(default_factory=threading.Lock)
    
    def get_window_id(self, timestamp: float) -> str:
        """Tính window_id cho một timestamp."""
        ts = datetime.fromtimestamp(timestamp)
        
        if self.window_type == WindowType.TUMBLING:
            start = self._get_tumbling_start(ts)
        elif self.window_type == WindowType.SLIDING:
            start = self._get_sliding_start(ts)
        else:  # SESSION
            start = ts  # Session windows are keyed
            
        end = start + self.size
        return f"window_{start.timestamp()}_{end.timestamp()}"
    
    def get_windows_for_tuple(self, timestamp: float) -> List[str]:
        """
        Lấy tất cả windows mà tuple thuộc về.
        
        CRITICAL: Sliding windows có thể thuộc về nhiều windows.
        """
        if self.window_type == WindowType.TUMBLING:
            return [self.get_window_id(timestamp)]
        
        elif self.window_type == WindowType.SLIDING:
            windows = []
            current = self._get_sliding_start(datetime.fromtimestamp(timestamp))
            window_end = current + self.size
            
            while current < datetime.fromtimestamp(timestamp):
                windows.append(f"window_{current.timestamp()}_{window_end.timestamp()}")
                current += self.hop
                window_end += self.hop
            
            return windows
        
        else:  # SESSION
            return [self.get_window_id(timestamp)]
    
    def get_expired_windows(self, current_time: float) -> List[str]:
        """Lấy các windows đã hết hạn (để cleanup)."""
        with self._lock:
            expired = []
            cutoff = current_time - self.size.total_seconds()
            
            for window_id in list(self._window_states.keys()):
                parts = window_id.split("_")
                window_end = float(parts[2])
                
                if window_end <= cutoff:
                    expired.append(window_id)
            
            return expired
    
    def cleanup_window(self, window_id: str) -> Optional['WindowState']:
        """
        Cleanup window state.
        
        CRITICAL: Phải cleanup để tránh memory leak.
        Weever index cũng phải được cleanup ở đây.
        """
        with self._lock:
            return self._window_states.pop(window_id, None)
    
    # --- Internal methods ---
    
    def _get_tumbling_start(self, ts: datetime) -> datetime:
        if self.origin:
            origin_ts = self.origin.timestamp()
            size_sec = self.size.total_seconds()
            elapsed = ts.timestamp() - origin_ts
            num_windows = int(elapsed / size_sec)
            return datetime.fromtimestamp(origin_ts + num_windows * size_sec)
        return ts.replace(minute=ts.minute // (self.size.total_seconds() // 60) * (self.size.total_seconds() // 60),
                          second=0, microsecond=0)
    
    def _get_sliding_start(self, ts: datetime) -> datetime:
        # Tương tự tumbling nhưng với hop
        return self._get_tumbling_start(ts)  # Simplified


@dataclass
class WindowState:
    """
    State của một window.
    
    CRITICAL: Đây là nơi Weever integration HOẠT ĐỘNG.
    """
    window_id: str
    tuples: List[Any] = field(default_factory=list)
    tuple_count: int = 0
    
    # CRITICAL: Weever LT-Tree Index per window
    # Key = column_name, Value = LT-Tree instance
    lt_indexes: Dict[str, 'LTTree'] = field(default_factory=dict)
    
    # Watermark status
    watermark_status: WatermarkStatus = WatermarkStatus.PROVISIONAL
    
    # Timestamps
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    last_update: float = field(default_factory=time.time)
    
    def add_tuple(self, tuple_data: Any) -> None:
        """Thêm tuple vào window state."""
        self.tuples.append(tuple_data)
        self.tuple_count += 1
        self.last_update = time.time()
        
        # CRITICAL: Update Weever indexes
        for col_name, lt_tree in self.lt_indexes.items():
            if col_name in tuple_data:
                lt_tree.insert(tuple_data[col_name], tuple_data)
    
    def remove_tuple(self, tuple_data: Any) -> None:
        """Xóa tuple khỏi window state (cho retraction)."""
        if tuple_data in self.tuples:
            self.tuples.remove(tuple_data)
            
            # CRITICAL: Update Weever indexes
            for col_name, lt_tree in self.lt_indexes.items():
                if col_name in tuple_data:
                    lt_tree.delete(tuple_data[col_name], tuple_data)


class WindowManager:
    """
    Quản lý tất cả windows và orchestrate với Watermark + Weever.
    
    CRITICAL: Đây là điểm TÍCH HỢP chính giữa các layers.
    """
    
    def __init__(
        self,
        windows: List[Window],
        watermark_handler: 'WatermarkHandler',
        weever_config: Optional['WeeverConfig'] = None
    ):
        self.windows = windows
        self.watermark = watermark_handler
        self.wever_config = weever_config or WeeverConfig()
        
        # Global window states (shared across window types)
        self._states: Dict[str, WindowState] = {}
        self._lock = threading.Lock()
        
        # Callbacks
        self._on_window_complete: Optional[Callable] = None
    
    def process_tuple(
        self,
        tuple_data: Any,
        timestamp: float,
        source_id: str
    ) -> List[WindowProcessResult]:
        """
        Xử lý tuple thông qua watermark và assign vào windows.
        
        Returns:
            List of WindowProcessResult cho mỗi window tuple thuộc về
        """
        # 1. Watermark check
        for window in self.windows:
            window_id = window.get_window_id(timestamp)
            
            # Watermark processing
            wm_result = self.watermark.process_tuple(
                tuple_data=tuple_data,
                tuple_timestamp=timestamp,
                window_id=window_id,
                source_id=source_id
            )
            
            if wm_result.action == TupleAction.DISCARD:
                continue
            
            # 2. Get or create window state
            with self._lock:
                if window_id not in self._states:
                    self._states[window_id] = self._create_window_state(window_id, window)
                
                state = self._states[window_id]
            
            # 3. Handle late buffer
            if wm_result.action == TupleAction.BUFFER_LATE:
                # Buffer nhưng vẫn update indexes cho future checks
                state.add_tuple(tuple_data)
                continue
            
            # 4. Normal processing
            state.add_tuple(tuple_data)
            
            # 5. Check if window should emit
            if self._should_emit(window, state):
                result = self._emit_window(window, state)
                if result:
                    yield result
    
    def _create_window_state(self, window_id: str, window: Window) -> WindowState:
        """
        Tạo window state với Weever indexes.
        
        CRITICAL: Khởi tạo LT-Tree indexes ở đây.
        """
        state = WindowState(window_id=window_id)
        
        # CRITICAL: Initialize Weever LT-Tree indexes nếu configured
        if self.wever_config.enabled:
            for col_config in self.wever_config.indexed_columns:
                state.lt_indexes[col_config.name] = LTTree(
                    column=col_config.name,
                    column_type=col_config.type,
                    config=self.wever_config
                )
        
        return state
    
    def _should_emit(self, window: Window, state: WindowState) -> bool:
        """Kiểm tra xem window nên emit chưa."""
        # Tumbling: Emit khi window end time <= current time
        # Sliding: Emit khi window full hoặc trigger
        
        current_time = time.time()
        
        if state.end_time is None:
            parts = state.window_id.split("_")
            state.start_time = float(parts[1])
            state.end_time = float(parts[2])
        
        return current_time >= state.end_time
    
    def _emit_window(
        self,
        window: Window,
        state: WindowState
    ) -> Optional[WindowProcessResult]:
        """Emit window results."""
        # Cleanup expired windows
        self._cleanup_expired()
        
        return WindowProcessResult(
            window_id=state.window_id,
            window_type=window.window_type,
            tuple_count=state.tuple_count,
            tuples=list(state.tuples),  # Snapshot
            lt_indexes_snapshot=state.lt_indexes,  # CRITICAL: For Weever queries
            watermark_status=state.watermark_status
        )
    
    def _cleanup_expired(self) -> None:
        """Cleanup expired windows."""
        current_time = time.time()
        
        with self._lock:
            for window in self.windows:
                expired = window.get_expired_windows(current_time)
                
                for window_id in expired:
                    state = self._states.pop(window_id, None)
                    if state:
                        window.cleanup_window(window_id)


@dataclass
class WindowProcessResult:
    """Kết quả khi window emit."""
    window_id: str
    window_type: WindowType
    tuple_count: int
    tuples: List[Any]
    lt_indexes_snapshot: Dict[str, 'LTTree']  # CRITICAL: Weever indexes
    watermark_status: WatermarkStatus


@dataclass
class WeeverConfig:
    """Configuration cho Weever integration."""
    enabled: bool = True
    indexed_columns: List['ColumnConfig'] = field(default_factory=list)
    use_roaring_bitmap: bool = True
    
    # Predicate scheduling
    enable_scheduling: bool = True
    schedule_by_selectivity: bool = True


@dataclass
class ColumnConfig:
    """Configuration cho một column được index."""
    name: str
    type: str  # "int", "float", "string"
    indexed: bool = True
```

### 3.3 DC Checker (Rapidash - Ported to Python)

```python
# =============================================================================
# WAVES/dc_checker.py
# =============================================================================

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from enum import Enum
import re


class Operator(Enum):
    """Operators cho predicates."""
    EQUAL = "=="
    NOT_EQUAL = "<>"
    LESS = "<"
    LESS_EQUAL = "<="
    GREATER = ">"
    GREATER_EQUAL = ">="


@dataclass
class Predicate:
    """
    Một predicate trong DC.
    
    Ví dụ: "s.Salary < t.Salary"
    """
    left_column: str
    right_column: str  # Có thể là column khác (heterogeneous) hoặc cùng (homogeneous)
    operator: Operator
    
    @property
    def is_homogeneous(self) -> bool:
        """Kiểm tra xem predicate có homogeneous không."""
        return self.left_column == self.right_column


@dataclass
class DenialConstraint:
    """
    Một Denial Constraint.
    
    Ví dụ: NOT (State = t.State AND Salary <= t.Salary AND FedTaxRate > t.FedTaxRate)
    
    CRITICAL: Đây là format tương thích với Rapidash.
    """
    dc_id: str
    predicates: List[Predicate] = field(default_factory=list)
    
    # Parse từ string
    @classmethod
    def from_string(cls, dc_id: str, dc_string: str) -> 'DenialConstraint':
        """
        Parse DC từ string.
        
        Format: "NOT (pred1 AND pred2 AND ...)"
        Ví dụ: "NOT (s.State = t.State AND s.Salary <= t.Salary AND s.FedTaxRate > t.FedTaxRate)"
        """
        dc = cls(dc_id=dc_id)
        
        # Extract predicates từ string
        # Bỏ qua "NOT (" và ")"
        inner = dc_string.replace("NOT (", "").replace(")", "")
        
        # Split by "AND"
        pred_strings = [p.strip() for p in inner.split("AND")]
        
        for pred_str in pred_strings:
            pred = cls._parse_predicate(pred_str)
            if pred:
                dc.predicates.append(pred)
        
        return dc
    
    @staticmethod
    def _parse_predicate(pred_str: str) -> Optional[Predicate]:
        """Parse một predicate string."""
        pred_str = pred_str.strip()
        
        # Match pattern: column op column
        match = re.match(r's\.(\w+)\s*([<>=!]+)\s*t\.(\w+)', pred_str)
        if match:
            return Predicate(
                left_column=match.group(1),
                right_column=match.group(3),
                operator=Operator(match.group(2))
        
        return None


@dataclass
class DCViolation:
    """Kết quả vi phạm DC."""
    dc_id: str
    violating_tuples: List[Tuple[Any, Any]]  # List of (t1, t2) pairs
    violation_count: int
    is_violated: bool  # True nếu có vi phạm


class KDTreeNode:
    """
    KD-Tree node cho orthogonal range search.
    
    CRITICAL: Port từ Rapidash/Java sang Python.
    """
    
    def __init__(
        self,
        point: List[Any],
        point_id: Any,
        dimension: int = 0,
        left: Optional['KDTreeNode'] = None,
        right: Optional['KDTreeNode'] = None
    ):
        self.point = point
        self.point_id = point_id
        self.dimension = dimension
        self.left = left
        self.right = right
        
        # Bounds for pruning
        self.min_bounds: List[Any] = point.copy()
        self.max_bounds: List[Any] = point.copy()
        
        # LT aggregate (cho Weever integration)
        self.lt_aggregate: Set[Any] = set()
    
    def insert(self, point: List[Any], point_id: Any) -> None:
        """Chèn một điểm vào cây."""
        if point[self.dimension] < self.point[self.dimension]:
            if self.left:
                self.left.insert(point, point_id)
            else:
                self.left = KDTreeNode(point, point_id, (self.dimension + 1) % len(point))
        else:
            if self.right:
                self.right.insert(point, point_id)
            else:
                self.right = KDTreeNode(point, point_id, (self.dimension + 1) % len(point))
        
        # Update bounds
        self._update_bounds(point)
    
    def query_range(
        self,
        lower: List[Any],
        upper: List[Any]
    ) -> List[Tuple[List[Any], Any]]:
        """Query tất cả điểm trong range [lower, upper]."""
        results = []
        
        # Check if current point is in range
        if self._point_in_range(lower, upper):
            results.append((self.point, self.point_id))
        
        # Recurse
        dim = self.dimension
        
        if lower[dim] <= self.point[dim] and self.left:
            results.extend(self.left.query_range(lower, upper))
        
        if upper[dim] >= self.point[dim] and self.right:
            results.extend(self.right.query_range(lower, upper))
        
        return results
    
    def _point_in_range(self, lower: List[Any], upper: List[Any]) -> bool:
        """Kiểm tra xem điểm hiện tại có trong range không."""
        for i, val in enumerate(self.point):
            if val < lower[i] or val > upper[i]:
                return False
        return True
    
    def _update_bounds(self, point: List[Any]) -> None:
        """Cập nhật bounds sau khi insert."""
        for i in range(len(point)):
            self.min_bounds[i] = min(self.min_bounds[i], point[i])
            self.max_bounds[i] = max(self.max_bounds[i], point[i])


class DCChecker:
    """
    DC Checker với Orthogonal Range Search.
    
    CRITICAL: Đây là port của Rapidash DCVerifier sang Python.
    """
    
    def __init__(
        self,
        constraints: List[DenialConstraint],
        indexed_columns: List[str],
        use_kdtree: bool = True,  # True = KD-Tree, False = Range-Tree
        early_stop: bool = True    # Dừng sớm khi tìm thấy violation
    ):
        self.constraints = constraints
        self.indexed_columns = indexed_columns
        self.use_kdtree = use_kdtree
        self.early_stop = early_stop
        
        # Hash table: equality_value -> tree
        # Rapidash: Map<List<Integer>, RangeTreeHelper>
        self._hash_table: Dict[Tuple, KDTreeNode] = {}
        
        # Column indices
        self._column_indices = {col: i for i, col in enumerate(indexed_columns)}
    
    def check_window(
        self,
        tuples: List[Dict[str, Any]],
        window_id: str
    ) -> List[DCViolation]:
        """
        Kiểm tra tất cả DCs trên một window.
        
        CRITICAL: Sử dụng orthogonal range search để tránh O(N²).
        """
        violations = []
        
        # Build hash table + trees
        self._build_indexes(tuples)
        
        # Check each constraint
        for dc in self.constraints:
            violation = self._check_dc(dc, tuples)
            violations.append(violation)
        
        return violations
    
    def update_with_tuple(self, tuple_data: Dict[str, Any]) -> List[DCViolation]:
        """
        Cập nhật indexes với một tuple mới.
        
        CRITICAL: Đây là chỗ tích hợp với Weever/Windowing.
        """
        violations = []
        
        # Insert vào hash table + tree
        self._insert_tuple(tuple_data)
        
        # Check violations với tuple mới
        for dc in self.constraints:
            violation = self._check_dc_incremental(dc, tuple_data)
            if violation.is_violated:
                violations.append(violation)
        
        return violations
    
    def _build_indexes(self, tuples: List[Dict[str, Any]]) -> None:
        """
        Xây dựng hash table và kd-tree indexes.
        
        Rapidash logic:
        1. Partition by equality predicates
        2. Build kd-tree trong mỗi partition
        """
        self._hash_table.clear()
        
        for tuple_data in tuples:
            self._insert_tuple(tuple_data)
    
    def _insert_tuple(self, tuple_data: Dict[str, Any]) -> None:
        """Chèn một tuple vào indexes."""
        # Extract key cho hash table (từ equality predicates)
        # Ví dụ: nếu DC có "State = t.State", key = tuple_data["State"]
        # Ở đây đơn giản hóa: dùng tất cả indexed columns
        key_values = tuple_data.get(self.indexed_columns[0]) if self.indexed_columns else None
        key = (key_values,)
        
        # Get hoặc tạo tree cho partition này
        if key not in self._hash_table:
            # Extract point cho kd-tree (từ inequality predicates)
            point = self._extract_point(tuple_data)
            self._hash_table[key] = KDTreeNode(
                point=point,
                point_id=id(tuple_data),
                dimension=0
            )
        else:
            point = self._extract_point(tuple_data)
            self._hash_table[key].insert(point, id(tuple_data))
    
    def _extract_point(self, tuple_data: Dict[str, Any]) -> List[Any]:
        """Trích xuất điểm cho kd-tree từ tuple."""
        # Lấy tất cả columns được index
        return [tuple_data.get(col, 0) for col in self.indexed_columns]
    
    def _check_dc(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """
        Kiểm tra một DC sử dụng orthogonal range search.
        
        CRITICAL: Đây là Rapidash core algorithm.
        """
        # Phân loại predicates
        eq_preds = [p for p in dc.predicates if p.operator == Operator.EQUAL]
        ineq_preds = [p for p in dc.predicates if p.operator != Operator.EQUAL]
        
        if len(ineq_preds) == 0:
            # Chỉ equality predicates - dùng hash table thuần
            return self._check_eq_only(dc, tuples)
        
        elif len(ineq_preds) == 1:
            # Single inequality - dùng optimization O(N)
            return self._check_single_inequality(dc, tuples)
        
        else:
            # Multiple inequalities - dùng kd-tree range search
            return self._check_multiple_inequalities(dc, tuples)
    
    def _check_single_inequality(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """
        Kiểm tra DC với single inequality predicate.
        
        CRITICAL: Optimization O(N) - không cần kd-tree.
        """
        violation_pairs = []
        
        # Track min/max values per equality partition
        min_max: Dict[Tuple, Tuple[float, float]] = {}
        
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            
            if key not in min_max:
                col = dc.predicates[0].left_column
                val = tuple_data.get(col, 0)
                min_max[key] = (val, val)
            else:
                col = dc.predicates[0].left_column
                val = tuple_data.get(col, 0)
                current_min, current_max = min_max[key]
                min_max[key] = (min(current_min, val), max(current_max, val))
        
        # Check if violation exists
        is_violated = False
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            current_min, current_max = min_max[key]
            
            col = dc.predicates[0].left_column
            val = tuple_data.get(col, 0)
            
            # Check violation condition
            pred = dc.predicates[0]
            if self._is_violation(val, current_min, current_max, pred.operator):
                is_violated = True
                violation_pairs.append((tuple_data, None))  # Single tuple violation
        
        return DCViolation(
            dc_id=dc.dc_id,
            violating_tuples=violation_pairs,
            violation_count=len(violation_pairs),
            is_violated=is_violated
        )
    
    def _check_multiple_inequalities(self, dc: DenialConstraint, tuples: List[Dict[str, Any]]) -> DCViolation:
        """
        Kiểm tra DC với multiple inequality predicates.
        
        CRITICAL: Sử dụng kd-tree range search.
        """
        violation_pairs = []
        
        for tuple_data in tuples:
            key = self._get_partition_key(tuple_data, dc)
            
            if key not in self._hash_table:
                continue
            
            tree = self._hash_table[key]
            
            # Build range query bounds
            lower, upper = self._build_range_bounds(tuple_data, dc)
            
            # Query kd-tree
            results = tree.query_range(lower, upper)
            
            for point, other_id in results:
                if other_id != id(tuple_data):
                    violation_pairs.append((tuple_data, other_id))
        
        return DCViolation(
            dc_id=dc.dc_id,
            violating_tuples=violation_pairs,
            violation_count=len(violation_pairs),
            is_violated=len(violation_pairs) > 0
        )
    
    def _get_partition_key(self, tuple_data: Dict[str, Any], dc: DenialConstraint) -> Tuple:
        """Lấy partition key từ equality predicates."""
        key_parts = []
        for pred in dc.predicates:
            if pred.operator == Operator.EQUAL:
                key_parts.append(tuple_data.get(pred.left_column))
        return tuple(key_parts) if key_parts else None
    
    def _build_range_bounds(
        self,
        tuple_data: Dict[str, Any],
        dc: DenialConstraint
    ) -> Tuple[List[Any], List[Any]]:
        """Build range bounds cho kd-tree query."""
        # Simplified: dùng tất cả columns
        lower = []
        upper = []
        
        for pred in dc.predicates:
            val = tuple_data.get(pred.left_column, 0)
            
            if pred.operator == Operator.LESS:
                upper.append(val)
                lower.append(float('-inf'))
            elif pred.operator == Operator.LESS_EQUAL:
                upper.append(val)
                lower.append(float('-inf'))
            elif pred.operator == Operator.GREATER:
                upper.append(float('inf'))
                lower.append(val)
            elif pred.operator == Operator.GREATER_EQUAL:
                upper.append(float('inf'))
                lower.append(val)
            else:
                lower.append(val)
                upper.append(val)
        
        return lower, upper
    
    def _is_violation(
        self,
        val: float,
        min_val: float,
        max_val: float,
        operator: Operator
    ) -> bool:
        """Kiểm tra xem có vi phạm không."""
        if operator == Operator.LESS:
            return val < min_val
        elif operator == Operator.LESS_EQUAL:
            return val <= min_val
        elif operator == Operator.GREATER:
            return val > max_val
        elif operator == Operator.GREATER_EQUAL:
            return val >= max_val
        return False
```

### 3.4 Weever Layer (Incremental Indexing)

```python
# =============================================================================
# WAVES/wever.py
# =============================================================================

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Generic, TypeVar, Callable
from enum import Enum
import bisect


T = TypeVar('T')


class AVLNode(Generic[T]):
    """
    AVL Tree node với LT (Less-Than) aggregate.
    
    CRITICAL: Mỗi node lưu trữ tất cả TIDs nhỏ hơn nó (lt_aggregate).
    Đây là cốt lõi của Weever algorithm.
    """
    
    def __init__(
        self,
        key: T,
        tuple_data: Any,
        left: Optional['AVLNode[T]'] = None,
        right: Optional['AVLNode[T]'] = None,
        height: int = 1
    ):
        self.key = key
        self.tuple_data = tuple_data
        self.left = left
        self.right = right
        self.height = height
        
        # CRITICAL: Less-Than aggregate
        # Tất cả TIDs trong subtree nhỏ hơn self.key
        self.lt_aggregate: Set[Any] = {id(tuple_data)}
        self.lt_count: int = 1
    
    def update_lt_aggregate(self) -> None:
        """Cập nhật lt_aggregate sau khi insert/delete."""
        self.lt_aggregate.clear()
        self.lt_count = 1
        
        if self.left:
            self.lt_aggregate.update(self.left.lt_aggregate)
            self.lt_aggregate.add(id(self.left.tuple_data))
            self.lt_count += self.left.lt_count + 1
    
    def get_lt_count(self) -> int:
        """Lấy số lượng TIDs nhỏ hơn node này."""
        return self.lt_count


class LTTree(Generic[T]):
    """
    Less-Than Tree - Weever's core data structure.
    
    Mỗi node lưu trữ:
    1. Key (giá trị column)
    2. Tuple data
    3. lt_aggregate: tất cả tuples nhỏ hơn key
    
    Operations:
    - Insert: O(log N)
    - Delete: O(log N)
    - Query less than: O(log N) + |result| (trả về lt_aggregate)
    """
    
    def __init__(
        self,
        column: str,
        column_type: str = "int",
        config: Optional['WeeverConfig'] = None
    ):
        self.column = column
        self.column_type = column_type
        self.config = config or WeeverConfig()
        self.root: Optional[AVLNode[T]] = None
        self.size: int = 0
    
    def insert(self, key: T, tuple_data: Any) -> None:
        """Chèn một tuple vào tree."""
        if self.root is None:
            self.root = AVLNode(key=key, tuple_data=tuple_data)
        else:
            self._insert_recursive(self.root, key, tuple_data)
        
        self.size += 1
        self._update_heights(self.root)
        self.root = self._rebalance(self.root)
    
    def delete(self, key: T, tuple_data: Any) -> bool:
        """Xóa một tuple khỏi tree."""
        if self.root is None:
            return False
        
        deleted = self._delete_recursive(self.root, key, tuple_data)
        if deleted:
            self.size -= 1
            self._update_heights(self.root)
            self.root = self._rebalance(self.root)
        
        return deleted
    
    def query_less_than(self, key: T) -> List[Any]:
        """
        Query tất cả tuples có key < given key.
        
        CRITICAL: Sử dụng lt_aggregate để query nhanh O(log N).
        """
        if self.root is None:
            return []
        
        results = []
        self._query_recursive(self.root, key, results)
        return results
    
    def query_less_equal(self, key: T) -> List[Any]:
        """Query tất cả tuples có key <= given key."""
        results = self.query_less_than(key)
        # Thêm node có key == given key
        node = self._find_node(self.root, key)
        if node:
            results.append(node.tuple_data)
        return results
    
    def _insert_recursive(self, node: AVLNode[T], key: T, tuple_data: Any) -> AVLNode[T]:
        """Chèn đệ quy."""
        # Insert như BST
        if key < node.key:
            if node.left is None:
                node.left = AVLNode(key=key, tuple_data=tuple_data)
            else:
                node.left = self._insert_recursive(node.left, key, tuple_data)
        else:
            if node.right is None:
                node.right = AVLNode(key=key, tuple_data=tuple_data)
            else:
                node.right = self._insert_recursive(node.right, key, tuple_data)
        
        # Update lt_aggregate
        node.lt_aggregate.add(id(tuple_data))
        node.lt_count += 1
        
        # Rebalance nếu cần
        return self._rebalance(node)
    
    def _delete_recursive(
        self,
        node: Optional[AVLNode[T]],
        key: T,
        tuple_data: Any
    ) -> Optional[AVLNode[T]]:
        """Xóa đệ quy."""
        if node is None:
            return None
        
        if key < node.key:
            node.left = self._delete_recursive(node.left, key, tuple_data)
            if node.left:
                node.left.lt_aggregate.discard(id(tuple_data))
                node.left.lt_count -= 1
        elif key > node.key:
            node.right = self._delete_recursive(node.right, key, tuple_data)
        else:
            # Found node to delete
            if node.left is None:
                return node.right
            elif node.right is None:
                return node.left
            else:
                # Replace with inorder successor
                successor = self._find_min(node.right)
                node.key = successor.key
                node.tuple_data = successor.tuple_data
                node.right = self._delete_recursive(node.right, successor.key, successor.tuple_data)
        
        self._update_heights(node)
        return self._rebalance(node)
    
    def _query_recursive(
        self,
        node: Optional[AVLNode[T]],
        key: T,
        results: List[Any]
    ) -> None:
        """Query đệ quy."""
        if node is None:
            return
        
        if key < node.key:
            # Key nhỏ hơn current node, đi sang trái
            # Nhưng vẫn cần kiểm tra lt_aggregate của node
            self._query_recursive(node.left, key, results)
        elif key > node.key:
            # Key lớn hơn current node
            # Thêm current node và tất cả lt_aggregate của nó
            # + đi sang phải
            results.append(node.tuple_data)
            results.extend([
                t for t in self._collect_lt_aggregate(node.left)
                if t != node.tuple_data
            ])
            self._query_recursive(node.right, key, results)
        else:
            # Key == current node
            # Thêm tất cả lt_aggregate của current node
            results.extend(self._collect_lt_aggregate(node.left))
    
    def _collect_lt_aggregate(self, node: Optional[AVLNode[T]]) -> List[Any]:
        """Thu thập tất cả tuples trong lt_aggregate của subtree."""
        if node is None:
            return []
        results = [node.tuple_data]
        if node.left:
            results.extend(self._collect_lt_aggregate(node.left))
        return results
    
    def _find_node(self, node: Optional[AVLNode[T]], key: T) -> Optional[AVLNode[T]]:
        """Tìm node có key."""
        if node is None:
            return None
        if key < node.key:
            return self._find_node(node.left, key)
        elif key > node.key:
            return self._find_node(node.right, key)
        return node
    
    def _find_min(self, node: AVLNode[T]) -> AVLNode[T]:
        """Tìm node có key nhỏ nhất trong subtree."""
        while node.left:
            node = node.left
        return node
    
    def _update_heights(self, node: Optional[AVLNode[T]]) -> None:
        """Cập nhật heights."""
        if node:
            node.height = 1 + max(
                self._height(node.left),
                self._height(node.right)
            )
    
    def _height(self, node: Optional[AVLNode[T]]) -> int:
        """Lấy height của node."""
        return node.height if node else 0
    
    def _balance_factor(self, node: Optional[AVLNode[T]]) -> int:
        """Tính balance factor."""
        return self._height(node.left) - self._height(node.right) if node else 0
    
    def _rebalance(self, node: Optional[AVLNode[T]]) -> Optional[AVLNode[T]]:
        """Rebalance cây AVL."""
        if node is None:
            return None
        
        # Update lt_aggregate
        node.update_lt_aggregate()
        
        # Check balance
        bf = self._balance_factor(node)
        
        # Left heavy
        if bf > 1:
            if self._balance_factor(node.left) < 0:
                node.left = self._rotate_left(node.left)
            return self._rotate_right(node)
        
        # Right heavy
        if bf < -1:
            if self._balance_factor(node.right) > 0:
                node.right = self._rotate_right(node.right)
            return self._rotate_left(node)
        
        return node
    
    def _rotate_right(self, y: AVLNode[T]) -> AVLNode[T]:
        """Rotate right."""
        x = y.left
        T2 = x.right
        
        x.right = y
        y.left = T2
        
        self._update_heights(y)
        self._update_heights(x)
        
        # Update lt_aggregates
        y.update_lt_aggregate()
        x.update_lt_aggregate()
        
        return x
    
    def _rotate_left(self, x: AVLNode[T]) -> AVLNode[T]:
        """Rotate left."""
        y = x.right
        T2 = y.left
        
        y.left = x
        x.right = T2
        
        self._update_heights(x)
        self._update_heights(y)
        
        # Update lt_aggregates
        x.update_lt_aggregate()
        y.update_lt_aggregate()
        
        return y


class PredicateScheduler:
    """
    Schedule predicates để optimize execution.
    
    CRITICAL: Đây là Weever's PredicateScheduler.
    """
    
    def __init__(
        self,
        use_selectivity: bool = True,
        use_prefix_sharing: bool = True
    ):
        self.use_selectivity = use_selectivity
        self.use_prefix_sharing = use_prefix_sharing
    
    def schedule(self, dc: DenialConstraint) -> List[Predicate]:
        """
        Sắp xếp predicates theo thứ tự tối ưu.
        
        Strategy:
        1. Equality predicates trước (ít selectivity)
        2. Inequality predicates có selectivity thấp trước
        3. Shử dụng prefix sharing nếu có
        """
        predicates = list(dc.predicates)
        
        if self.use_selectivity:
            # Sort by selectivity (ít kết quả trước)
            predicates.sort(key=self._estimate_selectivity)
        
        return predicates
    
    def _estimate_selectivity(self, pred: Predicate) -> float:
        """
        Ước lượng selectivity của predicate.
        
        Returns:
            0.0 = very selective (ít kết quả)
            1.0 = not selective (nhiều kết quả)
        """
        if pred.operator == Operator.EQUAL:
            return 0.1  # Rất selective
        elif pred.operator in (Operator.NOT_EQUAL,):
            return 0.9  # Không selective
        else:
            return 0.5  # Trung bình
```

---

## 4. API Design - Mở rộng Stream DaQ

```python
# =============================================================================
# WAVES/__init__.py - Main API
# =============================================================================

# ============================================================================
# QUAN TRỌNG: Import nguyên Stream DaQ gốc
# ============================================================================
from streamdaq import StreamDaQ as _StreamDaQBase
from streamdaq import DaQMeasures as _DaQMeasures
from streamdaq import Window, TumblingWindow, SlidingWindow, SessionWindow
from streamdaq import Task as _TaskBase

# Import các component mới
from .watermark import WatermarkHandler, WatermarkStatus, WatermarkEvent
from .windowing import WindowType, WeeverConfig
from .dc_checker import DenialConstraint, Predicate, Operator, DCChecker, DCViolation
from .wever import LTTree, PredicateScheduler


# ============================================================================
# WAVES: MỞ RỘNG StreamDaQ, KHÔNG THAY THẾ
# ============================================================================

class WAVES(_StreamDaQBase):
    """
    WAVES: Watermark-Aware Violation Detection for Streaming.
    
    KẾ THỪA HOÀN TOÀN StreamDaQ:
    - Tất cả methods của StreamDaQ đều hoạt động
    - Thêm các methods mới cho DC checking và Watermark
    
    EXAMPLE:
    --------
    ```python
    from waves import WAVES, Window, WindowType
    
    # Initialize - y hệt như StreamDaQ
    waves = WAVES()
    
    # Configure - y hệt như StreamDaQ
    waves.configure(
        window=Window(WindowType.TUMBLING, size=timedelta(minutes=5)),
        source=csv_source("data.csv")
    )
    
    # === Stream DaQ API (GIỮ NGUYÊN) ===
    waves.add_check("null_count", "destination", threshold=0.1)
    waves.add_check("range", "fare", min=0, max=100)
    waves.add_check("distinct_count", "passenger_count")
    
    # === WAVES API (MỚI) ===
    waves.configure_watermark(
        watermark_interval=timedelta(minutes=1),
        max_lateness=timedelta(minutes=5)
    )
    waves.add_dc("dc1", "NOT (s.State = t.State AND s.Salary <= t.Salary AND s.Rate > t.Rate)")
    waves.enable_wever(indexed_columns=["Salary", "Rate"])
    
    # Start monitoring - y hệt như StreamDaQ
    meta_stream = waves.watch()
    
    # Process results
    for result in meta_stream:
        # Result giờ có thêm:
        # - result.watermark_status
        # - result.dc_violations
        # - result.provisional / result.final
        print(result)
    ```
    """
    
    # ============================================================================
    # KHỞI TẠO - GỌI PARENT CLASS
    # ============================================================================
    
    def __init__(
        self,
        *args,
        enable_waves_features: bool = True,  # Feature toggle
        **kwargs
    ):
        # Gọi constructor của StreamDaQ
        super().__init__(*args, **kwargs)
        
        # =========================================================================
        # CÁC COMPONENT MỚI CỦA WAVES
        # =========================================================================
        
        # Watermark Handler (từ Section 3.1)
        self._watermark: Optional[WatermarkHandler] = None
        
        # DC Checker - port từ Rapidash (từ Section 3.3)
        self._dc_checker: Optional[DCChecker] = None
        
        # Weever Scheduler - từ Weever (từ Section 3.4)
        self._wever_scheduler: Optional[PredicateScheduler] = None
        
        # Cấu hình
        self._enable_waves = enable_waves_features
        self._wever_config: Optional[WeeverConfig] = None
        
        # DC constraints đã đăng ký
        self._dcs: List[DenialConstraint] = []
    
    # ============================================================================
    # STREAQ DAQ API (GIỮ NGUYÊN - INHERITED)
    # ============================================================================
    # Tất cả methods dưới đây được KẾ THỪA từ StreamDaQ:
    # - configure()
    # - add_check()
    # - watch()
    # - watch_out()
    # - get_measures()
    # - etc.
    # KHÔNG CẦN OVERRIDE vì vẫn hoạt động đúng
    
    # ============================================================================
    # WAVES API (MỞ RỘNG MỚI)
    # ============================================================================
    
    def configure_watermark(
        self,
        watermark_interval: timedelta = timedelta(minutes=1),
        max_lateness: timedelta = timedelta(minutes=5),
        alignment_mode: str = "event_time"
    ) -> 'WAVES':
        """
        Configure watermark cho late data handling.
        
        Args:
            watermark_interval: Khoảng thời gian giữa các watermark events
            max_lateness: Thời gian tối đa một tuple được coi là "late"
            alignment_mode: "event_time" | "processing_time" | "heuristic"
        
        Returns:
            self (fluent API)
        
        EXAMPLE:
        --------
        ```python
        waves.configure_watermark(
            watermark_interval=timedelta(minutes=1),
            max_lateness=timedelta(minutes=5)
        )
        ```
        """
        self._watermark = WatermarkHandler(
            watermark_interval=watermark_interval,
            max_lateness=max_lateness,
            alignment_mode=alignment_mode
        )
        return self
    
    def add_dc(
        self,
        dc_id: str,
        dc_string: str,
        enabled: bool = True
    ) -> 'WAVES':
        """
        Thêm một Denial Constraint (DC) để kiểm tra.
        
        Args:
            dc_id: Unique identifier cho DC
            dc_string: DC dạng string
                Format: "NOT (s.col1 = t.col1 AND s.col2 < t.col2 AND ...)"
            enabled: Enable/disable DC
        
        Returns:
            self (fluent API)
        
        EXAMPLE:
        --------
        ```python
        # DC: Không có taxi nào có fare < 0 nếu distance > 0
        waves.add_dc(
            "dc_no_free_rides",
            "NOT (s.Distance > 0 AND s.Fare < 0)"
        )
        
        # DC với nhiều predicates
        waves.add_dc(
            "dc_salary_order",
            "NOT (s.State = t.State AND s.Salary < t.Salary AND s.Rank > t.Rank)"
        )
        ```
        """
        dc = DenialConstraint.from_string(dc_id, dc_string)
        dc.enabled = enabled
        self._dcs.append(dc)
        
        # Lazy initialization của DC checker
        if self._dc_checker is None:
            self._dc_checker = DCChecker(
                constraints=[],
                indexed_columns=self._get_dc_indexed_columns()
            )
        
        self._dc_checker.add_constraint(dc)
        return self
    
    def enable_wever(
        self,
        indexed_columns: List[str] = None,
        enable_scheduling: bool = True
    ) -> 'WAVES':
        """
        Bật Weever integration cho incremental indexing.
        
        Args:
            indexed_columns: Các columns cần index cho DC queries
            enable_scheduling: Bật predicate scheduling
        
        Returns:
            self (fluent API)
        
        EXAMPLE:
        --------
        ```python
        waves.enable_wever(
            indexed_columns=["Salary", "Rate", "Distance", "Fare"],
            enable_scheduling=True
        )
        ```
        """
        self._wever_config = WeeverConfig(
            enabled=True,
            indexed_columns=[
                ColumnConfig(name=col, type="float") 
                for col in (indexed_columns or [])
            ],
            enable_scheduling=enable_scheduling
        )
        
        # Initialize scheduler
        self._wever_scheduler = PredicateScheduler(
            use_selectivity=enable_scheduling
        )
        
        return self
    
    def get_watermark_status(self) -> WatermarkStatus:
        """
        Lấy watermark status hiện tại.
        
        Returns:
            Current watermark timestamp
        """
        if self._watermark is None:
            return WatermarkStatus.PROVISIONAL  # Default
        return self._watermark.current_watermark
    
    def get_dc_violations(self, window_id: str) -> List[DCViolation]:
        """
        Lấy DC violations cho một window.
        
        Args:
            window_id: ID của window
        
        Returns:
            List of DCViolation objects
        """
        if self._dc_checker is None:
            return []
        return self._dc_checker.get_violations(window_id)
    
    # ============================================================================
    # INTERNAL METHODS
    # ============================================================================
    
    def _get_dc_indexed_columns(self) -> List[str]:
        """Extract columns cần index từ tất cả DCs."""
        columns = set()
        for dc in self._dcs:
            for pred in dc.predicates:
                columns.add(pred.left_column)
                columns.add(pred.right_column)
        return list(columns)
    
    def _process_with_watermark(self, tuple_data, timestamp):
        """
        Xử lý tuple với watermark awareness.
        
        Internal method được gọi trong pipeline xử lý.
        """
        if self._watermark is None:
            return tuple_data, False  # No watermark, process normal
        
        result = self._watermark.process_tuple(
            tuple_data=tuple_data,
            tuple_timestamp=timestamp,
            window_id=self._get_current_window_id(),
            source_id=self._source_id
        )
        
        return tuple_data, result.is_late
    
    def _check_dc_violations(self, window_data) -> List[DCViolation]:
        """
        Kiểm tra DC violations cho window data.
        
        Internal method được gọi khi window complete.
        """
        if self._dc_checker is None or not self._dcs:
            return []
        
        return self._dc_checker.check_window(
            tuples=window_data,
            window_id=self._get_current_window_id()
        )
    
    # ============================================================================
    # OVERRIDE watch() để tích hợp WAVES features
    # ============================================================================
    
    def watch(self, *args, **kwargs):
        """
        Start monitoring - OVERRIDE để thêm WAVES features.
        
        Returns:
            MetaStream với cả Stream DaQ results và DC violations
        """
        # Gọi parent watch() để lấy Stream DaQ meta-stream
        base_meta_stream = super().watch(*args, **kwargs)
        
        if not self._enable_waves:
            return base_meta_stream
        
        # Wrap với WAVES enrichment
        return WAVESMetaStream(
            base_meta_stream=base_meta_stream,
            waves=self
        )


class WAVESMetaStream:
    """
    Meta-stream wrapper thêm WAVES features.
    
    Enriches Stream DaQ meta-stream với:
    - Watermark status
    - DC violations (từ Rapidash)
    - Provisional/Final status
    """
    
    def __init__(self, base_meta_stream, waves: 'WAVES'):
        self._base = base_meta_stream
        self._waves = waves
    
    def __iter__(self):
        for base_result in self._base:
            # Enrich với WAVES data
            enriched = WAVESResult(
                base_result=base_result,
                watermark_status=self._waves.get_watermark_status(),
                dc_violations=self._waves.get_dc_violations(
                    base_result.window_id
                ),
                is_provisional=(
                    self._waves.get_watermark_status() 
                    != WatermarkStatus.FINAL
                )
            )
            yield enriched


@dataclass
class WAVESResult:
    """
    Enriched result từ WAVES.
    
    Chứa:
    - Tất cả fields từ Stream DaQ result
    - Thêm watermark_status, dc_violations
    """
    base_result: Any  # StreamDaQ result
    
    # WAVES extensions
    watermark_status: WatermarkStatus
    dc_violations: List[DCViolation]
    is_provisional: bool
    
    # Proxy tất cả attributes từ base_result
    def __getattr__(self, name):
        return getattr(self.base_result, name)


# ============================================================================
# BACKWARD COMPATIBILITY: StreamDaQ vẫn hoạt động độc lập
# ============================================================================

class StreamDaQ(_StreamDaQBase):
    """
    Stream DaQ gốc - vẫn hoạt động không thay đổi.
    
    Dùng class này nếu KHÔNG cần WAVES features.
    """
    pass


# ============================================================================
# DAQMeasures - Import nguyên từ StreamDaQ
# ============================================================================

# Sử dụng trực tiếp từ streamdaq
DaQMeasures = _DaQMeasures
```

---

## 5. Porting Strategy: Java → Python

### 5.1 Files cần port

```
JAVA (Rapidash)                          PYTHON (WAVES)
─────────────────────────────────────────────────────────────────────
src/main/java/org/dc/
├── DCVerifier.java              ───▶   dc_checker.py
├── Constraint.java              ───▶   dc_checker.py (DenialConstraint)
├── Predicate.java               ───▶   dc_checker.py (Predicate)
└── Main.java                    ───▶   dc_checker.py (CLI)

src/main/java/rangetree/           ───▶   rangetree.py
src/main/java/kdrange/            ───▶   kdtree.py
src/main/java/kdrangeDouble/       ───▶   kdtree_double.py

JAVA (Weever)                         PYTHON (WAVES)
─────────────────────────────────────────────────────────────────────
src/main/java/de/hpi/isg/
├── WeeverSequential.java       ───▶   wever.py (LTTree)
├── PredicateScheduler.java     ───▶   wever.py (PredicateScheduler)
├── schedules/Intermediate*.java──▶   wever.py (IncrementalMaintainer)
├── tidSets/RoaringTidSet.java  ───▶   wever.py (TIdSet - optional)
└── dataStructures/ltAggregateMap/──▶  wever.py (LT-Aggregate)
```

### 5.2 Priority

| Priority | Component | Effort | Impact |
|---------|-----------|--------|--------|
| 1 | DC Checker (DCChecker) | Medium | HIGH |
| 2 | KD-Tree | Low | HIGH |
| 3 | Range-Tree | Medium | MEDIUM |
| 4 | LT-Tree (Weever) | High | HIGH |
| 5 | Predicate Scheduler | Medium | MEDIUM |
| 6 | TIdSet (optional) | Low | LOW |

---

---

## 6. Integration Strategy - Refactor vs Extend

### 6.1 Khi nào REFACTOR, khi nào EXTEND?

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    REFACTOR vs EXTEND DECISION                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐  │
│  │          EXTEND                │  │         REFACTOR                 │  │
│  │   (Thêm code mới bên ngoài)   │  │   (Sửa internal structure)      │  │
│  ├─────────────────────────────────┤  ├─────────────────────────────────┤  │
│  │                                 │  │                                 │  │
│  │ • Windowing API                 │  │ • Window state management      │  │
│  │ • Check registration            │  │ • Data flow between components │  │
│  │ • Meta-stream output            │  │ • How tuples flow              │  │
│  │ • Public methods                │  │ • Internal state storage        │  │
│  │ • Configuration                │  │ • Pipeline execution            │  │
│  │                                 │  │ • Check result aggregation     │  │
│  │                                 │  │                                 │  │
│  │ DÙNG KHI:                      │  │ DÙNG KHI:                      │  │
│  │ • Thêm feature mới hoàn toàn   │  │ • Tích hợp sâu watermark       │  │
│  │ • Không ảnh hưởng existing    │  │ • DC checker cần window state  │  │
│  │ • Có thể disable/enable        │  │ • LT-Tree cần incremental      │  │
│  │                                 │  │ • Retraction cần tracking      │  │
│  └─────────────────────────────────┘  └─────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Các vùng CÓ THỂ phải refactor

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              CÁC VÙNG CÓ THỂ PHẢI REFACTOR TRONG StreamDaQ                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. PIPELINE EXECUTION                                                     │
│     ┌───────────────────────────────────────────────────────────────────┐  │
│     │ Stream DaQ hiện tại:                                              │  │
│     │   Tuple → Window → Checks → Meta-Stream                           │  │
│     │                                                                   │  │
│     │ WAVES cần thêm:                                                   │  │
│     │   Tuple → [Watermark] → Window → [DC Checks] → [Watermark Status]│  │
│     │                          → Checks → Meta-Stream                   │  │
│     │                                                                   │  │
│     │ REFACTOR: Thêm bước xử lý mới vào pipeline                       │  │
│     └───────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  2. WINDOW STATE MANAGEMENT                                                 │
│     ┌───────────────────────────────────────────────────────────────────┐  │
│     │ Stream DaQ hiện tại:                                              │  │
│     │   WindowState chỉ lưu tuples và aggregates                        │  │
│     │                                                                   │  │
│     │ WAVES cần thêm:                                                   │  │
│     │   WindowState cần lưu:                                            │  │
│     │     • LT-Tree indexes (cho DC checking)                          │  │
│     │     • Provisional results (cho retraction)                       │  │
│     │     • Late tuple buffer                                          │  │
│     │                                                                   │  │
│     │ REFACTOR: Mở rộng WindowState class                              │  │
│     └───────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  3. CHECK RESULT AGGREGATION                                                │
│     ┌───────────────────────────────────────────────────────────────────┐  │
│     │ Stream DaQ hiện tại:                                              │  │
│     │   Results chỉ chứa check results                                  │  │
│     │                                                                   │  │
│     │ WAVES cần thêm:                                                   │  │
│     │   Results cần chứa thêm:                                          │  │
│     │     • DC violation results                                        │  │
│     │     • Watermark status                                            │  │
│     │     • Provisional/Final flag                                      │  │
│     │                                                                   │  │
│     │ REFACTOR: Mở rộng Result format                                   │  │
│     └───────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  4. TUPLE PROCESSING (Watermark integration)                                │
│     ┌───────────────────────────────────────────────────────────────────┐  │
│     │ Stream DaQ hiện tại:                                              │  │
│     │   Tuple với timestamp → Assign vào window                         │  │
│     │                                                                   │  │
│     │ WAVES cần thêm:                                                   │  │
│     │   Tuple với timestamp → Check watermark → Buffer late → Window    │  │
│     │                                                                   │  │
│     │ REFACTOR: Thêm watermark check vào tuple processing              │  │
│     └───────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 Chiến lược refactor an toàn

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     REFACTOR AN TOÀN                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  BƯỚC 1: WRAPPER / FACADE trước                                             │
│  ─────────────────────────────────────                                     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │  class WAVESFacade:                                             │       │
│  │      """Wrapper không sửa StreamDaQ internal."""                 │       │
│  │                                                               │       │
│  │      def __init__(self, streamdaq_instance):                   │       │
│  │          self._daq = streamdaq_instance                        │       │
│  │          self._watermark = WatermarkHandler()                  │       │
│  │          self._dc_checker = DCChecker()                        │       │
│  │                                                               │       │
│  │      def watch(self):                                          │       │
│  │          base_stream = self._daq.watch()                       │       │
│  │          return EnrichedMetaStream(base_stream, self)          │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│  BƯỚC 2: NẾU FACADE không đủ → REFACTOR INTERNAL                           │
│  ───────────────────────────────────────────────                            │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │  # Đánh dấu rõ ràng những gì đang refactor                     │       │
│  │  class StreamDaQ:                                               │       │
│  │      # WAVES_INTEGRATION_START                                  │       │
│  │      def _process_tuple_waves(self, tuple_data, timestamp):     │       │
│  │          # Refactored code                                      │       │
│  │      # WAVES_INTEGRATION_END                                    │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│  BƯỚC 3: TESTING CHẶT CHẼ                                                   │
│  ────────────────────────                                                   │
│                                                                             │
│  • Unit tests cho từng refactored component                                │
│  • Integration tests cho toàn bộ StreamDaQ (không WAVES)                   │
│  • Đảm bảo StreamDaQ độc lập vẫn chạy đúng                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.4 Hai cách tiếp cận

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CÁCH TIẾP CẬN 1: FACADE (An toàn)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  WAVES = StreamDaQ (độc lập) + WAVES wrapper                              │
│                                                                             │
│  ┌─────────────────────┐      ┌─────────────────────┐                      │
│  │    StreamDaQ        │      │    WAVES Wrapper    │                      │
│  │    (GIỮ NGUYÊN)    │      │    (THÊM MỚI)      │                      │
│  ├─────────────────────┤      ├─────────────────────┤                      │
│  │ • Windowing ✓       │      │ • Watermark ✓       │                      │
│  │ • Checks ✓          │◄────►│ • DC Checker ✓      │                      │
│  │ • Meta-stream ✓     │      │ • LT-Tree ✓         │                      │
│  │ • API ✓             │      │ • Retraction ✓       │                      │
│  └──────────┬──────────┘      └──────────┬──────────┘                      │
│             │                            │                                  │
│             └──────────┬─────────────────┘                                  │
│                        │                                                     │
│                        ▼                                                     │
│              ┌─────────────────────┐                                         │
│              │   WAVES OUTPUT     │                                         │
│              │ • StreamDaQ results│                                         │
│              │ • DC violations    │                                         │
│              │ • Watermark status │                                         │
│              └─────────────────────┘                                         │
│                                                                             │
│  ƯU ĐIỂM: Không sửa StreamDaQ, dễ maintain                                │
│  NHƯỢC ĐIỂM: Overhead, không tích hợp sâu                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                   CÁCH TIẾP CẬN 2: INTEGRATION (Sâu)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  WAVES = StreamDaQ (REFACTORED) + WAVES features                           │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          StreamDaQ Core                            │   │
│  │                                                                   │   │
│  │   ┌──────────────────────────────────────────────────────────┐   │   │
│  │   │  Tích hợp WAVES vào internal:                           │   │   │
│  │   │                                                           │   │   │
│  │   │  • WindowState → thêm LT-Tree indexes                   │   │   │
│  │   │  • Pipeline → thêm watermark check                       │   │   │
│  │   │  • Result → thêm DC + watermark fields                   │   │   │
│  │   │                                                           │   │   │
│  │   └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                        │
│                                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         WAVES                                      │   │
│  │                                                                   │   │
│  │   class WAVES:                                                   │   │
│  │       def __init__(self):                                        │   │
│  │           super().__init__()  # Gọi StreamDaQ                    │   │
│  │           self._watermark = WatermarkHandler()                    │   │
│  │           self._dc_checker = DCChecker()                          │   │
│  │                                                                   │   │
│  │   ƯU ĐIỂM: Tích hợp sâu, hiệu suất tốt hơn                      │   │
│  │   NHƯỢC ĐIỂM: Phức tạp hơn, cần refactor internal                │   │
│  │                                                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.5 Khuyến nghị

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        KHUYẾN NGHỊ                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  GIAI ĐOẠN 1: Prototype (1-2 tháng đầu)                                    │
│  ──────────────────────────────────────                                    │
│  → DÙNG CÁCH 1 (FACADE)                                                   │
│  → Nhanh chóng validate ý tưởng                                           │
│  → Không rủi ro cho StreamDaQ gốc                                         │
│                                                                             │
│  GIAI ĐOẠN 2: Production (sau khi validate)                              │
│  ────────────────────────────────────────────                              │
│  → CHUYỂN Sang CÁCH 2 (INTEGRATION) nếu cần:                             │
│    • Hiệu suất không đạt yêu cầu                                          │
│    • Cần tích hợp sâu watermark/retraction                               │
│    • Cần DC checking real-time trong window state                          │
│                                                                             │
│  GIAI ĐOẠN 3: Optimize                                                    │
│  ────────────────────                                                      │
│  → Refactor từng phần dựa trên profiling                                  │
│  → Có thể quay lại FACADE nếu integration quá phức tạp                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. API Design

```python
# ============================================================================
# WAVES/__init__.py - Main API
# ============================================================================

# ============================================================================
# CÁCH 1: FACADE PATTERN (An toàn)
# ============================================================================

class WAVES:
    """
    WAVES - Facade wrapper around StreamDaQ.
    
    Dùng facade pattern để không sửa StreamDaQ internal.
    """
    
    def __init__(
        self,
        enable_waves_features: bool = True,
        integration_mode: str = "facade"  # "facade" | "integration"
    ):
        # Tạo StreamDaQ instance
        self._streamdaq = StreamDaQ()
        
        # WAVES components
        self._watermark: Optional[WatermarkHandler] = None
        self._dc_checker: Optional[DCChecker] = None
        self._wever_scheduler: Optional[PredicateScheduler] = None
        
        # Cấu hình
        self._integration_mode = integration_mode
    
    # ============================================================================
    # DELEGATE TẤT CẢ StreamDaQ methods
    # ============================================================================
    
    def configure(self, *args, **kwargs):
        """Delegate cho StreamDaQ."""
        self._streamdaq.configure(*args, **kwargs)
        return self
    
    def add(self, measure, predicate=None, instance=None):
        """Delegate cho StreamDaQ - thêm quality check."""
        self._streamdaq.add(measure, predicate, instance)
        return self
    
    def add_check(self, *args, **kwargs):
        """Alias cho add() - StreamDaQ style."""
        return self.add(*args, **kwargs)
    
    def watch(self):
        """Override watch() để thêm WAVES enrichment."""
        base_stream = self._streamdaq.watch()
        return WAVESMetaStream(base_stream, self)
    
    def watch_out(self):
        """Delegate cho StreamDaQ."""
        return self._streamdaq.watch_out()
    
    # ============================================================================
    # WAVES NEW METHODS
    # ============================================================================
    
    def configure_watermark(
        self,
        watermark_interval: timedelta = timedelta(minutes=1),
        max_lateness: timedelta = timedelta(minutes=5)
    ):
        """Configure watermark handler."""
        self._watermark = WatermarkHandler(
            watermark_interval=watermark_interval,
            max_lateness=max_lateness
        )
        return self
    
    def add_dc(self, dc_id: str, dc_string: str):
        """Thêm DC constraint."""
        dc = DenialConstraint.from_string(dc_id, dc_string)
        
        if self._dc_checker is None:
            self._dc_checker = DCChecker()
        
        self._dc_checker.add_constraint(dc)
        return self
    
    def enable_wever(self, indexed_columns: List[str] = None):
        """Bật Weever incremental indexing."""
        self._wever_scheduler = PredicateScheduler()
        return self


# ============================================================================
# CÁCH 2: INTEGRATION PATTERN (Sâu hơn) - Khi cần hiệu suất
# ============================================================================

class WAVESIntegrated(StreamDaQ):
    """
    WAVES - Tích hợp trực tiếp vào StreamDaQ.
    
    REFACTOR StreamDaQ internal để tích hợp sâu hơn.
    Chỉ dùng khi FACADE không đủ hiệu suất.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # WAVES components
        self._watermark: Optional[WatermarkHandler] = None
        self._dc_checker: Optional[DCChecker] = None
        self._wever_config: Optional[WeeverConfig] = None
        
        # REFACTOR: Mở rộng window state
        self._window_states: Dict[str, WindowStateExtended] = {}
    
    def configure_watermark(self, *args, **kwargs):
        """Configure watermark - tích hợp vào pipeline."""
        self._watermark = WatermarkHandler(*args, **kwargs)
        
        # REFACTOR: Hook vào tuple processing
        self._original_process_tuple = self._process_tuple
        self._process_tuple = self._process_tuple_with_watermark
        
        return self
    
    # ============================================================================
    # REFACTOR: Mở rộng internal methods
    # ============================================================================
    
    def _process_tuple_with_watermark(self, tuple_data, timestamp):
        """
        REFACTORED: Xử lý tuple với watermark awareness.
        
        Thêm watermark check vào pipeline.
        """
        if self._watermark is None:
            return self._original_process_tuple(tuple_data, timestamp)
        
        # 1. Watermark check
        wm_result = self._watermark.process_tuple(
            tuple_data=tuple_data,
            tuple_timestamp=timestamp,
            window_id=self._get_current_window_id(),
            source_id=self._source_id
        )
        
        if wm_result.action == TupleAction.DISCARD:
            return None  # Bỏ qua tuple
        
        # 2. Nếu late, buffer nhưng vẫn process
        if wm_result.action == TupleAction.BUFFER_LATE:
            self._buffer_late_tuple(wm_result.late_tuple)
        
        # 3. Process bình thường
        return self._original_process_tuple(tuple_data, timestamp)
    
    def _process_window_complete_extended(self, window_id, window_data):
        """
        REFACTORED: Xử lý window complete với DC checking.
        
        Thêm DC violation check vào window processing.
        """
        # 1. StreamDaQ checks (gốc)
        results = self._process_window_complete_original(window_id, window_data)
        
        # 2. WAVES: DC checking (mới)
        if self._dc_checker is not None:
            dc_violations = self._dc_checker.check_window(
                tuples=window_data,
                window_id=window_id
            )
            results.dc_violations = dc_violations
        
        # 3. WAVES: Watermark status
        if self._watermark is not None:
            results.watermark_status = self._watermark.get_status(window_id)
        
        return results


class WindowStateExtended:
    """
    REFACTORED: Mở rộng WindowState để hỗ trợ WAVES.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # WAVES extensions
        self.lt_indexes: Dict[str, LTTree] = {}  # DC checking
        self.late_tuples: List[LateTuple] = []    # Late arrivals
        self.provisional_results: Dict = {}       # Cho retraction


# ============================================================================
# FACTORY: Chọn implementation phù hợp
# ============================================================================

def create_waves(
    mode: str = "facade",  # "facade" | "integrated"
    **kwargs
) -> WAVES:
    """
    Factory function để tạo WAVES instance.
    
    Args:
        mode: "facade" - An toàn, không sửa StreamDaQ
              "integrated" - Tích hợp sâu, hiệu suất tốt hơn
    """
    if mode == "facade":
        return WAVES(**kwargs)
    elif mode == "integrated":
        return WAVESIntegrated(**kwargs)
    else:
        raise ValueError(f"Unknown mode: {mode}")

### 6.1 Data Flow Integration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         INTEGRATION POINTS                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. WATERMARK ──▶ WINDOWING                                             │
│     - WindowManager nhận WatermarkHandler qua constructor                  │
│     - WindowManager gọi watermark.process_tuple() cho mỗi tuple          │
│     - WindowManager nhận TupleProcessResult                              │
│                                                                          │
│  2. WINDOWING ──▶ CHECK PIPELINE                                       │
│     - Window emit WindowProcessResult chứa:                             │
│       • tuples: List[Dict]                                              │
│       • lt_indexes: Dict[str, LTTree]  ← CRITICAL: Weever indexes       │
│     - Basic checks và DC checker chạy song song                         │
│                                                                          │
│  3. DC CHECKER ──▶ WEVER                                               │
│     - DCChecker nhận lt_indexes từ WindowState                         │
│     - DC Checker insert/delete vào LT-Tree thay vì rebuild             │
│     - DC Checker query LT-Tree cho range queries                        │
│                                                                          │
│  4. ALL ──▶ META-STREAM                                                │
│     - UnifiedResult chứa tất cả results + watermark status              │
│     - Provisional → Final transition được emit khi watermark advance   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 State Management

```python
# CRITICAL: State phải được chia sẻ giữa các components

class SharedState:
    """
    Shared state giữa tất cả components.
    
    CRITICAL: Đây là key cho integration thành công.
    """
    
    def __init__(self):
        # Watermark state
        self.watermark: WatermarkState = WatermarkState()
        
        # Window states: window_id -> WindowState
        self.windows: Dict[str, WindowState] = {}
        
        # DC results cache: window_id -> List[DCViolation]
        self.dc_results: Dict[str, List[DCViolation]] = {}
        
        # Provisional results: có thể bị retract
        self.provisional: Dict[str, Any] = {}
        
        # Final results: không thể retract
        self.final: Dict[str, Any] = {}
    
    def get_window_state(self, window_id: str) -> WindowState:
        """Lấy hoặc tạo window state."""
        if window_id not in self.windows:
            self.windows[window_id] = WindowState(window_id=window_id)
        return self.windows[window_id]
    
    def cleanup_window(self, window_id: str) -> None:
        """Cleanup window khi hết hạn."""
        # Lưu final results trước
        if window_id in self.provisional:
            self.final[window_id] = self.provisional.pop(window_id)
        
        # Xóa window state
        self.windows.pop(window_id, None)
        
        # Xóa DC results
        self.dc_results.pop(window_id, None)
```

---

## 7. Watermark Integration

### 7.1 Xuyên suốt mọi Layer

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WATERMARK INTEGRATION                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  INPUT ──▶ WATERMARK LAYER                                              │
│            │                                                              │
│            ▼                                                              │
│         ┌─────────────────────────────────────────────────────────┐       │
│         │  • Assign event time                                   │       │
│         │  • Buffer late tuples                                 │       │
│         │  • Track watermark progress                           │       │
│         │  • Emit watermark events                             │       │
│         └─────────────────────────────────────────────────────────┘       │
│            │                                                              │
│            ▼                                                              │
│         WINDOW MANAGER                                                   │
│            │                                                              │
│            ▼                                                              │
│         ┌─────────────────────────────────────────────────────────┐       │
│         │  • Mark windows as provisional/final                    │       │
│         │  • Trigger window emission on watermark                 │       │
│         │  • Cleanup expired windows                             │       │
│         │  • Retract on late data                              │       │
│         └─────────────────────────────────────────────────────────┘       │
│            │                                                              │
│            ▼                                                              │
│         CHECK PIPELINE                                                   │
│            │                                                              │
│            ▼                                                              │
│         ┌─────────────────────────────────────────────────────────┐       │
│         │  • Results tagged with provisional/final               │       │
│         │  • DC results cáo provisional cho đến khi watermark    │       │
│         │  • Retraction events khi late tuple đến               │       │
│         └─────────────────────────────────────────────────────────┘       │
│            │                                                              │
│            ▼                                                              │
│         META-STREAM OUTPUT                                               │
│            │                                                              │
│            ▼                                                              │
│         ┌─────────────────────────────────────────────────────────┐       │
│         │  • WindowResult có watermark_status field               │       │
│         │  • Provisional results có thể retract                  │       │
│         │  • Final results không retract được                    │       │
│         │  • Retraction events được emit                        │       │
│         └─────────────────────────────────────────────────────────┘       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Retraction Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RETRACTION FLOW                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. T1 arrives @ t=10:00 (on-time)                                     │
│     Window [9:00-10:00] emitted with:                                    │
│       violations = [(A, B), (C, D)]                                       │
│       status = PROVISIONAL                                                │
│                                                                          │
│  2. Watermark advances to 9:30                                           │
│     Window [9:00-10:00] still PROVISIONAL                                 │
│                                                                          │
│  3. LATE TUPLE arrives @ t=10:15, original t=9:45                        │
│     Window [9:00-10:00] phải be retracted!                              │
│                                                                          │
│     ┌─────────────────────────────────────────────────────────┐         │
│     │ RETRACTION EVENT                                       │         │
│     │ old_violations = [(A, B), (C, D)]                      │         │
│     │ new_violations = [(A, E), (C, D)]  ← B thay bằng E    │         │
│     │ retraction_type = "late_tuple_arrival"                 │         │
│     │ late_tuple_id = X                                      │         │
│     └─────────────────────────────────────────────────────────┘         │
│                                                                          │
│  4. META-STREAM emits:                                                  │
│     - RETRACTION of old result                                          │
│     - NEW result with E instead of B                                     │
│                                                                          │
│  5. Watermark advances to 10:00                                          │
│     Window [9:00-10:00] FINALIZED                                       │
│     Không còn retract được nữa                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Implementation Phases

### Phase 1: Core Infrastructure (Tuần 1-2)

```
□ Implement WatermarkHandler
  □ update_watermark()
  □ process_tuple() với late detection
  □ Provisional/final status management
  □ Retraction events

□ Implement Window cơ bản
  □ Tumbling windows
  □ Sliding windows
  □ Window state management
```

### Phase 2: Basic Checks (Tuần 3-4)

```
□ Extend Stream DaQ basic checks
  □ Integrate với WindowManager
  □ Integrate với WatermarkHandler
  □ Emit results qua MetaStream
```

### Phase 3: DC Checker - Port Rapidash (Tuần 5-8)

```
□ Implement KD-Tree
□ Implement Range-Tree
□ Implement DC parsing (DenialConstraint)
□ Implement DCChecker
  □ Single inequality optimization
  □ Multiple inequality với kd-tree
  □ Early termination
```

### Phase 4: Weever Integration (Tuần 9-12)

```
□ Implement LTTree (AVL + LT-aggregate)
□ Implement PredicateScheduler
□ Integrate LT-Tree vào WindowState
□ Incremental updates (insert/delete)
□ Connect DCChecker với LT-Tree
```

### Phase 5: Integration & Testing (Tuần 13-16)

```
□ Full integration test
□ Performance benchmark vs Stream DaQ gốc
□ NYC Taxi dataset demo
□ Documentation
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

```python
# tests/test_watermark.py

def test_late_tuple_detection():
    """Tuple có timestamp < watermark được đánh dấu late."""
    wm = WatermarkHandler(max_lateness=timedelta(minutes=5))
    wm._current_watermark = 10000.0  # 10:00
    
    result = wm.process_tuple(
        tuple_data={"id": 1},
        tuple_timestamp=9500.0,  # 9:55
        window_id="w1",
        source_id="kafka"
    )
    
    assert result.is_late == True
    assert result.action in (TupleAction.BUFFER_LATE, TupleAction.DISCARD)


def test_retraction_on_late_tuple():
    """Late tuple trigger retraction."""
    wm = WatermarkHandler()
    wm._current_watermark = 10000.0
    
    # Process tuple late
    wm.process_tuple(..., tuple_timestamp=9500.0, window_id="w1")
    
    # Trigger retraction
    retraction = wm.retract_window("w1", new_results)
    
    assert retraction is not None
    assert retraction.old_results != retraction.new_results
```

### 9.2 Integration Tests

```python
# tests/test_waves_integration.py

def test_full_pipeline():
    """Test end-to-end WAVES pipeline."""
    # Setup
    waves = WAVES()
    waves.configure(
        window=Window(WindowType.TUMBLING, size=timedelta(minutes=5))
    )
    waves.add_dc("dc1", "NOT (s.State = t.State AND s.Salary > t.Salary)")
    
    # Input tuples
    input_stream = [
        {"id": 1, "State": "NY", "Salary": 5000},
        {"id": 2, "State": "NY", "Salary": 6000},
        {"id": 3, "State": "NY", "Salary": 4000},  # Violation!
    ]
    
    # Process
    results = list(waves.watch(input_stream))
    
    # Assert
    assert len(results) == 1
    assert results[0].dc_results[0].is_violated == True
    assert results[0].violations[0] == (input_stream[2], ANY)


def test_watermark_finalization():
    """Test provisional → final transition."""
    waves = WAVES()
    waves.configure(window=Window(...))
    
    # Process tuples
    for t in tuples:
        waves.process(t)
    
    # Advance watermark
    waves.watermark.update_watermark("source", FINAL_TIMESTAMP)
    
    # Verify finalization
    final_results = [r for r in waves.results if r.watermark_status == WatermarkStatus.FINAL]
    assert len(final_results) > 0
```

### 9.3 Performance Benchmarks

```python
# benchmarks/test_scalability.py

def benchmark_sliding_window():
    """
    Benchmark WAVES vs Stream DaQ trên sliding windows.
    
    EXPECTED: WAVES Full nhanh hơn khi overlap cao.
    """
    # Setup
    waves = WAVES()
    waves.configure(
        window=Window(WindowType.SLIDING, size=timedelta(minutes=10), hop=timedelta(minutes=1))
    )
    
    # Generate 100K tuples
    tuples = generate_nyc_taxi(100000)
    
    # Benchmark
    start = time.time()
    results = list(waves.watch(stream(tuples)))
    elapsed = time.time() - start
    
    print(f"WAVES: {elapsed:.2f}s")
    
    # Compare với Stream DaQ baseline
    baseline = run_stream_daq_baseline(tuples)
    speedup = baseline / elapsed
    
    assert speedup > 1.0, "WAVES nên nhanh hơn baseline"
```

---

## 10. Files Structure

```
WAVES/
├── __init__.py                    # Main API (WAVES class)
├── watermark.py                   # WatermarkHandler
├── windowing.py                   # Window, WindowManager, WindowState
├── dc_checker.py                  # DCChecker, DenialConstraint, Predicate
├── wever.py                       # LTTree, AVLNode, PredicateScheduler
├── meta_stream.py                 # MetaStream, UnifiedResult
├── checks/                        # Basic checks (Stream DaQ)
│   ├── __init__.py
│   ├── null_check.py
│   ├── range_check.py
│   ├── freshness_check.py
│   └── ... (30+ checks)
├── utils/
│   ├── __init__.py
│   ├── kdtree.py                  # KD-Tree implementation
│   ├── rangetree.py               # Range-Tree implementation
│   └── tidset.py                  # TIdSet implementations
└── tests/
    ├── test_watermark.py
    ├── test_windowing.py
    ├── test_dc_checker.py
    ├── test_wever.py
    └── test_integration.py
```

---

## 10. Stream DaQ Gốc - Chi tiết giữ nguyên

### 10.1 Files được giữ nguyên (Import, không sửa)

```
stream-DaQ/
├── streamdaq/
│   ├── __init__.py              # Import nguyên: StreamDaQ, DaQMeasures
│   ├── StreamDaQ.py            # Giữ nguyên 100% - parent class
│   ├── DaQMeasures.py          # Giữ nguyên 100% - 50+ measures
│   ├── Windows.py              # Giữ nguyên 100% - windowing
│   ├── Task.py                 # Giữ nguyên 100% - multi-task
│   ├── SchemaValidator.py      # Giữ nguyên 100% - Pydantic validation
│   ├── CompactData.py          # Giữ nguyên 100% - IoT compact data
│   ├── CustomReducers.py       # Giữ nguyên 100% - HLL++, etc.
│   ├── utils.py                # Giữ nguyên 100%
│   ├── artificial_stream_generators.py  # Giữ nguyên 100%
│   ├── anomaly_detectors/      # Giữ nguyên 100% - Bonus feature
│   │   ├── AnomalyDetector.py
│   │   ├── StatisticalDetector.py
│   │   └── Severity.py
│   └── main.py                 # Giữ nguyên 100% - Entry point
```

### 10.2 Các class được kế thừa (Inheritance)

```python
# ============================================================================
# WAVES kế thừa hoàn toàn từ StreamDaQ
# ============================================================================

class WAVES(StreamDaQ):
    """
    WAVES mở rộng StreamDaQ bằng cách kế thừa.
    
    Tất cả methods dưới đây được KẾ THỪA từ StreamDaQ:
    """
    
    # === Parent StreamDaQ methods (GIỮ NGUYÊN) ===
    
    def configure(
        self,
        window,
        source=None,
        sink=None,
        schema=None,
        wait_for_late=None,
        # ... tất cả params khác từ StreamDaQ
    ):
        # Gọi parent method
        super().configure(window, source, sink, schema, wait_for_late)
        return self
    
    def add(self, measure, predicate=None, instance=None):
        """Add quality measure - giữ nguyên từ StreamDaQ."""
        return super().add(measure, predicate, instance)
    
    def watch(self):
        """Watch output - giữ nguyên từ StreamDaQ."""
        return super().watch()
    
    def watch_out(self):
        """Get output stream - giữ nguyên từ StreamDaQ."""
        return super().watch_out()
    
    # === WAVES new methods (MỞ RỘNG) ===
    
    def configure_watermark(...):
        """MỚI: Configure watermark."""
        pass
    
    def add_dc(...):
        """MỚI: Add denial constraint."""
        pass
    
    def enable_wever(...):
        """MỚI: Enable Weever incremental indexing."""
        pass
```

### 10.3 DaQMeasures - Import nguyên

```python
# ============================================================================
# Sử dụng DaQMeasures từ Stream DaQ gốc - KHÔNG implement lại
# ============================================================================

from streamdaq import DaQMeasures as dqm

# Tất cả 50+ measures đều hoạt động:

# === Tuple-at-a-time checks ===
dqm.count(column)
dqm.null_count(column)
dqm.distinct_count(column)
dqm.distinct_count_approx(column)
dqm.most_frequent(column, n=top_n)

# === Range checks ===
dqm.range_conformance(column, min_val, max_val)
dqm.range_conformance_count(column, min_val, max_val)
dqm.range_conformance_fraction(column, min_val, max_val)

# === Set checks ===
dqm.set_conformance(column, accepted_values)
dqm.set_conformance_count(column, accepted_values)

# === Pattern checks ===
dqm.regex_conformance(column, pattern)

# === Ordering checks ===
dqm.satisfies_ordering(column, order='asc')

# === Window statistics ===
dqm.mean(column)
dqm.median(column)
dqm.std_dev(column)
dqm.percentiles(column, *percentile_values)

# === Freshness checks ===
dqm.freshness(column)

# === Frozen/Dead stream checks ===
dqm.is_frozen(column)

# === Distribution analysis ===
dqm.first_digit_frequencies(column)

# === Volume monitoring ===
dqm.min_length(column)
dqm.max_length(column)
dqm.mean_length(column)

# === Missing elements ===
dqm.missing_count(column)
dqm.missing_fraction(column)

# === Correlation ===
dqm.correlation(column1, column2, method='pearson')

# === Uniqueness ===
dqm.unique_count(column)
dqm.unique_fraction(column)

# ... và nhiều hơn nữa!
```

### 10.4 Windows - Import nguyên

```python
# ============================================================================
# Sử dụng Window classes từ Stream DaQ gốc - KHÔNG implement lại
# ============================================================================

from streamdaq import Window, TumblingWindow, SlidingWindow, SessionWindow

# Tất cả window types hoạt động:

# Tumbling windows
window = TumblingWindow(duration=timedelta(hours=1))

# Sliding windows
window = SlidingWindow(
    hop=timedelta(minutes=5),
    duration=timedelta(hours=1)
)

# Session windows
window = SessionWindow(
    predicate=lambda a, b: abs(a.timestamp - b.timestamp) < gap,
    max_gap=timedelta(minutes=30)
)
```

### 10.5 Migration Guide: Từ Stream DaQ sang WAVES

```python
# ============================================================================
# MIGRATION: StreamDaQ → WAVES
# ============================================================================

# === TRƯỚC (Stream DaQ gốc) ===
from streamdaq import StreamDaQ, DaQMeasures as dqm
from datetime import timedelta

daq = StreamDaQ()
daq.configure(
    window=TumblingWindow(duration=timedelta(hours=1)),
    source=csv_source("data.csv")
)

daq.add(dqm.null_count("destination"), "> 0.01")
daq.add(dqm.range("fare", min=0, max=500))

meta_stream = daq.watch()


# === SAU (WAVES) ===
from waves import WAVES, DaQMeasures as dqm
from datetime import timedelta

waves = WAVES()

# Cấu hình cơ bản - Y HỆT như StreamDaQ
waves.configure(
    window=TumblingWindow(duration=timedelta(hours=1)),
    source=csv_source("data.csv")
)

# Stream DaQ checks - Y HỆT như trước
waves.add_check("null_count", "destination", threshold=0.01)
waves.add_check("range", "fare", min=0, max=500)

# === WAVES NEW FEATURES ===

# 1. Watermark cho late data
waves.configure_watermark(
    watermark_interval=timedelta(minutes=1),
    max_lateness=timedelta(minutes=5)
)

# 2. DC Checks (Rapidash)
waves.add_dc(
    "dc_no_free_rides",
    "NOT (s.Distance > 0 AND s.Fare < 0)"
)
waves.add_dc(
    "dc_salary_order",
    "NOT (s.State = t.State AND s.Salary < t.Salary)"
)

# 3. Weever incremental indexing
waves.enable_wever(
    indexed_columns=["Distance", "Fare", "Salary"],
    enable_scheduling=True
)

# Meta stream - giờ có thêm DC violations!
meta_stream = waves.watch()

for result in meta_stream:
    # Stream DaQ data (như cũ)
    print(result.window_id)
    print(result.basic_check_results)
    
    # WAVES new data
    print(result.watermark_status)     # PROVISIONAL | FINAL
    print(result.dc_violations)        # DC violations
    print(result.is_provisional)       # True/False
```

---

## 11. So sánh StreamDaQ vs WAVES

| Tính năng | StreamDaQ | WAVES |
|-----------|-----------|-------|
| **Windowing** | ✅ Tumbling/Sliding/Session | ✅ Giữ nguyên |
| **Basic Checks (30+)** | ✅ 50+ built-in | ✅ Giữ nguyên |
| **Meta-Stream Output** | ✅ watch_out() | ✅ Giữ nguyên + mở rộng |
| **Schema Validation** | ✅ Pydantic | ✅ Giữ nguyên |
| **Anomaly Detection** | ✅ Bonus | ✅ Giữ nguyên |
| **DC Checking** | ❌ Không có | ✅ Mới: Orthogonal Range Search |
| **LT-Tree Indexing** | ❌ Không có | ✅ Mới: Incremental updates |
| **Watermark** | ⚠️ Partial | ✅ Mới: Full watermark handling |
| **Late Data Handling** | ❌ Không có | ✅ Mới: Retraction mechanism |
| **Incremental Updates** | ❌ Rebuild mỗi window | ✅ Mới: O(log N) per update |

---

## 12. Summary

### Điều cần nhớ

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUAN TRỌNG NHẤT                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MỤC TIÊU: KHÔNG LÀM MẤT BẢN CHẤT + GIỮ NHỮNG GÌ StreamDaQ LÀM ĐƯỢC  │
│                                                                             │
│  ✅ StreamDaQ vẫn hoạt động như một hệ thống độc lập                     │
│  ✅ Tất cả 50+ quality checks vẫn chạy đúng                                │
│  ✅ Meta-stream output vẫn xuất ra đúng format                            │
│  ✅ Windowing logic vẫn đúng                                               │
│                                                                             │
│  CÓ THỂ THAY ĐỔI:                                                         │
│  • Internal structure, data flow, state management                          │
│  • Cách tích hợp watermark vào pipeline                                   │
│  • Cách DC checker truy cập window state                                   │
│  • Implementation details                                                  │
│                                                                             │
│  KHÔNG ĐƯỢC THAY ĐỔI:                                                     │
│  • Public API contract                                                     │
│  • Output format (có thể mở rộng, không thay đổi)                         │
│  • Check behavior                                                         │
│  • Windowing semantics                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Hai cách tiếp cận

| Aspect | FACADE (An toàn) | INTEGRATED (Sâu) |
|--------|------------------|------------------|
| Sửa StreamDaQ | ❌ Không | ✅ Có |
| Thời gian | Nhanh (1-2 tuần) | Lâu (2-4 tuần) |
| Hiệu suất | Trung bình | Tốt hơn |
| Rủi ro | Thấp | Cao hơn |
| Dùng khi | Prototype, POC | Production |

### Khuyến nghị

```
GIAI ĐOẠN 1: Prototype → Dùng FACADE
GIAI ĐOẠN 2: Production → Chuyển INTEGRATED nếu cần hiệu suất
GIAI ĐOẠN 3: Optimize → Refactor từng phần dựa trên profiling
```

---

*Document Version: 2.0*
*Created: 2026-04-09*
*Updated: 2026-04-09*
*Specification for: WAVES Full (A) Implementation*
*Key Principle: "GIỮ BẢN CHẤT, CÓ THỂ REFACTOR - KHÔNG MẤT NHỮNG GÌ STREAQ DAQ LÀM ĐƯỢC"*
