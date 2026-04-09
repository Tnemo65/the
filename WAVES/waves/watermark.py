"""
Watermark Handler - Xử lý late arrivals và retraction.

Port từ watermark concepts trong:
- Pathway temporal behaviors
- Google Dataflow watermark semantics
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import threading
import time


class WatermarkStatus(Enum):
    """Trạng thái của kết quả liên quan đến watermark."""
    PROVISIONAL = "provisional"  # Chưa final, có thể retract
    FINAL = "final"              # Đã final, không retract được
    RETRACTED = "retracted"      # Đã bị retract


class TupleAction(Enum):
    """Action cần thực hiện với tuple."""
    PROCESS = "process"          # Xử lý bình thường
    BUFFER_LATE = "buffer_late"  # Buffer để retract sau
    DISCARD = "discard"          # Bỏ qua hoàn toàn


@dataclass
class WatermarkEvent:
    """Sự kiện watermark."""
    timestamp: float
    event_time: datetime
    is_aligned: bool
    source_id: str


@dataclass
class LateTuple:
    """Tuple đến muộn."""
    tuple_data: Any
    window_id: str
    arrival_time: float
    original_timestamp: float


@dataclass
class TupleProcessResult:
    """Kết quả xử lý tuple."""
    action: TupleAction
    is_late: bool
    tuple_data: Any
    window_id: str
    late_tuple: Optional[LateTuple] = None


@dataclass
class RetractionEvent:
    """Sự kiện retraction."""
    window_id: str
    old_results: Any
    new_results: Any
    timestamp: float


class WatermarkHandler:
    """
    Xử lý watermark cho WAVES.
    
    RESPONSIBILITIES:
    1. Theo dõi watermark progress
    2. Buffer late tuples cho potential retraction
    3. Emit provisional → final transitions
    4. Quản lý watermark alignment
    """
    
    def __init__(
        self,
        watermark_interval: timedelta = timedelta(minutes=1),
        max_lateness: timedelta = timedelta(minutes=5),
        alignment_mode: str = "event_time"
    ):
        self.watermark_interval = watermark_interval
        self.max_lateness = max_lateness
        self.alignment_mode = alignment_mode
        
        # Watermark state
        self._current_watermark: float = 0.0
        self._watermark_sources: Dict[str, float] = {}
        self._lock = threading.Lock()
        
        # Late tuple buffer: window_id -> List[LateTuple]
        self._late_buffers: Dict[str, List[LateTuple]] = {}
        
        # Provisional results: window_id -> result_data
        self._provisional_results: Dict[str, Any] = {}
        
        # Callbacks
        self._on_watermark_advance: Optional[Callable] = None
        self._on_late_tuple: Optional[Callable] = None
        self._on_retraction: Optional[Callable] = None
    
    def register_source(self, source_id: str) -> None:
        """Đăng ký một nguồn dữ liệu."""
        with self._lock:
            self._watermark_sources[source_id] = 0.0
    
    def update_watermark(self, source_id: str, watermark: float) -> Optional[WatermarkEvent]:
        """
        Cập nhật watermark cho một nguồn.
        
        Returns:
            WatermarkEvent nếu watermark advance
        """
        with self._lock:
            old_watermark = self._watermark_sources.get(source_id, 0.0)
            
            if watermark <= old_watermark:
                return None
            
            self._watermark_sources[source_id] = watermark
            
            # Global watermark = min của tất cả sources
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
        Xử lý tuple, quyết định có late hay không.
        """
        with self._lock:
            max_lateness_sec = self.max_lateness.total_seconds()
            
            # Tuple quá muộn
            if tuple_timestamp <= self._current_watermark - max_lateness_sec:
                return TupleProcessResult(
                    action=TupleAction.DISCARD,
                    is_late=True,
                    tuple_data=tuple_data,
                    window_id=window_id
                )
            
            # Tuple late nhưng còn trong ngưỡng
            elif tuple_timestamp < self._current_watermark:
                late_tuple = LateTuple(
                    tuple_data=tuple_data,
                    window_id=window_id,
                    arrival_time=time.time(),
                    original_timestamp=tuple_timestamp
                )
                
                if window_id not in self._late_buffers:
                    self._late_buffers[window_id] = []
                self._late_buffers[window_id].append(late_tuple)
                
                return TupleProcessResult(
                    action=TupleAction.BUFFER_LATE,
                    is_late=True,
                    tuple_data=tuple_data,
                    window_id=window_id,
                    late_tuple=late_tuple
                )
            
            # Tuple đúng giờ
            else:
                return TupleProcessResult(
                    action=TupleAction.PROCESS,
                    is_late=False,
                    tuple_data=tuple_data,
                    window_id=window_id
                )
    
    def retract_window(self, window_id: str, new_results: Any) -> Optional[RetractionEvent]:
        """
        Retract một provisional window khi late tuple đến.
        """
        with self._lock:
            old_results = self._provisional_results.pop(window_id, None)
            
            if old_results is None:
                return None
            
            self._provisional_results[window_id] = new_results
            
            return RetractionEvent(
                window_id=window_id,
                old_results=old_results,
                new_results=new_results,
                timestamp=time.time()
            )
    
    def _emit_watermark_advance(self, old_wm: float, new_wm: float) -> None:
        """Emit watermark advance event và finalize provisional windows."""
        finalized_windows = []
        
        for window_id in list(self._provisional_results.keys()):
            window_start = self._extract_window_start(window_id)
            if window_start <= new_wm:
                finalized_windows.append(window_id)
        
        for window_id in finalized_windows:
            del self._provisional_results[window_id]
        
        if self._on_watermark_advance:
            self._on_watermark_advance(old_wm, new_wm, finalized_windows)
    
    def _extract_window_start(self, window_id: str) -> float:
        """Trích xuất window start timestamp từ window_id."""
        parts = window_id.split("_")
        return float(parts[1])
    
    @property
    def current_watermark(self) -> float:
        """Lấy watermark hiện tại."""
        with self._lock:
            return self._current_watermark
