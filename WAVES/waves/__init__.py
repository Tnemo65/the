"""
WAVES: Watermark-Aware Violation Detection for Streaming

Extension of StreamDaQ with:
- DC Checking (from Rapidash)
- Watermark Layer
- Weever Integration (LT-Tree)
"""

# Import từ Stream DaQ gốc (khi đã tích hợp)
# TODO: Import sau khi tích hợp
# from streamdaq import StreamDaQ as _StreamDaQBase
# from streamdaq import DaQMeasures as _DaQMeasures
# from streamdaq import Windows

# Import components mới của WAVES
from .watermark import WatermarkHandler, WatermarkStatus, WatermarkEvent
from .dc_checker import DenialConstraint, DCChecker, DCViolation
from .wever import LTTree, PredicateScheduler

__version__ = "0.1.0"

__all__ = [
    # Stream DaQ (sau khi tích hợp)
    # "StreamDaQ",
    # "DaQMeasures",
    # "Windows",
    
    # WAVES components
    "WAVES",
    "WAVESConfig",
    "WatermarkHandler",
    "WatermarkStatus",
    "WatermarkEvent",
    "DenialConstraint",
    "DCChecker",
    "DCViolation",
    "LTTree",
    "PredicateScheduler",
]


class WAVES:
    """
    WAVES: Watermark-Aware Violation Detection for Streaming.
    
    Extension of StreamDaQ với:
    - DC Checking (từ Rapidash)
    - Watermark Layer
    - Weever Integration
    """
    
    def __init__(self):
        # TODO: Implement sau
        # Gọi parent StreamDaQ
        # super().__init__()
        
        # WAVES components
        self._watermark = None
        self._dc_checker = None
        self._wever_scheduler = None
        self._dcs = []
        
        raise NotImplementedError(
            "WAVES đang trong quá trình phát triển. "
            "Xem docs/WAVES_FULL_Architecture.md để biết chi tiết."
        )


class WAVESConfig:
    """Configuration cho WAVES."""
    
    def __init__(
        self,
        watermark_enabled: bool = True,
        dc_enabled: bool = True,
        wever_enabled: bool = True,
    ):
        self.watermark_enabled = watermark_enabled
        self.dc_enabled = dc_enabled
        self.wever_enabled = wever_enabled
