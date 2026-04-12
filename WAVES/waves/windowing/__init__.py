"""Module 2.2 — Window Manager.

Public API:
- WindowConfig, WindowBuffer, Pane, WindowedEvent, WindowDelta
- PaneManager, WindowManager, WatermarkClock, WatermarkConfig
- floor_ts, to_ms
"""
from waves.windowing.pane import (
    WindowConfig,
    WindowBuffer,
    Pane,
    WindowedEvent,
    WindowDelta,
    PaneManager,
    floor_ts,
    to_ms,
)
from waves.windowing.manager import WindowManager
from waves.windowing.watermark import WatermarkClock, WatermarkConfig

__all__ = [
    # Data structures
    "WindowConfig",
    "WindowBuffer",
    "Pane",
    "WindowedEvent",
    "WindowDelta",
    # Managers
    "PaneManager",
    "WindowManager",
    "WatermarkClock",
    "WatermarkConfig",
    # Helpers
    "floor_ts",
    "to_ms",
]
