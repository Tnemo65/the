"""Module 2.3 — Quality Meta Stream.

Aggregates DQ metrics per window and emits WindowMeta events.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class WindowMeta:
    window_id: str
    total_count: int = 0
    fail_counts: Dict[str, int] = field(default_factory=dict)  # rule_id -> count
    pass_rate: float = 1.0


@dataclass
class MetaEvent:
    """A quality meta event emitted to the output stream."""
    window_id: str
    timestamp: datetime
    total: int
    pass_count: int
    fail_count: int
    violation_counts: Dict[str, int] = field(default_factory=dict)


class QualityMetaStream:
    """
    Emits quality meta-stream events.

    Usage:
        stream = QualityMetaStream()
        stream.emit(window_meta)  # writes MetaEvent to output
    """

    def __init__(self, sink=None):
        self._sink = sink  # optional callable(event: MetaEvent)
        self._history: List[MetaEvent] = []

    def emit(self, meta: WindowMeta) -> MetaEvent:
        fail_count = sum(meta.fail_counts.values())
        pass_count = meta.total_count - fail_count
        event = MetaEvent(
            window_id=meta.window_id,
            timestamp=datetime.utcnow(),
            total=meta.total_count,
            pass_count=pass_count,
            fail_count=fail_count,
            violation_counts=dict(meta.fail_counts),
        )
        self._history.append(event)
        if self._sink:
            self._sink(event)
        return event

    def get_history(self) -> List[MetaEvent]:
        return list(self._history)

    def clear(self):
        self._history.clear()
