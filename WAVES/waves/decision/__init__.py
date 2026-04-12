"""Module 2.8 — Decision Layer: Watermark-aware alert decisions."""

from waves.decision.alert_store import AlertStateStore, AlertStatus, AlertRecord
from waves.decision.decision import (
    AlertDecision,
    ProvisionalDecision,
    FinalDecision,
    RetractionDecision,
    process_candidate,
    finalize_window,
    retract_alert,
    cleanup_expired,
)

__all__ = [
    # Store
    "AlertStateStore",
    "AlertStatus",
    "AlertRecord",
    # Decision classes
    "AlertDecision",
    "ProvisionalDecision",
    "FinalDecision",
    "RetractionDecision",
    # Functions
    "process_candidate",
    "finalize_window",
    "retract_alert",
    "cleanup_expired",
]
