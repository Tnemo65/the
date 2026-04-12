"""WAVES: Window-based Adaptive Violation Detection with Elastic Streaming."""

__version__ = "0.1.0"

from waves.ingestion.schema import DataEvent, Schema
from waves.logical_engine import LogicalEngine, StatisticalState, ElasticBox, ElasticBoxConfig

__all__ = [
    "__version__",
    "DataEvent",
    "Schema",
    "LogicalEngine",
    "StatisticalState",
    "ElasticBox",
    "ElasticBoxConfig",
]
