"""Module 2.1 — Stream Ingestion (CSV-only).

Public API:
- DataEvent, Schema, ParseError
- BaseConnector, ConnectorFactory
- parse_and_normalize, parse_datetime
"""
from waves.ingestion.schema import (
    DataEvent,
    Schema,
    ParseError,
    parse_and_normalize,
    parse_datetime,
)
from waves.ingestion.connector import BaseConnector, ConnectorFactory

__all__ = [
    "DataEvent",
    "Schema",
    "ParseError",
    "BaseConnector",
    "ConnectorFactory",
    "parse_and_normalize",
    "parse_datetime",
]
