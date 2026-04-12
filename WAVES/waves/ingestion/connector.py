"""Base connector interface và factory.

Module 2.1 — Stream Ingestion (CSV-only: NYC Taxi dataset)
"""
from abc import ABC, abstractmethod
from typing import Iterable, List

from waves.ingestion.schema import DataEvent, Schema


class BaseConnector(ABC):
    """Abstract base cho stream connectors."""

    def __init__(self, schema: Schema):
        self.schema = schema

    @abstractmethod
    def read(self) -> Iterable[DataEvent]:
        """Read events one by one (generator)."""
        raise NotImplementedError

    @abstractmethod
    def read_batch(self) -> List[DataEvent]:
        """Read all events as a list."""
        raise NotImplementedError


class ConnectorFactory:
    """Factory tạo connector từ file path.

    Hiện chỉ hỗ trợ CSV — phù hợp với NYC Taxi dataset.
    """

    @classmethod
    def create(cls, path: str, schema: Schema, **kwargs) -> BaseConnector:
        """Create CSV connector from file path.

        Args:
            path: CSV file path
            schema: Schema for parsing
            **kwargs: Additional args passed to CSVFileSource (e.g. encoding)
        """
        from waves.ingestion.csv_connector import CSVFileSource
        return CSVFileSource(path, schema, **kwargs)
