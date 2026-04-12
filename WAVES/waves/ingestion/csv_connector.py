"""CSV file source connector.

Module 2.1 — Stream Ingestion
"""
import csv
from typing import Iterable, List

from waves.ingestion.schema import DataEvent, Schema, parse_and_normalize


class CSVFileSource:
    """Read events from a CSV file."""

    def __init__(self, path: str, schema: Schema, encoding: str = "utf-8"):
        self.path = path
        self.schema = schema
        self.encoding = encoding

    def read(self) -> Iterable[DataEvent]:
        with open(self.path, newline="", encoding=self.encoding) as f:
            for row in csv.DictReader(f):
                ev = parse_and_normalize(row, self.schema)
                if ev is not None:
                    yield ev

    def read_batch(self) -> List[DataEvent]:
        return list(self.read())
