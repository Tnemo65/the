"""Unit tests cho Stream Ingestion (Module 2.1)."""
import csv
import json
import tempfile
import os
from datetime import datetime, timezone

import pytest

from waves.ingestion.schema import (
    DataEvent,
    Schema,
    ParseError,
    parse_and_normalize,
    parse_datetime,
)
from waves.ingestion.connector import ConnectorFactory
from waves.ingestion.csv_connector import CSVFileSource


# ─── NYC Taxi Schema fixture ────────────────────────────────────────────────

@pytest.fixture
def nyc_schema():
    return Schema(
        event_time_field="tpep_pickup_datetime",
        event_time_format="%Y-%m-%d %H:%M:%S",
        partition_key_field="PULocationID",
        field_types={
            "passenger_count": int,
            "trip_distance": float,
            "fare_amount": float,
            "tolls_amount": float,
            "trip_duration": float,
        },
        required_fields=["tpep_pickup_datetime", "trip_distance", "fare_amount"],
    )


@pytest.fixture
def raw_nyc_row():
    return {
        "tpep_pickup_datetime": "2024-01-15 10:30:00",
        "tpep_dropoff_datetime": "2024-01-15 10:50:00",
        "passenger_count": "2",
        "trip_distance": "3.5",
        "fare_amount": "12.50",
        "tolls_amount": "2.50",
        "trip_duration": "1200.0",
        "PULocationID": "132",
    }


# ─── parse_datetime ─────────────────────────────────────────────────────────

class TestParseDatetime:
    def test_strptime_with_format(self):
        dt = parse_datetime("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30
        assert dt.second == 0
        assert dt.tzinfo == timezone.utc

    def test_strptime_without_tz_always_utc(self):
        dt = parse_datetime("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S")
        assert dt.tzinfo == timezone.utc

    def test_isoformat_no_format(self):
        dt = parse_datetime("2024-01-15T10:30:00+00:00", None)
        assert dt.tzinfo is not None

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_datetime("not-a-date", "%Y-%m-%d")


# ─── parse_and_normalize ────────────────────────────────────────────────────

class TestParseAndNormalize:
    def test_happy_path_creates_correct_event(self, nyc_schema, raw_nyc_row):
        ev = parse_and_normalize(raw_nyc_row, nyc_schema)

        assert ev.event_id is not None
        assert len(ev.event_id) == 36  # UUID4
        assert ev.event_time.year == 2024
        assert ev.event_time.month == 1
        assert ev.event_time.day == 15
        assert ev.ingestion_time.tzinfo == timezone.utc
        assert ev.partition_key == "132"
        assert ev.attributes["trip_distance"] == 3.5
        assert ev.attributes["fare_amount"] == 12.50
        assert ev.attributes["passenger_count"] == 2
        assert ev.window_id is None
        assert ev.pane_id is None

    def test_missing_required_field_raises_parse_error(self, nyc_schema):
        row = {"tpep_pickup_datetime": "2024-01-15 10:30:00"}
        with pytest.raises(ParseError) as exc:
            parse_and_normalize(row, nyc_schema)
        assert "trip_distance" in str(exc.value)

    def test_missing_event_time_field_raises(self, nyc_schema):
        row = {"trip_distance": "3.5"}
        with pytest.raises(ParseError) as exc:
            parse_and_normalize(row, nyc_schema)
        assert "tpep_pickup_datetime" in str(exc.value)

    def test_invalid_datetime_format_raises(self, nyc_schema):
        row = {"tpep_pickup_datetime": "not-a-date", "trip_distance": "3.5", "fare_amount": "10"}
        with pytest.raises(ParseError) as exc:
            parse_and_normalize(row, nyc_schema)
        assert "cannot parse event_time" in str(exc.value)

    def test_cast_failure_raises_parse_error(self, nyc_schema):
        row = {
            "tpep_pickup_datetime": "2024-01-15 10:30:00",
            "trip_distance": "not-a-float",
            "fare_amount": "10",
        }
        with pytest.raises(ParseError) as exc:
            parse_and_normalize(row, nyc_schema)
        assert "cannot cast field 'trip_distance'" in str(exc.value)

    def test_optional_field_types_are_skipped_when_null(self, nyc_schema):
        row = {
            "tpep_pickup_datetime": "2024-01-15 10:30:00",
            "trip_distance": "3.5",
            "fare_amount": "12.50",
            "passenger_count": "",  # empty string → skip
            "tolls_amount": "",     # empty string → skip
        }
        ev = parse_and_normalize(row, nyc_schema)
        assert "passenger_count" not in ev.attributes
        assert "tolls_amount" not in ev.attributes

    def test_empty_string_required_field_raises(self, nyc_schema):
        row = {
            "tpep_pickup_datetime": "2024-01-15 10:30:00",
            "trip_distance": "   ",  # whitespace-only
            "fare_amount": "10",
        }
        with pytest.raises(ParseError):
            parse_and_normalize(row, nyc_schema)


# ─── CSV Connector ───────────────────────────────────────────────────────────

class TestCSVFileSource:
    def test_read_batch_happy_path(self, nyc_schema, raw_nyc_row):
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=raw_nyc_row.keys())
            writer.writeheader()
            writer.writerow(raw_nyc_row)
            f.flush()
            path = f.name

        try:
            conn = CSVFileSource(path, nyc_schema)
            events = conn.read_batch()
            assert len(events) == 1
            ev = events[0]
            assert ev.attributes["trip_distance"] == 3.5
            assert ev.partition_key == "132"
        finally:
            os.unlink(path)

    def test_read_generator(self, nyc_schema, raw_nyc_row):
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=raw_nyc_row.keys())
            writer.writeheader()
            for _ in range(3):
                writer.writerow(raw_nyc_row)
            f.flush()
            path = f.name

        try:
            conn = CSVFileSource(path, nyc_schema)
            count = sum(1 for _ in conn.read())
            assert count == 3
        finally:
            os.unlink(path)

    def test_parse_error_raises(self, nyc_schema):
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=["tpep_pickup_datetime"])
            writer.writeheader()
            writer.writerow({"tpep_pickup_datetime": "bad-date"})
            f.flush()
            path = f.name

        try:
            conn = CSVFileSource(path, nyc_schema)
            with pytest.raises(ParseError):
                conn.read_batch()
        finally:
            os.unlink(path)


# ─── Connector Factory ───────────────────────────────────────────────────────

class TestConnectorFactory:
    def test_csv_from_path(self, nyc_schema, raw_nyc_row):
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=raw_nyc_row.keys())
            writer.writeheader()
            writer.writerow(raw_nyc_row)
            f.flush()
            path = f.name

        try:
            conn = ConnectorFactory.create(path, nyc_schema)
            assert isinstance(conn, CSVFileSource)
            events = conn.read_batch()
            assert len(events) == 1
        finally:
            os.unlink(path)
