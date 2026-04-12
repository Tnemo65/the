"""Schema definition và DataEvent dataclass.

Module 2.1 — Stream Ingestion
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import uuid


class ParseError(ValueError):
    """Raised when a raw event cannot be parsed into a DataEvent."""
    pass


@dataclass
class DataEvent:
    """Standardized event sau khi ingestion."""
    event_id: str
    event_time: datetime
    ingestion_time: datetime
    window_id: Optional[str] = None
    pane_id: Optional[str] = None
    partition_key: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Schema:
    """Schema định nghĩa cách parse raw event."""
    event_time_field: str
    event_time_format: str = "%Y-%m-%d %H:%M:%S"
    ingestion_time_field: Optional[str] = None
    ingestion_time_format: Optional[str] = None
    partition_key_field: Optional[str] = None
    field_types: Dict[str, type] = field(default_factory=dict)
    required_fields: List[str] = field(default_factory=list)


def parse_datetime(value: str, fmt: Optional[str]) -> datetime:
    """Parse datetime string, always return UTC-aware datetime."""
    if fmt is not None:
        dt = datetime.strptime(value, fmt)
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_empty(val: Any) -> bool:
    """Check if value is None or whitespace-only string."""
    if val is None:
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


def parse_and_normalize(raw_event: Dict[str, Any], schema: Schema) -> DataEvent:
    """Parse raw event dict into DataEvent.

    Raises:
        ParseError: if required field missing or type cast fails.
    """
    # 1. Parse event_time (required)
    event_time_str = raw_event.get(schema.event_time_field)
    if event_time_str is None:
        raise ParseError(f"missing required field: {schema.event_time_field}")
    try:
        event_time = parse_datetime(str(event_time_str), schema.event_time_format)
    except (ValueError, TypeError) as e:
        raise ParseError(f"cannot parse event_time '{event_time_str}': {e}")

    # 2. Parse ingestion_time
    if schema.ingestion_time_field is not None:
        ing_str = raw_event.get(schema.ingestion_time_field)
        if ing_str is None:
            raise ParseError(f"missing required field: {schema.ingestion_time_field}")
        try:
            ingestion_time = parse_datetime(str(ing_str), schema.ingestion_time_format)
        except (ValueError, TypeError) as e:
            raise ParseError(f"cannot parse ingestion_time '{ing_str}': {e}")
    else:
        ingestion_time = datetime.now(timezone.utc)

    # 3. Check required fields
    for field_name in schema.required_fields:
        val = raw_event.get(field_name)
        if _is_empty(val):
            raise ParseError(f"missing required field: {field_name}")

    # 4. Cast typed fields (skip empty/None)
    attributes: Dict[str, Any] = {}
    for field_name, expected_type in schema.field_types.items():
        val = raw_event.get(field_name)
        if _is_empty(val):
            continue
        try:
            attributes[field_name] = expected_type(val)
        except (ValueError, TypeError) as e:
            raise ParseError(f"cannot cast field '{field_name}'={val!r} to {expected_type.__name__}: {e}")

    # 5. Partition key
    partition_key: Optional[str] = None
    if schema.partition_key_field is not None:
        pk = raw_event.get(schema.partition_key_field)
        if pk is not None and not _is_empty(pk):
            partition_key = str(pk)

    return DataEvent(
        event_id=str(uuid.uuid4()),
        event_time=event_time,
        ingestion_time=ingestion_time,
        partition_key=partition_key,
        attributes=attributes,
    )
