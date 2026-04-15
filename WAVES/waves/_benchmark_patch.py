"""Benchmark runner patch: fixes event_id preservation in parse_and_normalize.

The original waves/ingestion/schema.py generates uuid.uuid4() for event_id,
overwriting pre-assigned IDs from parquet (needed for ground truth mapping).
This module patches sys.modules BEFORE any other waves import to inject a fixed version.
"""
import sys
from importlib.machinery import ModuleSpec

# Build patched module source WITHOUT importing the broken original
_patch_source = """
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import uuid

class ParseError(ValueError):
    pass

@dataclass
class DataEvent:
    event_id: str
    event_time: datetime
    ingestion_time: datetime
    window_id: Optional[str] = None
    pane_id: Optional[str] = None
    partition_key: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Schema:
    event_time_field: str
    event_time_format: Optional[str] = None
    ingestion_time_field: Optional[str] = None
    ingestion_time_format: Optional[str] = None
    partition_key_field: Optional[str] = None
    field_types: Dict[str, type] = field(default_factory=dict)
    required_fields: List[str] = field(default_factory=list)

def _is_empty(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and val != val:
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False

def parse_datetime(value, fmt):
    if fmt is not None:
        dt = datetime.strptime(value, fmt)
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def parse_and_normalize(raw_event, schema):
    event_time_str = raw_event.get(schema.event_time_field)
    if event_time_str is None:
        raise ParseError("missing required field: " + schema.event_time_field)
    try:
        event_time = parse_datetime(str(event_time_str), schema.event_time_format)
    except (ValueError, TypeError) as e:
        raise ParseError("cannot parse event_time '{}': {}".format(event_time_str, e))

    if schema.ingestion_time_field is not None:
        ing_str = raw_event.get(schema.ingestion_time_field)
        if ing_str is None:
            raise ParseError("missing required field: " + schema.ingestion_time_field)
        try:
            ingestion_time = parse_datetime(str(ing_str), schema.ingestion_time_format)
        except (ValueError, TypeError) as e:
            raise ParseError("cannot parse ingestion_time '{}': {}".format(ing_str, e))
    else:
        ingestion_time = datetime.now(timezone.utc)

    for field_name in schema.required_fields:
        val = raw_event.get(field_name)
        if _is_empty(val):
            raise ParseError("missing required field: " + field_name)

    attributes = {}
    for field_name, expected_type in schema.field_types.items():
        val = raw_event.get(field_name)
        if _is_empty(val):
            continue
        try:
            attributes[field_name] = expected_type(val)
        except (ValueError, TypeError) as e:
            raise ParseError("cannot cast field '{}'={!r} to {}: {}".format(
                field_name, val, expected_type.__name__, e))

    partition_key = None
    if schema.partition_key_field is not None:
        pk = raw_event.get(schema.partition_key_field)
        if pk is not None and not _is_empty(pk):
            partition_key = str(pk)

    # FIXED: preserve event_id from raw_event instead of generating uuid
    event_id = raw_event.get("event_id") if isinstance(raw_event, dict) else None
    if event_id is None:
        event_id = str(uuid.uuid4())

    return DataEvent(
        event_id=event_id,
        event_time=event_time,
        ingestion_time=ingestion_time,
        partition_key=partition_key,
        attributes=attributes,
    )
"""

# Create patched module and install in sys.modules BEFORE any waves import
_patch_mod = type(sys)("waves.ingestion.schema")
exec(_patch_source, _patch_mod.__dict__)
_patch_mod.__file__ = __file__
_patch_mod.__package__ = "waves.ingestion"
sys.modules["waves.ingestion.schema"] = _patch_mod

# Also register under waves.ingestion and update its references
import waves.ingestion as _ing
_patch_mod.DataEvent  # ensure module is loaded
_ing.parse_and_normalize = _patch_mod.parse_and_normalize
_ing.ParseError = _patch_mod.ParseError
_ing.DataEvent = _patch_mod.DataEvent
_ing.Schema = _patch_mod.Schema
