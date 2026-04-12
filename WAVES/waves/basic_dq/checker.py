"""Module 2.3 — Basic DQ Checks.

Per-record data quality checks: null, type, range, regex.
Counters O(1) via Dict[rule_id, int].
"""
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any

from waves.basic_dq.meta_stream import WindowMeta


class DQResult(Enum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class CheckResult:
    rule_id: str
    result: DQResult
    event_id: str
    window_id: Optional[str]
    detail: Dict[str, Any] = field(default_factory=dict)



@dataclass
class BasicDQRule:
    rule_id: str
    rule_type: str   # "null", "type", "range", "regex"
    field: str
    params: Dict[str, Any] = field(default_factory=dict)


class BasicDQChecker:
    """
    Per-record DQ checker.

    check_event(event, rules) -> List[CheckResult]
    Updates counters in-place (O(1) per rule per event).
    """

    def __init__(self, rules: List[BasicDQRule]):
        self.rules = {r.rule_id: r for r in rules}
        # Counters: Dict[rule_id, fail_count] — O(1) access
        self._counters: Dict[str, int] = {r.rule_id: 0 for r in rules}
        self._total: Dict[str, int] = {r.rule_id: 0 for r in rules}

    def check_event(
        self,
        event: Any,
        rules: Optional[List[BasicDQRule]] = None,
    ) -> List[CheckResult]:
        """
        Check event against all rules (or provided rules).
        Updates internal counters in O(1) per rule.
        """
        if rules is None:
            rules = list(self.rules.values())

        results = []
        for rule in rules:
            result = self._check_rule(event, rule)
            results.append(result)
            self._total[rule.rule_id] = self._total.get(rule.rule_id, 0) + 1
            if result.result == DQResult.FAIL:
                self._counters[rule.rule_id] = self._counters.get(rule.rule_id, 0) + 1

        return results

    def _check_rule(self, event: Any, rule: BasicDQRule) -> CheckResult:
        value = event.attributes.get(rule.field) if hasattr(event, "attributes") else None

        if rule.rule_type == "null":
            if value is None:
                return CheckResult(
                    rule_id=rule.rule_id,
                    result=DQResult.FAIL,
                    event_id=getattr(event, "event_id", ""),
                    window_id=getattr(event, "window_id", None),
                    detail={"field": rule.field, "value": None},
                )
            return CheckResult(
                rule_id=rule.rule_id,
                result=DQResult.PASS,
                event_id=getattr(event, "event_id", ""),
                window_id=getattr(event, "window_id", None),
            )

        if rule.rule_type == "type":
            expected_type = rule.params.get("type")
            if expected_type is None:
                return CheckResult(
                    rule_id=rule.rule_id,
                    result=DQResult.SKIP,
                    event_id=getattr(event, "event_id", ""),
                    window_id=getattr(event, "window_id", None),
                    detail={"field": rule.field, "reason": "no type param"},
                )
            if value is not None and not isinstance(value, expected_type):
                return CheckResult(
                    rule_id=rule.rule_id,
                    result=DQResult.FAIL,
                    event_id=getattr(event, "event_id", ""),
                    window_id=getattr(event, "window_id", None),
                    detail={"field": rule.field, "value": value, "expected": expected_type.__name__},
                )
            return CheckResult(
                rule_id=rule.rule_id,
                result=DQResult.PASS,
                event_id=getattr(event, "event_id", ""),
                window_id=getattr(event, "window_id", None),
            )

        if rule.rule_type == "range":
            mn = rule.params.get("min")
            mx = rule.params.get("max")
            if value is not None:
                if (mn is not None and value < mn) or (mx is not None and value > mx):
                    return CheckResult(
                        rule_id=rule.rule_id,
                        result=DQResult.FAIL,
                        event_id=getattr(event, "event_id", ""),
                        window_id=getattr(event, "window_id", None),
                        detail={"field": rule.field, "value": value, "min": mn, "max": mx},
                    )
            return CheckResult(
                rule_id=rule.rule_id,
                result=DQResult.PASS,
                event_id=getattr(event, "event_id", ""),
                window_id=getattr(event, "window_id", None),
            )

        if rule.rule_type == "regex":
            pattern = rule.params.get("pattern")
            if pattern is None:
                return CheckResult(
                    rule_id=rule.rule_id,
                    result=DQResult.SKIP,
                    event_id=getattr(event, "event_id", ""),
                    window_id=getattr(event, "window_id", None),
                    detail={"field": rule.field, "reason": "no pattern param"},
                )
            if value is not None and not re.match(pattern, str(value)):
                return CheckResult(
                    rule_id=rule.rule_id,
                    result=DQResult.FAIL,
                    event_id=getattr(event, "event_id", ""),
                    window_id=getattr(event, "window_id", None),
                    detail={"field": rule.field, "value": value, "pattern": pattern},
                )
            return CheckResult(
                rule_id=rule.rule_id,
                result=DQResult.PASS,
                event_id=getattr(event, "event_id", ""),
                window_id=getattr(event, "window_id", None),
            )

        return CheckResult(
            rule_id=rule.rule_id,
            result=DQResult.SKIP,
            event_id=getattr(event, "event_id", ""),
            window_id=getattr(event, "window_id", None),
            detail={"field": rule.field, "reason": f"unknown rule_type: {rule.rule_type}"},
        )

    def get_counters(self) -> Dict[str, int]:
        return dict(self._counters)

    def get_totals(self) -> Dict[str, int]:
        return dict(self._total)

    def build_window_meta(self, window_id: str) -> WindowMeta:
        total = sum(self._total.values())
        fail_sum = sum(self._counters.values())
        pass_rate = 1.0 - fail_sum / max(total, 1)
        return WindowMeta(
            window_id=window_id,
            total_count=total,
            fail_counts=dict(self._counters),
            pass_rate=pass_rate,
        )
