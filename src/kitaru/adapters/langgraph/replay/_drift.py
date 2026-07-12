from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from kitaru._replay_verify_imported_runner import FieldComparison

SEMANTIC_FIELDS = (
    "policy_label",
    "risk_status",
    "required_action",
    "tool_names",
    "evidence_ids",
)


def compare_decisions(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> list[FieldComparison]:
    comparisons: list[FieldComparison] = []
    for field_name in SEMANTIC_FIELDS:
        if field_name not in baseline and field_name not in candidate:
            continue
        b = baseline.get(field_name)
        c = candidate.get(field_name)
        comparisons.append(
            FieldComparison(
                field=field_name,
                baseline_value=b,
                comparison_value=c,
                matches=(b == c),
            )
        )
    return comparisons


@dataclass(frozen=True)
class DriftReport:
    reproduction: list[FieldComparison]
    fork: list[FieldComparison]

    @property
    def has_reproduction_drift(self) -> bool:
        return any(not c.matches for c in self.reproduction)

    @property
    def has_fork_drift(self) -> bool:
        return any(not c.matches for c in self.fork)

    def __str__(self) -> str:
        changed = [
            f"{c.field}: {c.baseline_value!r} -> {c.comparison_value!r}"
            for c in self.fork
            if not c.matches
        ]
        return "fork drift — " + "; ".join(changed) if changed else "no fork drift"
