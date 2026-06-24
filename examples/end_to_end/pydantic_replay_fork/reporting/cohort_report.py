"""Cohort experiment report model for the PydanticAI support-copilot demo."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

from utils.metrics import MetricDelta, ReplayRun


@dataclasses.dataclass
class CohortRow:
    """One cohort case — original production run vs variant replay."""

    base_exec_id: str
    original_decision: dict | None = None
    original_run: ReplayRun | None = None
    variant_run: ReplayRun | None = None
    deltas: list[MetricDelta] = dataclasses.field(default_factory=list)
    decision_changed: bool | None = None
    compare_urls: list[str] = dataclasses.field(default_factory=list)
    skipped: bool = False
    skip_reason: str | None = None

    @property
    def variant_exec_id(self) -> str | None:
        if self.variant_run is None:
            return None
        return self.variant_run.exec_id


class Report:
    """Aggregate result of a cohort experiment."""

    QUALITY_TOLERANCE: float = 0.1

    def __init__(self, rows: list[CohortRow], skipped_count: int) -> None:
        self._all_rows = rows
        self.rows = [row for row in rows if not row.skipped]
        self.skipped = skipped_count

    @property
    def decision_change_count(self) -> int:
        """Rows where the variant replay changed the original decision."""
        return sum(1 for row in self.rows if row.decision_changed is True)

    def _mean_baseline(self, metric_name: str) -> float | None:
        vals = [
            delta.baseline_value
            for row in self.rows
            for delta in row.deltas
            if delta.name == metric_name and delta.baseline_value is not None
        ]
        return sum(vals) / len(vals) if vals else None

    def _mean_variant(self, metric_name: str) -> float | None:
        vals = [
            delta.variant_value
            for row in self.rows
            for delta in row.deltas
            if delta.name == metric_name and delta.variant_value is not None
        ]
        return sum(vals) / len(vals) if vals else None

    def _metric_names(self) -> list[str]:
        seen: list[str] = []
        for row in self.rows:
            for delta in row.deltas:
                if delta.name not in seen:
                    seen.append(delta.name)
        return seen

    def _lower_is_better(self, metric_name: str) -> bool:
        for row in self.rows:
            for delta in row.deltas:
                if delta.name == metric_name:
                    return delta.lower_is_better
        return True

    @property
    def improvement(self) -> bool:
        """True iff no metric regressed across the cohort."""
        for name in self._metric_names():
            baseline = self._mean_baseline(name)
            variant = self._mean_variant(name)
            if baseline is None or variant is None:
                continue
            if self._lower_is_better(name):
                if variant > baseline:
                    return False
            elif variant < baseline - self.QUALITY_TOLERANCE:
                return False
        return True

    def metric_aggregates(self) -> list[MetricDelta]:
        """Per-metric aggregate: mean original vs mean variant."""
        return [
            MetricDelta(
                name=name,
                baseline_value=self._mean_baseline(name),
                variant_value=self._mean_variant(name),
                lower_is_better=self._lower_is_better(name),
            )
            for name in self._metric_names()
        ]

    def per_case(self) -> list[dict[str, Any]]:
        """Each non-skipped case as a plain dict, for reporting."""
        return [
            {
                "exec_id": row.base_exec_id,
                "variant_exec_id": row.variant_exec_id,
                "decision_changed": bool(row.decision_changed),
                "compare_urls": list(row.compare_urls),
                "metrics": {
                    delta.name: {
                        "baseline": delta.baseline_value,
                        "variant": delta.variant_value,
                        "lower_is_better": delta.lower_is_better,
                        "worse": delta.is_worse,
                    }
                    for delta in row.deltas
                },
            }
            for row in self.rows
        ]

    def skipped_cases(self) -> list[dict[str, str]]:
        """Each skipped case as ``{exec_id, reason}``."""
        return [
            {"exec_id": row.base_exec_id, "reason": row.skip_reason or "skipped"}
            for row in self._all_rows
            if row.skipped
        ]

    def regressions(self) -> list:
        """Metric aggregates that regressed, plus decision drift labels."""
        result = []
        for aggregate in self.metric_aggregates():
            if aggregate.is_worse:
                result.append(aggregate)
        if self.decision_change_count > 0:
            result.append("decision_changed")
        return result

    def to_dict(self, *, cohort: dict[str, Any] | None = None) -> dict[str, Any]:
        """Serialize the report for MCP / Claude Code consumption."""
        payload: dict[str, Any] = {
            "summary": {
                "cases": len(self.rows),
                "skipped": self.skipped,
                "decision_change_count": self.decision_change_count,
                "improvement": self.improvement,
                "metric_aggregates": [
                    {
                        "name": item.name,
                        "baseline": item.baseline_value,
                        "variant": item.variant_value,
                        "lower_is_better": item.lower_is_better,
                        "worse": item.is_worse,
                    }
                    for item in self.metric_aggregates()
                ],
            },
            "cases": self.per_case(),
            "skipped_cases": self.skipped_cases(),
        }
        if cohort is not None:
            payload["cohort"] = cohort
        return payload

    def to_json(
        self,
        path: str | Path,
        *,
        cohort: dict[str, Any] | None = None,
    ) -> str:
        """Write the JSON report to *path* and return the resolved path."""
        resolved = Path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(
            json.dumps(self.to_dict(cohort=cohort), indent=2),
            encoding="utf-8",
        )
        return str(resolved.resolve())

    def summary(self) -> str:
        """Return and print a human-readable summary of the experiment."""
        lines = [
            f"cohort experiment — {len(self.rows)} runs",
            f"  rows: {len(self.rows)} | skipped: {self.skipped}"
            f" | original→variant decision drift: {self.decision_change_count}",
        ]
        for name in self._metric_names():
            baseline = self._mean_baseline(name)
            variant = self._mean_variant(name)
            direction = "↓ better" if self._lower_is_better(name) else "↑ better"
            lines.append(
                f"  {name:<12} original={baseline}  variant={variant}  ({direction})"
            )
        lines.append(f"  improvement: {self.improvement}")
        out = "\n".join(lines)
        print(out)
        return out

    def __str__(self) -> str:
        return self.summary()
