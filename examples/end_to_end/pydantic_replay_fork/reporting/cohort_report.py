"""Cohort experiment report model for the PydanticAI support-copilot demo.

``Report`` aggregates per-case rows produced by ``utils.cohort.run_cohort``.
HTML rendering lives in ``reporting.cohort_html``; replay orchestration in
``utils.cohort``.
"""

from __future__ import annotations

import dataclasses

from utils.metrics import MetricDelta, ReplayRun


@dataclasses.dataclass
class CohortRow:
    """One cohort case — original, reproduce replay, variant replay, metrics."""

    base_exec_id: str
    original_decision: dict | None = None
    baseline_run: ReplayRun | None = None
    variant_run: ReplayRun | None = None
    deltas: list[MetricDelta] = dataclasses.field(default_factory=list)
    reproduction_changed: bool | None = None
    decision_changed: bool | None = None
    skipped: bool = False
    skip_reason: str | None = None


class Report:
    """Aggregate result of a cohort experiment."""

    QUALITY_TOLERANCE: float = 0.1

    def __init__(self, rows: list[CohortRow], skipped_count: int) -> None:
        self._all_rows = rows
        self.rows = [row for row in rows if not row.skipped]
        self.skipped = skipped_count

    @property
    def decision_change_count(self) -> int:
        """Rows where the edited replay changed the unchanged replay decision."""
        return sum(1 for row in self.rows if row.decision_changed is True)

    @property
    def reproduction_drift_count(self) -> int:
        """Rows where the unchanged replay did not match the original decision."""
        return sum(1 for row in self.rows if row.reproduction_changed is True)

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
        """Per-metric aggregate: mean unchanged-replay vs mean edited-replay."""
        return [
            MetricDelta(
                name=name,
                baseline_value=self._mean_baseline(name),
                variant_value=self._mean_variant(name),
                lower_is_better=self._lower_is_better(name),
            )
            for name in self._metric_names()
        ]

    def per_case(self) -> list[dict]:
        """Each non-skipped case as a plain dict, for reporting."""
        return [
            {
                "exec_id": row.base_exec_id,
                "reproduction_faithful": row.reproduction_changed is False,
                "decision_changed": bool(row.decision_changed),
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

    def skipped_cases(self) -> list[dict]:
        """Each skipped case as ``{exec_id, reason}``."""
        return [
            {"exec_id": row.base_exec_id, "reason": row.skip_reason or "skipped"}
            for row in self._all_rows
            if row.skipped
        ]

    def regressions(self) -> list:
        """Metric aggregates that regressed, plus drift labels when present."""
        result = []
        for aggregate in self.metric_aggregates():
            if aggregate.is_worse:
                result.append(aggregate)
        if self.reproduction_drift_count > 0:
            result.append("reproduction_drift")
        if self.decision_change_count > 0:
            result.append("edited_decision_changed")
        return result

    def summary(self) -> str:
        """Return and print a human-readable summary of the experiment."""
        lines = [
            f"cohort experiment — {len(self.rows)} runs",
            f"  rows: {len(self.rows)} | skipped: {self.skipped}"
            f" | original→reproduction drift: {self.reproduction_drift_count}"
            f" | reproduction→edited drift: {self.decision_change_count}",
        ]
        for name in self._metric_names():
            baseline = self._mean_baseline(name)
            variant = self._mean_variant(name)
            direction = "↓ better" if self._lower_is_better(name) else "↑ better"
            lines.append(
                f"  {name:<12} baseline={baseline}  variant={variant}  ({direction})"
            )
        lines.append(f"  improvement: {self.improvement}")
        out = "\n".join(lines)
        print(out)
        return out

    def __str__(self) -> str:
        return self.summary()
