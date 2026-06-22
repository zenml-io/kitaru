"""Cohort experiment runner for the PydanticAI support-copilot demo.

Public surface
--------------
cohort(cases) -> Cohort
    Wrap a list of exec_ids in a Cohort ready to run an experiment.

Cohort.experiment(agent, *, variant: Recipe, metrics: list, repeats: int = 1) -> Report
    For each case: rerun (baseline) and replay (variant).  Apply each metric.
    Track decision changes.  Skip cases whose CUT cannot be resolved.

Report.summary()
    Print/return aggregate deltas per metric (mean baseline vs variant),
    decision-change count, and an ``improvement`` verdict.

Report.regressions() -> list[MetricDelta | str]
    Items that got WORSE (direction-aware per metric; decision changes flagged
    as strings).

Report.__str__()
    Human-readable one-pager.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING, Callable

from .utils import MetricDelta, Recipe

if TYPE_CHECKING:
    from .support_copilot import KitaruAdapterPA, RunHandle

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class _CohortRow:
    """Internal per-case row."""
    base_exec_id: str
    baseline_handle: "RunHandle | None" = None
    variant_handle: "RunHandle | None" = None
    deltas: list[MetricDelta] = dataclasses.field(default_factory=list)
    decision_changed: bool | None = None
    skipped: bool = False
    skip_reason: str | None = None


class Report:
    """Aggregate result of a cohort experiment.

    Attributes:
        rows:           Internal per-case rows (non-skipped only).
        skipped:        Number of skipped cases.
        _all_rows:      All rows including skipped (for internal use).
    """

    QUALITY_TOLERANCE: float = 0.1  # experiment >= baseline - tolerance for quality

    def __init__(self, rows: list[_CohortRow], skipped_count: int) -> None:
        self._all_rows = rows
        self.rows = [r for r in rows if not r.skipped]
        self.skipped = skipped_count

    # ---- aggregates --------------------------------------------------------

    @property
    def decision_change_count(self) -> int:
        """Number of non-skipped rows where the decision changed."""
        return sum(1 for r in self.rows if r.decision_changed is True)

    def _mean_baseline(self, metric_name: str) -> float | None:
        vals = [
            d.baseline_value
            for r in self.rows
            for d in r.deltas
            if d.name == metric_name and d.baseline_value is not None
        ]
        return sum(vals) / len(vals) if vals else None

    def _mean_variant(self, metric_name: str) -> float | None:
        vals = [
            d.variant_value
            for r in self.rows
            for d in r.deltas
            if d.name == metric_name and d.variant_value is not None
        ]
        return sum(vals) / len(vals) if vals else None

    def _metric_names(self) -> list[str]:
        seen: list[str] = []
        for r in self.rows:
            for d in r.deltas:
                if d.name not in seen:
                    seen.append(d.name)
        return seen

    @property
    def improvement(self) -> bool:
        """True iff no metric regressed across the cohort.

        - lower_is_better metrics: improvement requires variant mean <= baseline mean.
        - higher_is_better metrics: improvement requires variant mean >= baseline mean
          (with QUALITY_TOLERANCE for quality).
        """
        for name in self._metric_names():
            b = self._mean_baseline(name)
            v = self._mean_variant(name)
            if b is None or v is None:
                continue
            # Determine direction from any row that has this metric.
            lower_is_better = next(
                (d.lower_is_better for r in self.rows for d in r.deltas if d.name == name),
                True,
            )
            if lower_is_better:
                if v > b:
                    return False
            else:
                # quality: allow QUALITY_TOLERANCE
                if v < b - self.QUALITY_TOLERANCE:
                    return False
        return True

    # ---- public API --------------------------------------------------------

    def regressions(self) -> list:
        """Return per-metric aggregates that got WORSE, as MetricDelta objects.

        A regression is a MetricDelta where the mean variant value is worse
        than the mean baseline value (direction-aware).  Also includes a
        ``"decision_changed"`` string entry when any decision changed.

        Returns:
            List of MetricDelta (aggregate) objects and/or ``"decision_changed"``.
        """
        result = []
        for name in self._metric_names():
            b = self._mean_baseline(name)
            v = self._mean_variant(name)
            lower_is_better = next(
                (d.lower_is_better for r in self.rows for d in r.deltas if d.name == name),
                True,
            )
            agg = MetricDelta(
                name=name,
                baseline_value=b,
                variant_value=v,
                lower_is_better=lower_is_better,
            )
            if agg.is_worse:
                result.append(agg)
        if self.decision_change_count > 0:
            result.append("decision_changed")
        return result

    def summary(self) -> str:
        """Return and print a human-readable summary of the experiment."""
        lines = ["Report.summary()", f"  rows: {len(self.rows)} | skipped: {self.skipped} | decision_changed: {self.decision_change_count}"]
        for name in self._metric_names():
            b = self._mean_baseline(name)
            v = self._mean_variant(name)
            lower_is_better = next(
                (d.lower_is_better for r in self.rows for d in r.deltas if d.name == name),
                True,
            )
            direction = "↓ better" if lower_is_better else "↑ better"
            lines.append(
                f"  {name:<12} baseline={b}  variant={v}  ({direction})"
            )
        lines.append(f"  improvement: {self.improvement}")
        out = "\n".join(lines)
        print(out)
        return out

    def __str__(self) -> str:
        return self.summary()


# ---------------------------------------------------------------------------
# Cohort
# ---------------------------------------------------------------------------

class Cohort:
    """A set of exec_ids ready for a controlled experiment."""

    def __init__(self, cases: list[str]) -> None:
        self._cases = cases

    def experiment(
        self,
        agent: "KitaruAdapterPA",
        *,
        variant: Recipe,
        metrics: list[Callable],
        repeats: int = 1,
    ) -> Report:
        """Apply a config change across all cases and measure the delta.

        For each case:
        1. ``agent.rerun(case)`` -> baseline RunHandle.
        2. ``agent.replay(case, **variant.as_kwargs())`` -> variant RunHandle.
        3. Apply each metric callable to (baseline, variant).
        4. Track decision changes.
        5. Skip cases where CUT cannot be resolved (record in skipped count).

        Args:
            agent:   The KitaruAdapterPA to use for rerun and replay.
            variant: The Recipe describing the config change.
            metrics: List of BYO metric callables
                     ``metric(baseline: RunHandle, variant: RunHandle) -> MetricDelta``.
            repeats: Number of repeats per case (default 1).

        Returns:
            A Report with per-case rows and aggregate metrics.
        """
        all_rows: list[_CohortRow] = []
        skipped_count = 0

        for base_id in self._cases:
            row = _CohortRow(base_exec_id=base_id)

            # Validate CUT.
            try:
                agent.cut_of(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = str(exc)
                all_rows.append(row)
                skipped_count += 1
                _log.warning("Skipping exec %s (CUT not resolvable): %s", base_id, exc)
                continue

            # Baseline: rerun (no edit).
            try:
                baseline_handle = agent.rerun(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = f"rerun failed: {exc}"
                all_rows.append(row)
                skipped_count += 1
                _log.warning("Skipping exec %s (rerun failed): %s", base_id, exc)
                continue

            # Variant: replay (with edit).
            try:
                variant_handle = agent.replay(base_id, **variant.as_kwargs())
            except Exception as exc:
                row.skipped = True
                row.skip_reason = f"replay failed: {exc}"
                all_rows.append(row)
                skipped_count += 1
                _log.warning("Skipping exec %s (replay failed): %s", base_id, exc)
                continue

            row.baseline_handle = baseline_handle
            row.variant_handle = variant_handle

            # Decision change.
            try:
                dr = baseline_handle.diff(variant_handle)
                row.decision_changed = dr.has_fork_drift
            except Exception as exc:
                _log.warning("diff failed for %s vs %s: %s", baseline_handle.exec_id, variant_handle.exec_id, exc)
                row.decision_changed = None

            # BYO metrics.
            for metric_fn in metrics:
                try:
                    delta = metric_fn(baseline_handle, variant_handle)
                    row.deltas.append(delta)
                except Exception as exc:
                    _log.warning("Metric %s failed: %s", getattr(metric_fn, "__name__", metric_fn), exc)

            all_rows.append(row)

        return Report(rows=all_rows, skipped_count=skipped_count)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def cohort(cases: list[str]) -> Cohort:
    """Wrap a list of exec_ids in a Cohort ready for an experiment.

    Args:
        cases: List of baseline execution IDs.

    Returns:
        A Cohort instance.
    """
    return Cohort(cases)
