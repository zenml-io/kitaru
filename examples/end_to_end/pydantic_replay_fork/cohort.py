"""Cohort experiment runner for the PydanticAI support-copilot demo.

Public surface
--------------
run_cohort(exec_ids, *, baseline_model, variant_model, variant_prompt_profile,
           metrics, repeats=1) -> Report
    For each exec_id: replay with NO edits (baseline) and replay WITH the variant
    config (averaged over ``repeats``). Both legs call the real SDK primitive
    ``support_copilot_flow.replay(...)`` directly — there is no wrapper. Apply
    each metric, track decision changes, and skip cases without the ``decide``
    checkpoint.

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
from collections.abc import Callable

from support_copilot import CUT, support_copilot_flow
from utils import MetricDelta, ReplayRun, decision_of, diff_decisions

from kitaru import KitaruClient

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _CohortRow:
    """Internal per-case row."""

    base_exec_id: str
    baseline_run: ReplayRun | None = None
    variant_run: ReplayRun | None = None
    deltas: list[MetricDelta] = dataclasses.field(default_factory=list)
    decision_changed: bool | None = None
    skipped: bool = False
    skip_reason: str | None = None


class Report:
    """Aggregate result of a cohort experiment.

    Attributes:
        rows:     Non-skipped per-case rows.
        skipped:  Number of skipped cases.
    """

    QUALITY_TOLERANCE: float = 0.1

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
            lower_is_better = next(
                (
                    d.lower_is_better
                    for r in self.rows
                    for d in r.deltas
                    if d.name == name
                ),
                True,
            )
            if lower_is_better:
                if v > b:
                    return False
            else:
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
                (
                    d.lower_is_better
                    for r in self.rows
                    for d in r.deltas
                    if d.name == name
                ),
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
        lines = [
            f"cohort experiment — {len(self.rows)} runs",
            f"  rows: {len(self.rows)} | skipped: {self.skipped}"
            f" | decision_changed: {self.decision_change_count}",
        ]
        for name in self._metric_names():
            b = self._mean_baseline(name)
            v = self._mean_variant(name)
            lower_is_better = next(
                (
                    d.lower_is_better
                    for r in self.rows
                    for d in r.deltas
                    if d.name == name
                ),
                True,
            )
            direction = "↓ better" if lower_is_better else "↑ better"
            lines.append(f"  {name:<12} baseline={b}  variant={v}  ({direction})")
        lines.append(f"  improvement: {self.improvement}")
        out = "\n".join(lines)
        print(out)
        return out

    def __str__(self) -> str:
        return self.summary()


# ---------------------------------------------------------------------------
# The experiment — replay each case twice (baseline vs variant), measure deltas
# ---------------------------------------------------------------------------


def _has_cut(client: KitaruClient, exec_id: str) -> bool:
    """True if *exec_id* has the ``decide`` checkpoint we replay from."""
    run = client.executions.get(exec_id)
    return CUT in [c.name for c in run.checkpoints]


def _replay_run(
    client: KitaruClient,
    exec_id: str,
    *,
    model: str,
    prompt_profile: str | None,
) -> ReplayRun:
    """Replay *exec_id* from the ``decide`` checkpoint and collect the result.

    ``prompt_profile=None`` replays with no config edit (the baseline leg) — the
    flow reuses the recorded inputs. Passing a profile overrides the decide +
    finalize config (the variant leg). Either way this is just the SDK call:

        support_copilot_flow.replay(exec_id, from_="decide", cache=False, **edits)
    """
    edits: dict = {"model": model}
    if prompt_profile is not None:
        edits["prompt_profile"] = prompt_profile
    handle = support_copilot_flow.replay(exec_id, from_=CUT, cache=False, **edits)
    handle.wait()
    return ReplayRun(
        exec_id=handle.exec_id,
        decision=decision_of(client, handle.exec_id),
        model=model,
    )


def run_cohort(
    exec_ids: list[str],
    *,
    baseline_model: str,
    variant_model: str,
    variant_prompt_profile: str,
    metrics: list[Callable],
    repeats: int = 1,
) -> Report:
    """Apply one config change across many production runs and measure the delta.

    For each exec_id:

    1. Replay with NO edits -> baseline ``ReplayRun`` (run once).
    2. Replay with the variant config -> variant ``ReplayRun``, repeated
       ``repeats`` times; metric variant_values are averaged.
    3. Apply each metric callable to (baseline, variant).
    4. Track whether the decision changed.
    5. Skip cases without the ``decide`` checkpoint.

    Args:
        exec_ids: Baseline (original) execution IDs to experiment on.
        baseline_model: The model the originals ran under (recorded on the
            baseline ReplayRun so the quality judge can score with it).
        variant_model: The model to replay the variant leg under.
        variant_prompt_profile: The prompt profile for the variant leg.
        metrics: BYO metric callables
            ``metric(baseline: ReplayRun, variant: ReplayRun) -> MetricDelta``.
        repeats: Variant replays per case (default 1); variant_values averaged.

    Returns:
        A Report with per-case rows and aggregate metrics.
    """
    client = KitaruClient()
    all_rows: list[_CohortRow] = []
    skipped_count = 0

    for base_id in exec_ids:
        row = _CohortRow(base_exec_id=base_id)

        if not _has_cut(client, base_id):
            row.skipped = True
            row.skip_reason = f"no {CUT!r} checkpoint"
            all_rows.append(row)
            skipped_count += 1
            _log.warning("Skipping exec %s (no %s checkpoint)", base_id, CUT)
            continue

        try:
            baseline = _replay_run(
                client, base_id, model=baseline_model, prompt_profile=None
            )
            variants = [
                _replay_run(
                    client,
                    base_id,
                    model=variant_model,
                    prompt_profile=variant_prompt_profile,
                )
                for _ in range(max(1, repeats))
            ]
        except Exception as exc:
            row.skipped = True
            row.skip_reason = f"replay failed: {exc}"
            all_rows.append(row)
            skipped_count += 1
            _log.warning("Skipping exec %s (replay failed): %s", base_id, exc)
            continue

        first_variant = variants[0]
        row.baseline_run = baseline
        row.variant_run = first_variant
        row.decision_changed = diff_decisions(
            baseline.decision, first_variant.decision
        ).has_fork_drift

        for metric_fn in metrics:
            try:
                per_repeat = [metric_fn(baseline, v) for v in variants]
                first = per_repeat[0]
                if len(per_repeat) == 1:
                    row.deltas.append(first)
                else:
                    variant_vals = [
                        d.variant_value
                        for d in per_repeat
                        if d.variant_value is not None
                    ]
                    avg_variant = (
                        sum(variant_vals) / len(variant_vals) if variant_vals else None
                    )
                    row.deltas.append(
                        MetricDelta(
                            name=first.name,
                            baseline_value=first.baseline_value,
                            variant_value=avg_variant,
                            lower_is_better=first.lower_is_better,
                        )
                    )
            except Exception as exc:
                _log.warning(
                    "Metric %s failed: %s",
                    getattr(metric_fn, "__name__", metric_fn),
                    exc,
                )

        all_rows.append(row)

    return Report(rows=all_rows, skipped_count=skipped_count)
