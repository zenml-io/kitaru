"""Cohort experiment runner for the PydanticAI support-copilot demo.

Public surface
--------------
run_cohort(exec_ids, *, baseline_model, variant_model, variant_prompt_profile,
           metrics, repeats=1) -> Report
    For each exec_id: load the original recorded decision, replay with NO edits
    (unchanged reproduction), and replay WITH the variant config (averaged over
    ``repeats``). Both replay legs call the real SDK primitive
    ``support_copilot_flow.replay(...)`` directly — there is no wrapper. Apply
    each metric, track original→reproduction and reproduction→edited decision
    changes, and skip cases without the ``decide`` checkpoint.

Report.summary()
    Print/return aggregate deltas per metric (mean unchanged replay vs edited
    replay), reproduction drift count, edited decision-change count, and an
    ``improvement`` verdict.

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
from utils import (
    MetricDelta,
    ReplayRun,
    diff_decisions,
    load_support_decision_from_execution,
)

from kitaru import KitaruClient

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _CohortRow:
    """Internal per-case row."""

    base_exec_id: str
    original_decision: dict | None = None
    baseline_run: ReplayRun | None = None  # unchanged replay / reproduction
    variant_run: ReplayRun | None = None  # edited replay
    deltas: list[MetricDelta] = dataclasses.field(default_factory=list)
    reproduction_changed: bool | None = None
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
        """Rows where the edited replay changed the unchanged replay decision."""
        return sum(1 for r in self.rows if r.decision_changed is True)

    @property
    def reproduction_drift_count(self) -> int:
        """Rows where the unchanged replay did not match the original decision."""
        return sum(1 for r in self.rows if r.reproduction_changed is True)

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

        A regression is a MetricDelta where the mean edited replay value is worse
        than the mean unchanged replay value (direction-aware). Also includes
        string entries when reproduction drift or edited decision drift occurs.

        Returns:
            List of MetricDelta aggregate objects and/or string drift labels.
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

    ``prompt_profile=None`` replays with no config edit — the flow reuses the
    recorded inputs. Passing a profile overrides the decide + finalize config
    with ``model`` and ``prompt_profile``. Either way this is just the SDK call:

        support_copilot_flow.replay(exec_id, from_="decide", cache=False, **edits)
    """
    edits: dict = {}
    if prompt_profile is not None:
        edits["model"] = model
        edits["prompt_profile"] = prompt_profile
    handle = support_copilot_flow.replay(exec_id, from_=CUT, cache=False, **edits)
    handle.wait()
    return ReplayRun(
        exec_id=handle.exec_id,
        decision=load_support_decision_from_execution(client, handle.exec_id),
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

    1. Load the original recorded decision from checkpoint artifacts.
    2. Replay with NO edits -> unchanged reproduction ``ReplayRun`` (run once).
    3. Replay with the variant config -> edited ``ReplayRun``, repeated
       ``repeats`` times; metric variant_values are averaged.
    4. Apply each metric callable to (unchanged replay, edited replay).
    5. Track original→reproduction and reproduction→edited decision drift.
    6. Skip cases without the ``decide`` checkpoint.

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
            original_decision = load_support_decision_from_execution(client, base_id)
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
        row.original_decision = original_decision
        row.baseline_run = baseline
        row.variant_run = first_variant
        row.reproduction_changed = diff_decisions(
            original_decision, baseline.decision
        ).has_drift
        row.decision_changed = diff_decisions(
            baseline.decision, first_variant.decision
        ).has_drift

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
