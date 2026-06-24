"""Cohort experiment runner for the PydanticAI support-copilot demo.

``run_cohort(...)`` replays many parent executions once with a variant config
and compares each replay directly to the original production run.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from reporting.cohort_report import CohortRow, Report
from support_agent import REPLAY_POINT, support_copilot_flow

from kitaru import KitaruClient, diff
from utils.metrics import (
    MetricDelta,
    ReplayRun,
    diff_decisions,
    load_support_decision_from_execution,
)

_log = logging.getLogger(__name__)


def _to_replay_run(client: KitaruClient, exec_id: str, *, model: str) -> ReplayRun:
    return ReplayRun(
        exec_id=exec_id,
        decision=load_support_decision_from_execution(client, exec_id),
        model=model,
    )


def _skipped_row(exec_id: str, reason: str) -> CohortRow:
    _log.warning("Skipping exec %s: %s", exec_id, reason)
    return CohortRow(base_exec_id=exec_id, skipped=True, skip_reason=reason)


def _metric_deltas(
    baseline: ReplayRun,
    variants: list[ReplayRun],
    metrics: list[Callable],
) -> list[MetricDelta]:
    deltas: list[MetricDelta] = []
    for metric_fn in metrics:
        try:
            per_repeat = [metric_fn(baseline, variant) for variant in variants]
            first = per_repeat[0]
            if len(per_repeat) == 1:
                deltas.append(first)
                continue
            variant_vals = [
                item.variant_value
                for item in per_repeat
                if item.variant_value is not None
            ]
            deltas.append(
                MetricDelta(
                    name=first.name,
                    baseline_value=first.baseline_value,
                    variant_value=(
                        sum(variant_vals) / len(variant_vals) if variant_vals else None
                    ),
                    lower_is_better=first.lower_is_better,
                )
            )
        except Exception as exc:
            _log.warning(
                "Metric %s failed: %s",
                getattr(metric_fn, "__name__", metric_fn),
                exc,
            )
    return deltas


def _evaluate_case(
    client: KitaruClient,
    exec_id: str,
    *,
    original_decision: dict,
    original_run: ReplayRun,
    variants: list[ReplayRun],
    metrics: list[Callable],
) -> CohortRow:
    first_variant = variants[0]
    compare_urls = list(diff(exec_id, first_variant.exec_id).urls)
    return CohortRow(
        base_exec_id=exec_id,
        original_decision=original_decision,
        original_run=original_run,
        variant_run=first_variant,
        decision_changed=diff_decisions(
            original_decision, first_variant.decision
        ).has_drift,
        deltas=_metric_deltas(original_run, variants, metrics),
        compare_urls=compare_urls,
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
    """Replay a cohort with one config change and compare against originals."""
    client = KitaruClient()
    problems: dict[str, str] = {}
    variants_by_parent: dict[str, list[ReplayRun]] = {
        exec_id: [] for exec_id in exec_ids
    }

    for _ in range(max(1, repeats)):
        variant = support_copilot_flow.replay_many(
            exec_ids,
            at=REPLAY_POINT,
            cache=False,
            wait=True,
            model=variant_model,
            prompt_profile=variant_prompt_profile,
            on_error="collect",
        )
        for exec_id, error in variant.failures:
            problems.setdefault(exec_id, f"variant failed: {error}")
        for exec_id, reason in variant.skipped:
            problems.setdefault(exec_id, reason)
        for exec_id, handle in variant.successes:
            variants_by_parent[exec_id].append(
                _to_replay_run(client, handle.exec_id, model=variant_model)
            )

    rows: list[CohortRow] = []
    for exec_id in exec_ids:
        variants = variants_by_parent.get(exec_id, [])
        if not variants:
            rows.append(
                _skipped_row(exec_id, problems.get(exec_id, "variant replay failed"))
            )
            continue
        try:
            original_decision = load_support_decision_from_execution(client, exec_id)
            original_run = _to_replay_run(client, exec_id, model=baseline_model)
        except Exception as exc:
            rows.append(_skipped_row(exec_id, f"load failed: {exc}"))
            continue
        rows.append(
            _evaluate_case(
                client,
                exec_id,
                original_decision=original_decision,
                original_run=original_run,
                variants=variants,
                metrics=metrics,
            )
        )

    return Report(
        rows=rows,
        skipped_count=sum(1 for row in rows if row.skipped),
    )
