"""Replay Lab verdict, drift, trust, and summary policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from kitaru._replay_lab_models import (
    CANDIDATE_VERDICT_CAUTION,
    CANDIDATE_VERDICT_HOLD,
    CANDIDATE_VERDICT_SHIP,
    EFFICIENCY_WIN_THRESHOLD,
    REPLAY_DRIFT_QUALITY_THRESHOLD,
    REPLAY_TRUST_INSPECT,
    REPLAY_TRUST_PARTIAL,
    REPLAY_TRUST_STEADY,
    CandidateDescriptor,
    CandidateResult,
    CandidateVerdictState,
    CaseDelta,
    LaneReport,
    MetricSnapshot,
    NumericDelta,
    ReplayLabCaseReport,
    ReplayTrustState,
    ReplayTrustStatus,
    to_plain_data,
)


@dataclass
class _CandidateSummaryAccumulator:
    """Mutable per-candidate evidence while building the public summary."""

    candidate_id: str
    label: str
    aggregate_verdict: CandidateVerdictState = CANDIDATE_VERDICT_HOLD
    completed_count: int = 0
    changed_output_count: int = 0
    failed_or_timed_out_lane_count: int = 0
    efficiency_win_count: int = 0
    quality_loss_count: int = 0
    quality_evidence_count: int = 0
    efficiency_evidence_count: int = 0
    cases_to_inspect: list[str] = field(default_factory=list)
    average_cost: float | None = None
    average_latency_seconds: float | None = None
    average_quality_score: float | None = None

    def to_report_dict(self) -> dict[str, Any]:
        return to_plain_data(self)


def build_case_delta(baseline: MetricSnapshot, comparison: MetricSnapshot) -> CaseDelta:
    """Build deltas for the standard numeric metrics."""
    return CaseDelta(
        cost=_numeric_delta(baseline.cost, comparison.cost),
        duration_seconds=_numeric_delta(
            baseline.duration_seconds, comparison.duration_seconds
        ),
        latency_seconds=_numeric_delta(
            baseline.latency_seconds, comparison.latency_seconds
        ),
        quality_score=_numeric_delta(baseline.quality_score, comparison.quality_score),
    )


def candidate_result_from_lane(
    *,
    candidate_id: str,
    candidate_label: str,
    lane: LaneReport,
    baseline_lane: LaneReport,
    replay_trust: ReplayTrustStatus,
    limitations: Sequence[str] | None = None,
) -> CandidateResult:
    """Build a candidate result and verdict from one candidate lane."""
    effect = build_case_delta(baseline_lane.metrics, lane.metrics)
    output_changed = changed_output(
        baseline_lane.metrics.output_text,
        lane.metrics.output_text,
    )
    verdict = candidate_case_verdict(
        lane,
        replay_trust=replay_trust,
        effect=effect,
        output_changed=output_changed,
    )
    evidence_limitations = _candidate_evidence_limitations(
        effect=effect,
        replay_trust=replay_trust,
    )
    base_limitations = (
        list(limitations) if limitations is not None else list(lane.limitations)
    )
    return CandidateResult(
        candidate_id=candidate_id,
        candidate_label=candidate_label,
        lane=lane,
        effect_vs_baseline=effect,
        output_changed_vs_baseline=output_changed,
        verdict=verdict,
        limitations=dedupe([*base_limitations, *evidence_limitations]),
    )


def candidate_case_verdict(
    lane: LaneReport,
    *,
    replay_trust: ReplayTrustStatus,
    effect: CaseDelta,
    output_changed: bool | None,
) -> CandidateVerdictState:
    """Return the case-level candidate verdict.

    A candidate can only get `ship` when the comparison itself is trustworthy,
    quality was measured, and cost/latency evidence shows a real efficiency win.
    Missing evidence is a `caution`, not a silent pass.
    """
    if lane.timed_out or lane.status != "completed":
        return CANDIDATE_VERDICT_HOLD
    if has_quality_loss_delta(effect):
        return CANDIDATE_VERDICT_HOLD
    if replay_trust.status != REPLAY_TRUST_STEADY or output_changed is True:
        return CANDIDATE_VERDICT_CAUTION
    if not has_quality_evidence(effect) or not has_efficiency_evidence(effect):
        return CANDIDATE_VERDICT_CAUTION
    if has_efficiency_win_delta(effect):
        return CANDIDATE_VERDICT_SHIP
    return CANDIDATE_VERDICT_CAUTION


def build_case_replay_trust(
    *,
    case_id: str,
    lanes: Mapping[str, LaneReport],
    replay_drift_warning: bool,
) -> ReplayTrustStatus:
    """Build the canonical replay-trust status for one case."""
    failed_lanes = [
        name for name, lane in lanes.items() if lane_failed_or_timed_out(lane)
    ]
    if replay_drift_warning:
        return ReplayTrustStatus(
            status=REPLAY_TRUST_INSPECT,
            label="Replay trust: inspect first",
            detail=(
                f"Observed and baseline replay diverged for case `{case_id}`. "
                "Treat candidate effects as directional until this case is understood."
            ),
            reasons=["replay_drift_warning"],
        )
    if failed_lanes:
        return ReplayTrustStatus(
            status=REPLAY_TRUST_PARTIAL,
            label="Replay trust: partial",
            detail=(
                f"Case `{case_id}` has failed or timed-out replay lane(s): "
                f"{', '.join(failed_lanes)}."
            ),
            reasons=[f"lane_not_completed:{name}" for name in failed_lanes],
        )
    return ReplayTrustStatus(
        status=REPLAY_TRUST_STEADY,
        label="Replay trust: steady",
        detail=(
            "Observed and baseline replay stayed within the configured drift threshold."
        ),
        reasons=[],
    )


def case_has_replay_drift_warning(
    lanes: Mapping[str, LaneReport], replay_drift: CaseDelta
) -> bool:
    """Return whether observed-vs-baseline replay drift makes a case suspect."""
    if has_large_replay_drift(replay_drift):
        return True
    observed = lanes.get("observed")
    baseline = lanes.get("baseline_replay")
    if observed is None or baseline is None:
        return False
    observed_signature = drift_signature(observed.metrics.evaluation)
    baseline_signature = drift_signature(baseline.metrics.evaluation)
    if observed_signature is None or baseline_signature is None:
        return False
    return observed_signature != baseline_signature


def drift_signature(evaluation: Mapping[str, Any] | None) -> Any | None:
    """Return an evaluator-provided drift signature, if present.

    Core Replay Lab does not know about example-specific scorecard section names.
    Evaluators that want semantic replay-drift checks can return a stable
    `drift_signature` value in their evaluation mapping.
    """
    if not evaluation or "drift_signature" not in evaluation:
        return None
    return to_plain_data(evaluation["drift_signature"])


def has_large_replay_drift(delta: CaseDelta) -> bool:
    """Return whether numeric observed-vs-baseline drift is large."""
    for numeric_delta in (delta.cost, delta.duration_seconds, delta.latency_seconds):
        if numeric_delta.percent is not None and abs(numeric_delta.percent) >= 20:
            return True
    return (
        delta.quality_score.absolute is not None
        and abs(delta.quality_score.absolute) >= REPLAY_DRIFT_QUALITY_THRESHOLD
    )


def summarize_cases(
    cases: Sequence[ReplayLabCaseReport],
    candidates: Sequence[CandidateDescriptor],
) -> dict[str, Any]:
    """Build the canonical report summary."""
    failed_or_timed_out = 0
    drift_warning_case_ids: list[str] = []
    partial_trust_case_ids: list[str] = []
    candidate_accumulators = {
        candidate.id: _CandidateSummaryAccumulator(
            candidate_id=candidate.id,
            label=candidate.label,
        )
        for candidate in candidates
    }
    candidate_costs: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }
    candidate_latencies: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }
    candidate_qualities: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }

    for case in cases:
        all_lanes = [
            *case.lanes.values(),
            *(result.lane for result in case.candidate_results),
        ]
        failed_or_timed_out += sum(
            1 for lane in all_lanes if lane_failed_or_timed_out(lane)
        )
        if case.replay_trust.status == REPLAY_TRUST_INSPECT:
            drift_warning_case_ids.append(case.case_id)
        elif case.replay_trust.status == REPLAY_TRUST_PARTIAL:
            partial_trust_case_ids.append(case.case_id)
        for result in case.candidate_results:
            summary = candidate_accumulators[result.candidate_id]
            if lane_failed_or_timed_out(result.lane):
                summary.failed_or_timed_out_lane_count += 1
            else:
                summary.completed_count += 1
                append_number(
                    candidate_costs[result.candidate_id], result.lane.metrics.cost
                )
                append_number(
                    candidate_latencies[result.candidate_id],
                    result.lane.metrics.latency_seconds,
                )
                append_number(
                    candidate_qualities[result.candidate_id],
                    result.lane.metrics.quality_score,
                )
            if has_quality_evidence(result.effect_vs_baseline):
                summary.quality_evidence_count += 1
            if has_efficiency_evidence(result.effect_vs_baseline):
                summary.efficiency_evidence_count += 1
            if result.output_changed_vs_baseline is True:
                summary.changed_output_count += 1
                summary.cases_to_inspect.append(case.case_id)
            if result.verdict != CANDIDATE_VERDICT_SHIP:
                summary.cases_to_inspect.append(case.case_id)
            if has_efficiency_win(result):
                summary.efficiency_win_count += 1
            if has_quality_loss(result):
                summary.quality_loss_count += 1

    candidate_summaries: dict[str, dict[str, Any]] = {}
    for candidate_id, summary in candidate_accumulators.items():
        summary.average_cost = average(candidate_costs[candidate_id])
        summary.average_latency_seconds = average(candidate_latencies[candidate_id])
        summary.average_quality_score = average(candidate_qualities[candidate_id])
        summary.cases_to_inspect = dedupe(summary.cases_to_inspect)
        summary.aggregate_verdict = aggregate_candidate_verdict(
            summary.to_report_dict()
        )
        candidate_summaries[candidate_id] = summary.to_report_dict()

    candidate_decision_evidence = sorted(
        candidate_summaries.values(), key=candidate_rank_key
    )
    replay_trust = replay_trust_summary(
        drift_warning_case_ids=drift_warning_case_ids,
        partial_trust_case_ids=partial_trust_case_ids,
        failed_or_timed_out_lane_count=failed_or_timed_out,
    )

    return {
        "case_count": len(cases),
        "candidate_count": len(candidates),
        "candidate_ids": [candidate.id for candidate in candidates],
        "candidates": candidate_summaries,
        "candidate_decision_evidence": candidate_decision_evidence,
        # Compatibility alias for older report renderers; this is evidence ordering,
        # not a leaderboard.
        "candidate_ranking": candidate_decision_evidence,
        "overall_recommendation": overall_recommendation(
            candidate_decision_evidence,
            replay_trust_status=replay_trust.status,
        ),
        "failed_or_timed_out_lane_count": failed_or_timed_out,
        "replay_drift_case_ids": drift_warning_case_ids,
        # Legacy counter derived from the canonical `replay_trust` state.
        "replay_drift_warning_count": len(drift_warning_case_ids),
        "partial_replay_trust_case_ids": partial_trust_case_ids,
        "replay_trust": to_plain_data(replay_trust),
    }


def aggregate_candidate_verdict(summary: Mapping[str, Any]) -> CandidateVerdictState:
    """Return the aggregate candidate verdict from canonical evidence counts."""
    failed_count = int(summary.get("failed_or_timed_out_lane_count", 0) or 0)
    completed_count = int(summary.get("completed_count", 0) or 0)
    quality_loss_count = int(summary.get("quality_loss_count", 0) or 0)
    efficiency_win_count = int(summary.get("efficiency_win_count", 0) or 0)
    quality_evidence_count = int(summary.get("quality_evidence_count", 0) or 0)
    efficiency_evidence_count = int(summary.get("efficiency_evidence_count", 0) or 0)
    cases_to_inspect = summary.get("cases_to_inspect", []) or []
    if failed_count or not completed_count or quality_loss_count:
        return CANDIDATE_VERDICT_HOLD
    if (
        quality_evidence_count < completed_count
        or efficiency_evidence_count < completed_count
    ):
        return CANDIDATE_VERDICT_CAUTION
    if cases_to_inspect:
        return CANDIDATE_VERDICT_CAUTION
    if efficiency_win_count:
        return CANDIDATE_VERDICT_SHIP
    return CANDIDATE_VERDICT_CAUTION


def candidate_rank_key(summary: Mapping[str, Any]) -> tuple[int, int, int, str]:
    verdict = str(summary.get("aggregate_verdict", CANDIDATE_VERDICT_HOLD))
    verdict_ranks: dict[str, int] = {
        CANDIDATE_VERDICT_SHIP: 0,
        CANDIDATE_VERDICT_CAUTION: 1,
        CANDIDATE_VERDICT_HOLD: 2,
    }
    verdict_rank = verdict_ranks.get(verdict, 3)
    return (
        verdict_rank,
        int(summary.get("quality_loss_count", 0) or 0),
        -int(summary.get("efficiency_win_count", 0) or 0),
        str(summary.get("candidate_id", "")),
    )


def replay_trust_summary(
    *,
    drift_warning_case_ids: Sequence[str],
    partial_trust_case_ids: Sequence[str],
    failed_or_timed_out_lane_count: int,
) -> ReplayTrustStatus:
    if drift_warning_case_ids:
        return ReplayTrustStatus(
            status=REPLAY_TRUST_INSPECT,
            label="Replay trust: inspect first",
            detail=(
                "High replay drift was detected for "
                f"{', '.join(drift_warning_case_ids)}. Treat candidate decisions "
                "as directional until those baseline replays are understood."
            ),
            reasons=[f"replay_drift:{case_id}" for case_id in drift_warning_case_ids],
        )
    if failed_or_timed_out_lane_count or partial_trust_case_ids:
        reasons = [f"partial_case:{case_id}" for case_id in partial_trust_case_ids] or [
            "lane_not_completed"
        ]
        return ReplayTrustStatus(
            status=REPLAY_TRUST_PARTIAL,
            label="Replay trust: partial",
            detail=(
                "One or more lanes failed or timed out, so the report can still "
                "teach you where to look but should not be treated as "
                "complete evidence."
            ),
            reasons=reasons,
        )
    return ReplayTrustStatus(
        status=REPLAY_TRUST_STEADY,
        label="Replay trust: steady",
        detail=(
            "Observed and baseline replay lanes stayed within the configured drift "
            "threshold for this cohort."
        ),
        reasons=[],
    )


def overall_recommendation(
    candidate_decision_evidence: Sequence[Mapping[str, Any]],
    *,
    replay_trust_status: ReplayTrustState,
) -> str:
    if replay_trust_status != REPLAY_TRUST_STEADY:
        return (
            "Hold: inspect replay reliability before using this comparison as "
            "shipping evidence."
        )
    shippable = [
        candidate
        for candidate in candidate_decision_evidence
        if candidate.get("aggregate_verdict") == CANDIDATE_VERDICT_SHIP
    ]
    if shippable:
        return (
            f"Ship candidate `{shippable[0].get('candidate_id')}` for a guarded trial: "
            "safe enough from this replay cohort, not a blind deployment."
        )
    if any(
        candidate.get("aggregate_verdict") == CANDIDATE_VERDICT_CAUTION
        for candidate in candidate_decision_evidence
    ):
        return (
            "Caution: at least one candidate is promising, but inspect the named "
            "cases before changing production traffic."
        )
    return (
        "Hold: no candidate produced enough efficiency gain without quality risk "
        "in this cohort."
    )


def has_efficiency_win(result: CandidateResult) -> bool:
    return has_efficiency_win_delta(result.effect_vs_baseline)


def has_efficiency_win_delta(effect: CaseDelta) -> bool:
    for delta in (effect.cost, effect.latency_seconds):
        if delta.percent is not None and delta.percent <= -(
            EFFICIENCY_WIN_THRESHOLD * 100
        ):
            return True
    return False


def has_efficiency_evidence(effect: CaseDelta) -> bool:
    return effect.cost.percent is not None or effect.latency_seconds.percent is not None


def has_quality_loss(result: CandidateResult) -> bool:
    return has_quality_loss_delta(result.effect_vs_baseline)


def has_quality_loss_delta(effect: CaseDelta) -> bool:
    quality_delta = effect.quality_score.absolute
    return quality_delta is not None and quality_delta < -REPLAY_DRIFT_QUALITY_THRESHOLD


def has_quality_evidence(effect: CaseDelta) -> bool:
    return effect.quality_score.absolute is not None


def lane_failed_or_timed_out(lane: LaneReport) -> bool:
    return lane.timed_out or lane.status != "completed"


def append_number(values: list[float], value: float | None) -> None:
    if value is not None:
        values.append(value)


def average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def changed_output(baseline: str | None, candidate: str | None) -> bool | None:
    if baseline is None or candidate is None:
        return None
    return baseline.strip() != candidate.strip()


def dedupe(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def append_lane_limitations(lane: LaneReport, limitations: list[str]) -> LaneReport:
    return replace(lane, limitations=[*lane.limitations, *limitations])


def new_limitations(before: LaneReport, after: LaneReport) -> list[str]:
    return after.limitations[len(before.limitations) :]


def _candidate_evidence_limitations(
    *,
    effect: CaseDelta,
    replay_trust: ReplayTrustStatus,
) -> list[str]:
    limitations: list[str] = []
    if replay_trust.status != REPLAY_TRUST_STEADY:
        limitations.append(f"Replay trust is `{replay_trust.status}`, not `steady`.")
    if not has_quality_evidence(effect):
        limitations.append("Missing candidate-vs-baseline quality evidence.")
    if not has_efficiency_evidence(effect):
        limitations.append("Missing candidate-vs-baseline cost or latency evidence.")
    return limitations


def _numeric_delta(baseline: float | None, comparison: float | None) -> NumericDelta:
    if baseline is None or comparison is None:
        return NumericDelta(
            baseline=baseline,
            comparison=comparison,
            absolute=None,
            percent=None,
        )
    absolute = comparison - baseline
    percent = None if baseline == 0 else (absolute / baseline) * 100
    return NumericDelta(
        baseline=baseline,
        comparison=comparison,
        absolute=absolute,
        percent=percent,
    )
