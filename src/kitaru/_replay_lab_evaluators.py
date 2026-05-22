"""Evaluator loading and application for Replay Lab."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from kitaru._replay_lab_models import (
    EVALUATOR_ERROR_POLICIES,
    EVALUATOR_PRECEDENCE_POLICIES,
    RUNTIME_METRIC_KEYS,
    CandidateResult,
    EvaluatorConfig,
    EvaluatorDescriptor,
    EvaluatorInput,
    LaneReport,
    MetricSnapshot,
    ReplayLabCase,
    ReplayLabEvaluator,
)
from kitaru._replay_lab_policy import (
    append_lane_limitations,
    dedupe,
    lane_failed_or_timed_out,
    new_limitations,
)
from kitaru._replay_lab_utils import number_or_none
from kitaru._replay_lab_validation import (
    validate_evaluator_descriptor,
    validate_optional_policy,
    validate_optional_string_list,
)


def resolve_evaluator_config(
    *,
    evaluator: ReplayLabEvaluator | None,
    evaluator_descriptor: Mapping[str, Any] | None,
    evaluator_on_error: str,
    evaluator_precedence: str,
) -> EvaluatorConfig | None:
    """Resolve either a trusted callable or a trusted local descriptor."""
    if evaluator_descriptor is not None:
        descriptor = validate_evaluator_descriptor(evaluator_descriptor)
        return EvaluatorConfig(
            evaluator=load_evaluator(descriptor),
            on_error=descriptor.on_error,
            precedence=descriptor.precedence,
            evaluator_id=descriptor.id,
        )
    if evaluator is None:
        return None
    return EvaluatorConfig(
        evaluator=evaluator,
        on_error=validate_optional_policy(
            evaluator_on_error,
            field_name="evaluator_on_error",
            allowed=EVALUATOR_ERROR_POLICIES,
        ),
        precedence=validate_optional_policy(
            evaluator_precedence,
            field_name="evaluator_precedence",
            allowed=EVALUATOR_PRECEDENCE_POLICIES,
        ),
    )


def load_evaluator(descriptor: EvaluatorDescriptor) -> ReplayLabEvaluator:
    """Import an evaluator function for a trusted local caller."""
    module_name, function_name = descriptor.target.split(":", 1)
    if not module_name or not function_name:
        raise ValueError("Evaluator `target` must use module:function format.")
    module = importlib.import_module(module_name)
    loaded = getattr(module, function_name)
    if not callable(loaded):
        raise ValueError("Evaluator target must resolve to a callable.")
    return loaded


def apply_evaluator_to_case(
    *,
    case: ReplayLabCase,
    from_checkpoint: str,
    lanes: Mapping[str, LaneReport],
    candidate_results: Sequence[CandidateResult],
    evaluator_config: EvaluatorConfig,
) -> tuple[dict[str, LaneReport], list[CandidateResult]]:
    """Apply a resolved evaluator to all completed lanes for one case."""
    updated_lanes = dict(lanes)
    for lane_name, lane in lanes.items():
        updated_lanes[lane_name] = evaluate_lane(
            evaluator=evaluator_config.evaluator,
            evaluator_id=evaluator_config.evaluator_id,
            on_error=evaluator_config.on_error,
            precedence=evaluator_config.precedence,
            request=EvaluatorInput(
                case_id=case.case_id,
                source_exec_id=case.exec_id,
                from_checkpoint=from_checkpoint,
                lane_name=lane_name,
                lane=lane,
            ),
        )

    updated_results: list[CandidateResult] = []
    for result in candidate_results:
        evaluated_lane = evaluate_lane(
            evaluator=evaluator_config.evaluator,
            evaluator_id=evaluator_config.evaluator_id,
            on_error=evaluator_config.on_error,
            precedence=evaluator_config.precedence,
            request=EvaluatorInput(
                case_id=case.case_id,
                source_exec_id=case.exec_id,
                from_checkpoint=from_checkpoint,
                lane_name="candidate_replay",
                lane=result.lane,
                candidate_id=result.candidate_id,
                candidate_label=result.candidate_label,
            ),
        )
        updated_results.append(
            replace(
                result,
                lane=evaluated_lane,
                limitations=dedupe(
                    [
                        *result.limitations,
                        *new_limitations(result.lane, evaluated_lane),
                    ]
                ),
            )
        )
    return updated_lanes, updated_results


def evaluate_lane(
    *,
    evaluator: ReplayLabEvaluator,
    evaluator_id: str | None,
    on_error: str,
    precedence: str,
    request: EvaluatorInput,
) -> LaneReport:
    """Run an evaluator on one lane and merge its output into metrics."""
    if lane_failed_or_timed_out(request.lane):
        return request.lane
    try:
        raw_evaluation = evaluator(request)
        if raw_evaluation is None:
            return request.lane
        if not isinstance(raw_evaluation, Mapping):
            raise ValueError("Evaluator must return a mapping or None.")
        return apply_evaluation_result(
            request.lane,
            raw_evaluation,
            evaluator_id=evaluator_id,
            precedence=precedence,
        )
    except Exception as exc:
        if on_error == "fail":
            raise
        return append_lane_limitations(
            request.lane,
            [f"Evaluator failed ({type(exc).__name__}: {exc})."],
        )


def apply_evaluation_result(
    lane: LaneReport,
    raw_evaluation: Mapping[str, Any],
    *,
    evaluator_id: str | None,
    precedence: str,
) -> LaneReport:
    """Normalize and merge evaluator output into one lane report."""
    evaluation, limitations = normalize_evaluation(raw_evaluation, evaluator_id)
    quality_score = number_or_none(evaluation.get("quality_score"))
    final_quality = lane.metrics.quality_score
    if quality_score is not None and (
        precedence == "override" or lane.metrics.quality_score is None
    ):
        final_quality = quality_score
    return replace(
        lane,
        metrics=replace_metrics(
            lane.metrics,
            quality_score=final_quality,
            evaluation=evaluation,
        ),
        limitations=[*lane.limitations, *limitations],
    )


def normalize_evaluation(
    raw_evaluation: Mapping[str, Any],
    evaluator_id: str | None,
) -> tuple[dict[str, Any], list[str]]:
    """Strip runtime facts from evaluator output and keep evaluator facts."""
    evaluation = dict(raw_evaluation)
    limitations = validate_optional_string_list(
        evaluation.get("limitations", []), field_name="Evaluator `limitations`"
    )
    for key in RUNTIME_METRIC_KEYS:
        evaluation.pop(key, None)
    if evaluator_id is not None and "evaluator_id" not in evaluation:
        evaluation["evaluator_id"] = evaluator_id
    return evaluation, limitations


def replace_metrics(
    metrics: MetricSnapshot,
    *,
    quality_score: float | None,
    evaluation: dict[str, Any] | None,
) -> MetricSnapshot:
    """Return a metric snapshot with evaluator-owned fields updated."""
    return replace(
        metrics,
        quality_score=quality_score,
        evaluation=evaluation,
    )
