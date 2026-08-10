"""Load exported evaluators and map their exact results to target rewards."""

import hashlib
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.task.evaluator import (
    EvaluationError,
    EvaluatorReturn,
    SessionView,
    call_evaluator,
)
from kitaru.task.plugins import (
    PluginLoadError,
    load_plugin_entrypoint,
    load_source_ref,
)

from .models import (
    ExportError,
    MaterializedEvaluator,
    RewardSelector,
)
from .trace import redact_secret_values

_LABEL = "Evaluator"
LoadedEvaluator = Callable[..., EvaluatorReturn]


@dataclass(frozen=True)
class EvaluationOutcome:
    """Record the selected reward and all numeric evaluator outputs."""

    reward: float
    metrics: dict[str, float]
    results: dict[str, tuple[EvaluationResult, ...]]


def load_evaluator(
    evaluator: MaterializedEvaluator, *, script_path: Path | None = None
) -> LoadedEvaluator:
    """Load one exact materialized script or installed package evaluator.

    Args:
        evaluator: Pinned evaluator metadata resolved by the exporter.
        script_path: Path where the generated artifact materialized script bytes.

    Raises:
        ExportError: The source is inconsistent or the entrypoint cannot load.

    Returns:
        Evaluator callable accepted by Kitaru's existing evaluator helper.
    """
    source = evaluator.version.source
    try:
        if isinstance(source, ScriptPluginSource):
            if script_path is None or evaluator.script is None:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} requires its materialized script.",
                )
            try:
                actual = script_path.read_bytes()
            except OSError as error:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Could not read evaluator {evaluator.name!r}: {error}",
                ) from error
            if actual != evaluator.script:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} script does not match "
                    "the exported source.",
                )
            if hashlib.sha256(actual).hexdigest() != evaluator.source_sha256:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} script digest is invalid.",
                )
            return load_plugin_entrypoint(script_path, source.entrypoint, _LABEL)
        if isinstance(source, PackagePluginSource):
            return load_source_ref(source.entrypoint, _LABEL)
    except PluginLoadError as error:
        raise ExportError(
            "evaluator_load_failed",
            f"Evaluator {evaluator.name!r} failed to load: {error}",
        ) from error
    raise ExportError(
        "evaluator_load_failed",
        f"Evaluator {evaluator.name!r} has an unsupported source.",
    )


def _get_numeric_score(result: EvaluationResult) -> float | None:
    score = result.score
    if score is None or isinstance(score, bool):
        return None
    value = float(score)
    return value if math.isfinite(value) else None


def _get_selected_reward(
    selector: RewardSelector, results: Sequence[EvaluationResult]
) -> float:
    selected = [result for result in results if result.name == selector.result]
    if len(selected) != 1:
        raise ExportError(
            "missing_reward_result",
            f"Evaluator {selector.evaluator!r} did not return exactly one "
            f"result named {selector.result!r}.",
        )
    result = selected[0]
    if selector.field == "passed":
        if result.passed is None:
            raise ExportError(
                "invalid_reward_value",
                f"Selected result {selector.result!r} has no boolean passed value.",
            )
        return 1.0 if result.passed else 0.0
    score = _get_numeric_score(result)
    if score is None:
        raise ExportError(
            "invalid_reward_value",
            f"Selected result {selector.result!r} has no numeric score.",
        )
    return score


def evaluate_session(
    evaluators: Sequence[tuple[MaterializedEvaluator, LoadedEvaluator]],
    selector: RewardSelector,
    session: SessionView,
    *,
    secret_values: Iterable[str] = (),
) -> EvaluationOutcome:
    """Run pinned evaluators and map one explicit result to a numeric reward.

    Raises:
        ExportError: An evaluator fails or the selected reward is absent or invalid.
    """
    if (
        sum(materialized.name == selector.evaluator for materialized, _ in evaluators)
        != 1
    ):
        raise ExportError(
            "invalid_reward_selector",
            f"Primary reward evaluator {selector.evaluator!r} is not loaded "
            "exactly once.",
        )

    all_results: dict[str, tuple[EvaluationResult, ...]] = {}
    metrics: dict[str, float] = {}
    reward: float | None = None
    secrets = tuple(secret_values)
    for materialized, evaluator in evaluators:
        try:
            raw_results = call_evaluator(
                materialized.name, evaluator, session, materialized.params
            )
        except EvaluationError as error:
            message = redact_secret_values(str(error), secrets)
            raise ExportError("evaluator_failed", str(message)) from error

        redacted_results = tuple(
            EvaluationResult.model_validate(
                redact_secret_values(result.model_dump(mode="python"), secrets)
            )
            for result in raw_results
        )
        all_results[materialized.name] = redacted_results
        for result in redacted_results:
            prefix = f"{materialized.name}:{result.name}"
            if (score := _get_numeric_score(result)) is not None:
                metrics[f"{prefix}:score"] = score
            if result.passed is not None:
                metrics[f"{prefix}:passed"] = 1.0 if result.passed else 0.0
        if materialized.name == selector.evaluator:
            reward = _get_selected_reward(selector, redacted_results)

    if reward is None:
        raise ExportError("invalid_reward_value", "Primary reward was not produced.")
    return EvaluationOutcome(reward=reward, metrics=metrics, results=all_results)
