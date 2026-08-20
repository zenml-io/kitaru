"""Tests for exported evaluator execution and reward mapping."""

import hashlib
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Never

import pytest

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.api_models.v1.session import SessionOrigin, SessionResponse, SessionStatus
from kitaru.exports.evaluators import evaluate_session, load_evaluator
from kitaru.exports.models import (
    ExportError,
    MaterializedEvaluator,
    RewardSelector,
)
from kitaru.task.evaluator import SessionView


def _version(
    source: ScriptPluginSource | PackagePluginSource,
) -> EvaluatorVersionResponse:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return EvaluatorVersionResponse(
        id=uuid.uuid4(),
        evaluator_id=uuid.uuid4(),
        version=1,
        display_version="1",
        source=source,
        created=now,
        updated=now,
    )


def _materialized_script() -> MaterializedEvaluator:
    path = Path(__file__).parents[1] / "fixtures" / "exports" / "evaluator.py"
    script = path.read_bytes()
    return MaterializedEvaluator(
        name="quality",
        version=_version(
            ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="evaluate")
        ),
        params={"expected": "42", "weight": 0.5},
        script=script,
        source_sha256=hashlib.sha256(script).hexdigest(),
    )


def _session(output: str = "42") -> SessionView:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return SessionView(
        session=SessionResponse(
            id=uuid.uuid4(),
            owner_id=uuid.uuid4(),
            agent_id=uuid.uuid4(),
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"question": "6 * 7"},
            outputs=output,
            metadata={},
            llm_call_count=1,
            tool_call_count=1,
            created=now,
            updated=now,
        ),
        nodes=[],
    )


def test_load_script_and_evaluate_with_configured_parameters(tmp_path: Path) -> None:
    materialized = _materialized_script()
    script_path = tmp_path / "evaluator.py"
    script_path.write_bytes(materialized.script or b"")

    evaluator = load_evaluator(materialized, script_path=script_path)
    outcome = evaluate_session(
        [(materialized, evaluator)],
        RewardSelector.parse("quality:correctness:score"),
        _session(),
    )

    assert outcome.reward == 0.5
    assert outcome.metrics == {
        "quality:correctness:score": 0.5,
        "quality:correctness:passed": 1.0,
        "quality:length:score": 2.0,
    }
    assert outcome.results["quality"][0].name == "correctness"


def test_load_package_evaluator_uses_existing_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = PackagePluginSource(
        requirement="fixture-evaluator==1.0.0",
        entrypoint="fixture_evaluator:evaluate",
    )
    materialized = MaterializedEvaluator(
        name="quality",
        version=_version(source),
        params={},
        script=None,
        source_sha256="fixture",
    )
    sentinel = lambda session: None  # noqa: E731
    seen: list[tuple[str, str]] = []

    def fake_load(ref: str, label: str) -> object:
        seen.append((ref, label))
        return sentinel

    monkeypatch.setattr("kitaru.exports.evaluators.load_source_ref", fake_load)

    assert load_evaluator(materialized) is sentinel
    assert seen == [("fixture_evaluator:evaluate", "Evaluator")]


def test_boolean_passed_maps_to_reward() -> None:
    materialized = _materialized_script()

    outcome = evaluate_session(
        [(materialized, lambda session, **params: _passed_result())],
        RewardSelector.parse("quality:correctness:passed"),
        _session(),
    )

    assert outcome.reward == 1.0


def _passed_result() -> EvaluationResult:
    return EvaluationResult(name="correctness", score=0.25, passed=True)


@pytest.mark.parametrize(
    ("selector", "result", "code"),
    [
        ("quality:missing:score", _passed_result(), "missing_reward_result"),
        (
            "quality:correctness:score",
            None,
            "invalid_reward_value",
        ),
    ],
)
def test_evaluate_session_rejects_missing_or_string_selected_results(
    selector: str, result: EvaluationResult | None, code: str
) -> None:
    materialized = _materialized_script()
    if result is None:
        result = EvaluationResult(name="correctness", value="looks good")

    with pytest.raises(ExportError) as raised:
        evaluate_session(
            [(materialized, lambda session, **params: result)],
            RewardSelector.parse(selector),
            _session(),
        )

    assert raised.value.code == code


def test_evaluate_session_turns_evaluator_exception_into_task_failure() -> None:
    materialized = _materialized_script()

    def fail(session: SessionView, **params: object) -> Never:
        raise RuntimeError("judge unavailable")

    with pytest.raises(ExportError) as raised:
        evaluate_session(
            [(materialized, fail)],
            RewardSelector.parse("quality:correctness:score"),
            _session(),
        )

    assert raised.value.code == "evaluator_failed"
    assert "judge unavailable" in raised.value.message


def test_evaluate_session_redacts_results() -> None:
    materialized = _materialized_script()
    result = EvaluationResult(
        name="correctness",
        score=1.0,
        explanation="used token secret-value",
    )

    outcome = evaluate_session(
        [(materialized, lambda session, **params: result)],
        RewardSelector.parse("quality:correctness:score"),
        _session(),
        secret_values=["secret-value"],
    )

    assert outcome.results["quality"][0].explanation == "used token [REDACTED]"


def test_evaluate_session_rejects_short_secret_values() -> None:
    materialized = _materialized_script()

    with pytest.raises(ExportError) as raised:
        evaluate_session(
            [(materialized, lambda session, **params: _passed_result())],
            RewardSelector.parse("quality:correctness:score"),
            _session(),
            secret_values=["short"],
        )

    assert raised.value.code == "unsafe_secret_value"
