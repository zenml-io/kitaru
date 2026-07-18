"""Strict scoring contract tests."""

from __future__ import annotations

import math
from typing import Any, cast

import pytest
from pydantic import ValidationError
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import Score, scorer
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.errors import KitaruUsageError
from kitaru.scoring import (
    GroundedCapabilityDeclaration,
    ProtectionDeclaration,
    ScoreObservationOutcome,
    ScoreObservationStatus,
    ScorerDeclaration,
    ScorerSnapshot,
)
from kitaru.scoring._contracts import sha256_json


def _score_evidence(_: object) -> Score:
    return Score(value=1.0, explanation="complete", metadata={"checks": 1})


def test_score_is_strict_finite_and_bool_normalized() -> None:
    assert Score(value=True).value == 1.0
    assert Score(value=False).value == 0.0
    assert Score(value=0).value == 0.0
    assert Score(value=1.0, explanation="ok", metadata={"rule": "exact"}).value == 1.0

    for value in (None, "0.5", math.nan, math.inf, -0.1, 1.1):
        with pytest.raises(ValidationError):
            Score(value=cast(Any, value))


def test_scorer_requires_explicit_capability_and_unambiguous_signature() -> None:
    with pytest.raises(KitaruUsageError, match="explicit"):
        scorer(_score_evidence)

    declared = cast(
        ScorerDeclaration,
        scorer(_score_evidence, capability="pure"),
    )
    assert declared.snapshot.capability.value == "pure"
    assert declared.snapshot.output_contract.returns == "score"
    assert declared(object()).value == 1.0

    def ambiguous(*args: Any) -> Score:
        return Score(value=1.0)

    with pytest.raises(KitaruUsageError, match="unambiguous"):
        scorer(ambiguous, capability="pure")


def test_scorer_snapshot_is_deterministic_and_secret_safe() -> None:
    first = ScorerSnapshot.from_callable(
        _score_evidence,
        capability="pure",
        configuration={"threshold": 0.5},
    )
    second = ScorerSnapshot.from_callable(
        _score_evidence,
        capability="pure",
        configuration={"threshold": 0.5},
    )

    assert first.revision == second.revision
    assert first.configuration_hash == sha256_json({"threshold": 0.5})
    assert first.source.status == "captured"
    assert "_score_evidence" in (first.source.text or "")
    assert "pickle" not in first.model_dump_json()
    assert "bytecode" not in first.model_dump_json()

    with pytest.raises(KitaruUsageError, match="secret"):
        ScorerSnapshot.from_callable(
            _score_evidence,
            capability="pure",
            configuration={"api_key": "should-not-persist"},
        )


def test_protection_declaration_has_stable_fixed_identity() -> None:
    declaration = ProtectionDeclaration.from_callable(
        _score_evidence,
        protection_id="complete-output",
        capability="pure",
        configuration={"rule_version": 1},
    )

    assert declaration.snapshot.protection_id == "complete-output"
    assert declaration.snapshot.pass_rule == "score == 1.0"
    assert declaration.snapshot.scorer.configuration_hash == sha256_json(
        {"rule_version": 1}
    )
    assert declaration.snapshot.scorer.source.status == "captured"
    assert declaration(object()).value == 1.0
    assert declaration.__wrapped__ is declaration.scorer
    assert "_func" not in declaration.__dict__
    assert "_func" not in declaration.snapshot.model_dump_json()

    with pytest.raises(KitaruUsageError, match="secret"):
        ProtectionDeclaration.from_callable(
            _score_evidence,
            protection_id="unsafe-config",
            capability="pure",
            configuration={"access_token": "do-not-store"},
        )


def test_agent_rejects_duplicate_protection_ids() -> None:
    agent = KitaruAgent(Agent(TestModel(), name="protected-agent", output_type=str))
    agent.protection("complete-output", capability="pure")(_score_evidence)

    with pytest.raises(KitaruUsageError, match="already declared"):
        agent.protection("complete-output", capability="pure")(_score_evidence)


def test_source_capture_failure_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    import inspect

    def unavailable(_: object) -> Score:
        return Score(value=0.5)

    def raise_oserror(_: object) -> str:
        raise OSError("source unavailable")

    monkeypatch.setattr(inspect, "getsource", raise_oserror)
    snapshot = ScorerSnapshot.from_callable(unavailable, capability="pure")

    assert snapshot.source.status == "unavailable"
    assert snapshot.source.text is None
    assert snapshot.source.sha256.startswith("sha256:")


def test_grounded_capabilities_must_be_read_only() -> None:
    with pytest.raises(ValidationError, match="read-only"):
        GroundedCapabilityDeclaration(
            name="search",
            revision="v1",
            read_only=False,
        )


def test_outcomes_are_typed_and_never_invent_zero_scores() -> None:
    assert (
        ScoreObservationOutcome(
            status=ScoreObservationStatus.SCORED, score=Score(value=0.0)
        ).score
        is not None
    )
    with pytest.raises(ValidationError, match="require a Score"):
        ScoreObservationOutcome(status=ScoreObservationStatus.SCORED)
    with pytest.raises(ValidationError, match="Only SCORED"):
        ScoreObservationOutcome(
            status=ScoreObservationStatus.ERROR, score=Score(value=0.0), reason="boom"
        )
    with pytest.raises(ValidationError, match="require a reason"):
        ScoreObservationOutcome(status=ScoreObservationStatus.ABSTAINED)
