#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Requested/served model-name comparison through both evaluator entrypoints."""

import pytest
from test_deterministic import _by_name, _node, _view

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeType
from kitaru_evaluator import deterministic as evaluators

# Provider-documented spellings, not a reproduction of the customer's identifiers:
# https://ai.google.dev/api/models and https://ai.google.dev/gemini-api/docs/models
_MODEL = "gemini-3-flash-preview"
_RESOURCE_MODEL = "models/gemini-3-flash-preview"


@pytest.mark.parametrize(
    ("requested", "served", "provider", "differences", "passed"),
    [
        pytest.param(_MODEL, _MODEL, None, 0, True, id="identical"),
        pytest.param(_MODEL, _RESOURCE_MODEL, "google", 0, True, id="google-alias"),
        pytest.param(_RESOURCE_MODEL, _MODEL, "google", 0, True, id="reverse-alias"),
        pytest.param(_MODEL, _RESOURCE_MODEL, None, 1, None, id="missing-provider"),
        pytest.param(_MODEL, _RESOURCE_MODEL, "other", 1, None, id="wrong-provider"),
        pytest.param("requested", "served", "google", 1, None, id="unknown-pair"),
        pytest.param(
            "gpt-4o-2024-05-13",
            "gpt-4o-2024-08-06",
            "openai",
            1,
            None,
            id="different-versions",
        ),
        pytest.param(_MODEL, "gemini-3-flash", "google", 1, None, id="keep-suffix"),
        pytest.param(_MODEL, "google/" + _MODEL, "google", 1, None, id="keep-prefix"),
        pytest.param(None, _MODEL, "google", 0, None, id="missing-requested"),
        pytest.param(_MODEL, None, "google", 0, None, id="missing-served"),
        pytest.param(None, None, None, 0, None, id="missing-both"),
    ],
)
def test_model_name_comparison(
    requested: str | None,
    served: str | None,
    provider: str | None,
    differences: int,
    passed: bool | None,
) -> None:
    node = _node(
        0,
        node_type=NodeType.LLM_CALL,
        requested_model=requested,
        model=served,
        model_provider=provider,
    )
    view = _view([node])

    signal = _by_name(evaluators.llm_call_signals(view))["requested_model_mismatches"]
    policy = _by_name(
        evaluators.model_policy(view, require_requested_model_match=True)
    )["requested_model_match"]

    for result in (signal, policy):
        assert result.score == differences
        assert result.min_score == 0
        assert result.max_score == 1
        assert result.value is None
        assert result.data_type is EvaluationDataType.FLOAT
        if differences:
            assert requested is not None and served is not None
            assert result.explanation is not None
            assert requested in result.explanation
            assert served in result.explanation
            assert "unresolved" in result.explanation.lower()
        else:
            assert result.explanation is not None
            assert "No unresolved" in result.explanation
            assert "unknown" not in result.explanation
    assert signal.passed is None
    assert policy.passed is passed
    assert node.requested_model == requested
    assert node.model == served


@pytest.mark.parametrize("status", [SessionStatus.COMPLETED, SessionStatus.IN_PROGRESS])
@pytest.mark.parametrize("incomplete", [False, True])
def test_model_match_requires_terminal_complete_evidence(
    status: SessionStatus, incomplete: bool
) -> None:
    nodes = [
        _node(
            0,
            node_type=NodeType.LLM_CALL,
            requested_model=_MODEL,
            model=_RESOURCE_MODEL,
            model_provider="google",
        )
    ]
    if incomplete:
        nodes.append(_node(1, node_type=NodeType.LLM_CALL, model=_MODEL))

    policy = _by_name(
        evaluators.model_policy(
            _view(nodes, status=status), require_requested_model_match=True
        )
    )["requested_model_match"]

    assert policy.score == 0
    assert policy.passed is (
        True if status is SessionStatus.COMPLETED and not incomplete else None
    )


def test_aliases_do_not_relax_exact_allowlists() -> None:
    view = _view(
        [
            _node(
                0,
                node_type=NodeType.LLM_CALL,
                requested_model=_MODEL,
                model=_RESOURCE_MODEL,
                model_provider="google",
            )
        ]
    )
    policy = _by_name(
        evaluators.model_policy(
            view,
            allowed_models=[_MODEL],
            allowed_providers=["Google"],
            require_requested_model_match=True,
        )
    )

    assert policy["requested_model_match"].passed is True
    assert policy["allowed_models"].passed is False
    assert policy["allowed_models"].score == 1
    assert policy["allowed_providers"].passed is False


def test_different_versions_fail_an_explicit_model_allowlist() -> None:
    view = _view(
        [
            _node(
                0,
                node_type=NodeType.LLM_CALL,
                requested_model="gpt-4o-2024-05-13",
                model="gpt-4o-2024-08-06",
                model_provider="openai",
            )
        ]
    )
    policy = _by_name(
        evaluators.model_policy(
            view,
            allowed_models=["gpt-4o-2024-05-13"],
            require_requested_model_match=True,
        )
    )

    assert policy["allowed_models"].passed is False
    assert policy["requested_model_match"].score == 1
    assert policy["requested_model_match"].passed is None


def test_mixed_model_evidence_counts_only_unresolved_pairs() -> None:
    view = _view(
        [
            _node(0),
            _node(
                1,
                node_type=NodeType.LLM_CALL,
                requested_model=_MODEL,
                model=_RESOURCE_MODEL,
                model_provider="google",
            ),
            _node(
                2,
                node_type=NodeType.LLM_CALL,
                requested_model="requested",
                model="served",
            ),
            _node(3, node_type=NodeType.LLM_CALL, model=_MODEL),
        ]
    )
    signal = _by_name(evaluators.llm_call_signals(view))["requested_model_mismatches"]
    policy = _by_name(
        evaluators.model_policy(view, require_requested_model_match=True)
    )["requested_model_match"]

    for result in (signal, policy):
        assert result.score == 1
        assert result.max_score == 3
        assert result.passed is None
        assert result.explanation is not None
        assert '"index": 2' in result.explanation
        assert '"requested": "requested"' in result.explanation
        assert _RESOURCE_MODEL not in result.explanation


def test_model_name_evidence_is_bounded_without_losing_the_count() -> None:
    nodes = [
        _node(
            index,
            node_type=NodeType.LLM_CALL,
            requested_model="requested-" + "\0" * 10_000,
            model="served-" + "\0" * 10_000,
        )
        for index in range(7)
    ]
    view = _view(nodes)
    signal = _by_name(evaluators.llm_call_signals(view))["requested_model_mismatches"]
    policy = _by_name(
        evaluators.model_policy(view, require_requested_model_match=True)
    )["requested_model_match"]

    for result in (signal, policy):
        assert result.score == 7
        assert result.explanation is not None
        assert "5 of 7 calls" in result.explanation
        assert "requested-" in result.explanation
        assert "served-" in result.explanation
        assert len(result.explanation.encode("utf-8")) < 10_000
    assert nodes[0].requested_model == "requested-" + "\0" * 10_000
