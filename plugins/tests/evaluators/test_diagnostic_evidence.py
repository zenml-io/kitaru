#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Regression coverage for localized numeric evaluator findings."""

from datetime import timedelta
from decimal import Decimal

from test_deterministic import NOW, _by_name, _node, _view

from kitaru.api_models.v1.session_node import NodeLink, NodeStatus, NodeType
from kitaru_evaluator import deterministic as evaluators


def test_diagnostics_explain_node_relationships_and_resource_fields() -> None:
    child = _node(
        8,
        parent_ordinal=99,
        started_at=NOW,
        ended_at=NOW - timedelta(seconds=1),
    ).model_copy(
        update={
            "cost": Decimal("-1"),
            "links": [NodeLink(external_id="node-2", kind="follows")],
        }
    )
    results = _by_name(
        evaluators.session_diagnostics(_view([child, _node(2)], cost=Decimal("-1")))
    )

    linkage = results["parent_linkage"]
    assert linkage.score == 1
    assert linkage.min_score == 0
    assert linkage.max_score == 2
    assert linkage.value is None
    assert linkage.passed is None
    assert linkage.explanation is not None
    assert "zero-based node indexes" in linkage.explanation
    assert '"index":0' in linkage.explanation
    assert '"missing_references":["node-99"]' in linkage.explanation
    assert '"nonpreceding_references":["node-2"]' in linkage.explanation
    chronology = results["chronology_findings"].explanation
    assert chronology is not None
    assert '"index":0' in chronology
    assert "negative_duration" in chronology
    resources = results["resource_integrity"]
    assert resources.score == 2
    assert resources.max_score is None
    assert resources.explanation is not None
    assert '"scope":"session"' in resources.explanation
    assert '"index":0,"scope":"node"' in resources.explanation
    assert '"field":"cost"' in resources.explanation


def test_shared_finding_results_explain_tools_models_and_node_pairs() -> None:
    private_input = {"secret": "must-not-appear"}
    tools = _view(
        [
            _node(
                8, tool_name="search", inputs=private_input, status=NodeStatus.FAILED
            ),
            _node(2, tool_name="search", inputs=private_input, outputs={}),
        ]
    )
    repeats = _by_name(evaluators.trajectory_signals(tools))["failed_identical_retries"]
    assert repeats.explanation is not None
    assert '"indexes":[0,1]' in repeats.explanation
    assert '"tool_name":"search"' in repeats.explanation
    assert "must-not-appear" not in repeats.explanation
    failures = _by_name(evaluators.tool_health(tools))["failed_calls"]
    assert failures.explanation is not None
    assert '"index":0' in failures.explanation
    assert '"tool_name":"search"' in failures.explanation
    models = _view(
        [_node(0, node_type=NodeType.LLM_CALL, requested_model="a", model="b")]
    )
    mismatch = _by_name(evaluators.llm_call_signals(models))[
        "requested_model_mismatches"
    ]
    assert mismatch.explanation is not None
    assert '"index":0' in mismatch.explanation
    assert '"requested":"a"' in mismatch.explanation
    assert '"served":"b"' in mismatch.explanation


def test_cycle_findings_explain_the_node_range_and_repetition() -> None:
    view = _view(
        [_node(index, tool_name=name, inputs={}) for index, name in enumerate("ababab")]
    )
    cycle = _by_name(evaluators.trajectory_signals(view))["short_cycles"]
    assert cycle.score == 1
    assert cycle.explanation is not None
    assert '"start_index":0' in cycle.explanation
    assert '"end_index":5' in cycle.explanation
    assert '"period":2' in cycle.explanation
    assert '"repetitions":3' in cycle.explanation
    assert '"tools":["a","b"]' in cycle.explanation


def test_empty_findings_keep_the_readable_description() -> None:
    result = _by_name(evaluators.session_diagnostics(_view()))["parent_linkage"]
    assert result.score == 0
    assert result.max_score == 0
    assert result.explanation == "Nodes with missing or nonpreceding parents or links."


def test_llm_call_signals_keep_native_empty_object_output_semantics() -> None:
    view = _view([_node(0, node_type=NodeType.LLM_CALL, outputs={})])
    results = _by_name(evaluators.llm_call_signals(view))
    assert results["failed_calls"].score == 0
    empty = results["empty_results"]
    assert empty.score == 1
    assert empty.max_score == 1
    assert empty.passed is None


def test_many_large_findings_keep_full_counts_and_bounded_evidence() -> None:
    reference = "missing-" + "界" * 10_000
    nodes = [
        _node(index).model_copy(
            update={
                "parent_external_id": reference,
                "links": [
                    NodeLink(external_id=f"{reference}-{link}", kind="follows")
                    for link in range(20)
                ],
            }
        )
        for index in range(100)
    ]
    result = _by_name(evaluators.session_diagnostics(_view(nodes)))["parent_linkage"]
    assert result.score == 100
    assert result.max_score == 100
    assert result.explanation is not None
    assert '"index":0' in result.explanation
    assert "missing-" in result.explanation
    assert reference not in result.explanation
    assert "more" in result.explanation
    assert "findings omitted" in result.explanation
    assert len(result.explanation.encode("utf-8")) < 8_200


def test_many_short_findings_show_only_a_bounded_sample() -> None:
    nodes = [_node(index, parent_ordinal=999) for index in range(100)]
    result = _by_name(evaluators.session_diagnostics(_view(nodes)))["parent_linkage"]
    assert result.score == 100
    assert result.explanation is not None
    assert '"index":19' in result.explanation
    assert '"index":20' not in result.explanation
    assert "80 additional findings omitted" in result.explanation


def test_escaped_large_details_keep_locators_and_later_findings() -> None:
    missing = [f"m{index}" + "\x01" * 158 for index in range(5)]
    nonpreceding = [f"p{index}" + "\x02" * 158 for index in range(5)]
    first = _node(0).model_copy(
        update={
            "links": [
                NodeLink(external_id=reference, kind="follows")
                for reference in missing + nonpreceding
            ]
        }
    )
    later = _node(1, parent_ordinal=999)
    targets = [
        _node(index + 2).model_copy(update={"external_id": reference})
        for index, reference in enumerate(nonpreceding)
    ]
    result = _by_name(evaluators.session_diagnostics(_view([first, later, *targets])))[
        "parent_linkage"
    ]

    assert result.score == 2
    assert result.min_score == 0
    assert result.max_score == 7
    assert result.value is None
    assert result.passed is None
    assert result.explanation is not None
    assert '"index":0' in result.explanation
    assert '"index":1' in result.explanation
    assert '"missing_references":["node-999"]' in result.explanation
    assert "details omitted" in result.explanation
    assert len(result.explanation.encode("utf-8")) < 8_200
