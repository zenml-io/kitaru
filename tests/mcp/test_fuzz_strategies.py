#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Regression checks for schema-derived MCP request generation."""

import uuid
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter
from test_fuzz_tools import (
    _bound_free_json,
    _import_query_strategy,
    _require_discriminator_tags,
    _update_strategy,
    request_adapter_for,
)

from kitaru.api_models.v1.imports import ImportQuery

_ID = str(uuid.UUID(int=1))


@pytest.mark.parametrize(
    "value",
    [
        {"nested": list(range(6))},
        {"nested": {str(index): index for index in range(6)}},
        {"one": {"two": {"three": None}}},
        {"nested": "a" * 81},
        {"a" * 81: None},
    ],
)
def test_free_json_bounds_apply_inside_containers(value: Any) -> None:
    """A small outer container must not hide unbounded nested JSON."""
    schema = request_adapter_for("kitaru_review_manage").json_schema()
    request = {"operation": "create_annotation", "session_id": _ID, "value": value}
    assert Draft202012Validator(schema).is_valid(request)
    assert not Draft202012Validator(_bound_free_json(schema)).is_valid(request)


def test_free_json_bounds_apply_to_importer_extra_fields() -> None:
    schema = request_adapter_for("kitaru_session_import").json_schema()
    query = {"since": "2026-10-01T00:00:00Z", "provider_selection": {"ids": [0] * 6}}
    request = {
        "source": {"type": "api", "query": query},
        "importer_id": _ID,
        "importer_version": 1,
        "agent_version_id": _ID,
    }
    assert Draft202012Validator(schema).is_valid(request)
    assert not Draft202012Validator(_bound_free_json(schema)).is_valid(request)
    query["provider_selection"] = {"ids": [0] * 5}
    assert Draft202012Validator(_bound_free_json(schema)).is_valid(request)


def test_free_json_preserves_dictionary_types_and_public_batch_limit() -> None:
    schema = _bound_free_json(
        request_adapter_for("kitaru_workflow_start").json_schema()
    )
    evaluator: dict[str, Any] = {
        "evaluator_id": _ID,
        "version": 1,
        "params": {"nested": [1]},
    }
    request = {
        "operation": "evaluation",
        "session_ids": [str(uuid.UUID(int=index)) for index in range(1, 101)],
        "evaluators": [evaluator],
    }
    validator = Draft202012Validator(schema)
    assert validator.is_valid(request)
    evaluator["params"] = [1]
    assert not validator.is_valid(request)


@pytest.mark.mcp_fuzz
@given(data=st.data())
@settings(deadline=None)
def test_import_queries_validate_without_model_filter(data: st.DataObject) -> None:
    schema = _bound_free_json(TypeAdapter(ImportQuery).json_schema())
    query = data.draw(_import_query_strategy(schema, schema.get("$defs", {})))
    # This validates the raw constructive strategy before any rejection filter.
    ImportQuery.model_validate(query)


@pytest.mark.mcp_fuzz
@pytest.mark.parametrize(
    ("tool", "model"),
    [
        ("kitaru_cohorts_manage", "CohortUpdate"),
        ("kitaru_cohorts_manage", "CohortVersionUpdate"),
        ("kitaru_experiments_manage", "ExperimentUpdate"),
        ("kitaru_connections_manage", "ConnectionUpdate"),
        ("kitaru_evaluators_manage", "EvaluatorUpdate"),
        ("kitaru_evaluators_manage", "EvaluatorVersionUpdate"),
        ("kitaru_analyzers_manage", "AnalyzerUpdate"),
        ("kitaru_analyzers_manage", "AnalyzerVersionUpdate"),
        ("kitaru_review_manage", "InvestigationUpdate"),
        ("kitaru_review_manage", "InsightUpdate"),
    ],
)
@given(data=st.data())
@settings(deadline=None)
def test_sparse_updates_validate_without_model_filter(
    tool: str, model: str, data: st.DataObject
) -> None:
    adapter = request_adapter_for(tool)
    schema = _bound_free_json(_require_discriminator_tags(adapter.json_schema()))
    defs = schema["$defs"]
    request = data.draw(_update_strategy(defs[model], defs))
    adapter.validate_python(request)
    assert Draft202012Validator(adapter.json_schema()).is_valid(request)
