"""Focused unit tests for imported execution persistence helpers."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from kitaru._llm_usage import CalculatedCostMetadata
from kitaru.imports import normalize_langfuse_observations
from kitaru.imports._writer import (
    ImportedTraceConflictError,
    _get_or_create_snapshot,
    _import_environment,
    _step_metadata,
    _steps_by_name,
    _validate_existing_run,
)


def test_steps_by_name_reads_every_page() -> None:
    first_page = [SimpleNamespace(name=f"step-{index}") for index in range(200)]
    second_page = [SimpleNamespace(name="step-200")]
    client = MagicMock()
    client.active_project.id = "project-id"
    client.list_run_steps.side_effect = [
        SimpleNamespace(items=first_page),
        SimpleNamespace(items=second_page),
    ]

    steps = _steps_by_name(client, run_id=UUID(int=1))

    assert len(steps) == 201
    assert client.list_run_steps.call_count == 2
    assert client.list_run_steps.call_args_list[0].kwargs["page"] == 1
    assert client.list_run_steps.call_args_list[1].kwargs["page"] == 2


def test_non_llm_usage_is_not_published_as_llm_usage() -> None:
    observation = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "characters-processed",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "usageDetails": {"input": 12.5, "unit": "characters"},
                "totalCost": 0.01,
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="span-1")

    assert "llm_usage_v1" not in metadata
    assert metadata["kitaru_import_usage_v1"]["unit"] == "characters"
    assert metadata["kitaru_import_cost_v1"]["total"] == 0.01


def test_llm_usage_uses_canonical_token_normalization() -> None:
    observation = normalize_langfuse_observations(
        [
            {
                "id": "generation-1",
                "traceId": "trace-1",
                "type": "GENERATION",
                "name": "answer",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "usageDetails": {"input": 4, "output": 6},
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="generation-1")
    record = next(iter(metadata["llm_usage_v1"].values()))

    assert record["usage"]["input_tokens"] == 4
    assert record["usage"]["output_tokens"] == 6
    assert record["usage"]["total_tokens"] == 10


def test_imported_provider_cost_is_preserved_as_historical_actual(
    monkeypatch,
) -> None:
    def unexpected_estimate(**kwargs):
        raise AssertionError("reported historical cost must not be re-estimated")

    monkeypatch.setattr(
        "kitaru.imports._writer.estimate_genai_prices_cost", unexpected_estimate
    )
    observation = normalize_langfuse_observations(
        [
            {
                "id": "generation-1",
                "traceId": "trace-1",
                "type": "GENERATION",
                "name": "answer",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "providedModelName": "gpt-4o-mini",
                "usageDetails": {"input": 4, "output": 6},
                "totalCost": 0.000123,
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="generation-1")
    record = next(iter(metadata["llm_usage_v1"].values()))

    assert record["provider"] is None
    assert record["cost"] == {
        "actual_cost_usd": 0.000123,
        "estimated_cost_usd": None,
        "currency": "USD",
        "source": "provider_reported",
        "source_label": "Langfuse imported provider cost",
        "pricing_version": None,
    }


def test_missing_historical_cost_uses_current_catalog_estimate(monkeypatch) -> None:
    estimate = MagicMock(
        return_value=CalculatedCostMetadata(
            estimated_cost_usd=0.000456,
            cost_source="calculator",
            cost_source_label="genai-prices",
            pricing_version="genai-prices:test",
        )
    )
    monkeypatch.setattr("kitaru.imports._writer.estimate_genai_prices_cost", estimate)
    observation = normalize_langfuse_observations(
        [
            {
                "id": "generation-1",
                "traceId": "trace-1",
                "type": "GENERATION",
                "name": "answer",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "modelId": "gpt-4o-mini",
                "usageDetails": {"input": 4, "output": 6},
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="generation-1")
    record = next(iter(metadata["llm_usage_v1"].values()))

    estimate.assert_called_once_with(
        provider=None,
        model="gpt-4o-mini",
        usage={
            "input_tokens": 4.0,
            "output_tokens": 6.0,
            "total_tokens": None,
            "details": {"input": 4, "output": 6},
        },
        warnings=estimate.call_args.kwargs["warnings"],
        adapter_name="Langfuse import",
    )
    assert record["cost"] == {
        "actual_cost_usd": None,
        "estimated_cost_usd": 0.000456,
        "currency": "USD",
        "source": "calculator",
        "source_label": "genai-prices",
        "pricing_version": "genai-prices:test",
    }
    assert any("current genai-prices catalog" in item for item in record["warnings"])


def test_fractional_token_usage_is_preserved_without_token_coercion() -> None:
    observation = normalize_langfuse_observations(
        [
            {
                "id": "generation-1",
                "traceId": "trace-1",
                "type": "GENERATION",
                "name": "answer",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "usageDetails": {"input": 12.5, "unit": "tokens"},
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="generation-1")

    assert metadata["kitaru_import_usage_v1"]["input"] == 12.5
    assert "llm_usage_v1" not in metadata


def test_model_and_latency_without_usage_are_preserved() -> None:
    observation = normalize_langfuse_observations(
        [
            {
                "id": "generation-1",
                "traceId": "trace-1",
                "type": "GENERATION",
                "name": "answer",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "providedModelName": "example-model",
                "latencyMs": 250,
            }
        ],
        project_id="project-1",
    )[0].observations[0]

    metadata = _step_metadata(observation, step_name="generation-1")
    record = next(iter(metadata["llm_usage_v1"].values()))

    assert metadata["kitaru_import_model_v1"] == "example-model"
    assert metadata["kitaru_import_latency_ms_v1"] == 250
    assert record["model"] == "example-model"
    assert record["latency_ms"] == 250


def test_import_snapshot_identity_includes_selected_stack() -> None:
    client = MagicMock()
    client.list_snapshots.return_value = SimpleNamespace(items=[])
    client.zen_store.create_snapshot.side_effect = lambda request: request
    first_stack = UUID(int=1)
    second_stack = UUID(int=2)
    trace = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "span",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
            }
        ],
        project_id="project-1",
    )[0]

    first = _get_or_create_snapshot(
        client=client,
        project_id=UUID(int=3),
        pipeline_id=UUID(int=4),
        pipeline_name="imported-agent",
        trace=trace,
        step_config_by_observation={},
        stack_id=first_stack,
    )
    second = _get_or_create_snapshot(
        client=client,
        project_id=UUID(int=3),
        pipeline_id=UUID(int=4),
        pipeline_name="imported-agent",
        trace=trace,
        step_config_by_observation={},
        stack_id=second_stack,
    )

    assert first.name != second.name
    assert first.pipeline_version_hash != second.pipeline_version_hash
    assert first.stack == first_stack
    assert second.stack == second_stack


def test_existing_import_cannot_silently_change_stacks() -> None:
    first_stack = UUID(int=1)
    second_stack = UUID(int=2)
    trace = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "span",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
            }
        ],
        project_id="project-1",
    )[0]
    run = SimpleNamespace(
        id=UUID(int=3),
        orchestrator_environment=_import_environment(
            trace,
            agent_name="support-agent",
            stack_id=first_stack,
        ),
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=first_stack)),
    )

    with pytest.raises(
        ImportedTraceConflictError, match="already imported using stack"
    ) as exc_info:
        _validate_existing_run(
            cast(Any, run),
            trace=trace,
            agent_name="support-agent",
            stack_id=second_stack,
        )

    assert "cannot move existing artifact bytes" in (exc_info.value.resolution or "")


def test_legacy_import_reads_stack_from_snapshot() -> None:
    stack_id = UUID(int=1)
    trace = normalize_langfuse_observations(
        [
            {
                "id": "span-1",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "span",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
            }
        ],
        project_id="project-1",
    )[0]
    environment = _import_environment(
        trace,
        agent_name="support-agent",
        stack_id=stack_id,
    )
    environment.pop("kitaru_import_stack_id_v1")
    run = SimpleNamespace(
        id=UUID(int=2),
        orchestrator_environment=environment,
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=stack_id)),
    )

    _validate_existing_run(
        cast(Any, run),
        trace=trace,
        agent_name="support-agent",
        stack_id=stack_id,
    )
