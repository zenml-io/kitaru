"""Shared source-identity behavior across the provider importers."""

import json
from types import ModuleType
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession
from kitaru_braintrust_importer import importer as braintrust
from kitaru_jsonl_importer.importer import parse as parse_jsonl
from kitaru_langfuse_importer import importer as langfuse
from kitaru_langsmith_importer import importer as langsmith
from kitaru_logfire_importer import importer as logfire
from kitaru_phoenix_importer import importer as phoenix


@pytest.fixture(
    params=[
        pytest.param(
            (
                langfuse,
                "project_id",
                "projectId",
                {"id": "root", "traceId": "trace-1", "type": "SPAN"},
            ),
            id="langfuse",
        ),
        pytest.param(
            (
                langsmith,
                "project_name",
                "session_id",
                {"id": "root", "trace_id": "trace-1", "run_type": "chain"},
            ),
            id="langsmith",
        ),
        pytest.param(
            (
                braintrust,
                "project_id",
                "project_id",
                {
                    "id": "root",
                    "span_id": "root",
                    "root_span_id": "trace-1",
                    "span_parents": [],
                    "span_attributes": {"name": "root", "type": "task"},
                },
            ),
            id="braintrust",
        ),
        pytest.param(
            (
                logfire,
                "project_id",
                "project_id",
                {"trace_id": "trace-1", "span_id": "root", "kind": "span"},
            ),
            id="logfire",
        ),
        pytest.param(
            (
                phoenix,
                "project",
                "project",
                {"context": {"trace_id": "trace-1", "span_id": "root"}},
            ),
            id="phoenix",
        ),
    ]
)
def provider(
    request: pytest.FixtureRequest,
) -> tuple[ModuleType, str, str, dict[str, Any]]:
    """Provide a parser, its identity fields, and one minimal trace."""
    return request.param


def test_explicit_source_wins_and_is_trimmed(
    provider: tuple[ModuleType, str, str, dict[str, Any]],
) -> None:
    """Choose the explicit namespace over the alias and embedded project."""
    parser, alias, embedded_field, record = provider
    payload = json.dumps({**record, embedded_field: "embedded"}).encode()

    [session] = parser.parse(payload, {"source_instance": " explicit ", alias: "alias"})

    assert isinstance(session, ImportedSession)
    assert session.external_id == "explicit:trace-1"


@pytest.mark.parametrize("empty", [None, "", " \t "])
def test_alias_wins_when_explicit_source_is_empty(
    provider: tuple[ModuleType, str, str, dict[str, Any]], empty: str | None
) -> None:
    """Use the trimmed provider alias ahead of an embedded project."""
    parser, alias, embedded_field, record = provider
    payload = json.dumps({**record, embedded_field: "embedded"}).encode()

    [session] = parser.parse(payload, {"source_instance": empty, alias: " alias "})

    assert isinstance(session, ImportedSession)
    assert session.external_id == "alias:trace-1"


def test_embedded_project_is_trimmed(
    provider: tuple[ModuleType, str, str, dict[str, Any]],
) -> None:
    """Use embedded identity when neither parameter supplies one."""
    parser, _, embedded_field, record = provider
    payload = json.dumps({**record, embedded_field: " embedded "}).encode()

    [session] = parser.parse(payload, {})

    assert isinstance(session, ImportedSession)
    assert session.external_id == "embedded:trace-1"


def test_missing_identity_never_uses_filename_or_constant(
    provider: tuple[ModuleType, str, str, dict[str, Any]],
) -> None:
    """Require source identity with an actionable remedy for every provider."""
    parser, _, _, record = provider

    [failure] = parser.parse(json.dumps(record).encode(), {"filename": "export.jsonl"})

    assert isinstance(failure, ImportFailure)
    assert "--params" in failure.error
    assert "source_instance" in failure.error


def test_embedded_conflict_cannot_be_hidden_by_explicit_source(
    provider: tuple[ModuleType, str, str, dict[str, Any]],
) -> None:
    """Reject a mixed-project trace before applying any namespace override."""
    parser, _, embedded_field, record = provider
    other = {**record, embedded_field: "second-project"}
    for key in ("id", "span_id"):
        if key in other:
            other[key] = "child"
    if "context" in other:
        other["context"] = {**other["context"], "span_id": "child"}
    payload = json.dumps([{**record, embedded_field: "first-project"}, other]).encode()

    [failure] = parser.parse(payload, {"source_instance": "explicit"})

    assert isinstance(failure, ImportFailure)
    assert "conflicting" in failure.error.lower()


@pytest.mark.parametrize("invalid", [42, False, [], {}])
@pytest.mark.parametrize("location", ["source_instance", "alias", "embedded"])
def test_nonstring_identity_is_rejected_even_with_an_override(
    provider: tuple[ModuleType, str, str, dict[str, Any]],
    location: str,
    invalid: Any,
) -> None:
    """Do not stringify malformed identity values or hide them behind an override."""
    parser, alias, embedded_field, record = provider
    record = {**record, embedded_field: "embedded"}
    params = {"source_instance": "explicit", alias: "alias"}
    if location == "embedded":
        record[embedded_field] = invalid
    else:
        params[alias if location == "alias" else "source_instance"] = invalid

    [failure] = parser.parse(json.dumps(record).encode(), params)

    assert isinstance(failure, ImportFailure)
    assert "string" in failure.error


def test_native_jsonl_keeps_the_supplied_external_id() -> None:
    """Native sessions already contain their final identity and need no prefix."""
    payload = json.dumps(
        {
            "external_id": "already-qualified",
            "status": "completed",
            "inputs": {},
            "outputs": {},
            "nodes": [],
        }
    ).encode()

    [session] = parse_jsonl(payload, {"source_instance": "ignored"})

    assert isinstance(session, ImportedSession)
    assert session.external_id == "already-qualified"
