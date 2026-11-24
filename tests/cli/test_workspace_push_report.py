#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Workspace push requests, report, and command errors."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from tests.cli.workspace_push_fakes import (
    DESTINATION_URL,
    SOURCE_URL,
    build_counts,
)

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionDetailResponse,
    SessionResponse,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeCreateRequest,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.cli import app as app_module
from kitaru.cli import workspace_push, workspace_push_destination
from kitaru.cli.config import ResolvedCredential
from kitaru.cli.output import CLIError
from kitaru.cli.schema import describe_schema
from kitaru.cli.workspace_push import (
    PUSH_IMPORTED_FROM,
    build_ingest_request,
    get_session_identity,
)
from kitaru.cli.workspace_push_report import (
    NOT_COPIED,
    PushReport,
    build_push_result,
    describe_not_copied,
)


def test_session_identity_keeps_source_identity_and_fills_gaps() -> None:
    """The copy keeps a complete source identity and derives one otherwise."""
    session_id = uuid.uuid4()

    def build(**fields: Any) -> SessionResponse:
        return SessionResponse.model_construct(id=session_id, **fields)

    assert get_session_identity(
        build(imported_from="langfuse", external_id="trace-42")
    ) == ("langfuse", "trace-42")
    assert get_session_identity(build(imported_from=None, external_id=None)) == (
        PUSH_IMPORTED_FROM,
        str(session_id),
    )
    assert get_session_identity(build(imported_from=None, external_id="run-7")) == (
        PUSH_IMPORTED_FROM,
        str(session_id),
    )
    assert get_session_identity(build(imported_from="langfuse", external_id=None)) == (
        PUSH_IMPORTED_FROM,
        str(session_id),
    )


def test_push_result_fails_when_a_record_failed() -> None:
    """A failed record turns the report into a retryable partial failure."""
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    report.count("sessions", "copied", 3)
    report.fail("sessions", "abc", "HTTP 500: boom", retryable=True)
    report.warnings.append("A warning raised while copying.")
    report.add_incomplete_connection(
        "langfuse-prod", {"SECRET_KEY"}, set(), make_default=False
    )

    with pytest.raises(CLIError) as raised:
        build_push_result(report)

    details = raised.value.details
    assert raised.value.kind == "partial_failure"
    assert raised.value.retryable is True
    assert details["counts"]["sessions"] == build_counts(copied=3, failed=1)
    assert details["failed"] == [
        {
            "kind": "sessions",
            "record": "abc",
            "error": "HTTP 500: boom",
            "retryable": True,
        }
    ]
    assert details["warnings"][0] == "A warning raised while copying."
    assert any("incomplete_connections" in warning for warning in details["warnings"])
    assert any("--set-secret SECRET_KEY=<value>" in a for a in details["next_actions"])
    assert describe_not_copied() in details["next_actions"][-1]


def test_push_result_is_not_retryable_after_a_permanent_failure() -> None:
    """A failure a rerun cannot fix makes the whole partial failure permanent."""
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    report.fail("sessions", "abc", "HTTP 503: busy", retryable=True)
    report.fail("connections", "langfuse-prod", "Provider differs.", retryable=False)

    with pytest.raises(CLIError) as raised:
        build_push_result(report)

    assert raised.value.retryable is False
    assert raised.value.hint is not None
    assert raised.value.hint.startswith("Fix the failures")


def test_push_command_prints_the_report_of_a_partial_failure_as_text(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Text mode shows the counts, failures, and follow-up commands of a failure."""
    serve_info(
        monkeypatch,
        {SOURCE_URL: None, DESTINATION_URL: None},
        destination_scheme=AuthScheme.NONE,
    )
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    report.count("sessions", "copied", 3)
    report.fail("sessions", "abc", "HTTP 500: boom", retryable=True)
    report.add_incomplete_connection(
        "langfuse-prod", {"SECRET_KEY"}, set(), make_default=True
    )

    async def return_report(*_args: Any, **_kwargs: Any) -> PushReport:
        return report

    monkeypatch.setattr(workspace_push, "push_workspace", return_report)

    code = run_push("text")

    err = capsys.readouterr().err
    assert code == 8
    assert "1 record(s) failed to copy" in err
    assert "Failed sessions abc: HTTP 500: boom" in err
    assert "sessions" in err and "3" in err
    assert "--set-secret SECRET_KEY=<value> --default" in err
    assert "Rerun the same command" in err


def test_not_copied_list_is_the_same_everywhere() -> None:
    """The help, the report, and the guide name the same uncopied records."""
    result = build_push_result(PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False))
    [schema] = describe_schema(("workspace", "push"))
    guide = Path(__file__).parents[2] / "docs/book/guides/push-workspace.md"
    sentence = describe_not_copied()

    assert result.item["not_copied"] == list(NOT_COPIED)
    assert sentence in result.next_actions[-1]
    assert sentence in schema["description"]
    assert sentence.capitalize() in guide.read_text(encoding="utf-8")
    assert DESTINATION_URL in result.next_actions[0]
    assert "--copy-secrets" not in {item["name"] for item in schema["parameters"]}


def test_ingest_request_repeats_every_field_of_the_source() -> None:
    """No session or node field is dropped between the source and its copy."""
    started = datetime(2026, 7, 22, 10, tzinfo=UTC)
    session = SessionDetailResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "created": started,
            "updated": started,
            "owner_id": uuid.uuid4(),
            "agent_id": uuid.uuid4(),
            "number": 7,
            "agent_version_id": uuid.uuid4(),
            "task_id": uuid.uuid4(),
            "import_id": uuid.uuid4(),
            "origin": "imported",
            "status": "failed",
            "name": "Weather request",
            "error": "boom",
            "started_at": started,
            "ended_at": started + timedelta(seconds=1),
            "external_id": "trace-42",
            "metadata": {"environment": "production"},
            "imported_from": "langfuse",
            "framework": "pydantic-ai",
            "adapter_version": "1.2.3",
            "cost": "0.5",
            "tokens": {"input_tokens": 3},
            "llm_call_count": 1,
            "tool_call_count": 0,
            "input_text_selector": "/question",
            "output_text_selector": "/answer",
            "inputs": {"question": "weather?"},
            "outputs": {"answer": "rain"},
        }
    )
    node = SessionNodeResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "created": started,
            "updated": started,
            "session_id": session.id,
            "external_id": "child",
            "parent_external_id": "root",
            "links": [{"external_id": "root", "kind": "follows"}],
            "trace_id": "trace-42",
            "node_type": "tool_call",
            "name": "lookup",
            "status": "failed",
            "error": "timeout",
            "started_at": started,
            "ended_at": started + timedelta(seconds=1),
            "input_text_selector": "/query",
            "output_text_selector": "/result",
            "system_prompt_selector": "/system",
            "reasoning_selectors": ["/reasoning"],
            "inputs": {"query": "delft"},
            "outputs": {"result": "rain"},
            "requested_model": "claude-haiku",
            "model": "claude-haiku-4-5",
            "model_provider": "anthropic",
            "tokens": {"input_tokens": 3, "output_tokens": 1},
            "cost": "0.25",
            "model_params": {"temperature": 0},
            "tool_name": "weather",
            "subagent_id": "sub-1",
            "cache_key": "cache",
            "attributes": {"span": "child"},
            "metadata": {"source": "langfuse"},
        }
    )
    agent_id, agent_version_id = uuid.uuid4(), uuid.uuid4()

    request = build_ingest_request(
        SessionWithNodesResponse(session=session, nodes=[node]),
        agent_id,
        agent_version_id,
    )

    assert set(SessionCreateRequest.model_fields) <= set(
        SessionDetailResponse.model_fields
    )
    assert set(SessionNodeCreateRequest.model_fields) <= set(
        SessionNodeResponse.model_fields
    )
    overrides = {"agent_id": agent_id, "agent_version_id": agent_version_id}
    for name in SessionCreateRequest.model_fields:
        expected = overrides.get(name, getattr(session, name))
        assert expected is not None, name
        assert getattr(request.session, name) == expected, name
    [copied] = request.nodes
    for name in SessionNodeCreateRequest.model_fields:
        assert getattr(node, name) not in (None, [], {}), name
        assert getattr(copied, name) == getattr(node, name), name
    sent = request.model_dump(mode="json", exclude_unset=True)
    assert set(sent["session"]) == set(SessionCreateRequest.model_fields)
    assert set(sent["nodes"][0]) == set(SessionNodeCreateRequest.model_fields)


def test_report_bounds_its_detail_lists() -> None:
    """Counts stay complete when more records are skipped than are listed."""
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    for index in range(250):
        report.skip("sessions", str(index), "The session is still in progress.")

    assert report.counts["sessions"]["skipped"] == 250
    assert len(report.skipped) == 200
    assert report.details_truncated is True


def serve_info(
    monkeypatch: pytest.MonkeyPatch,
    ids: dict[str, uuid.UUID | None],
    destination_scheme: AuthScheme = AuthScheme.LOCAL,
) -> None:
    """Answer the info endpoint of the source and destination without a server."""

    async def get_info(_client: Any, server_url: str) -> ServerInfoResponse:
        scheme = AuthScheme.LOCAL
        if server_url == DESTINATION_URL:
            scheme = destination_scheme
        return ServerInfoResponse(
            id=ids[server_url], version="1.0.0", auth_scheme=scheme
        )

    monkeypatch.setattr(workspace_push_destination, "get_server_info", get_info)


def run_push(output: str = "json") -> int:
    """Run the push command from the source to the destination."""
    return app_module.main(
        [
            "--output",
            output,
            "--server",
            SOURCE_URL,
            "workspace",
            "push",
            "--to",
            DESTINATION_URL,
        ]
    )


def test_push_command_requires_a_stored_destination_credential(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The source's environment key is never used for the destination."""
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_source")
    serve_info(monkeypatch, {SOURCE_URL: uuid.uuid4(), DESTINATION_URL: uuid.uuid4()})

    code = run_push()

    error = json.loads(capsys.readouterr().err)["error"]
    assert code == 3
    assert error["kind"] == "authentication_failed"
    assert DESTINATION_URL in error["message"]
    assert f"kitaru login {DESTINATION_URL}" in error["hint"]


def test_push_command_needs_no_credential_for_an_open_destination(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A destination without authentication is pushed to without a login."""
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_source")
    serve_info(
        monkeypatch,
        {SOURCE_URL: uuid.uuid4(), DESTINATION_URL: uuid.uuid4()},
        destination_scheme=AuthScheme.NONE,
    )
    build_client = workspace_push_destination.build_api_client
    credentials: list[str] = []

    def record_client(
        server_url: str, credential: ResolvedCredential, *args: Any
    ) -> Any:
        credentials.append(credential.source)
        return build_client(server_url, credential, *args)

    async def push_nothing(*_args: Any, **_kwargs: Any) -> PushReport:
        return PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)

    monkeypatch.setattr(workspace_push_destination, "build_api_client", record_client)
    monkeypatch.setattr(workspace_push, "push_workspace", push_nothing)

    code = run_push()

    assert code == 0, capsys.readouterr().err
    assert credentials == ["none", "none"]


def test_push_command_rejects_another_url_of_the_source_server(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two URLs reporting one server id are the same server."""
    server_id = uuid.uuid4()
    serve_info(monkeypatch, {SOURCE_URL: server_id, DESTINATION_URL: server_id})

    code = run_push()

    error = json.loads(capsys.readouterr().err)["error"]
    assert code == 2
    assert error["kind"] == "invalid_arguments"
    assert str(server_id) in error["message"]


def test_push_command_rejects_the_source_as_destination(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Pushing a server to itself is refused."""
    code = app_module.main(
        [
            "--output",
            "json",
            "--server",
            SOURCE_URL,
            "workspace",
            "push",
            "--to",
            f"{SOURCE_URL}/",
        ]
    )

    assert code == 2
    assert json.loads(capsys.readouterr().err)["error"]["kind"] == "invalid_arguments"
