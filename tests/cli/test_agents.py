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
"""Agent registration without --command, version update, and soft-delete tests."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.cli.output_rendering import render_text

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionResponse,
    AgentVersionUpdateRequest,
    RunSpec,
    RuntimeCapabilities,
)
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context
from kitaru.cli.output import CommandResult
from kitaru.client.exceptions import APIError
from kitaru.server.domain.agent_version import AgentVersionWithoutRunSpec


def _agent(name: str = "support-agent", *, latest_version: int = 1) -> AgentResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return AgentResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name=name,
        description="Support agent",
        latest_version=latest_version,
        created=now,
        updated=now,
    )


def _agent_version(
    agent: AgentResponse,
    *,
    version: int = 1,
    run_spec: RunSpec | None = None,
    capabilities: AgentCapabilities | None = None,
    display_version: str | None = None,
    description: str | None = None,
) -> AgentVersionResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return AgentVersionResponse(
        id=uuid.uuid4(),
        owner_id=agent.owner_id,
        agent_id=agent.id,
        version=version,
        display_version=display_version,
        description=description,
        run_spec=run_spec,
        capabilities=capabilities or AgentCapabilities(),
        created=now,
        updated=now,
    )


class StubAgentClient:
    """Fake SDK client covering agents, agent_versions, sessions, and cohorts."""

    def __init__(
        self,
        agent: AgentResponse | None = None,
        versions: list[AgentVersionResponse] | None = None,
    ) -> None:
        self.agent = agent or _agent()
        self.versions = versions if versions is not None else []
        self.created_agents: list[Any] = []
        self.created_versions: list[tuple[uuid.UUID, Any]] = []
        self.updated_versions: list[tuple[uuid.UUID, AgentVersionUpdateRequest]] = []
        self.deleted_agents: list[uuid.UUID] = []
        self.session_items: list[Any] = []
        self.session_next_cursor: str | None = None
        self.cohort_items: list[Any] = []
        self.cohort_next_cursor: str | None = None
        self.session_list_calls: list[Any] = []
        self.cohort_list_calls: list[Any] = []
        self.session_list_error: Exception | None = None
        self.cohort_list_error: Exception | None = None

        self.agents = self._Agents(self)
        self.agent_versions = self._AgentVersions(self)
        self.sessions = self._Sessions(self)
        self.cohorts = self._Cohorts(self)

    class _Agents:
        def __init__(self, owner: "StubAgentClient") -> None:
            self.owner = owner

        async def create(self, request: Any, idempotency_key: str | None = None):
            del idempotency_key
            self.owner.created_agents.append(request)
            return self.owner.agent

        async def get(self, agent_id: uuid.UUID) -> AgentResponse:
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def list(self, params: Any = None) -> Any:
            del params
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

        async def create_version(
            self,
            agent_id: uuid.UUID,
            request: Any,
            idempotency_key: str | None = None,
        ) -> AgentVersionResponse:
            del idempotency_key
            self.owner.created_versions.append((agent_id, request))
            version = _agent_version(
                self.owner.agent,
                version=len(self.owner.versions) + 1,
                run_spec=request.run_spec,
                capabilities=request.capabilities,
                display_version=request.display_version,
                description=request.description,
            )
            self.owner.versions.append(version)
            return version

        async def iter_versions(self, agent_id: uuid.UUID):
            assert agent_id == self.owner.agent.id
            for version in self.owner.versions:
                yield version

        async def delete(self, agent_id: uuid.UUID) -> None:
            self.owner.deleted_agents.append(agent_id)

    class _AgentVersions:
        def __init__(self, owner: "StubAgentClient") -> None:
            self.owner = owner

        async def update(
            self, agent_version_id: uuid.UUID, request: AgentVersionUpdateRequest
        ) -> AgentVersionResponse:
            self.owner.updated_versions.append((agent_version_id, request))
            current = next(v for v in self.owner.versions if v.id == agent_version_id)
            updates: dict[str, Any] = {}
            for field_name in request.model_fields_set:
                value = getattr(request, field_name)
                if field_name == "capabilities" and value is None:
                    value = AgentCapabilities()
                updates[field_name] = value
            updated = current.model_copy(update=updates)
            self.owner.versions = [
                updated if v.id == agent_version_id else v for v in self.owner.versions
            ]
            return updated

    class _Sessions:
        def __init__(self, owner: "StubAgentClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            self.owner.session_list_calls.append(params)
            if self.owner.session_list_error is not None:
                raise self.owner.session_list_error
            return SimpleNamespace(
                items=self.owner.session_items,
                next_cursor=self.owner.session_next_cursor,
            )

    class _Cohorts:
        def __init__(self, owner: "StubAgentClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            self.owner.cohort_list_calls.append(params)
            if self.owner.cohort_list_error is not None:
                raise self.owner.cohort_list_error
            return SimpleNamespace(
                items=self.owner.cohort_items,
                next_cursor=self.owner.cohort_next_cursor,
            )


def _patch_client(monkeypatch: pytest.MonkeyPatch, client: StubAgentClient) -> None:
    @asynccontextmanager
    async def open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", open_client)


def test_agent_register_without_command_creates_version_with_null_run_spec(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Import-only agents can register without --command."""
    client = StubAgentClient()
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "register",
                "support-agent",
                "--description",
                "Imported traces only",
                "--tool",
                "lookup_order",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.register"
    assert payload["item"]["version"]["run_spec"] is None
    _, version_request = client.created_versions[0]
    assert version_request.run_spec is None
    assert version_request.capabilities.tools == ["lookup_order"]


def test_agent_version_register_without_command_creates_version_with_null_run_spec(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """New versions can also omit --command when no run-spec flags are passed."""
    client = StubAgentClient()
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "register",
                "support-agent",
                "--display-version",
                "imported-v2",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.version.register"
    assert payload["item"]["version"]["run_spec"] is None
    assert payload["item"]["version"]["display_version"] == "imported-v2"


@pytest.mark.parametrize(
    ("args", "expected_message"),
    [
        (["--working-dir", "/tmp/work"], "--working-dir requires --command."),
        (["--env", "FOO=bar"], "--env requires --command."),
        (
            ["--secret-id", str(uuid.uuid4())],
            "--secret-id requires --command.",
        ),
        (["--timeout-seconds", "120"], "--timeout-seconds requires --command."),
        (
            ["--working-dir", "/tmp/work", "--env", "FOO=bar"],
            "--working-dir, --env require --command.",
        ),
    ],
)
def test_agent_register_rejects_run_spec_flags_without_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    expected_message: str,
) -> None:
    """Run-spec-only flags still require --command before any API call."""
    client = StubAgentClient()
    _patch_client(monkeypatch, client)

    assert app_module.main(["agent", "register", "support-agent", *args]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert error["message"] == expected_message
    assert client.created_agents == []


def test_agent_register_spec_allows_omitted_run_spec(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """YAML spec registration allows omitting version.run_spec."""
    spec_file = tmp_path / "agent.yaml"
    spec_file.write_text(
        "parent:\n  description: Import only\nversion:\n  display_version: v1\n",
        encoding="utf-8",
    )
    client = StubAgentClient()
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            ["agent", "register", "support-agent", "--spec", str(spec_file)]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["version"]["run_spec"] is None
    assert payload["item"]["version"]["display_version"] == "v1"


def test_agent_version_without_run_spec_error_is_actionable() -> None:
    """Server domain error explains how to add a run spec before replaying."""
    version_id = uuid.uuid4()
    error = AgentVersionWithoutRunSpec(version_id)
    assert f"Agent version {version_id} has no run spec" in str(error)
    assert "--command" in str(error)


def test_agent_version_update_merges_partial_run_spec_and_capabilities(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Updating --working-dir preserves existing command, env, timeout, and hooks."""
    agent = _agent("support-agent", latest_version=3)
    secret_id = uuid.uuid4()
    existing_spec = RunSpec(
        command="python support.py",
        working_dir="./releases/1.6.2",
        env={"MODE": "prod"},
        secret_ids=[secret_id],
        runtime_capabilities=RuntimeCapabilities(overrides=False, tool_policies=True),
        timeout_seconds=900,
    )
    existing = _agent_version(
        agent,
        version=3,
        run_spec=existing_spec,
        capabilities=AgentCapabilities(
            tools=["lookup"], mcp_servers=["crm"], skills=["triage"]
        ),
    )
    client = StubAgentClient(agent, [existing])
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "update",
                "support-agent@3",
                "--working-dir",
                "./releases/1.6.3",
                "--tool",
                "lookup",
                "--tool",
                "refund",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.version.update"
    assert payload["item"]["previous_run_spec"]["working_dir"] == "./releases/1.6.2"
    assert payload["item"]["run_spec"]["working_dir"] == "./releases/1.6.3"
    assert payload["item"]["run_spec"]["command"] == "python support.py"
    assert payload["item"]["run_spec"]["env"] == {"MODE": "prod"}
    assert payload["item"]["run_spec"]["secret_ids"] == [str(secret_id)]
    assert payload["item"]["run_spec"]["timeout_seconds"] == 900
    assert payload["item"]["run_spec"]["runtime_capabilities"] == {
        "overrides": False,
        "tool_policies": True,
    }
    assert payload["item"]["capabilities"] == {
        "tools": ["lookup", "refund"],
        "mcp_servers": ["crm"],
        "skills": ["triage"],
    }

    _, sent_request = client.updated_versions[0]
    assert sent_request.model_fields_set == {"run_spec", "capabilities"}


def test_agent_version_update_adds_run_spec_when_previously_none(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An import-only version can gain a run spec in place with --command."""
    agent = _agent("support-agent")
    existing = _agent_version(agent, version=1, run_spec=None)
    client = StubAgentClient(agent, [existing])
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "update",
                "support-agent@1",
                "--command",
                "python support.py",
                "--working-dir",
                "./src",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["previous_run_spec"] is None
    assert payload["item"]["run_spec"]["command"] == "python support.py"
    assert payload["item"]["run_spec"]["working_dir"] == "./src"


def test_agent_version_update_rejects_run_spec_flag_without_existing_run_spec(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Setting --working-dir on a version with no run_spec requires --command."""
    agent = _agent("support-agent")
    existing = _agent_version(agent, version=1, run_spec=None)
    client = StubAgentClient(agent, [existing])
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "update",
                "support-agent@1",
                "--working-dir",
                "./src",
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "--working-dir requires --command" in error["message"]
    assert "no existing run spec" in error["message"]
    assert client.updated_versions == []


def test_agent_version_update_supports_spec_file_and_rejects_conflicts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """--spec updates runtime_capabilities cleanly and conflicts with direct flags."""
    spec_file = tmp_path / "update.yaml"
    spec_file.write_text(
        "run_spec:\n"
        "  command: python agent.py\n"
        "  runtime_capabilities:\n"
        "    overrides: false\n"
        "    tool_policies: false\n",
        encoding="utf-8",
    )
    agent = _agent("support-agent")
    existing = _agent_version(
        agent, version=1, run_spec=RunSpec(command="python old.py")
    )
    client = StubAgentClient(agent, [existing])
    _patch_client(monkeypatch, client)

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "update",
                "support-agent@1",
                "--spec",
                str(spec_file),
                "--working-dir",
                "./other",
            ]
        )
        == 2
    )
    assert "--spec conflicts" in json.loads(capsys.readouterr().err)["error"]["message"]
    assert client.updated_versions == []

    assert (
        app_module.main(
            [
                "agent",
                "version",
                "update",
                "support-agent@1",
                "--spec",
                str(spec_file),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["previous_run_spec"]["command"] == "python old.py"
    assert payload["item"]["run_spec"]["command"] == "python agent.py"
    assert payload["item"]["run_spec"]["runtime_capabilities"] == {
        "overrides": False,
        "tool_policies": False,
    }


def test_agent_version_update_rejects_empty_update(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Both bare update commands and empty spec files fail before API mutation."""
    client = StubAgentClient()
    _patch_client(monkeypatch, client)

    assert app_module.main(["agent", "version", "update", "support-agent@1"]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "Select at least one" in error["message"]

    empty_spec = tmp_path / "empty.yaml"
    empty_spec.write_text("{}\n", encoding="utf-8")
    assert (
        app_module.main(
            ["agent", "version", "update", "support-agent@1", "--spec", str(empty_spec)]
        )
        == 2
    )
    assert (
        "at least one field" in json.loads(capsys.readouterr().err)["error"]["message"]
    )
    assert client.updated_versions == []


def test_agent_version_update_renders_before_and_after_run_specs() -> None:
    """Human detail view displays both previous and updated run specs."""
    stdout, _ = render_text(
        "agent.version.update",
        CommandResult(
            item={
                "id": str(uuid.uuid4()),
                "version": 3,
                "previous_run_spec": {"command": "python old.py"},
                "run_spec": {"command": "python new.py"},
            }
        ),
        rich=True,
    )
    assert "Previous run spec" in stdout
    assert "Updated run spec" in stdout
    assert "python old.py" in stdout
    assert "python new.py" in stdout


def test_agent_delete_reports_retained_counts_warnings_and_cleanup_actions(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Soft deleting an agent surfaces its old ID, retained counts, and actions."""
    agent = _agent("support-agent")
    client = StubAgentClient(agent)
    client.session_items = [SimpleNamespace(id=uuid.uuid4()) for _ in range(3)]
    client.session_next_cursor = "more-sessions"
    client.cohort_items = [SimpleNamespace(id=uuid.uuid4())]
    _patch_client(monkeypatch, client)

    assert app_module.main(["agent", "delete", "support-agent", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.delete"
    assert payload["item"] == {
        "id": str(agent.id),
        "name": "support-agent",
        "deleted": True,
        "soft_deleted": True,
        "retained_sessions": 3,
        "retained_sessions_truncated": True,
        "retained_cohorts": 1,
        "retained_cohorts_truncated": False,
    }
    assert any("More than 1000 sessions" in w for w in payload["warnings"])
    expected_filter = f'{{"field":"agent_id","op":"eq","value":"{agent.id}"}}'
    assert (
        f"kitaru session delete --filter '{expected_filter}' --force"
        in payload["next_actions"]
    )
    assert f"kitaru cohort list --filter '{expected_filter}'" in payload["next_actions"]
    assert client.deleted_agents == [agent.id]


def test_agent_delete_preserves_receipt_when_retention_queries_fail(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Retention-count query failures still return the completed deletion receipt."""
    agent = _agent("support-agent")
    client = StubAgentClient(agent)
    client.session_list_error = TimeoutError("timed out")
    client.cohort_list_error = APIError(503, "unavailable")
    _patch_client(monkeypatch, client)

    assert app_module.main(["agent", "delete", "support-agent", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.delete"
    assert payload["item"] == {
        "id": str(agent.id),
        "name": "support-agent",
        "deleted": True,
        "soft_deleted": True,
        "retained_sessions": None,
        "retained_sessions_truncated": None,
        "retained_cohorts": None,
        "retained_cohorts_truncated": None,
    }
    assert any(
        "retained sessions could not be counted" in w for w in payload["warnings"]
    )
    assert any(
        "retained cohorts could not be counted" in w for w in payload["warnings"]
    )
    expected_filter = f'{{"field":"agent_id","op":"eq","value":"{agent.id}"}}'
    assert (
        f"kitaru session delete --filter '{expected_filter}' --force"
        in payload["next_actions"]
    )
    assert f"kitaru cohort list --filter '{expected_filter}'" in payload["next_actions"]
    assert client.deleted_agents == [agent.id]
