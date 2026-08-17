#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Standalone replay CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import replays
from kitaru.cli.output import CLIError


class StubReplayClient:
    """Protocol-shaped client recording replay and resolution calls."""

    def __init__(self) -> None:
        self.agent = SimpleNamespace(
            id=uuid.uuid4(), name="assistant", latest_version=2
        )
        self.agent_version = SimpleNamespace(
            id=uuid.uuid4(), agent_id=self.agent.id, version=2
        )
        self.evaluator = SimpleNamespace(
            id=uuid.uuid4(), name="quality", latest_version=3
        )
        self.evaluator_version = SimpleNamespace(
            id=uuid.uuid4(), evaluator_id=self.evaluator.id, version=3
        )
        self.replay_id = uuid.uuid4()
        self.job_id = uuid.uuid4()
        self.baseline_id = uuid.uuid4()
        now = datetime.now(UTC)
        self.replay = ReplayResponse(
            id=self.replay_id,
            job_id=self.job_id,
            experiment_run_id=None,
            baseline_session_id=self.baseline_id,
            result_session_id=None,
            override=None,
            tool_policy={"default": {"type": "passthrough"}, "tools": {}},
            evaluators=[{"evaluator": "quality", "version": 3, "params": {}}],
            evaluate_baselines=False,
            status=ReplayStatus.PENDING,
            error=None,
            created=now,
            updated=now,
        )
        self.create_calls: list[ReplayCreateRequest] = []
        self.list_calls: list[ReplayListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.agents = self._Agents(self)
        self.evaluators = self._Evaluators(self)
        self.replays = self._Replays(self)

    class _Agents:
        def __init__(self, owner: "StubReplayClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

        async def get(self, agent_id: uuid.UUID) -> Any:
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def iter_versions(self, agent_id: uuid.UUID):
            assert agent_id == self.owner.agent.id
            yield self.owner.agent_version

    class _Evaluators:
        def __init__(self, owner: "StubReplayClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            return SimpleNamespace(items=[self.owner.evaluator], next_cursor=None)

        async def get(self, evaluator_id: uuid.UUID) -> Any:
            assert evaluator_id == self.owner.evaluator.id
            return self.owner.evaluator

        async def get_version(self, evaluator_id: uuid.UUID, version: int) -> Any:
            assert evaluator_id == self.owner.evaluator.id
            assert version == self.owner.evaluator_version.version
            return self.owner.evaluator_version

    class _Replays:
        def __init__(self, owner: "StubReplayClient") -> None:
            self.owner = owner

        async def create(self, request: ReplayCreateRequest) -> ReplayResponse:
            self.owner.create_calls.append(request)
            return self.owner.replay

        async def list(self, params: ReplayListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.replay], next_cursor="next")

        async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
            self.owner.get_calls.append(replay_id)
            return self.owner.replay


async def test_create_forwards_all_supported_fields_and_next_actions() -> None:
    """Create resolves exact versions and returns the finite follow-up commands."""
    client = StubReplayClient()

    result = await replays.create_replay(
        client,
        client.baseline_id,
        evaluators=["quality@3"],
        evaluator_params=['quality@3={"threshold":0.8}'],
        agent="assistant@2",
        override='{"model":"gpt-5"}',
        tool_policy=(
            '{"default":{"type":"history","scope":"baseline",'
            '"on_miss":"fail"},"tools":{}}'
        ),
        evaluate_baselines=True,
    )

    [request] = client.create_calls
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "baseline_session_id": str(client.baseline_id),
        "agent_version_id": str(client.agent_version.id),
        "override": {"model": "gpt-5"},
        "tool_policy": {
            "default": {"type": "history", "scope": "baseline", "on_miss": "fail"},
            "tools": {},
        },
        "evaluators": [
            {"evaluator": "quality", "version": 3, "params": {"threshold": 0.8}}
        ],
        "evaluate_baselines": True,
    }
    assert result.item["id"] == str(client.replay_id)
    assert result.item["job_id"] == str(client.job_id)
    assert result.next_actions == [
        f"kitaru job watch {client.job_id}",
        f"kitaru job cancel {client.job_id}",
        f"kitaru replay get {client.replay_id}",
    ]


async def test_create_omits_server_decided_optional_fields() -> None:
    """An omitted agent, override, and policy remain unset on the wire model."""
    client = StubReplayClient()

    await replays.create_replay(
        client,
        client.baseline_id,
        evaluators=["quality@3"],
        evaluator_params=None,
        agent=None,
        override=None,
        tool_policy=None,
        evaluate_baselines=False,
    )

    [request] = client.create_calls
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "baseline_session_id": str(client.baseline_id),
        "evaluators": [{"evaluator": "quality", "version": 3, "params": {}}],
        "evaluate_baselines": False,
    }


@pytest.mark.parametrize(
    ("evaluators", "evaluator_params", "override", "message"),
    [
        ([], None, None, "Provide at least one --evaluator"),
        (["quality@3", "quality@3"], None, None, "must be unique"),
        (["quality@3"], ["other@1={}"], None, "is not a selected evaluator"),
        (["quality@3"], ["quality@3"], None, "must be EVALUATOR@VERSION"),
        (["quality@3"], ["quality@3={"], None, "is not valid JSON"),
        (["quality@3"], None, "[]", "must contain a JSON object"),
    ],
)
async def test_invalid_create_inputs_do_not_mutate(
    evaluators: list[str],
    evaluator_params: list[str] | None,
    override: str | None,
    message: str,
) -> None:
    """Local input failures occur before replay creation."""
    client = StubReplayClient()

    with pytest.raises(CLIError, match=message):
        await replays.create_replay(
            client,
            client.baseline_id,
            evaluators=evaluators,
            evaluator_params=evaluator_params,
            agent=None,
            override=override,
            tool_policy=None,
            evaluate_baselines=False,
        )

    assert client.create_calls == []


async def test_create_rejects_tokens_resolving_to_same_evaluator_version() -> None:
    """Aliases cannot make one evaluator version run twice."""
    client = StubReplayClient()

    with pytest.raises(CLIError, match="resolved to the same evaluator version"):
        await replays.create_replay(
            client,
            client.baseline_id,
            evaluators=["quality@3", f"{client.evaluator.id}@3"],
            evaluator_params=None,
            agent=None,
            override=None,
            tool_policy=None,
            evaluate_baselines=False,
        )

    assert client.create_calls == []


async def test_list_and_get_preserve_sdk_results() -> None:
    """Finite reads forward pagination and do not remap replay state."""
    client = StubReplayClient()

    listed = await replays.list_replays(
        client,
        size=7,
        cursor="cursor",
        sort="created:asc",
        filter='{"field":"status","op":"eq","value":"pending"}',
    )
    pending = await replays.get_replay(client, client.replay_id)
    client.replay.status = ReplayStatus.COMPLETED
    completed = await replays.get_replay(client, client.replay_id)

    [params] = client.list_calls
    assert params.model_dump(mode="json", exclude_unset=True) == {
        "cursor": "cursor",
        "size": 7,
        "sort": "created:asc",
        "filter": '{"field": "status", "op": "eq", "value": "pending"}',
    }
    assert listed.page == {"limit": 7, "next_cursor": "next", "truncated": True}
    assert pending.item["status"] == "pending"
    assert completed.item["status"] == "completed"
    assert client.get_calls == [client.replay_id, client.replay_id]


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubReplayClient:
    """Route public replay commands through one recording client."""
    client = StubReplayClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_replay_argv_covers_all_leaves(
    argv_client: StubReplayClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The public root exposes create, list, and exact get commands."""
    client = argv_client

    assert (
        app_module.main(
            [
                "replay",
                "create",
                str(client.baseline_id),
                "--evaluator",
                "quality@3",
                "--output",
                "json",
            ]
        )
        == 0
    )
    created = json.loads(capsys.readouterr().out)
    assert created["command"] == "replay.create"
    assert created["item"]["job_id"] == str(client.job_id)

    assert app_module.main(["replay", "list", "--size", "7"]) == 0
    assert json.loads(capsys.readouterr().out)["command"] == "replay.list"

    assert app_module.main(["replay", "get", str(client.replay_id)]) == 0
    assert json.loads(capsys.readouterr().out)["item"]["status"] == "pending"


def test_public_invalid_create_is_structured_and_does_not_mutate(
    argv_client: StubReplayClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Malformed inline JSON fails locally before replay creation."""
    client = argv_client

    assert (
        app_module.main(
            [
                "replay",
                "create",
                str(client.baseline_id),
                "--evaluator",
                "quality@3",
                "--tool-policy",
                "{",
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "replay.create"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.create_calls == []
