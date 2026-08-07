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
"""Experiment configuration and CRUD CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import experiments
from kitaru.cli.output import CLIError
from kitaru.client.exceptions import APIError


@dataclass
class StubModel:
    """Small response exposing the Pydantic serialization surface."""

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.values[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


class StubExperimentClient:
    """Protocol-shaped client recording experiment and evaluator SDK calls."""

    def __init__(self) -> None:
        self.agent = StubModel(uuid.uuid4(), {"name": "assistant"})
        self.quality = SimpleNamespace(
            id=uuid.uuid4(), name="quality", latest_version=2
        )
        self.quality_version = SimpleNamespace(
            id=uuid.uuid4(), evaluator_id=self.quality.id, version=2
        )
        self.experiment = StubModel(
            uuid.uuid4(),
            {
                "name": "regression",
                "description": "Original",
                "override": None,
                "tool_policy": {
                    "default": {"type": "passthrough"},
                    "tools": {},
                },
                "evaluators": [{"evaluator": "quality", "version": 2, "params": {}}],
            },
        )
        self.agent_lookups = 0
        self.evaluator_lookups = 0
        self.experiment_lookups = 0
        self.create_calls: list[ExperimentCreateRequest] = []
        self.list_calls: list[ExperimentListParams] = []
        self.update_calls: list[tuple[uuid.UUID, ExperimentUpdateRequest]] = []
        self.deleted: list[uuid.UUID] = []
        self.update_error: Exception | None = None
        self.agents = self._Agents(self)
        self.evaluators = self._Evaluators(self)
        self.experiments = self._Experiments(self)

    class _Agents:
        def __init__(self, owner: "StubExperimentClient") -> None:
            self.owner = owner

        async def get(self, agent_id: uuid.UUID) -> StubModel:
            self.owner.agent_lookups += 1
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def iter(self):
            self.owner.agent_lookups += 1
            yield self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.agent_lookups += 1
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

    class _Evaluators:
        def __init__(self, owner: "StubExperimentClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.evaluator_lookups += 1
            yield self.owner.quality

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.evaluator_lookups += 1
            return SimpleNamespace(items=[self.owner.quality], next_cursor=None)

        async def get(self, parent_id: uuid.UUID) -> Any:
            self.owner.evaluator_lookups += 1
            if parent_id == self.owner.quality.id:
                return self.owner.quality
            raise AssertionError(f"Unexpected evaluator ID: {parent_id}")

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            self.owner.evaluator_lookups += 1
            if version != 2 or parent_id != self.owner.quality.id:
                raise AssertionError(
                    f"Unexpected evaluator version: {parent_id}@{version}"
                )
            return self.owner.quality_version

    class _Experiments:
        def __init__(self, owner: "StubExperimentClient") -> None:
            self.owner = owner

        async def create(self, request: ExperimentCreateRequest) -> StubModel:
            self.owner.create_calls.append(request)
            return self.owner.experiment

        async def get(self, experiment_id: uuid.UUID) -> StubModel:
            self.owner.experiment_lookups += 1
            assert experiment_id == self.owner.experiment.id
            return self.owner.experiment

        async def iter(self):
            self.owner.experiment_lookups += 1
            yield self.owner.experiment

        async def list(self, params: ExperimentListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.experiment], next_cursor="next")

        async def update(
            self, experiment_id: uuid.UUID, request: ExperimentUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((experiment_id, request))
            if self.owner.update_error is not None:
                raise self.owner.update_error
            return self.owner.experiment

        async def delete(self, experiment_id: uuid.UUID) -> None:
            self.owner.deleted.append(experiment_id)


async def test_create_parses_inline_config_and_pins_exact_evaluators() -> None:
    """Create maps inline JSON and latest references to one exact SDK request."""
    client = StubExperimentClient()

    result = await experiments.create_experiment(
        client,
        "nightly",
        agent=str(client.agent.id),
        description="Nightly regression",
        override='{"model":"gpt-5","model_params":{"temperature":0.1}}',
        tool_policy=(
            '{"default":{"type":"history","scope":"baseline",'
            '"on_miss":"fail"},"tools":{}}'
        ),
        evaluators=["quality@latest"],
        evaluator_params=['quality@latest={"threshold":0.8}'],
    )

    [request] = client.create_calls
    assert isinstance(request, ExperimentCreateRequest)
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "name": "nightly",
        "agent_id": str(client.agent.id),
        "description": "Nightly regression",
        "override": {
            "model": "gpt-5",
            "model_params": {"temperature": 0.1},
        },
        "tool_policy": {
            "default": {
                "type": "history",
                "scope": "baseline",
                "on_miss": "fail",
            },
            "tools": {},
        },
        "evaluators": [
            {
                "evaluator": "quality",
                "version": 2,
                "params": {"threshold": 0.8},
            }
        ],
    }
    assert result.item["tool_policy"]["default"]["type"] == "passthrough"
    assert result.next_actions == []


async def test_create_omits_tool_policy_for_server_default() -> None:
    """An absent tool-policy option stays unset while the response shows the default."""
    client = StubExperimentClient()

    result = await experiments.create_experiment(
        client,
        "default-policy",
        agent="assistant",
        description=None,
        override=None,
        tool_policy=None,
        evaluators=["quality@2"],
        evaluator_params=None,
    )

    [request] = client.create_calls
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "name": "default-policy",
        "agent_id": str(client.agent.id),
        "evaluators": [{"evaluator": "quality", "version": 2, "params": {}}],
    }
    assert result.item["tool_policy"] == {
        "default": {"type": "passthrough"},
        "tools": {},
    }


@pytest.mark.parametrize(
    ("payload", "expected_type"),
    [
        ('{"default":{"type":"passthrough"}}', "passthrough"),
        (
            '{"default":{"type":"history","scope":"cohort_version",'
            '"on_miss":"passthrough"}}',
            "history",
        ),
        (
            '{"default":{"type":"static","cases":[{"match":{"x":1},'
            '"match_mode":"exact","result":{"ok":true}}],'
            '"on_miss":"error_result"}}',
            "static",
        ),
        (
            '{"default":{"type":"llm","model":"gpt-5","instructions":"Return JSON"}}',
            "llm",
        ),
    ],
)
def test_tool_policy_parser_accepts_every_current_discriminator(
    payload: str, expected_type: str
) -> None:
    """Inline policies accept exactly the discriminators in the shared API model."""
    policy = experiments.parse_tool_policy(payload, option="--tool-policy")
    assert policy.default.type == expected_type


@pytest.mark.parametrize(
    ("parser", "value", "message"),
    [
        (experiments.parse_replay_override, "[]", "must contain a JSON object"),
        (experiments.parse_tool_policy, "{", "is not valid JSON"),
    ],
)
def test_inline_parsers_reject_malformed_json(
    parser: Any, value: str, message: str
) -> None:
    """Non-object and malformed JSON fail through stable CLI validation."""
    with pytest.raises(CLIError, match=message):
        parser(value, option="--config")


def test_inline_parsers_reject_unknown_model_shapes() -> None:
    """API models reject shorthand, missing fields, and unknown configuration."""
    with pytest.raises(PydanticValidationError):
        experiments.parse_replay_override('{"unknown":true}', option="--override")
    with pytest.raises(PydanticValidationError):
        experiments.parse_tool_policy(
            '{"default":{"type":"history"}}', option="--tool-policy"
        )


async def test_create_rejects_duplicate_selected_and_resolved_evaluators() -> None:
    """Both duplicate tokens and aliases resolving to one version fail pre-mutation."""
    client = StubExperimentClient()

    with pytest.raises(CLIError, match="token must be unique"):
        await experiments.create_experiment(
            client,
            "duplicate",
            agent="assistant",
            description=None,
            override=None,
            tool_policy=None,
            evaluators=["quality@2", "quality@2"],
            evaluator_params=None,
        )
    with pytest.raises(CLIError, match="resolved to the same evaluator version"):
        await experiments.create_experiment(
            client,
            "alias",
            agent="assistant",
            description=None,
            override=None,
            tool_policy=None,
            evaluators=["quality@2", f"{client.quality.id}@2"],
            evaluator_params=None,
        )

    assert client.create_calls == []


async def test_list_and_get_map_to_existing_sdk() -> None:
    """Experiment reads resolve exactly and return one bounded server page."""
    client = StubExperimentClient()

    listed = await experiments.list_experiments(
        client,
        size=6,
        cursor="cursor",
        sort="created:asc",
        filter='{"field":"name","op":"eq","value":"regression"}',
    )
    [params] = client.list_calls
    assert isinstance(params, ExperimentListParams)
    assert params.size == 6
    assert params.cursor == "cursor"
    assert params.sort == "created:asc"
    assert json.loads(params.model_dump(mode="json")["filter"]) == {
        "field": "name",
        "op": "eq",
        "value": "regression",
    }
    assert listed.page == {
        "limit": 6,
        "next_cursor": "next",
        "truncated": True,
    }

    fetched = await experiments.get_experiment(client, "regression")
    assert fetched.item["id"] == str(client.experiment.id)


async def test_update_is_sparse_and_supports_explicit_clears() -> None:
    """Preserve omissions and explicit description and override clears."""
    client = StubExperimentClient()

    await experiments.update_experiment(
        client,
        "regression",
        name="renamed",
        description=None,
        clear_description=False,
        override=None,
        clear_override=False,
        tool_policy=None,
        evaluators=None,
        evaluator_params=None,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {"name": "renamed"}

    await experiments.update_experiment(
        client,
        str(client.experiment.id),
        name=None,
        description=None,
        clear_description=True,
        override=None,
        clear_override=True,
        tool_policy=None,
        evaluators=None,
        evaluator_params=None,
    )
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "description": None,
        "override": None,
    }


async def test_update_replaces_complete_config_atomically() -> None:
    """Selected config fields and evaluators travel in one sparse PATCH request."""
    client = StubExperimentClient()

    await experiments.update_experiment(
        client,
        "regression",
        name="candidate",
        description=None,
        clear_description=False,
        override='{"prompt":"Try again"}',
        clear_override=False,
        tool_policy='{"default":{"type":"passthrough"}}',
        evaluators=["quality@latest"],
        evaluator_params=['quality@latest={"strict":true}'],
    )

    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "name": "candidate",
        "override": {"prompt": "Try again"},
        "tool_policy": {"default": {"type": "passthrough"}},
        "evaluators": [
            {
                "evaluator": "quality",
                "version": 2,
                "params": {"strict": True},
            }
        ],
    }


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"description": "set", "clear_description": True},
            "cannot be used together",
        ),
        ({"override": "{}", "clear_override": True}, "cannot be used together"),
        ({"evaluator_params": ["quality@2={}"]}, "requires at least one"),
        ({}, "Select at least one experiment update"),
    ],
)
async def test_update_validation_precedes_experiment_lookup(
    kwargs: dict[str, Any], message: str
) -> None:
    """Invalid sparse updates fail before exact experiment resolution or PATCH."""
    client = StubExperimentClient()
    values: dict[str, Any] = {
        "name": None,
        "description": None,
        "clear_description": False,
        "override": None,
        "clear_override": False,
        "tool_policy": None,
        "evaluators": None,
        "evaluator_params": None,
    }
    values.update(kwargs)

    with pytest.raises(CLIError, match=message):
        await experiments.update_experiment(client, "regression", **values)

    assert client.experiment_lookups == 0
    assert client.update_calls == []


async def test_delete_requires_force_before_network_access() -> None:
    """Destructive experiment deletion is force-gated before exact resolution."""
    client = StubExperimentClient()

    with pytest.raises(CLIError, match="requires --force"):
        await experiments.delete_experiment(client, "regression", force=False)
    assert client.experiment_lookups == 0
    assert client.deleted == []

    result = await experiments.delete_experiment(client, "regression", force=True)
    assert result.item == {"id": str(client.experiment.id), "deleted": True}
    assert client.deleted == [client.experiment.id]


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubExperimentClient:
    """Route public CLI invocations through one recording client."""
    client = StubExperimentClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_experiment_argv_covers_all_crud_commands(
    argv_client: StubExperimentClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every registered experiment leaf emits the standard finite envelope."""
    client = argv_client

    assert (
        app_module.main(
            [
                "experiment",
                "create",
                "nightly",
                "--agent",
                "assistant",
                "--evaluator",
                "quality@latest",
                "--override",
                '{"model":"gpt-5"}',
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.create"
    assert payload["item"]["tool_policy"]["default"]["type"] == "passthrough"

    assert app_module.main(["experiment", "list", "--size", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.list"
    assert payload["count"] == 1

    assert app_module.main(["experiment", "get", "regression", "--output", "text"]) == 0
    assert "regression" in capsys.readouterr().out

    assert (
        app_module.main(["experiment", "update", "regression", "--clear-override"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.update"
    _, update = client.update_calls[-1]
    assert update.model_dump(mode="json", exclude_unset=True) == {"override": None}

    assert app_module.main(["experiment", "delete", "regression", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.delete"
    assert payload["item"]["deleted"] is True


def test_public_argv_errors_are_structured_and_do_not_mutate(
    argv_client: StubExperimentClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Force and inline model errors use stable structured stderr."""
    client = argv_client

    assert app_module.main(["experiment", "delete", "regression"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "experiment.delete"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.deleted == []

    assert (
        app_module.main(
            [
                "experiment",
                "create",
                "broken",
                "--evaluator",
                "quality@2",
                "--tool-policy",
                '{"default":{"type":"history"}}',
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "experiment.create"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.create_calls == []


def test_frozen_config_conflict_is_server_authoritative(
    argv_client: StubExperimentClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The CLI sends one PATCH and preserves the server's frozen-config conflict."""
    client = argv_client
    client.update_error = APIError(409, "Experiment configuration is frozen")

    assert (
        app_module.main(["experiment", "update", "regression", "--override", "{}"]) == 5
    )
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "conflict"
    assert error["error"]["details"]["status_code"] == 409
    assert len(client.update_calls) == 1

    client.update_error = None
    assert (
        app_module.main(
            ["experiment", "update", "regression", "--description", "Allowed"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "experiment.update"
    _, request = client.update_calls[-1]
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "description": "Allowed"
    }
