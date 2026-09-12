#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Isolated agent and agent-version API sequence properties.

The broad API fuzzer deliberately shares one database and tests independent
requests. This opt-in suite instead runs every complete action list in a fresh
PostgreSQL database, carries symbolic ids through successful prerequisites,
and emits a sanitized receipt when an invariant fails.
"""

import asyncio
import importlib
import os
import re
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from fuzz_sequences import (
    CredentialRole,
    SequenceAction,
    SequenceReceipt,
    SequenceRuntime,
    annotate_sequence_failure,
    isolate_sequence,
)
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from conftest import db_settings
from kitaru.server.database.service import DatabaseService

_SEQUENCES_ENABLED = os.environ.get("KITARU_FUZZ_API_SEQUENCES") == "1"
if not _SEQUENCES_ENABLED:
    pytest.skip(
        "API sequence fuzzing is opt-in; set KITARU_FUZZ_API_SEQUENCES=1 "
        "(needs docker compose up -d db)",
        allow_module_level=True,
    )

try:
    schemathesis = importlib.import_module("schemathesis")
    response_schema_conformance = importlib.import_module(
        "schemathesis.specs.openapi.checks"
    ).response_schema_conformance
except ImportError as exc:
    raise pytest.UsageError(
        "API sequence fuzzing requires the fuzz dependency group"
    ) from exc

SPEC_PATH = Path(__file__).parents[2] / "openapi" / "openapi.json"
SCHEMA = schemathesis.openapi.from_path(str(SPEC_PATH))
UUID_PATTERN = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)


def _get_positive_int(name: str, default: int, *, maximum: int | None = None) -> int:
    """Read a positive bounded integer from the environment."""
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise pytest.UsageError(f"{name} must be a positive integer") from exc
    if value < 1 or (maximum is not None and value > maximum):
        bound = f" no greater than {maximum}" if maximum is not None else ""
        raise pytest.UsageError(f"{name} must be a positive integer{bound}")
    return value


MAX_EXAMPLES = _get_positive_int("KITARU_FUZZ_API_SEQUENCE_MAX_EXAMPLES", 25)
MAX_ACTIONS = _get_positive_int("KITARU_FUZZ_API_SEQUENCE_MAX_ACTIONS", 15, maximum=15)
if MAX_ACTIONS < 2:
    raise pytest.UsageError("KITARU_FUZZ_API_SEQUENCE_MAX_ACTIONS must be at least 2")

SEQUENCE_SETTINGS = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    derandomize=os.environ.get("KITARU_FUZZ_RANDOM") is None,
    suppress_health_check=[HealthCheck.too_slow],
)

SAFE_DESCRIPTION = st.one_of(
    st.none(),
    st.text(
        alphabet=(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ._-é漢🙂"
        ),
        min_size=0,
        max_size=32,
    ),
)
SAFE_VERSION = st.one_of(
    st.none(),
    st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.+/",
        min_size=1,
        max_size=24,
    ).filter(lambda value: value[0].isalnum() and value[-1].isalnum()),
)


def _action(
    operation: str,
    target: str | None = None,
    **arguments: Any,
) -> SequenceAction:
    """Build one symbolic sequence action."""
    return SequenceAction(name=operation, target=target, arguments=arguments)


@st.composite
def _successful_crud_sequences(
    draw: st.DrawFn,
) -> list[SequenceAction]:
    """Generate bounded chains whose prerequisites always exist when needed."""
    action_count = draw(st.integers(min_value=2, max_value=MAX_ACTIONS))
    agent_name = draw(
        st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=24
        )
        .filter(lambda value: value[0] != "-" and value[-1] != "-")
        .map(lambda value: f"fuzz-{value}")
    )
    description = draw(SAFE_DESCRIPTION)
    actions = [
        _action("create_agent", "agent_0", name=agent_name, description=description)
    ]
    agent_alive = True
    live_versions: list[str] = []
    deleted_versions: list[str] = []
    version_count = 0

    while len(actions) < action_count:
        if agent_alive:
            choices = ["get_agent", "list_agents", "update_agent", "create_version"]
            if live_versions or deleted_versions:
                choices.append("get_version")
            if live_versions:
                choices.extend(["list_versions", "update_version", "delete_version"])
            choices.append("delete_agent")
        else:
            choices = ["get_agent", "delete_agent", "create_version"]
            if live_versions or deleted_versions:
                choices.append("get_version")
            if live_versions:
                choices.append("delete_version")

        action_name = draw(st.sampled_from(choices))
        if action_name == "update_agent":
            updated = draw(SAFE_DESCRIPTION)
            actions.append(_action(action_name, "agent_0", description=updated))
        elif action_name == "create_version":
            symbol = f"version_{version_count}"
            display_version = draw(SAFE_VERSION)
            version_description = draw(SAFE_DESCRIPTION)
            actions.append(
                _action(
                    action_name,
                    symbol,
                    agent="agent_0",
                    display_version=display_version,
                    description=version_description,
                )
            )
            if agent_alive:
                live_versions.append(symbol)
                version_count += 1
        elif action_name in {"get_version", "update_version", "delete_version"}:
            candidates = (
                [*live_versions, *deleted_versions]
                if action_name == "get_version"
                else live_versions
            )
            symbol = draw(st.sampled_from(candidates))
            if action_name == "update_version":
                updated = draw(SAFE_DESCRIPTION)
                actions.append(_action(action_name, symbol, description=updated))
            else:
                actions.append(_action(action_name, symbol))
            if action_name == "delete_version":
                live_versions.remove(symbol)
                deleted_versions.append(symbol)
        elif action_name == "list_versions":
            actions.append(_action(action_name, "agent_0"))
        else:
            actions.append(_action(action_name, "agent_0"))
            if action_name == "delete_agent":
                agent_alive = False

    return actions


def _validate_response(
    *,
    method: str,
    path: str,
    response: httpx.Response,
    path_parameters: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> None:
    """Validate an actual response against its checked-in OpenAPI operation."""
    case_arguments: dict[str, Any] = {}
    if path_parameters is not None:
        case_arguments["path_parameters"] = path_parameters
    if body is not None:
        case_arguments["body"] = body
    case = SCHEMA[path][method.upper()].Case(**case_arguments)
    case.validate_response(response, checks=[response_schema_conformance])


async def _prepare_runtime(runtime: SequenceRuntime) -> None:
    """Bind the account id so response receipts contain no live UUIDs."""
    response = await runtime.client.get(
        "/api/v1/accounts/me",
        headers=runtime.get_headers(CredentialRole.ACCOUNT),
    )
    assert response.status_code == 200
    runtime.bind_id("account", response.json()["id"])


async def _check_database_exists(name: str) -> bool:
    """Check one disposable database name through PostgreSQL's admin database."""
    engine = create_async_engine(
        DatabaseService.generate_database_uri(db_settings(), use_default_db=True)
    )
    try:
        async with engine.connect() as connection:
            row = await connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": name},
            )
            return row.first() is not None
    finally:
        await engine.dispose()


def _bind_created_id(
    runtime: SequenceRuntime,
    target: str,
    response: httpx.Response,
) -> AssertionError | None:
    """Bind a valid created UUID, deferring any collision until after recording."""
    if response.status_code != 201:
        return None
    try:
        response_id = response.json().get("id")
    except (AttributeError, ValueError):
        return None
    if not isinstance(response_id, str):
        return None
    try:
        uuid.UUID(response_id)
    except ValueError:
        return None
    try:
        runtime.bind_id(target, response_id)
    except AssertionError as exc:
        return exc
    return None


async def _execute_action(
    runtime: SequenceRuntime,
    action: SequenceAction,
    state: dict[str, dict[str, Any]],
) -> None:
    """Execute one symbolic action and check the modeled API state."""
    role = CredentialRole.ACCOUNT
    headers = runtime.get_headers(role)
    target = action.target
    arguments = action.arguments
    invariants: list[str] = ["response_schema"]

    if action.name == "create_agent":
        assert target is not None
        path = "/api/v1/agents"
        method = "POST"
        body = {"name": arguments["name"], "description": arguments["description"]}
        response = await runtime.client.post(path, json=body, headers=headers)
        binding_error = _bind_created_id(runtime, target, response)
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=[*invariants, "created_values"],
        )
        if binding_error is not None:
            raise binding_error
        _validate_response(method=method, path=path, response=response, body=body)
        assert response.status_code == 201
        payload = response.json()
        assert payload["name"] == body["name"]
        assert payload["description"] == body["description"]
        assert payload["latest_version"] == 0
        state[target] = {
            "kind": "agent",
            "alive": True,
            "name": body["name"],
            "description": body["description"],
            "versions": [],
            "next_version": 1,
        }
        return

    if action.name in {"get_agent", "update_agent", "delete_agent"}:
        assert target is not None
        model = state[target]
        agent_id = runtime.resolve_id(target)
        path = "/api/v1/agents/{agent_id}"
        concrete_path = f"/api/v1/agents/{agent_id}"
        path_parameters = {"agent_id": agent_id}
        method = {
            "get_agent": "GET",
            "update_agent": "PATCH",
            "delete_agent": "DELETE",
        }[action.name]
        body = (
            {"description": arguments["description"]}
            if action.name == "update_agent"
            else None
        )
        response = await runtime.client.request(
            method, concrete_path, json=body, headers=headers
        )
        expected_status = 200 if model["alive"] else 404
        if action.name == "delete_agent":
            expected_status = 204 if model["alive"] else 404
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=[*invariants, "agent_lifecycle"],
        )
        _validate_response(
            method=method,
            path=path,
            response=response,
            path_parameters=path_parameters,
            body=body,
        )
        assert response.status_code == expected_status
        if response.status_code == 200:
            payload = response.json()
            if action.name == "update_agent":
                model["description"] = arguments["description"]
            assert payload["id"] == agent_id
            assert payload["name"] == model["name"]
            assert payload["description"] == model["description"]
            assert payload["latest_version"] == model["next_version"] - 1
        if action.name == "delete_agent" and response.status_code == 204:
            model["alive"] = False
        return

    if action.name == "list_agents":
        path = "/api/v1/agents"
        response = await runtime.client.get(path, headers=headers)
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=[*invariants, "agent_collection"],
        )
        _validate_response(method="GET", path=path, response=response)
        assert response.status_code == 200
        live_ids = [
            runtime.resolve_id(symbol)
            for symbol, model in reversed(state.items())
            if model["kind"] == "agent" and model["alive"]
        ]
        assert [item["id"] for item in response.json()["items"]] == live_ids
        return

    if action.name == "create_version":
        assert target is not None
        agent_symbol = str(arguments["agent"])
        agent = state[agent_symbol]
        agent_id = runtime.resolve_id(agent_symbol)
        path = "/api/v1/agents/{agent_id}/versions"
        concrete_path = f"/api/v1/agents/{agent_id}/versions"
        path_parameters = {"agent_id": agent_id}
        body = {
            "display_version": arguments["display_version"],
            "description": arguments["description"],
        }
        response = await runtime.client.post(concrete_path, json=body, headers=headers)
        binding_error = _bind_created_id(runtime, target, response)
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=[*invariants, "version_parent"],
        )
        if binding_error is not None:
            raise binding_error
        _validate_response(
            method="POST",
            path=path,
            response=response,
            path_parameters=path_parameters,
            body=body,
        )
        expected_status = 201 if agent["alive"] else 404
        assert response.status_code == expected_status
        if response.status_code == 201:
            payload = response.json()
            assert payload["agent_id"] == agent_id
            assert payload["version"] == agent["next_version"]
            assert payload["display_version"] == body["display_version"]
            assert payload["description"] == body["description"]
            state[target] = {
                "kind": "version",
                "alive": True,
                "agent": agent_symbol,
                "version": agent["next_version"],
                "display_version": body["display_version"],
                "description": body["description"],
            }
            agent["versions"].append(target)
            agent["next_version"] += 1
            persisted_parent = await runtime.client.get(
                f"/api/v1/agents/{agent_id}", headers=headers
            )
            _validate_response(
                method="GET",
                path="/api/v1/agents/{agent_id}",
                response=persisted_parent,
                path_parameters={"agent_id": agent_id},
            )
            assert persisted_parent.status_code == 200
            assert persisted_parent.json()["latest_version"] == payload["version"]
        return

    if action.name in {"get_version", "update_version", "delete_version"}:
        assert target is not None
        model = state[target]
        version_id = runtime.resolve_id(target)
        path = "/api/v1/agent-versions/{agent_version_id}"
        concrete_path = f"/api/v1/agent-versions/{version_id}"
        path_parameters = {"agent_version_id": version_id}
        method = {
            "get_version": "GET",
            "update_version": "PATCH",
            "delete_version": "DELETE",
        }[action.name]
        body = (
            {"description": arguments["description"]}
            if action.name == "update_version"
            else None
        )
        response = await runtime.client.request(
            method, concrete_path, json=body, headers=headers
        )
        expected_status = 200 if model["alive"] else 404
        if action.name == "delete_version":
            expected_status = 204 if model["alive"] else 404
        checked_invariants = [*invariants, "version_lifecycle"]
        if action.name == "delete_version":
            checked_invariants.append("version_high_water")
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=checked_invariants,
        )
        _validate_response(
            method=method,
            path=path,
            response=response,
            path_parameters=path_parameters,
            body=body,
        )
        assert response.status_code == expected_status
        if response.status_code == 200:
            if action.name == "update_version":
                model["description"] = arguments["description"]
            payload = response.json()
            assert payload["id"] == version_id
            assert payload["agent_id"] == runtime.resolve_id(model["agent"])
            assert payload["version"] == model["version"]
            assert payload["description"] == model["description"]
        if action.name == "delete_version" and response.status_code == 204:
            model["alive"] = False
            parent = state[model["agent"]]
            if parent["alive"]:
                parent_id = runtime.resolve_id(model["agent"])
                persisted_parent = await runtime.client.get(
                    f"/api/v1/agents/{parent_id}", headers=headers
                )
                _validate_response(
                    method="GET",
                    path="/api/v1/agents/{agent_id}",
                    response=persisted_parent,
                    path_parameters={"agent_id": parent_id},
                )
                assert persisted_parent.status_code == 200
                assert (
                    persisted_parent.json()["latest_version"]
                    == parent["next_version"] - 1
                )
        return

    if action.name == "list_versions":
        assert target is not None
        agent = state[target]
        agent_id = runtime.resolve_id(target)
        path = "/api/v1/agents/{agent_id}/versions"
        concrete_path = f"/api/v1/agents/{agent_id}/versions"
        response = await runtime.client.get(concrete_path, headers=headers)
        runtime.record_response(
            action=action,
            credential_role=role,
            response=response,
            invariants=[*invariants, "version_collection"],
        )
        _validate_response(
            method="GET",
            path=path,
            response=response,
            path_parameters={"agent_id": agent_id},
        )
        assert response.status_code == 200
        expected = [
            runtime.resolve_id(symbol)
            for symbol in reversed(agent["versions"])
            if state[symbol]["alive"]
        ]
        assert [item["id"] for item in response.json()["items"]] == expected
        return

    raise AssertionError(f"Unknown sequence action: {action.name}")


async def _run_crud_sequence(actions: list[SequenceAction]) -> SequenceReceipt:
    """Run one complete action list in one isolated database."""
    receipt = SequenceReceipt()
    with annotate_sequence_failure(receipt):
        async with isolate_sequence(receipt) as runtime:
            await _prepare_runtime(runtime)
            state: dict[str, dict[str, Any]] = {}
            for action in actions:
                await _execute_action(runtime, action, state)
        receipt.assert_accepted()
    return receipt


def _receipt_shape(receipt: SequenceReceipt) -> list[tuple[Any, ...]]:
    """Return stable replay evidence, excluding timestamps in response bodies."""
    return [
        (
            step.action,
            step.credential_role,
            step.status,
            step.invariants,
        )
        for step in receipt.steps
    ]


def test_fixed_crud_chain_replays_on_fresh_databases() -> None:
    """Replay create/read/update/list/delete behavior from symbolic actions."""
    actions = [
        _action("create_agent", "agent_0", name="receipt-agent", description="one"),
        _action("get_agent", "agent_0"),
        _action("update_agent", "agent_0", description="two"),
        _action("get_agent", "agent_0"),
        _action(
            "create_version",
            "version_0",
            agent="agent_0",
            display_version="v1",
            description="first",
        ),
        _action("get_version", "version_0"),
        _action("update_version", "version_0", description="revised"),
        _action("get_version", "version_0"),
        _action("list_versions", "agent_0"),
        _action("delete_version", "version_0"),
        _action("get_version", "version_0"),
        _action("delete_agent", "agent_0"),
        _action("get_agent", "agent_0"),
        _action("delete_agent", "agent_0"),
    ]

    first = asyncio.run(_run_crud_sequence(actions))
    serialized = first.serialize()
    recorded = SequenceReceipt.model_validate_json(serialized)
    second = asyncio.run(_run_crud_sequence([step.action for step in recorded.steps]))

    assert first.successful_operations == second.successful_operations == 11
    assert _receipt_shape(first) == _receipt_shape(second)
    assert not UUID_PATTERN.search(serialized)


def test_deleted_agent_retains_its_version() -> None:
    """Keep a version readable after its parent agent is deleted."""
    actions = [
        _action("create_agent", "agent_0", name="retained-agent", description=None),
        _action(
            "create_version",
            "version_0",
            agent="agent_0",
            display_version=None,
            description="retained",
        ),
        _action("delete_agent", "agent_0"),
        _action("get_version", "version_0"),
        _action(
            "create_version",
            "version_1",
            agent="agent_0",
            display_version=None,
            description=None,
        ),
        _action("delete_version", "version_0"),
        _action("get_version", "version_0"),
    ]

    receipt = asyncio.run(_run_crud_sequence(actions))

    assert [step.status for step in receipt.steps] == [
        201,
        201,
        204,
        200,
        404,
        204,
        404,
    ]


def test_versions_stay_isolated_to_their_parent_collection() -> None:
    """List each created version only through its correct parent agent."""
    actions = [
        _action("create_agent", "agent_0", name="first-agent", description=None),
        _action("create_agent", "agent_1", name="second-agent", description=None),
        _action("list_agents"),
        _action(
            "create_version",
            "version_0",
            agent="agent_0",
            display_version="v1",
            description=None,
        ),
        _action("list_versions", "agent_0"),
        _action("list_versions", "agent_1"),
    ]

    receipt = asyncio.run(_run_crud_sequence(actions))

    assert receipt.successful_operations == len(actions)


async def test_schema_failure_carries_the_failing_response_receipt() -> None:
    """Record a malformed create response before schema validation raises."""

    async def return_malformed_response(request: httpx.Request) -> httpx.Response:
        _ = request
        response = httpx.Response(
            201,
            json={"id": "not-a-uuid", "name": "incomplete"},
        )
        response.elapsed = timedelta()
        return response

    receipt = SequenceReceipt()
    transport = httpx.MockTransport(return_malformed_response)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        runtime = SequenceRuntime(client, receipt)
        runtime.set_credential(CredentialRole.ACCOUNT, "test-token")
        with (
            pytest.raises(BaseExceptionGroup) as raised,
            annotate_sequence_failure(receipt),
        ):
            await _execute_action(
                runtime,
                _action(
                    "create_agent",
                    "agent_0",
                    name="schema-agent",
                    description=None,
                ),
                {},
            )

    assert receipt.steps[0].status == 201
    notes = "\n".join(raised.value.__notes__)
    assert "Sequence receipt:\n{" in notes
    assert "not-a-uuid" in notes


async def test_duplicate_created_id_is_recorded_without_leaking_uuid() -> None:
    """Retain the conflicting response before reporting a sanitized id collision."""
    raw_id = "018f7777-1234-7abc-8123-123456789abc"

    async def return_duplicate_id(request: httpx.Request) -> httpx.Response:
        _ = request
        return httpx.Response(201, json={"id": raw_id})

    receipt = SequenceReceipt()
    transport = httpx.MockTransport(return_duplicate_id)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        runtime = SequenceRuntime(client, receipt)
        runtime.set_credential(CredentialRole.ACCOUNT, "test-token")
        runtime.bind_id("agent_0", raw_id)
        with pytest.raises(
            AssertionError,
            match="Live id is already bound to another symbol",
        ) as raised:
            await _execute_action(
                runtime,
                _action(
                    "create_agent",
                    "agent_1",
                    name="duplicate",
                    description=None,
                ),
                {},
            )

    assert raw_id not in str(raised.value)
    serialized = receipt.serialize()
    assert len(receipt.steps) == 1
    assert receipt.steps[0].response == {"id": {"$ref": "agent_0"}}
    assert raw_id not in serialized


@given(actions=_successful_crud_sequences())
@SEQUENCE_SETTINGS
def test_generated_successful_crud_sequences(actions: list[SequenceAction]) -> None:
    """Preserve agent and version invariants across generated action lists."""
    receipt = asyncio.run(_run_crud_sequence(actions))
    assert len(receipt.steps) == len(actions)


async def test_consecutive_isolated_sequences_share_no_rows() -> None:
    """Create a fresh database for each complete sequence context."""
    for _ in range(2):
        receipt = SequenceReceipt()
        async with isolate_sequence(receipt) as runtime:
            await _prepare_runtime(runtime)
            response = await runtime.client.get(
                "/api/v1/agents", headers=runtime.get_headers(CredentialRole.ACCOUNT)
            )
            assert response.status_code == 200
            assert response.json()["items"] == []
            response = await runtime.client.post(
                "/api/v1/agents",
                json={"name": "same-name"},
                headers=runtime.get_headers(CredentialRole.ACCOUNT),
            )
            assert response.status_code == 201


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(AssertionError("controlled failure"), id="assertion"),
        pytest.param(KeyboardInterrupt(), id="interruption"),
    ],
)
async def test_body_failure_drops_the_sequence_database(
    failure: BaseException,
) -> None:
    """Drop the real disposable database after a body failure or interruption."""
    receipt = SequenceReceipt()
    database_name: str | None = None
    with pytest.raises(type(failure)) as raised:
        async with isolate_sequence(receipt) as runtime:
            database_name = runtime.database_name
            assert database_name is not None
            assert await _check_database_exists(database_name)
            raise failure

    assert raised.value is failure
    assert database_name is not None
    assert not await _check_database_exists(database_name)
