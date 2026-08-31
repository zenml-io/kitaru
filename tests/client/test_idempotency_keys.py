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
"""Tests that the SDK exposes idempotency_key on exactly the marked routes."""

import inspect
import uuid
from collections.abc import AsyncGenerator
from types import FunctionType, MethodType
from typing import cast

import pytest

from conftest import (
    FakeTagRepository,
    RequestRecordingTransport,
    local_settings,
    marked_idempotent_routes,
    override_idempotency,
    recording_asgi_api_client,
)
from kitaru.api_models.v1.tag import TagCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.rest.dependencies import authorize, get_tag_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.transport import IDEMPOTENCY_KEY_HEADER

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

# Maps each idempotent route to the SDK resource attribute and method name
# that issues it, so drift in either direction fails a test below.
MAPPING = {
    ("POST", "/api/v1/sessions"): ("sessions", "create"),
    ("POST", "/api/v1/replays"): ("replays", "create"),
    ("POST", "/api/v1/evaluations"): ("evaluations", "create"),
    ("POST", "/api/v1/session-runs"): ("session_runs", "create"),
    ("POST", "/api/v1/imports"): ("imports", "create"),
    ("POST", "/api/v1/agents"): ("agents", "create"),
    ("POST", "/api/v1/agents/{agent_id}/versions"): ("agents", "create_version"),
    ("POST", "/api/v1/cohorts"): ("cohorts", "create"),
    ("POST", "/api/v1/cohorts/{cohort_id}/versions"): ("cohorts", "create_version"),
    ("POST", "/api/v1/experiments"): ("experiments", "create"),
    ("POST", "/api/v1/experiments/{experiment_id}/runs"): ("experiments", "start_run"),
    ("POST", "/api/v1/annotations"): ("annotations", "create"),
    ("POST", "/api/v1/investigations"): ("investigations", "create"),
    ("POST", "/api/v1/api-keys"): ("api_keys", "create"),
    ("POST", "/api/v1/api-keys/{api_key_id}/rotate"): ("api_keys", "rotate"),
    ("POST", "/api/v1/evaluators"): ("evaluators", "create"),
    ("POST", "/api/v1/evaluators/{evaluator_id}/versions"): (
        "evaluators",
        "create_version",
    ),
    ("POST", "/api/v1/importers"): ("importers", "create"),
    ("POST", "/api/v1/importers/{importer_id}/versions"): (
        "importers",
        "create_version",
    ),
    ("POST", "/api/v1/tags"): ("tags", "create"),
    ("POST", "/api/v1/secrets"): ("secrets", "create"),
    ("POST", "/api/v1/service-accounts"): ("service_accounts", "create"),
    ("POST", "/api/v1/users"): ("users", "create"),
}


def _resource_methods(client: KitaruAPIClient) -> dict[tuple[str, str], MethodType]:
    """Return every public method on every resource bound to a client.

    Args:
        client: Client whose bound resources are walked.

    Returns:
        Mapping from (resource attribute, method name) to the bound method.
    """
    methods: dict[tuple[str, str], MethodType] = {}
    for attribute, resource in vars(client).items():
        if attribute.startswith("_"):
            continue
        for name, member in inspect.getmembers(resource, predicate=inspect.ismethod):
            if name.startswith("_"):
                continue
            methods[(attribute, name)] = member
    return methods


def _parameter_names(method: MethodType) -> set[str]:
    """Return the parameter names of a bound method.

    Args:
        method: Bound method to inspect.

    Returns:
        Parameter names.
    """
    # Read the parameters off the code object because inspect.signature
    # evaluates annotations, which fails on Python 3.14 where a resource
    # method named list shadows the builtin that a return annotation
    # subscripts.
    code = cast(FunctionType, method.__func__).__code__
    return set(code.co_varnames[: code.co_argcount + code.co_kwonlyargcount])


def test_mapping_matches_the_marked_routes() -> None:
    """Assert the mapping covers exactly the routes the app marks idempotent."""
    assert set(MAPPING.keys()) == marked_idempotent_routes(create_app(local_settings()))


def test_mapped_methods_accept_idempotency_key() -> None:
    """Assert every mapped SDK method accepts an idempotency_key parameter."""
    client = KitaruAPIClient("http://test")
    resources = _resource_methods(client)
    for attribute, method_name in MAPPING.values():
        method = resources[(attribute, method_name)]
        assert "idempotency_key" in _parameter_names(method), (
            f"{attribute}.{method_name} does not accept idempotency_key"
        )


def test_no_other_method_accepts_idempotency_key() -> None:
    """Assert idempotency_key is absent from every method the mapping omits."""
    client = KitaruAPIClient("http://test")
    mapped = set(MAPPING.values())
    for (attribute, method_name), method in _resource_methods(client).items():
        if (attribute, method_name) in mapped:
            continue
        assert "idempotency_key" not in _parameter_names(method), (
            f"{attribute}.{method_name} accepts idempotency_key but is not "
            "in the idempotent route mapping"
        )


@pytest.fixture
async def api_client() -> AsyncGenerator[
    tuple[KitaruAPIClient, RequestRecordingTransport], None
]:
    """Provide an API client routed to the app with a fake-backed tag service."""
    app = create_app(local_settings())
    service = TagService(repository=FakeTagRepository())
    app.dependency_overrides[get_tag_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    client, recorder = recording_asgi_api_client(app)
    async with client:
        yield client, recorder


async def test_caller_supplied_key_wins_over_the_transport_default(
    api_client: tuple[KitaruAPIClient, RequestRecordingTransport],
) -> None:
    """Send the caller's key on the wire instead of the transport's random one."""
    client, recorder = api_client
    await client.tags.create(
        TagCreateRequest(name="prod"), idempotency_key="caller-key"
    )
    assert recorder.requests[0].headers[IDEMPOTENCY_KEY_HEADER] == "caller-key"
