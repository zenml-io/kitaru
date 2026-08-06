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
"""End-to-end default plugin registration tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_default_plugins_are_registered_at_startup(
    client: httpx.AsyncClient,
) -> None:
    """List the packaged default plugins with a null owner after startup."""
    evaluators = (await client.get("/v1/evaluators")).json()["items"]
    importers = (await client.get("/v1/importers")).json()["items"]

    evaluator_names = {item["name"] for item in evaluators}
    importer_names = {item["name"] for item in importers}
    assert f"{RESERVED_PLUGIN_NAME_PREFIX}cost" in evaluator_names
    assert f"{RESERVED_PLUGIN_NAME_PREFIX}latency" in evaluator_names
    assert f"{RESERVED_PLUGIN_NAME_PREFIX}tool-call-patterns" in evaluator_names
    assert f"{RESERVED_PLUGIN_NAME_PREFIX}langfuse" in importer_names
    assert all(
        item["owner_id"] is None
        for item in evaluators + importers
        if item["name"].startswith(RESERVED_PLUGIN_NAME_PREFIX)
    )
