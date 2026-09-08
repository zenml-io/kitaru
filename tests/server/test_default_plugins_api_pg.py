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

import pytest

from conftest import db_settings, lifespan_client
from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import DefaultPluginDefinition
from kitaru.server.domain.names import RESERVED_NAMESPACE
from kitaru.server.domain.plugin import PluginKind


async def test_default_plugins_are_registered_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """List a catalog-declared default plugin with a null owner after startup."""
    definition = DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/evaluator",
        description="Test evaluator.",
        provider=None,
        entrypoint="package.evaluator:evaluate",
        requirement="kitaru-evaluator==1.0.0",
        display_version="1.0.0",
    )
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", (definition,))

    async with lifespan_client(db_settings()) as client:
        evaluators = (await client.get("/api/v1/evaluators")).json()["items"]

    matches = [item for item in evaluators if item["name"] == definition.name]
    assert len(matches) == 1
    assert matches[0]["owner_id"] is None


@pytest.mark.parametrize(
    "name", ["kitaru/post-import-insights", "kitaru/openai-post-import-insights"]
)
async def test_builtin_analyzer_cannot_be_replaced_or_deleted(name: str) -> None:
    """Keep the analyzer executed by hosted workers controlled by the server."""
    async with lifespan_client(db_settings()) as client:
        analyzers = (await client.get("/api/v1/analyzers")).json()["items"]
        analyzer = next(item for item in analyzers if item["name"] == name)
        path = f"/api/v1/analyzers/{analyzer['id']}"
        original = (await client.get(f"{path}/versions/1")).json()
        replaced = await client.post(
            f"{path}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "other-code==1.0.0",
                    "entrypoint": "other_code:run",
                }
            },
        )
        assert replaced.status_code == 403
        assert (await client.delete(path)).status_code == 403
        assert (await client.get(f"{path}/versions/1")).json() == original
