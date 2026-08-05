#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Cross-resource MCP mutation integration contracts."""

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.management import EvaluatorSelection
from kitaru.mcp.models.workflows import SessionImportRequest
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.experiments import _get_evaluator_configs
from kitaru.mcp.tools.workflows import handle_session_import


class _ImportClient:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.blobs = SimpleNamespace(get=self._get_blob)
        self.importers = SimpleNamespace(
            get_version=self._get_importer_version, get=self._get_importer
        )
        self.agent_versions = SimpleNamespace(get=self._get_agent_version)
        self.imports = SimpleNamespace(create=self._create_import)

    async def _get_blob(self, item_id: uuid.UUID) -> object:
        self.calls.append("blob")
        return SimpleNamespace(id=item_id)

    async def _get_importer_version(self, parent_id: uuid.UUID, version: int) -> object:
        self.calls.append("importer_version")
        return SimpleNamespace(id=uuid.uuid4(), importer_id=parent_id, version=version)

    async def _get_importer(self, item_id: uuid.UUID) -> object:
        self.calls.append("importer")
        return SimpleNamespace(id=item_id, name="json")

    async def _get_agent_version(self, item_id: uuid.UUID) -> object:
        self.calls.append("agent_version")
        return SimpleNamespace(id=item_id, agent_id=uuid.uuid4())

    async def _create_import(self, _request: object) -> object:
        self.calls.append("create")
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})


class _EvaluatorClient:
    def __init__(self, evaluator_id: uuid.UUID) -> None:
        self.evaluator_id = evaluator_id
        self.parent_calls = 0
        self.version_calls: list[int] = []
        self.returned_parent_id = evaluator_id
        self.evaluators = SimpleNamespace(
            get=self._get_evaluator, get_version=self._get_version
        )

    async def _get_evaluator(self, evaluator_id: uuid.UUID) -> object:
        assert evaluator_id == self.evaluator_id
        self.parent_calls += 1
        return SimpleNamespace(id=evaluator_id, name="accuracy")

    async def _get_version(self, evaluator_id: uuid.UUID, version: int) -> object:
        assert evaluator_id == self.evaluator_id
        self.version_calls.append(version)
        return SimpleNamespace(
            id=uuid.uuid4(),
            evaluator_id=self.returned_parent_id,
            version=version,
        )


def _get_state(client: object) -> MCPServerState:
    return MCPServerState(MCPSettings(), cast(Any, client))


async def test_existing_blob_import_uses_four_bounded_preflight_reads() -> None:
    """Import performs four direct reads, one create, and no traversal or polling."""
    client = _ImportClient()
    result = cast(
        dict[str, Any],
        await handle_session_import(
            _get_state(client),
            SessionImportRequest(
                payload_blob_id=uuid.uuid4(),
                importer_id=uuid.uuid4(),
                importer_version=2,
                agent_version_id=uuid.uuid4(),
            ),
        ),
    )
    assert client.calls == [
        "blob",
        "importer_version",
        "importer",
        "agent_version",
        "create",
    ]
    assert result["operation"] == "session_import"
    assert result["idempotency"] == "domain-deduplicated-only"


async def test_evaluator_selections_use_name_version_dto_and_cache_parent() -> None:
    evaluator_id = uuid.uuid4()
    client = _EvaluatorClient(evaluator_id)
    configs = await _get_evaluator_configs(
        _get_state(client),
        [
            EvaluatorSelection(
                evaluator_id=evaluator_id,
                version=2,
                params={"threshold": 0.8},
            ),
            EvaluatorSelection(evaluator_id=evaluator_id, version=3),
        ],
    )
    assert configs is not None
    assert [config.model_dump(mode="json") for config in configs] == [
        {"evaluator": "accuracy", "version": 2, "params": {"threshold": 0.8}},
        {"evaluator": "accuracy", "version": 3, "params": {}},
    ]
    assert client.parent_calls == 1
    assert client.version_calls == [2, 3]


async def test_evaluator_version_must_belong_to_selected_parent() -> None:
    evaluator_id = uuid.uuid4()
    client = _EvaluatorClient(evaluator_id)
    client.returned_parent_id = uuid.uuid4()
    with pytest.raises(MCPToolError, match="different parent or version") as raised:
        await _get_evaluator_configs(
            _get_state(client),
            [EvaluatorSelection(evaluator_id=evaluator_id, version=2)],
        )
    assert raised.value.code == "conflict"
