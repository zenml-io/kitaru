#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Cross-resource MCP workflow integration contracts."""

import uuid
from types import SimpleNamespace
from typing import Any, cast

from kitaru.mcp.lifecycle import MCPConnection, MCPServerState
from kitaru.mcp.models.management import EvaluatorSelection
from kitaru.mcp.models.workflows import (
    ReplayStart,
    SessionEvaluationStart,
    SessionImportStart,
    SessionRunStart,
)
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.workflows import handle_workflow_start


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
        return SimpleNamespace(id=uuid.uuid4(), parent_id=parent_id, version=version)

    async def _get_importer(self, item_id: uuid.UUID) -> object:
        self.calls.append("importer")
        return SimpleNamespace(id=item_id, name="json")

    async def _get_agent_version(self, item_id: uuid.UUID) -> object:
        self.calls.append("agent_version")
        return SimpleNamespace(id=item_id, agent_id=uuid.uuid4())

    async def _create_import(self, _request: object) -> object:
        self.calls.append("create")
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})


class _ProtectedEvaluatorClient:
    def __init__(
        self, evaluator_id: uuid.UUID, evaluator_version_id: uuid.UUID
    ) -> None:
        self.evaluator_id = evaluator_id
        self.evaluator_version_id = evaluator_version_id
        self.evaluator_name = "accuracy"
        self.requests: list[object] = []
        self.evaluators = SimpleNamespace(get_version=self._get_version)
        self.replays = SimpleNamespace(create=self._create)
        self.evaluations = SimpleNamespace(create=self._create)

    async def _get_version(self, evaluator_id: uuid.UUID, version: int) -> object:
        assert evaluator_id == self.evaluator_id
        return SimpleNamespace(
            id=self.evaluator_version_id,
            evaluator_id=evaluator_id,
            version=version,
            evaluator_name=self.evaluator_name,
        )

    async def _create(self, request: object, *, idempotency_key: str) -> object:
        assert idempotency_key == "stable-id"
        self.requests.append(request)
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})


class _ProtectedClient:
    def __init__(self) -> None:
        self.session_runs = SimpleNamespace(create=self._create)
        self.create_calls: list[str] = []

    async def _create(self, _request: object, *, idempotency_key: str) -> object:
        self.create_calls.append(idempotency_key)
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})


def _get_state(client: object) -> MCPServerState:
    return MCPServerState(
        MCPSettings(), cast(MCPConnection, object()), cast(Any, client)
    )


async def test_existing_blob_import_uses_four_bounded_preflight_reads() -> None:
    """Import performs four direct reads, one create, and no traversal or polling."""
    client = _ImportClient()
    result = cast(
        dict[str, Any],
        await handle_workflow_start(
            _get_state(client),
            SessionImportStart(
                operation="session_import",
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
    assert result["idempotency"] == "domain-deduplicated-only"


async def test_protected_evaluator_workflows_use_immutable_version_identity() -> None:
    """Keep protected replay and evaluation bodies stable across parent renames."""
    evaluator_id = uuid.uuid4()
    evaluator_version_id = uuid.uuid4()
    client = _ProtectedEvaluatorClient(evaluator_id, evaluator_version_id)
    state = _get_state(client)
    state._features = frozenset({"idempotency.v1"})

    selection = EvaluatorSelection(
        evaluator_id=evaluator_id,
        version=3,
        params={"threshold": 0.8},
    )
    replay = ReplayStart(
        operation="replay",
        request_id="stable-id",
        baseline_session_id=uuid.uuid4(),
        evaluators=[selection],
    )
    evaluation = SessionEvaluationStart(
        operation="session_evaluation",
        request_id="stable-id",
        session_ids=[uuid.uuid4()],
        evaluators=[selection],
    )

    await handle_workflow_start(state, replay)
    await handle_workflow_start(state, evaluation)
    client.evaluator_name = "renamed"
    await handle_workflow_start(state, replay)
    await handle_workflow_start(state, evaluation)

    assert len(client.requests) == 4
    serialized = [
        cast(Any, request).model_dump(mode="json", exclude_unset=True)
        for request in client.requests
    ]
    assert serialized[0] == serialized[2]
    assert serialized[1] == serialized[3]
    for request in client.requests:
        config = cast(Any, request).evaluators[0]
        assert config.evaluator_version_id == evaluator_version_id
        assert config.evaluator is None
        assert config.version is None
        assert config.params == {"threshold": 0.8}


async def test_protected_receipt_does_not_claim_stored_or_replayed() -> None:
    """Do not claim stored/replayed after typed resources discard headers."""
    client = _ProtectedClient()
    state = _get_state(client)
    state._features = frozenset({"idempotency.v1"})
    result = cast(
        dict[str, Any],
        await handle_workflow_start(
            state,
            SessionRunStart(
                operation="session_run",
                request_id="stable-id",
                agent_version_id=uuid.uuid4(),
                inputs={},
            ),
        ),
    )
    assert client.create_calls == ["stable-id"]
    assert result["idempotency"] == "server-enforced"
    assert "idempotency_status" not in result
