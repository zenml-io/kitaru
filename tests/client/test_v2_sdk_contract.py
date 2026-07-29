"""Focused model and transport tests for the v2 SDK contract."""

import uuid
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.agent import AgentUpdateRequest
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay_config import PassthroughConfig
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.task import LabelSelector, TaskListParams, WorkerScope
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.resources.tasks import TasksResource


class StubResponse:
    """Small response carrying a JSON body."""

    def __init__(self, body: Any) -> None:
        self._body = body

    def json(self) -> Any:
        """Return the response body."""
        return self._body


class StubClient:
    """Record resource requests."""

    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def request(self, method: str, path: str, **kwargs: Any) -> StubResponse:
        """Record one request and return the next response."""
        self.calls.append((method, path, kwargs))
        return StubResponse(self.responses.pop(0))


def test_patch_distinguishes_omitted_and_explicit_null() -> None:
    """Preserve PATCH unset semantics in JSON dumps."""
    assert AgentUpdateRequest().model_dump(mode="json", exclude_unset=True) == {}
    assert AgentUpdateRequest(description=None).model_dump(
        mode="json", exclude_unset=True
    ) == {"description": None}


def test_discriminators_survive_exclude_unset() -> None:
    """Keep default union discriminators on the wire."""
    assert PassthroughConfig().model_dump(mode="json", exclude_unset=True) == {
        "type": "passthrough"
    }
    source = ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="evaluate")
    assert source.model_dump(mode="json", exclude_unset=True)["type"] == "script"


def test_evaluation_result_positional_values_and_validation() -> None:
    """Route supported positional result values to their wire channels."""
    assert EvaluationResult(True, name="passed").score is True
    assert EvaluationResult(0.5, name="quality").score == 0.5
    assert EvaluationResult("great", name="label").value == "great"
    with pytest.raises(ValidationError):
        EvaluationResult(name="missing")


def test_worker_scope_validates_selector_keys() -> None:
    """Reject ambiguous duplicate selector keys."""
    with pytest.raises(ValidationError):
        WorkerScope(
            selectors=[
                LabelSelector(key="runtime", values=["cpu"]),
                LabelSelector(key="runtime", values=["gpu"]),
            ]
        )


def test_node_batch_allows_parent_from_previous_batch() -> None:
    """Allow a child to reference an already-stored parent index."""
    request = SessionNodeBatchRequest(
        nodes=[
            SessionNodeCreateRequest(
                index=201,
                parent_index=1,
                node_type=NodeType.SPAN,
                name="child",
                status=NodeStatus.COMPLETED,
            )
        ]
    )
    assert request.nodes[0].parent_index == 1


async def test_task_list_and_iter_use_params_models() -> None:
    """Send list params and follow opaque cursors."""
    task_id = uuid.uuid4()
    job_id = uuid.uuid4()
    body = {
        "id": str(task_id),
        "job_id": str(job_id),
        "kind": "agent",
        "status": "pending",
        "on_failure": "abort",
        "attempt": 0,
        "labels": {},
        "agent_version_id": None,
        "plugin_version_id": None,
        "payload_blob_id": None,
        "input_session_id": None,
        "agent_id": None,
        "worker_id": None,
        "result_session_id": None,
        "claimed_at": None,
        "heartbeat_at": None,
        "cancel_requested_at": None,
        "started_at": None,
        "ended_at": None,
        "error": None,
        "result": None,
        "created": "2026-01-01T00:00:00Z",
        "updated": "2026-01-01T00:00:00Z",
    }
    stub = StubClient(
        [
            {"items": [body], "next_cursor": "next"},
            {"items": [], "next_cursor": None},
        ]
    )
    resource = TasksResource(stub)  # ty: ignore[invalid-argument-type]
    items = [item async for item in resource.iter(TaskListParams(size=1))]
    assert [item.id for item in items] == [task_id]
    assert stub.calls[0][2]["params"] == {"size": 1}
    assert stub.calls[1][2]["params"] == {"size": 1, "cursor": "next"}


async def test_blob_upload_and_download() -> None:
    """Send multipart bytes and return downloaded bytes."""
    requests: list[httpx.Request] = []
    blob_id = uuid.uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/content"):
            return httpx.Response(200, content=b"payload")
        return httpx.Response(
            201,
            json={
                "id": str(blob_id),
                "sha256": "0" * 64,
                "size": 7,
                "media_type": "text/plain",
                "created": "2026-01-01T00:00:00Z",
            },
        )

    client = KitaruAPIClient("http://test")
    await client._http.aclose()
    client._http = httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(handler)
    )
    uploaded = await client.blobs.upload(
        b"payload", filename="data.txt", media_type="text/plain"
    )
    downloaded = await client.blobs.download(blob_id)
    assert uploaded.id == blob_id
    assert downloaded == b"payload"
    assert b'name="file"; filename="data.txt"' in requests[0].content
    await client.close()


def test_client_registers_every_v2_resource() -> None:
    """Expose every endpoint group on the async client."""
    client = KitaruAPIClient("http://test")
    expected = {
        "accounts",
        "agents",
        "agent_versions",
        "api_keys",
        "auth",
        "blobs",
        "cohorts",
        "devices",
        "evaluations",
        "evaluators",
        "experiments",
        "experiment_runs",
        "health",
        "importers",
        "imports",
        "info",
        "jobs",
        "replays",
        "secrets",
        "session_runs",
        "sessions",
        "tags",
        "tasks",
        "workers",
    }
    assert expected <= vars(client).keys()
