"""Atomic session ingestion tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator
from typing import Annotated, Any

import httpx
import pytest
from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from conftest import db_settings, lifespan_client
from kitaru.server.adapters.rest.dependencies import (
    get_engine,
    get_payload_store,
    get_session,
    get_session_node_service,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.session_node_service import SessionNodeService
from kitaru.server.domain.session_node import SessionNode
from kitaru.transport import IDEMPOTENCY_KEY_HEADER


def _body(agent_id: str, node_count: int = 601) -> dict[str, Any]:
    return {
        "session": {
            "agent_id": agent_id,
            "origin": "imported",
            "status": "completed",
            "external_id": "source-session",
            "imported_from": "langfuse",
            "inputs": "source input" * 20,
            "outputs": "source output" * 20,
        },
        "nodes": [
            {
                "external_id": f"node-{index}",
                "node_type": "llm_call",
                "name": f"call-{index}",
                "status": "completed",
                "inputs": "node input" * 20,
                "outputs": "node output" * 20,
                "tokens": {"input_tokens": 1, "output_tokens": 2},
                "cost": "0.01",
                "attributes": None,
            }
            for index in range(node_count)
        ],
    }


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with lifespan_client(
        db_settings(PAYLOAD_OFFLOAD_THRESHOLD_BYTES=64)
    ) as client:
        yield client


async def test_ingest_stores_all_nodes_and_rollups(client: httpx.AsyncClient) -> None:
    """Accept more than a recording batch and persist offloaded node payloads."""
    agent = (await client.post("/api/v1/agents", json={"name": "atomic"})).json()
    body = _body(agent["id"])
    response = await client.post("/api/v1/sessions/ingest", json=body)
    assert response.status_code == 201, response.text
    stored = response.json()
    assert stored["llm_call_count"] == 601
    assert stored["tokens"]["input_tokens"] == 601
    assert stored["tokens"]["output_tokens"] == 1202
    assert stored["cost"] == "6.01"
    full = (await client.get(f"/api/v1/sessions/{stored['id']}/full")).json()
    assert len(full["nodes"]) == 601
    assert full["session"]["inputs"] == body["session"]["inputs"]
    assert full["nodes"][0]["inputs"] == body["nodes"][0]["inputs"]
    assert full["nodes"][-1]["outputs"] == body["nodes"][-1]["outputs"]


async def test_ingest_retry_preserves_successful_duplicate(
    client: httpx.AsyncClient,
) -> None:
    """Replay a lost response and keep complete duplicates unchanged."""
    agent = (await client.post("/api/v1/agents", json={"name": "duplicate"})).json()
    body = _body(agent["id"], 1)
    headers = {IDEMPOTENCY_KEY_HEADER: str(uuid.uuid4())}
    first = await client.post("/api/v1/sessions/ingest", json=body, headers=headers)
    assert first.status_code == 201, first.text
    replay = await client.post("/api/v1/sessions/ingest", json=body, headers=headers)
    assert replay.status_code == 201
    assert replay.json() == first.json()
    body["nodes"][0]["outputs"] = "replacement"
    duplicate = await client.post("/api/v1/sessions/ingest", json=body)
    assert duplicate.status_code == 409
    full = (await client.get(f"/api/v1/sessions/{first.json()['id']}/full")).json()
    assert len(full["nodes"]) == 1
    assert full["nodes"][0]["outputs"] != "replacement"


async def test_ingest_late_node_failure_rolls_back_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back the header and an earlier node batch on a later failure."""
    fail = True
    calls = 0

    def configure(app: FastAPI) -> None:
        def node_service(
            session: Annotated[AsyncSession, Depends(get_session)],
            engine: Annotated[AsyncEngine, Depends(get_engine)],
            payload_store: Annotated[PayloadStore, Depends(get_payload_store)],
        ) -> SessionNodeService:
            service = get_session_node_service(session, engine, payload_store)
            original_ingest = service.ingest_nodes

            async def ingest(
                session_id: uuid.UUID,
                batch: list[SessionNodeUpsert],
                actor: AuthContext,
            ) -> list[SessionNode]:
                nonlocal calls
                calls += 1
                if fail and calls == 2:
                    raise RuntimeError("injected late node failure")
                return await original_ingest(session_id, batch, actor=actor)

            monkeypatch.setattr(service, "ingest_nodes", ingest)
            return service

        app.dependency_overrides[get_session_node_service] = node_service

    async with lifespan_client(db_settings(), mutate_app=configure) as client:
        agent = (await client.post("/api/v1/agents", json={"name": "retry"})).json()
        body = _body(agent["id"])
        with pytest.raises(RuntimeError, match="injected late node failure"):
            await client.post("/api/v1/sessions/ingest", json=body)
        assert calls == 2
        assert (await client.get("/api/v1/sessions")).json()["items"] == []
        fail = False
        response = await client.post("/api/v1/sessions/ingest", json=body)
        assert response.status_code == 201, response.text
        full = (
            await client.get(f"/api/v1/sessions/{response.json()['id']}/full")
        ).json()
        assert len(full["nodes"]) == 601


async def test_ingest_rejects_duplicate_nodes_across_batches(
    client: httpx.AsyncClient,
) -> None:
    """Validate node identity across the complete request before creating a session."""
    agent = (await client.post("/api/v1/agents", json={"name": "invalid"})).json()
    body = _body(agent["id"])
    body["nodes"][-1]["external_id"] = body["nodes"][0]["external_id"]
    response = await client.post("/api/v1/sessions/ingest", json=body)
    assert response.status_code == 422
    assert (await client.get("/api/v1/sessions")).json()["items"] == []
