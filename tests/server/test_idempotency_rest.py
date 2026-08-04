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
"""Unit tests for REST idempotency canonicalization and replay."""

from types import SimpleNamespace
from typing import Any

import httpx
from fastapi import Request, Response

from kitaru.server.adapters.rest.idempotency import (
    build_idempotency_request,
    build_replay_response,
    get_replay_safe_headers,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.idempotency import IdempotencyStoredResponse
from kitaru.server.domain.idempotency import IdempotencyRequestInProgress


def _request(
    body: bytes,
    *,
    path_value: str = "019632fa-0000-7000-8000-000000000000",
    query: bytes = b"",
    content_type: bytes = b"application/json; charset=UTF-8",
) -> Request:
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": f"/v1/experiments/{path_value}/runs",
            "raw_path": f"/v1/experiments/{path_value}/runs".encode(),
            "query_string": query,
            "headers": [(b"content-type", content_type)],
            "path_params": {"experiment_id": path_value},
            "route": SimpleNamespace(
                path_format="/v1/experiments/{experiment_id}/runs"
            ),
            "server": ("test", 80),
            "client": ("test", 123),
            "scheme": "http",
            "root_path": "",
        },
        receive=receive,
    )


async def test_body_read_is_cached_for_downstream_json_parsing() -> None:
    """Fingerprint exact bytes without consuming the handler's body."""
    body = b'{"agent_version_id":"stable","inputs":{"q":"hi"}}'
    request = _request(body)
    built = await build_idempotency_request(request, "request-1")
    assert len(built.fingerprint) == 64
    assert await request.body() == body
    assert await request.json() == {
        "agent_version_id": "stable",
        "inputs": {"q": "hi"},
    }


async def test_mutation_relevant_inputs_change_the_fingerprint() -> None:
    """Detect changed raw body, path, query, and content type."""
    base = await build_idempotency_request(_request(b"{}"), "request-1")
    changed = [
        await build_idempotency_request(_request(b'{"x":1}'), "request-1"),
        await build_idempotency_request(
            _request(b"{}", path_value="019632fa-0000-7000-8000-000000000001"),
            "request-1",
        ),
        await build_idempotency_request(
            _request(b"{}", query=b"mode=alternate"), "request-1"
        ),
        await build_idempotency_request(
            _request(b"{}", content_type=b"application/problem+json"), "request-1"
        ),
    ]
    assert all(candidate.route == base.route for candidate in changed)
    assert (
        len({base.fingerprint, *(candidate.fingerprint for candidate in changed)}) == 5
    )


async def test_equivalent_query_order_and_content_type_case_are_canonical() -> None:
    """Ignore ordering and insignificant media-type case differences."""
    first = await build_idempotency_request(
        _request(
            b"{}",
            query=b"b=2&a=1",
            content_type=b"Application/JSON; Charset=UTF-8",
        ),
        "request-1",
    )
    second = await build_idempotency_request(
        _request(
            b"{}",
            query=b"a=1&b=2",
            content_type=b"application/json;charset=utf-8",
        ),
        "request-1",
    )
    assert first.fingerprint == second.fingerprint


def test_safe_headers_are_filtered_when_stored_and_replayed() -> None:
    """Never persist or reconstruct cookies, tracing, or hop-by-hop headers."""
    original = Response(
        b"{}",
        media_type="application/json",
        headers={
            "Set-Cookie": "secret=value",
            "Authorization": "Bearer secret",
            "Traceparent": "trace",
            "Connection": "close",
        },
    )
    assert get_replay_safe_headers(original.headers) == {
        "content-type": "application/json"
    }

    replay = build_replay_response(
        IdempotencyStoredResponse(
            status_code=201,
            body=b'{"id":"stable"}',
            headers={
                "content-type": "application/json",
                "set-cookie": "should-not-escape",
            },
        )
    )
    assert replay.body == b'{"id":"stable"}'
    assert replay.status_code == 201
    assert replay.headers["content-length"] == str(len(replay.body))
    assert replay.headers["idempotency-status"] == "replayed"
    assert "set-cookie" not in replay.headers


async def test_in_progress_error_has_stable_retryable_http_contract() -> None:
    """Map bounded reservation waits to HTTP 425 with retry guidance."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )

    @app.get("/in-progress")
    async def in_progress() -> None:
        raise IdempotencyRequestInProgress(retry_after_seconds=5)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/in-progress")
    assert response.status_code == 425
    assert response.headers["Retry-After"] == "5"
    assert response.json() == {
        "detail": "A request with this idempotency key is still in progress.",
        "code": "request_in_progress",
        "retryable": True,
    }
