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
"""REST-specific idempotency fingerprint and replay helpers."""

import hashlib
import json
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import Request, Response

from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.idempotency import (
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.application.services.idempotency_service import (
    IdempotencyService,
)
from kitaru.transport import IDEMPOTENCY_KEY_HEADER as IDEMPOTENCY_KEY_HEADER

IDEMPOTENCY_STATUS_HEADER = "Idempotency-Status"
IDEMPOTENCY_STATUS_STORED = "stored"
IDEMPOTENCY_STATUS_REPLAYED = "replayed"
REPLAY_SAFE_RESPONSE_HEADERS = frozenset({"content-type"})


@dataclass(frozen=True)
class IdempotencyExecution:
    """Owned reservation attached to a request until commit."""

    service: IdempotencyService
    reservation: IdempotencyReservation
    actor: AuthContext


class IdempotencyReplay(Exception):
    """Internal control flow carrying an authoritative stored response."""

    def __init__(self, response: IdempotencyStoredResponse) -> None:
        """Initialize the replay.

        Args:
            response: Stored response to return without running the handler.
        """
        self.response = response
        super().__init__("Replay the completed idempotent response")


def _normalize_text(value: object) -> str:
    """Normalize a canonical fingerprint text value.

    Args:
        value: Value from Starlette route or query parsing.

    Returns:
        NFC-normalized string.
    """
    return unicodedata.normalize("NFC", str(value))


def _canonical_content_type(value: str) -> list[Any]:
    """Canonicalize a Content-Type header.

    Args:
        value: Raw header value, or an empty string.

    Returns:
        Media type and sorted parameter pairs.
    """
    if not value:
        return ["", []]
    segments = [segment.strip() for segment in value.split(";")]
    media_type = segments[0].lower()
    parameters: list[tuple[str, str]] = []
    for segment in segments[1:]:
        name, separator, parameter_value = segment.partition("=")
        if not separator:
            parameters.append((_normalize_text(name).lower(), ""))
            continue
        normalized_name = _normalize_text(name).strip().lower()
        normalized_value = _normalize_text(parameter_value).strip()
        if (
            len(normalized_value) >= 2
            and normalized_value[0] == normalized_value[-1]
            and normalized_value[0] in {'"', "'"}
        ):
            normalized_value = normalized_value[1:-1]
        if normalized_name == "charset":
            normalized_value = normalized_value.lower()
        parameters.append((normalized_name, normalized_value))
    return [media_type, sorted(parameters)]


def _encode_json(value: object) -> bytes:
    """Serialize canonical fingerprint metadata.

    Args:
        value: JSON-compatible value.

    Returns:
        Stable UTF-8 JSON bytes.
    """
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()


def _add_frame_part(digest: Any, value: bytes) -> None:
    """Add one unambiguous length-prefixed fingerprint part.

    Args:
        digest: SHA-256 digest object.
        value: Part bytes.
    """
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


async def build_idempotency_request(
    request: Request, caller_key: str
) -> IdempotencyRequest:
    """Build the canonical identity and fingerprint for a mutation.

    Reading the body is safe because Starlette caches the exact bytes for
    downstream request-model parsing.

    Args:
        request: Incoming request.
        caller_key: Validated caller-provided key.

    Returns:
        Canonical request identity with a SHA-256 fingerprint.
    """
    method = request.method.upper()
    path_parameters = sorted(
        (_normalize_text(key), _normalize_text(value))
        for key, value in request.path_params.items()
    )
    # FastAPI's nested included routers expose only the child path on the
    # matched APIRoute (often an empty string). Start from the full ASGI path
    # and substitute normalized parameter values to retain the router prefix.
    route = _normalize_text(request.scope.get("path", request.url.path))
    for name, value in sorted(
        path_parameters, key=lambda item: len(item[1]), reverse=True
    ):
        route = route.replace(value, f"{{{name}}}", 1)
    query = sorted(
        (_normalize_text(key), _normalize_text(value))
        for key, value in request.query_params.multi_items()
    )
    content_type = _canonical_content_type(request.headers.get("content-type", ""))
    body = await request.body()

    digest = hashlib.sha256()
    for part in (
        b"kitaru-idempotency-v1",
        method.encode(),
        route.encode(),
        _encode_json(path_parameters),
        _encode_json(query),
        _encode_json(content_type),
        body,
    ):
        _add_frame_part(digest, part)
    return IdempotencyRequest(
        method=method,
        route=route,
        caller_key=caller_key,
        fingerprint=digest.hexdigest(),
    )


def get_replay_safe_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Copy only headers safe to store and replay.

    Args:
        headers: Response headers.

    Returns:
        Lowercase allowlisted headers.
    """
    return {
        name: value
        for name, value in headers.items()
        if name.lower() in REPLAY_SAFE_RESPONSE_HEADERS
    }


def build_replay_response(stored: IdempotencyStoredResponse) -> Response:
    """Build a raw response from an authoritative completed record.

    Args:
        stored: Exact stored status, bytes, and safe headers.

    Returns:
        Replay response with a newly derived content length.
    """
    response = Response(
        content=stored.body,
        status_code=stored.status_code,
        headers=get_replay_safe_headers(stored.headers),
    )
    response.headers[IDEMPOTENCY_STATUS_HEADER] = IDEMPOTENCY_STATUS_REPLAYED
    return response
