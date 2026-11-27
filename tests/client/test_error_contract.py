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
"""Structured API error response contracts."""

import httpx
import pytest

from kitaru.api_models.v1.base import ErrorBody
from kitaru.client.exceptions import (
    APIError,
    AuthenticationError,
    AuthorizationError,
    InvalidServerResponseError,
    NotFoundError,
    ServerError,
    TokenGrantError,
    ValidationError,
    raise_for_response,
)


def test_error_body_accepts_optional_code() -> None:
    """Describe both legacy and structured errors in the shared DTO."""
    assert ErrorBody(detail="conflict").code is None
    assert ErrorBody(detail="conflict", code="future_code").code == "future_code"


def test_api_error_preserves_constructor_and_string() -> None:
    """Keep existing exception construction and display compatible."""
    legacy = APIError(409, "conflict")
    assert legacy.code is None
    structured = APIError(409, "conflict", code="future_code")
    assert structured.code == "future_code"
    assert str(legacy) == str(structured) == "409: conflict"


@pytest.mark.parametrize("code", ["duplicate_session_external_id", "future_code", ""])
def test_error_response_retains_code(code: str) -> None:
    """Retain arbitrary string codes so unknown codes cannot look absent."""
    response = httpx.Response(409, json={"detail": "conflict", "code": code})
    with pytest.raises(APIError) as error:
        raise_for_response(response)
    assert error.value.code == code
    assert error.value.detail == "conflict"
    assert str(error.value) == "409: conflict"


@pytest.mark.parametrize(
    "payload", [{"detail": "conflict"}, {"detail": "conflict", "code": None}]
)
def test_legacy_and_null_code_response(payload: dict[str, str | None]) -> None:
    """Treat omitted or explicitly null codes as absent."""
    with pytest.raises(APIError) as error:
        raise_for_response(httpx.Response(409, json=payload))
    assert error.value.code is None
    assert error.value.detail == "conflict"


@pytest.mark.parametrize("code", [0, True, [], {}])
def test_malformed_code_rejects_response(code: object) -> None:
    """Reject malformed codes rather than falling back to legacy matching."""
    with pytest.raises(InvalidServerResponseError, match="code"):
        raise_for_response(
            httpx.Response(409, json={"detail": "conflict", "code": code})
        )


@pytest.mark.parametrize(
    ("status", "error_class"),
    [
        (401, AuthenticationError),
        (403, AuthorizationError),
        (404, NotFoundError),
        (422, ValidationError),
        (500, ServerError),
    ],
)
def test_status_subclasses_preserve_code(
    status: int, error_class: type[APIError]
) -> None:
    """Keep typed status exceptions while retaining their structured code."""
    with pytest.raises(error_class) as error:
        raise_for_response(
            httpx.Response(status, json={"detail": "failure", "code": "future_code"})
        )
    assert error.value.code == "future_code"
    assert error.value.status_code == status


def test_plain_text_error_remains_compatible() -> None:
    """Keep non-JSON error text when the server has no structured body."""
    with pytest.raises(ServerError) as error:
        raise_for_response(httpx.Response(503, text="unavailable"))
    assert error.value.detail == "unavailable"
    assert error.value.code is None


def test_oauth_error_remains_compatible() -> None:
    """Keep OAuth error codes distinct from the optional API error code."""
    with pytest.raises(TokenGrantError) as error:
        raise_for_response(
            httpx.Response(
                400, json={"error": "access_denied", "error_description": "Denied"}
            )
        )
    assert error.value.error == "access_denied"
    assert error.value.detail == "Denied"
    assert error.value.code is None
