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
"""Typed client exceptions."""

import httpx


class KitaruClientError(Exception):
    """Kitaru client error."""


class APIError(KitaruClientError):
    """API error."""

    def __init__(self, status_code: int, detail: str) -> None:
        """Initialize the error.

        Args:
            status_code: HTTP status code.
            detail: Error detail.
        """
        super().__init__(f"{status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class AuthenticationError(APIError):
    """Authentication error."""


class AuthorizationError(APIError):
    """Authorization error."""


class NotFoundError(APIError):
    """Not found error."""


class ValidationError(APIError):
    """Validation error."""


class ServerError(APIError):
    """Server error."""


class TokenGrantError(APIError):
    """Token grant error."""

    def __init__(self, status_code: int, detail: str, error: str) -> None:
        """Initialize the error.

        Args:
            status_code: HTTP status code.
            detail: Error detail.
            error: OAuth 2.0 error code.
        """
        super().__init__(status_code, detail)
        self.error = error


class InvalidServerResponseError(KitaruClientError):
    """Invalid server response error."""


class ResponseTooLargeError(KitaruClientError):
    """Response exceeded a caller-selected byte limit."""

    def __init__(self, max_bytes: int, content_length: int | None = None) -> None:
        """Initialize the error.

        Args:
            max_bytes: Maximum response bytes accepted by the caller.
            content_length: Declared response size when the server supplied one.
        """
        detail = f"Response exceeds the {max_bytes}-byte limit"
        if content_length is not None:
            detail = f"{detail} (declared {content_length} bytes)"
        super().__init__(detail)
        self.max_bytes = max_bytes
        self.content_length = content_length


_STATUS_ERRORS: dict[int, type[APIError]] = {
    401: AuthenticationError,
    403: AuthorizationError,
    404: NotFoundError,
    422: ValidationError,
}


def raise_for_response(response: httpx.Response) -> None:
    """Raise a typed error for an error response.

    Args:
        response: HTTP response.

    Raises:
        APIError: The response has an error status code.
    """
    if response.is_success:
        return

    detail = response.text
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("detail"), str):
        detail = payload["detail"]
    if response.status_code == httpx.codes.BAD_REQUEST and isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, str):
            # OAuth 2.0 error bodies carry error_description where Kitaru's own
            # error bodies carry detail.
            description = payload.get("error_description")
            if isinstance(description, str):
                detail = description
            raise TokenGrantError(response.status_code, detail, error)

    error_class = _STATUS_ERRORS.get(response.status_code)
    if error_class is None:
        error_class = ServerError if response.status_code >= 500 else APIError
    raise error_class(response.status_code, detail)
