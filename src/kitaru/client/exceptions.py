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


class ConflictError(APIError):
    """Conflict error."""


class ValidationError(APIError):
    """Validation error."""


class ServerError(APIError):
    """Server error."""


_STATUS_ERRORS: dict[int, type[APIError]] = {
    401: AuthenticationError,
    403: AuthorizationError,
    404: NotFoundError,
    409: ConflictError,
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

    error_class = _STATUS_ERRORS.get(response.status_code)
    if error_class is None:
        error_class = ServerError if response.status_code >= 500 else APIError
    raise error_class(response.status_code, detail)
