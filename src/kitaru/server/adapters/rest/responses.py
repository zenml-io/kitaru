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
"""OpenAPI error response declarations."""

from typing import Any

from kitaru.api_models.v1.base import ErrorBody

# Fixed response descriptions, because FastAPI otherwise fills them from
# Python's HTTP phrase table, which changes wording across Python versions
# and makes the generated specification nondeterministic.
_DESCRIPTIONS: dict[int, str] = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    413: "Content Too Large",
    426: "Upgrade Required",
    503: "Service Unavailable",
}


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """Declare the string-detail error body for the given status codes.

    Args:
        status_codes: HTTP status codes to declare.

    Returns:
        Response declarations keyed by status code.
    """
    responses: dict[int | str, dict[str, Any]] = {}
    for code in status_codes:
        responses[code] = {"model": ErrorBody, "description": _DESCRIPTIONS[code]}
    return responses
