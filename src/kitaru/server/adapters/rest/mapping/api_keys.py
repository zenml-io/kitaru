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
"""API key DTO conversions."""

from kitaru.api_models.v1.api_key import ApiKeyIssuedResponse, ApiKeyResponse
from kitaru.server.domain.api_key import ApiKey


def api_key_to_response(api_key: ApiKey) -> ApiKeyResponse:
    """Convert an API key entity to its response DTO.

    Args:
        api_key: Stored API key.

    Returns:
        API key response.
    """
    assert api_key.created is not None
    assert api_key.updated is not None
    return ApiKeyResponse(
        id=api_key.id,
        owner_id=api_key.owner_id,
        name=api_key.name,
        active=api_key.active,
        last_used=api_key.last_used,
        created=api_key.created,
        updated=api_key.updated,
    )


def api_key_to_issued_response(api_key: ApiKey, key: str) -> ApiKeyIssuedResponse:
    """Convert an API key entity and its plaintext key to the issued DTO.

    Args:
        api_key: Stored API key.
        key: Plaintext key.

    Returns:
        API key response with the plaintext key.
    """
    assert api_key.created is not None
    assert api_key.updated is not None
    return ApiKeyIssuedResponse(
        id=api_key.id,
        owner_id=api_key.owner_id,
        name=api_key.name,
        active=api_key.active,
        key=key,
        last_used=api_key.last_used,
        created=api_key.created,
        updated=api_key.updated,
    )
