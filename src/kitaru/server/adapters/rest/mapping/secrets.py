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
"""Secret DTO conversions."""

from typing import Literal, overload

from kitaru.api_models.v1.secrets import SecretResponse, SecretWithValuesResponse
from kitaru.server.domain.secret import Secret


@overload
def secret_to_response(
    secret: Secret, include_values: Literal[True]
) -> SecretWithValuesResponse: ...


@overload
def secret_to_response(
    secret: Secret, include_values: Literal[False] = False
) -> SecretResponse: ...


def secret_to_response(
    secret: Secret, include_values: bool = False
) -> SecretResponse | SecretWithValuesResponse:
    """Convert a secret entity to its response DTO.

    Args:
        secret: Stored secret.
        include_values: Whether to include the secret values.

    Returns:
        Secret response, with values when requested.
    """
    assert secret.created is not None
    assert secret.updated is not None
    if include_values:
        return SecretWithValuesResponse(
            id=secret.id,
            owner_id=secret.owner_id,
            name=secret.name,
            type=secret.type,
            values=secret.values,
            created=secret.created,
            updated=secret.updated,
        )
    return SecretResponse(
        id=secret.id,
        owner_id=secret.owner_id,
        name=secret.name,
        type=secret.type,
        created=secret.created,
        updated=secret.updated,
    )
