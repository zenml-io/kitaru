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
"""End-to-end device tests against PostgreSQL."""

from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest

from conftest import lifespan_client, local_settings


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(
        local_settings(use_db=True, DEFAULT_ACCOUNT_PASSWORD="secret")
    ) as client:
        yield client


async def _create_verified_device(
    client: httpx.AsyncClient,
) -> tuple[dict[str, Any], str]:
    """Request a device authorization and verify it as the default account.

    Args:
        client: HTTP client for the app.

    Returns:
        Device authorization body and the bearer token of the approving account.
    """
    response = await client.post(
        "/api/v1/device_authorization", data={"hostname": "ci"}
    )
    assert response.status_code == 200
    authorization = response.json()

    response = await client.post(
        "/api/v1/login", data={"username": "default", "password": "secret"}
    )
    assert response.status_code == 200
    account_token = response.json()["access_token"]

    response = await client.post(
        f"/api/v1/devices/{authorization['device_id']}/verify",
        json={"user_code": authorization["user_code"], "trusted": False},
        headers={"Authorization": f"Bearer {account_token}"},
    )
    assert response.status_code == 200

    return authorization, account_token


async def test_device_authorization_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Prove the per-request commit through the full device authorization flow."""
    authorization, account_token = await _create_verified_device(client)
    headers = {"Authorization": f"Bearer {account_token}"}

    response = await client.get(
        f"/api/v1/devices/{authorization['device_id']}", headers=headers
    )
    assert response.status_code == 200
    assert response.json()["status"] == "verified"

    response = await client.post(
        "/api/v1/login",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_id": authorization["device_id"],
            "device_code": authorization["device_code"],
        },
    )
    assert response.status_code == 200

    response = await client.get(
        f"/api/v1/devices/{authorization['device_id']}", headers=headers
    )
    assert response.status_code == 200
    assert response.json()["status"] == "active"


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    authorization, account_token = await _create_verified_device(client)
    headers = {"Authorization": f"Bearer {account_token}"}

    response = await client.patch(
        f"/api/v1/devices/{authorization['device_id']}",
        json={"locked": True},
        headers=headers,
    )
    assert response.status_code == 200

    response = await client.get(
        f"/api/v1/devices/{authorization['device_id']}", headers=headers
    )
    assert response.status_code == 200
    assert response.json()["locked"] is True


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    authorization, account_token = await _create_verified_device(client)
    headers = {"Authorization": f"Bearer {account_token}"}

    response = await client.delete(
        f"/api/v1/devices/{authorization['device_id']}", headers=headers
    )
    assert response.status_code == 204

    response = await client.get(
        f"/api/v1/devices/{authorization['device_id']}", headers=headers
    )
    assert response.status_code == 404


async def test_wrong_user_code_lock_survives_the_rolled_back_request(
    client: httpx.AsyncClient,
) -> None:
    """Persist the failed attempt counter even though each verify request fails.

    Each wrong code answers with HTTP 422, which rolls back the request
    transaction. The attempt counter has to survive that rollback or the
    device would never lock no matter how many codes are guessed.
    """
    response = await client.post(
        "/api/v1/device_authorization", data={"hostname": "ci"}
    )
    assert response.status_code == 200
    authorization = response.json()

    response = await client.post(
        "/api/v1/login", data={"username": "default", "password": "secret"}
    )
    assert response.status_code == 200
    headers = {"Authorization": f"Bearer {response.json()['access_token']}"}

    for _ in range(3):
        response = await client.post(
            f"/api/v1/devices/{authorization['device_id']}/verify",
            json={"user_code": "WRONG-CODE", "trusted": False},
            headers=headers,
        )
        assert response.status_code == 422

    response = await client.post(
        f"/api/v1/devices/{authorization['device_id']}/verify",
        json={"user_code": authorization["user_code"], "trusted": False},
        headers=headers,
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Device is locked"}
