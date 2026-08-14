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
"""End-to-end server info tests against PostgreSQL."""

import uuid

from conftest import db_settings, lifespan_client


async def test_info_reports_the_generated_server_id() -> None:
    """Report the id the startup bootstrap generated and stored."""
    async with lifespan_client(db_settings()) as client:
        payload = (await client.get("/v1/info")).json()

    assert payload["id"] is not None


async def test_info_reports_the_configured_server_id() -> None:
    """Report the configured id once startup stored it."""
    server_id = uuid.uuid4()

    async with lifespan_client(db_settings(SERVER_ID=server_id)) as client:
        payload = (await client.get("/v1/info")).json()

    assert payload["id"] == str(server_id)
