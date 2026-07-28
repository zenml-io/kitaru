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
"""Tests for the async analytics client."""

import json
import uuid

import httpx

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.source import AnalyticsSource, current_source


def record_requests(client: AnalyticsClient) -> list[httpx.Request]:
    """Route the client's HTTP traffic to an in-memory recorder.

    Args:
        client: Client to reroute.

    Returns:
        List collecting every sent request.
    """
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    client._http = httpx.AsyncClient(
        base_url="https://analytics.test", transport=httpx.MockTransport(handler)
    )
    return requests


async def test_track_posts_message_batch() -> None:
    """Post a queued track message to the batch endpoint."""
    client = AnalyticsClient()
    requests = record_requests(client)
    user_id = uuid.uuid4()

    client.track(user_id, "Test event", {"key": "value"})
    await client.aclose()

    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == "/batch"
    assert request.headers["Source-Context"] == "kitaru-python"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.content) == [
        {
            "user_id": str(user_id),
            "event": "Test event",
            "properties": {"key": "value"},
            "type": "track",
            "debug": False,
        }
    ]


async def test_identify_posts_message_batch() -> None:
    """Post a queued identify message to the batch endpoint."""
    client = AnalyticsClient(debug=True)
    requests = record_requests(client)
    user_id = uuid.uuid4()

    client.identify(user_id, {"email": "alice@example.com"})
    await client.aclose()

    assert len(requests) == 1
    assert json.loads(requests[0].content) == [
        {
            "user_id": str(user_id),
            "traits": {"email": "alice@example.com"},
            "type": "identify",
            "debug": True,
        }
    ]


async def test_messages_grouped_by_source() -> None:
    """Send one request per source found in a batch."""
    client = AnalyticsClient()
    requests = record_requests(client)

    client.track(uuid.uuid4(), "Test event")
    token = current_source.set(AnalyticsSource.UI)
    try:
        client.track(uuid.uuid4(), "Test event")
    finally:
        current_source.reset(token)
    await client.aclose()

    assert [request.headers["Source-Context"] for request in requests] == [
        "kitaru-python",
        "kitaru-ui",
    ]


async def test_disabled_client_posts_nothing() -> None:
    """Drop all messages when the client is disabled."""
    client = AnalyticsClient(enabled=False)
    requests = record_requests(client)

    client.track(uuid.uuid4(), "Test event")
    client.identify(uuid.uuid4())
    await client.aclose()

    assert requests == []


async def test_send_failure_is_swallowed() -> None:
    """Never raise when the analytics server rejects a batch."""
    client = AnalyticsClient()
    client._http = httpx.AsyncClient(
        base_url="https://analytics.test",
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
    )

    client.track(uuid.uuid4(), "Test event")
    await client.aclose()
