#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""The viewer preview relays tool calls only for its own page."""

from typing import Any, cast

from mcp.types import CallToolResult, TextContent
from starlette.testclient import TestClient

from kitaru.mcp.viewer_preview import DEFAULT_APP_CSP, PreviewOptions, build_app

OPTIONS = PreviewOptions(
    server=None,
    mode="standard",
    viewers_dir=None,
    port=8766,
    viewer="gene-map",
    session=None,
    investigation="0193",
    position=1,
)


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        self.calls.append((name, arguments))
        return CallToolResult(content=[TextContent(type="text", text="ok")])


def _client(fake: _Client) -> TestClient:
    app = build_app(cast(Any, fake), "<p>viewer</p>", OPTIONS)
    return TestClient(app, base_url="http://127.0.0.1:8766")


def test_the_page_reaches_the_tools_with_a_json_post() -> None:
    fake = _Client()
    response = _client(fake).post(
        "/call",
        json={"name": "kitaru_session_view", "arguments": {"request": {}}},
        headers={"origin": "http://127.0.0.1:8766"},
    )
    assert response.status_code == 200
    assert fake.calls == [("kitaru_session_view", {"request": {}})]


def test_other_pages_and_rebound_hosts_cannot_call_tools() -> None:
    fake = _Client()
    client = _client(fake)
    body = '{"name": "kitaru_delete", "arguments": {}}'
    attempts = [
        # A form or fetch with a simple content type skips the CORS preflight.
        client.post("/call", content=body, headers={"content-type": "text/plain"}),
        client.post(
            "/call",
            content=body,
            headers={"content-type": "application/json", "origin": "https://evil.test"},
        ),
        client.post(
            "/call",
            content=body,
            headers={"content-type": "application/json", "host": "evil.test:8766"},
        ),
    ]
    assert [response.status_code for response in attempts] == [403, 403, 403]
    assert fake.calls == []


def test_the_viewer_frame_is_served_under_the_mcp_apps_default_policy() -> None:
    client = _client(_Client())
    frame = client.get("/app")
    host = client.get("/")
    assert frame.headers["content-security-policy"] == DEFAULT_APP_CSP
    assert "connect-src 'none'" in DEFAULT_APP_CSP
    assert (
        '"investigation": "0193"' in host.text
        and 'sandbox="allow-scripts"' in host.text
    )
