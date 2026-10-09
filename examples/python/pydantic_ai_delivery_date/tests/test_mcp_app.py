"""MCP tool registration and self-contained App resource contracts."""

import asyncio

from kitaru.mcp.settings import MCPSettings

from delivery_date.mcp_app import APP_URI, create_server, failure, load_html, result
from delivery_date.preview import parse_open_request


def test_launch_tool_and_app_only_actions_are_registered():
    async def check():
        server = create_server(MCPSettings(server_url="http://127.0.0.1:8000"))
        tools = {tool.name: tool for tool in await server.list_tools()}
        assert "kitaru_activity_read" in tools
        assert tools["kitaru_simulation_open"].meta["ui"]["resourceUri"] == APP_URI
        assert "visibility" not in tools["kitaru_simulation_open"].meta["ui"]
        for name in (
            "propose",
            "policy",
            "run",
            "keep",
            "set",
            "compare",
            "experiment",
            "experiment_status",
            "handoff",
        ):
            assert tools[f"kitaru_simulation_{name}"].meta["ui"]["visibility"] == [
                "app"
            ]
        assert tools["kitaru_simulation_keep"].annotations.idempotent_hint
        assert not tools["kitaru_simulation_run"].annotations.read_only_hint

    asyncio.run(check())


def test_portable_app_needs_no_external_assets_or_demo_banners():
    html = load_html()
    assert 'src="app.js"' not in html
    assert 'href="style.css"' not in html
    assert 'src="assets/' not in html
    assert "data:font/woff2;base64," in html
    assert "Disposable prototype" not in html
    assert "Planned storage" not in html
    assert "ui/initialize" in html


def test_protocol_errors_and_results_match_text_envelopes():
    import json

    success = result({"sessionId": "source"})
    assert json.loads(success.content[0].text) == success.structured_content
    error = failure(RuntimeError("secret-token-value"))
    assert "secret-token-value" not in error.content[0].text
    assert error.is_error
    assert error.structured_content["ok"] is False


def test_browser_reopen_retains_exact_pinned_version_and_sources():
    assert parse_open_request(
        "session_ids=a%2Cb&cohort_version_id=pinned", ["other"]
    ) == {
        "session_ids": ["a", "b"],
        "cohort_version_id": "pinned",
    }
    assert parse_open_request("cohort_version_id=pinned", ["default"]) == {
        "session_ids": ["default"],
        "cohort_version_id": "pinned",
    }
