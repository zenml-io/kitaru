#!/usr/bin/env python3
#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Exercise an installed Kitaru MCP entrypoint over real stdio."""

import argparse
import asyncio
import contextlib
import json
import tempfile
import threading
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from mcp.client import session as client_session
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.types import TextContent

EXPECTED_TOOLS = {
    "read-only": ["kitaru_registry_read", "kitaru_activity_read"],
    "standard": [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_session_import",
    ],
    "destructive": [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_session_import",
        "kitaru_workflow_cancel",
        "kitaru_delete",
    ],
}
SESSION_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
OWNER_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
AGENT_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")


class _StubHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != f"/v1/sessions/{SESSION_ID}":
            self.send_error(404)
            return
        now = datetime.now(UTC).isoformat()
        body = json.dumps(
            {
                "id": str(SESSION_ID),
                "owner_id": str(OWNER_ID),
                "agent_id": str(AGENT_ID),
                "origin": "recorded",
                "status": "completed",
                "inputs": {},
                "outputs": {},
                "expected": None,
                "metadata": {},
                "llm_call_count": 0,
                "tool_call_count": 0,
                "created": now,
                "updated": now,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


@contextlib.contextmanager
def _stub_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


async def _probe_mode(
    console: Path,
    server_url: str,
    mode: str,
    *,
    protocol_version: str | None = None,
) -> None:
    original_version = client_session.LATEST_HANDSHAKE_VERSION
    if protocol_version is not None:
        client_session.LATEST_HANDSHAKE_VERSION = protocol_version
    try:
        with tempfile.TemporaryFile(mode="w+") as stderr:
            parameters = StdioServerParameters(
                command=str(console), args=["--server", server_url, "--mode", mode]
            )
            async with (
                stdio_client(parameters, errlog=stderr) as (reader, writer),
                ClientSession(reader, writer) as session,
            ):
                initialized = await session.initialize()
                if protocol_version is not None:
                    assert initialized.protocol_version == protocol_version
                tools = await session.list_tools()
                assert [tool.name for tool in tools.tools] == EXPECTED_TOOLS[mode]
                if mode == "read-only":
                    result = await session.call_tool(
                        "kitaru_activity_read",
                        {
                            "request": {
                                "operation": "get",
                                "kind": "session",
                                "id": str(SESSION_ID),
                            }
                        },
                    )
                    assert not result.is_error
                    assert result.structured_content is not None
                    assert isinstance(result.content[0], TextContent)
                    text = result.content[0].text
                    assert json.loads(text) == result.structured_content
                    assert result.structured_content["data"]["id"] == str(SESSION_ID)
            stderr.seek(0)
            diagnostics = stderr.read()
            assert str(SESSION_ID) not in diagnostics
    finally:
        client_session.LATEST_HANDSHAKE_VERSION = original_version


async def _run(console: Path) -> None:
    with _stub_server() as server_url:
        await _probe_mode(console, server_url, "read-only")
        await _probe_mode(
            console,
            server_url,
            "read-only",
            protocol_version="2025-06-18",
        )
        await _probe_mode(console, server_url, "standard")
        await _probe_mode(console, server_url, "destructive")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("console", type=Path)
    arguments = parser.parse_args()
    asyncio.run(_run(arguments.console))
    print(
        "Installed MCP protocol contract passed for read-only, standard, "
        "and destructive"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
