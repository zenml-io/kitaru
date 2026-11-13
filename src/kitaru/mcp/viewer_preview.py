#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Preview trace viewers in a local stand-in for an MCP Apps host.

Run `python -m kitaru.mcp.viewer_preview --server URL` and open the printed
address. The page plays the host's part: it starts `kitaru-mcp` over stdio,
shows the session viewer in a sandboxed frame under the MCP Apps default
content security policy, and relays the frame's tool calls to that process.
With `--check`, a headless Chromium opens the viewer instead and reports any
text anchor whose rendered text differs from the stored text.
"""

import argparse
import asyncio
import importlib
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

import uvicorn
from mcp.types import TextResourceContents
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from kitaru.mcp.apps import SESSION_VIEWER_URI
from mcp import Client, StdioServerParameters

# The MCP Apps default policy for a view that declares no domains.
DEFAULT_APP_CSP = (
    "default-src 'none'; script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
    "media-src 'self' data:; connect-src 'none'"
)
_SERVER_COMMAND = "from kitaru.mcp_entrypoint import main; raise SystemExit(main())"
# Injected first in the frame's head, so it answers the preview's anchor check
# before the viewer runtime's listener replies that the method is unknown.
_SELF_CHECK_SCRIPT = """<script>
window.addEventListener("message", (event) => {
  const m = event.data;
  if (event.source !== window.parent || !m || m.method !== "kitaru/self-check") return;
  event.stopImmediatePropagation();
  const missing = [{ problem: "the viewer runtime did not load" }];
  const result = window.kitaru
    ? window.kitaru.selfCheck()
    : { ok: false, checked: 0, problems: missing, hints: [] };
  window.parent.postMessage({ jsonrpc: "2.0", id: m.id, result }, "*");
});
</script>"""


@dataclass(frozen=True)
class PreviewOptions:
    """What to start and what to open."""

    server: str | None
    mode: str
    viewers_dir: Path | None
    port: int
    viewer: str
    session: str | None
    investigation: str | None
    position: int


def get_server_parameters(options: PreviewOptions) -> StdioServerParameters:
    """Build the command that starts the Kitaru MCP server for the preview."""
    args = ["-c", _SERVER_COMMAND, "--mode", options.mode]
    if options.server:
        args += ["--server", options.server]
    if options.viewers_dir:
        args += ["--viewers-dir", str(options.viewers_dir)]
    return StdioServerParameters(
        command=sys.executable, args=args, env=dict(os.environ)
    )


def build_app(client: Client, app_html: str, options: PreviewOptions) -> Starlette:
    """Build the preview host's web app around a connected MCP client."""
    config = {
        "viewer": options.viewer,
        "session": options.session,
        "investigation": options.investigation,
        "position": options.position,
    }
    host_html = (
        (files("kitaru.mcp") / "ui" / "preview_host.html")
        .read_text(encoding="utf-8")
        .replace("/*{{PREVIEW_CONFIG}}*/ {}", json.dumps(config))
    )

    async def host(_request: Request) -> Response:
        return HTMLResponse(host_html)

    frame_html = app_html.replace("<head>", f"<head>{_SELF_CHECK_SCRIPT}", 1)

    async def app(_request: Request) -> Response:
        return HTMLResponse(
            frame_html,
            headers={
                "content-security-policy": DEFAULT_APP_CSP,
                "cache-control": "no-store",
            },
        )

    allowed_hosts = {f"127.0.0.1:{options.port}", f"localhost:{options.port}"}
    allowed_origins = {f"http://{host}" for host in allowed_hosts}

    async def call(request: Request) -> Response:
        # Any web page in the browser can post to a local port. Requiring JSON
        # forces a cross-origin preflight that this app never answers, and the
        # host and origin checks stop DNS rebinding, so only the preview page
        # itself reaches the MCP server's tools through this route.
        origin = request.headers.get("origin")
        if (
            not request.headers.get("content-type", "").startswith("application/json")
            or request.headers.get("host") not in allowed_hosts
            or (origin is not None and origin not in allowed_origins)
        ):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        body = await request.json()
        result = await client.call_tool(body["name"], body.get("arguments") or {})
        return JSONResponse(
            result.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    return Starlette(
        routes=[
            Route("/", host),
            Route("/app", app),
            Route("/call", call, methods=["POST"]),
        ]
    )


async def read_app_html(client: Client) -> str:
    """Read the session viewer page from the running MCP server."""
    result = await client.read_resource(SESSION_VIEWER_URI)
    for content in result.contents:
        if isinstance(content, TextResourceContents):
            return content.text
    raise RuntimeError(f"{SESSION_VIEWER_URI} returned no HTML")


async def run_preview(options: PreviewOptions, *, check: bool) -> int:
    """Serve the preview until interrupted, or run one headless check."""
    async with Client(get_server_parameters(options)) as client:
        app = build_app(client, await read_app_html(client), options)
        server = uvicorn.Server(
            uvicorn.Config(
                app, host="127.0.0.1", port=options.port, log_level="warning"
            )
        )
        url = f"http://127.0.0.1:{options.port}/"
        if not check:
            print(f"Kitaru viewer preview: {url}", flush=True)
            await server.serve()
            return 0
        serving = asyncio.create_task(server.serve())
        while not server.started:
            await asyncio.sleep(0.05)
        try:
            return await _check_in_browser(url)
        finally:
            server.should_exit = True
            await serving


async def _check_in_browser(url: str) -> int:
    try:
        # Playwright is optional and only `--check` needs it.
        playwright_api = importlib.import_module("playwright.async_api")
    except ModuleNotFoundError:
        print(
            "--check needs Playwright: uv run --with playwright python -m "
            "kitaru.mcp.viewer_preview --check ..., after "
            "`playwright install chromium`",
            file=sys.stderr,
        )
        return 2
    async with playwright_api.async_playwright() as playwright:
        executable = os.environ.get("KITARU_PREVIEW_CHROMIUM") or None
        browser = await playwright.chromium.launch(executable_path=executable)
        page = await browser.new_page()
        errors: list[str] = []
        page.on(
            "console",
            lambda message: (
                errors.append(message.text) if message.type == "error" else None
            ),
        )
        page.on("pageerror", lambda error: errors.append(str(error)))
        await page.goto(url)
        frame = page.frame_locator("#app")
        await frame.locator("#kv-status").wait_for(state="hidden", timeout=60_000)
        result: dict[str, Any] = await page.evaluate("window.previewCheck()")
        await browser.close()
    print(json.dumps({"anchors": result, "console_errors": errors}, indent=2))
    return 0 if result.get("ok") and not errors else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and run the preview."""
    parser = argparse.ArgumentParser(
        prog="python -m kitaru.mcp.viewer_preview", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument(
        "--server", help="Kitaru server URL (else KITARU_MCP_SERVER or KITARU_API_URL)"
    )
    parser.add_argument(
        "--mode", default="standard", choices=["read-only", "standard", "destructive"]
    )
    parser.add_argument("--viewers-dir", type=Path, help="Folder of custom viewers")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--viewer", default="default")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--session", help="Session id to open")
    target.add_argument("--investigation", help="Investigation id to open")
    parser.add_argument("--position", type=int, default=0)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Open headless, check the viewer's anchors, and exit",
    )
    arguments = parser.parse_args(argv)
    if arguments.check and not (arguments.session or arguments.investigation):
        parser.error("--check needs --session or --investigation")
    options = PreviewOptions(
        server=arguments.server,
        mode=arguments.mode,
        viewers_dir=arguments.viewers_dir,
        port=arguments.port,
        viewer=arguments.viewer,
        session=arguments.session,
        investigation=arguments.investigation,
        position=arguments.position,
    )
    try:
        return asyncio.run(run_preview(options, check=arguments.check))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
