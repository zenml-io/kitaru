"""Serve the delivery scenario editor through the MCP Apps protocol."""

import argparse
import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

from kitaru.client import KitaruAPIClient
from kitaru.client.config import get_server_url
from kitaru.mcp.apps import build_apps_extension
from kitaru.mcp.connection import resolve_connection
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.registry import register_tools
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, TextContent, ToolAnnotations

from .editor_service import (
    CompareRequest,
    EditorService,
    ExperimentReadRequest,
    KeepRequest,
    OpenRequest,
    SetRequest,
)
from .experiments import ExperimentRequest
from .generation import Request
from .policy import PolicyRequest
from .simulation import RunRequest

APP_URI = "ui://kitaru/scenario-editor.html"
UI = Path(__file__).with_name("ui")


def load_html() -> str:
    """Inline scripts, styles, fonts, and the logo into a portable App resource."""
    html = (UI / "index.html").read_text()
    css = (UI / "style.css").read_text()

    def inline_asset(match: re.Match[str]) -> str:
        name = match.group(1)
        path = UI / name
        mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
        return (
            f'url("data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}")'
        )

    css = re.sub(r'url\(["\']?(assets/[^"\')]+)["\']?\)', inline_asset, css)
    html = html.replace(
        '<link rel="stylesheet" href="style.css">', f"<style>{css}</style>"
    )
    scripts = []
    for name in ("bridge.js", "app.js"):
        html = html.replace(f'<script defer src="{name}"></script>', "")
        scripts.append(f"<script>{(UI / name).read_text()}</script>")
    logo = base64.b64encode((UI / "assets/kitaru-logo.svg").read_bytes()).decode()
    html = html.replace("assets/kitaru-logo.svg", f"data:image/svg+xml;base64,{logo}")
    return html.replace("</body>", "\n".join(scripts) + "</body>")


def result(data: dict[str, Any]) -> CallToolResult:
    """Return the same envelope to model and App clients."""
    envelope = {"ok": True, "data": data}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(envelope))],
        structured_content=envelope,
    )


def failure(error: Exception) -> CallToolResult:
    """Expose useful validation messages without provider errors or credentials."""
    message = (
        str(error)
        if isinstance(error, ValueError)
        else "The operation failed. Check the connection or retry."
    )
    envelope = {"ok": False, "error": {"message": message}}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(envelope))],
        structured_content=envelope,
        is_error=True,
    )


def create_server(settings: MCPSettings) -> MCPServer[MCPServerState]:
    """Reuse native Kitaru tools and add scenario actions in this agent process."""
    connection = resolve_connection(settings.server_url)
    service: EditorService | None = None

    @asynccontextmanager
    async def lifespan(server: MCPServer) -> AsyncIterator[MCPServerState]:
        nonlocal service
        client = KitaruAPIClient(
            base_url=connection.server_url,
            api_key=connection.api_key,
            credential_store=connection.credential_store,
        )
        state = MCPServerState(settings=settings, client=client)
        service = EditorService(client)
        try:
            yield state
        finally:
            service = None
            await state.close()

    apps = build_apps_extension()
    apps.add_html_resource(
        APP_URI, load_html(), name="scenario_editor", title="Scenario editor"
    )
    server = MCPServer[MCPServerState](
        "kitaru-simulation", lifespan=lifespan, extensions=[apps]
    )
    register_tools(server, CapabilityMode.STANDARD)

    async def invoke(action: str, request: object, context: Context) -> CallToolResult:
        state = cast(MCPServerState, context.request_context.lifespan_context)
        active = service or EditorService(state.client)
        try:
            if action == "open":
                data = await active.open(cast(OpenRequest, request))
            elif action == "propose":
                data = await active.propose(cast(Request, request))
            elif action == "policy":
                data = await active.policy(cast(PolicyRequest, request))
            elif action == "run":
                data = await active.run(cast(RunRequest, request))
            elif action == "experiment":
                data = await active.experiment(cast(ExperimentRequest, request))
            elif action == "experiment_status":
                data = await active.experiment_status(
                    cast(ExperimentReadRequest, request)
                )
            elif action == "handoff":
                data = await active.handoff(cast(ExperimentReadRequest, request))
            elif action == "keep":
                data = await active.keep(cast(KeepRequest, request))
            elif action == "set":
                data = await active.read(cast(SetRequest, request))
            else:
                data = await active.compare(cast(CompareRequest, request))
            return result(data)
        except Exception as error:
            logging.getLogger(__name__).warning(
                "Scenario action %s failed: %s", action, type(error).__name__
            )
            return failure(error)

    async def open_editor(request: OpenRequest, context: Context) -> CallToolResult:
        return await invoke("open", request, context)

    async def propose(request: Request, context: Context) -> CallToolResult:
        return await invoke("propose", request, context)

    async def propose_agent_policy(
        request: PolicyRequest, context: Context
    ) -> CallToolResult:
        return await invoke("policy", request, context)

    async def execute(request: RunRequest, context: Context) -> CallToolResult:
        return await invoke("run", request, context)

    async def start_comparison(
        request: ExperimentRequest, context: Context
    ) -> CallToolResult:
        return await invoke("experiment", request, context)

    async def read_comparison(
        request: ExperimentReadRequest, context: Context
    ) -> CallToolResult:
        return await invoke("experiment_status", request, context)

    async def prepare_handoff(
        request: ExperimentReadRequest, context: Context
    ) -> CallToolResult:
        return await invoke("handoff", request, context)

    async def keep(request: KeepRequest, context: Context) -> CallToolResult:
        return await invoke("keep", request, context)

    async def read(request: SetRequest, context: Context) -> CallToolResult:
        return await invoke("set", request, context)

    async def compare(request: CompareRequest, context: Context) -> CallToolResult:
        return await invoke("compare", request, context)

    for name, handler, read_only in (
        ("open", open_editor, True),
        ("propose", propose, False),
        ("policy", propose_agent_policy, False),
        ("experiment", start_comparison, False),
        ("experiment_status", read_comparison, True),
        ("handoff", prepare_handoff, True),
        ("run", execute, False),
        ("keep", keep, False),
        ("set", read, True),
        ("compare", compare, False),
    ):
        description = {
            "open": "Open the scenario editor for exact Kitaru delivery session IDs. Read their captured customer messages and shipping evidence before creating simulations.",
            "propose": "Propose one reviewed variation; calls the configured language model.",
            "policy": "Propose one agent policy change for explicit review; calls the configured language model.",
            "experiment": "Compare two reviewed policies on a pinned cohort using native Kitaru workers and registered evaluators.",
            "experiment_status": "Read validated native experiment and evaluator results.",
            "handoff": "Validate completed passing candidate results and prepare a regression PR handoff for the coding agent.",
            "run": "Run the edited case against the delivery agent and record its scenario and results in Kitaru.",
            "keep": "Add the recorded execution to a versioned regression test set.",
            "set": "Read a pinned saved regression test set.",
            "compare": "Run one agent policy against a pinned test set and record fresh results.",
        }[name]
        ui = {"resourceUri": APP_URI}
        if name != "open":
            ui["visibility"] = ["app"]
        server.add_tool(
            handler,
            name=f"kitaru_simulation_{name}",
            description=description,
            annotations=ToolAnnotations(
                read_only_hint=read_only,
                destructive_hint=False,
                idempotent_hint=read_only or name == "keep",
                open_world_hint=True,
            ),
            meta={"ui": ui},
            structured_output=True,
        )
    return server


def main() -> int:
    """Start the stdio MCP server using the selected Kitaru connection."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server", default=os.environ.get("KITARU_API_URL") or get_server_url()
    )
    args = parser.parse_args()
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    settings = MCPSettings(server_url=args.server, mode=CapabilityMode.STANDARD)
    asyncio.run(create_server(settings).run_stdio_async())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
