"""Run the same editor operations in a local browser for iteration."""

import argparse
import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlsplit

from kitaru.client import KitaruAPIClient
from pydantic import ValidationError

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
from .mcp_app import failure, load_html
from .policy import PolicyRequest
from .simulation import RunRequest


def parse_open_request(query: str, source_ids: list[str]) -> dict[str, Any]:
    """Read exact source and saved-version selections from a browser launch."""
    values = parse_qs(query)
    ids = values.get("session_ids", [",".join(source_ids)])[0].split(",")
    return {
        "session_ids": [value for value in ids if value],
        "cohort_version_id": values.get("cohort_version_id", [None])[0],
    }


def serve(port: int, server_url: str | None, source_ids: list[str]) -> None:
    """Serve the portable editor and dispatch exact local-origin actions."""
    origin = f"http://127.0.0.1:{port}"
    model_lock = Lock()

    async def action(name: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with KitaruAPIClient(base_url=server_url) as client:
            service = EditorService(client)
            if name == "cases":
                data = await service.open(OpenRequest.model_validate(payload))
            elif name == "proposals":
                data = await service.propose(Request.model_validate(payload))
            elif name == "policy":
                data = await service.policy(PolicyRequest.model_validate(payload))
            elif name == "runs":
                data = await service.run(RunRequest.model_validate(payload))
            elif name == "experiment":
                data = await service.experiment(
                    ExperimentRequest.model_validate(payload)
                )
            elif name == "experiment_status":
                data = await service.experiment_status(
                    ExperimentReadRequest.model_validate(payload)
                )
            elif name == "handoff":
                data = await service.handoff(
                    ExperimentReadRequest.model_validate(payload)
                )
            elif name == "keep":
                data = await service.keep(KeepRequest.model_validate(payload))
            elif name == "set":
                data = await service.read(SetRequest.model_validate(payload))
            elif name == "compare":
                data = await service.compare(CompareRequest.model_validate(payload))
            else:
                raise ValueError("Unknown action")
            return {"ok": True, "data": data}

    class Handler(BaseHTTPRequestHandler):
        def respond(
            self, status: int, payload: bytes, content_type: str = "application/json"
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def execute(self, name: str, payload: dict[str, Any]) -> None:
            if not model_lock.acquire(blocking=False):
                self.respond(
                    409,
                    b'{"ok":false,"error":{"message":"An operation is already running."}}',
                )
                return
            try:
                response = asyncio.run(action(name, payload))
                self.respond(200, json.dumps(response).encode())
            except Exception as error:
                response = failure(error).structured_content
                self.respond(
                    400 if isinstance(error, (ValueError, ValidationError)) else 502,
                    json.dumps(response).encode(),
                )
            finally:
                model_lock.release()

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if self.headers.get("Host") != f"127.0.0.1:{port}":
                self.respond(403, b"{}")
                return
            if parsed.path == "/":
                self.respond(200, load_html().encode(), "text/html; charset=utf-8")
            elif parsed.path == "/api/cases":
                self.execute("cases", parse_open_request(parsed.query, source_ids))
            else:
                self.respond(404, b"{}")

        def do_POST(self) -> None:
            if (
                self.headers.get("Origin") != origin
                or self.headers.get("Host") != f"127.0.0.1:{port}"
            ):
                self.respond(403, b"{}")
                return
            name = self.path.removeprefix("/api/")
            if name not in {
                "proposals",
                "policy",
                "runs",
                "keep",
                "set",
                "compare",
                "experiment",
                "experiment_status",
                "handoff",
            }:
                self.respond(404, b"{}")
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 40000:
                    raise ValueError("Invalid request size")
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict):
                    raise ValueError("Expected an object")
            except (ValueError, json.JSONDecodeError):
                self.respond(400, b'{"ok":false,"error":{"message":"Invalid input."}}')
                return
            self.execute(name, payload)

    print(f"Scenario editor: {origin}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8876)
    parser.add_argument("--server")
    parser.add_argument("--sessions", nargs="+", default=[])
    args = parser.parse_args()
    serve(args.port, args.server, args.sessions)


if __name__ == "__main__":
    main()
