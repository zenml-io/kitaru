"""Live-server harness for Schemathesis fuzzing of the real Kitaru app.

Runs the real FastAPI app (real Postgres database, real migrations, real
auth) under uvicorn in a background thread so the lifespan runs exactly once
for the whole fuzzing session. An ASGI wrapper around the app captures every
unhandled exception (Starlette's ServerErrorMiddleware sends the 500 response
and then re-raises, so the wrapper sees the traceback) and appends it to a
JSONL file plus an in-memory list keyed by method+path.
"""

import asyncio
import json
import os
import socket
import threading
import time
import traceback
from typing import Any

import httpx
import uvicorn

from conftest import drop_test_database, local_settings
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService

CAPTURE_PATH = os.environ.get("KITARU_FUZZ_CAPTURE", "fuzz-exceptions.jsonl")


class ExceptionCaptureApp:
    """ASGI wrapper recording unhandled exceptions escaping the app."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send)
        except Exception as exc:
            headers = {
                k.decode("latin1").lower(): v.decode("latin1")
                for k, v in scope.get("headers", [])
            }
            record = {
                "method": scope.get("method"),
                "path": scope.get("path"),
                "query": (scope.get("query_string") or b"").decode(
                    "latin1", "replace"
                ),
                "case_id": headers.get("x-schemathesis-testcaseid"),
                "exc_type": type(exc).__name__,
                "exc_msg": str(exc)[:800],
                "traceback": traceback.format_exc()[-6000:],
            }
            with self._lock:
                self.records.append(record)
                with open(CAPTURE_PATH, "a") as fh:
                    fh.write(json.dumps(record) + "\n")
            # The 500 response was already sent by ServerErrorMiddleware;
            # swallowing here keeps uvicorn's connection handling clean.


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class FuzzServer:
    """One real Kitaru server on a disposable database for a fuzz session."""

    settings: APISettings
    capture: ExceptionCaptureApp
    base_url: str
    token: str

    def start(self) -> None:
        self.settings = local_settings(
            use_db=True,
            DEFAULT_ACCOUNT_PASSWORD="fuzz-secret",
            TASK_SWEEP_INTERVAL_SECONDS=0,
        )
        asyncio.run(DatabaseService.create_db(self.settings))
        app = create_app(self.settings)
        self.capture = ExceptionCaptureApp(app)
        port = _find_free_port()
        config = uvicorn.Config(
            self.capture,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 120
        while not self._server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("uvicorn failed to start within 120s")
            if not self._thread.is_alive():
                raise RuntimeError("uvicorn thread died during startup")
            time.sleep(0.05)
        self.base_url = f"http://127.0.0.1:{port}"
        response = httpx.post(
            f"{self.base_url}/api/v1/login",
            data={"username": "default", "password": "fuzz-secret"},
            timeout=30,
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]

    @property
    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=30)
        asyncio.run(drop_test_database(self.settings))
