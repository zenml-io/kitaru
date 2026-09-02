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
"""Live server on a disposable database for API fuzzing."""

import asyncio
import json
import os
import socket
import threading
import time
import traceback
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from conftest import drop_test_database, local_settings
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService

DEFAULT_ACCOUNT_PASSWORD = "fuzz-secret"
STARTUP_TIMEOUT_SECONDS = 120


class ExceptionCaptureApp:
    """ASGI wrapper recording unhandled exceptions escaping the app.

    A 500 response body carries no detail, so a fuzz failure would otherwise
    name an endpoint without naming the exception behind it. Starlette's
    ServerErrorMiddleware sends the response and then re-raises, so wrapping
    the app outside it observes the traceback without changing the reply.
    """

    def __init__(self, app: Any, capture_path: Path | None = None) -> None:
        self.app = app
        self.capture_path = capture_path
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send)
        except Exception as exc:
            self._record(scope, exc)

    def _record(self, scope: Any, exc: Exception) -> None:
        record = {
            "method": scope.get("method"),
            "path": scope.get("path"),
            "query": (scope.get("query_string") or b"").decode("latin1", "replace"),
            "exc_type": type(exc).__name__,
            "exc_msg": str(exc)[:800],
            "traceback": traceback.format_exc()[-6000:],
        }
        with self._lock:
            self.records.append(record)
            if self.capture_path is not None:
                with self.capture_path.open("a") as handle:
                    handle.write(json.dumps(record) + "\n")

    def describe(self, method: str, path: str) -> str:
        """Summarize captured exceptions for one operation."""
        with self._lock:
            matches = [
                f"{record['exc_type']}: {record['exc_msg']}"
                for record in self.records
                if record["method"] == method and record["path"] == path
            ]
        return "\n".join(dict.fromkeys(matches))


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class FuzzServer:
    """One real Kitaru server on a disposable database for a fuzz session.

    The app runs under uvicorn in a background thread rather than through an
    in-process ASGI transport because those transports enter and exit their
    client context per request, which reruns the lifespan — Alembic
    migrations included — for every generated example.
    """

    settings: APISettings
    capture: ExceptionCaptureApp
    base_url: str
    token: str

    def __init__(self) -> None:
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Create the database, boot the app, and log in as the admin account.

        A failure after the database exists still drops it, so a broken
        migration or login does not leave one disposable database per attempt.
        """
        self.settings = local_settings(
            use_db=True,
            DEFAULT_ACCOUNT_PASSWORD=DEFAULT_ACCOUNT_PASSWORD,
            TASK_SWEEP_INTERVAL_SECONDS=0,
        )
        asyncio.run(DatabaseService.create_db(self.settings))
        try:
            self._boot()
        except Exception:
            self.stop()
            raise

    def _boot(self) -> None:
        capture_path = os.environ.get("KITARU_FUZZ_CAPTURE")
        self.capture = ExceptionCaptureApp(
            create_app(self.settings),
            Path(capture_path) if capture_path else None,
        )
        port = _find_free_port()
        self._server = uvicorn.Server(
            uvicorn.Config(
                self.capture,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                access_log=False,
            )
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while not self._server.started:
            if not self._thread.is_alive():
                raise RuntimeError("uvicorn thread died during startup")
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"uvicorn did not start within {STARTUP_TIMEOUT_SECONDS}s"
                )
            time.sleep(0.05)
        self.base_url = f"http://127.0.0.1:{port}"
        response = httpx.post(
            f"{self.base_url}/api/v1/login",
            data={"username": "default", "password": DEFAULT_ACCOUNT_PASSWORD},
            timeout=30,
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]

    @property
    def auth_headers(self) -> dict[str, str]:
        """Return the bearer header for the bootstrapped admin account."""
        return {"Authorization": f"Bearer {self.token}"}

    def stop(self) -> None:
        """Shut the server down and drop the disposable database.

        Safe to call on a server that only got partway through ``start``.
        """
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=30)
        asyncio.run(drop_test_database(self.settings))
