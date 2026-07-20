"""Local HTTP API used by reference-agent tools."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

from .config import FIXTURES_DIR
from .db import load_seed


class MockApiServer:
    """Small localhost API server for support evidence reads."""

    def __init__(self, seed_path: Path = FIXTURES_DIR / "seed_data.json") -> None:
        self.seed_path = seed_path
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        """Return the server URL after start."""
        if self._server is None:
            raise RuntimeError("MockApiServer has not been started")
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> "MockApiServer":
        """Start the server in a background thread."""
        seed = load_seed(self.seed_path)
        handler = _build_handler(seed)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="reference-agent-mock-api",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop the server if it is running."""
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None

    def __enter__(self) -> "MockApiServer":
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()


def fetch_json(base_url: str, path: str, params: dict[str, str]) -> dict[str, Any]:
    """Fetch JSON from the local API using only the standard library."""
    query = urlencode(params)
    with urlopen(f"{base_url}{path}?{query}", timeout=5) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("Expected JSON object response")
    return data


def _build_handler(seed: dict[str, Any]) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
            if parsed.path == "/status":
                body = _status_response(seed, params)
            elif parsed.path == "/usage":
                body = _lookup_response(seed["usage"], params.get("customer_id"))
            elif parsed.path == "/billing":
                body = _lookup_response(seed["billing"], params.get("customer_id"))
            elif parsed.path == "/entitlements":
                body = _lookup_response(seed["entitlements"], params.get("customer_id"))
            elif parsed.path == "/seats":
                body = _lookup_response(seed["seat_usage"], params.get("customer_id"))
            else:
                self.send_response(404)
                self.end_headers()
                return
            encoded = json.dumps(body, sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return Handler


def _status_response(seed: dict[str, Any], params: dict[str, str]) -> dict[str, Any]:
    service = params.get("service", "")
    normalized = service.strip().lower()
    aliases = {
        "export api": "exports",
        "export-service": "exports",
        "export_api": "exports",
        "exporter": "exports",
        "exports api": "exports",
        "sso": "sso",
    }
    lookup_key = aliases.get(normalized, normalized)
    if lookup_key.startswith("export"):
        lookup_key = "exports"
    return dict(
        seed["service_status"].get(lookup_key, {"service": service, "ok": True})
    )


def _lookup_response(records: dict[str, Any], key: str | None) -> dict[str, Any]:
    if key is None:
        return {"found": False, "error": "missing customer_id"}
    return dict(records.get(key, {"found": False, "customer_id": key}))
