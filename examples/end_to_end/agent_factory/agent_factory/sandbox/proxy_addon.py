"""mitmproxy addon for the DockerProxy.

Loaded by mitmdump running inside the proxy container. Never imported
by the host process. Credentials reach this script via the
`AGENT_FACTORY_CREDENTIALS` env var (JSON: `{host: {header: value}}`),
authentication of incoming proxy requests goes through the
`AGENT_FACTORY_PROXY_TOKEN` (basic-auth-as-bearer pattern, same as kami).

Ported near-verbatim from `kami_agent/sandbox/proxy_addon.py` — only
the env-var names and the log prefix were renamed.
"""

import base64
import binascii
import json
import os

from mitmproxy import http
from mitmproxy.http import HTTPFlow

_CREDENTIALS: dict[str, dict[str, str]] = json.loads(
    os.environ.get("AGENT_FACTORY_CREDENTIALS", "{}")
)
_PROXY_TOKEN = os.environ.get("AGENT_FACTORY_PROXY_TOKEN", "")
_PROXY_AUTH_CHALLENGE_HEADERS = {
    "Proxy-Authenticate": 'Basic realm="agent-factory-proxy"'
}

if _CREDENTIALS:
    print(
        f"[agent-factory-proxy] credentials loaded for hosts: {sorted(_CREDENTIALS)}",
        flush=True,
    )
else:
    print(
        "[agent-factory-proxy] WARNING: AGENT_FACTORY_CREDENTIALS not set "
        "— no headers will be injected",
        flush=True,
    )

if _PROXY_TOKEN:
    print("[agent-factory-proxy] proxy token configured", flush=True)
else:
    print(
        "[agent-factory-proxy] WARNING: AGENT_FACTORY_PROXY_TOKEN not set "
        "— rejecting all requests",
        flush=True,
    )


def _extract_proxy_username(proxy_auth_header: str) -> str | None:
    """Decode the username from a Basic Proxy-Authorization header."""
    if not proxy_auth_header.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(proxy_auth_header[6:]).decode("utf-8")
    except (ValueError, UnicodeDecodeError, binascii.Error):
        return None
    username, _, _ = decoded.partition(":")
    return username or None


class CredentialAddon:
    """Per-connection proxy auth + per-request header injection."""

    def __init__(self) -> None:
        self._authorized_client_connections: set[str] = set()

    def _authorize_or_reject(self, flow: HTTPFlow) -> bool:
        conn_id = flow.client_conn.id
        if not _PROXY_TOKEN:
            flow.response = http.Response.make(
                407,
                b"Proxy authentication is required.\n",
                _PROXY_AUTH_CHALLENGE_HEADERS,
            )
            return False
        proxy_auth_header = flow.request.headers.get("Proxy-Authorization", "")
        if proxy_auth_header:
            provided_token = _extract_proxy_username(proxy_auth_header)
            if provided_token != _PROXY_TOKEN:
                flow.response = http.Response.make(
                    407,
                    b"Invalid proxy credentials.\n",
                    _PROXY_AUTH_CHALLENGE_HEADERS,
                )
                return False
            self._authorized_client_connections.add(conn_id)
            # Strip the proxy auth so we don't leak it upstream.
            flow.request.headers.pop("Proxy-Authorization", None)
            return True
        if conn_id in self._authorized_client_connections:
            return True
        flow.response = http.Response.make(
            407,
            b"Proxy authentication is required.\n",
            _PROXY_AUTH_CHALLENGE_HEADERS,
        )
        return False

    def http_connect(self, flow: HTTPFlow) -> None:
        if not self._authorize_or_reject(flow):
            return

    def request(self, flow: HTTPFlow) -> None:
        if not self._authorize_or_reject(flow):
            return
        host = flow.request.pretty_host.rstrip(".").lower()
        for pattern, headers in _CREDENTIALS.items():
            normalized_pattern = pattern.rstrip(".").lower()
            if host == normalized_pattern or host.endswith(f".{normalized_pattern}"):
                for header_name, header_value in headers.items():
                    flow.request.headers[header_name] = header_value
                print(
                    f"[agent-factory-proxy] injected headers for {host}: "
                    f"{sorted(headers)}",
                    flush=True,
                )
                return
        print(
            f"[agent-factory-proxy] no match for host={host!r} "
            f"known={sorted(_CREDENTIALS)}",
            flush=True,
        )


addons = [CredentialAddon()]
