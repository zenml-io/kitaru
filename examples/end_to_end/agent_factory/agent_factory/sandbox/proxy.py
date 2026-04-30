"""DockerProxy — mitmproxy in a container, injects auth headers per host.

The credential injection model (kami's two-process pattern):

- Worker container runs the agent's shell commands and has NO secrets.
- Proxy container runs mitmdump, holds the credential map in
  `AGENT_FACTORY_CREDENTIALS`, and injects matching headers on outbound
  requests it intercepts.
- Worker is wired to the proxy via `http_proxy` / `https_proxy` env
  vars + a self-signed CA cert it trusts.
- Authentication between worker and proxy uses a per-run bearer token
  (basic-auth-as-bearer pattern, ported from kami) so other processes
  on the host can't accidentally use the proxy.

Ported from `kami_agent/sandbox/proxy.py:ProxySandbox`. Modal-specific
30 lines (`modal.Sandbox.create`, tunnels, blind sleep) become
subprocess.run(`docker run`) + Docker network DNS + a readiness poll.
"""

import json
import secrets
import subprocess
import time
from pathlib import Path
from types import TracebackType

from kitaru.errors import KitaruRuntimeError

from .certs import (
    cert_dir,
    ensure_certs,
    public_cert_path,
)

_PROXY_IMAGE = "agent-factory-proxy"
_PROXY_NETWORK = "agent_factory"
_PROXY_CONTAINER_NAME = "agent_factory_proxy"
_PROXY_LISTEN_PORT = 8080
_STOP_TIMEOUT_SECONDS = 2
_READY_POLL_INTERVAL = 0.1
_READY_TIMEOUT_SECONDS = 10.0


class DockerProxy:
    """A mitmproxy container that injects auth headers based on host patterns."""

    def __init__(self, *, credential_map: dict[str, dict[str, str]]) -> None:
        """Initialize.

        Args:
            credential_map: ``{host_pattern: {header_name: header_value}}``.
                Resolved (no `{{ secret }}` templates) — the caller is
                responsible for resolving via `kitaru.secrets`.
        """
        self._credential_map = credential_map
        self._container_id: str | None = None
        # New per-run token; the worker embeds this as basic-auth-as-bearer
        # in its proxy URL so unrelated host processes can't use this proxy.
        self._proxy_token = secrets.token_urlsafe(32)

    @property
    def url(self) -> str:
        """The authenticated proxy URL the worker should use as `http_proxy`."""
        return f"http://{self._proxy_token}:@{_PROXY_CONTAINER_NAME}:{_PROXY_LISTEN_PORT}"

    @property
    def public_cert_path(self) -> Path:
        """Path to the public CA cert for the worker to trust."""
        return public_cert_path()

    def __enter__(self) -> "DockerProxy":
        self._ensure_image()
        self._ensure_network()
        ensure_certs()
        self._start_container()
        self._wait_until_ready()
        print(
            f"[proxy] Started container {(self._container_id or '')[:12]} "
            f"(image={_PROXY_IMAGE}, "
            f"injecting for hosts={sorted(self._credential_map)})"
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._container_id is not None:
            print(f"[proxy] Stopping container {self._container_id[:12]}")
        self._stop_container()

    # --- Lifecycle internals -------------------------------------------------

    def _ensure_image(self) -> None:
        result = subprocess.run(
            ["docker", "image", "inspect", _PROXY_IMAGE],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        raise KitaruRuntimeError(
            f"Proxy image {_PROXY_IMAGE!r} is not built locally. "
            "Build it once (run from the example root) with:\n\n"
            f"    docker build -t {_PROXY_IMAGE} "
            "-f docker/proxy.Dockerfile .\n\n"
            "Then re-run this stage."
        )

    def _ensure_network(self) -> None:
        result = subprocess.run(
            ["docker", "network", "inspect", _PROXY_NETWORK],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        subprocess.run(
            ["docker", "network", "create", _PROXY_NETWORK],
            check=True,
            capture_output=True,
        )

    def _start_container(self) -> None:
        # Replace any stale container with the same name (e.g. from a prior
        # crashed run). --rm doesn't catch the stale-container case.
        subprocess.run(
            ["docker", "rm", "-f", _PROXY_CONTAINER_NAME],
            capture_output=True,
        )
        completed = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                _PROXY_CONTAINER_NAME,
                "--network",
                _PROXY_NETWORK,
                "-e",
                f"AGENT_FACTORY_CREDENTIALS={json.dumps(self._credential_map)}",
                "-e",
                f"AGENT_FACTORY_PROXY_TOKEN={self._proxy_token}",
                # mitmproxy expects to find `mitmproxy-ca.pem` in its
                # `confdir` and writes runtime files there too (e.g.
                # `mitmproxy-dhparam.pem`), so the dir must be writable
                # for the non-root mitmproxy user.
                "-v",
                f"{cert_dir()}:/certs",
                _PROXY_IMAGE,
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to start proxy container: {completed.stderr.strip()}"
            )
        self._container_id = completed.stdout.strip()

    def _wait_until_ready(self) -> None:
        """Poll until mitmdump is bound to its listen port. Replaces kami's blind sleep(2)."""
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    _PROXY_CONTAINER_NAME,
                    "sh",
                    "-c",
                    f"nc -z localhost {_PROXY_LISTEN_PORT}",
                ],
                capture_output=True,
            )
            if result.returncode == 0:
                return
            time.sleep(_READY_POLL_INTERVAL)
        raise KitaruRuntimeError(
            f"proxy container did not become ready within "
            f"{_READY_TIMEOUT_SECONDS}s"
        )

    def _stop_container(self) -> None:
        if self._container_id is None:
            return
        subprocess.run(
            [
                "docker",
                "stop",
                "--time",
                str(_STOP_TIMEOUT_SECONDS),
                _PROXY_CONTAINER_NAME,
            ],
            capture_output=True,
            text=True,
        )
        self._container_id = None
