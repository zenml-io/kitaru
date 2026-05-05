"""DockerProxy — mitmproxy in a container, injects auth headers per host.

The credential injection model (two-process pattern):

- Worker container runs the agent's shell commands and has NO secrets.
- Proxy container runs mitmdump, holds the credential map in
  `AGENT_FACTORY_CREDENTIALS`, and injects matching headers on outbound
  requests it intercepts.
- Worker is wired to the proxy via `http_proxy` / `https_proxy` env
  vars + a self-signed CA cert it trusts.
- Authentication between worker and proxy uses a per-run bearer token
  (basic-auth-as-bearer pattern) so other processes on the host can't
  accidentally use the proxy.
"""

import json
import secrets
import subprocess
import time
from pathlib import Path
from types import TracebackType

from kitaru.errors import KitaruRuntimeError

from .._docker import ensure_image, ensure_network, stop_container
from .certs import (
    cert_dir,
    ensure_certs,
    public_cert_path,
)

_PROXY_IMAGE = "agent-factory-proxy"
_PROXY_NETWORK = "agent_factory"
_PROXY_CONTAINER_NAME_PREFIX = "agent_factory_proxy_"
_PROXY_LISTEN_PORT = 8080
_STOP_TIMEOUT_SECONDS = 2
_READY_POLL_INTERVAL = 0.1
_READY_TIMEOUT_SECONDS = 10.0


class DockerProxy:
    """A mitmproxy container that injects auth headers based on host patterns."""

    def __init__(
        self,
        *,
        credential_map: dict[str, dict[str, str]],
        execution_id: str,
    ) -> None:
        """Initialize.

        Args:
            credential_map: ``{host_pattern: {header_name: header_value}}``.
                Resolved (no `{{ secret }}` templates) — the caller is
                responsible for resolving via `kitaru.secrets`.
            execution_id: Suffixed onto the container name so concurrent
                flow runs don't collide on a fixed name.
        """
        self._credential_map = credential_map
        self._container_name = f"{_PROXY_CONTAINER_NAME_PREFIX}{execution_id}"
        self._container_id: str | None = None
        # New per-run token; the worker embeds this as basic-auth-as-bearer
        # in its proxy URL so unrelated host processes can't use this proxy.
        self._proxy_token = secrets.token_urlsafe(32)

    @property
    def url(self) -> str:
        """The authenticated proxy URL the worker should use as `http_proxy`."""
        return (
            f"http://{self._proxy_token}:@{self._container_name}:{_PROXY_LISTEN_PORT}"
        )

    @property
    def public_cert_path(self) -> Path:
        """Path to the public CA cert for the worker to trust."""
        return public_cert_path()

    def __enter__(self) -> "DockerProxy":
        ensure_image(
            _PROXY_IMAGE,
            "Build it once (run from the example root) with:\n\n"
            f"    docker build -t {_PROXY_IMAGE} "
            "-f docker/proxy.Dockerfile .\n\n"
            "Then re-run this stage.",
        )
        ensure_network(_PROXY_NETWORK)
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
            stop_container(self._container_name, timeout_seconds=_STOP_TIMEOUT_SECONDS)
            self._container_id = None

    # --- Lifecycle internals -------------------------------------------------

    def _start_container(self) -> None:
        # Container name is per-execution (suffixed with execution_id), so
        # we don't risk killing another concurrent run's proxy. --rm cleans
        # up our own container on stop.
        completed = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                self._container_name,
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
        """Poll until mitmdump is bound to its listen port."""
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    self._container_name,
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
            f"proxy container did not become ready within {_READY_TIMEOUT_SECONDS}s"
        )
