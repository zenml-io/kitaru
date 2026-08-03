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
"""Foreground worker orchestration and worker inspection commands."""

import asyncio
import json
import os
import shutil
import signal
import tempfile
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from pydantic import ValidationError

from kitaru.api_models.v1.task import LabelSelector, TaskKind, WorkerScope
from kitaru.api_models.v1.worker import WorkerListParams
from kitaru.cli.config import ResolvedTarget, resolve_credential
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.registration import resolve_asset
from kitaru.client.credential_store import DIRECTORY_MODE, FILE_MODE, CredentialStore

_ENV_MISSING = object()
_WORKER_CONTRACT_ENV = (
    "KITARU_API_URL",
    "KITARU_API_KEY",
    "KITARU_CREDENTIALS_PATH",
)


class WorkerRunner(Protocol):
    """Runtime surface used by foreground worker orchestration."""

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Run until the supplied event requests a drain."""


def load_worker_runtime() -> tuple[type[Any], type[Any]]:
    """Import worker-only dependencies when foreground execution is requested.

    Raises:
        CLIError: The optional worker dependencies are not installed.

    Returns:
        Worker and WorkerConfig classes.
    """
    try:
        from kitaru.worker import Worker, WorkerConfig
    except ModuleNotFoundError as error:
        if error.name == "pydantic_settings":
            raise CLIError(
                "invalid_configuration",
                "Worker runtime dependencies are not installed.",
                hint="Install kitaru[cli,worker].",
            ) from error
        raise
    return Worker, WorkerConfig


def build_worker_config(
    *,
    name: str | None = None,
    kinds: list[TaskKind] | None = None,
    selectors: list[str] | None = None,
    job_id: uuid.UUID | None = None,
    concurrency: int | None = None,
    claim_batch_size: int | None = None,
    poll_interval: float | None = None,
    heartbeat_interval: float | None = None,
    request_timeout: float | None = None,
    timeout: float | None = None,
    blob_cache_root: Path | None = None,
    payload_cache_root: Path | None = None,
    metadata: list[str] | None = None,
) -> Any:
    """Merge explicit CLI values over ``KITARU_WORKER_*`` settings.

    The optional job id refines the configured scope; it does not discard
    configured kinds or selectors.
    """
    _, config_type = load_worker_runtime()
    base = config_type()
    scope = WorkerScope(
        kinds=kinds if kinds is not None else base.scope.kinds,
        selectors=(
            _parse_selectors(selectors)
            if selectors is not None
            else base.scope.selectors
        ),
        job_id=job_id if job_id is not None else base.scope.job_id,
    )
    updates: dict[str, Any] = {"scope": scope}
    explicit = {
        "name": name,
        "concurrency": concurrency,
        "claim_batch_size": claim_batch_size,
        "poll_interval": poll_interval,
        "heartbeat_interval": heartbeat_interval,
        "request_timeout": request_timeout,
        "timeout": timeout,
        "blob_cache_root": blob_cache_root,
        "payload_cache_root": payload_cache_root,
    }
    updates.update({key: value for key, value in explicit.items() if value is not None})
    if metadata is not None:
        updates["metadata"] = {**base.metadata, **_parse_metadata(metadata)}
    return config_type.model_validate({**base.model_dump(), **updates})


def _parse_selectors(values: list[str]) -> list[LabelSelector]:
    """Parse JSON selector objects or compact ``KEY=VALUE[,VALUE]`` forms."""
    selectors: list[LabelSelector] = []
    for value in values:
        try:
            if value.lstrip().startswith("{"):
                selector = LabelSelector.model_validate(json.loads(value))
            else:
                key, separator, raw_values = value.partition("=")
                selected = [
                    item.strip() for item in raw_values.split(",") if item.strip()
                ]
                if not separator or not key.strip() or not selected:
                    raise ValueError("expected KEY=VALUE[,VALUE] or a JSON object")
                selector = LabelSelector(key=key.strip(), values=selected)
        except (json.JSONDecodeError, ValidationError, ValueError) as error:
            raise CLIError(
                "invalid_arguments", f"Invalid --selector {value!r}: {error}"
            ) from error
        selectors.append(selector)
    return selectors


def _parse_metadata(values: list[str]) -> dict[str, str]:
    """Parse repeatable worker metadata entries."""
    metadata: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key.strip():
            raise CLIError(
                "invalid_arguments", "--metadata entries must use KEY=VALUE."
            )
        metadata[key.strip()] = item
    return metadata


@contextmanager
def worker_contract_environment(
    target: ResolvedTarget, credential_store: CredentialStore
) -> Iterator[Callable[[], None]]:
    """Set an isolated foreground-worker environment and restore it afterward."""
    original = {
        name: os.environ.get(name, _ENV_MISSING) for name in _WORKER_CONTRACT_ENV
    }
    credential = resolve_credential(target.server_url, credential_store)
    with tempfile.TemporaryDirectory(prefix="kitaru-worker-credentials-") as directory:
        directory_path = Path(directory)
        if os.name == "posix":
            os.chmod(directory_path, DIRECTORY_MODE)
        credentials_path = directory_path / "credentials.json"
        payload = {}
        if credential.stored is not None:
            payload[target.server_url] = credential.stored.model_dump(
                mode="json", exclude_none=True
            )
            control_plane_url = credential.stored.control_plane_api_url
            if control_plane_url is not None:
                control_plane = credential_store.get(control_plane_url)
                if control_plane is not None:
                    payload[control_plane_url] = control_plane.model_dump(
                        mode="json", exclude_none=True
                    )
        handle = os.open(
            credentials_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            FILE_MODE,
        )
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, sort_keys=True)
        if os.name == "posix":
            os.chmod(credentials_path, FILE_MODE)

        os.environ["KITARU_API_URL"] = target.server_url
        os.environ["KITARU_CREDENTIALS_PATH"] = str(credentials_path)
        if credential.source == "environment":
            assert credential.api_key is not None
            os.environ["KITARU_API_KEY"] = credential.api_key
        else:
            os.environ.pop("KITARU_API_KEY", None)
        try:
            yield lambda: _remove_directory(directory_path)
        finally:
            for name, value in original.items():
                if value is _ENV_MISSING:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = str(value)


def _remove_directory(path: Path) -> None:
    """Remove a scoped credential directory, including during emergency exit."""
    shutil.rmtree(path, ignore_errors=True)


class ForegroundWorkerProcess:
    """Run one worker with portable graceful and emergency signal semantics."""

    def __init__(
        self,
        worker: WorkerRunner,
        summary: dict[str, Any],
        *,
        immediate_exit: Callable[[int], Any] = os._exit,
        emergency_cleanup: Callable[[], None] | None = None,
    ) -> None:
        """Initialize a foreground lifecycle around a reusable worker runtime."""
        self._worker = worker
        self._summary = summary
        self._immediate_exit = immediate_exit
        self._emergency_cleanup = emergency_cleanup
        self.stop = asyncio.Event()
        self.interrupted = False
        self._signal_handlers: dict[int, Any] = {}

    async def run(self) -> CommandResult:
        """Run the worker until natural completion or a graceful drain."""
        emit_event("starting", self._summary)
        with self._installed_signal_handlers():
            await self._worker.run(self.stop)
        item = {
            **self._summary,
            "status": "stopped",
            "server_record": "retained_until_stale",
        }
        return CommandResult(
            item=item,
            event="stopped",
            exit_code=130 if self.interrupted else 0,
        )

    def handle_sigint(self, *_: Any) -> None:
        """Drain on the first SIGINT and exit without cleanup on the second."""
        if self.interrupted:
            if self._emergency_cleanup is not None:
                self._emergency_cleanup()
            self._immediate_exit(130)
            return
        self.interrupted = True
        if not self.stop.is_set():
            self.stop.set()
            emit_event("draining", {"reason": "sigint"})

    def handle_sigterm(self, *_: Any) -> None:
        """Request the same graceful drain for SIGTERM."""
        if not self.stop.is_set():
            self.stop.set()
            emit_event("draining", {"reason": "sigterm"})

    @contextmanager
    def _installed_signal_handlers(self) -> Iterator[None]:
        """Install available process signals and restore the prior handlers."""
        handlers: list[tuple[int, Callable[..., None]]] = [
            (signal.SIGINT, self.handle_sigint)
        ]
        if hasattr(signal, "SIGTERM"):
            handlers.append((signal.SIGTERM, self.handle_sigterm))
        try:
            for signum, handler in handlers:
                self._signal_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, handler)
            yield
        finally:
            for signum, handler in self._signal_handlers.items():
                signal.signal(signum, handler)
            self._signal_handlers.clear()


async def start_worker(
    target: ResolvedTarget,
    credential_store: CredentialStore,
    **options: Any,
) -> CommandResult:
    """Build and run the generic worker without creating durable CLI state."""
    worker_type, _ = load_worker_runtime()
    config = build_worker_config(**options)
    summary = _worker_summary(config)
    with worker_contract_environment(target, credential_store) as emergency_cleanup:
        process = ForegroundWorkerProcess(
            worker_type(config), summary, emergency_cleanup=emergency_cleanup
        )
        return await process.run()


def _worker_summary(config: Any) -> dict[str, Any]:
    """Return a non-secret lifecycle projection of worker configuration."""
    return {
        "name": config.name,
        "kinds": (
            [kind.value for kind in config.scope.kinds]
            if config.scope.kinds is not None
            else None
        ),
        "selectors": (
            [selector.model_dump(mode="json") for selector in config.scope.selectors]
            if config.scope.selectors is not None
            else None
        ),
        "job_id": str(config.scope.job_id) if config.scope.job_id else None,
        "concurrency": config.concurrency,
        "claim_batch_size": config.claim_batch_size,
    }


async def list_workers(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of workers with explicit live/stale wording."""
    params = WorkerListParams(size=size, cursor=cursor, sort=sort, filter=filter)
    page = await client.workers.list(params)
    return CommandResult(
        items=[_worker_item(item) for item in page.items],
        page={
            "limit": size,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor is not None,
        },
    )


async def get_worker(client: Any, reference: str) -> CommandResult:
    """Get a worker by exact UUID or exact case-sensitive name."""
    item = await resolve_asset(client.workers, reference, "Worker")
    return CommandResult(item=_worker_item(item))


def _worker_item(worker: Any) -> dict[str, Any]:
    """Project a worker response with human-safe liveness terminology."""
    item = worker.model_dump(mode="json")
    item["status"] = "live" if worker.live else "stale"
    return item
