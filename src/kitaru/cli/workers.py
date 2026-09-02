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
import logging
import os
import signal
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import ValidationError

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    LabelSelector,
    WorkerClaim,
    WorkerListParams,
    WorkerScope,
)
from kitaru.cli.config import ResolvedTarget
from kitaru.cli.output import (
    CLIError,
    CommandResult,
    OutputContext,
    emit_event,
    get_output_context,
    reset_output_context,
    set_output_context,
)
from kitaru.cli.registration import build_list_params

_ENV_MISSING = object()
_WORKER_CONTRACT_ENV = (
    "KITARU_API_URL",
    "KITARU_API_KEY",
    "KITARU_CREDENTIALS_PATH",
)
_TerminationReason = Literal["completed", "sigint", "sigterm"]
_TERMINATION_EXIT_CODES: dict[_TerminationReason, int] = {
    "completed": 0,
    "sigint": 130,
    "sigterm": 143,
}


class WorkerRunner(Protocol):
    """Runtime surface used by foreground worker orchestration."""

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Run until the supplied event requests a drain."""

    def cancel_inflight(self) -> None:
        """Request cancellation of every task the worker currently holds."""


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
    claims: list[str] | None = None,
    selectors: list[str] | None = None,
    job_id: uuid.UUID | None = None,
    concurrency: int | None = None,
    claim_batch_size: int | None = None,
    poll_interval: float | None = None,
    heartbeat_interval: float | None = None,
    timeout: float | None = None,
    drain_timeout: float | None = None,
    blob_cache_root: Path | None = None,
    payload_cache_root: Path | None = None,
    metadata: list[str] | None = None,
) -> Any:
    """Merge explicit CLI values over ``KITARU_WORKER_*`` settings.

    The optional job id refines the configured scope. It does not discard
    configured claims or selectors.
    """
    _, config_type = load_worker_runtime()
    base = config_type()
    scope = WorkerScope(
        claims=_parse_claims(claims) if claims is not None else base.scope.claims,
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
        "timeout": timeout,
        "drain_timeout": drain_timeout,
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


def _parse_claims(values: list[str]) -> list[WorkerClaim]:
    """Parse compact ``KIND`` or ``agent=AGENT_VERSION_ID`` claim forms."""
    claims: list[WorkerClaim] = []
    for value in values:
        try:
            kind_value, separator, raw_agent_version_id = value.partition("=")
            claim = WorkerClaim(
                kind=TaskKind(kind_value.strip()),
                agent_version_id=(
                    uuid.UUID(raw_agent_version_id.strip()) if separator else None
                ),
            )
        except (ValidationError, ValueError) as error:
            raise CLIError(
                "invalid_arguments", f"Invalid --claim {value!r}: {error}"
            ) from error
        claims.append(claim)
    return claims


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
    target: ResolvedTarget,
) -> Iterator[None]:
    """Set the unchanged worker environment contract and restore it afterward."""
    original = {
        name: os.environ.get(name, _ENV_MISSING) for name in _WORKER_CONTRACT_ENV
    }
    try:
        if target.source == "explicit":
            os.environ["KITARU_API_URL"] = target.server_url
        os.environ.pop("KITARU_CREDENTIALS_PATH", None)
        yield
    finally:
        for name, value in original.items():
            if value is _ENV_MISSING:
                os.environ.pop(name, None)
            else:
                os.environ[name] = str(value)


class ForegroundWorkerProcess:
    """Run one worker with portable graceful signal semantics."""

    def __init__(
        self,
        worker: WorkerRunner,
        summary: dict[str, Any],
    ) -> None:
        """Initialize a foreground lifecycle around a reusable worker runtime."""
        self._worker = worker
        self._summary = summary
        self.stop = asyncio.Event()
        self._stop_reason: _TerminationReason | None = None
        self._cancel_requested = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._output_context: OutputContext | None = None
        self._signal_handlers: dict[int, Any] = {}

    async def run(self) -> CommandResult:
        """Run the worker until natural completion or a graceful drain."""
        self._loop = asyncio.get_running_loop()
        self._output_context = get_output_context()
        emit_event("starting", self._summary)
        with self._installed_signal_handlers():
            await self._worker.run(self.stop)
        stop_reason: _TerminationReason = self._stop_reason or "completed"
        item = {
            **self._summary,
            "status": "stopped",
            "server_record": "retained_until_stale",
            "stop_reason": stop_reason,
        }
        return CommandResult(
            item=item,
            event="stopped",
            exit_code=_TERMINATION_EXIT_CODES[stop_reason],
        )

    def handle_sigint(self, *_: Any) -> None:
        """Request a drain on the first SIGINT, cancel held tasks on the second."""
        self._handle_signal("sigint")

    def handle_sigterm(self, *_: Any) -> None:
        """Request a drain on the first SIGTERM, cancel held tasks on the second."""
        self._handle_signal("sigterm")

    def _handle_signal(self, reason: _TerminationReason) -> None:
        """Escalate a graceful drain to canceling held tasks on repeated signals."""
        if self._stop_reason is None:
            self._request_stop(reason)
        elif not self._cancel_requested:
            self._request_cancel(reason)

    def _request_stop(self, reason: _TerminationReason) -> None:
        """Record the first signal and request a best-effort graceful drain."""
        self._stop_reason = reason
        self.stop.set()
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self.stop.set)
        self._emit_signal_event("draining", reason)

    def _request_cancel(self, reason: _TerminationReason) -> None:
        """Record the second signal and cancel every task the worker holds."""
        self._cancel_requested = True
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._worker.cancel_inflight)
        self._emit_signal_event("canceling", reason)

    def _emit_signal_event(self, event: str, reason: _TerminationReason) -> None:
        """Emit a signal lifecycle event, tolerating a closed output stream."""
        token = (
            set_output_context(self._output_context)
            if self._output_context is not None
            else None
        )
        try:
            emit_event(event, {"reason": reason})
        except (OSError, ValueError):
            pass
        finally:
            if token is not None:
                reset_output_context(token)

    @contextmanager
    def _installed_signal_handlers(self) -> Iterator[None]:
        """Install available process signals and restore the prior handlers."""
        from kitaru.worker.platforms import current_platform

        handlers: list[tuple[int, Callable[..., None]]] = [
            (
                signum,
                self.handle_sigterm if signum == signal.SIGTERM else self.handle_sigint,
            )
            for signum in current_platform().stop_signals()
        ]
        try:
            for signum, handler in handlers:
                self._signal_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, handler)
            yield
        finally:
            for signum, handler in self._signal_handlers.items():
                signal.signal(signum, handler)
            self._signal_handlers.clear()


def _configure_worker_logging(log_level: str) -> None:
    """Install a root logging handler at the requested level."""
    level = logging.getLevelNamesMapping().get(log_level.upper())
    if level is None:
        raise CLIError("invalid_arguments", f"Unknown log level: {log_level}")
    logging.basicConfig(
        level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )


async def start_worker(
    target: ResolvedTarget,
    log_level: str | None = None,
    **options: Any,
) -> CommandResult:
    """Build and run the generic worker without creating durable CLI state."""
    if log_level is not None:
        _configure_worker_logging(log_level)
    with worker_contract_environment(target):
        worker_type, _ = load_worker_runtime()
        config = build_worker_config(**options)
        summary = _worker_summary(config)
        process = ForegroundWorkerProcess(worker_type(config), summary)
        return await process.run()


def _worker_summary(config: Any) -> dict[str, Any]:
    """Return a non-secret lifecycle projection of worker configuration."""
    return {
        "name": config.name,
        "claims": [_claim_syntax(claim) for claim in config.scope.claims],
        "selectors": (
            [selector.model_dump(mode="json") for selector in config.scope.selectors]
            if config.scope.selectors is not None
            else None
        ),
        "job_id": str(config.scope.job_id) if config.scope.job_id else None,
        "concurrency": config.concurrency,
        "claim_batch_size": config.claim_batch_size,
    }


def _claim_syntax(claim: WorkerClaim) -> str:
    """Render a claim in the compact syntax ``--claim`` accepts."""
    if claim.agent_version_id is not None:
        return f"{claim.kind.value}={claim.agent_version_id}"
    return claim.kind.value


async def list_workers(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
    include_stale: bool = False,
) -> CommandResult:
    """List one server page of workers with explicit live/stale wording."""
    params = build_list_params(
        WorkerListParams,
        size=size,
        cursor=cursor,
        sort=sort,
        filter=filter,
    )
    # Leave include_stale unset unless it was asked for, so the default
    # listing stays a request a server without the parameter still accepts.
    if include_stale:
        params = params.model_copy(update={"include_stale": True})
    page = await client.workers.list(params)
    return CommandResult(
        items=[_worker_item(item) for item in page.items],
        page={
            "limit": size,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor is not None,
        },
    )


async def get_worker(client: Any, worker_id: uuid.UUID) -> CommandResult:
    """Get one worker by id."""
    item = await client.workers.get(worker_id)
    return CommandResult(item=_worker_item(item))


def _worker_item(worker: Any) -> dict[str, Any]:
    """Project a worker response with human-safe liveness terminology."""
    item = worker.model_dump(mode="json")
    item["status"] = "live" if worker.live else "stale"
    return item
