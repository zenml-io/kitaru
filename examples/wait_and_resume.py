"""Compatibility wrapper.

Re-exports `examples.execution_management.wait_and_resume` while preserving the
old module-level monkeypatch surface used by unit tests.
"""

from __future__ import annotations

# ruff: noqa: F403
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import examples.execution_management.wait_and_resume as _impl
from examples.execution_management.wait_and_resume import *
from examples.execution_management.wait_and_resume import (
    _WAIT_DISCOVERY_TIMEOUT_SECONDS,
    _find_pending_wait_for_topic,
    _prime_zenml_runtime,
    _start_flow_in_background,
    _wait_for_pending_wait,
    wait_for_approval_flow,
)
from kitaru.client import KitaruClient

_IMPL_MODULE = cast(Any, _impl)
_IMPL_RUN_WORKFLOW = _impl.run_workflow
_IMPL_RUN_WORKFLOW_INTERACTIVE = _impl.run_workflow_interactive
_IMPL_WATCH_AND_PRINT = _impl._watch_and_print_unblock_commands


@contextmanager
def _override_impl(**overrides: Any) -> Iterator[None]:
    """Temporarily override _IMPL_MODULE attributes, restoring on exit."""
    saved = {name: getattr(_IMPL_MODULE, name) for name in overrides}
    try:
        for name, value in overrides.items():
            setattr(_IMPL_MODULE, name, value)
        yield
    finally:
        for name, value in saved.items():
            setattr(_IMPL_MODULE, name, value)


def _watch_and_print_unblock_commands(
    *,
    client: KitaruClient,
    topic: str,
    stop_event: threading.Event,
) -> None:
    """Delegate to the grouped implementation with wrapper-visible globals."""
    with _override_impl(
        _find_pending_wait_for_topic=_find_pending_wait_for_topic,
        time=time,
    ):
        _IMPL_WATCH_AND_PRINT(client=client, topic=topic, stop_event=stop_event)


def run_workflow(
    topic: str | None = None,
    *,
    approve: bool = True,
    wait_discovery_timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str, str]:
    """Run the grouped implementation while honoring wrapper monkeypatches."""
    with _override_impl(
        _prime_zenml_runtime=_prime_zenml_runtime,
        _start_flow_in_background=_start_flow_in_background,
        _wait_for_pending_wait=_wait_for_pending_wait,
        KitaruClient=KitaruClient,
        time=time,
        threading=threading,
        wait_for_approval_flow=wait_for_approval_flow,
    ):
        return _IMPL_RUN_WORKFLOW(
            topic=topic,
            approve=approve,
            wait_discovery_timeout_seconds=wait_discovery_timeout_seconds,
        )


def run_workflow_interactive(topic: str | None = None) -> str:
    """Run the grouped implementation while honoring wrapper monkeypatches."""
    with _override_impl(
        _prime_zenml_runtime=_prime_zenml_runtime,
        _watch_and_print_unblock_commands=_watch_and_print_unblock_commands,
        KitaruClient=KitaruClient,
        time=time,
        threading=threading,
        wait_for_approval_flow=wait_for_approval_flow,
    ):
        return _IMPL_RUN_WORKFLOW_INTERACTIVE(topic=topic)


def main() -> None:
    """Run the example as a script."""
    with _override_impl(
        run_workflow=run_workflow,
        run_workflow_interactive=run_workflow_interactive,
        _watch_and_print_unblock_commands=_watch_and_print_unblock_commands,
    ):
        _impl.main()


if __name__ == "__main__":
    main()
