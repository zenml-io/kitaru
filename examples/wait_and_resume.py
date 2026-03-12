"""Compatibility wrapper.

Re-exports `examples.execution_management.wait_and_resume` while preserving the
old module-level monkeypatch surface used by unit tests.
"""

from __future__ import annotations

# ruff: noqa: F403
import threading
import time
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


def _watch_and_print_unblock_commands(
    *,
    client: KitaruClient,
    topic: str,
    stop_event: threading.Event,
) -> None:
    """Delegate to the grouped implementation with wrapper-visible globals."""
    _IMPL_MODULE._find_pending_wait_for_topic = _find_pending_wait_for_topic
    _IMPL_MODULE.time = time
    _IMPL_WATCH_AND_PRINT(client=client, topic=topic, stop_event=stop_event)


def run_workflow(
    topic: str | None = None,
    *,
    approve: bool = True,
    wait_discovery_timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str, str]:
    """Run the grouped implementation while honoring wrapper monkeypatches."""
    _IMPL_MODULE._prime_zenml_runtime = _prime_zenml_runtime
    _IMPL_MODULE._start_flow_in_background = _start_flow_in_background
    _IMPL_MODULE._wait_for_pending_wait = _wait_for_pending_wait
    _IMPL_MODULE.KitaruClient = KitaruClient
    _IMPL_MODULE.time = time
    _IMPL_MODULE.threading = threading
    _IMPL_MODULE.wait_for_approval_flow = wait_for_approval_flow
    return _IMPL_RUN_WORKFLOW(
        topic=topic,
        approve=approve,
        wait_discovery_timeout_seconds=wait_discovery_timeout_seconds,
    )


def run_workflow_interactive(topic: str | None = None) -> str:
    """Run the grouped implementation while honoring wrapper monkeypatches."""
    _IMPL_MODULE._prime_zenml_runtime = _prime_zenml_runtime
    _IMPL_MODULE._watch_and_print_unblock_commands = _watch_and_print_unblock_commands
    _IMPL_MODULE.KitaruClient = KitaruClient
    _IMPL_MODULE.time = time
    _IMPL_MODULE.threading = threading
    _IMPL_MODULE.wait_for_approval_flow = wait_for_approval_flow
    return _IMPL_RUN_WORKFLOW_INTERACTIVE(topic=topic)


def main() -> None:
    """Run the example as a script."""
    _IMPL_MODULE.run_workflow = run_workflow
    _IMPL_MODULE.run_workflow_interactive = run_workflow_interactive
    _IMPL_MODULE._watch_and_print_unblock_commands = _watch_and_print_unblock_commands
    _impl.main()


if __name__ == "__main__":
    main()
