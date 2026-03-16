# ruff: noqa: E402
"""Kitaru: durable execution for AI agents.

Kitaru provides primitives for making AI agent workflows persistent,
replayable, and observable. Decorate your orchestration function with
``@flow`` and your work units with ``@checkpoint`` to get automatic
durability.

Example::

    from kitaru import flow, checkpoint

    @checkpoint
    def fetch_data(url: str) -> str:
        return requests.get(url).text

    @flow
    def my_agent(url: str) -> str:
        data = fetch_data(url)
        return data.upper()

Current status:

- Implemented: ``@flow``, ``@checkpoint``, ``kitaru.log()``,
  ``save()``, ``load()``, ``wait()``, ``llm()``, ``connect()``,
  ``configure()``, stack lifecycle helpers (``list_stacks()``,
  ``current_stack()``, ``use_stack()``, ``create_stack()``,
  ``delete_stack()``), model alias helpers via CLI
  (``kitaru model register/list``), ``KitaruClient`` execution/artifact APIs
  (`get/list/latest/logs/input/retry/resume/cancel/replay` + artifacts), and a typed
  Kitaru exception hierarchy with failure journaling (`Execution.failure`,
  `CheckpointCall.attempts`).
- Implemented: replay support (`KitaruClient.executions.replay(...)`).

The CLI also supports global runtime log-store configuration via
``kitaru log-store set/show/reset``, stack lifecycle via
``kitaru stack list/current/use/create/delete``, and execution lifecycle commands via
``kitaru executions get/list/logs/input/replay/retry/resume/cancel``.
"""

from ._env import apply_env_translations

apply_env_translations()

from kitaru.artifacts import load, save
from kitaru.checkpoint import checkpoint
from kitaru.client import KitaruClient
from kitaru.config import (
    ImageSettings,
    KitaruConfig,
    StackInfo,
    configure,
    connect,
    create_stack,
    current_stack,
    delete_stack,
    list_stacks,
    use_stack,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruContextError,
    KitaruDivergenceError,
    KitaruError,
    KitaruExecutionError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    KitaruUserCodeError,
    KitaruWaitValidationError,
)
from kitaru.flow import FlowHandle, flow
from kitaru.llm import llm
from kitaru.logging import log
from kitaru.wait import wait

__all__ = [
    "FailureOrigin",
    "FlowHandle",
    "ImageSettings",
    "KitaruBackendError",
    "KitaruClient",
    "KitaruConfig",
    "KitaruContextError",
    "KitaruDivergenceError",
    "KitaruError",
    "KitaruExecutionError",
    "KitaruFeatureNotAvailableError",
    "KitaruRuntimeError",
    "KitaruStateError",
    "KitaruUsageError",
    "KitaruUserCodeError",
    "KitaruWaitValidationError",
    "StackInfo",
    "checkpoint",
    "configure",
    "connect",
    "create_stack",
    "current_stack",
    "delete_stack",
    "flow",
    "list_stacks",
    "llm",
    "load",
    "log",
    "save",
    "use_stack",
    "wait",
]
