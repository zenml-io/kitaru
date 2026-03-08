"""Kitaru: durable execution for AI agents.

Kitaru provides primitives for making AI agent workflows persistent,
replayable, and observable. Decorate your orchestration function with
``@kitaru.flow`` and your work units with ``@kitaru.checkpoint`` to
get automatic durability.

Example::

    import kitaru

    @kitaru.checkpoint
    def fetch_data(url: str) -> str:
        return requests.get(url).text

    @kitaru.flow
    def my_agent(url: str) -> str:
        data = fetch_data(url)
        return data.upper()

Current status:

- Implemented: ``@kitaru.flow``, ``@kitaru.checkpoint``, ``kitaru.log()``,
  ``save()``, ``load()``, ``wait()``, ``llm()``, ``connect()``,
  ``configure()``, stack selection helpers (``list_stacks()``,
  ``current_stack()``, ``use_stack()``), model alias helpers via CLI
  (``kitaru model register/list``), ``KitaruClient`` execution/artifact APIs
  (`get/list/latest/input/retry/resume/cancel` + artifacts), and a typed
  Kitaru exception hierarchy with failure journaling (`Execution.failure`,
  `CheckpointCall.attempts`).
- In progress: replay support (`KitaruClient.executions.replay(...)`).

The CLI also supports global runtime log-store configuration via
``kitaru log-store set/show/reset``, stack selection via
``kitaru stack list/current/use``, and execution lifecycle commands via
``kitaru run`` plus ``kitaru executions get/list/input/retry/resume/cancel``.
"""

from kitaru.artifacts import load, save
from kitaru.checkpoint import checkpoint
from kitaru.client import KitaruClient
from kitaru.config import (
    ImageSettings,
    KitaruConfig,
    StackInfo,
    configure,
    connect,
    current_stack,
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
    "current_stack",
    "flow",
    "list_stacks",
    "llm",
    "load",
    "log",
    "save",
    "use_stack",
    "wait",
]
