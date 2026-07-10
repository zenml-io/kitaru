# ruff: noqa: E402
"""Kitaru: durable execution for AI agents.

Kitaru provides primitives for making AI agent workflows persistent,
replayable, and observable. Decorate your orchestration function with
``@flow`` and your work units with ``@checkpoint`` to get automatic
durability.

Example:
    ```python
    from kitaru import flow, checkpoint

    @checkpoint
    def fetch_data(url: str) -> str:
        return requests.get(url).text

    @flow
    def my_agent(url: str) -> str:
        data = fetch_data(url)
        return data.upper()
    ```

Current status:

- Implemented: ``@flow``, ``@checkpoint``, ``kitaru.log()``,
  ``kitaru.progress()``, ``kitaru.events.publish()``,
  ``save()``, ``load()``, ``wait()``, ``llm()``, ``get_secret()``,
  ``create_secret()``, ``delete_secret()``, ``connect()``,
  ``configure()``, stack lifecycle helpers (``list_stacks()``,
  ``current_stack()``, ``use_stack()``, ``create_stack()``,
  ``delete_stack()``), model alias helpers via CLI
  (``kitaru model register/list``), ``KitaruClient`` execution/artifact APIs
  (`get/list/latest/logs/statistics/input/retry/resume/cancel/replay` +
  artifacts), a typed Kitaru exception hierarchy with failure journaling
  (``Execution.failure``, ``CheckpointCall.attempts``), and live-event watching
  (``KitaruClient.executions.events(...)``).
- Implemented: replay support (`KitaruClient.executions.replay(...)`).

The CLI also supports global runtime log-store configuration via
``kitaru log-store set/show/reset``, stack lifecycle via
``kitaru stack list/current/use/create/delete``, and execution lifecycle commands via
``kitaru executions get/list/logs/statistics/input/replay/retry/resume/cancel``.
"""

# ZenML must be imported explicitly here so that its init_logging() runs
# (installing console + storage handlers on the root logger) before we swap
# the console handler with Kitaru's terminal handler.
import zenml as _zenml  # noqa: F401

from ._terminal_logging import install_terminal_log_intercept

install_terminal_log_intercept()

import os

from kitaru.analytics import set_source

_default_analytics_source = os.environ.get(
    "KITARU_DEFAULT_ANALYTICS_SOURCE", "kitaru-python"
)
set_source(_default_analytics_source)

from kitaru._client._models import (
    AuthAPIKey,
    AuthAPIKeyWithValue,
    AuthServiceAccount,
    ExecutionEvent,
    ExecutionStatistics,
    ExecutionStatisticsDimension,
    ExecutionStatisticsGroup,
    ExecutionStatisticsGrouping,
    ExecutionStatisticsMetric,
    ExecutionStatisticsMetricAggregation,
    ExecutionStatisticsMetricSource,
    ExecutionStatisticsTimeGranularity,
)
from kitaru._interface_deployments import Deployment
from kitaru.artifacts import load, save
from kitaru.checkpoint import checkpoint
from kitaru.client import KitaruClient
from kitaru.cohort import CohortQuery, CohortResult, cohort
from kitaru.config import (
    ImageSettings,
    KitaruConfig,
    ProjectCreateResult,
    ProjectDeleteResult,
    ProjectInfo,
    SandboxCommandResult,
    StackInfo,
    configure,
    connect,
    create_project,
    create_stack,
    current_project,
    current_stack,
    delete_project,
    delete_stack,
    get_project,
    list_projects,
    list_stacks,
    run_sandbox_command,
    use_project,
    use_stack,
)
from kitaru.diff import (
    CohortDiff,
    ExecutionDiff,
    build_compare_url,
    build_compare_url_for_executions,
    build_compare_urls,
    compare_url_for_executions,
    diff,
    diff_cohort,
    diff_matrix,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruAmbiguousFlowResultError,
    KitaruBackendError,
    KitaruContextError,
    KitaruDivergenceError,
    KitaruError,
    KitaruExecutionError,
    KitaruFeatureNotAvailableError,
    KitaruLogRetrievalError,
    KitaruRuntimeError,
    KitaruStackIntegrationDependencyError,
    KitaruStateError,
    KitaruTimeoutError,
    KitaruUsageError,
    KitaruUserCodeError,
    KitaruWaitValidationError,
)
from kitaru.events import progress
from kitaru.flow import FlowHandle, flow
from kitaru.llm import llm
from kitaru.logging import log
from kitaru.replay import ReplaySubmission
from kitaru.replay_context import (
    ReplayRuntimeContext,
    get_replay_runtime_context,
    is_replay,
)
from kitaru.runtime import current_execution_id
from kitaru.secrets import (
    Secret,
    SecretSummary,
    create_secret,
    delete_secret,
    get_secret,
)
from kitaru.wait import wait

from . import events as events

__all__ = [
    "AuthAPIKey",
    "AuthAPIKeyWithValue",
    "AuthServiceAccount",
    "CohortDiff",
    "CohortQuery",
    "CohortResult",
    "Deployment",
    "ExecutionDiff",
    "ExecutionEvent",
    "ExecutionStatistics",
    "ExecutionStatisticsDimension",
    "ExecutionStatisticsGroup",
    "ExecutionStatisticsGrouping",
    "ExecutionStatisticsMetric",
    "ExecutionStatisticsMetricAggregation",
    "ExecutionStatisticsMetricSource",
    "ExecutionStatisticsTimeGranularity",
    "FailureOrigin",
    "FlowHandle",
    "ImageSettings",
    "KitaruAmbiguousFlowResultError",
    "KitaruBackendError",
    "KitaruClient",
    "KitaruConfig",
    "KitaruContextError",
    "KitaruDivergenceError",
    "KitaruError",
    "KitaruExecutionError",
    "KitaruFeatureNotAvailableError",
    "KitaruLogRetrievalError",
    "KitaruRuntimeError",
    "KitaruStackIntegrationDependencyError",
    "KitaruStateError",
    "KitaruTimeoutError",
    "KitaruUsageError",
    "KitaruUserCodeError",
    "KitaruWaitValidationError",
    "ProjectCreateResult",
    "ProjectDeleteResult",
    "ProjectInfo",
    "ReplayRuntimeContext",
    "ReplaySubmission",
    "SandboxCommandResult",
    "Secret",
    "SecretSummary",
    "StackInfo",
    "build_compare_url",
    "build_compare_url_for_executions",
    "build_compare_urls",
    "checkpoint",
    "cohort",
    "compare_url_for_executions",
    "configure",
    "connect",
    "create_project",
    "create_secret",
    "create_stack",
    "current_execution_id",
    "current_project",
    "current_stack",
    "delete_project",
    "delete_secret",
    "delete_stack",
    "diff",
    "diff_cohort",
    "diff_matrix",
    "events",
    "flow",
    "get_project",
    "get_replay_runtime_context",
    "get_secret",
    "is_replay",
    "list_projects",
    "list_stacks",
    "llm",
    "load",
    "log",
    "progress",
    "run_sandbox_command",
    "save",
    "use_project",
    "use_stack",
    "wait",
]
