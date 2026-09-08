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
"""Analyzer plugin contract and the analysis flow."""

import inspect
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.insight import InsightInput
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.task import AnalysisTaskDetails, ScriptPluginSpec
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.plugins import PluginLoadError, load_plugin_entrypoint, load_source_ref
from kitaru.task.task_io import get_required_env, write_task_result

__all__ = [
    "AnalysisError",
    "AnalyzerReturn",
    "InsightInput",
    "call_analyzer",
    "run",
]

_LABEL = "Analyzer"


class AnalysisError(Exception):
    """Raised when loading or invoking an analyzer plugin fails."""


AnalyzerReturn = InsightInput | list[InsightInput]


async def call_analyzer(
    name: str,
    analyzer: Callable[..., AnalyzerReturn | Awaitable[AnalyzerReturn]],
    session_ids: list[uuid.UUID],
    params: dict[str, Any],
) -> list[InsightInput]:
    """Invoke an analyzer and validate its results.

    Args:
        name: Analyzer name, named in error messages.
        analyzer: Analyzer callable, sync or returning an awaitable.
        session_ids: Imported session IDs passed to the analyzer. Analyzers
            fetch any session details they need through the API client.
        params: Parameters passed to the analyzer.

    Raises:
        AnalysisError: The analyzer raised, returned a
            non-InsightInput value, or returned duplicate result names.

    Returns:
        Insight inputs.
    """
    try:
        result = analyzer(session_ids, **params)
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:
        raise AnalysisError(f"Analyzer '{name}' raised an error: {exc}") from exc
    results = result if isinstance(result, list) else [result]
    if not all(isinstance(item, InsightInput) for item in results):
        raise AnalysisError(f"Analyzer '{name}' returned a non-InsightInput value")
    names = [item.name for item in results]
    if len(set(names)) != len(names):
        raise AnalysisError(f"Analyzer '{name}' returned duplicate result names")
    return results


def _resolve_analyzer(
    details: AnalysisTaskDetails,
) -> Callable[..., AnalyzerReturn | Awaitable[AnalyzerReturn]]:
    """Load the analyzer callable named by a task's plugin spec.

    Args:
        details: Analysis task details.

    Raises:
        AnalysisError: The plugin file or module fails to import, or the
            entrypoint is missing or not callable.

    Returns:
        Analyzer callable.
    """
    try:
        if isinstance(details.plugin, ScriptPluginSpec):
            path = Path(get_required_env("KITARU_TASK_PLUGIN_PATH"))
            return load_plugin_entrypoint(path, details.plugin.entrypoint, _LABEL)
        return load_source_ref(details.plugin.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise AnalysisError(str(exc)) from exc


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the analysis flow: analyze the import's sessions and write the result.

    Args:
        client: API client.
        task_id: Id of the analyzer task.

    Raises:
        AnalysisError: The task is not an analyzer task, the plugin fails to
            load, or the analyzer invocation fails validation.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, AnalysisTaskDetails):
        raise AnalysisError(f"Task {task_id} is not an analyzer task")
    analyzer = _resolve_analyzer(details)

    params = SessionListParams(
        filter=FilterCondition(
            field="import_id", op=FilterOp.EQ, value=str(details.import_id)
        ),
        size=1000,
    )
    session_ids = [session.id async for session in client.sessions.iter(params)]
    results = await call_analyzer(
        details.analyzer_name, analyzer, session_ids, details.params
    )
    write_task_result(results)
