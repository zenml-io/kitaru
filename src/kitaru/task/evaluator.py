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
"""Evaluator plugin contract and the evaluation flow."""

import asyncio
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeListParams, SessionNodeResponse
from kitaru.api_models.v1.task import EvaluationTaskDetails, ScriptPluginSpec
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.plugins import PluginLoadError, load_plugin_entrypoint, load_source_ref
from kitaru.task.task_io import get_required_env, write_task_result

__all__ = [
    "EvaluationError",
    "EvaluationResult",
    "EvaluatorReturn",
    "SessionView",
    "call_evaluator",
    "run",
]

_LABEL = "Evaluator"


class EvaluationError(Exception):
    """Raised when loading or invoking an evaluator plugin fails."""


class SessionView(BaseModel):
    """Session view."""

    session: SessionResponse
    nodes: list[SessionNodeResponse]


EvaluatorReturn = EvaluationResult | list[EvaluationResult]


def call_evaluator(
    name: str,
    evaluator: Callable[..., EvaluatorReturn],
    session: SessionView,
    params: dict[str, Any],
) -> list[EvaluationResult]:
    """Invoke an evaluator and validate its results.

    Args:
        name: Evaluator name, named in error messages.
        evaluator: Evaluator callable.
        session: Session view passed to the evaluator.
        params: Parameters passed to the evaluator.

    Raises:
        EvaluationError: The evaluator raised, returned no results, returned
            a non-EvaluationResult value, or returned duplicate result names.

    Returns:
        Evaluation results.
    """
    try:
        result = evaluator(session, **params)
    except Exception as exc:
        raise EvaluationError(f"Evaluator '{name}' raised an error: {exc}") from exc
    results = result if isinstance(result, list) else [result]
    if not results:
        raise EvaluationError(f"Evaluator '{name}' returned no results")
    if not all(isinstance(item, EvaluationResult) for item in results):
        raise EvaluationError(
            f"Evaluator '{name}' returned a non-EvaluationResult value"
        )
    names = [item.name for item in results]
    if len(set(names)) != len(names):
        raise EvaluationError(f"Evaluator '{name}' returned duplicate result names")
    return results


def _resolve_evaluator(
    details: EvaluationTaskDetails,
) -> Callable[..., EvaluatorReturn]:
    """Load the evaluator callable named by a task's plugin spec.

    Args:
        details: Evaluation task details.

    Raises:
        EvaluationError: The plugin file or module fails to import, or the
            entrypoint is missing or not callable.

    Returns:
        Evaluator callable.
    """
    try:
        if isinstance(details.plugin, ScriptPluginSpec):
            path = Path(get_required_env("KITARU_TASK_PLUGIN_PATH"))
            return load_plugin_entrypoint(path, details.plugin.entrypoint, _LABEL)
        return load_source_ref(details.plugin.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise EvaluationError(str(exc)) from exc


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the evaluation flow: score the input session and write the result.

    Args:
        client: API client.
        task_id: Id of the evaluator task.

    Raises:
        EvaluationError: The task is not an evaluator task, the plugin fails
            to load, or the evaluator invocation fails validation.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, EvaluationTaskDetails):
        raise EvaluationError(f"Task {task_id} is not an evaluator task")
    evaluator = _resolve_evaluator(details)

    async def _fetch_nodes() -> list[SessionNodeResponse]:
        return [
            node
            async for node in client.sessions.iter_nodes(
                details.input_session_id,
                SessionNodeListParams(include_payloads=True),
            )
        ]

    session, nodes = await asyncio.gather(
        client.sessions.get(details.input_session_id), _fetch_nodes()
    )
    view = SessionView(session=session, nodes=nodes)
    results = call_evaluator(details.evaluator_name, evaluator, view, details.params)
    write_task_result(results)
