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
"""Evaluator plugin contract and task flow."""

import asyncio
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.api_models.v1.task import EvaluationTaskDetails
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.plugins import (
    PluginLoadError,
    load_plugin_entrypoint,
    load_source_ref,
)
from kitaru.task.task_io import get_required_env, write_task_result


class EvaluationError(Exception):
    """An evaluator task failed."""


class SessionView(BaseModel):
    """Session data supplied to an evaluator."""

    session: SessionResponse
    nodes: list[SessionNodeResponse]


EvaluatorReturn = EvaluationResult | list[EvaluationResult]


def call_evaluator(
    name: str,
    evaluator: Callable[..., EvaluatorReturn],
    session: SessionView,
    params: dict[str, Any],
) -> list[EvaluationResult]:
    """Call an evaluator and validate its result set.

    Args:
        name: Evaluator name used in errors.
        evaluator: Evaluator callable.
        session: Complete session view.
        params: Evaluator keyword arguments.

    Raises:
        EvaluationError: Invocation or result validation failed.

    Returns:
        Nonempty list of uniquely named evaluation results.
    """
    try:
        returned = evaluator(session, **params)
    except Exception as exc:
        raise EvaluationError(f"Evaluator {name!r} failed: {exc}") from exc

    results = returned if isinstance(returned, list) else [returned]
    if not results:
        raise EvaluationError(f"Evaluator {name!r} returned no results.")
    if not all(isinstance(result, EvaluationResult) for result in results):
        raise EvaluationError(
            f"Evaluator {name!r} returned a value that is not an EvaluationResult."
        )

    seen: set[str] = set()
    for result in results:
        if result.name in seen:
            raise EvaluationError(
                f"Evaluator {name!r} returned duplicate result name {result.name!r}."
            )
        seen.add(result.name)
    return results


async def _get_nodes(
    client: KitaruAPIClient, session_id: uuid.UUID
) -> list[SessionNodeResponse]:
    """Consume every page of a session's nodes."""
    return [
        node
        async for node in client.sessions.iter_nodes(session_id, include_payloads=True)
    ]


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run an evaluator task.

    Args:
        client: API client for task and session reads.
        task_id: Evaluator task id.

    Raises:
        EvaluationError: The spec is not for an evaluator or plugin loading
            fails.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, EvaluationTaskDetails):
        raise EvaluationError(f"Task {task_id!r} is not an evaluator task.")

    try:
        if details.plugin.type == "script":
            evaluator = load_plugin_entrypoint(
                Path(get_required_env("KITARU_TASK_PLUGIN_PATH")),
                details.plugin.entrypoint,
                "Evaluator",
            )
        else:
            evaluator = load_source_ref(
                details.plugin.entrypoint,
                "Evaluator",
            )
    except PluginLoadError as exc:
        raise EvaluationError(str(exc)) from exc

    session, nodes = await asyncio.gather(
        client.sessions.get(details.input_session_id),
        _get_nodes(client, details.input_session_id),
    )
    results = call_evaluator(
        details.evaluator_name,
        evaluator,
        SessionView(session=session, nodes=nodes),
        details.params,
    )
    write_task_result(results)


__all__ = [
    "EvaluationError",
    "EvaluationResult",
    "EvaluatorReturn",
    "SessionView",
    "call_evaluator",
    "run",
]
