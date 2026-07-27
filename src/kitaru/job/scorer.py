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
"""Scorer contract, loading, validation, and the score flow."""

import asyncio
import importlib
import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kitaru.api_models.v1.jobs import JobSpecScorer, SourceScorerConfig
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import SessionResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.job.plugins import (
    PluginLoadError,
    get_module_attribute,
    load_plugin_module,
)

PLUGIN_MODULE_NAME = "kitaru_scorer_plugin"
SCORER_LABEL = "Scorer"


class ScoringError(Exception):
    """Raised when a scorer cannot be loaded or does not produce a valid score."""


class SessionView(BaseModel):
    """Session view."""

    session: SessionResponse
    nodes: list[SessionNodeResponse]


def _required_env(name: str) -> str:
    """Read an environment variable of the score job contract.

    Args:
        name: Name of the variable.

    Raises:
        ScoringError: The variable is not set.

    Returns:
        Value of the variable.
    """
    value = os.environ.get(name)
    if not value:
        raise ScoringError(f"{name} is not set")
    return value


def load_scorer(source: str) -> Callable[..., float]:
    """Import a scorer function from a source reference.

    A scorer is called as ``score(session: SessionView, **params)`` and
    returns a float in 0..1.

    Args:
        source: Scorer reference as ``module:attribute``.

    Raises:
        ScoringError: The source is malformed, the module does not import,
            or the attribute is missing or not callable.

    Returns:
        Scorer function.
    """
    module_name, separator, attribute = source.partition(":")
    if not separator or not module_name or not attribute:
        raise ScoringError(
            f"Invalid scorer source {source!r}, expected 'module:attribute'"
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ScoringError(
            f"Failed to import scorer module {module_name!r}: {exc}"
        ) from exc
    try:
        return get_module_attribute(module, attribute, SCORER_LABEL)
    except PluginLoadError as exc:
        raise ScoringError(str(exc)) from exc


def call_scorer(
    name: str,
    scorer: Callable[..., float],
    session: SessionView,
    params: dict[str, Any],
) -> float:
    """Call a scorer on a session view and validate its score.

    Args:
        name: Name of the scorer.
        scorer: Scorer function.
        session: Session view to score.
        params: Keyword arguments for the scorer.

    Raises:
        ScoringError: The scorer raised or returned a value outside 0..1.

    Returns:
        Score in 0..1.
    """
    try:
        value = scorer(session, **params)
    except Exception as exc:
        raise ScoringError(
            f"Scorer {name!r} raised {type(exc).__name__}: {exc}"
        ) from exc
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ScoringError(
            f"Scorer {name!r} returned {value!r}, expected a float in 0..1"
        )
    if not 0 <= value <= 1:
        raise ScoringError(
            f"Scorer {name!r} returned {value}, expected a value in 0..1"
        )
    return float(value)


def _resolve_scorer(scorer: JobSpecScorer) -> Callable[..., float]:
    """Load the scorer function a score job runs.

    Registered code is imported from the file the worker materialized,
    source references resolve against the ambient environment.

    Args:
        scorer: Scorer of the job spec.

    Raises:
        ScoringError: The code does not import, or the attribute is
            missing or not callable.

    Returns:
        Scorer function.
    """
    if scorer.plugin is not None:
        path = Path(_required_env("KITARU_JOB_PLUGIN_PATH"))
        try:
            module = load_plugin_module(PLUGIN_MODULE_NAME, path)
        except PluginLoadError as exc:
            raise ScoringError(
                f"Failed to import scorer code from {path}: {exc}"
            ) from exc
        try:
            return get_module_attribute(module, scorer.plugin.entrypoint, SCORER_LABEL)
        except PluginLoadError as exc:
            raise ScoringError(str(exc)) from exc
    if not isinstance(scorer.config, SourceScorerConfig):
        raise ScoringError(f"Scorer {scorer.config.name!r} has no code to run")
    return load_scorer(scorer.config.source)


async def run(client: KitaruAPIClient, job_id: uuid.UUID) -> None:
    """Score the session of a score job and write the result.

    Args:
        client: API client.
        job_id: Id of the job.

    Raises:
        ScoringError: The job is not a score job, its scorer does not
            load, or the scorer raised or returned an invalid score.
        APIError: A read failed.
    """
    spec = await client.jobs.get_spec(job_id)
    if spec.scorer is None:
        raise ScoringError(f"Job {job_id} is not a score job")
    session, nodes = await asyncio.gather(
        client.sessions.get(spec.scorer.input_session_id),
        client.session_nodes.list(spec.scorer.input_session_id, include_payloads=True),
    )
    scorer = _resolve_scorer(spec.scorer)
    score = call_scorer(
        spec.scorer.config.name,
        scorer,
        SessionView(session=session, nodes=nodes),
        spec.scorer.config.params,
    )
    Path(_required_env("KITARU_JOB_RESULT_PATH")).write_text(json.dumps(score))
