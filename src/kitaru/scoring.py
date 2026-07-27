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
"""Scoring of sessions with user-defined scorer functions."""

import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Any

from pydantic import BaseModel

from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import SessionResponse
from kitaru.plugin_loader import PluginLoadError, module_attribute

SCORER_LABEL = "Scorer"


class ScoringError(Exception):
    """Raised when a scorer cannot be loaded or does not produce a valid score."""


class SessionView(BaseModel):
    """Session view."""

    session: SessionResponse
    nodes: list[SessionNodeResponse]


def scorer_attribute(module: ModuleType, attribute: str) -> Callable[..., float]:
    """Return the scorer attribute of an imported module.

    Args:
        module: Module holding the scorer.
        attribute: Name of the scorer attribute.

    Raises:
        ScoringError: The attribute is missing or not callable.

    Returns:
        Scorer function.
    """
    try:
        return module_attribute(module, attribute, SCORER_LABEL)
    except PluginLoadError as exc:
        raise ScoringError(str(exc)) from exc


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
    return scorer_attribute(module, attribute)


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
