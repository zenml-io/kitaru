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
"""Conventions every versioned API model must follow."""

import importlib
import inspect
import pkgutil
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, get_args, get_origin

import pytest
from pydantic import BaseModel

import kitaru.api_models.v1 as v1
from kitaru.api_models.v1.base import RequestModel

DEPRECATIONS = Path(__file__).resolve().parents[2] / "DEPRECATIONS.md"

# Guards against a package layout change that would make the walk below find
# nothing, so every check here would pass without inspecting anything.
MIN_MODEL_COUNT = 150


@pytest.fixture(scope="module")
def models() -> list[type[BaseModel]]:
    """Return every Pydantic model defined in the v1 API model modules."""
    found = []
    for module_info in pkgutil.iter_modules(v1.__path__):
        module = importlib.import_module(f"{v1.__name__}.{module_info.name}")
        found.extend(
            obj
            for obj in vars(module).values()
            if inspect.isclass(obj)
            and issubclass(obj, BaseModel)
            and obj.__module__ == module.__name__
        )
    assert len(found) >= MIN_MODEL_COUNT
    return found


def _is_discriminator_tag(annotation: Any) -> bool:
    return get_origin(annotation) is Literal and len(get_args(annotation)) == 1


def _contains_naive_datetime(annotation: Any) -> bool:
    if annotation is datetime:
        return True
    return any(_contains_naive_datetime(arg) for arg in get_args(annotation))


def _get_referenced_models(annotation: Any) -> set[type[BaseModel]]:
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return {annotation}
    return {
        model for arg in get_args(annotation) for model in _get_referenced_models(arg)
    }


def _get_request_body_models(models: list[type[BaseModel]]) -> set[type[BaseModel]]:
    reached: set[type[BaseModel]] = set()
    pending: list[type[BaseModel]] = [
        model for model in models if issubclass(model, RequestModel)
    ]
    while pending:
        model = pending.pop()
        if model in reached:
            continue
        reached.add(model)
        for field in model.model_fields.values():
            pending.extend(_get_referenced_models(field.annotation))
    return reached


def test_fields_have_descriptions(models: list[type[BaseModel]]) -> None:
    """Describe every field except single-value discriminator tags."""
    missing = {
        f"{model.__name__}.{name}"
        for model in models
        for name, field in model.model_fields.items()
        if not field.description and not _is_discriminator_tag(field.annotation)
    }
    assert not missing, (
        "Add Field(description=...): descriptions feed the OpenAPI schema. Use "
        "the shortest noun phrase that the type and name do not already convey."
    )


def test_request_datetimes_are_timezone_aware(
    models: list[type[BaseModel]],
) -> None:
    """Reject naive datetimes anywhere inside request and query parameter models."""
    naive = {
        f"{model.__name__}.{name}"
        for model in _get_request_body_models(models)
        for name, field in model.model_fields.items()
        if _contains_naive_datetime(field.annotation)
    }
    assert not naive, (
        "Declare pydantic.AwareDatetime so naive client input fails with 422 "
        "instead of being read in an implicit timezone."
    )


def test_deprecated_fields_are_listed(models: list[type[BaseModel]]) -> None:
    """List every deprecated wire field in DEPRECATIONS.md."""
    text = DEPRECATIONS.read_text(encoding="utf-8")
    unlisted = {
        f"{model.__name__}.{name}"
        for model in models
        for name, field in model.model_fields.items()
        if field.deprecated and f"`{name}`" not in text
    }
    assert not unlisted, (
        "Add an entry to DEPRECATIONS.md naming the field in backticks, what "
        "replaces it, and when it can be removed."
    )
