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
"""Explicit idempotency-key forwarding for protected trigger resources."""

import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.resources.evaluations import EvaluationsResource
from kitaru.client.resources.experiments import ExperimentsResource
from kitaru.client.resources.replays import ReplaysResource
from kitaru.client.resources.session_runs import SessionRunsResource


class RequestCaptured(Exception):
    """Stop resource execution after recording the outgoing request."""


class CapturingClient:
    """Minimal API client fake recording request arguments."""

    def __init__(self) -> None:
        """Initialize the call record."""
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def request(self, *args: Any, **kwargs: Any) -> Any:
        """Record one call before response DTO parsing can begin."""
        self.calls.append((args, kwargs))
        raise RequestCaptured


async def _replay(resource: Any) -> None:
    await resource.create(
        ReplayCreateRequest.model_construct(), idempotency_key="stable-request-id"
    )


async def _evaluation(resource: Any) -> None:
    await resource.create(
        EvaluationBatchCreateRequest.model_construct(),
        idempotency_key="stable-request-id",
    )


async def _session_run(resource: Any) -> None:
    await resource.create(
        SessionRunCreateRequest.model_construct(),
        idempotency_key="stable-request-id",
    )


async def _experiment_run(resource: Any) -> None:
    await resource.start_run(
        uuid.uuid4(),
        ExperimentRunCreateRequest.model_construct(),
        idempotency_key="stable-request-id",
    )


@pytest.mark.parametrize(
    ("resource_type", "invoke"),
    [
        (ReplaysResource, _replay),
        (EvaluationsResource, _evaluation),
        (SessionRunsResource, _session_run),
        (ExperimentsResource, _experiment_run),
    ],
)
async def test_protected_trigger_forwards_explicit_key(
    resource_type: type[Any], invoke: Callable[[Any], Awaitable[None]]
) -> None:
    """Every protected create method forwards the caller key unchanged."""
    fake = CapturingClient()
    resource = resource_type(cast(KitaruAPIClient, fake))

    with pytest.raises(RequestCaptured):
        await invoke(resource)

    assert len(fake.calls) == 1
    assert fake.calls[0][1]["idempotency_key"] == "stable-request-id"
