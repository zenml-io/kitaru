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
"""Shared LangSmith SDK fakes for the LangSmith adapter."""

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from langsmith.schemas import Run

import kitaru_langsmith.adapter as adapter_module

RunsBuilder = Callable[[str], list[Run]]

PROJECT_ID = "11111111-1111-4111-8111-111111111111"


def build_run(
    run_id: str,
    trace_id: str,
    *,
    parent_run_id: str | None = None,
    name: str = "kitaru-run",
    run_type: str = "chain",
    start_time: datetime = datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    end_time: datetime | None = datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    **extra: Any,
) -> Run:
    """Build one LangSmith run."""
    return Run(
        id=uuid.UUID(run_id),
        trace_id=uuid.UUID(trace_id),
        parent_run_id=uuid.UUID(parent_run_id) if parent_run_id else None,
        session_id=uuid.UUID(PROJECT_ID),
        name=name,
        run_type=run_type,
        status="success",
        start_time=start_time,
        end_time=end_time,
        inputs={"messages": [{"role": "user", "content": "Weather?"}]},
        outputs={"role": "assistant", "content": "Sunny."},
        **extra,
    )


def build_complete_runs(trace_id: str) -> list[Run]:
    """Build finished runs with a root run and a nested LLM run."""
    return [
        build_run(trace_id, trace_id),
        build_run(
            str(uuid.uuid5(uuid.NAMESPACE_OID, f"{trace_id}-llm")),
            trace_id,
            parent_run_id=trace_id,
            name="llm-call",
            run_type="llm",
            start_time=datetime(2026, 7, 24, 10, 0, 1, tzinfo=UTC),
            extra={"metadata": {"ls_model_name": "gpt-5-nano"}},
        ),
    ]


class FakeRunTree:
    """Run tree fake carrying only the client the adapter reads."""

    def __init__(self, client: "FakeLangSmithClient") -> None:
        self.client = client


class FakeLangSmithClient:
    """LangSmith client fake routing run listings to the scripted runs."""

    def __init__(self, fake: "FakeLangSmith") -> None:
        self._fake = fake

    def flush(self) -> None:
        self._fake.events.append("flush")

    def list_runs(self, *, trace_id: str) -> Iterator[Run]:
        self._fake.requested.append(trace_id)
        self._fake.events.append("poll")
        assert self._fake.runs_builders, "unexpected run listing poll"
        return iter(self._fake.runs_builders.pop(0)(trace_id))


class FakeLangSmith:
    """LangSmith SDK fake with scripted trace runs."""

    def __init__(self) -> None:
        self.runs_builders: list[RunsBuilder] = []
        self.requested: list[str] = []
        self.events: list[str] = []
        self.pinned_run_ids: list[uuid.UUID] = []
        self.client = FakeLangSmithClient(self)

    @contextmanager
    def tracing_context(self, *, enabled: bool) -> Iterator[None]:
        assert enabled is True
        self.events.append("tracing-enter")
        yield
        self.events.append("tracing-exit")

    @contextmanager
    def trace(
        self, *, name: str, run_id: uuid.UUID, parent: str
    ) -> Iterator[FakeRunTree]:
        assert name == "kitaru-run"
        assert parent == "ignore"
        self.pinned_run_ids.append(run_id)
        self.events.append("trace-enter")
        yield FakeRunTree(self.client)
        self.events.append("trace-exit")

    def get_cached_client(self) -> FakeLangSmithClient:
        return self.client


@pytest.fixture(autouse=True)
def _fast_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(adapter_module, "_POLL_INTERVAL", 0.0)


@pytest.fixture
def fake_langsmith(monkeypatch: pytest.MonkeyPatch) -> FakeLangSmith:
    """Create a fake LangSmith SDK and route the adapter to it."""
    fake = FakeLangSmith()
    monkeypatch.setattr(adapter_module, "tracing_context", fake.tracing_context)
    monkeypatch.setattr(adapter_module, "trace", fake.trace)
    monkeypatch.setattr(adapter_module, "get_cached_client", fake.get_cached_client)
    return fake
