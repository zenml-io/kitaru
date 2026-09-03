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
"""Tests for the Modal worker launcher."""

import uuid
from collections.abc import Callable
from typing import Any, NamedTuple

import pytest

pytest.importorskip("modal")

import modal

from kitaru import images
from kitaru.server.adapters.worker_launcher.modal import ModalWorkerLauncher
from kitaru.server.application.models.worker import WorkerLaunch
from kitaru.server.worker_launcher_settings import (
    ModalWorkerLauncherSettings,
    WorkerLauncherBackend,
    WorkerLauncherSettings,
)


class _CallRecorder:
    """Records the positional and keyword arguments of a faked SDK call."""

    def __init__(self, result: object = None) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self._result = result

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Record a synchronous call and return the configured result."""
        self.calls.append((args, kwargs))
        return self._result

    async def _aio(self, *args: Any, **kwargs: Any) -> Any:
        """Record an asynchronous call and return the configured result."""
        self.calls.append((args, kwargs))
        return self._result

    @property
    def aio(self) -> Callable[..., Any]:
        """Bound async call recorder mirroring a Modal SDK method's `.aio`."""
        return self._aio


class _FakeClient:
    """Fake Modal client that, like the SDK, is already open when returned."""

    async def __aenter__(self) -> "_FakeClient":
        raise AssertionError("from_credentials clients are already open")

    async def __aexit__(self, *exc_info: object) -> None:
        raise AssertionError("from_credentials clients are already open")


class _FakeModal(NamedTuple):
    """Faked Modal SDK entry points and the values they return."""

    client: _FakeClient
    app: object
    image: object
    from_credentials: _CallRecorder
    app_lookup: _CallRecorder
    sandbox_create: _CallRecorder
    from_registry: _CallRecorder


@pytest.fixture
def fake_modal(monkeypatch: pytest.MonkeyPatch) -> _FakeModal:
    """Replace the Modal SDK entry points the launcher calls with fakes."""
    client = _FakeClient()
    app = object()
    image = object()

    from_credentials = _CallRecorder(client)
    app_lookup = _CallRecorder(app)
    sandbox_create = _CallRecorder(None)
    from_registry = _CallRecorder(image)

    monkeypatch.setattr(modal.Client, "from_credentials", from_credentials)
    monkeypatch.setattr(modal.App, "lookup", app_lookup)
    monkeypatch.setattr(modal.Sandbox, "create", sandbox_create)
    monkeypatch.setattr(modal.Image, "from_registry", from_registry)

    return _FakeModal(
        client=client,
        app=app,
        image=image,
        from_credentials=from_credentials,
        app_lookup=app_lookup,
        sandbox_create=sandbox_create,
        from_registry=from_registry,
    )


def _settings(
    image: str | None = "zenmldocker/kitaru-worker:1.0.0",
    command: str = "python -m kitaru.worker",
    cpu: float | None = None,
    memory_mb: int | None = None,
) -> WorkerLauncherSettings:
    return WorkerLauncherSettings(
        backend=WorkerLauncherBackend.MODAL,
        image=image,
        command=command,
        timeout_seconds=120,
        modal=ModalWorkerLauncherSettings(
            token_id="ak-test", token_secret="as-test", cpu=cpu, memory_mb=memory_mb
        ),
    )


def _command() -> WorkerLaunch:
    return WorkerLaunch(
        worker_id=uuid.uuid4(),
        worker_token="worker-token",
        server_url="https://kitaru.example.com",
        job_id=uuid.uuid4(),
    )


async def test_launch_creates_sandbox_with_resource_limits(
    fake_modal: _FakeModal,
) -> None:
    """Start a sandbox with the credentials, image, env, and resource limits."""
    launcher = ModalWorkerLauncher(_settings(cpu=2.0, memory_mb=4096))
    command = _command()

    await launcher.launch(command)

    assert fake_modal.from_credentials.calls == [(("ak-test", "as-test"), {})]
    assert fake_modal.from_registry.calls == [
        (("zenmldocker/kitaru-worker:1.0.0",), {})
    ]
    assert fake_modal.app_lookup.calls == [
        (
            ("kitaru-workers",),
            {"client": fake_modal.client, "create_if_missing": True},
        )
    ]
    assert len(fake_modal.sandbox_create.calls) == 1
    args, kwargs = fake_modal.sandbox_create.calls[0]
    assert args == ("python", "-m", "kitaru.worker")
    assert kwargs["app"] is fake_modal.app
    assert kwargs["image"] is fake_modal.image
    assert kwargs["env"] == {
        "KITARU_API_URL": command.server_url,
        "KITARU_API_TOKEN": "worker-token",
        "KITARU_WORKER_ID": str(command.worker_id),
        "KITARU_WORKER_TIMEOUT": "120",
    }
    assert kwargs["timeout"] == 120
    assert kwargs["cpu"] == 2.0
    assert kwargs["memory"] == 4096
    assert kwargs["client"] is fake_modal.client


async def test_launch_without_resource_limits_passes_none_through(
    fake_modal: _FakeModal,
) -> None:
    """Pass cpu and memory through as None when not configured."""
    launcher = ModalWorkerLauncher(_settings())

    await launcher.launch(_command())

    _, kwargs = fake_modal.sandbox_create.calls[0]
    assert kwargs["cpu"] is None
    assert kwargs["memory"] is None


async def test_launch_splits_a_configured_command(fake_modal: _FakeModal) -> None:
    """Run a configured command as its shell-split argument list."""
    launcher = ModalWorkerLauncher(
        _settings(command="/app/.venv/bin/python -m kitaru.worker --log-level debug")
    )

    await launcher.launch(_command())

    args, _ = fake_modal.sandbox_create.calls[0]
    assert args == (
        "/app/.venv/bin/python",
        "-m",
        "kitaru.worker",
        "--log-level",
        "debug",
    )


async def test_launch_defaults_to_the_published_worker_image(
    fake_modal: _FakeModal, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the published worker image at the installed version when unset."""
    monkeypatch.setattr(images, "version", lambda name: "0.25.0")
    launcher = ModalWorkerLauncher(_settings(image=None))

    await launcher.launch(_command())

    assert fake_modal.from_registry.calls == [
        (("zenmldocker/kitaru-worker:0.25.0",), {})
    ]


async def test_launch_reuses_the_client_across_launches(
    fake_modal: _FakeModal,
) -> None:
    """Open the Modal client once and reuse it for every launch."""
    launcher = ModalWorkerLauncher(_settings())

    await launcher.launch(_command())
    await launcher.launch(_command())

    assert len(fake_modal.from_credentials.calls) == 1
    assert len(fake_modal.sandbox_create.calls) == 2
