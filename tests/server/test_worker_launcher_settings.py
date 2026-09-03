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
"""Tests for worker launcher settings validation."""

import pydantic
import pytest
from pydantic import ValidationError

from conftest import local_settings
from kitaru.server import worker_launcher_settings
from kitaru.server.worker_launcher_settings import (
    ModalWorkerLauncherSettings,
    WorkerLauncherBackend,
    WorkerLauncherSettings,
)


def test_none_backend_needs_no_modal_settings() -> None:
    """Allow the none backend with no modal sub-model set."""
    settings = WorkerLauncherSettings()
    assert settings.backend == WorkerLauncherBackend.NONE
    assert settings.modal is None


def test_modal_backend_requires_modal_settings() -> None:
    """Reject the modal backend with no modal sub-model set."""
    with pytest.raises(
        ValidationError, match="KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_ID"
    ):
        WorkerLauncherSettings(backend=WorkerLauncherBackend.MODAL)


def test_modal_backend_with_modal_settings() -> None:
    """Allow the modal backend with a matching modal sub-model set."""
    settings = WorkerLauncherSettings(
        backend=WorkerLauncherBackend.MODAL,
        modal=ModalWorkerLauncherSettings(token_id="ak-test", token_secret="as-test"),
    )
    assert settings.modal is not None
    assert settings.modal.token_id == "ak-test"
    assert settings.command == "python -m kitaru.worker"
    assert settings.timeout_seconds == 3600


def test_image_defaults_to_the_published_worker_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve the published worker image at the installed version when unset."""
    monkeypatch.setattr(worker_launcher_settings, "version", lambda name: "0.25.0rc1")
    assert (
        WorkerLauncherSettings().get_image() == "zenmldocker/kitaru-worker:0.25.0-rc.1"
    )


def test_image_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """A configured image is used as is."""
    monkeypatch.setattr(worker_launcher_settings, "version", lambda name: "0.25.0")
    settings = WorkerLauncherSettings(image="registry.example.com/worker:custom")
    assert settings.get_image() == "registry.example.com/worker:custom"


def test_image_required_for_a_development_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the image variable when no published image exists for this build."""
    monkeypatch.setattr(worker_launcher_settings, "version", lambda name: "0.25.0.dev3")
    with pytest.raises(ValueError, match="KITARU_SERVER_WORKER_LAUNCHER__IMAGE"):
        WorkerLauncherSettings().get_image()


def test_modal_settings_parsed_from_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse the modal sub-model from its nested environment variables."""
    monkeypatch.setenv("KITARU_SERVER_WORKER_LAUNCHER__BACKEND", "modal")
    monkeypatch.setenv("KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_ID", "ak-test")
    monkeypatch.setenv("KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_SECRET", "as-test")
    monkeypatch.setenv(
        "KITARU_SERVER_WORKER_LAUNCHER__IMAGE", "zenmldocker/kitaru-worker:1.0.0"
    )
    monkeypatch.setenv(
        "KITARU_SERVER_WORKER_LAUNCHER__COMMAND", "python -m kitaru.worker --debug"
    )
    monkeypatch.setenv("KITARU_SERVER_WORKER_LAUNCHER__TIMEOUT_SECONDS", "120")

    settings = local_settings(SERVER_URL="https://kitaru.example.com")

    assert settings.WORKER_LAUNCHER.backend == WorkerLauncherBackend.MODAL
    assert settings.WORKER_LAUNCHER.modal is not None
    assert settings.WORKER_LAUNCHER.modal.token_id == "ak-test"
    assert settings.WORKER_LAUNCHER.modal.token_secret.get_secret_value() == "as-test"
    assert settings.WORKER_LAUNCHER.image == "zenmldocker/kitaru-worker:1.0.0"
    assert settings.WORKER_LAUNCHER.command == "python -m kitaru.worker --debug"
    assert settings.WORKER_LAUNCHER.timeout_seconds == 120
    assert "as-test" not in repr(settings.WORKER_LAUNCHER)


def test_modal_backend_requires_server_url() -> None:
    """Reject the modal backend with no server URL configured."""
    worker_launcher = WorkerLauncherSettings(
        backend=WorkerLauncherBackend.MODAL,
        modal=ModalWorkerLauncherSettings(token_id="ak-test", token_secret="as-test"),
    )
    with pytest.raises(pydantic.ValidationError, match="KITARU_SERVER_SERVER_URL"):
        local_settings(WORKER_LAUNCHER=worker_launcher)

    settings = local_settings(
        WORKER_LAUNCHER=worker_launcher, SERVER_URL="https://kitaru.example.com"
    )
    assert settings.WORKER_LAUNCHER is worker_launcher
