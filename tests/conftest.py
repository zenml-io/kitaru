"""Shared test fixtures."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.constants import (
    ENV_ZENML_ACTIVE_PROJECT_ID,
    ENV_ZENML_ACTIVE_STACK_ID,
    ENV_ZENML_CONFIG_PATH,
    ENV_ZENML_LOCAL_STORES_PATH,
    ENV_ZENML_REPOSITORY_PATH,
    ENV_ZENML_SERVER,
    ENV_ZENML_STORE_PREFIX,
)


@pytest.fixture(autouse=True)
def isolated_zenml_global_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path]:
    """Isolate ZenML's global config so tests never touch real user state."""
    config_dir = tmp_path / ".zenml"
    config_dir.mkdir()

    monkeypatch.setenv(ENV_ZENML_CONFIG_PATH, str(config_dir))

    for env_name in (
        ENV_ZENML_ACTIVE_PROJECT_ID,
        ENV_ZENML_ACTIVE_STACK_ID,
        ENV_ZENML_LOCAL_STORES_PATH,
        ENV_ZENML_REPOSITORY_PATH,
        ENV_ZENML_SERVER,
    ):
        monkeypatch.delenv(env_name, raising=False)

    for env_name in list(os.environ):
        if env_name.startswith(ENV_ZENML_STORE_PREFIX):
            monkeypatch.delenv(env_name, raising=False)

    GlobalConfiguration._reset_instance()
    Client._reset_instance()

    yield config_dir

    Client._reset_instance()
    GlobalConfiguration._reset_instance()
