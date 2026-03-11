"""Shared test fixtures."""

from __future__ import annotations

import os
import sys
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

from kitaru.config import (
    KITARU_AUTH_TOKEN_ENV,
    KITARU_CACHE_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LOG_STORE_API_KEY_ENV,
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    KITARU_PROJECT_ENV,
    KITARU_RETRIES_ENV,
    KITARU_SERVER_URL_ENV,
    KITARU_STACK_ENV,
    _reset_runtime_configuration,
)


@pytest.fixture(autouse=True)
def isolated_zenml_global_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path]:
    """Isolate ZenML and Kitaru config so tests never touch real user state."""
    config_dir = tmp_path / ".zenml"
    config_dir.mkdir()

    # Redirect Kitaru's own app config dir into tmp_path.
    kitaru_home = tmp_path / "kitaru-config"
    kitaru_home.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("click.get_app_dir", lambda app_name: str(kitaru_home))

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

    for env_name in (
        KITARU_LOG_STORE_BACKEND_ENV,
        KITARU_LOG_STORE_ENDPOINT_ENV,
        KITARU_LOG_STORE_API_KEY_ENV,
        KITARU_STACK_ENV,
        KITARU_CACHE_ENV,
        KITARU_RETRIES_ENV,
        KITARU_IMAGE_ENV,
        KITARU_SERVER_URL_ENV,
        KITARU_AUTH_TOKEN_ENV,
        KITARU_PROJECT_ENV,
    ):
        monkeypatch.delenv(env_name, raising=False)

    _reset_runtime_configuration()

    # xdist workers lack __main__.__file__, which ZenML needs for source root
    main = sys.modules.get("__main__")
    if main is not None and not getattr(main, "__file__", None):
        monkeypatch.setattr(
            main, "__file__", str(Path(__file__).resolve().parent), raising=False
        )

    GlobalConfiguration._reset_instance()
    Client._reset_instance()

    yield config_dir

    Client._reset_instance()
    GlobalConfiguration._reset_instance()
    _reset_runtime_configuration()
