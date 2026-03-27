# ruff: noqa: E402
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

_EARLY_TEST_ENV_VARS = (
    "KITARU_SERVER_URL",
    "KITARU_AUTH_TOKEN",
    "KITARU_PROJECT",
    "KITARU_RUNNER",
    "KITARU_STACK",
    "KITARU_CACHE",
    "KITARU_RETRIES",
    "KITARU_IMAGE",
    "KITARU_LOG_STORE_BACKEND",
    "KITARU_LOG_STORE_ENDPOINT",
    "KITARU_LOG_STORE_API_KEY",
    "KITARU_DEFAULT_MODEL",
    "KITARU_MODEL_REGISTRY",
    "KITARU_CONFIG_PATH",
    "KITARU_DEBUG",
    "KITARU_ENGINE",
    "KITARU_ENABLE_EXPERIMENTAL_DAPR",
    "KITARU_ANALYTICS_OPT_IN",
    "ZENML_CONFIG_PATH",
    "ZENML_ACTIVE_PROJECT_ID",
    "ZENML_DEBUG",
    "ZENML_ANALYTICS_OPT_IN",
)

for _env_name in _EARLY_TEST_ENV_VARS:
    os.environ.pop(_env_name, None)
for _env_name in list(os.environ):
    if _env_name.startswith(ENV_ZENML_STORE_PREFIX):
        os.environ.pop(_env_name, None)

_SRC_PATH = str(Path(__file__).resolve().parent.parent / "src")
if _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from kitaru._env import (
    KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV,
    KITARU_ENGINE_ENV,
)
from kitaru._env import (
    _reset_applied as _reset_env_applied,
)
from kitaru.config import (
    KITARU_ANALYTICS_OPT_IN_ENV,
    KITARU_AUTH_TOKEN_ENV,
    KITARU_CACHE_ENV,
    KITARU_CONFIG_PATH_ENV,
    KITARU_DEBUG_ENV,
    KITARU_DEFAULT_MODEL_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LOG_STORE_API_KEY_ENV,
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    KITARU_MODEL_REGISTRY_ENV,
    KITARU_PROJECT_ENV,
    KITARU_RETRIES_ENV,
    KITARU_SERVER_URL_ENV,
    KITARU_STACK_ENV,
    _reset_runtime_configuration,
)
from kitaru.engines._registry import _reset_engine_backend_cache


@pytest.fixture(autouse=True)
def isolated_zenml_global_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path]:
    """Isolate ZenML and Kitaru config so tests never touch real user state.

    Mirrors the production init hook: both Kitaru and ZenML share a single
    unified config directory.
    """
    config_dir = tmp_path / "kitaru-config"
    config_dir.mkdir(parents=True)

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("click.get_app_dir", lambda app_name: str(config_dir))

    monkeypatch.setenv(ENV_ZENML_CONFIG_PATH, str(config_dir))

    for env_name in (
        ENV_ZENML_ACTIVE_PROJECT_ID,
        ENV_ZENML_ACTIVE_STACK_ID,
        ENV_ZENML_LOCAL_STORES_PATH,
        ENV_ZENML_REPOSITORY_PATH,
        ENV_ZENML_SERVER,
        "ZENML_REPOSITORY_DIRECTORY_NAME",
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
        KITARU_DEFAULT_MODEL_ENV,
        KITARU_MODEL_REGISTRY_ENV,
        KITARU_CONFIG_PATH_ENV,
        KITARU_DEBUG_ENV,
        KITARU_ANALYTICS_OPT_IN_ENV,
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.delenv("KITARU_RUNNER", raising=False)
    monkeypatch.delenv(KITARU_ENGINE_ENV, raising=False)
    monkeypatch.delenv(KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV, raising=False)

    _reset_runtime_configuration()
    _reset_env_applied()
    _reset_engine_backend_cache()

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
    _reset_env_applied()
    _reset_engine_backend_cache()


@pytest.fixture()
def primed_zenml() -> None:
    """Eagerly initialize ZenML's store so flow-running tests avoid lazy-init races.

    Only request this fixture in tests that actually execute flows, use
    KitaruClient against real state, or spawn threads that touch the ZenML
    runtime.  Lightweight unit/mock tests should NOT use this fixture —
    keeping them free of ZenML bootstrap is what makes xdist parallelism fast.
    """
    _ = Client().zen_store


@pytest.fixture()
def experimental_dapr_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None]:
    """Enable the experimental Dapr engine for the duration of a test.

    Sets ``KITARU_ENGINE=dapr`` and ``KITARU_ENABLE_EXPERIMENTAL_DAPR=1``
    and resets the backend cache before and after.
    """
    _reset_engine_backend_cache()
    monkeypatch.setenv(KITARU_ENGINE_ENV, "dapr")
    monkeypatch.setenv(KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV, "1")
    yield
    _reset_engine_backend_cache()
