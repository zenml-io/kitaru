# ruff: noqa: E402
"""Shared test fixtures."""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

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
from zenml.utils.source_utils import set_custom_source_root

_PROVIDER_HOST_SUFFIX_BY_NAME = {
    "OpenAI": ("api.openai.com",),
    "Anthropic": ("api.anthropic.com",),
    "Gemini/Google GenAI": (
        "generativelanguage.googleapis.com",
        "aiplatform.googleapis.com",
    ),
}
_AZURE_OPENAI_RESOURCE_SUFFIX = ".openai.azure.com"
_LIVE_PROVIDER_MARKERS = (
    "live_openai",
    "live_anthropic",
    "live_gemini",
)
_PROVIDER_CALL_GUARD_ACTIVE = False
_PROVIDER_CALL_GUARD_NODEID: str | None = None
_EARLY_TEST_ENV_VARS = (
    "KITARU_SERVER_URL",
    "KITARU_AUTH_TOKEN",
    "KITARU_PROJECT",
    "KITARU_RUNNER",
    "KITARU_STACK",
    "KITARU_CACHE",
    "KITARU_RETRIES",
    "KITARU_IMAGE",
    "KITARU_LLM_ESTIMATED_COSTS",
    "KITARU_LOG_STORE_BACKEND",
    "KITARU_LOG_STORE_ENDPOINT",
    "KITARU_LOG_STORE_API_KEY",
    "KITARU_DEFAULT_MODEL",
    "KITARU_MODEL_REGISTRY",
    "KITARU_CONFIG_PATH",
    "KITARU_DEBUG",
    "KITARU_ANALYTICS_OPT_IN",
    "KITARU_UI_DIST_PATH",
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

# Immediately disable analytics after stripping env vars.
# ZenML defaults analytics_opt_in to True, so without this the test suite
# sends real events to Mixpanel under randomly-generated user IDs.
os.environ["ZENML_ANALYTICS_OPT_IN"] = "false"

_SRC_PATH = str(Path(__file__).resolve().parent.parent / "src")
if _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from kitaru._env import _reset_applied as _reset_env_applied
from kitaru._google_auth_env import (
    CLOUD_LOCATION_ENV,
    CLOUD_PROJECT_ENV,
    GEMINI_API_KEY_ENV,
    GOOGLE_API_KEY_ENV,
    VERTEXAI_ENV,
    has_google_live_auth_env,
)
from kitaru.config import (
    KITARU_ANALYTICS_OPT_IN_ENV,
    KITARU_AUTH_TOKEN_ENV,
    KITARU_CACHE_ENV,
    KITARU_CONFIG_PATH_ENV,
    KITARU_DEBUG_ENV,
    KITARU_DEFAULT_MODEL_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LLM_ESTIMATED_COSTS_ENV,
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


def _speed_probe_enabled() -> bool:
    return os.environ.get("KITARU_TEST_SPEED_PROBE") == "1"


def _speed_report_dir() -> Path:
    return Path(
        os.environ.get(
            "KITARU_TEST_SPEED_REPORT_DIR",
            "prompt-exports/test-speed/probe",
        )
    )


def _speed_worker_id() -> str:
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


def _record_speed_event(kind: str, payload: dict[str, Any]) -> None:
    if not _speed_probe_enabled():
        return

    report_dir = _speed_report_dir()
    report_dir.mkdir(parents=True, exist_ok=True)
    worker = _speed_worker_id()
    pid = os.getpid()
    event = {
        "kind": kind,
        "worker": worker,
        "pid": pid,
        "timestamp": time.time(),
        **payload,
    }
    event_path = report_dir / f"events-{worker}-{pid}.jsonl"
    with event_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, sort_keys=True) + "\n")


@pytest.fixture(autouse=True)
def isolated_zenml_global_config(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[Path]:
    """Isolate ZenML and Kitaru config so tests never touch real user state.

    Mirrors the production init hook: both Kitaru and ZenML share a single
    unified config directory.
    """
    speed_probe = _speed_probe_enabled()
    setup_started = time.perf_counter() if speed_probe else 0.0
    config_dir = tmp_path / "kitaru-config"
    config_dir.mkdir(parents=True)

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("click.get_app_dir", lambda app_name: str(config_dir))

    repo_root = tmp_path / "isolated-repo"
    repo_config_dir = repo_root / ".kitaru"
    repo_config_dir.mkdir(parents=True)
    (repo_config_dir / "config.yaml").write_text("{}\n", encoding="utf-8")

    monkeypatch.setenv(ENV_ZENML_CONFIG_PATH, str(config_dir))
    monkeypatch.setenv(ENV_ZENML_REPOSITORY_PATH, str(repo_root))
    monkeypatch.setenv("ZENML_ANALYTICS_OPT_IN", "false")

    for env_name in (
        ENV_ZENML_ACTIVE_PROJECT_ID,
        ENV_ZENML_ACTIVE_STACK_ID,
        ENV_ZENML_LOCAL_STORES_PATH,
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
        KITARU_LLM_ESTIMATED_COSTS_ENV,
        KITARU_SERVER_URL_ENV,
        KITARU_AUTH_TOKEN_ENV,
        KITARU_PROJECT_ENV,
        KITARU_DEFAULT_MODEL_ENV,
        KITARU_MODEL_REGISTRY_ENV,
        KITARU_CONFIG_PATH_ENV,
        KITARU_DEBUG_ENV,
        KITARU_ANALYTICS_OPT_IN_ENV,
        "KITARU_UI_DIST_PATH",
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.delenv("KITARU_RUNNER", raising=False)

    set_custom_source_root(None)
    _reset_runtime_configuration()
    _reset_env_applied()

    # xdist workers lack __main__.__file__, which ZenML needs for source root
    main = sys.modules.get("__main__")
    if main is not None and not getattr(main, "__file__", None):
        monkeypatch.setattr(
            main, "__file__", str(Path(__file__).resolve().parent), raising=False
        )

    GlobalConfiguration._reset_instance()
    Client._reset_instance()

    if speed_probe:
        _record_speed_event(
            "fixture_setup",
            {
                "fixture": "isolated_zenml_global_config",
                "nodeid": request.node.nodeid,
                "seconds": time.perf_counter() - setup_started,
            },
        )

    yield config_dir

    teardown_started = time.perf_counter() if speed_probe else 0.0
    Client._reset_instance()
    GlobalConfiguration._reset_instance()
    set_custom_source_root(None)
    _reset_runtime_configuration()
    _reset_env_applied()
    if speed_probe:
        _record_speed_event(
            "fixture_teardown",
            {
                "fixture": "isolated_zenml_global_config",
                "nodeid": request.node.nodeid,
                "seconds": time.perf_counter() - teardown_started,
            },
        )


def _set_provider_call_guard_state(*, active: bool, nodeid: str | None) -> None:
    global _PROVIDER_CALL_GUARD_ACTIVE, _PROVIDER_CALL_GUARD_NODEID
    _PROVIDER_CALL_GUARD_ACTIVE = active
    _PROVIDER_CALL_GUARD_NODEID = nodeid


def _clear_provider_call_guard_state() -> None:
    _set_provider_call_guard_state(active=False, nodeid=None)


def _is_live_provider_test(item: pytest.Item) -> bool:
    return item.get_closest_marker("live_llm") is not None or any(
        item.get_closest_marker(marker_name) is not None
        for marker_name in _LIVE_PROVIDER_MARKERS
    )


def _missing_live_provider_auth_messages(item: pytest.Item) -> list[str]:
    missing: list[str] = []
    if item.get_closest_marker("live_openai") is not None and not os.environ.get(
        "OPENAI_API_KEY"
    ):
        missing.append("OPENAI_API_KEY")
    if item.get_closest_marker("live_anthropic") is not None and not os.environ.get(
        "ANTHROPIC_API_KEY"
    ):
        missing.append("ANTHROPIC_API_KEY")
    if (
        item.get_closest_marker("live_gemini") is not None
        and not has_google_live_auth_env()
    ):
        missing.append(
            f"{GEMINI_API_KEY_ENV} or {GOOGLE_API_KEY_ENV}; or "
            f"{VERTEXAI_ENV}=true plus {CLOUD_PROJECT_ENV} and "
            f"{CLOUD_LOCATION_ENV}"
        )
    return missing


def _skip_if_live_provider_keys_are_missing(item: pytest.Item) -> None:
    missing = _missing_live_provider_auth_messages(item)
    if not missing:
        return

    pytest.skip(
        "live provider test requires environment variable(s): " + "; ".join(missing)
    )


def _validate_live_marker_contract(
    items: Iterable[tuple[str, set[str]]],
) -> list[str]:
    """Return marker-contract errors for collected pytest items."""
    missing_live_llm: list[str] = []
    missing_provider_marker: list[str] = []
    for nodeid, marker_names in items:
        provider_markers = [
            marker_name
            for marker_name in _LIVE_PROVIDER_MARKERS
            if marker_name in marker_names
        ]
        has_live_llm = "live_llm" in marker_names
        if provider_markers and not has_live_llm:
            missing_live_llm.append(f"{nodeid} ({', '.join(provider_markers)})")
        if has_live_llm and not provider_markers:
            missing_provider_marker.append(nodeid)

    errors: list[str] = []
    if missing_live_llm:
        formatted = "\n".join(f"- {nodeid}" for nodeid in missing_live_llm)
        errors.append(
            "Provider-specific live tests must also be marked with "
            f"@pytest.mark.live_llm:\n{formatted}"
        )
    if missing_provider_marker:
        formatted = "\n".join(f"- {nodeid}" for nodeid in missing_provider_marker)
        provider_markers = ", ".join(
            f"@pytest.mark.{name}" for name in _LIVE_PROVIDER_MARKERS
        )
        errors.append(
            "Tests marked with @pytest.mark.live_llm must also declare at least "
            f"one provider marker ({provider_markers}). If a future live test is "
            "not provider-specific, deliberately extend the live marker contract "
            f"in tests/conftest.py first:\n{formatted}"
        )
    return errors


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Require live tests to carry umbrella and provider-specific markers."""
    del config
    marker_items = [
        (
            item.nodeid,
            {
                marker_name
                for marker_name in ("live_llm", *_LIVE_PROVIDER_MARKERS)
                if item.get_closest_marker(marker_name) is not None
            },
        )
        for item in items
    ]
    errors = _validate_live_marker_contract(marker_items)
    if errors:
        raise pytest.UsageError("\n\n".join(errors))
    _record_speed_event(
        "collection_finished",
        {"item_count": len(items)},
    )


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if not _speed_probe_enabled():
        return
    if _speed_worker_id() == "master" and hasattr(report, "worker_id"):
        return
    _record_speed_event(
        "test_phase",
        {
            "nodeid": report.nodeid,
            "phase": report.when,
            "outcome": report.outcome,
            "seconds": report.duration,
        },
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not _speed_probe_enabled():
        return
    _record_speed_event(
        "session_finished",
        {
            "exitstatus": exitstatus,
            "testsfailed": session.testsfailed,
            "testscollected": session.testscollected,
        },
    )


def _is_azure_openai_host(normalized: str) -> bool:
    if normalized == "api.openai.azure.com":
        return True
    if not normalized.endswith(_AZURE_OPENAI_RESOURCE_SUFFIX):
        return False
    resource_name = normalized[: -len(_AZURE_OPENAI_RESOURCE_SUFFIX)]
    return bool(resource_name) and "." not in resource_name


def _provider_name_for_host(host: str | None) -> str | None:
    if not host:
        return None
    normalized = host.lower().rstrip(".")
    if _is_azure_openai_host(normalized):
        return "OpenAI"
    if normalized.endswith("-aiplatform.googleapis.com"):
        return "Gemini/Google GenAI"
    for provider_name, suffixes in _PROVIDER_HOST_SUFFIX_BY_NAME.items():
        if any(
            normalized == suffix or normalized.endswith(f".{suffix}")
            for suffix in suffixes
        ):
            return provider_name
    return None


def _provider_name_for_url(url: Any) -> str | None:
    parsed = urlparse(str(url))
    return _provider_name_for_host(parsed.hostname)


def _fail_provider_call(provider_name: str) -> None:
    nodeid = _PROVIDER_CALL_GUARD_NODEID
    location = f" in {nodeid}" if nodeid else ""
    raise AssertionError(
        f"Blocked {provider_name} provider call{location} from an unmarked "
        "deterministic pytest test. Mark paid/provider tests with "
        "@pytest.mark.live_llm and the provider-specific marker, for example "
        "@pytest.mark.live_openai."
    )


def _guard_provider_call_if_active(provider_name: str | None) -> None:
    if _PROVIDER_CALL_GUARD_ACTIVE and provider_name is not None:
        _fail_provider_call(provider_name)


def _resolve_attr_target(dotted_path: str) -> tuple[Any, str] | None:
    module_name, _, attr_path = dotted_path.partition(":")
    try:
        target: Any = importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None
    parts = attr_path.split(".")
    for part in parts[:-1]:
        target = getattr(target, part, None)
        if target is None:
            return None
    return target, parts[-1]


def _provider_name_for_sdk_request(
    client: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    default_provider: str,
) -> str | None:
    options = kwargs.get("options")
    if options is None and len(args) >= 2:
        options = args[1]

    raw_url = getattr(options, "url", None)
    base_url = getattr(client, "base_url", None)

    if raw_url is not None:
        raw_url_string = str(raw_url)
        if urlparse(raw_url_string).hostname is not None:
            return _provider_name_for_url(raw_url_string)
        if base_url is not None:
            return _provider_name_for_url(urljoin(str(base_url), raw_url_string))

    if base_url is not None:
        return _provider_name_for_url(base_url)

    return default_provider


def _patch_sdk_request_if_available(
    monkeypatch: pytest.MonkeyPatch,
    dotted_path: str,
    *,
    default_provider: str,
    is_async: bool,
) -> None:
    resolved = _resolve_attr_target(dotted_path)
    if resolved is None:
        return
    target, attr_name = resolved
    original = getattr(target, attr_name, None)
    if original is None:
        return

    if is_async:

        async def guarded_async_request(self: Any, *args: Any, **kwargs: Any) -> Any:
            provider_name = _provider_name_for_sdk_request(
                self, args, kwargs, default_provider=default_provider
            )
            _guard_provider_call_if_active(provider_name)
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(target, attr_name, guarded_async_request, raising=False)
        return

    def guarded_request(self: Any, *args: Any, **kwargs: Any) -> Any:
        provider_name = _provider_name_for_sdk_request(
            self, args, kwargs, default_provider=default_provider
        )
        _guard_provider_call_if_active(provider_name)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(target, attr_name, guarded_request, raising=False)


def _install_provider_call_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    for dotted_path, default_provider, is_async in (
        ("openai._base_client:SyncAPIClient.request", "OpenAI", False),
        ("openai._base_client:AsyncAPIClient.request", "OpenAI", True),
        ("anthropic._base_client:SyncAPIClient.request", "Anthropic", False),
        ("anthropic._base_client:AsyncAPIClient.request", "Anthropic", True),
    ):
        _patch_sdk_request_if_available(
            monkeypatch,
            dotted_path,
            default_provider=default_provider,
            is_async=is_async,
        )

    try:
        import httpx
    except ModuleNotFoundError:
        pass
    else:
        original_client_request = httpx.Client.request
        original_async_client_request = httpx.AsyncClient.request

        def guarded_httpx_request(
            self: Any, method: str, url: Any, **kwargs: Any
        ) -> Any:
            provider_name = _provider_name_for_url(url)
            _guard_provider_call_if_active(provider_name)
            return original_client_request(self, method, url, **kwargs)

        async def guarded_httpx_request_async(
            self: Any, method: str, url: Any, **kwargs: Any
        ) -> Any:
            provider_name = _provider_name_for_url(url)
            _guard_provider_call_if_active(provider_name)
            return await original_async_client_request(self, method, url, **kwargs)

        monkeypatch.setattr(httpx.Client, "request", guarded_httpx_request)
        monkeypatch.setattr(httpx.AsyncClient, "request", guarded_httpx_request_async)

    try:
        import requests
    except ModuleNotFoundError:
        pass
    else:
        original_requests_request = requests.sessions.Session.request

        def guarded_requests_request(
            self: Any, method: str, url: Any, **kwargs: Any
        ) -> Any:
            provider_name = _provider_name_for_url(url)
            _guard_provider_call_if_active(provider_name)
            return original_requests_request(self, method, url, **kwargs)

        monkeypatch.setattr(
            requests.sessions.Session,
            "request",
            guarded_requests_request,
        )

    try:
        import urllib3
    except ModuleNotFoundError:
        pass
    else:
        original_urlopen = urllib3.connectionpool.HTTPConnectionPool.urlopen

        def guarded_urllib3_urlopen(self: Any, *args: Any, **kwargs: Any) -> Any:
            provider_name = _provider_name_for_host(getattr(self, "host", None))
            _guard_provider_call_if_active(provider_name)
            return original_urlopen(self, *args, **kwargs)

        monkeypatch.setattr(
            urllib3.connectionpool.HTTPConnectionPool,
            "urlopen",
            guarded_urllib3_urlopen,
        )


@pytest.fixture(scope="session", autouse=True)
def provider_call_guard_installation() -> Generator[None]:
    """Install provider-call wrappers once in each pytest worker process."""
    speed_probe = _speed_probe_enabled()
    started = time.perf_counter() if speed_probe else 0.0
    monkeypatch = pytest.MonkeyPatch()
    try:
        _install_provider_call_guard(monkeypatch)
    finally:
        if speed_probe:
            _record_speed_event(
                "fixture_setup",
                {
                    "fixture": "provider_call_guard_installation",
                    "seconds": time.perf_counter() - started,
                },
            )

    try:
        yield
    finally:
        _clear_provider_call_guard_state()
        monkeypatch.undo()


@pytest.fixture(autouse=True)
def provider_spend_guard(request: pytest.FixtureRequest) -> Generator[None]:
    """Block accidental provider calls unless a test is explicitly live-marked."""
    speed_probe = _speed_probe_enabled()
    started = time.perf_counter() if speed_probe else 0.0
    is_live = _is_live_provider_test(request.node)
    _clear_provider_call_guard_state()
    try:
        if is_live:
            _skip_if_live_provider_keys_are_missing(request.node)
            _set_provider_call_guard_state(active=False, nodeid=request.node.nodeid)
        else:
            _set_provider_call_guard_state(active=True, nodeid=request.node.nodeid)
    finally:
        if speed_probe:
            _record_speed_event(
                "fixture_setup",
                {
                    "fixture": "provider_spend_guard",
                    "is_live_provider_test": is_live,
                    "nodeid": request.node.nodeid,
                    "seconds": time.perf_counter() - started,
                },
            )

    try:
        yield
    finally:
        _clear_provider_call_guard_state()


@pytest.fixture()
def primed_zenml(request: pytest.FixtureRequest) -> None:
    """Eagerly initialize ZenML's store so flow-running tests avoid lazy-init races.

    Only request this fixture in tests that actually execute flows, use
    KitaruClient against real state, or spawn threads that touch the ZenML
    runtime.  Lightweight unit/mock tests should NOT use this fixture —
    keeping them free of ZenML bootstrap is what makes xdist parallelism fast.
    """
    speed_probe = _speed_probe_enabled()
    started = time.perf_counter() if speed_probe else 0.0
    _ = Client().zen_store
    if speed_probe:
        _record_speed_event(
            "primed_zenml_setup",
            {
                "nodeid": request.node.nodeid,
                "seconds": time.perf_counter() - started,
            },
        )
