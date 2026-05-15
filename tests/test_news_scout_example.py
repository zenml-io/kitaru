"""Integration tests for the news_scout example.

We don't run the agent itself here — PydanticAI's granular-mode dispatch
path is already exercised in ``test_phase17_pydantic_ai_adapter.py``.
These tests focus on the example-specific wiring: that the module imports
cleanly under the expected env vars, that interests are CLI/default driven,
and that ``SCOUT_IMAGE`` carries the right non-secret env + pinned
requirements (provider API keys travel via ``secret_environment_from`` on
remote stacks only, not via ``ImageSettings.environment``).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

pytest.importorskip("pydantic_ai")

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "end_to_end" / "news_scout"
)


def _load_scout_from_path() -> ModuleType:
    """Load ``examples/end_to_end/news_scout/scout.py`` by file path."""
    spec = importlib.util.spec_from_file_location("scout", _EXAMPLE_DIR / "scout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["scout"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def scout_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Import examples/end_to_end/news_scout/scout.py with env vars pre-set.

    The module constructs a PydanticAI Agent at import time, which requires an
    ANTHROPIC_API_KEY in the environment even to initialize. Set dummy keys
    before import and evict the cached submodules so the fresh env is picked up.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("XAI_API_KEY", "test-xai-key")
    monkeypatch.setenv("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")

    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    for module_name in [
        name
        for name in list(sys.modules)
        if name == "scout"
        or name.startswith("tools")
        or name.startswith("utils")
        or name == "models"
        or name == "prompts"
    ]:
        monkeypatch.delitem(sys.modules, module_name, raising=False)

    return _load_scout_from_path()


def test_scout_module_imports_and_wires_the_agent(scout_module: Any) -> None:
    """Importing the module builds the granular-checkpoint agent + image."""
    from kitaru.adapters.pydantic_ai import KitaruAgent

    assert isinstance(scout_module.scout_agent, KitaruAgent)
    assert scout_module.scout_agent.name == "news_scout"

    # Flow is a proper Kitaru flow definition with the image attached.
    assert scout_module.news_scout is not None
    # The image config exists and declares the expected extras.
    assert scout_module.SCOUT_IMAGE is not None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("robotics,biotech", ["robotics", "biotech"]),
        (" robotics, , biotech ", ["robotics", "biotech"]),
        (" , ", []),
    ],
)
def test_parse_interests_splits_comma_separated_cli_values(
    scout_module: Any,
    raw: str | None,
    expected: list[str] | None,
) -> None:
    """The CLI accepts a compact comma-separated interest list."""
    assert scout_module._parse_interests(raw) == expected


def test_main_uses_default_interests_when_no_cli_override(
    monkeypatch: pytest.MonkeyPatch,
    scout_module: Any,
) -> None:
    """A plain run should pass the built-in interests into the flow."""
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=None)
    monkeypatch.setattr(scout_module, "news_scout", fake_flow)
    monkeypatch.setattr(scout_module, "_image_override_for_active_stack", lambda: None)

    assert scout_module.main([]) == 0

    fake_flow.run.assert_called_once_with(interests=scout_module.DEFAULT_INTERESTS)


def test_main_uses_cli_interests_without_persisting_profile(
    monkeypatch: pytest.MonkeyPatch,
    scout_module: Any,
) -> None:
    """CLI interests are per-run arguments, not persisted profile writes."""
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=None)
    monkeypatch.setattr(scout_module, "news_scout", fake_flow)
    monkeypatch.setattr(scout_module, "_image_override_for_active_stack", lambda: None)

    assert scout_module.main(["--interests", "robotics, biotech"]) == 0

    fake_flow.run.assert_called_once_with(interests=["robotics", "biotech"])


def test_main_attaches_remote_image_override(
    monkeypatch: pytest.MonkeyPatch,
    scout_module: Any,
) -> None:
    """Remote-stack secret injection still travels through the flow run call."""
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=None)
    image_override = {"secret_environment_from": ["news-scout-keys"]}
    monkeypatch.setattr(scout_module, "news_scout", fake_flow)
    monkeypatch.setattr(
        scout_module,
        "_image_override_for_active_stack",
        lambda: image_override,
    )

    assert scout_module.main(["--interests", "ai"]) == 0

    fake_flow.run.assert_called_once_with(
        interests=["ai"],
        image=image_override,
    )


def test_image_settings_carries_non_secret_env_and_pinned_requirements(
    scout_module: Any,
) -> None:
    """SCOUT_IMAGE propagates non-secret config + pinned requirements.

    Provider API keys intentionally do NOT travel via
    ``ImageSettings.environment`` (that would land them in Docker build
    metadata, image layers, and logs). Post-refactor they travel through
    ``secret_environment_from`` at run time on remote stacks only.
    """
    env = scout_module.SCOUT_IMAGE.environment or {}

    # Non-secret model overrides are propagated into remote images.
    assert env.get("KITARU_SCOUT_MODEL") == "anthropic:claude-sonnet-4-6"

    # Provider keys MUST NOT be present — they travel through secrets.
    assert "ANTHROPIC_API_KEY" not in env
    assert "XAI_API_KEY" not in env

    # The decorator image itself carries no secret reference — the
    # conditional runtime override attaches it when the active stack is
    # remote.
    assert scout_module.SCOUT_IMAGE.secret_environment_from is None
    assert scout_module.SECRET_NAME == "news-scout-keys"

    # On a local default stack the conditional override stays None so
    # local runs don't demand that the secret exists.
    assert scout_module._image_override_for_active_stack() is None

    requirements = scout_module.SCOUT_IMAGE.requirements or []
    assert any("pydantic-ai-slim" in req for req in requirements)
    # Pinned below 1.80 for ZenML otel-sdk compatibility — don't regress.
    assert any("<1.80" in req for req in requirements)
