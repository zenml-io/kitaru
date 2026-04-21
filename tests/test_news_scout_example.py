"""Integration tests for the news_scout example.

We don't run the agent itself here — PydanticAI's granular-mode dispatch
path is already exercised in ``test_phase17_pydantic_ai_adapter.py``.
These tests focus on the example-specific wiring: that the module imports
cleanly under the expected env vars, that ``seed_profile`` writes to the
configured namespace scope, and that ``SCOUT_IMAGE`` carries the right
non-secret env + pinned requirements (provider API keys travel via
``secret_environment_from`` on remote stacks only, not via
``ImageSettings.environment``).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

pytest.importorskip("pydantic_ai")

_EXAMPLE_DIR = Path(__file__).resolve().parent.parent / "examples" / "news_scout"


def _load_scout_from_path() -> ModuleType:
    """Load ``examples/news_scout/scout.py`` by file path.

    We avoid a bare ``import scout`` because the example directory isn't on
    the package search path at static-analysis time, which trips the type
    checker. Loading by path makes the import explicit to both static tools
    and the reader.
    """
    spec = importlib.util.spec_from_file_location("scout", _EXAMPLE_DIR / "scout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["scout"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def scout_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Import examples/news_scout/scout.py with env vars pre-set.

    The module constructs a PydanticAI Agent at import time, which
    requires an ANTHROPIC_API_KEY in the environment even to initialize.
    Set dummy keys before import and evict the cached submodules so the
    fresh env is picked up.
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


@pytest.mark.usefixtures("primed_zenml")
def test_seed_profile_writes_namespace_memory(
    scout_module: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``seed_profile`` writes interests to namespace memory and reports them."""
    from kitaru import memory

    interests = ["alpha", "beta"]
    scout_module.seed_profile(interests)

    captured = capsys.readouterr()
    assert "alpha" in captured.out
    assert "beta" in captured.out
    assert "news_scout" in captured.out

    memory.configure(scope="news_scout", scope_type="namespace")
    stored = memory.get("interests")
    assert stored == interests


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

    # Provider keys MUST NOT be present — refactor moved them to secrets.
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
