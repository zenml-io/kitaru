"""CI guard: adapter frameworks must be importable when running in CI.

Adapter test suites are gated by module-level ``pytest.importorskip`` so
local contributors without every framework installed get skips, not
failures. The cost of that convenience is that CI would also skip them
silently if a dependency-group or lockfile change ever dropped a framework.
This module turns that silent skip into a hard CI failure. Outside CI the
whole module skips, preserving the contributor experience.
"""

import importlib
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CI", "").lower() not in ("true", "1"),
    reason="Presence guard only enforced on CI runners.",
)

# (import name, distribution name in pyproject [dependency-groups] dev)
_FRAMEWORKS = [
    ("pydantic_ai", "pydantic-ai-slim"),
    ("agents", "openai-agents"),
    ("langgraph", "langgraph"),
    ("claude_agent_sdk", "claude-agent-sdk"),
    ("google.genai", "google-genai"),
]


@pytest.mark.parametrize(("import_name", "distribution"), _FRAMEWORKS)
def test_adapter_framework_importable(import_name: str, distribution: str) -> None:
    try:
        importlib.import_module(import_name)
    except ImportError as exc:
        pytest.fail(
            f"Adapter framework `{import_name}` (distribution `{distribution}`) "
            "is not installed in this CI environment. Adapter test suites gated "
            "by `pytest.importorskip` are now silently skipping. Restore the "
            "distribution to the `dev` dependency group in pyproject.toml "
            f"(import error: {exc})."
        )
