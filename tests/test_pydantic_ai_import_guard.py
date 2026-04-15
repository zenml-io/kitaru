"""Import-time guard tests for the PydanticAI adapter.

Kept in its own file so the module-level `pytest.importorskip("pydantic_ai")`
in the other adapter test files doesn't short-circuit this test when the
extra IS installed — we want the guard itself to be under test regardless.
"""

from __future__ import annotations

import sys

import pytest

from kitaru.errors import KitaruFeatureNotAvailableError


def test_import_without_pydantic_ai_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Typed error when the `pydantic-ai-slim` extra is missing."""
    # Force a clean re-import of the adapter package and its `pydantic_ai`
    # dependency so the `try: import pydantic_ai` in __init__ runs again.
    for cached in list(sys.modules):
        if cached == "pydantic_ai" or cached.startswith("pydantic_ai."):
            monkeypatch.delitem(sys.modules, cached, raising=False)
        if cached.startswith("kitaru.adapters.pydantic_ai"):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    # Make `import pydantic_ai` raise ImportError.
    monkeypatch.setitem(sys.modules, "pydantic_ai", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="pydantic-ai-slim"):
        import kitaru.adapters.pydantic_ai  # noqa: F401
