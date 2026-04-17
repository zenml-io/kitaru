"""Import-time guard tests for the PydanticAI adapter.

Lives outside the adapter test files so `pytest.importorskip("pydantic_ai")`
doesn't short-circuit the guard when the extra is installed.
"""

from __future__ import annotations

import sys

import pytest

from kitaru.errors import KitaruFeatureNotAvailableError


def test_import_without_pydantic_ai_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for cached in list(sys.modules):
        if cached == "pydantic_ai" or cached.startswith("pydantic_ai."):
            monkeypatch.delitem(sys.modules, cached, raising=False)
        if cached.startswith("kitaru.adapters.pydantic_ai"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.setitem(sys.modules, "pydantic_ai", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="pydantic-ai-slim"):
        import kitaru.adapters.pydantic_ai  # noqa: F401
