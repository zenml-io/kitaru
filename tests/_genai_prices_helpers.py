"""Test helpers for installing a deterministic fake genai_prices module."""

import sys
from types import SimpleNamespace

import pytest


class FakeGenAIUsage:
    """Small stand-in for genai_prices.Usage that preserves constructor args."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def install_fake_genai_calc_price(
    monkeypatch: pytest.MonkeyPatch,
    *,
    total_price: float = 0.0123,
    calc_error: Exception | None = None,
) -> list[dict[str, object]]:
    """Install a fake genai_prices module and return captured calc_price calls."""
    calls: list[dict[str, object]] = []

    def calc_price(
        usage: FakeGenAIUsage,
        model_ref: str,
        *,
        provider_id: str | None = None,
    ) -> SimpleNamespace:
        calls.append(
            {
                "usage": usage.kwargs,
                "model_ref": model_ref,
                "provider_id": provider_id,
            }
        )
        if calc_error is not None:
            raise calc_error
        return SimpleNamespace(total_price=total_price)

    monkeypatch.setitem(
        sys.modules,
        "genai_prices",
        SimpleNamespace(Usage=FakeGenAIUsage, calc_price=calc_price),
    )
    return calls
