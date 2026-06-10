"""Shared test helpers for Gemini canonical usage metadata tests."""

from __future__ import annotations

import importlib
from typing import Any

import pytest


def collect_usage_records(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Patch the Gemini agent module to collect canonical usage records."""
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    records: list[dict[str, Any]] = []
    monkeypatch.setattr(
        agent,
        "log_usage_record",
        lambda record: records.append(dict(record)),
    )
    return records
