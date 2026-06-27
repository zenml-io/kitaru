# ruff: noqa: E402
"""Low-cost live Google ADK/Gemini provider checks.

These tests are excluded from default pytest runs by the ``live_llm`` marker.
They are intended for trusted manual runs with provider credentials and the
isolated ``google-adk`` optional extra installed.
"""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

pytestmark = [pytest.mark.live_llm, pytest.mark.live_gemini]

pytest.importorskip("google.adk")

from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig
from google.adk.runners import InMemoryRunner
from google.genai import types as genai_types

from kitaru.adapters.google_adk import (
    ADKRunRequest,
    KitaruADKRunner,
    final_output_preview,
)

_PROMPT = "Explain one Kitaru checkpoint in one short sentence. Do not use tools."
_MODEL = os.environ.get("KITARU_LIVE_GOOGLE_ADK_MODEL", "gemini-2.5-flash")


def _text_content(text: str, *, role: str) -> Any:
    part_cls = genai_types.Part
    if hasattr(part_cls, "from_text"):
        part = part_cls.from_text(text=text)
    else:
        part = part_cls(text=text)
    return genai_types.Content(role=role, parts=[part])


async def _create_runner_session(
    runner: Any,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
) -> None:
    session_service = getattr(runner, "session_service", None)
    if session_service is None:
        pytest.fail("Installed ADK runner does not expose `.session_service`.")
    create_session = getattr(session_service, "create_session", None)
    if not callable(create_session):
        pytest.fail("Installed ADK session service has no `create_session(...)`.")

    parameters = inspect.signature(create_session).parameters
    kwargs: dict[str, Any] = {}
    if "app_name" in parameters:
        kwargs["app_name"] = app_name
    if "user_id" in parameters:
        kwargs["user_id"] = user_id
    if "session_id" in parameters:
        kwargs["session_id"] = session_id
    elif "id" in parameters:
        kwargs["id"] = session_id

    session = create_session(**kwargs)
    if inspect.isawaitable(session):
        await session


def _contains_non_null_key(value: Any, key: str) -> bool:
    if isinstance(value, Mapping):
        return any(
            (item_key == key and item is not None) or _contains_non_null_key(item, key)
            for item_key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return any(_contains_non_null_key(item, key) for item in value)
    return False


def _prepare_google_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("GOOGLE_API_KEY"):
        return
    if gemini_key := os.environ.get("GEMINI_API_KEY"):
        monkeypatch.setenv("GOOGLE_API_KEY", gemini_key)


async def _run_model_only_turn() -> Any:
    app_name = "kitaru_live_google_adk_core_app"
    user_id = "live-google-adk-user"
    session_id = "live-google-adk-session"
    agent = LlmAgent(
        name="kitaru_live_google_adk_core_agent",
        model=_MODEL,
        instruction="Answer in one short sentence. Do not use tools.",
        tools=[],
    )
    runner = InMemoryRunner(agent=agent, app_name=app_name)
    await _create_runner_session(
        runner,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    kitaru_runner = KitaruADKRunner(
        runner,
        name=app_name,
        checkpoint_strategy="runner_call",
    )
    return await kitaru_runner.run(
        ADKRunRequest(
            user_id=user_id,
            session_id=session_id,
            message=_text_content(_PROMPT, role="user"),
            run_kwargs={"run_config": RunConfig(max_llm_calls=1)},
        )
    )


def test_google_adk_runner_call_with_gemini_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Google ADK adapter can run one bounded Gemini-backed ADK turn."""
    if not (os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")):
        pytest.skip("GEMINI_API_KEY or GOOGLE_API_KEY is required for live ADK smoke")
    _prepare_google_api_key(monkeypatch)

    result = asyncio.run(_run_model_only_turn())
    preview = final_output_preview(result.final_output)

    assert result.status == "completed"
    assert preview
    assert not _contains_non_null_key(result.events, "functionCall")
