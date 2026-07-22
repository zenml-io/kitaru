# ruff: noqa: E402
"""Provider-extended live Google ADK/Gemini behavior checks."""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.live_llm,
    pytest.mark.live_gemini,
    pytest.mark.provider_extended,
]

_adk_agents = pytest.importorskip("google.adk.agents")
_adk_function_tool = pytest.importorskip("google.adk.tools.function_tool")
_adk_run_config = pytest.importorskip("google.adk.agents.run_config")
_adk_runners = pytest.importorskip("google.adk.runners")

from examples.integrations.google_adk_agent.google_adk_adapter import (
    prepare_live_google_credentials,
)
from google.genai import types as genai_types

from kitaru.adapters.google_adk import (
    ADKRunRequest,
    KitaruADKRunner,
    KitaruADKTool,
    final_output_preview,
)

LlmAgent: Any = _adk_agents.LlmAgent
FunctionTool: Any = _adk_function_tool.FunctionTool
RunConfig: Any = _adk_run_config.RunConfig
InMemoryRunner: Any = _adk_runners.InMemoryRunner

_MODEL = os.environ.get("KITARU_LIVE_GOOGLE_ADK_MODEL", "gemini-2.5-flash")
_TOOL_NAME = "lookup_live_number"
_TOOL_MARKER = "google-adk-live-tool-marker-7319"


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


def _contains_text(value: Any, text: str) -> bool:
    if isinstance(value, str):
        return text in value
    if isinstance(value, Mapping):
        return any(_contains_text(item, text) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return any(_contains_text(item, text) for item in value)
    return False


async def _run_tool_turn() -> Any:
    app_name = f"kitaru_live_google_adk_behavior_{uuid4().hex[:8]}"
    user_id = "live-google-adk-behavior-user"
    session_id = f"live-google-adk-behavior-{uuid4().hex}"
    physical_tool_calls: list[int] = []

    def lookup_live_number(value: int) -> dict[str, Any]:
        """Return a deterministic marker for a live Google ADK tool call."""
        physical_tool_calls.append(value)
        return {"value": value, "answer": value + 1, "marker": _TOOL_MARKER}

    agent = LlmAgent(
        name="kitaru_live_google_adk_behavior_agent",
        model=_MODEL,
        instruction=(
            f"Call {_TOOL_NAME} exactly once with value=41. After the tool returns, "
            "answer in one short sentence and include the tool answer."
        ),
        tools=[
            KitaruADKTool(
                FunctionTool(lookup_live_number),
                name=_TOOL_NAME,
            )
        ],
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
        checkpoint_strategy="calls",
    )
    result = await kitaru_runner.run(
        ADKRunRequest(
            user_id=user_id,
            session_id=session_id,
            message=_text_content(
                f"Use {_TOOL_NAME} now with value 41. Marker: {_TOOL_MARKER}.",
                role="user",
            ),
            run_kwargs={"run_config": RunConfig(max_llm_calls=3)},
        )
    )
    assert physical_tool_calls == [41]
    return result


def test_google_adk_runner_call_with_gemini_captures_function_call(
    primed_zenml,
) -> None:
    """The Google ADK adapter captures a real Gemini function/tool turn."""
    prepare_live_google_credentials()

    result = asyncio.run(_run_tool_turn())
    preview = final_output_preview(result.final_output)

    assert result.status == "completed"
    assert preview
    assert _contains_non_null_key(result.events, "functionCall")
    assert _contains_non_null_key(result.events, "functionResponse")
    assert _contains_text(result.events, _TOOL_MARKER)
