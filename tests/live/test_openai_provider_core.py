# ruff: noqa: E402
"""Low-cost live OpenAI provider checks.

These tests are excluded from default pytest runs by the ``live_llm`` marker.
They are intended for trusted manual/weekly runs with provider credentials.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.live_llm, pytest.mark.live_openai]

agents = pytest.importorskip("agents")

from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

_PROMPT = (
    "Explain one Kitaru checkpoint in one short sentence. "
    "Do not use tools, Bash, or files."
)
_OPENAI_MODEL = os.environ.get("KITARU_LIVE_OPENAI_MODEL", "gpt-4o-mini")


def _runner() -> KitaruRunner:
    agent = agents.Agent(
        name="kitaru-live-openai-core",
        instructions=(
            "Answer in one short sentence. Do not use tools, Bash, files, or web."
        ),
        model=_OPENAI_MODEL,
    )
    return KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: agents.RunConfig(tracing_disabled=True),
    )


def test_openai_agents_adapter_basic_run_completes() -> None:
    """The OpenAI Agents adapter can run one bounded provider call."""
    result = _runner().run_sync(OpenAIRunRequest.start(_PROMPT, max_turns=1))

    assert result.status == "completed"
    assert isinstance(result.final_output, str)
    assert result.final_output.strip()


def test_openai_agents_adapter_streaming_run_completes() -> None:
    """The OpenAI Agents streaming adapter can run one bounded provider call."""
    result = _runner().run_stream_sync(OpenAIRunRequest.start(_PROMPT, max_turns=1))

    assert result.status == "completed"
    assert isinstance(result.final_output, str)
    assert result.final_output.strip()
