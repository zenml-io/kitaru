"""Provider-free fast-agent adapter example tests."""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
from pathlib import Path
from typing import Any

import pytest
from zenml.client import Client


def _purge_fast_agent_modules() -> None:
    for cached in list(sys.modules):
        if cached == "fast_agent" or cached.startswith("fast_agent."):
            sys.modules.pop(cached, None)


def _import_real_fast_agent() -> None:
    """Import real fast-agent or skip when the optional runtime is absent."""
    errors: list[str] = []
    local_src = Path(__file__).resolve().parents[1] / "design" / "fast-agent" / "src"
    candidates: list[Path | None] = [None]
    if local_src.exists() and os.environ.get("KITARU_TEST_FAST_AGENT_LOCAL_SRC") == "1":
        candidates.insert(0, local_src)

    for candidate in candidates:
        _purge_fast_agent_modules()
        if str(local_src) in sys.path:
            sys.path.remove(str(local_src))
        if candidate is not None:
            sys.path.insert(0, str(candidate))
        try:
            importlib.import_module("fast_agent.core.agent_app")
            importlib.import_module("fast_agent.agents.tool_agent")
            importlib.import_module("fast_agent.types")
            return
        except Exception as exc:  # pragma: no cover - optional package state
            source = (
                str(candidate) if candidate is not None else "installed fast-agent-mcp"
            )
            errors.append(f"{source}: {type(exc).__name__}: {exc}")

    pytest.skip("fast-agent is unavailable or incompatible: " + "; ".join(errors))


def _wait_for_hydrated_run(exec_id: str) -> Any:
    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    assert run.status.is_successful
    return run.get_hydrated_version()


def _step_names(hydrated_run: Any) -> set[str]:
    return set(hydrated_run.steps)


def test_fast_agent_provider_free_example_runs(primed_zenml: None) -> None:
    del primed_zenml
    _import_real_fast_agent()

    from examples.integrations.fast_agent_agent.fast_agent_adapter import (
        _run_agent_turns,
        fast_agent_demo_flow,
    )

    direct_result = asyncio.run(_run_agent_turns("hello from pytest"))
    assert direct_result.model_reply == "memory reply to hello from pytest"
    assert direct_result.app_tool_reply == "memory tool loop complete"
    assert "REPLAY" in direct_result.direct_tool_reply
    assert direct_result.model_generate_calls == 3

    handle = fast_agent_demo_flow.run("hello from pytest", cache=False)
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)
    step_names = _step_names(hydrated_run)

    assert any("generate_model_call" in name for name in step_names)
    assert any("uppercase_tool_call" in name for name in step_names)


def test_fast_agent_example_builds_real_app_without_provider_credentials() -> None:
    _import_real_fast_agent()

    from examples.integrations.fast_agent_agent.fast_agent_adapter import (
        _build_demo_app,
    )

    fast_agent_run, agent, llm = asyncio.run(_build_demo_app())

    assert fast_agent_run is not None
    assert agent.config.name == "fast_agent_demo"
    assert llm.model_name == "memory-fast-agent-demo"
