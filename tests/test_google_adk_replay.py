"""Replay/cache proofs for Google ADK wrapper checkpoints."""

import asyncio
import importlib
import time
from typing import Any
from uuid import uuid4

from zenml.client import Client

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.flow import flow


class ReplayProofModel:
    model = "gemini-fake-replay"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def supported_models(self) -> list[str]:
        return [self.model]

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        self.calls.append({"request": dict(llm_request), "stream": stream})
        yield {"text": f"model:{llm_request['prompt']}", "stream": stream}


class ReplayProofTool:
    name = "replay_lookup"
    description = "Local replay proof lookup."

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["lookup_marker"] = f"tool:{args['query']}"
        return {"answer": f"tool:{args['query']}"}


class ReplayProofToolContext:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {"seed": "same"}


async def _collect_model_events(model: Any, request: dict[str, Any]) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def _wait_for_hydrated_run(exec_id: str) -> Any:
    deadline = time.monotonic() + 10
    last_run: Any | None = None
    while time.monotonic() < deadline:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
        last_run = run
        if run.status.is_finished:
            assert run.status.is_successful
            return run.get_hydrated_version()
        time.sleep(0.1)
    assert last_run is not None
    raise AssertionError(
        f"Pipeline run {exec_id} did not finish within 10 seconds; "
        f"last status was {last_run.status}."
    )


def test_wrapped_model_and_tool_flow_reuses_cached_checkpoint_outputs(
    monkeypatch,
    primed_zenml,
) -> None:
    _ = primed_zenml
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")

    local_model = ReplayProofModel()
    local_tool = ReplayProofTool()
    policy = adapter.ADKCallCheckpointPolicy(
        model_checkpoint_config={"cache": True},
        tool_checkpoint_config={"cache": True},
    )
    wrapped_model = adapter.KitaruADKModel(
        local_model,
        name=f"model_{uuid4().hex[:8]}",
        call_policy=policy,
    )
    wrapped_tool = adapter.KitaruADKTool(
        local_tool,
        name=f"tool_{uuid4().hex[:8]}",
        call_policy=policy,
    )

    @flow(cache=True)
    def adk_replay_proof_flow(query: str, nonce: str) -> dict[str, Any]:
        _ = nonce
        model_events = asyncio.run(
            _collect_model_events(wrapped_model, {"prompt": query})
        )
        tool_context = ReplayProofToolContext()
        tool_result = asyncio.run(
            wrapped_tool.run_async(
                args={"query": query},
                tool_context=tool_context,
            )
        )
        return {
            "model_events": model_events,
            "tool_result": tool_result,
            "tool_state": tool_context.state,
        }

    first_handle = adk_replay_proof_flow.run("cats", "first", cache=True)
    first_result = first_handle.wait()

    assert first_result == {
        "model_events": [{"text": "model:cats", "stream": False}],
        "tool_result": {"answer": "tool:cats"},
        "tool_state": {"seed": "same", "lookup_marker": "tool:cats"},
    }
    assert local_model.calls == [{"request": {"prompt": "cats"}, "stream": False}]
    assert local_tool.calls == [{"query": "cats"}]
    step_names = set(_wait_for_hydrated_run(first_handle.exec_id).steps)
    assert any(name.startswith("google_adk_model_") for name in step_names)
    assert any(name.startswith("google_adk_tool_") for name in step_names)

    second_result = adk_replay_proof_flow.run("cats", "second", cache=True).wait()

    assert second_result == first_result
    assert local_model.calls == [{"request": {"prompt": "cats"}, "stream": False}]
    assert local_tool.calls == [{"query": "cats"}]
