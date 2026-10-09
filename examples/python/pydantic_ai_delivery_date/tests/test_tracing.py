"""Provider-free checks for canonical payloads and explicit recording sources."""

import asyncio
import importlib.util
import json
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from delivery_date import tracing
from delivery_date.models import Scenario, get_scenario
from delivery_date.runner import run_scenario


def test_trace_payload_preserves_native_seed_and_actual_output() -> None:
    result = asyncio.run(
        run_scenario(get_scenario("missing-date"), mode="tool-boundary", variant="fix")
    )
    payload = tracing.build_trace_input(result, continue_after_boundary=False)
    assert set(payload) == {
        "schema_version",
        "scenario_snapshot",
        "scenario_sha256",
        "input_seed",
        "variant",
        "mode",
        "backend",
        "model_name",
        "continue_after_boundary",
    }
    assert payload["schema_version"] == "delivery-date-run.v1"
    assert (
        Scenario.model_validate(payload["scenario_snapshot"]).calculate_hash()
        == payload["scenario_sha256"]
    )
    assert payload["input_seed"] == result.input_seed
    assert payload["input_seed"][-1]["parts"][0]["part_kind"] == "tool-return"
    output = tracing.build_trace_output(result)
    assert output["messages"] == result.model_dump(mode="json")["messages"]
    assert "delivery date is unknown" in output["transcript_text"]
    assert output["backend"] == "scripted"


def test_provider_numeric_serialization_preserves_scenario_hash() -> None:
    result = asyncio.run(run_scenario(get_scenario("missing-date"), variant="fix"))
    snapshot = result.scenario.model_dump(mode="json")

    def provider_json(value):
        if isinstance(value, dict):
            return {key: provider_json(item) for key, item in value.items()}
        if isinstance(value, list):
            return [provider_json(item) for item in value]
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    assert (
        Scenario.model_validate(provider_json(snapshot)).calculate_hash()
        == result.scenario_hash
    )
    changed = provider_json(snapshot)
    changed["shipping"]["estimated_delivery"] = "2026-10-09"
    assert Scenario.model_validate(changed).calculate_hash() != result.scenario_hash


@pytest.mark.parametrize("export_state", ["complete", "eventual", "timeout"])
def test_native_root_captures_run_before_export(
    monkeypatch: pytest.MonkeyPatch,
    export_state: str,
) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "test-public")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "test-secret")
    events = []
    root_data = {}
    trace_data = {}
    reads = executions = 0
    delays = []
    real_sleep = asyncio.sleep
    original_run = tracing.run_scenario

    async def counted_run(*args, **kwargs):
        nonlocal executions
        executions += 1
        return await original_run(*args, **kwargs)

    async def fast_sleep(seconds):
        delays.append(seconds)
        await real_sleep(0.001)

    monkeypatch.setattr(tracing, "run_scenario", counted_run)
    monkeypatch.setattr(tracing.asyncio, "sleep", fast_sleep)

    class Root:
        def update(self, **values):
            root_data.update(values)

    class Client:
        def set_current_trace_io(self, **values):
            trace_data.update(values)

        @contextmanager
        def start_as_current_observation(self, **values):
            events.append("root-start")
            yield Root()
            events.append("root-end")

        def flush(self):
            events.append("flush")

        def get_trace_url(self, **values):
            return "https://langfuse.example/trace/abc"

    async def wait_for_trace(trace_id):
        nonlocal reads
        reads += 1
        assert events[-2:] == ["root-end", "flush"]
        assert trace_data == root_data
        if export_state == "timeout" or (export_state == "eventual" and reads == 1):
            return {"input": trace_data["input"], "output": None}
        return trace_data

    langfuse = types.ModuleType("langfuse")
    langfuse.Langfuse = SimpleNamespace(create_trace_id=lambda: "a" * 32)
    langfuse.get_client = lambda: Client()
    langfuse.propagate_attributes = lambda **kwargs: contextmanager(
        lambda: iter([None])
    )()
    langfuse_types = types.ModuleType("langfuse.types")
    langfuse_types.TraceContext = lambda **kwargs: kwargs
    importer_api = types.ModuleType("kitaru_langfuse_importer.api")
    importer_api.wait_for_trace = wait_for_trace
    importer_api.serialize_trace = lambda trace: json.dumps(trace).encode()
    for name, module in (
        ("langfuse", langfuse),
        ("langfuse.types", langfuse_types),
        ("kitaru_langfuse_importer.api", importer_api),
    ):
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(tracing.Agent, "instrument_all", lambda: None)
    execution = tracing.trace_scenario(
        get_scenario("known-date"),
        variant="fix",
        backend="scripted",
        timeout=0.01 if export_state == "timeout" else 180,
    )
    if export_state == "timeout":
        with pytest.raises(TimeoutError, match="Langfuse trace " + "a" * 32):
            asyncio.run(execution)
        assert executions == 1
        assert reads >= 1
        assert delays and set(delays) == {2}
        return
    result, payload, url = asyncio.run(execution)
    assert executions == 1
    assert reads == (2 if export_state == "eventual" else 1)
    assert delays == ([2] if export_state == "eventual" else [])
    assert json.loads(payload)["output"]["run_id"] == str(result.run_id)
    assert trace_data["input"]["backend"] == "scripted"
    assert url == "https://langfuse.example/trace/abc"


def test_missing_credentials_do_not_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    with pytest.raises(ValueError, match="LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY"):
        asyncio.run(tracing.trace_scenario(get_scenario("missing-date"), variant="fix"))


def test_prepare_defaults_require_langfuse_and_luna(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = Path(__file__).parents[1] / "scripts" / "prepare_sessions.py"
    spec = importlib.util.spec_from_file_location("prepare_delivery_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(
        sys, "argv", [str(path), "--server-url", "http://localhost:8000"]
    )
    args = module.get_args()
    assert (args.source, args.backend, args.model) == (
        "langfuse",
        "openai",
        "gpt-6-luna",
    )


def test_importer_retains_canonical_payload_in_its_turn_contract() -> None:
    from kitaru.task.importer import ImportedSession

    importer = pytest.importorskip("kitaru_langfuse_importer.importer")

    result = asyncio.run(run_scenario(get_scenario("missing-date"), variant="fix"))
    inputs = tracing.build_trace_input(result, continue_after_boundary=False)
    outputs = tracing.build_trace_output(result)
    fixture = {
        "id": "synthetic-native-export-shape",
        "projectId": "synthetic-langfuse-project",
        "sessionId": "synthetic-delivery-session",
        "timestamp": result.started_at.isoformat(),
        "input": inputs,
        "output": outputs,
        "observations": [
            {
                "id": "synthetic-root",
                "traceId": "synthetic-native-export-shape",
                "type": "SPAN",
                "name": "Delivery conversation",
                "startTime": result.started_at.isoformat(),
                "endTime": result.ended_at.isoformat(),
                "input": inputs,
                "output": outputs,
                "metadata": {"framework": "pydantic-ai"},
            }
        ],
    }
    sessions = list(importer.parse(json.dumps(fixture).encode(), {}))
    assert len(sessions) == 1
    assert isinstance(sessions[0], ImportedSession)
    session = sessions[0]
    assert session.inputs["turns"][0]["inputs"] == inputs
    assert session.outputs == outputs
