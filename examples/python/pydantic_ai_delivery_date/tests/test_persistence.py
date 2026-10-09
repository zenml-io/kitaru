"""Real-server round trips for scenario inputs and execution evidence."""

import asyncio
import os
import subprocess
import sys
from uuid import UUID

import pytest
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeType
from kitaru.client import KitaruAPIClient
from kitaru.json_pointer import resolve_json_pointer

from delivery_date.models import get_scenario
from delivery_date.persistence import load_scenario, record_result
from delivery_date.runner import run_scenario

SERVER = os.environ.get("KITARU_DELIVERY_E2E_URL", "")
pytestmark = pytest.mark.skipif(
    not SERVER, reason="Set KITARU_DELIVERY_E2E_URL to an isolated local test server."
)


async def get_evaluations(client: KitaruAPIClient, session_id: UUID):
    params = EvaluationListParams(
        filter=FilterCondition(
            field="session_id", op=FilterOp.EQ, value=str(session_id)
        )
    )
    return [row async for row in client.evaluations.iter(params)]


@pytest.mark.parametrize("mode", ["full", "n-minus-one", "tool-boundary"])
def test_snapshot_round_trip_and_new_tool_accounting(mode: str) -> None:
    async def check() -> None:
        result = await run_scenario(
            get_scenario("missing-date"), mode=mode, variant="fix"
        )
        session_id = await record_result(
            result, server_url=SERVER, api_key="local-test"
        )
        restored = await load_scenario(
            session_id=session_id, server_url=SERVER, api_key="local-test"
        )
        assert restored.model_dump(mode="json") == result.scenario.model_dump(
            mode="json"
        )
        assert restored.calculate_hash() == result.scenario_hash
        rerun = await run_scenario(restored, mode=mode, variant="fix")
        assert rerun.input_seed == result.input_seed
        assert rerun.scenario_hash == result.scenario_hash
        assert rerun.verdict == result.verdict
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            full = await client.sessions.get_with_nodes(session_id)
            assert full.session.status == SessionStatus.COMPLETED
            assert full.session.outputs["backend"] == "scripted"
            assert full.session.outputs["input_seed"] == result.input_seed
            assert (
                sum(n.node_type == NodeType.TOOL_CALL for n in full.nodes)
                == result.tool_calls
            )
            assert not any(n.node_type == NodeType.LLM_CALL for n in full.nodes)
            responses = [n for n in full.nodes if n.external_id.startswith("response-")]
            assert responses
            for node in responses:
                expected = [
                    part["content"]
                    for message in node.inputs["history"]
                    for part in message["parts"]
                    if part["part_kind"] == "user-prompt"
                ][-1]
                assert resolve_json_pointer(node.inputs, node.input_text_selector) == (
                    True,
                    expected,
                )
            if mode == "full":
                assert (
                    len(
                        {
                            resolve_json_pointer(n.inputs, n.input_text_selector)
                            for n in responses
                        }
                    )
                    == 2
                )
            evaluations = await get_evaluations(client, session_id)
            assert len(evaluations) == 2
            assert all(row.passed is True for row in evaluations)
        assert (
            await record_result(result, server_url=SERVER, api_key="local-test")
            == session_id
        )

    asyncio.run(check())


def test_incomplete_simulation_is_not_a_passing_execution() -> None:
    async def check() -> None:
        result = await run_scenario(get_scenario("missing-date"), variant="fix")
        result = result.model_copy(update={"status": "turn-limit"})
        session_id = await record_result(
            result, server_url=SERVER, api_key="local-test"
        )
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            session = await client.sessions.get(session_id)
            assert session.status == SessionStatus.FAILED
            assert "turn-limit" in session.error
            rows = await get_evaluations(client, session_id)
            assert len(rows) == 2
            assert all(
                row.passed is None and row.value == "unavailable" for row in rows
            )

    asyncio.run(check())


def test_recording_retry_repairs_evaluations_after_finalization(monkeypatch) -> None:
    import delivery_date.persistence as persistence

    original = persistence._save_evaluations

    async def interrupted(*args) -> None:
        raise ConnectionError("Recording interrupted after finalization")

    async def check() -> None:
        result = await run_scenario(get_scenario("known-date"), variant="fix")
        monkeypatch.setattr(persistence, "_save_evaluations", interrupted)
        with pytest.raises(ConnectionError):
            await record_result(result, server_url=SERVER, api_key="local-test")
        monkeypatch.setattr(persistence, "_save_evaluations", original)
        session_id = await record_result(
            result, server_url=SERVER, api_key="local-test"
        )
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            rows = await get_evaluations(client, session_id)
            assert len(rows) == 2
            assert all(row.passed is True for row in rows)

    asyncio.run(check())


def test_cli_loads_saved_scenario_without_an_export_file() -> None:
    async def save() -> UUID:
        result = await run_scenario(get_scenario("known-date"), variant="fix")
        return await record_result(result, server_url=SERVER, api_key="local-test")

    session_id = asyncio.run(save())
    command = subprocess.run(
        [
            sys.executable,
            "-m",
            "delivery_date",
            "--backend",
            "scripted",
            "--from-session",
            str(session_id),
            "--server-url",
            SERVER,
            "--variant",
            "fix",
            "--mode",
            "tool-boundary",
            "--check",
        ],
        env=os.environ | {"KITARU_API_KEY": "local-test", "PYDANTIC_AI_NO_BANNER": "1"},
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert command.returncode == 0, command.stdout + command.stderr
    assert "known-date | fix | tool-boundary | scripted" in command.stdout
    assert "Agent checks: PASS" in command.stdout
    assert "HISTORICAL tool" in command.stdout


def test_failed_customer_diagnostics_survive_recording() -> None:
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.models.function import FunctionModel

    def invalid_customer(messages, info) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {"next_message": "Delivery is 2024-05-02."},
                    f"invalid-{len(messages)}",
                )
            ]
        )

    async def check() -> None:
        result = await run_scenario(
            get_scenario("missing-date"),
            simulator_model=FunctionModel(invalid_customer),
        )
        assert result.status == "invalid-simulation"
        session_id = await record_result(
            result, server_url=SERVER, api_key="local-test"
        )
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            session = await client.sessions.get(session_id)
            assert session.status == SessionStatus.FAILED
            attempt = session.outputs["simulator_attempts"][0]
            assert "Exceeded maximum output retries" in attempt["error"]
            assert "2024-05-02" in str(attempt["messages"])
            assert "Do not invent a delivery date" in str(attempt["messages"])
            assert "2024-05-02" not in str(session.outputs["messages"])

    asyncio.run(check())
