"""Readable conversation output and honest incomplete-run summaries."""

import asyncio
import sys

import pytest
from pydantic_ai.messages import ModelResponse, TextPart

from delivery_date import __main__ as cli
from delivery_date.models import SimulatorAttempt, get_scenario
from delivery_date.runner import run_scenario


def make_result():
    return asyncio.run(
        run_scenario(
            get_scenario("missing-date"),
            backend="scripted",
            variant="fix",
            mode="full",
        )
    )


def test_conversation_prints_tool_between_customer_and_agent(capsys) -> None:
    result = make_result()
    cli.print_result(result)
    output = capsys.readouterr().out
    assert output.index("NEW customer:") < output.index("EXECUTED tool: check_shipping")
    assert output.index("EXECUTED tool: check_shipping") < output.index("NEW agent:")
    assert "Scenario SHA256:" not in output
    assert "model requests=" not in output
    assert "fulfillment_status" not in output
    assert "Simulation complete." in output
    assert "Agent checks: PASS" in output


@pytest.mark.parametrize(
    ("status", "summary"),
    [
        ("invalid-simulation", "customer continuation failed"),
        ("agent-error", "delivery agent failed"),
        ("turn-limit", "agent-turn limit reached"),
    ],
)
def test_incomplete_run_has_no_passing_verdict(status, summary, capsys) -> None:
    result = make_result().model_copy(update={"status": status, "error": "diagnostic"})
    cli.print_result(result)
    output = capsys.readouterr().out
    assert f"Simulation incomplete: {summary}." in output
    assert "PASS" not in output
    assert "Business verdict" not in output
    assert "partial conversation" in output
    assert "diagnostic" not in output
    assert "--verbose" in output


def test_verbose_retains_tool_payload_hash_counts_and_error(capsys) -> None:
    result = make_result().model_copy(
        update={"status": "invalid-simulation", "error": "invalid customer date"}
    )
    cli.print_result(result, verbose=True)
    output = capsys.readouterr().out
    assert "fulfillment_status" in output
    assert f"Scenario SHA256: {result.scenario_hash}" in output
    assert "model requests=" in output
    assert "Error: invalid customer date" in output
    assert "PASS" not in output


def test_historical_tool_precedes_new_reply(capsys) -> None:
    result = asyncio.run(
        run_scenario(get_scenario("missing-date"), mode="tool-boundary", variant="fix")
    )
    cli.print_result(result)
    output = capsys.readouterr().out
    assert output.index("PREFIX customer:") < output.index("HISTORICAL tool:")
    assert output.index("HISTORICAL tool:") < output.index("NEW agent:")
    assert "Next-reply test complete." in output


def test_incomplete_cli_exits_unsuccessfully_without_check(monkeypatch, capsys) -> None:
    result = make_result().model_copy(update={"status": "invalid-simulation"})

    async def run(*args, **kwargs):
        return result

    monkeypatch.setattr(cli, "run_scenario", run)
    monkeypatch.setattr(
        sys, "argv", ["delivery_date", "--scenario", "missing-date", "--variant", "fix"]
    )
    assert asyncio.run(cli.main()) == 1
    assert "PASS" not in capsys.readouterr().out


def test_verbose_exposes_rejected_simulator_response_and_error(capsys) -> None:
    result = make_result().model_copy(
        update={
            "status": "invalid-simulation",
            "simulator_attempts": [
                SimulatorAttempt(
                    messages=[
                        ModelResponse(parts=[TextPart("invented customer date")])
                    ],
                    error="ISO date was not visible to the customer",
                )
            ],
        }
    )
    cli.print_result(result)
    normal = capsys.readouterr().out
    assert "invented customer date" not in normal
    assert "ISO date was not visible" not in normal
    cli.print_result(result, verbose=True)
    verbose = capsys.readouterr().out
    assert "Customer simulator call 1:" in verbose
    assert "invented customer date" in verbose
    assert "ISO date was not visible to the customer" in verbose
