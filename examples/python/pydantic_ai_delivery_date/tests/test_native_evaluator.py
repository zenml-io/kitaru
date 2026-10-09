"""Independent evaluation uses recorded evidence, not the runner's verdict."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.plugins import load_plugin_entrypoint

from delivery_date.evaluator import evaluate_delivery, get_evaluator_source
from delivery_date.models import get_scenario
from delivery_date.runner import run_scenario


def make_view(scenario_name="missing-date", variant="fix", mode="full"):
    result = asyncio.run(
        run_scenario(get_scenario(scenario_name), variant=variant, mode=mode)
    )
    outputs = result.model_dump(mode="json")
    return SimpleNamespace(
        session=SimpleNamespace(
            inputs={
                "scenario_snapshot": outputs["scenario"],
                "input_seed": result.input_seed,
            },
            outputs=outputs,
            status=SessionStatus.COMPLETED,
        ),
        nodes=[],
    )


def test_evaluator_ignores_claimed_runner_verdict():
    view = make_view(variant="control")
    view.session.outputs["verdict"] = {
        "no_unsupported_date": True,
        "uses_supported_date": True,
    }
    checks = {row.name: row.passed for row in evaluate_delivery(view)}
    assert checks == {
        "simulation_complete": True,
        "no_unsupported_date": False,
        "uses_supported_date": True,
    }


def test_evaluator_ignores_historical_date_mentions():
    view = make_view(mode="tool-boundary")
    # Only new messages count, even if a historical assistant contained a bad date.
    prefix = {
        "kind": "response",
        "parts": [{"part_kind": "text", "content": "It arrives 2099-01-01."}],
    }
    view.session.inputs["input_seed"].insert(0, prefix)
    view.session.outputs["messages"].insert(0, deepcopy(prefix))
    view.session.outputs["seed_message_count"] += 1
    assert all(row.passed for row in evaluate_delivery(view))


@pytest.mark.parametrize("status", ["turn-limit", "invalid-simulation", "agent-error"])
def test_incomplete_simulations_cannot_pass(status):
    view = make_view()
    view.session.outputs["status"] = status
    assert not any(row.passed for row in evaluate_delivery(view))


def test_missing_or_changed_evidence_is_rejected():
    view = make_view()
    view.session.inputs["scenario_snapshot"] = deepcopy(
        view.session.inputs["scenario_snapshot"]
    )
    view.session.inputs["scenario_snapshot"]["shipping"]["estimated_delivery"] = (
        "2099-01-01"
    )
    with pytest.raises(ValueError, match="snapshot"):
        evaluate_delivery(view)


def test_uploaded_script_is_self_contained(tmp_path):
    path = tmp_path / "delivery_checks.py"
    path.write_bytes(get_evaluator_source())
    evaluator = load_plugin_entrypoint(path, "evaluate_delivery", "Evaluator")
    view = make_view("known-date")
    assert all(row.passed for row in evaluator(view))
