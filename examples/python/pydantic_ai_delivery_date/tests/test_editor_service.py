"""Keep execution outcomes distinct from failed result recording."""

import asyncio
from uuid import uuid4

from delivery_date import editor_service
from delivery_date.editor_service import CompareRequest, EditorService
from delivery_date.models import get_scenario
from delivery_date.runner import run_scenario
from delivery_date.sources import to_editor


def test_recording_failure_is_preserved_after_successful_simulation(monkeypatch):
    async def check():
        result = await run_scenario(get_scenario("missing-date"), variant="fix")
        case_id = str(uuid4())
        case = {
            "sessionId": case_id,
            "sourceId": str(uuid4()),
            "title": "Known failure",
            "scenario": to_editor(result.scenario, {}).model_dump(),
        }
        service = EditorService(None)

        async def read(request):
            return {"cases": [case]}

        async def execute(*args, **kwargs):
            return {
                "passed": False,
                "cases": [
                    {
                        "sourceSessionId": case_id,
                        "sessionId": None,
                        "result": result.model_dump(mode="json"),
                        "status": "partial",
                        "passed": False,
                        "error": "private-provider-diagnostic",
                    }
                ],
            }

        monkeypatch.setattr(service, "read", read)
        monkeypatch.setattr(editor_service, "execute_test_set", execute)
        service.client = type("Client", (), {"base_url": "http://example.test"})()
        displayed = await service.compare(CompareRequest(cohort_version_id=uuid4()))
        row = displayed["results"][0]
        assert row["status"] == "completed"
        assert row["receiptStatus"] == "partial"
        assert row["receiptPassed"] is False
        assert row["sessionId"] is None
        assert row["receiptError"] == "The result could not be recorded in Kitaru."
        assert "private-provider-diagnostic" not in str(displayed)

    asyncio.run(check())


def test_policy_options_are_review_only_and_serialized(monkeypatch):
    from delivery_date.policy import Policy, PolicyRequest

    request = PolicyRequest(
        baseline=Policy(name="Original", prompt="Use evidence"),
        scenario=to_editor(get_scenario("missing-date"), {}),
    )
    requests = []
    options = [
        {
            "policy": {"name": "Concise", "prompt": "Answer concisely from evidence"},
            "rationale": "Less repetition",
        },
        {
            "policy": {"name": "Cautious", "prompt": "State uncertainty from evidence"},
            "rationale": "Handle delivery pressure",
        },
        {
            "policy": {"name": "Helpful", "prompt": "Explain evidence and next steps"},
            "rationale": "Make the next action clear",
        },
    ]

    async def propose(value):
        requests.append(value)
        return {
            "proposals": options,
            "model": "gpt-6-luna",
        }

    monkeypatch.setattr(editor_service, "propose_policy", propose)

    async def check():
        import pytest

        service = EditorService(None)
        result = await service.policy(request)
        assert result == {"proposals": options, "model": "gpt-6-luna"}
        assert requests == [request]
        assert request.baseline.prompt == "Use evidence"
        assert request.instruction == ""
        async with service._model_lock:
            with pytest.raises(ValueError, match="already running"):
                await service.policy(request)
        assert len(requests) == 1

    asyncio.run(check())


def _make_receipt(**updates):
    from delivery_date.experiments import ExperimentReceipt

    source_id = str(uuid4())
    return ExperimentReceipt(
        experiment_id=uuid4(),
        agent_id=uuid4(),
        cohort_version_id=uuid4(),
        baseline_run_id=uuid4(),
        candidate_run_id=uuid4(),
        baseline_agent_version_id=uuid4(),
        candidate_agent_version_id=uuid4(),
        evaluator_id=uuid4(),
        evaluator_version_id=uuid4(),
        evaluator_name="delivery-date-checks",
        evaluator_version=1,
        evaluator_hash="a" * 64,
        runner_revision="b" * 64,
        baseline_policy_hash="c" * 64,
        candidate_policy_hash="d" * 64,
        case_hashes={source_id: "e" * 64},
        model="gpt-6-luna",
        **updates,
    )


def test_handoff_rechecks_stored_results_and_refuses_stale_ui_success(monkeypatch):
    import pytest

    from delivery_date.editor_service import ExperimentReadRequest

    receipt = _make_receipt(ready_for_handoff=True)

    async def read(client, pins):
        assert pins.experiment_id == receipt.experiment_id
        return receipt.model_copy(update={"ready_for_handoff": False})

    monkeypatch.setattr(editor_service, "read_experiment", read)

    async def check():
        service = EditorService(None)
        with pytest.raises(ValueError, match="passing candidate"):
            await service.handoff(ExperimentReadRequest(receipt=receipt))

    asyncio.run(check())


def test_handoff_contains_only_verified_pins_and_local_skill(monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from delivery_date.editor_service import ExperimentReadRequest

    receipt = _make_receipt(status="completed", ready_for_handoff=True)

    async def read(client, pins):
        return receipt

    monkeypatch.setattr(editor_service, "read_experiment", read)

    async def check():
        service = EditorService(SimpleNamespace(base_url="http://example.test"))
        handoff = await service.handoff(ExperimentReadRequest(receipt=receipt))
        assert handoff["pins"]["experiment_id"] == str(receipt.experiment_id)
        assert "ready_for_handoff" not in handoff["pins"]
        assert "candidate" not in handoff["pins"]
        assert "$kitaru-regression-pr" in handoff["prompt"]
        assert (
            str(
                Path(editor_service.__file__).resolve().parents[4]
                / ".agents/skills/kitaru-regression-pr/SKILL.md"
            )
            in handoff["prompt"]
        )
        assert json.dumps(handoff["pins"], indent=2) in handoff["prompt"]
        assert "--verify-only" in handoff["prompt"]
        assert "Leave merging for human review" in handoff["prompt"]

    asyncio.run(check())
