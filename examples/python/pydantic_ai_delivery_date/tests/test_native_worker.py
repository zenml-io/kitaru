"""Frozen replay validation and one task-linked session recording."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import AgentTaskDetails

from delivery_date import worker_run
from delivery_date.models import get_scenario
from delivery_date.persistence import SCHEMA_VERSION, get_runner_revision
from delivery_date.policy import get_default_policy
from delivery_date.runner import run_scenario


def make_inputs(result):
    return {
        "schema_version": SCHEMA_VERSION,
        "scenario_snapshot": result.scenario.model_dump(mode="json"),
        "scenario_sha256": result.scenario_hash,
        "input_seed": result.input_seed,
        "mode": result.mode,
        "continue_after_boundary": False,
        "runner_revision": get_runner_revision(),
    }


def test_worker_rejects_changed_snapshot_before_inference():
    result = asyncio.run(run_scenario(get_scenario("missing-date")))
    inputs = make_inputs(result)
    inputs["scenario_snapshot"]["shipping"]["estimated_delivery"] = "2099-01-01"
    with pytest.raises(ValueError, match="hash"):
        worker_run.read_worker_inputs(inputs)


def test_worker_records_one_replay_and_does_not_upload_manual_evaluations(monkeypatch):
    async def exercise():
        result = await run_scenario(get_scenario("missing-date"), mode="tool-boundary")
        policy = get_default_policy("fix")
        result = result.model_copy(
            update={
                "policy_name": policy.name,
                "policy_prompt": policy.prompt,
                "policy_hash": policy.calculate_hash(),
            }
        )
        inputs = make_inputs(result)
        task_id, session_id = uuid4(), uuid4()
        creates, updates, ingests = [], [], []

        class Tasks:
            async def get_spec(self, requested):
                assert requested == task_id
                return SimpleNamespace(
                    details=AgentTaskDetails(inputs=inputs, replay_id=uuid4())
                )

        class Sessions:
            async def create(self, request, **kwargs):
                creates.append(request)
                return SimpleNamespace(id=session_id)

            async def get(self, requested):
                return SimpleNamespace(
                    status=SessionStatus.IN_PROGRESS, metadata={}, outputs=None
                )

            async def update(self, requested, request):
                assert requested == session_id
                updates.append(request)

            async def ingest_nodes(self, requested, request):
                assert requested == session_id
                ingests.append(request)

        async def simulate(*args, **kwargs):
            assert kwargs["custom_policy"] == policy
            return result

        monkeypatch.setattr(worker_run, "run_scenario", simulate)
        outputs = []
        monkeypatch.setattr(worker_run, "write_task_result", outputs.append)
        monkeypatch.setenv("DELIVERY_POLICY_JSON", policy.model_dump_json())
        monkeypatch.setenv("DELIVERY_RUNNER_REVISION", get_runner_revision())
        client = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
        assert await worker_run.execute_task(client, task_id) == session_id
        assert len(creates) == 1
        assert creates[0].origin == SessionOrigin.REPLAY
        assert creates[0].agent_id is None and creates[0].agent_version_id is None
        assert updates[-1].status == SessionStatus.COMPLETED
        assert updates[-1].outputs["policy_hash"] == policy.calculate_hash()
        assert len(ingests) == 1
        assert outputs == [{"session_id": str(session_id)}]

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "stored_status",
    [SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.IN_PROGRESS],
)
def test_retry_preserves_finalized_or_started_recording(monkeypatch, stored_status):
    async def exercise():
        result = await run_scenario(get_scenario("missing-date"), mode="tool-boundary")
        result = result.model_copy(
            update={"backend": "openai", "model_name": "gpt-6-luna"}
        )
        policy = get_default_policy("fix")
        task_id, session_id = uuid4(), uuid4()
        inputs = make_inputs(result)

        class Tasks:
            async def get_spec(self, requested):
                return SimpleNamespace(
                    details=AgentTaskDetails(inputs=inputs, replay_id=uuid4())
                )

        class Sessions:
            async def create(self, request, **kwargs):
                return SimpleNamespace(id=session_id)

            async def get(self, requested):
                return SimpleNamespace(
                    status=stored_status,
                    outputs=result.model_dump(mode="json"),
                    metadata={"execution_started": True},
                )

            async def update(self, *args, **kwargs):
                raise AssertionError("Retry must not mutate a stored execution")

        async def forbidden(*args, **kwargs):
            raise AssertionError("Retry must not make another model call")

        monkeypatch.setattr(worker_run, "run_scenario", forbidden)
        monkeypatch.setenv("DELIVERY_POLICY_JSON", policy.model_dump_json())
        monkeypatch.setenv("DELIVERY_RUNNER_REVISION", get_runner_revision())
        results = []
        monkeypatch.setattr(worker_run, "write_task_result", results.append)
        client = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
        if stored_status == SessionStatus.COMPLETED:
            assert await worker_run.execute_task(client, task_id) == session_id
            assert results == [{"session_id": str(session_id)}]
        else:
            with pytest.raises(RuntimeError, match="safely repeated"):
                await worker_run.execute_task(client, task_id)
            assert results == []

    asyncio.run(exercise())


def test_recorded_model_nodes_expose_user_text_and_resolved_policy():
    result = asyncio.run(
        run_scenario(get_scenario("missing-date"), mode="tool-boundary")
    )
    nodes = worker_run.get_conversation_nodes(result)
    reply = next(node for node in nodes if node.outputs.get("text"))
    assert reply.input_text_selector == "/history/0/parts/0/content"
    assert reply.output_text_selector == "/text"
    assert reply.inputs["instructions"] == get_default_policy("fix").prompt
