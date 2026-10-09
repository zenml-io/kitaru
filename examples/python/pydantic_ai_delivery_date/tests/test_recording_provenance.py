"""Creation-time provenance and reuse of authenticated recording clients."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

from kitaru.api_models.v1.session import SessionStatus

from delivery_date.models import get_scenario
from delivery_date.persistence import get_runner_revision, record_result
from delivery_date.runner import run_scenario


def test_recording_uses_supplied_client_and_sets_provenance_before_finalizing(
    monkeypatch,
):
    import delivery_date.persistence as persistence

    def forbidden_connection(*args, **kwargs):
        raise AssertionError("Recording must reuse the authenticated client")

    monkeypatch.setattr(persistence, "KitaruAPIClient", forbidden_connection)
    requests = []
    session_id = uuid4()
    agent_id = uuid4()
    source_session_id = uuid4()

    async def list_agents(params):
        return SimpleNamespace(items=[SimpleNamespace(id=agent_id)])

    async def create_session(request, idempotency_key):
        requests.append(("create", request))
        return SimpleNamespace(id=session_id)

    async def get_session(sid):
        assert sid == session_id
        return SimpleNamespace(status=SessionStatus.IN_PROGRESS)

    async def ingest_nodes(sid, request):
        requests.append(("nodes", request))

    async def update_session(sid, request):
        requests.append(("finalize", request))

    async def iter_evaluations(params):
        for row in []:
            yield row

    async def create_evaluations(sid, request):
        requests.append(("evaluations", request))

    client = SimpleNamespace(
        agents=SimpleNamespace(list=list_agents),
        sessions=SimpleNamespace(
            create=create_session,
            get=get_session,
            ingest_nodes=ingest_nodes,
            update=update_session,
            create_evaluations=create_evaluations,
        ),
        evaluations=SimpleNamespace(iter=iter_evaluations),
    )

    async def check():
        result = await run_scenario(get_scenario("missing-date"), mode="tool-boundary")
        stored_id = await record_result(
            result,
            server_url=None,
            client=client,
            editor_snapshot={"goal": "Keep a visible reviewed snapshot"},
            source_session_id=source_session_id,
            title="Saved variation",
            continue_after_boundary=True,
        )
        assert stored_id == session_id
        assert [kind for kind, _ in requests] == [
            "create",
            "nodes",
            "finalize",
            "evaluations",
        ]
        creation = requests[0][1]
        assert creation.name == "Saved variation"
        assert creation.agent_id == agent_id
        assert creation.inputs["editor_snapshot"] == {
            "goal": "Keep a visible reviewed snapshot"
        }
        assert creation.inputs["source_session_id"] == str(source_session_id)
        assert creation.metadata["source_session_id"] == str(source_session_id)
        assert creation.inputs["continue_after_boundary"] is True
        assert creation.inputs["runner_revision"] == get_runner_revision()
        assert creation.inputs["scenario_sha256"] == result.scenario_hash
        assert creation.inputs["policy_name"] == result.policy_name
        assert creation.inputs["policy_prompt"] == result.policy_prompt
        assert creation.inputs["policy_hash"] == result.policy_hash
        responses = [
            node
            for node in requests[1][1].nodes
            if node.external_id.startswith("response-")
        ]
        assert responses
        assert all(
            node.inputs["instructions"] == result.policy_prompt for node in responses
        )
        assert requests[2][1].status == SessionStatus.COMPLETED
        assert all(row.passed for row in requests[3][1].evaluations)

    asyncio.run(check())
