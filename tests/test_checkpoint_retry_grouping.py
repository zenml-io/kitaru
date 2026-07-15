"""Regression tests for checkpoint retry history grouping."""

from zenml.client import Client as ZenMLClient

from kitaru import KitaruClient, checkpoint, flow


def test_real_zenml_retry_maps_to_one_checkpoint_call(
    primed_zenml: None,
) -> None:
    calls = {"count": 0}

    @checkpoint(retries=1, runtime="inline", cache=False)
    def research() -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("fail once")
        return "ok"

    @checkpoint(runtime="inline", cache=False)
    def publish(result: str) -> str:
        return f"published: {result}"

    @flow(cache=False)
    def retry_once() -> None:
        publish(research())

    handle = retry_once.run()
    handle.wait()

    raw_attempts = list(
        ZenMLClient()
        .list_run_steps(
            pipeline_run_id=handle.exec_id,
            exclude_retried=False,
            hydrate=True,
            sort_by="asc:created",
            size=20,
        )
        .items
    )
    diagnostics = [
        {
            "id": str(step.id),
            "name": step.name,
            "version": step.version,
            "status": step.status.value,
            "original_step_run_id": (
                str(step.original_step_run_id)
                if step.original_step_run_id is not None
                else None
            ),
            "parent_step_ids": [str(parent_id) for parent_id in step.parent_step_ids],
            "invocation_id": step.spec.invocation_id,
            "created": step.created,
            "start_time": step.start_time,
            "end_time": step.end_time,
        }
        for step in raw_attempts
    ]
    research_attempts = [step for step in raw_attempts if step.name == "research"]
    publish_attempts = [step for step in raw_attempts if step.name == "publish"]

    assert calls["count"] == 2
    assert len(raw_attempts) == 3, diagnostics
    assert len(research_attempts) == 2, diagnostics
    assert len(publish_attempts) == 1, diagnostics
    assert [step.version for step in research_attempts] == [1, 2], diagnostics
    assert [step.spec.invocation_id for step in raw_attempts] == [
        step.name for step in raw_attempts
    ], diagnostics
    assert [step.original_step_run_id for step in raw_attempts] == [
        None,
        None,
        None,
    ], diagnostics
    assert [step.status.value for step in research_attempts] == [
        "retried",
        "completed",
    ], diagnostics
    assert publish_attempts[0].status.value == "completed", diagnostics
    assert all(
        step.created is not None
        and step.start_time is not None
        and step.end_time is not None
        for step in raw_attempts
    ), diagnostics

    hidden_retried_id = str(research_attempts[0].id)
    visible_successful_id = str(research_attempts[1].id)
    raw_publish_parent_ids = [
        str(parent_id) for parent_id in publish_attempts[0].parent_step_ids
    ]
    assert raw_publish_parent_ids == [hidden_retried_id], diagnostics
    assert raw_publish_parent_ids != [visible_successful_id], diagnostics

    execution = KitaruClient().executions.get(handle.exec_id)

    assert len(execution.checkpoints) == 2
    checkpoints_by_name = {
        checkpoint_call.name: checkpoint_call
        for checkpoint_call in execution.checkpoints
    }
    research_call = checkpoints_by_name["research"]
    publish_call = checkpoints_by_name["publish"]
    assert research_call.call_id == visible_successful_id
    assert research_call.status.value == "completed"
    assert [attempt.attempt_id for attempt in research_call.attempts] == [
        str(step.id) for step in research_attempts
    ]
    assert [attempt.status.value for attempt in research_call.attempts] == [
        "failed",
        "completed",
    ]
    assert research_call.failure is None
    assert publish_call.parent_call_ids == [research_call.call_id]
