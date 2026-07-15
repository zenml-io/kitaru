"""Regression tests for checkpoint retry history grouping."""

from zenml.client import Client as ZenMLClient

from kitaru import KitaruClient, checkpoint, flow
from kitaru._llm_usage import build_usage_record, log_usage_record_best_effort


def test_real_zenml_retry_maps_to_one_checkpoint_call(
    primed_zenml: None,
) -> None:
    calls = {"count": 0}

    @checkpoint(retries=1, runtime="inline", cache=False)
    def research() -> str:
        calls["count"] += 1
        log_usage_record_best_effort(
            build_usage_record(
                adapter="kitaru.llm",
                surface="direct_llm",
                record_id="research-call",
                total_tokens=calls["count"],
            )
        )
        if calls["count"] == 1:
            raise RuntimeError("fail once")
        return "ok"

    @flow(cache=False)
    def retry_once() -> None:
        research()

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
            "invocation_id": step.spec.invocation_id,
            "created": step.created,
            "start_time": step.start_time,
            "end_time": step.end_time,
        }
        for step in raw_attempts
    ]

    assert calls["count"] == 2
    assert len(raw_attempts) == 2, diagnostics
    assert [step.version for step in raw_attempts] == [1, 2], diagnostics
    assert len({step.name for step in raw_attempts}) == 1, diagnostics
    assert [step.spec.invocation_id for step in raw_attempts] == [
        step.name for step in raw_attempts
    ], diagnostics
    assert [step.original_step_run_id for step in raw_attempts] == [None, None], (
        diagnostics
    )
    assert [step.status.value for step in raw_attempts] == [
        "retried",
        "completed",
    ], diagnostics
    assert all(
        step.created is not None
        and step.start_time is not None
        and step.end_time is not None
        for step in raw_attempts
    ), diagnostics

    execution = KitaruClient().executions.get(handle.exec_id)

    assert len(execution.checkpoints) == 1
    checkpoint_call = execution.checkpoints[0]
    assert checkpoint_call.status.value == "completed"
    assert [attempt.attempt_id for attempt in checkpoint_call.attempts] == [
        str(step.id) for step in raw_attempts
    ]
    assert [attempt.status.value for attempt in checkpoint_call.attempts] == [
        "failed",
        "completed",
    ]
    assert [
        attempt.llm_usage_records[0]["checkpoint_id"]
        for attempt in checkpoint_call.attempts
    ] == [str(step.id) for step in raw_attempts]
    assert [
        attempt.llm_usage_records[0]["checkpoint_name"]
        for attempt in checkpoint_call.attempts
    ] == [checkpoint_call.name, checkpoint_call.name]
    assert [
        record["checkpoint_id"] for record in checkpoint_call.llm_usage_records
    ] == [str(step.id) for step in raw_attempts]
    assert checkpoint_call.failure is None
