"""Native policy experiments and pinned regression checks for the delivery demo."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from kitaru.api_models.v1.agent_version import (
    AgentVersionCreateRequest,
    RunSpec,
    RuntimeCapabilities,
)
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.plugin import EvaluatorConfig, ScriptPluginSource
from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayListParams
from kitaru.api_models.v1.replay_config import PassthroughConfig, ToolPolicy
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from pydantic import BaseModel, ConfigDict, Field

from .evaluator import CHECK_NAMES, get_evaluator_source
from .persistence import get_runner_revision
from .policy import Policy
from .scenario_library import read_test_set

WORKER_COMMAND = "uv run --frozen python -m delivery_date.worker_run"
HASH_PATTERN = r"^[a-f0-9]{64}$"


class ExperimentRequest(BaseModel):
    """Reviewed policies to run on one frozen cohort of up to five cases."""

    model_config = ConfigDict(extra="forbid")
    cohort_version_id: UUID
    baseline: Policy
    candidate: Policy
    model: str = "gpt-6-luna"
    backend: Literal["openai"] = "openai"
    request_id: UUID = Field(default_factory=uuid4)


class ExperimentPins(BaseModel):
    """Identifiers and configuration hashes needed to reproduce a comparison."""

    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["delivery-policy-experiment.v1"] = (
        "delivery-policy-experiment.v1"
    )
    experiment_id: UUID
    agent_id: UUID
    cohort_version_id: UUID
    baseline_run_id: UUID
    candidate_run_id: UUID
    baseline_agent_version_id: UUID
    candidate_agent_version_id: UUID
    evaluator_id: UUID
    evaluator_version_id: UUID
    evaluator_name: str
    evaluator_version: int = Field(ge=1)
    evaluator_hash: str = Field(pattern=HASH_PATTERN)
    runner_revision: str = Field(pattern=HASH_PATTERN)
    baseline_policy_hash: str = Field(pattern=HASH_PATTERN)
    candidate_policy_hash: str = Field(pattern=HASH_PATTERN)
    case_hashes: dict[str, str] = Field(min_length=1, max_length=5)
    model: str
    backend: Literal["openai"] = "openai"


class ExperimentCase(BaseModel):
    """Replay completion and independently executed evaluator results."""

    source_session_id: UUID
    result_session_id: UUID | None = None
    status: Literal["running", "completed", "failed"] = "running"
    passed: bool = False
    checks: dict[str, bool] = Field(default_factory=dict)
    error: str | None = None


class ExperimentArm(BaseModel):
    """One policy version tested against every member of the pinned cohort."""

    status: Literal["running", "completed", "failed"] = "running"
    passed: bool = False
    cases: list[ExperimentCase] = Field(default_factory=list)


class ExperimentReceipt(ExperimentPins):
    """Verified native experiment state for the app and Codex handoff."""

    status: Literal["running", "completed", "failed"] = "running"
    baseline: ExperimentArm = Field(default_factory=ExperimentArm)
    candidate: ExperimentArm = Field(default_factory=ExperimentArm)
    ready_for_handoff: bool = False


def serialize_pins(receipt: ExperimentPins) -> dict[str, object]:
    """Return only replay identifiers and hashes, excluding displayed results."""
    values = receipt.model_dump(mode="json", include=set(ExperimentPins.model_fields))
    return ExperimentPins.model_validate(values).model_dump(mode="json")


def get_policy_run_spec(policy: Policy, model: str, revision: str) -> RunSpec:
    """Build a portable worker command without copying process credentials."""
    return RunSpec(
        command=WORKER_COMMAND,
        env={
            "DELIVERY_POLICY_JSON": policy.model_dump_json(),
            "DELIVERY_MODEL": model,
            "DELIVERY_RUNNER_REVISION": revision,
        },
        timeout_seconds=300,
        runtime_capabilities=RuntimeCapabilities(overrides=False, tool_policies=False),
    )


async def start_experiment(
    client: KitaruAPIClient, request: ExperimentRequest
) -> ExperimentReceipt:
    """Register reviewed policy versions and start two native experiment runs."""
    saved = await read_test_set(client, request.cohort_version_id)
    cases = saved["cases"]
    revision = get_runner_revision()
    if not 1 <= len(cases) <= 5:
        raise ValueError("Policy comparisons require one to five saved cases")
    if saved["runnerRevision"] != revision or any(
        case["runnerRevision"] != revision for case in cases
    ):
        raise ValueError(
            "Save the test set with the current runner before comparing policies"
        )
    if request.baseline.calculate_hash() == request.candidate.calculate_hash():
        raise ValueError("The candidate policy must differ from the baseline")
    agent_id = UUID(saved["agentId"])
    versions = []
    for role, policy in (
        ("baseline", request.baseline),
        ("candidate", request.candidate),
    ):
        run_spec = get_policy_run_spec(policy, request.model, revision)
        version_hash = hashlib.sha256(run_spec.model_dump_json().encode()).hexdigest()
        version = await client.agents.create_version(
            agent_id,
            AgentVersionCreateRequest(
                display_version=f"{role}-{policy.calculate_hash()[:8]}",
                description=f"Reviewed delivery policy: {policy.name}",
                run_spec=run_spec,
            ),
            idempotency_key=f"delivery-policy-{agent_id}-{version_hash}",
        )
        versions.append(version)
    source = get_evaluator_source()
    evaluator_hash = hashlib.sha256(source).hexdigest()
    evaluator_name = f"delivery-date-checks-{str(agent_id)[:8]}-{evaluator_hash[:12]}"
    existing = await client.evaluators.list(
        EvaluatorListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value=evaluator_name)
        )
    )
    if existing.items:
        evaluator = existing.items[0]
    else:
        try:
            evaluator = await client.evaluators.create(
                EvaluatorCreateRequest(
                    name=evaluator_name,
                    agent_id=agent_id,
                    description="Frozen shipping evidence and new assistant-message date checks.",
                )
            )
        except APIError as exc:
            if exc.status_code != 409:
                raise
            evaluator = (
                await client.evaluators.list(
                    EvaluatorListParams(
                        filter=FilterCondition(
                            field="name", op=FilterOp.EQ, value=evaluator_name
                        )
                    )
                )
            ).items[0]
    if evaluator.agent_id != agent_id:
        raise ValueError("Evaluator is scoped to another agent")
    blob = await client.blobs.upload(
        source, media_type="text/x-python", filename="delivery_checks.py"
    )
    evaluator_version = await client.evaluators.create_version(
        evaluator.id,
        EvaluatorVersionCreateRequest(
            source=ScriptPluginSource(blob_id=blob.id, entrypoint="evaluate_delivery"),
            display_version=evaluator_hash[:12],
        ),
        idempotency_key=f"delivery-evaluator-{evaluator.id}-{evaluator_hash}",
    )
    experiment = await client.experiments.create(
        ExperimentCreateRequest(
            name=f"delivery-policy-{str(request.request_id)[:12]}",
            agent_id=agent_id,
            description="Compare reviewed policies on a frozen regression test set.",
            tool_policy=ToolPolicy(default=PassthroughConfig()),
            evaluators=[
                EvaluatorConfig(
                    evaluator=evaluator.name, version=evaluator_version.version
                )
            ],
        ),
        idempotency_key=f"delivery-experiment-{request.request_id}",
    )
    runs = []
    for version in versions:
        runs.append(
            await client.experiments.start_run(
                experiment.id,
                ExperimentRunCreateRequest(
                    cohort_version_id=request.cohort_version_id,
                    agent_version_id=version.id,
                    baseline_evaluation_mode=BaselineEvaluationMode.NONE,
                ),
                idempotency_key=f"delivery-experiment-run-{request.request_id}-{version.id}",
            )
        )
    return ExperimentReceipt(
        experiment_id=experiment.id,
        agent_id=agent_id,
        cohort_version_id=request.cohort_version_id,
        baseline_run_id=runs[0].id,
        candidate_run_id=runs[1].id,
        baseline_agent_version_id=versions[0].id,
        candidate_agent_version_id=versions[1].id,
        evaluator_id=evaluator.id,
        evaluator_version_id=evaluator_version.id,
        evaluator_name=evaluator.name,
        evaluator_version=evaluator_version.version,
        evaluator_hash=evaluator_hash,
        runner_revision=revision,
        baseline_policy_hash=request.baseline.calculate_hash(),
        candidate_policy_hash=request.candidate.calculate_hash(),
        case_hashes={case["id"]: case["scenarioHash"] for case in cases},
        model=request.model,
    )


async def validate_pins(client: KitaruAPIClient, pins: ExperimentPins) -> None:
    """Check recorded membership, code, agent versions, and evaluator source pins."""
    if pins.runner_revision != get_runner_revision():
        raise ValueError("Pinned experiment uses a different local runner revision")
    saved = await read_test_set(client, pins.cohort_version_id)
    if (
        UUID(saved["agentId"]) != pins.agent_id
        or {case["id"]: case["scenarioHash"] for case in saved["cases"]}
        != pins.case_hashes
    ):
        raise ValueError("Pinned cohort membership or scenario hashes changed")
    if saved["runnerRevision"] != pins.runner_revision:
        raise ValueError("Pinned cohort uses a different runner revision")
    for version_id, policy_hash in (
        (pins.baseline_agent_version_id, pins.baseline_policy_hash),
        (pins.candidate_agent_version_id, pins.candidate_policy_hash),
    ):
        version = await client.agent_versions.get(version_id)
        if version.agent_id != pins.agent_id or version.run_spec is None:
            raise ValueError(
                "Pinned agent version belongs to another agent or has no run spec"
            )
        policy = Policy.model_validate_json(
            version.run_spec.env.get("DELIVERY_POLICY_JSON", "{}")
        )
        if (
            policy.calculate_hash() != policy_hash
            or version.run_spec
            != get_policy_run_spec(policy, pins.model, pins.runner_revision)
        ):
            raise ValueError("Pinned policy or its runtime configuration changed")
    evaluator = await client.evaluators.get(pins.evaluator_id)
    version = await client.evaluators.get_version(
        pins.evaluator_id, pins.evaluator_version
    )
    if (
        evaluator.name != pins.evaluator_name
        or evaluator.agent_id != pins.agent_id
        or version.id != pins.evaluator_version_id
        or not isinstance(version.source, ScriptPluginSource)
    ):
        raise ValueError("Pinned evaluator version changed")
    blob = await client.blobs.get(version.source.blob_id)
    if (
        version.source.entrypoint != "evaluate_delivery"
        or blob.sha256 != pins.evaluator_hash
        or hashlib.sha256(get_evaluator_source()).hexdigest() != pins.evaluator_hash
    ):
        raise ValueError("Pinned evaluator script differs from the validated source")
    experiment = await client.experiments.get(pins.experiment_id)
    expected = [
        EvaluatorConfig(evaluator=pins.evaluator_name, version=pins.evaluator_version)
    ]
    if (
        experiment.agent_id != pins.agent_id
        or experiment.override is not None
        or experiment.tool_policy != ToolPolicy(default=PassthroughConfig())
        or experiment.evaluators != expected
    ):
        raise ValueError("Pinned experiment configuration changed")


async def _read_arm(
    client: KitaruAPIClient,
    pins: ExperimentPins,
    run_id: UUID,
    version_id: UUID,
    policy_hash: str,
) -> ExperimentArm:
    """Reject missing, incomplete, or unrelated replay and evaluator evidence."""
    run = await client.experiment_runs.get(run_id)
    if (
        run.experiment_id != pins.experiment_id
        or run.cohort_version_id != pins.cohort_version_id
        or run.agent_version_id != version_id
        or run.baseline_evaluation_mode != BaselineEvaluationMode.NONE
    ):
        raise ValueError("Experiment run does not match its pinned configuration")
    cases = {
        key: ExperimentCase(source_session_id=UUID(key)) for key in pins.case_hashes
    }
    seen = set()
    async for replay in client.replays.iter(
        ReplayListParams(
            filter=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=str(run.id)
            )
        )
    ):
        key = str(replay.baseline_session_id)
        if key not in cases or key in seen:
            raise ValueError("Experiment replay membership is duplicated or unrelated")
        seen.add(key)
        case = cases[key]
        case.result_session_id = replay.result_session_id
        case.error = replay.error
        if replay.status in {"pending", "evaluating"}:
            continue
        case.status = "failed"
        if replay.status != "completed" or replay.result_session_id is None:
            case.error = replay.error or "Replay did not complete"
            continue
        if (
            replay.override is not None
            or replay.tool_policy != ToolPolicy(default=PassthroughConfig())
            or replay.evaluators
            != [
                EvaluatorConfig(
                    evaluator=pins.evaluator_name, version=pins.evaluator_version
                )
            ]
        ):
            raise ValueError(
                "Replay evaluator or tool configuration differs from the experiment"
            )
        session = await client.sessions.get(replay.result_session_id)
        outputs = session.outputs
        if (
            session.status != SessionStatus.COMPLETED
            or session.agent_version_id != version_id
            or session.task_id is None
            or not isinstance(outputs, dict)
            or outputs.get("status") not in {"completed", "boundary-completed"}
            or outputs.get("policy_hash") != policy_hash
            or outputs.get("model_name") != pins.model
            or outputs.get("scenario_hash") != pins.case_hashes[key]
        ):
            case.error = (
                "Replay recording is incomplete or differs from its pinned policy"
            )
            continue
        rows = []
        async for row in client.evaluations.iter(
            EvaluationListParams(
                filter=FilterCondition(
                    field="session_id", op=FilterOp.EQ, value=str(session.id)
                )
            )
        ):
            if row.evaluator_version_id == pins.evaluator_version_id:
                rows.append(row)
        if (
            len(rows) != len(CHECK_NAMES)
            or {row.name for row in rows} != CHECK_NAMES
            or any(row.task_id is None or type(row.passed) is not bool for row in rows)
        ):
            case.error = "Missing independent evaluator results"
            continue
        task_ids = {row.task_id for row in rows}
        if len(task_ids) != 1:
            case.error = "Evaluator results came from different tasks"
            continue
        task_id = next(iter(task_ids))
        assert task_id is not None
        task = await client.tasks.get(task_id)
        if (
            task.status != TaskStatus.COMPLETED
            or task.input_session_id != session.id
            or task.plugin_version_id != pins.evaluator_version_id
            or task.job_id != replay.job_id
        ):
            case.error = "Evaluator task provenance does not match the replay"
            continue
        case.checks = {row.name: row.passed is True for row in rows}
        case.status = "completed"
        case.passed = all(case.checks.values())
    terminal = run.status in {"completed", "failed", "canceled"}
    if terminal:
        for case in cases.values():
            if case.status == "running":
                case.status = "failed"
                case.error = "Run settled without completed replay evidence"
    status = (
        "running"
        if not terminal
        else "completed"
        if run.status == "completed"
        and len(seen) == len(cases)
        and all(case.status == "completed" for case in cases.values())
        else "failed"
    )
    return ExperimentArm(
        status=status,
        passed=status == "completed" and all(case.passed for case in cases.values()),
        cases=list(cases.values()),
    )


async def read_experiment(
    client: KitaruAPIClient, pins: ExperimentPins
) -> ExperimentReceipt:
    """Read both native runs and independently verify their stored evaluation evidence."""
    await validate_pins(client, pins)
    baseline = await _read_arm(
        client,
        pins,
        pins.baseline_run_id,
        pins.baseline_agent_version_id,
        pins.baseline_policy_hash,
    )
    candidate = await _read_arm(
        client,
        pins,
        pins.candidate_run_id,
        pins.candidate_agent_version_id,
        pins.candidate_policy_hash,
    )
    status = (
        "running"
        if "running" in {baseline.status, candidate.status}
        else "failed"
        if "failed" in {baseline.status, candidate.status}
        else "completed"
    )
    return ExperimentReceipt.model_validate(
        serialize_pins(pins)
        | {
            "status": status,
            "baseline": baseline,
            "candidate": candidate,
            "ready_for_handoff": status == "completed" and candidate.passed,
        }
    )


async def run_pinned_candidate(
    client: KitaruAPIClient, pins: ExperimentPins, timeout: float = 900
) -> ExperimentReceipt:
    """Start a fresh candidate run and wait for its independently evaluated outcome."""
    await validate_pins(client, pins)
    run = await client.experiments.start_run(
        pins.experiment_id,
        ExperimentRunCreateRequest(
            cohort_version_id=pins.cohort_version_id,
            agent_version_id=pins.candidate_agent_version_id,
            baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        ),
    )
    current = ExperimentPins.model_validate(
        serialize_pins(pins) | {"candidate_run_id": str(run.id)}
    )
    async with asyncio.timeout(timeout):
        while True:
            receipt = await read_experiment(client, current)
            if receipt.status != "running":
                return receipt
            await asyncio.sleep(2)


async def main() -> int:
    """Verify a reviewed receipt or rerun its exact candidate as a CI regression check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--server-url")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args()
    pins = ExperimentPins.model_validate_json(args.receipt.read_text())
    async with KitaruAPIClient(base_url=args.server_url) as client:
        receipt = (
            await read_experiment(client, pins)
            if args.verify_only
            else await run_pinned_candidate(client, pins, args.timeout)
        )
    print(receipt.model_dump_json(indent=2))
    return int(not receipt.ready_for_handoff)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except (APIError, ValueError, RuntimeError, OSError, TimeoutError) as exc:
        print(json.dumps({"status": "failed", "passed": False, "error": str(exc)}))
        raise SystemExit(1) from exc
