"""Dummy agent recording sessions live or producing replay result sessions."""

import argparse
import asyncio
import hashlib
import os
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

from simulation import (
    DEFAULT_MODEL,
    FRAMEWORK,
    RealClock,
    SimulatedSession,
    SimulationConfig,
    ToolOutcome,
    ToolResolutionError,
    ToolResolver,
    build_session_inputs,
    canonical_json,
    compute_expected_outputs,
    passthrough_tool,
    run_tool,
    simulate_session,
)

from kitaru.api_models.v1.replay import ReplayResponse, ToolLookupRequest
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    PassthroughConfig,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import NodeStatus, SessionNodeBatchRequest
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task import get_task_id, get_task_inputs

ADAPTER_VERSION = "dummy-0.1"
NODE_BATCH_SIZE = 400


def _rate_hit(inputs: dict[str, Any], salt: str, rate: float) -> bool:
    """Decide a failure event deterministically from the inputs and a rate."""
    if rate <= 0:
        return False
    digest = hashlib.sha256(f"{canonical_json(inputs)}:{salt}".encode()).hexdigest()
    return int(digest[:8], 16) % 10_000 < rate * 10_000


def _case_matches(case: StaticCase, inputs: dict[str, Any]) -> bool:
    """Check whether a static case matches the tool inputs."""
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return inputs == case.match
    if not isinstance(case.match, dict):
        return False
    return all(
        name in inputs and inputs[name] == value for name, value in case.match.items()
    )


class PolicyToolResolver:
    """Tool resolver honoring a replay's tool policy."""

    def __init__(self, client: KitaruAPIClient, replay: ReplayResponse) -> None:
        """Initialize the resolver for one replay."""
        self._client = client
        self._replay = replay
        self._history_occurrences: dict[str, int] = {}

    async def __call__(self, tool_name: str, inputs: dict[str, Any]) -> ToolOutcome:
        """Resolve one tool call through its configured policy."""
        policy = self._replay.tool_policy.tools.get(
            tool_name, self._replay.tool_policy.default
        )
        if isinstance(policy, PassthroughConfig):
            return ToolOutcome(result=run_tool(tool_name, inputs))
        if isinstance(policy, StaticConfig):
            case = next((c for c in policy.cases if _case_matches(c, inputs)), None)
            if case is not None:
                return ToolOutcome(
                    result=case.result,
                    attributes={"mocked": True, "policy": policy.type},
                )
            return self._handle_miss(policy.type, policy.on_miss, tool_name, inputs)
        if isinstance(policy, HistoryConfig):
            cache_key = compute_tool_cache_key(tool_name, inputs)
            if cache_key is None:
                return self._handle_miss(policy.type, policy.on_miss, tool_name, inputs)
            occurrence = (
                self._history_occurrences.get(cache_key, 0)
                if policy.scope is HistoryScope.BASELINE
                else None
            )
            response = await self._client.replays.tool_lookup(
                self._replay.id,
                ToolLookupRequest(
                    tool_name=tool_name, cache_key=cache_key, occurrence=occurrence
                ),
            )
            match = response.match
            if match is not None:
                if occurrence is not None:
                    self._history_occurrences[cache_key] = occurrence + 1
                if match.status is NodeStatus.COMPLETED:
                    return ToolOutcome(
                        result=match.result,
                        attributes={"mocked": True, "policy": policy.type},
                    )
                if match.status is NodeStatus.FAILED:
                    raise ToolResolutionError(
                        match.error or f"Recorded tool call {tool_name!r} failed"
                    )
                raise ToolResolutionError(
                    f"History lookup for tool {tool_name!r} returned unexpected "
                    f"status {match.status.value!r}"
                )
            return self._handle_miss(policy.type, policy.on_miss, tool_name, inputs)
        raise ToolResolutionError(
            f"Tool policy {policy.type!r} is not supported by the dummy agent"
        )

    def _handle_miss(
        self,
        policy_type: str,
        on_miss: ToolPolicyOnMiss,
        tool_name: str,
        inputs: dict[str, Any],
    ) -> ToolOutcome:
        """Apply the policy's miss behavior."""
        if on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return ToolOutcome(result=run_tool(tool_name, inputs))
        message = f"No {policy_type} result for tool {tool_name!r}"
        if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            return ToolOutcome(
                result={"error": message},
                attributes={"mocked": True, "policy": policy_type},
                failed=True,
            )
        raise ToolResolutionError(message)


async def record_session(
    client: KitaruAPIClient,
    agent_id: uuid.UUID,
    inputs: dict[str, Any],
    origin: SessionOrigin,
    resolver: ToolResolver,
    model: str,
    model_params: dict[str, Any] | None,
    session_name: str | None,
    latency_scale: float,
    force_fail: bool = False,
    crash_before_finalize: bool = False,
) -> tuple[uuid.UUID, SimulatedSession]:
    """Simulate one session and record it through the API."""
    expected = await compute_expected_outputs(inputs)
    started_at = datetime.now(UTC)
    metadata: dict[str, Any] = {"generator": FRAMEWORK}
    if expected is not None:
        metadata["expected"] = expected
    session = await client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=origin,
            status=SessionStatus.IN_PROGRESS,
            name=session_name,
            inputs=inputs,
            outputs=None,
            started_at=started_at,
            metadata=metadata,
            framework=FRAMEWORK,
            adapter_version=ADAPTER_VERSION,
        )
    )
    simulated = await simulate_session(
        inputs,
        resolve_tool=resolver,
        clock=RealClock(latency_scale),
        model=model,
        requested_model=DEFAULT_MODEL,
        model_params=model_params,
        force_fail=force_fail,
    )
    for start in range(0, len(simulated.nodes), NODE_BATCH_SIZE):
        batch = simulated.nodes[start : start + NODE_BATCH_SIZE]
        await client.sessions.ingest_nodes(
            session.id, SessionNodeBatchRequest(nodes=batch)
        )
    if crash_before_finalize:
        # Exit before the final update, leaving the session in progress the
        # way a killed agent process would.
        print("Simulated crash before session finalization", file=sys.stderr)
        sys.exit(1)
    update_kwargs: dict[str, Any] = {
        "status": simulated.status,
        "outputs": simulated.outputs,
        "error": simulated.error,
        "ended_at": datetime.now(UTC),
    }
    # An explicit null name clears the worker-assigned name, so only send one
    # when there is nothing to preserve.
    if session_name is None:
        update_kwargs["name"] = simulated.name
    await client.sessions.update(session.id, SessionUpdateRequest(**update_kwargs))
    return session.id, simulated


async def run_task() -> None:
    """Run one agent task under the worker, live or as a replay."""
    inputs = get_task_inputs() or {}
    if get_task_id() is None:
        raise RuntimeError("No task id in the environment")
    agent_id = uuid.UUID(os.environ["DUMMY_AGENT_ID"])
    replay_id = os.environ.get("KITARU_REPLAY_ID")
    session_name = os.environ.get("KITARU_SESSION_NAME")
    latency_scale = float(os.environ.get("DUMMY_AGENT_LATENCY_SCALE", "0"))
    failure_rate = float(os.environ.get("DUMMY_AGENT_FAILURE_RATE", "0"))
    crash_rate = float(os.environ.get("DUMMY_AGENT_CRASH_RATE", "0"))
    crash_before_session_rate = float(
        os.environ.get("DUMMY_AGENT_CRASH_BEFORE_SESSION_RATE", "0")
    )
    sleep_seconds = float(os.environ.get("DUMMY_AGENT_SLEEP_SECONDS", "0"))

    if sleep_seconds > 0:
        await asyncio.sleep(sleep_seconds)
    if _rate_hit(inputs, "crash-before-session", crash_before_session_rate):
        print("Simulated crash before session creation", file=sys.stderr)
        sys.exit(1)

    async with KitaruAPIClient() as client:
        model = DEFAULT_MODEL
        model_params: dict[str, Any] | None = None
        resolver: ToolResolver = passthrough_tool
        origin = SessionOrigin.RECORDED
        if replay_id:
            replay = await client.replays.get(uuid.UUID(replay_id))
            override = replay.override
            if override is not None:
                if isinstance(override.model, str):
                    model = override.model
                elif isinstance(override.model, dict):
                    model = override.model.get(model, model)
                # A prompt override changes the question and thereby the tool
                # inputs, so history lookups against the baseline can miss.
                if override.prompt is not None:
                    inputs = {**inputs, "question": override.prompt}
                if override.model_params is not None:
                    model_params = dict(override.model_params)
            resolver = PolicyToolResolver(client, replay)
            origin = SessionOrigin.REPLAY

        session_id, simulated = await record_session(
            client,
            agent_id=agent_id,
            inputs=inputs,
            origin=origin,
            resolver=resolver,
            model=model,
            model_params=model_params,
            session_name=session_name,
            latency_scale=latency_scale,
            force_fail=_rate_hit(inputs, "agent-fail", failure_rate),
            crash_before_finalize=_rate_hit(inputs, "agent-crash", crash_rate),
        )
        if simulated.aborted:
            raise RuntimeError(simulated.error or "tool resolution failed")
        print(f"Recorded session {session_id} ({simulated.status})")


async def record_directly(args: argparse.Namespace) -> None:
    """Record sessions straight through the API without worker infrastructure."""
    agent_id = uuid.UUID(args.agent_id or os.environ["DUMMY_AGENT_ID"])
    config = SimulationConfig(
        seed=args.seed,
        min_turns=args.min_turns,
        max_turns=args.max_turns,
        failure_rate=args.failure_rate,
    )
    async with KitaruAPIClient() as client:
        for index in range(args.start_index, args.start_index + args.record):
            inputs = build_session_inputs(config, index)
            session_id, simulated = await record_session(
                client,
                agent_id=agent_id,
                inputs=inputs,
                origin=SessionOrigin.RECORDED,
                resolver=passthrough_tool,
                model=DEFAULT_MODEL,
                model_params=None,
                session_name=None,
                latency_scale=args.latency_scale,
            )
            print(
                f"Recorded session {session_id} name={simulated.name} "
                f"status={simulated.status} nodes={len(simulated.nodes)}"
            )


def main() -> int:
    """Run as a worker task, or record sessions directly with --record."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--record",
        type=int,
        default=None,
        help="Record this many sessions directly instead of running as a task.",
    )
    parser.add_argument("--agent-id", default=None, help="Target agent id.")
    parser.add_argument("--seed", default="kitaru-dev")
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--min-turns", type=int, default=1)
    parser.add_argument("--max-turns", type=int, default=3)
    parser.add_argument("--failure-rate", type=float, default=0.0)
    parser.add_argument(
        "--latency-scale",
        type=float,
        default=0.0,
        help="Sleep this fraction of every simulated node duration.",
    )
    args = parser.parse_args()

    if args.record is not None:
        asyncio.run(record_directly(args))
    else:
        asyncio.run(run_task())
    return 0


if __name__ == "__main__":
    sys.exit(main())
