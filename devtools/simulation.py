"""Deterministic dummy agent simulation shared by trace generation and recording."""

import asyncio
import hashlib
import json
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
)

DEFAULT_MODEL = "dummy-llm-v1"
MODEL_PROVIDER = "kitaru-dummy"
FRAMEWORK = "kitaru-dummy"
SYSTEM_PROMPT = "You are a research assistant. Answer using the available tools."
SUBAGENT_ID = "dummy-researcher"

BASE_STARTED_AT = datetime(2026, 8, 1, tzinfo=UTC)

TOOLS = ("search_web", "fetch_record", "calculate")
TOPICS = (
    "solar panel efficiency",
    "container shipping rates",
    "wheat futures",
    "urban air quality",
    "battery recycling",
    "orbital debris",
    "deep sea mining",
    "vaccine cold chains",
    "wildfire prediction",
    "semiconductor yields",
    "rail freight delays",
    "coral reef restoration",
)

_COST_PER_TOKEN = Decimal("0.000004")


class ToolResolutionError(RuntimeError):
    """Raised when a tool call cannot be resolved."""


@dataclass(frozen=True)
class SimulationConfig:
    """Dummy session population shape."""

    seed: str = "kitaru-dev"
    min_turns: int = 1
    max_turns: int = 3
    failure_rate: float = 0.0
    big_payload_every: int = 0
    payload_bytes: int = 4096


@dataclass(frozen=True)
class LLMResult:
    """Dummy LLM call result."""

    text: str
    reasoning: str
    tokens: TokenUsage
    cost: Decimal


@dataclass(frozen=True)
class ToolOutcome:
    """Resolved dummy tool call."""

    result: Any
    attributes: dict[str, Any] = field(default_factory=dict)
    failed: bool = False


ToolResolver = Callable[[str, dict[str, Any]], Awaitable[ToolOutcome]]


@dataclass
class SimulatedSession:
    """Simulated session transcript."""

    name: str
    status: SessionStatus
    inputs: dict[str, Any]
    outputs: dict[str, Any] | None
    error: str | None
    started_at: datetime
    ended_at: datetime
    metadata: dict[str, Any]
    nodes: list[SessionNodeCreateRequest]
    aborted: bool = False


class Clock(Protocol):
    """Clock driving node timestamps."""

    def now(self) -> datetime:
        """Return the current time."""
        ...

    async def advance(self, milliseconds: int) -> None:
        """Advance time by a node duration."""
        ...


class FixedClock:
    """Clock advancing instantly from a fixed start time."""

    def __init__(self, start: datetime = BASE_STARTED_AT) -> None:
        """Initialize the clock at a start time."""
        self._now = start

    def now(self) -> datetime:
        """Return the current simulated time."""
        return self._now

    async def advance(self, milliseconds: int) -> None:
        """Advance the simulated time."""
        self._now += timedelta(milliseconds=milliseconds)


class RealClock:
    """Clock reading wall time, optionally sleeping scaled node durations."""

    def __init__(self, latency_scale: float = 0.0) -> None:
        """Initialize the clock with a sleep scale factor."""
        self._latency_scale = latency_scale

    def now(self) -> datetime:
        """Return the current wall time."""
        return datetime.now(UTC)

    async def advance(self, milliseconds: int) -> None:
        """Sleep for the scaled node duration."""
        if self._latency_scale > 0:
            await asyncio.sleep(milliseconds * self._latency_scale / 1000)


def canonical_json(value: Any) -> str:
    """Serialize a value as canonical JSON."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    """Hash a value into a stable hex digest."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _digest_int(value: Any, modulo: int) -> int:
    """Reduce a value's digest to an integer below modulo."""
    return int(_digest(value)[:12], 16) % modulo


def _duration_ms(trace_id: str, node_index: int) -> int:
    """Derive a deterministic node duration in milliseconds."""
    return 40 + _digest_int([trace_id, node_index, "ms"], 860)


def build_session_inputs(config: SimulationConfig, index: int) -> dict[str, Any]:
    """Build deterministic session inputs for one generated session."""
    rng = random.Random(f"{config.seed}:{index}")
    topic = rng.choice(TOPICS)
    inputs: dict[str, Any] = {
        "question": f"What is the current status of {topic}?",
        "topic": topic,
        "turns": rng.randint(config.min_turns, config.max_turns),
        "variant": rng.randint(0, 999_999),
        "fail": rng.random() < config.failure_rate,
    }
    if config.big_payload_every and index % config.big_payload_every == 0:
        inputs["context"] = "x" * config.payload_bytes
    return inputs


def build_tool_inputs(
    tool_name: str, topic: str, turn: int, variant: int
) -> dict[str, Any]:
    """Build deterministic tool inputs for one planned tool call."""
    if tool_name == "search_web":
        return {"query": f"{topic} developments {turn + 1}"}
    if tool_name == "fetch_record":
        slug = topic.replace(" ", "-")
        return {"record_id": f"{slug}-{variant % 100:02d}-{turn + 1}"}
    return {"expression": f"({variant} + {turn + 1}) * 0.85"}


def run_tool(tool_name: str, inputs: dict[str, Any]) -> dict[str, Any]:
    """Execute a dummy tool deterministically from its inputs."""
    key = _digest([tool_name, inputs])
    if tool_name == "search_web":
        query = inputs.get("query", "")
        return {
            "results": [
                {
                    "title": f"Report {key[i * 2 : i * 2 + 6]} on {query}",
                    "url": f"https://example.com/{key[i * 8 : i * 8 + 8]}",
                    "snippet": f"Finding {i + 1}: {query} shifted {int(key[i], 16)}%.",
                }
                for i in range(3)
            ]
        }
    if tool_name == "fetch_record":
        return {
            "record": {
                "id": inputs.get("record_id"),
                "status": ("active", "archived", "pending")[_digest_int(key, 3)],
                "value": _digest_int([key, "value"], 10_000) / 100,
            }
        }
    if tool_name == "calculate":
        return {"result": _digest_int([key, "result"], 1_000_000) / 1000}
    return {"echo": inputs}


async def passthrough_tool(tool_name: str, inputs: dict[str, Any]) -> ToolOutcome:
    """Resolve a tool call by executing the dummy tool directly."""
    return ToolOutcome(result=run_tool(tool_name, inputs))


def run_llm(model: str, messages: list[dict[str, str]]) -> LLMResult:
    """Produce a deterministic dummy completion for a message list."""
    key = _digest([model, messages])
    prompt = messages[-1]["content"]
    text = (
        f"[{model}] {prompt.split('.')[0].strip()}: assessment {key[:8]} "
        f"with confidence {_digest_int([key, 'confidence'], 100)}%."
    )
    reasoning = f"Weighing the evidence behind {key[8:16]} before answering."
    input_tokens = sum(len(m["content"].split()) for m in messages) + 3 * len(messages)
    output_tokens = len(text.split()) + 2
    tokens = TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=_digest_int([key, "cached"], input_tokens + 1),
        reasoning_tokens=len(reasoning.split()),
    )
    cost = (Decimal(input_tokens + output_tokens) * _COST_PER_TOKEN).quantize(
        Decimal("0.000001")
    )
    return LLMResult(text=text, reasoning=reasoning, tokens=tokens, cost=cost)


def _llm_node(
    index: int,
    parent_index: int | None,
    name: str,
    messages: list[dict[str, str]],
    llm: LLMResult,
    started_at: datetime,
    ended_at: datetime,
    trace_id: str,
    requested_model: str,
    model: str,
    model_params: dict[str, Any],
    turn: int,
    output_text: str | None = None,
    secondary_parent_indexes: list[int] | None = None,
    error: str | None = None,
) -> SessionNodeCreateRequest:
    """Build one dummy LLM call node."""
    failed = error is not None
    return SessionNodeCreateRequest(
        index=index,
        parent_index=parent_index,
        secondary_parent_indexes=secondary_parent_indexes or [],
        external_id=f"{trace_id}-node-{index}",
        trace_id=trace_id,
        node_type=NodeType.LLM_CALL,
        name=name,
        status=NodeStatus.FAILED if failed else NodeStatus.COMPLETED,
        error=error,
        started_at=started_at,
        ended_at=ended_at,
        input_text_selector="/1/content",
        output_text_selector=None if failed else "/0/content",
        system_prompt_selector="/0/content",
        reasoning=None if failed else llm.reasoning,
        inputs=messages,
        outputs=(
            None
            if failed
            else [{"role": "assistant", "content": output_text or llm.text}]
        ),
        requested_model=requested_model,
        model=model,
        model_provider=MODEL_PROVIDER,
        tokens=llm.tokens,
        cost=llm.cost,
        model_params=model_params,
        attributes={"turn": turn + 1},
        metadata={"generator": FRAMEWORK},
    )


async def simulate_session(
    inputs: dict[str, Any],
    resolve_tool: ToolResolver | None = None,
    clock: Clock | None = None,
    model: str = DEFAULT_MODEL,
    requested_model: str = DEFAULT_MODEL,
    model_params: dict[str, Any] | None = None,
    force_fail: bool = False,
) -> SimulatedSession:
    """Simulate one agent session, resolving tool calls through the resolver."""
    resolve = resolve_tool or passthrough_tool
    clock = clock or RealClock()
    rng = random.Random(canonical_json(inputs))
    question = str(inputs.get("question", "What is the current status of everything?"))
    topic = str(inputs.get("topic", "everything"))
    variant = int(inputs.get("variant", 0))
    turn_count = max(1, int(inputs.get("turns", 1)))
    # force_fail stays outside the inputs so the rng draws and tool cache keys
    # match the non-failing baseline.
    fail = force_fail or bool(inputs.get("fail", False))
    params = model_params or {"temperature": 0.2, "max_output_tokens": 1024}
    trace_id = _digest([inputs, "trace"])[:32]
    slug = topic.replace(" ", "-")

    # Draw the structure upfront, so replay-time tool substitution cannot shift
    # later random draws away from the generated baseline.
    turn_tools = [rng.sample(TOOLS, k=rng.randint(1, 2)) for _ in range(turn_count)]

    started_at = clock.now()
    nodes: list[SessionNodeCreateRequest] = []
    node_counter = 0
    sources: list[str] = []
    answer: str | None = None
    session_error: str | None = None
    aborted = False
    previous_respond_index: int | None = None

    def allocate() -> int:
        """Allocate the next node index."""
        nonlocal node_counter
        node_counter += 1
        return node_counter - 1

    def tool_node(
        index: int,
        parent_index: int,
        tool_name: str,
        tool_inputs: dict[str, Any],
        outcome: ToolOutcome | None,
        node_started: datetime,
        error: str | None = None,
    ) -> SessionNodeCreateRequest:
        """Build one dummy tool call node."""
        failed = error is not None or (outcome is not None and outcome.failed)
        return SessionNodeCreateRequest(
            index=index,
            parent_index=parent_index,
            external_id=f"{trace_id}-node-{index}",
            trace_id=trace_id,
            node_type=NodeType.TOOL_CALL,
            name=tool_name,
            tool_name=tool_name,
            status=NodeStatus.FAILED if failed else NodeStatus.COMPLETED,
            error=(
                error
                if error is not None
                else canonical_json(outcome.result)
                if outcome is not None and outcome.failed
                else None
            ),
            started_at=node_started,
            ended_at=clock.now(),
            inputs=tool_inputs,
            outputs=outcome.result if outcome is not None else None,
            attributes=outcome.attributes if outcome is not None else {},
            metadata={"generator": FRAMEWORK},
        )

    for turn in range(turn_count):
        plan_index = allocate()
        plan_messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    question
                    if turn == 0
                    else f"Continue researching {topic}, step {turn + 1}."
                ),
            },
        ]
        plan_started = clock.now()
        await clock.advance(_duration_ms(trace_id, plan_index))
        plan_llm = run_llm(model, plan_messages)
        nodes.append(
            _llm_node(
                index=plan_index,
                parent_index=previous_respond_index,
                name="plan",
                messages=plan_messages,
                llm=plan_llm,
                started_at=plan_started,
                ended_at=clock.now(),
                trace_id=trace_id,
                requested_model=requested_model,
                model=model,
                model_params=params,
                turn=turn,
                output_text=(
                    f"Turn {turn + 1}: consulting {', '.join(turn_tools[turn])}."
                ),
            )
        )

        tools_parent = plan_index
        span_index: int | None = None
        span_started: datetime | None = None
        if len(turn_tools[turn]) > 1:
            span_index = allocate()
            span_started = clock.now()
            tools_parent = span_index

        turn_results: dict[str, Any] = {}
        turn_child_indexes: list[int] = []
        for tool_name in turn_tools[turn]:
            tool_inputs = build_tool_inputs(tool_name, topic, turn, variant)
            tool_index = allocate()
            tool_started = clock.now()
            await clock.advance(_duration_ms(trace_id, tool_index))
            try:
                outcome = await resolve(tool_name, tool_inputs)
            except ToolResolutionError as exc:
                nodes.append(
                    tool_node(
                        index=tool_index,
                        parent_index=tools_parent,
                        tool_name=tool_name,
                        tool_inputs=tool_inputs,
                        outcome=None,
                        node_started=tool_started,
                        error=str(exc),
                    )
                )
                session_error = str(exc)
                aborted = True
                break
            nodes.append(
                tool_node(
                    index=tool_index,
                    parent_index=tools_parent,
                    tool_name=tool_name,
                    tool_inputs=tool_inputs,
                    outcome=outcome,
                    node_started=tool_started,
                )
            )
            turn_results[tool_name] = outcome.result
            turn_child_indexes.append(tool_index)
            if tool_name == "search_web" and isinstance(outcome.result, dict):
                sources += [r["url"] for r in outcome.result.get("results", [])[:1]]

        if span_index is not None and span_started is not None:
            nodes.append(
                SessionNodeCreateRequest(
                    index=span_index,
                    parent_index=plan_index,
                    external_id=f"{trace_id}-node-{span_index}",
                    trace_id=trace_id,
                    node_type=NodeType.SPAN,
                    name="gather-context",
                    status=NodeStatus.COMPLETED,
                    started_at=span_started,
                    ended_at=clock.now(),
                    inputs={"tools": turn_tools[turn]},
                    outputs={"gathered": len(turn_results)},
                    attributes={"turn": turn + 1},
                    metadata={"generator": FRAMEWORK},
                )
            )
        if aborted:
            break

        if turn_count >= 2 and turn == 1:
            subagent_index = allocate()
            subagent_started = clock.now()
            await clock.advance(_duration_ms(trace_id, subagent_index))
            objective = f"Deep dive into {topic} anomalies."
            nodes.append(
                SessionNodeCreateRequest(
                    index=subagent_index,
                    parent_index=plan_index,
                    external_id=f"{trace_id}-node-{subagent_index}",
                    trace_id=trace_id,
                    node_type=NodeType.SUBAGENT_CALL,
                    name="delegate-research",
                    subagent_id=SUBAGENT_ID,
                    status=NodeStatus.COMPLETED,
                    started_at=subagent_started,
                    ended_at=clock.now(),
                    inputs={"objective": objective},
                    outputs={
                        "summary": (
                            f"Subagent verdict {_digest([trace_id, objective])[:8]} "
                            f"on {topic}."
                        )
                    },
                    tokens=TokenUsage(input_tokens=64, output_tokens=32),
                    cost=Decimal("0.000384"),
                    attributes={"turn": turn + 1},
                    metadata={"generator": FRAMEWORK},
                )
            )
            turn_child_indexes.append(subagent_index)

        respond_index = allocate()
        respond_messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Summarize the findings on {topic}: "
                    f"{canonical_json(turn_results)[:400]}"
                ),
            },
        ]
        respond_started = clock.now()
        await clock.advance(_duration_ms(trace_id, respond_index))
        respond_llm = run_llm(model, respond_messages)
        last_turn = turn == turn_count - 1
        respond_error = "Simulated model overload" if fail and last_turn else None
        nodes.append(
            _llm_node(
                index=respond_index,
                parent_index=plan_index,
                name="respond",
                messages=respond_messages,
                llm=respond_llm,
                started_at=respond_started,
                ended_at=clock.now(),
                trace_id=trace_id,
                requested_model=requested_model,
                model=model,
                model_params=params,
                turn=turn,
                secondary_parent_indexes=turn_child_indexes,
                error=respond_error,
            )
        )
        if respond_error is not None:
            session_error = respond_error
            break
        previous_respond_index = respond_index
        answer = respond_llm.text

    nodes.sort(key=lambda node: node.index)
    failed = session_error is not None
    outputs = (
        None
        if failed
        else {"answer": answer, "sources": sources[:5], "turns": turn_count}
    )
    return SimulatedSession(
        name=f"dummy-{slug}-{variant:06d}",
        status=SessionStatus.FAILED if failed else SessionStatus.COMPLETED,
        inputs=inputs,
        outputs=outputs,
        error=session_error,
        started_at=started_at,
        ended_at=clock.now(),
        metadata={"generator": FRAMEWORK, "topic": topic},
        nodes=nodes,
        aborted=aborted,
    )


async def compute_expected_outputs(inputs: dict[str, Any]) -> dict[str, Any] | None:
    """Compute the outputs the baseline simulation produces for the inputs."""
    baseline = await simulate_session(inputs, clock=FixedClock())
    return baseline.outputs
