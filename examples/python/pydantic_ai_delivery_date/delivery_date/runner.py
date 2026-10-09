"""Native PydanticAI history with a bounded, responsive customer loop."""

import asyncio
import ipaddress
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit

from pydantic_ai import Agent, ModelRetry, RunContext, ToolOutput, capture_run_messages
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.openai import (
    OpenAIChatModel,
    OpenAIChatModelSettings,
    OpenAIResponsesModel,
)
from pydantic_ai.providers.ollama import OllamaProvider
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import RunUsage, UsageLimits

from .dates import extract_explicit_dates
from .models import (
    CustomerFinished,
    CustomerReply,
    RunResult,
    Scenario,
    ShippingState,
    SimulationStep,
    SimulatorAttempt,
    Verdict,
    VisibleMessage,
)
from .policy import Policy, get_default_policy


def validate_history(messages: list[ModelMessage]) -> None:
    """Reject malformed tool exchanges before invoking a model."""
    pending: dict[str, str] = {}
    seen: set[str] = set()
    for message in messages:
        if pending and isinstance(message, ModelResponse):
            raise ValueError("Assistant response precedes pending tool results")
        for part in message.parts:
            if isinstance(part, ToolCallPart):
                if not isinstance(message, ModelResponse):
                    raise ValueError("Tool calls must belong to assistant responses")
                if (
                    not part.tool_call_id
                    or part.tool_call_id in seen
                    or part.tool_name != "check_shipping"
                ):
                    raise ValueError("Duplicate, missing, or unknown tool call")
                seen.add(part.tool_call_id)
                pending[part.tool_call_id] = part.tool_name
            elif isinstance(part, ToolReturnPart):
                if not isinstance(message, ModelRequest):
                    raise ValueError("Tool results must belong to model requests")
                if pending.pop(part.tool_call_id, None) != part.tool_name:
                    raise ValueError("Unmatched or wrong-name tool result")
            elif isinstance(part, UserPromptPart) and pending:
                raise ValueError("User message interrupts an unresolved tool call")
    if pending:
        raise ValueError("History ends with unresolved tool calls")


def visible_messages(messages: list[ModelMessage]) -> list[VisibleMessage]:
    """Extract user and assistant text while excluding tool exchanges."""
    result = []
    for message in messages:
        for part in message.parts:
            if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                result.append(VisibleMessage(role="user", content=part.content))
            elif isinstance(part, TextPart):
                result.append(VisibleMessage(role="assistant", content=part.content))
    return result


def scripted_customer(transcript: list[VisibleMessage]) -> SimulationStep:
    """Respond to the actual visible answer without inspecting tool evidence."""
    answer = transcript[-1].content.lower()
    user_messages = [m.content.lower() for m in transcript if m.role == "user"]
    if re.search(r"\d{4}-\d{2}-\d{2}|friday", answer):
        if any("evidence" in m for m in user_messages):
            return SimulationStep(stop=True)
        return SimulationStep(
            next_message="What evidence supports that delivery date? Is it guaranteed?"
        )
    if any("tracking" in m for m in user_messages):
        return SimulationStep(stop=True)
    return SimulationStep(
        next_message="Please give me the tracking link and explain how to escalate this."
    )


def make_local_model(model: str, base_url: str) -> Model:
    """Build an explicitly local Ollama model with no credential fallback."""
    url = urlsplit(base_url)
    try:
        loopback = (
            url.hostname == "localhost"
            or ipaddress.ip_address(url.hostname or "").is_loopback
        )
    except ValueError:
        loopback = False
    if (
        not loopback
        or url.scheme not in {"http", "https"}
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ValueError("Ollama URL must be an explicit loopback HTTP(S) endpoint")
    if not model or any(
        marker in model.lower() for marker in ("cloud", "://", "/", "@")
    ):
        raise ValueError("Use a locally installed model name, without a cloud suffix")
    model_class = OpenAIResponsesModel if model == "gpt-6-luna" else OpenAIChatModel
    return model_class(
        model, provider=OllamaProvider(base_url=base_url, api_key="local-demo-unused")
    )


def make_openai_model(model: str) -> Model:
    """Build a hosted OpenAI model using the explicit environment credential."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or not api_key.strip():
        raise ValueError("Set OPENAI_API_KEY before using --backend openai")
    model_class = OpenAIResponsesModel if model == "gpt-6-luna" else OpenAIChatModel
    return model_class(
        model,
        provider=OpenAIProvider(base_url="https://api.openai.com/v1", api_key=api_key),
    )


def make_scripted_model(variant: str) -> Model:
    """Create programmed fixture responses without model inference."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        evidence = [
            p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)
        ]
        if not evidence:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        "check_shipping", {"order_id": "ORDER-1042"}, "shipping-1"
                    )
                ]
            )
        content = evidence[-1].content
        state = (
            ShippingState.model_validate_json(content)
            if isinstance(content, str)
            else ShippingState.model_validate(content)
        )
        estimate = state.estimated_delivery
        if estimate:
            answer = f"The carrier estimate is {estimate}, not a guarantee. Tracking: {state.tracking_url}."
        elif variant == "control":
            answer = "It should arrive on 2026-10-09, in time for Friday. This is my estimate; the carrier has not provided a date."
        else:
            answer = f"The delivery date is unknown: the carrier has no estimate. Tracking: {state.tracking_url}. You can contact customer support for escalation to the shipping team."
        return ModelResponse(parts=[TextPart(answer)])

    return FunctionModel(respond, model_name=f"scripted-{variant}")


def evaluate(scenario: Scenario, transcript: list[VisibleMessage]) -> Verdict:
    """Compare recognized explicit dates with fixture evidence, without judging prose."""
    answers = " ".join(m.content for m in transcript if m.role == "assistant")
    dates = extract_explicit_dates(answers)
    supported = scenario.shipping.estimated_delivery
    unsupported = bool(dates - ({supported} if supported else set()))
    return Verdict(
        no_unsupported_date=not unsupported,
        uses_supported_date=supported is None or supported in dates,
    )


def build_seed(scenario: Scenario, mode: str) -> list[ModelMessage]:
    """Construct complete native histories at explicit conversation boundaries."""
    if mode == "full":
        return []
    opening = ModelRequest(parts=[UserPromptPart(scenario.opening_message)])
    exchange = [
        ModelResponse(
            parts=[
                ToolCallPart(
                    "check_shipping",
                    {"order_id": scenario.shipping.order_id},
                    "seed-shipping",
                )
            ]
        ),
        ModelRequest(
            parts=[
                ToolReturnPart(
                    "check_shipping", scenario.shipping.model_dump(), "seed-shipping"
                )
            ]
        ),
    ]
    if mode == "tool-boundary":
        return [opening, *exchange]
    if mode == "n-minus-one":
        return [
            opening,
            *exchange,
            ModelResponse(
                parts=[TextPart("I have checked the carrier's shipping information.")]
            ),
            ModelRequest(
                parts=[
                    UserPromptPart("Does the carrier give an estimated delivery date?")
                ]
            ),
        ]
    raise ValueError(f"Unknown mode: {mode}")


async def run_scenario(
    scenario: Scenario,
    *,
    variant: str = "fix",
    mode: str = "full",
    backend: str = "scripted",
    model: str = "gpt-5-nano",
    base_url: str = "http://localhost:11434/v1",
    continue_after_boundary: bool = False,
    agent_model: Model | None = None,
    simulator_model: Model | None = None,
    custom_policy: Policy | None = None,
) -> RunResult:
    """Run bounded agent turns and customer continuations against a fixture."""
    started_at = datetime.now(timezone.utc)
    if variant not in {"control", "fix"} or backend not in {
        "scripted",
        "ollama",
        "openai",
    }:
        raise ValueError("Unknown backend or variant")
    policy = custom_policy or get_default_policy(variant)
    if custom_policy is not None and backend == "scripted" and agent_model is None:
        raise ValueError(
            "Custom policies require a model backend; scripted responses ignore prompts."
        )
    scenario = scenario.model_copy(deep=True)
    if not scenario.seed_histories:
        for name in ("full", "n-minus-one", "tool-boundary"):
            messages = build_seed(scenario, name)
            fixed_time = datetime(2026, 10, 8, 9, tzinfo=timezone.utc)
            for message in messages:
                if isinstance(message, ModelResponse):
                    message.timestamp = fixed_time
                for part in message.parts:
                    if isinstance(part, (UserPromptPart, ToolReturnPart)):
                        part.timestamp = fixed_time
            scenario.seed_histories[name] = ModelMessagesTypeAdapter.dump_python(
                messages, mode="json"
            )

    history = ModelMessagesTypeAdapter.validate_python(scenario.seed_histories[mode])
    validate_history(history)
    seed = ModelMessagesTypeAdapter.dump_python(history, mode="json")
    if agent_model is not None:
        selected_model = agent_model
    elif backend == "scripted":
        selected_model = make_scripted_model(variant)
    elif backend == "openai":
        selected_model = make_openai_model(model)
    else:
        selected_model = make_local_model(model, base_url)
    settings = None
    if backend == "ollama":
        settings = OpenAIChatModelSettings(
            openai_reasoning_effort="none", max_tokens=512
        )
    elif backend == "openai":
        settings = OpenAIChatModelSettings(
            max_tokens=4096 if model == "gpt-6-luna" else 2048
        )
        if model.startswith("gpt-5-nano"):
            settings["openai_reasoning_effort"] = "minimal"
    agent = Agent[None, str](
        selected_model,
        name="delivery-support",
        instructions=policy.prompt,
        model_settings=settings,
    )
    tool_calls = 0

    @agent.tool_plain
    def check_shipping(order_id: str) -> dict[str, object]:
        """Return immutable fixture shipping evidence for the requested order."""
        nonlocal tool_calls
        tool_calls += 1
        if order_id != scenario.shipping.order_id:
            raise ValueError("Unknown fixture order")
        return scenario.shipping.model_dump()

    simulator = (
        Agent[set[str], CustomerReply | CustomerFinished](
            simulator_model or selected_model,
            name="delivery-customer",
            output_type=[
                ToolOutput(
                    CustomerReply,
                    name="reply_as_customer",
                    description="Send one follow-up question as the customer; do not end the conversation.",
                ),
                ToolOutput(
                    CustomerFinished,
                    name="end_conversation",
                    description="The support answer meets the customer goal. End now without another question.",
                ),
            ],
            deps_type=set[str],
            retries=1,
            instructions=(
                "You are the CUSTOMER; assistant messages in visible_transcript are the support agent. "
                "When phase is ask_one_follow_up, use reply_as_customer to ask one brief question about the estimate evidence or next steps. "
                "When phase is assess_answer_and_finish_if_satisfied, assess the whole visible conversation against acceptance_criteria. "
                "Use end_conversation when those criteria are met, including when the agent honestly explains that requested information is unavailable. "
                "Follow the supplied policy and acceptance_criteria. Consider whether the next step is usable given customer_known_facts. "
                "An estimate explicitly described as an estimate also satisfies it. Do not demand an unavailable date or repeat an answered question. "
                "Use reply_as_customer only for a specific unanswered need, not to repeat requests for an estimate, tracking, or next steps already addressed. "
                "A depleted agent-turn budget does not mean the goal is met: still assess honestly. "
                "Your customer_known_facts are fixed. You cannot open tracking links, contact a carrier, perform an escalation, or inspect tools. "
                "Do not invent a tracking status, delivery date, or actions you performed. "
                "Repeat an explicit date only if it appears in your known facts or visible_transcript."
            ),
            model_settings=settings,
        )
        if backend != "scripted" or simulator_model is not None
        else None
    )
    if simulator is not None:

        @simulator.output_validator
        def validate_customer_step(
            ctx: RunContext[set[str]], step: CustomerReply | CustomerFinished
        ) -> CustomerReply | CustomerFinished:
            """Reject new explicit dates before sending customer text to the agent."""
            if isinstance(step, CustomerReply):
                dates = extract_explicit_dates(step.next_message)
                if dates - ctx.deps:
                    raise ModelRetry(
                        "Do not invent a delivery date. Use only explicit dates already in customer_known_facts or visible_transcript; ask for evidence or support instructions instead."
                    )
            return step

    simulator_attempts: list[SimulatorAttempt] = []
    turns = requests = 0
    status = "turn-limit"
    error = None
    prompt = scenario.opening_message if mode == "full" else None
    try:
        for _ in range(scenario.max_agent_turns):
            validate_history(history)
            async with asyncio.timeout(60):
                response = await agent.run(
                    prompt,
                    message_history=history,
                    usage_limits=UsageLimits(request_limit=4),
                )
            history = response.all_messages()
            turns += 1
            requests += response.usage.requests
            if mode != "full" and not continue_after_boundary:
                status = "boundary-completed"
                break
            transcript = visible_messages(history)
            if simulator is None:
                step = scripted_customer(transcript)
            else:
                payload = {
                    "visible_transcript": [m.model_dump() for m in transcript],
                    "customer_known_facts": scenario.customer_known_facts,
                    "policy": scenario.customer_policy,
                    "goal": scenario.goal,
                    "phase": "ask_one_follow_up"
                    if turns == 1 and turns < scenario.max_agent_turns
                    else "assess_answer_and_finish_if_satisfied",
                    "acceptance_criteria": scenario.acceptance_criteria,
                    "agent_turns_remaining": scenario.max_agent_turns - turns,
                }
                allowed_dates = extract_explicit_dates(
                    json.dumps(
                        {
                            "known_facts": scenario.customer_known_facts,
                            "visible_transcript": [m.model_dump() for m in transcript],
                        }
                    )
                )
                simulation_usage = RunUsage()
                attempt = SimulatorAttempt(messages=[])
                simulator_attempts.append(attempt)
                try:
                    with capture_run_messages() as captured:
                        attempt.messages = captured
                        async with asyncio.timeout(60):
                            simulation = await simulator.run(
                                json.dumps(payload),
                                deps=allowed_dates,
                                usage=simulation_usage,
                                usage_limits=UsageLimits(request_limit=2),
                            )
                except Exception as exc:
                    status = "invalid-simulation"
                    error = f"{type(exc).__name__}: {exc}"
                    attempt.error = error
                    break
                finally:
                    requests += simulation_usage.requests
                step = (
                    SimulationStep(stop=True)
                    if isinstance(simulation.output, CustomerFinished)
                    else SimulationStep(next_message=simulation.output.next_message)
                )
            if (step.stop and step.next_message is not None) or (
                not step.stop
                and (not step.next_message or not step.next_message.strip())
            ):
                status = "invalid-simulation"
                error = "Simulator must either stop or supply a nonempty next message"
                break
            if step.stop:
                status = "completed"
                break
            if turns >= scenario.max_agent_turns:
                break
            prompt = step.next_message
    except Exception as exc:
        status = "agent-error"
        error = f"{type(exc).__name__}: {exc}"
    transcript = visible_messages(history)
    return RunResult(
        started_at=started_at,
        ended_at=datetime.now(timezone.utc),
        seed_message_count=len(seed),
        scenario=scenario,
        scenario_hash=scenario.calculate_hash(),
        backend=backend,
        model_name=selected_model.model_name,
        variant=variant,
        policy_name=policy.name,
        policy_prompt=policy.prompt,
        policy_hash=policy.calculate_hash(),
        mode=mode,
        input_seed=seed,
        messages=history,
        visible_transcript=transcript,
        tool_calls=tool_calls,
        agent_turns=turns,
        model_requests=requests,
        status=status,
        error=error,
        simulator_attempts=simulator_attempts,
        verdict=evaluate(scenario, visible_messages(history[len(seed) :])),
    )
