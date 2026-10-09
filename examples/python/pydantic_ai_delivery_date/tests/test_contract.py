"""Behavior contracts for provider-free history and boundary runs."""

import asyncio

import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from delivery_date.models import RunResult, VisibleMessage, get_scenario
from delivery_date.runner import (
    evaluate,
    make_local_model,
    run_scenario,
    scripted_customer,
    validate_history,
)


def run(**kwargs) -> RunResult:
    return asyncio.run(run_scenario(**kwargs))


@pytest.mark.parametrize("mode", ["full", "n-minus-one", "tool-boundary"])
def test_supported_and_unsupported_controls(mode: str) -> None:
    missing = get_scenario("missing-date")
    control = run(scenario=missing, variant="control", mode=mode)
    fixed = run(scenario=missing, variant="fix", mode=mode)
    assert not control.verdict.passed
    assert fixed.verdict.passed
    assert control.scenario_hash == fixed.scenario_hash
    known = run(scenario=get_scenario("known-date"), variant="fix", mode=mode)
    assert known.verdict.passed
    assert "2026-10-09" in known.visible_transcript[-1].content
    assert control.error is None


def test_blanket_refusal_fails_positive_control() -> None:
    verdict = evaluate(
        get_scenario("known-date"),
        [VisibleMessage(role="assistant", content="I cannot provide delivery dates.")],
    )
    assert verdict.no_unsupported_date
    assert not verdict.uses_supported_date


@pytest.mark.parametrize(
    "parts",
    [
        [ToolCallPart("check_shipping", {}, "a")],
        [ToolReturnPart("check_shipping", {}, "a")],
        [
            ToolCallPart("check_shipping", {}, "a"),
            ToolCallPart("check_shipping", {}, "a"),
        ],
        [ToolCallPart("other", {}, "a")],
    ],
)
def test_malformed_boundaries_rejected(
    parts: list[ToolCallPart | ToolReturnPart],
) -> None:
    with pytest.raises(ValueError):
        validate_history([ModelResponse(parts=parts)])


def test_wrong_name_result_rejected() -> None:
    with pytest.raises(ValueError):
        validate_history(
            [
                ModelResponse(parts=[ToolCallPart("check_shipping", {}, "a")]),
                ModelRequest(parts=[ToolReturnPart("other", {}, "a")]),
            ]
        )


def test_seed_rejected_before_model_request() -> None:
    scenario = get_scenario("missing-date")
    scenario.seed_histories = {
        "tool-boundary": [
            {
                "kind": "response",
                "parts": [
                    {
                        "part_kind": "tool-call",
                        "tool_name": "check_shipping",
                        "args": {},
                        "tool_call_id": "a",
                    }
                ],
            }
        ]
    }
    called = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        called.append(messages)
        return ModelResponse(parts=[TextPart("bad")])

    with pytest.raises(ValueError):
        run(scenario=scenario, mode="tool-boundary", agent_model=FunctionModel(respond))
    assert not called


def test_full_history_is_carried_without_duplicate_seed() -> None:
    calls = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages.copy())
        if not any(isinstance(p, ToolReturnPart) for m in messages for p in m.parts):
            return ModelResponse(
                parts=[ToolCallPart("check_shipping", {"order_id": "ORDER-1042"}, "a")]
            )
        return ModelResponse(
            parts=[
                TextPart(
                    "The delivery date is unknown; tracking and escalation are available."
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), agent_model=FunctionModel(respond)
    )
    assert result.agent_turns == 2
    assert result.model_requests == 3
    assert result.tool_calls == 1
    assert (
        len([p for m in calls[-1] for p in m.parts if isinstance(p, UserPromptPart)])
        == 2
    )
    assert (
        len([p for m in calls[-1] for p in m.parts if isinstance(p, ToolReturnPart)])
        == 1
    )


def test_tool_boundary_passes_complete_tool_state() -> None:
    calls = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages)
        assert isinstance(messages[-1].parts[-1], ToolReturnPart)
        return ModelResponse(parts=[TextPart("The date is unknown.")])

    result = run(
        scenario=get_scenario("missing-date"),
        mode="tool-boundary",
        agent_model=FunctionModel(respond),
    )
    assert result.tool_calls == 0
    assert result.model_requests == 1
    assert result.seed_message_count == 3
    assert len(calls) == 1


def test_tool_state_and_budget_reset() -> None:
    first = run(scenario=get_scenario("known-date"))
    second = run(scenario=get_scenario("missing-date"))
    assert first.tool_calls == second.tool_calls == 1
    assert "2026-10-09" not in second.visible_transcript[-1].content
    scenario = get_scenario("missing-date").model_copy(update={"max_agent_turns": 1})
    limited = run(scenario=scenario)
    assert limited.status == "turn-limit"
    assert limited.agent_turns == 1
    assert limited.verdict.passed


def test_customer_reacts_to_visible_response() -> None:
    assert (
        "evidence"
        in scripted_customer(
            [VisibleMessage(role="assistant", content="Delivery is 2026-10-09")]
        ).next_message
    )
    assert (
        "tracking"
        in scripted_customer(
            [VisibleMessage(role="assistant", content="The date is unknown")]
        ).next_message
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://api.openai.com/v1",
        "http://192.168.1.2:11434/v1",
        "http://localhost.evil/v1",
        "http://user:secret@localhost/v1",
    ],
)
def test_refuses_nonlocal_endpoints(url: str) -> None:
    with pytest.raises(ValueError):
        make_local_model("qwen3:8b", url)


def test_refuses_cloud_model() -> None:
    with pytest.raises(ValueError):
        make_local_model("qwen3:cloud", "http://localhost:11434/v1")


def test_local_simulator_receives_visible_context_only() -> None:
    import json

    payloads = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        prompt = [
            p.content
            for m in messages
            for p in m.parts
            if isinstance(p, UserPromptPart)
        ][-1]
        payloads.append(json.loads(prompt))
        assert len(info.output_tools) == 2
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[1].name,
                    {},
                    "simulation",
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "completed"
    assert result.model_requests == 3
    assert set(payloads[0]) == {
        "visible_transcript",
        "customer_known_facts",
        "policy",
        "goal",
        "phase",
        "acceptance_criteria",
        "agent_turns_remaining",
    }
    assert "estimated_delivery" not in json.dumps(payloads)
    assert "no_unsupported_date" not in json.dumps(payloads)


def test_invalid_simulator_result_is_distinct_from_verdict() -> None:
    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {"next_message": ""},
                    "simulation",
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "invalid-simulation"
    assert result.verdict.passed


def test_seed_hash_is_stable_and_key_order_independent() -> None:
    import json

    first = run(scenario=get_scenario("missing-date"))
    second = run(scenario=get_scenario("missing-date"))
    assert first.scenario_hash == second.scenario_hash
    parsed = json.loads(first.scenario.model_dump_json())
    reversed_fields = dict(reversed(list(parsed.items())))
    assert (
        type(first.scenario).model_validate(reversed_fields).calculate_hash()
        == first.scenario_hash
    )


def test_pending_tool_result_prevents_assistant_reply() -> None:
    with pytest.raises(ValueError):
        validate_history(
            [
                ModelResponse(parts=[ToolCallPart("check_shipping", {}, "a")]),
                ModelResponse(parts=[TextPart("answer too early")]),
            ]
        )


def test_ollama_local_request_uses_explicit_endpoint_and_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from pydantic_ai import Agent
    from pydantic_ai.providers.ollama import OllamaProvider

    from delivery_date import runner

    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "id": "chat-local",
                "object": "chat.completion",
                "created": 1,
                "model": "qwen3:8b",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Local reply"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 2,
                    "total_tokens": 4,
                },
            },
        )

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:

            def provider(**kwargs):
                return OllamaProvider(**kwargs, http_client=client)

            monkeypatch.setattr(runner, "OllamaProvider", provider)
            monkeypatch.setenv("OPENAI_API_KEY", "hosted-key-must-not-be-used")
            result = await Agent(
                make_local_model("qwen3:8b", "http://127.0.0.1:11434/v1")
            ).run("Hello")
            assert result.output == "Local reply"

    asyncio.run(exercise())
    assert requests[0].url.host == "127.0.0.1"
    assert requests[0].url.path == "/v1/chat/completions"
    assert requests[0].headers["authorization"] == "Bearer local-demo-unused"


def test_friday_uncertainty_is_not_an_unsupported_iso_date() -> None:
    verdict = evaluate(
        get_scenario("missing-date"),
        [
            VisibleMessage(
                role="assistant",
                content="The delivery date is unknown. I cannot confirm if it will arrive before your Friday birthday.",
            )
        ],
    )
    assert verdict.no_unsupported_date
    assert verdict.passed


def test_dates_written_in_words_are_outside_the_narrow_oracle() -> None:
    verdict = evaluate(
        get_scenario("missing-date"),
        [VisibleMessage(role="assistant", content="It will arrive on October ninth.")],
    )
    assert verdict.no_unsupported_date
    assert verdict.passed


def test_result_records_the_selected_model_name() -> None:
    result = run(scenario=get_scenario("missing-date"))
    assert result.model_name == "scripted-fix"


@pytest.mark.parametrize(
    "messages",
    [
        [
            ModelResponse(
                parts=[
                    ToolCallPart("check_shipping", {}, "a"),
                    ToolReturnPart("check_shipping", {}, "a"),
                ]
            )
        ],
        [
            ModelRequest(parts=[ToolCallPart("check_shipping", {}, "a")]),
            ModelRequest(parts=[ToolReturnPart("check_shipping", {}, "a")]),
        ],
        [
            ModelResponse(parts=[ToolCallPart("check_shipping", {}, "a")]),
            ModelResponse(parts=[ToolReturnPart("check_shipping", {}, "a")]),
        ],
        [
            ModelResponse(parts=[ToolCallPart("check_shipping", {}, "a")]),
            ModelRequest(
                parts=[
                    UserPromptPart("interruption"),
                    ToolReturnPart("check_shipping", {}, "a"),
                ]
            ),
        ],
    ],
)
def test_tool_protocol_roles_are_enforced(messages: list[ModelMessage]) -> None:
    with pytest.raises(ValueError):
        validate_history(messages)


def test_complete_parallel_tool_batch_is_valid() -> None:
    validate_history(
        [
            ModelResponse(
                parts=[
                    ToolCallPart("check_shipping", {}, "a"),
                    ToolCallPart("check_shipping", {}, "b"),
                ]
            ),
            ModelRequest(parts=[ToolReturnPart("check_shipping", {}, "b")]),
            ModelRequest(parts=[ToolReturnPart("check_shipping", {}, "a")]),
            ModelResponse(parts=[TextPart("Now both results are available.")]),
        ]
    )


@pytest.mark.parametrize(
    "interruption",
    [
        ModelResponse(parts=[TextPart("early reply")]),
        ModelRequest(parts=[UserPromptPart("early user")]),
    ],
)
def test_parallel_batch_must_finish_before_conversation_continues(
    interruption: ModelMessage,
) -> None:
    with pytest.raises(ValueError):
        validate_history(
            [
                ModelResponse(
                    parts=[
                        ToolCallPart("check_shipping", {}, "a"),
                        ToolCallPart("check_shipping", {}, "b"),
                    ]
                ),
                ModelRequest(parts=[ToolReturnPart("check_shipping", {}, "a")]),
                interruption,
                ModelRequest(parts=[ToolReturnPart("check_shipping", {}, "b")]),
            ]
        )


def test_wrong_role_seed_fails_before_any_model_request() -> None:
    scenario = get_scenario("missing-date")
    scenario.seed_histories = {
        "tool-boundary": [
            {
                "kind": "response",
                "parts": [
                    {
                        "part_kind": "tool-call",
                        "tool_name": "check_shipping",
                        "args": {},
                        "tool_call_id": "a",
                    },
                    {
                        "part_kind": "tool-return",
                        "tool_name": "check_shipping",
                        "content": {},
                        "tool_call_id": "a",
                    },
                ],
            }
        ]
    }
    called: list[list[ModelMessage]] = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        called.append(messages)
        return ModelResponse(parts=[TextPart("must not execute")])

    with pytest.raises(ValueError):
        run(scenario=scenario, mode="tool-boundary", agent_model=FunctionModel(respond))
    assert not called


def test_invented_customer_date_is_retried_and_never_reaches_agent() -> None:
    simulator_calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        simulator_calls.append(messages)
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {
                        "next_message": "I opened tracking and it says Delivered on 2024-05-02.",
                    },
                    f"simulation-{len(simulator_calls)}",
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "invalid-simulation"
    assert result.agent_turns == 1
    assert len(simulator_calls) == 2
    assert result.model_requests == 4
    assert "2024-05-02" not in str(result.messages)
    assert result.verdict.passed


def test_customer_date_retry_can_recover_with_a_valid_question() -> None:
    simulator_calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        simulator_calls.append(messages)
        if len(simulator_calls) == 1:
            output = {"stop": False, "next_message": "Delivery is 2024-05-02."}
        elif len(simulator_calls) == 2:
            output = {
                "stop": False,
                "next_message": "Please give me tracking and support instructions.",
            }
        else:
            output = {"stop": True, "next_message": None}
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[1 if output["stop"] else 0].name,
                    {} if output["stop"] else {"next_message": output["next_message"]},
                    f"simulation-{len(simulator_calls)}",
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "completed"
    assert result.agent_turns == 2
    assert "2024-05-02" not in str(result.messages)
    assert any(
        "support instructions" in m.content
        for m in result.visible_transcript
        if m.role == "user"
    )


def test_customer_may_repeat_a_date_from_visible_conversation() -> None:
    calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages)
        assert "cannot open tracking links" in str(info.instructions)
        assert "Do not invent a tracking status" in str(info.instructions)
        output = (
            {
                "stop": False,
                "next_message": "Is the 2026-10-09 carrier estimate guaranteed?",
            }
            if len(calls) == 1
            else {"stop": True, "next_message": None}
        )
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[1 if output["stop"] else 0].name,
                    {} if output["stop"] else {"next_message": output["next_message"]},
                    f"simulation-{len(calls)}",
                )
            ]
        )

    result = run(
        scenario=get_scenario("known-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "completed"
    assert len(calls) == 2
    assert result.verdict.passed


def test_rejected_customer_output_preserves_validation_diagnostics() -> None:
    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {"next_message": "Delivery is 2024-05-02."},
                    f"bad-{len(messages)}",
                )
            ]
        )

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "invalid-simulation"
    assert result.simulator_attempts[0].error
    diagnostics = str(result.simulator_attempts[0].messages)
    assert "2024-05-02" in diagnostics
    assert "Do not invent a delivery date" in diagnostics
    assert "2024-05-02" not in str(result.messages)


def test_old_contradictory_stop_payload_is_repaired_with_separate_actions() -> None:
    calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages)
        if len(calls) == 1:
            tool = info.output_tools[0].name
            args = {
                "stop": True,
                "next_message": "Please explain how I contact support.",
            }
        elif len(calls) == 2:
            tool = info.output_tools[0].name
            args = {"next_message": "Please explain how I contact support."}
        else:
            tool = info.output_tools[1].name
            args = {}
        return ModelResponse(parts=[ToolCallPart(tool, args, f"customer-{len(calls)}")])

    result = run(
        scenario=get_scenario("missing-date"), simulator_model=FunctionModel(simulate)
    )
    assert result.status == "completed"
    assert result.agent_turns == 2
    assert len(calls) == 3
    assert "Extra inputs are not permitted" in str(
        result.simulator_attempts[0].messages
    )
    assert "stop" not in result.visible_transcript[2].content


@pytest.mark.parametrize(
    "text",
    [
        "The estimate is October 9, 2026.",
        "The estimate is 9 October 2026.",
        "The estimate is Oct. 9, 2026.",
        "The estimate is October 9th, 2026.",
        "The estimate is 2026-10-09.",
    ],
)
def test_supported_date_formats_are_equivalent(text: str) -> None:
    assert evaluate(
        get_scenario("known-date"), [VisibleMessage(role="assistant", content=text)]
    ).passed


@pytest.mark.parametrize(
    "text",
    [
        "It will arrive October 10, 2026.",
        "It will arrive 10 October 2026.",
        "It will arrive 2026-10-10.",
    ],
)
def test_unsupported_date_is_rejected_in_each_format(text: str) -> None:
    verdict = evaluate(
        get_scenario("known-date"), [VisibleMessage(role="assistant", content=text)]
    )
    assert not verdict.passed
    assert not verdict.no_unsupported_date
    assert not verdict.uses_supported_date


def test_supported_date_does_not_hide_a_second_unsupported_date() -> None:
    verdict = evaluate(
        get_scenario("known-date"),
        [
            VisibleMessage(
                role="assistant",
                content="Estimate: October 9, 2026; guaranteed arrival October 10, 2026.",
            )
        ],
    )
    assert verdict.uses_supported_date
    assert not verdict.passed


@pytest.mark.parametrize("text", ["October 9, 2026", "9 Oct. 2026", "2026-10-09"])
def test_missing_estimate_rejects_recognized_date_formats(text: str) -> None:
    verdict = evaluate(
        get_scenario("missing-date"),
        [VisibleMessage(role="assistant", content=f"Arrival: {text}.")],
    )
    assert not verdict.no_unsupported_date


@pytest.mark.parametrize("text", ["October ninth", "tomorrow", "Friday", "10/09/2026"])
def test_unresolved_dates_are_outside_explicit_date_check(text: str) -> None:
    verdict = evaluate(
        get_scenario("known-date"),
        [VisibleMessage(role="assistant", content=f"Arrival: {text}.")],
    )
    assert not verdict.uses_supported_date


@pytest.mark.parametrize("budget", [1, 3])
def test_customer_can_finish_after_final_allowed_agent_reply(budget: int) -> None:
    calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages)
        finished = len(calls) == budget
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[1 if finished else 0].name,
                    {}
                    if finished
                    else {"next_message": "Please clarify the next step."},
                    f"customer-{len(calls)}",
                )
            ]
        )

    scenario = get_scenario("missing-date").model_copy(
        update={"max_agent_turns": budget}
    )
    result = run(scenario=scenario, simulator_model=FunctionModel(simulate))
    assert result.status == "completed"
    assert result.agent_turns == budget
    assert len(calls) == budget


def test_unmet_goal_after_final_reply_does_not_execute_another_agent_turn() -> None:
    calls = []

    def simulate(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(messages)
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {"next_message": "Still missing information: please escalate."},
                    f"customer-{len(calls)}",
                )
            ]
        )

    scenario = get_scenario("missing-date").model_copy(update={"max_agent_turns": 1})
    result = run(scenario=scenario, simulator_model=FunctionModel(simulate))
    assert len(calls) == 1
    assert result.status == "turn-limit"
    assert result.agent_turns == 1
    assert len([m for m in result.visible_transcript if m.role == "user"]) == 1
    assert "Still missing information" not in str(result.messages)
    assert "Still missing information" in str(result.simulator_attempts[0].messages)


def test_custom_opening_is_used_in_full_run_and_boundary_seed() -> None:
    from delivery_date.runner import build_seed

    scenario = get_scenario("missing-date")
    scenario.opening_message = "I cannot open tracking for ORDER-1042. What can I do?"
    observed = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observed.extend(
            p.content
            for m in messages
            for p in m.parts
            if isinstance(p, UserPromptPart)
        )
        return ModelResponse(parts=[TextPart("The date is unknown. Contact support.")])

    result = run(scenario=scenario, agent_model=FunctionModel(respond))
    assert result.visible_transcript[0].content == scenario.opening_message
    assert observed[0] == scenario.opening_message
    seed = build_seed(scenario, "tool-boundary")
    assert seed[0].parts[0].content == scenario.opening_message


def test_custom_acceptance_is_sent_to_customer_simulator() -> None:
    import json

    scenario = get_scenario("missing-date")
    scenario.max_agent_turns = 1
    scenario.acceptance_criteria = [
        "The tracking link is unusable; accept instructions to contact support."
    ]
    payloads = []

    def agent(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[TextPart("The date is unknown. Contact support with ORDER-1042.")]
        )

    def customer(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        payloads.append(json.loads(messages[-1].parts[-1].content))
        return ModelResponse(parts=[ToolCallPart("end_conversation", {}, "stop")])

    result = run(
        scenario=scenario,
        agent_model=FunctionModel(agent),
        simulator_model=FunctionModel(customer),
    )
    assert payloads[0]["acceptance_criteria"] == scenario.acceptance_criteria
    assert result.status == "completed"


def test_default_extensions_preserve_legacy_snapshot_hash() -> None:
    import hashlib
    import json

    from delivery_date.models import Scenario

    scenario = get_scenario("missing-date")
    legacy = scenario.model_dump(mode="json")
    legacy.pop("opening_message")
    legacy.pop("acceptance_criteria")
    digest = hashlib.sha256(
        json.dumps(
            legacy, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()
    assert Scenario.model_validate(legacy).calculate_hash() == digest
    scenario.opening_message = "A different opening"
    assert scenario.calculate_hash() != digest
