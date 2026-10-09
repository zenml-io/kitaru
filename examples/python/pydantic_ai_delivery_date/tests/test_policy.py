"""Reviewed policies alter target instructions without altering evaluation cases."""

import asyncio
import hashlib
import json

import pytest
from pydantic import ValidationError
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, UserPromptPart
from pydantic_ai.models.function import FunctionModel

from delivery_date.models import RunResult, get_scenario
from delivery_date.policy import Policy, PolicyOptions, PolicyProposal, PolicyRequest
from delivery_date.runner import run_scenario
from delivery_date.simulation import RunRequest
from delivery_date.sources import to_editor


def test_custom_policy_reaches_target_model_and_preserves_fixture():
    scenario = get_scenario("known-date")
    scenario.max_agent_turns = 1
    policy = Policy(name="Concise", prompt="  Only say the evidence-backed date.\n")
    instructions = []

    def respond(messages, info):
        instructions.append(info.instructions)
        return ModelResponse(parts=[TextPart("Estimate: 2026-10-09.")])

    result = asyncio.run(
        run_scenario(scenario, custom_policy=policy, agent_model=FunctionModel(respond))
    )
    assert instructions == [policy.prompt]
    assert result.policy_name == policy.name
    assert result.policy_prompt == policy.prompt
    assert result.policy_hash == hashlib.sha256(policy.prompt.encode()).hexdigest()
    assert result.scenario.shipping == scenario.shipping
    assert result.scenario.acceptance_criteria == scenario.acceptance_criteria
    assert result.verdict.passed
    stored = result.model_dump(mode="json")
    assert RunResult.model_validate(stored).policy_prompt == policy.prompt
    with pytest.raises(ValidationError, match="SHA256"):
        RunResult.model_validate(stored | {"policy_prompt": "different instructions"})


def test_custom_policy_cannot_claim_scripted_responses_follow_it():
    with pytest.raises(ValueError, match="scripted responses ignore prompts"):
        asyncio.run(
            run_scenario(
                get_scenario("missing-date"),
                custom_policy=Policy(name="Test", prompt="Hi"),
            )
        )


@pytest.mark.parametrize("extra", ["scenario", "checks", "tools"])
def test_policy_proposal_cannot_change_locked_case_fields(extra):
    policy = {"name": "Candidate", "prompt": "Use only shipping evidence."}
    with pytest.raises(ValidationError):
        PolicyProposal.model_validate(
            {"policy": policy, "rationale": "Be precise", extra: {}}
        )
    with pytest.raises(ValidationError):
        Policy.model_validate(policy | {extra: {}})


def test_blank_policy_and_injected_proposal_request_fields_are_rejected():
    with pytest.raises(ValidationError):
        Policy(name="Test", prompt=" \n ")
    with pytest.raises(ValidationError):
        PolicyRequest.model_validate(
            {
                "baseline": {"name": "Test", "prompt": "Hello"},
                "instruction": "Improve it",
                "acceptance": "Always pass",
            }
        )


def test_editor_request_retains_reviewed_policy_and_original_scenario():
    from uuid import uuid4

    scenario = to_editor(get_scenario("missing-date"), {})
    policy = Policy(name="Reviewed", prompt="Use tool evidence only.")
    request = RunRequest(
        title="Case", sourceId=uuid4(), scenario=scenario, policy=policy
    )
    assert RunRequest.model_validate(request.model_dump()).policy == policy
    assert request.scenario == scenario


def make_policy_options():
    return PolicyOptions(
        proposals=[
            PolicyProposal(
                policy=Policy(
                    name="Evidence first", prompt="Quote only shipping evidence."
                ),
                rationale="Avoid unsupported delivery promises.",
            ),
            PolicyProposal(
                policy=Policy(
                    name="Concise", prompt="Use shipping evidence in a short answer."
                ),
                rationale="Keep the answer brief.",
            ),
            PolicyProposal(
                policy=Policy(
                    name="Next steps",
                    prompt="Use shipping evidence and explain next steps.",
                ),
                rationale="Help customers act on uncertainty.",
            ),
        ]
    )


@pytest.mark.parametrize("instruction", ["", "Keep replies concise"])
def test_policy_generation_returns_three_options_in_one_call(monkeypatch, instruction):
    from delivery_date import policy as module

    baseline = Policy(name="Control", prompt="Answer delivery questions.")
    scenario = to_editor(get_scenario("missing-date"), {})
    request = PolicyRequest(
        baseline=baseline, instruction=instruction, scenario=scenario
    )
    proposals = make_policy_options()
    model_calls = []
    requests = []

    def respond(messages, info):
        requests.append(messages)
        assert info.model_settings["max_tokens"] == 6000
        assert "Do not change scenarios" in info.instructions
        payload = next(
            part.content
            for message in messages
            for part in message.parts
            if isinstance(part, UserPromptPart)
        )
        assert json.loads(payload) == request.model_dump()
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, proposals.model_dump())]
        )

    def model(name, provider):
        model_calls.append(name)
        return FunctionModel(respond)

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(module, "OpenAIResponsesModel", model)
    monkeypatch.setattr(module, "OpenAIProvider", lambda **kwargs: kwargs)
    result = asyncio.run(module.propose_policy(request))
    assert result == proposals.model_dump() | {"model": module.MODEL}
    assert model_calls == [module.MODEL]
    assert len(requests) == 1
    assert baseline.prompt == "Answer delivery questions."
    assert request.scenario == scenario


@pytest.mark.parametrize("count", [0, 1, 2, 4])
def test_policy_options_require_three_candidates(count):
    proposal = make_policy_options().proposals[0]
    with pytest.raises(ValidationError):
        PolicyOptions(proposals=[proposal] * count)


@pytest.mark.parametrize(
    "changes",
    [
        {"name": "EVIDENCE FIRST"},
        {"name": "Evidence   first"},
        {"prompt": "Quote only shipping evidence."},
        {"prompt": "Quote only  shipping evidence.\n"},
    ],
)
def test_policy_options_reject_duplicate_names_and_prompts(changes):
    proposals = make_policy_options().model_dump()
    proposals["proposals"][1]["policy"].update(changes)
    with pytest.raises(ValidationError, match="distinct"):
        PolicyOptions.model_validate(proposals)


@pytest.mark.parametrize(
    "baseline_prompt",
    ["Quote only shipping evidence.", "Quote only  shipping evidence.\n"],
)
def test_policy_generation_rejects_unchanged_option(monkeypatch, baseline_prompt):
    from pydantic_ai.models.test import TestModel

    from delivery_date import policy as module

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(module, "OpenAIProvider", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        module,
        "OpenAIResponsesModel",
        lambda *args, **kwargs: TestModel(
            custom_output_args=make_policy_options().model_dump()
        ),
    )
    with pytest.raises(ValueError, match="Every proposal must change"):
        asyncio.run(
            module.propose_policy(
                PolicyRequest(baseline=Policy(name="Original", prompt=baseline_prompt))
            )
        )


def test_policy_generation_missing_key_makes_no_model_request(monkeypatch):
    from delivery_date import policy as module

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        asyncio.run(
            module.propose_policy(
                PolicyRequest(baseline=Policy(name="Original", prompt="Hello"))
            )
        )


def test_invalid_generated_options_are_rejected_without_another_model_call(monkeypatch):
    from pydantic_ai.exceptions import UnexpectedModelBehavior

    from delivery_date import policy as module

    output = make_policy_options().model_dump()
    output["proposals"][1] = output["proposals"][0]
    calls = []

    def respond(messages, info):
        calls.append(messages)
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, output)])

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(module, "OpenAIProvider", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        module, "OpenAIResponsesModel", lambda *args, **kwargs: FunctionModel(respond)
    )
    with pytest.raises(UnexpectedModelBehavior):
        asyncio.run(
            module.propose_policy(
                PolicyRequest(baseline=Policy(name="Original", prompt="Hello"))
            )
        )
    assert len(calls) == 1


def test_editor_runs_exact_reviewed_policy_and_records_it(monkeypatch):
    from types import SimpleNamespace
    from uuid import uuid4

    from delivery_date import simulation

    source_id, session_id = uuid4(), uuid4()
    source = asyncio.run(run_scenario(get_scenario("missing-date"))).scenario
    policy = Policy(name="Reviewed candidate", prompt="Use evidence and be concise.")
    calls = []

    async def load(client, selected_id):
        assert selected_id == source_id
        return source

    def respond(messages, info):
        assert info.instructions == policy.prompt
        return ModelResponse(
            parts=[TextPart("Delivery date unknown. Use tracking or contact support.")]
        )

    async def simulate(scenario, **kwargs):
        calls.append(kwargs)
        return await run_scenario(
            scenario,
            custom_policy=kwargs["custom_policy"],
            mode="tool-boundary",
            agent_model=FunctionModel(respond),
        )

    async def record(result, **kwargs):
        assert result.policy_prompt == policy.prompt
        assert result.policy_hash == policy.calculate_hash()
        assert kwargs["source_session_id"] == source_id
        return session_id

    monkeypatch.setattr(simulation, "load_execution_source", load)
    monkeypatch.setattr(simulation, "run_scenario", simulate)
    monkeypatch.setattr(simulation, "record_result", record)
    request = RunRequest(
        title="Candidate trial",
        sourceId=source_id,
        scenario=to_editor(source, {}),
        policy=policy,
    )
    displayed = asyncio.run(
        simulation.run(request, SimpleNamespace(base_url="http://example.test"))
    )
    assert calls[0]["custom_policy"] == policy
    assert displayed["policy"] == policy.model_dump()
    assert displayed["policyHash"] == policy.calculate_hash()
    assert displayed["sessionId"] == str(session_id)
