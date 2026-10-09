"""Mocked OpenAI conversation requests and CLI connection selection."""

import asyncio
import json
import sys
from uuid import uuid4

import httpx
import pytest
from pydantic_ai.providers.openai import OpenAIProvider

from delivery_date import __main__ as cli
from delivery_date import persistence, runner
from delivery_date.models import get_scenario


def test_missing_openai_key_has_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="Set OPENAI_API_KEY"):
        runner.make_openai_model("gpt-5-nano")


@pytest.mark.parametrize("model", ["gpt-5-nano", "gpt-4o-mini"])
def test_openai_customer_loop_uses_tools_and_private_context(
    monkeypatch: pytest.MonkeyPatch, model: str
) -> None:
    requests = []
    agent_calls = customer_calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal agent_calls, customer_calls
        body = json.loads(request.content)
        requests.append((request, body))
        tools = body["tools"]
        customer = tools[0]["function"]["name"] != "check_shipping"
        if customer:
            customer_calls += 1
            payload = json.loads(body["messages"][-1]["content"])
            assert set(payload) == {
                "visible_transcript",
                "customer_known_facts",
                "policy",
                "goal",
                "phase",
                "acceptance_criteria",
                "agent_turns_remaining",
            }
            assert "estimated_delivery" not in json.dumps(payload)
            assert "no_unsupported_date" not in json.dumps(payload)
            output = (
                {
                    "next_message": "Please give me tracking and support instructions.",
                }
                if customer_calls == 1
                else {}
            )
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"customer-{customer_calls}",
                        "type": "function",
                        "function": {
                            "name": tools[0 if customer_calls == 1 else 1]["function"][
                                "name"
                            ],
                            "arguments": json.dumps(output),
                        },
                    }
                ],
            }
        else:
            instructions = " ".join(
                m["content"]
                for m in body["messages"]
                if m["role"] in {"system", "developer"}
            )
            assert (
                "does not provide the method used to calculate the estimate"
                in instructions
            )
            assert (
                "Do not present generic shipping factors as the actual calculation for this order"
                in instructions
            )
            agent_calls += 1
            if agent_calls == 1:
                message = {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "shipping-1",
                            "type": "function",
                            "function": {
                                "name": "check_shipping",
                                "arguments": '{"order_id":"ORDER-1042"}',
                            },
                        }
                    ],
                }
            else:
                tool_result = next(m for m in body["messages"] if m["role"] == "tool")
                assert json.loads(tool_result["content"])["estimated_delivery"] is None
                message = {
                    "role": "assistant",
                    "content": "The delivery date is unknown. Tracking: https://tracking.example.test/ORDER-1042. Contact customer support for escalation.",
                }
        return httpx.Response(
            200,
            json={
                "id": f"chat-{len(requests)}",
                "object": "chat.completion",
                "created": 1,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": "tool_calls"
                        if "tool_calls" in message
                        else "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 10,
                    "total_tokens": 20,
                },
            },
        )

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:

            def provider(**kwargs):
                return OpenAIProvider(**kwargs, http_client=client)

            monkeypatch.setattr(runner, "OpenAIProvider", provider)
            monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
            monkeypatch.setenv(
                "OPENAI_BASE_URL", "https://must-not-receive-key.example"
            )
            result = await runner.run_scenario(
                get_scenario("missing-date"), backend="openai", model=model
            )
            assert result.status == "completed", result.error
            assert result.backend == "openai"
            assert result.model_name == model
            assert result.tool_calls == 1
            assert result.agent_turns == 2
            assert result.model_requests == 5
            assert result.verdict.passed
            assert "test-openai-key" not in result.model_dump_json()

    asyncio.run(exercise())
    assert agent_calls == 3
    assert customer_calls == 2
    for request, body in requests:
        assert request.url.host == "api.openai.com"
        assert request.url.path == "/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer test-openai-key"
        assert body["model"] == model
        assert "max_tokens" not in body
        assert body["max_completion_tokens"] == 2048
        if model == "gpt-5-nano":
            assert body["reasoning_effort"] == "minimal"
        else:
            assert "reasoning_effort" not in body


@pytest.mark.parametrize("server_url", [None, "https://explicit.example"])
def test_cli_defaults_to_nano_and_delegates_server_selection(
    monkeypatch: pytest.MonkeyPatch, server_url: str | None
) -> None:
    calls = []
    recordings = []
    original = runner.run_scenario

    async def simulate(scenario, **kwargs):
        calls.append(kwargs.copy())
        kwargs["backend"] = "scripted"
        return await original(scenario, **kwargs)

    async def record(result, **kwargs):
        recordings.append(kwargs)
        return uuid4()

    arguments = [
        "delivery_date",
        "--scenario",
        "missing-date",
        "--variant",
        "fix",
        "--record",
    ]
    if server_url is not None:
        arguments += ["--server-url", server_url]
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(cli, "run_scenario", simulate)
    monkeypatch.setattr(persistence, "record_result", record)
    assert asyncio.run(cli.main()) == 0
    assert calls[0]["backend"] == "openai"
    assert calls[0]["model"] == "gpt-5-nano"
    assert recordings[0]["server_url"] == server_url
