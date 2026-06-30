"""Persisted Google ADK workflow example using Kitaru calls mode.

Run the deterministic local workflow from an isolated ADK environment:
    UV_PROJECT_ENVIRONMENT=.venv-google-adk \
      uv run --python 3.12 --no-dev --extra google-adk \
      python examples/integrations/google_adk_agent/google_adk_workflow.py
"""

import argparse
import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from typing import Any, Literal
from uuid import uuid4

if __package__:
    from .google_adk_adapter import (
        DEFAULT_LIVE_MODEL,
        DEFAULT_USER_ID,
        _build_in_memory_runner,
        _create_runner_session,
        _function_call_content,
        _import_adk_module,
        _load_adk_api,
        _load_kitaru_adk_adapter,
        _text_content,
        prepare_live_google_credentials,
    )
else:
    from google_adk_adapter import (
        DEFAULT_LIVE_MODEL,
        DEFAULT_USER_ID,
        _build_in_memory_runner,
        _create_runner_session,
        _function_call_content,
        _import_adk_module,
        _load_adk_api,
        _load_kitaru_adk_adapter,
        _text_content,
        prepare_live_google_credentials,
    )
from kitaru import flow

DEFAULT_APP_NAME = "kitaru_google_adk_workflow"
LIVE_APP_NAME = "kitaru_google_adk_live_workflow"
DEFAULT_SESSION_PREFIX = "workflow-session"
DEFAULT_PROMPT = (
    "Use the available tools to calculate (97 * 31) + 42, then label the "
    "answer for the requested topic."
)
DEFAULT_LIVE_WORKFLOW_PROMPT = (
    "Use the tools to calculate (97 * 31) + 42. Call multiply_numbers first "
    "with left=97 and right=31, then call add_offset with the product and "
    "offset=42. Return the final answer as workflow-tool-calculation=<number> "
    "for the topic label."
)
DEFAULT_QUERY = "cats"
WORKFLOW_MARKER = "workflow-tool-calculation"
MULTIPLY_TOOL_NAME = "multiply_numbers"
ADD_OFFSET_TOOL_NAME = "add_offset"
MULTIPLY_LEFT = 97
MULTIPLY_RIGHT = 31
ADD_OFFSET = 42
EXPECTED_PRODUCT = MULTIPLY_LEFT * MULTIPLY_RIGHT
EXPECTED_TOTAL = EXPECTED_PRODUCT + ADD_OFFSET
WorkflowMode = Literal["local", "live"]


def _function_response_payloads(llm_request: Any) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for content in getattr(llm_request, "contents", []) or []:
        for part in getattr(content, "parts", []) or []:
            function_response = getattr(part, "function_response", None)
            if function_response is None or not function_response.name:
                continue
            payload = getattr(function_response, "response", None)
            payloads[str(function_response.name)] = payload
    return payloads


def _payload_value(payload: Any, key: str) -> Any:
    if isinstance(payload, Mapping) and key in payload:
        return payload[key]
    if isinstance(payload, Mapping) and isinstance(payload.get("result"), Mapping):
        nested = payload["result"]
        if key in nested:
            return nested[key]
    raise RuntimeError(
        f"ADK function response is missing required {key!r}: {payload!r}"
    )


def _multiply_numbers(left: int, right: int) -> dict[str, Any]:
    product = left * right
    return {"left": left, "right": right, "product": product}


def _add_offset(subtotal: int, offset: int) -> dict[str, Any]:
    total = subtotal + offset
    return {"subtotal": subtotal, "offset": offset, "total": total}


def _validate_workflow_options(
    *, mode: str, approval_decision: bool | None
) -> WorkflowMode:
    if mode not in ("local", "live"):
        raise ValueError(f"Unsupported workflow mode: {mode!r}")
    if mode == "live" and approval_decision is not True:
        raise ValueError("approval_decision only applies to mode='local'.")
    return mode


def _build_confirmed_local_agent(api: Any, adapter: Any, *, query: str) -> Any:
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")

    class CalculationLoopLlm(api.BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-local-adk-workflow-model")
            object.__setattr__(self, "calls", [])

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-local-adk-workflow-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            self.calls.append(llm_request)
            responses = _function_response_payloads(llm_request)
            if ADD_OFFSET_TOOL_NAME in responses:
                total = _payload_value(responses[ADD_OFFSET_TOOL_NAME], "total")
                yield api.LlmResponse(
                    content=_text_content(
                        api,
                        f"final workflow answer: {WORKFLOW_MARKER}={total} for {query}",
                        role="model",
                    )
                )
                return
            if MULTIPLY_TOOL_NAME in responses:
                product = _payload_value(responses[MULTIPLY_TOOL_NAME], "product")
                yield api.LlmResponse(
                    content=_function_call_content(
                        api,
                        name=ADD_OFFSET_TOOL_NAME,
                        args={"subtotal": product, "offset": ADD_OFFSET},
                    )
                )
                return

            yield api.LlmResponse(
                content=_function_call_content(
                    api,
                    name=MULTIPLY_TOOL_NAME,
                    args={"left": MULTIPLY_LEFT, "right": MULTIPLY_RIGHT},
                )
            )

    physical_tool_executions: list[str] = []

    def multiply_numbers(left: int, right: int) -> dict[str, Any]:
        result = _multiply_numbers(left, right)
        physical_tool_executions.append(
            f"{MULTIPLY_TOOL_NAME}:{left}x{right}={result['product']}"
        )
        return result

    def add_offset(subtotal: int, offset: int) -> dict[str, Any]:
        result = _add_offset(subtotal, offset)
        physical_tool_executions.append(
            f"{ADD_OFFSET_TOOL_NAME}:{subtotal}+{offset}={result['total']}"
        )
        return result

    multiply_tool = function_tool_module.FunctionTool(
        multiply_numbers,
        require_confirmation=True,
    )
    add_tool = function_tool_module.FunctionTool(add_offset)
    wrapped_model = adapter.KitaruADKModel(CalculationLoopLlm())
    wrapped_tools = [
        adapter.KitaruADKTool(multiply_tool, name=MULTIPLY_TOOL_NAME),
        adapter.KitaruADKTool(add_tool, name=ADD_OFFSET_TOOL_NAME),
    ]
    agent = api.LlmAgent(
        name="kitaru_local_adk_workflow_agent",
        model=wrapped_model,
        tools=wrapped_tools,
    )
    return agent, physical_tool_executions


def _build_live_tool_agent(api: Any, adapter: Any, *, model: str) -> Any:
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")

    wrapped_tools = [
        adapter.KitaruADKTool(
            function_tool_module.FunctionTool(_multiply_numbers),
            name=MULTIPLY_TOOL_NAME,
        ),
        adapter.KitaruADKTool(
            function_tool_module.FunctionTool(_add_offset),
            name=ADD_OFFSET_TOOL_NAME,
        ),
    ]
    return api.LlmAgent(
        name="kitaru_live_adk_workflow_agent",
        model=model,
        instruction=(
            "You are running inside a Kitaru flow. For arithmetic tasks, call "
            "the provided tools instead of calculating mentally. First call "
            f"{MULTIPLY_TOOL_NAME}, then call {ADD_OFFSET_TOOL_NAME} with the product."
        ),
        tools=wrapped_tools,
    )


def _event_summary(result: Any) -> dict[str, Any]:
    kinds: list[str] = []
    checkpoint_names: list[str] = []
    for event in result.events:
        kind = event.get("kind")
        if isinstance(kind, str):
            kinds.append(kind)
        checkpoint_name = event.get("checkpoint_name")
        if isinstance(checkpoint_name, str):
            checkpoint_names.append(checkpoint_name)
    return {
        "event_count": len(result.events),
        "handoff_count": len(result.handoffs),
        "tracked_event_kinds": kinds,
        "checkpoint_names": checkpoint_names,
    }


def _status_history(*results: Any) -> list[str]:
    return [str(result.status) for result in results]


async def _run_adk_workflow_turns(
    *,
    query: str,
    prompt: str,
    app_name: str,
    user_id: str,
    session_id: str,
    approval_decision: bool | None,
) -> dict[str, Any]:
    api = _load_adk_api()
    adapter = _load_kitaru_adk_adapter()
    agent, physical_tool_executions = _build_confirmed_local_agent(
        api, adapter, query=query
    )
    runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
    await _create_runner_session(
        runner,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )

    call_policy = adapter.ADKCallCheckpointPolicy(
        model_checkpoint_config={"cache": True},
        tool_checkpoint_config={"cache": True},
        persist_run_artifacts=False,
    )
    kitaru_runner = adapter.KitaruADKRunner(
        runner,
        name=app_name,
        checkpoint_strategy="calls",
        call_checkpoint_policy=call_policy,
    )
    first_request = adapter.ADKRunRequest(
        user_id=user_id,
        session_id=session_id,
        message=_text_content(api, f"{prompt}\nLookup query: {query}", role="user"),
        metadata={"example": "google_adk_workflow", "phase": "first_turn"},
    )
    first = await kitaru_runner.run(first_request)

    decision_source = "not_needed"
    second = None
    if first.status == "requires_action":
        if approval_decision is None:
            decision_source = "kitaru_wait"
            followup = adapter.wait_for_tool_confirmation(
                first,
                user_id=user_id,
                session_id=session_id,
                metadata={"example": "google_adk_workflow"},
            )
        else:
            decision_source = "injected_decision"
            followup = adapter.build_tool_confirmation_request(
                first.handoffs[0],
                confirmed=approval_decision,
                user_id=user_id,
                session_id=session_id,
                metadata={"example": "google_adk_workflow"},
            )
        second = await kitaru_runner.run(followup)

    final = second or first
    preview = adapter.final_output_preview(final.final_output)
    return {
        "mode": "local",
        "status": final.status,
        "final_answer": preview,
        "human_decision_happened": first.status == "requires_action",
        "approval_decision": approval_decision,
        "approval_source": decision_source,
        "checkpoint_strategy": "calls",
        "physical_tool_executions": list(physical_tool_executions),
        "status_history": _status_history(first, final),
        "first_turn": _event_summary(first),
        "final_turn": _event_summary(final),
        "warnings": list(first.warnings) + list(final.warnings),
    }


async def _run_live_adk_workflow_turn(
    *,
    query: str,
    prompt: str,
    app_name: str,
    user_id: str,
    session_id: str,
    model: str,
) -> dict[str, Any]:
    prepare_live_google_credentials()
    api = _load_adk_api()
    adapter = _load_kitaru_adk_adapter()
    agent = _build_live_tool_agent(api, adapter, model=model)
    runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
    await _create_runner_session(
        runner,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )

    kitaru_runner = adapter.KitaruADKRunner(
        runner,
        name=app_name,
        checkpoint_strategy="runner_call",
    )
    request = adapter.ADKRunRequest(
        user_id=user_id,
        session_id=session_id,
        message=_text_content(api, f"{prompt}\nTopic label: {query}", role="user"),
        metadata={"example": "google_adk_workflow", "mode": "live"},
    )
    result = await kitaru_runner.run(request)
    usage = result.usage.model_dump(mode="python") if result.usage else None
    return {
        "mode": "live",
        "status": result.status,
        "final_answer": adapter.final_output_preview(result.final_output),
        "checkpoint_strategy": "runner_call",
        "model": model,
        "usage": usage,
        "turn": _event_summary(result),
        "warnings": list(result.warnings),
    }


@flow(cache=True)
def google_adk_workflow(
    query: str = DEFAULT_QUERY,
    *,
    mode: str = "local",
    prompt: str | None = None,
    app_name: str | None = None,
    user_id: str = DEFAULT_USER_ID,
    session_id: str | None = None,
    approval_decision: bool | None = True,
    model: str = DEFAULT_LIVE_MODEL,
) -> dict[str, Any]:
    """Run a Google ADK agent inside a persisted Kitaru workflow."""
    validated_mode = _validate_workflow_options(
        mode=mode,
        approval_decision=approval_decision,
    )
    stable_session_id = session_id or f"{DEFAULT_SESSION_PREFIX}-{uuid4().hex}"
    if validated_mode == "live":
        return asyncio.run(
            _run_live_adk_workflow_turn(
                query=query,
                prompt=prompt or DEFAULT_LIVE_WORKFLOW_PROMPT,
                app_name=app_name or LIVE_APP_NAME,
                user_id=user_id,
                session_id=stable_session_id,
                model=model,
            )
        )
    return asyncio.run(
        _run_adk_workflow_turns(
            query=query,
            prompt=prompt or DEFAULT_PROMPT,
            app_name=app_name or DEFAULT_APP_NAME,
            user_id=user_id,
            session_id=stable_session_id,
            approval_decision=approval_decision,
        )
    )


def run_workflow(
    *,
    query: str = DEFAULT_QUERY,
    mode: WorkflowMode = "local",
    prompt: str | None = None,
    approval_decision: bool | None = True,
    session_id: str | None = None,
    model: str = DEFAULT_LIVE_MODEL,
) -> dict[str, Any]:
    """Submit the example flow locally and return the structured output."""
    validated_mode = _validate_workflow_options(
        mode=mode,
        approval_decision=approval_decision,
    )
    return google_adk_workflow.run(
        query,
        mode=validated_mode,
        prompt=prompt,
        approval_decision=approval_decision,
        session_id=session_id,
        model=model,
    ).wait()


def _json_block(value: Mapping[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _print_result(result: Mapping[str, Any]) -> None:
    print("\n=== What happened ===")
    if result.get("mode") == "live":
        print(
            "A Kitaru flow ran Gemini through Google ADK with two calculation "
            "tools. Kitaru checkpointed the whole ADK runner turn and returned "
            "structured output from inside the flow."
        )
    else:
        print(
            "A Kitaru flow ran a real Google ADK in-memory runner. The local "
            "LLM chose a confirmed multiplication tool, then an addition tool, "
            "and the flow returned structured output."
        )
    print("\n=== Workflow result ===")
    print(_json_block(result))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a persisted Kitaru workflow around a Google ADK agent."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("local", "live"),
        default="local",
        help=(
            "local uses a deterministic in-process ADK model; live uses Gemini "
            "or Vertex AI through ADK. Defaults to local."
        ),
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"Topic label for the final answer. Defaults to {DEFAULT_QUERY}.",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Stable ADK session id. Defaults to a random workflow-session-* id.",
    )
    parser.add_argument(
        "--prompt",
        default=None,
        help="Override the default workflow prompt.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_LIVE_MODEL,
        help=f"Gemini/Vertex model for --mode live. Defaults to {DEFAULT_LIVE_MODEL}.",
    )
    parser.add_argument(
        "--deny",
        action="store_true",
        help="Inject a deterministic denial instead of approval.",
    )
    parser.add_argument(
        "--interactive-wait",
        action="store_true",
        help=(
            "Pause the Kitaru flow with wait_for_tool_confirmation(...) instead "
            "of injecting a deterministic decision."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.mode == "live" and (args.deny or args.interactive_wait):
        raise SystemExit("--deny and --interactive-wait only apply to --mode local.")

    approval_decision = None if args.interactive_wait else not args.deny
    result = run_workflow(
        query=str(args.query),
        mode=args.mode,
        prompt=args.prompt,
        approval_decision=approval_decision,
        session_id=args.session_id,
        model=args.model,
    )
    _print_result(result)


if __name__ == "__main__":
    main()
