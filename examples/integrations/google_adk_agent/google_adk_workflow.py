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
from typing import Any
from uuid import uuid4

from examples.integrations.google_adk_agent.google_adk_adapter import (
    DEFAULT_USER_ID,
    _build_in_memory_runner,
    _create_runner_session,
    _function_call_content,
    _import_adk_module,
    _load_adk_api,
    _load_kitaru_adk_adapter,
    _text_content,
)
from kitaru import flow

DEFAULT_APP_NAME = "kitaru_google_adk_workflow"
DEFAULT_SESSION_PREFIX = "workflow-session"
DEFAULT_PROMPT = "Please run the approved durable lookup."
DEFAULT_QUERY = "cats"
WORKFLOW_MARKER = "workflow-local-cat-fact"


def _function_response_names(llm_request: Any) -> list[str]:
    names: list[str] = []
    for content in getattr(llm_request, "contents", []) or []:
        for part in getattr(content, "parts", []) or []:
            function_response = getattr(part, "function_response", None)
            if function_response is not None and function_response.name:
                names.append(function_response.name)
    return names


def _build_confirmed_local_agent(api: Any, adapter: Any, *, query: str) -> Any:
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")

    class ConfirmationLoopLlm(api.BaseLlm):  # type: ignore[misc, valid-type]
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
            if "durable_lookup" in _function_response_names(llm_request):
                yield api.LlmResponse(
                    content=_text_content(
                        api,
                        f"final workflow answer: {WORKFLOW_MARKER} for {query}",
                        role="model",
                    )
                )
                return

            yield api.LlmResponse(
                content=_function_call_content(
                    api,
                    name="durable_lookup",
                    args={"query": query},
                )
            )

    physical_tool_executions: list[str] = []

    def durable_lookup(query: str) -> dict[str, Any]:
        physical_tool_executions.append(query)
        return {"query": query, "answer": WORKFLOW_MARKER}

    confirmed_tool = function_tool_module.FunctionTool(
        durable_lookup,
        require_confirmation=True,
    )
    wrapped_model = adapter.KitaruADKModel(ConfirmationLoopLlm())
    wrapped_tool = adapter.KitaruADKTool(confirmed_tool, name="durable_lookup")
    agent = api.LlmAgent(
        name="kitaru_local_adk_workflow_agent",
        model=wrapped_model,
        tools=[wrapped_tool],
    )
    return agent, physical_tool_executions


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


@flow(cache=True)
def google_adk_workflow(
    query: str = DEFAULT_QUERY,
    *,
    prompt: str = DEFAULT_PROMPT,
    app_name: str = DEFAULT_APP_NAME,
    user_id: str = DEFAULT_USER_ID,
    session_id: str | None = None,
    approval_decision: bool | None = True,
) -> dict[str, Any]:
    """Run a local Google ADK agent with replayable model/tool checkpoints."""
    stable_session_id = session_id or f"{DEFAULT_SESSION_PREFIX}-{uuid4().hex}"
    return asyncio.run(
        _run_adk_workflow_turns(
            query=query,
            prompt=prompt,
            app_name=app_name,
            user_id=user_id,
            session_id=stable_session_id,
            approval_decision=approval_decision,
        )
    )


def run_workflow(
    *,
    query: str = DEFAULT_QUERY,
    approval_decision: bool | None = True,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Submit the example flow locally and return the structured output."""
    return google_adk_workflow.run(
        query,
        approval_decision=approval_decision,
        session_id=session_id,
    ).wait()


def _json_block(value: Mapping[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _print_result(result: Mapping[str, Any]) -> None:
    print("\n=== What happened ===")
    print(
        "A Kitaru flow ran a real Google ADK in-memory runner. ADK asked for "
        "tool confirmation, Kitaru built the follow-up response, ADK ran the "
        "wrapped tool, and the flow returned structured output."
    )
    print("\n=== Workflow result ===")
    print(_json_block(result))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run a persisted Kitaru workflow around a local Google ADK agent.")
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"Deterministic lookup query. Defaults to {DEFAULT_QUERY}.",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Stable ADK session id. Defaults to a random workflow-session-* id.",
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
    approval_decision = None if args.interactive_wait else not args.deny
    result = run_workflow(
        query=str(args.query),
        approval_decision=approval_decision,
        session_id=args.session_id,
    )
    _print_result(result)


if __name__ == "__main__":
    main()
