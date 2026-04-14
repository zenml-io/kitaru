"""Manual two-turn tool-calling loop with ``kitaru.llm()``.

This example is deliberately mock-safe: it uses Kitaru's structured LLM mock
environment variable so you can run it without provider credentials. The model
call shape is the same as a real provider call, though. To use a real model,
remove the ``mock_llm_response(...)`` blocks and keep the same messages/tools
loop.
"""

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import kitaru
from kitaru import LLMResponse, LLMToolCall, LLMToolDefinition, checkpoint, flow

MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE_JSON"
MODEL = "openai/gpt-5-nano"

SEARCH_TOOL = LLMToolDefinition(
    name="search_documents",
    description="Search the project notes by query.",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query to run against the notes.",
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    },
)


@contextmanager
def mock_llm_response(response: dict[str, Any]) -> Iterator[None]:
    """Temporarily provide a structured mock response for one LLM call."""
    previous = os.environ.get(MOCK_RESPONSE_ENV)
    os.environ[MOCK_RESPONSE_ENV] = json.dumps(response)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(MOCK_RESPONSE_ENV, None)
        else:
            os.environ[MOCK_RESPONSE_ENV] = previous


def search_documents(query: str) -> str:
    """A tiny local tool used by the manual loop."""
    notes = {
        "kitaru llm": "kitaru.llm() returns LLMResponse with content, usage, "
        "finish_reason, and tool_calls.",
        "tool loop": "Kitaru returns tool-call intents, while user code owns "
        "tool execution and the next model turn.",
    }
    query_words = set(query.lower().split())
    matches = [text for key, text in notes.items() if query_words & set(key.split())]
    return "\n".join(matches) or "No matching notes found."


def assistant_message(response: LLMResponse) -> dict[str, Any]:
    """Convert an ``LLMResponse`` into a canonical assistant history message."""
    message: dict[str, Any] = {"role": "assistant"}
    if response.content is not None:
        message["content"] = response.content
    if response.tool_calls:
        message["tool_calls"] = [call.model_dump() for call in response.tool_calls]
    return message


def run_tool_call(tool_call: LLMToolCall) -> str:
    """Execute the single local tool used in this example."""
    if tool_call.name != "search_documents":
        raise ValueError(f"Unsupported tool: {tool_call.name}")
    query = (tool_call.arguments or {}).get("query")
    if not isinstance(query, str):
        raise ValueError("search_documents requires a string `query` argument")
    return search_documents(query)


@checkpoint
def answer_with_manual_tool_loop(question: str) -> str:
    """Ask for a tool call, execute it locally, then ask for a final answer."""
    messages: list[dict[str, Any]] = [{"role": "user", "content": question}]

    with mock_llm_response(
        {
            "content": None,
            "tool_calls": [
                {
                    "id": "call_search_documents_1",
                    "name": "search_documents",
                    "arguments_json": json.dumps({"query": "kitaru llm tool loop"}),
                    "arguments": {"query": "kitaru llm tool loop"},
                }
            ],
            "finish_reason": "tool_calls",
            "provider_finish_reason": "tool_calls",
        }
    ):
        first = kitaru.llm(
            messages,
            model=MODEL,
            tools=[SEARCH_TOOL],
            tool_choice="auto",
            name="tool_request",
        )

    messages.append(assistant_message(first))
    for tool_call in first.tool_calls:
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": run_tool_call(tool_call),
            }
        )

    with mock_llm_response(
        {
            "content": "Kitaru tracks the model turns, returns tool-call "
            "intents, and leaves tool execution under your control.",
            "finish_reason": "completed",
            "provider_finish_reason": "stop",
            "usage": {
                "prompt_tokens": 42,
                "completion_tokens": 18,
                "total_tokens": 60,
            },
        }
    ):
        second = kitaru.llm(
            messages,
            model=MODEL,
            tools=[SEARCH_TOOL],
            name="final_answer",
        )

    return second.content or ""


@flow
def manual_tool_loop(question: str) -> str:
    """Run the mock-safe manual tool-calling example."""
    return answer_with_manual_tool_loop(question)


def run_workflow(
    question: str = "How should I use kitaru.llm() for a tool loop?",
) -> tuple[str, str]:
    """Run the example workflow.

    Returns:
        Tuple of (execution_id, final answer text).
    """
    handle = manual_tool_loop.run(question)
    result = handle.wait()
    return handle.exec_id, result


def main() -> None:
    """Run the example as a script."""
    execution_id, result = run_workflow()
    print(f"Execution: {execution_id}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
