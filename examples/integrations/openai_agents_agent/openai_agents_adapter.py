"""Small OpenAI Agents SDK adapter example for Kitaru.

Run:
    uv sync --extra local --extra openai-agents
    uv run examples/integrations/openai_agents_agent/openai_agents_adapter.py
"""

import ast
import re
from typing import Any
from uuid import uuid4

from agents import Agent, RunConfig
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import ResponseOutputMessage, ResponseOutputText

from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    OpenAIRunResult,
)


class StaticTextModel(Model):
    """Minimal model stub so the example works without provider credentials."""

    def __init__(self, label: str) -> None:
        self._label = label
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(
            text=f"{self._label}: durable response #{self.call_count}",
            response_id=f"resp_{self._label}_{self.call_count}",
        )

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError("Streaming is not used in this example.")


def _text_response(text: str, *, response_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseOutputMessage(
                id=f"msg_{response_id}",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text=text,
                        type="output_text",
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            )
        ],
        usage=Usage(requests=1, input_tokens=4, output_tokens=6, total_tokens=10),
        response_id=response_id,
    )


def _extract_model_response_text(response: ModelResponse) -> str:
    for item in response.output:
        content = getattr(item, "content", None)
        if not isinstance(content, list):
            continue
        for part in content:
            text = getattr(part, "text", None)
            if isinstance(text, str) and text.strip():
                return text
    return str(response)


def _extract_final_output_from_envelope_text(text: str) -> str:
    match = re.search(r"final_output=(.+?)(?:\s\w+=|$)", text)
    if not match:
        return text
    raw = match.group(1).strip()
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw or text
    return str(parsed)


def _normalize_wait_output(value: Any) -> str:
    if isinstance(value, OpenAIRunResult):
        if value.status != "completed":
            raise RuntimeError(
                f"Expected a completed OpenAI run, got status={value.status!r}."
            )
        return str(value.final_output)
    if isinstance(value, ModelResponse):
        return _extract_model_response_text(value)
    text = str(value)
    return _extract_final_output_from_envelope_text(text)


def _run_flow(strategy: str) -> tuple[str, int]:
    model = StaticTextModel(label=strategy)
    agent = Agent(
        name=f"openai_agents_example_{strategy}_{uuid4().hex[:8]}",
        model=model,
    )
    runner = KitaruRunner(
        agent,
        checkpoint_strategy=strategy,
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def example_flow(prompt: str) -> str:
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    raw_output = example_flow.run(
        "Summarize why durable checkpoints are useful."
    ).wait()
    return _normalize_wait_output(raw_output), model.call_count


def main() -> None:
    calls_output, calls_count = _run_flow("calls")
    print("calls strategy output:", calls_output)
    print("calls strategy model calls:", calls_count)

    runner_call_output, runner_call_count = _run_flow("runner_call")
    print("runner_call strategy output:", runner_call_output)
    print("runner_call strategy model calls:", runner_call_count)


if __name__ == "__main__":
    main()
