#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Deterministic mock agent with lifecycle hooks."""

import ast
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MODEL = "mock-gpt-4"
DEFAULT_SYSTEM_PROMPT = "You are a helpful assistant."

ToolExecutor = Callable[[str, dict[str, Any]], Any]
ToolInterceptor = Callable[[str, dict[str, Any], ToolExecutor], Any]


def _digest(payload: Any) -> str:
    """Hash a JSON-serializable payload into a stable hex digest."""
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def get_weather(city: str) -> dict[str, Any]:
    """Return deterministic fake weather for a city."""
    digest = _digest({"city": city})
    conditions = ("sunny", "cloudy", "rainy", "windy")[int(digest[2], 16) % 4]
    return {
        "city": city,
        "temperature_c": int(digest[:2], 16) % 35,
        "conditions": conditions,
    }


def calculate(expression: str) -> dict[str, Any]:
    """Evaluate a basic arithmetic expression."""
    return {"expression": expression, "result": _evaluate(expression)}


def _evaluate(expression: str) -> float:
    """Evaluate an arithmetic expression restricted to basic operators."""
    return _evaluate_node(ast.parse(expression, mode="eval").body)


def _evaluate_node(node: ast.expr) -> float:
    """Evaluate one node of a parsed arithmetic expression."""
    if isinstance(node, ast.Constant) and isinstance(node.value, int | float):
        return float(node.value)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_evaluate_node(node.operand)
    if isinstance(node, ast.BinOp):
        left = _evaluate_node(node.left)
        right = _evaluate_node(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
    raise ValueError(f"Unsupported expression: {ast.dump(node)}")


DEFAULT_TOOLS: dict[str, Callable[..., Any]] = {
    "get_weather": get_weather,
    "calculate": calculate,
}


@dataclass(frozen=True)
class LLMCallStep:
    """LLM call scenario step."""

    name: str = "chat"


@dataclass(frozen=True)
class ToolCallStep:
    """Tool call scenario step."""

    tool: str
    arguments: dict[str, Any]


Step = LLMCallStep | ToolCallStep


def default_scenario() -> list[Step]:
    """Build the default plan, tools, answer scenario."""
    return [
        LLMCallStep(name="plan"),
        ToolCallStep(tool="get_weather", arguments={"city": "Berlin"}),
        ToolCallStep(tool="calculate", arguments={"expression": "21 * 2"}),
        LLMCallStep(name="answer"),
    ]


@dataclass
class LLMCall:
    """LLM call hook payload."""

    name: str
    model: str
    messages: list[dict[str, str]]
    model_params: dict[str, Any]
    output: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost: float | None = None


@dataclass
class ToolCall:
    """Tool call hook payload."""

    tool_name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: str | None = None


class AgentHooks:
    """No-op agent lifecycle callbacks."""

    def on_run_start(self, inputs: Any) -> None:
        """Handle the start of a run."""

    def on_llm_call_start(self, call: LLMCall) -> None:
        """Handle the start of an LLM call."""

    def on_llm_call_end(self, call: LLMCall) -> None:
        """Handle the end of an LLM call."""

    def on_tool_call_start(self, call: ToolCall) -> None:
        """Handle the start of a tool call."""

    def on_tool_call_end(self, call: ToolCall) -> None:
        """Handle the end of a tool call."""

    def on_run_end(self, outputs: Any) -> None:
        """Handle the successful end of a run."""

    def on_run_error(self, error: BaseException) -> None:
        """Handle a run failure."""


class MockAgent:
    """Deterministic agent simulating LLM and tool calls without a network."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
        model_params: dict[str, Any] | None = None,
        scenario: list[Step] | None = None,
        tools: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Initialize the agent."""
        self.model = model
        self.system_prompt = system_prompt
        self.model_params = dict(model_params or {"temperature": 0.0})
        self.scenario = list(scenario) if scenario is not None else default_scenario()
        self.tools = dict(tools if tools is not None else DEFAULT_TOOLS)
        self.hooks: list[AgentHooks] = []
        self.tool_interceptor: ToolInterceptor | None = None

    def register_hooks(self, hooks: AgentHooks) -> None:
        """Subscribe a hook set to the agent lifecycle."""
        self.hooks.append(hooks)

    def configure(
        self,
        model: str | None = None,
        system_prompt: str | None = None,
        model_params: dict[str, Any] | None = None,
    ) -> None:
        """Override configuration values, keeping unset ones unchanged."""
        if model is not None:
            self.model = model
        if system_prompt is not None:
            self.system_prompt = system_prompt
        if model_params is not None:
            self.model_params = dict(model_params)

    def run(self, inputs: Any) -> str | None:
        """Execute the scenario and return the final LLM output."""
        for hooks in self.hooks:
            hooks.on_run_start(inputs)
        try:
            transcript: list[dict[str, str]] = []
            output: str | None = None
            for index, step in enumerate(self.scenario):
                if isinstance(step, LLMCallStep):
                    output = self._llm_call(index, step, inputs, transcript)
                else:
                    result = self._tool_call(step)
                    transcript.append(
                        {
                            "role": "tool",
                            "name": step.tool,
                            "content": json.dumps(result, sort_keys=True, default=str),
                        }
                    )
        except BaseException as error:
            for hooks in self.hooks:
                hooks.on_run_error(error)
            raise
        for hooks in self.hooks:
            hooks.on_run_end(output)
        return output

    def _llm_call(
        self,
        index: int,
        step: LLMCallStep,
        inputs: Any,
        transcript: list[dict[str, str]],
    ) -> str:
        """Simulate one LLM call as a pure function of the agent state."""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {
                "role": "user",
                "content": json.dumps(inputs, sort_keys=True, default=str),
            },
            *transcript,
        ]
        call = LLMCall(
            name=step.name,
            model=self.model,
            messages=messages,
            model_params=dict(self.model_params),
        )
        for hooks in self.hooks:
            hooks.on_llm_call_start(call)
        signature = _digest(
            {
                "model": self.model,
                "model_params": self.model_params,
                "messages": messages,
                "step": index,
            }
        )
        verbosity = int(signature[:2], 16) % 4
        details = " ".join(
            f"fact-{signature[4 + 2 * position : 6 + 2 * position]}"
            for position in range(verbosity)
        )
        output = f"[{self.model}] {step.name} #{index}: {signature[:12]}"
        if details:
            output = f"{output} {details}"
        prompt_characters = sum(len(message["content"]) for message in messages)
        rate = (int(_digest({"model": self.model})[:4], 16) % 900 + 100) / 1e8
        call.output = output
        call.input_tokens = 10 + prompt_characters // 4
        call.output_tokens = len(output) // 4
        call.cost = round((call.input_tokens + call.output_tokens) * rate, 8)
        for hooks in self.hooks:
            hooks.on_llm_call_end(call)
        return output

    def _tool_call(self, step: ToolCallStep) -> Any:
        """Execute one tool call through the interceptor seam."""
        call = ToolCall(tool_name=step.tool, arguments=dict(step.arguments))
        for hooks in self.hooks:
            hooks.on_tool_call_start(call)
        try:
            if self.tool_interceptor is not None:
                call.result = self.tool_interceptor(
                    call.tool_name, call.arguments, self.execute_tool
                )
            else:
                call.result = self.execute_tool(call.tool_name, call.arguments)
        except BaseException as error:
            call.error = str(error)
            for hooks in self.hooks:
                hooks.on_tool_call_end(call)
            raise
        for hooks in self.hooks:
            hooks.on_tool_call_end(call)
        return call.result

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Execute a registered tool with its arguments."""
        tool = self.tools.get(tool_name)
        if tool is None:
            raise ValueError(f"Unknown tool '{tool_name}'")
        return tool(**arguments)
