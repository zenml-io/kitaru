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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Send chat requests to OpenAI-compatible endpoints."""

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from kitaru_model_swap.normalize import ToolCall, parse_arguments
from kitaru_model_swap.params import Candidate, ToolCallFormat

FailureCategory = Literal[
    "timeout",
    "rate_limited",
    "http_error",
    "empty_reply",
    "tool_call_parse_error",
    "refusal",
]

_RETRYABLE_STATUS = frozenset({408, 409, 429, 500, 502, 503, 504})
_HERMES_BLOCK = re.compile(r"<tool_call>\s*(.*?)\s*</tool_call>", re.DOTALL)


class MissingCredentialError(RuntimeError):
    """A candidate names an API key variable that is not set."""


@dataclass(frozen=True)
class Usage:
    """Token counts reported by the endpoint."""

    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0


@dataclass(frozen=True)
class CandidateReply:
    """The outcome of sending one request to one endpoint."""

    tool_calls: tuple[ToolCall, ...]
    text: str | None
    usage: Usage
    latency_seconds: float
    attempts: int
    failure: FailureCategory | None = None
    failure_detail: str | None = None


def get_api_key(candidate: Candidate) -> str | None:
    """Read a candidate's API key from the environment.

    Raises:
        MissingCredentialError: The named variable is not set.
    """
    if candidate.api_key_env is None:
        return None
    key = os.environ.get(candidate.api_key_env)
    if not key:
        raise MissingCredentialError(
            f"candidate {candidate.name!r} reads its API key from "
            f"{candidate.api_key_env}, which is not set on the worker. Add it "
            "to the analyzer's provider connection or the worker environment."
        )
    return key


def build_body(
    candidate: Candidate,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the chat-completions request body for a candidate."""
    settings = candidate.provider_settings
    body: dict[str, Any] = {"model": candidate.model, "messages": messages}
    if tools:
        body["tools"] = tools
    if settings.reasoning_effort is not None:
        body["reasoning_effort"] = settings.reasoning_effort
    if settings.max_tokens is not None:
        body[settings.max_tokens_field] = settings.max_tokens
    if settings.temperature is not None:
        body["temperature"] = settings.temperature
    body.update(settings.extra_body)
    return body


async def send(
    client: httpx.AsyncClient,
    candidate: Candidate,
    *,
    api_key: str | None,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
) -> CandidateReply:
    """Send one request, retrying transient failures, and classify the reply."""
    settings = candidate.provider_settings
    url = candidate.base_url.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    body = build_body(candidate, messages, tools)
    started = time.monotonic()
    attempts = 0
    failure: FailureCategory = "http_error"
    detail = ""
    while attempts <= settings.max_retries:
        attempts += 1
        try:
            response = await client.post(
                url, json=body, headers=headers, timeout=settings.timeout_seconds
            )
        except httpx.TimeoutException:
            failure, detail = "timeout", f"no reply in {settings.timeout_seconds}s"
        except httpx.HTTPError as error:
            failure, detail = "http_error", type(error).__name__
        else:
            if response.status_code == 200:
                return _classify(
                    response,
                    settings.tool_call_format,
                    latency=time.monotonic() - started,
                    attempts=attempts,
                )
            failure = "rate_limited" if response.status_code == 429 else "http_error"
            # Error bodies can echo the request, so only the status is kept.
            detail = f"HTTP {response.status_code}"
            if response.status_code not in _RETRYABLE_STATUS:
                break
        if attempts <= settings.max_retries:
            await asyncio.sleep(min(2.0**attempts, 10.0))
    return _failure(
        failure, detail, latency=time.monotonic() - started, attempts=attempts
    )


def _classify(
    response: httpx.Response,
    tool_call_format: ToolCallFormat,
    *,
    latency: float,
    attempts: int,
) -> CandidateReply:
    """Turn a successful HTTP response into a reply or a failure category."""
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        return _failure(
            "http_error",
            "the reply is not a JSON object",
            latency=latency,
            attempts=attempts,
        )
    usage = read_usage(payload.get("usage"))

    def failed(category: FailureCategory, detail: str) -> CandidateReply:
        return _failure(
            category, detail, latency=latency, attempts=attempts, usage=usage
        )

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return failed("empty_reply", "the reply has no choices")
    choice = _as_dict(choices[0])
    message = _as_dict(choice.get("message"))
    if message.get("refusal") or choice.get("finish_reason") == "content_filter":
        return failed("refusal", str(message.get("refusal") or "content filter"))
    text = message.get("content") if isinstance(message.get("content"), str) else None
    try:
        tool_calls, text = _read_tool_calls(message, text, tool_call_format)
    except ValueError as error:
        return failed("tool_call_parse_error", str(error))
    if not tool_calls and not (text and text.strip()):
        return failed("empty_reply", "the reply has no text or tool calls")
    return CandidateReply(
        tool_calls=tool_calls,
        text=text,
        usage=usage,
        latency_seconds=latency,
        attempts=attempts,
    )


def _failure(
    category: FailureCategory,
    detail: str,
    *,
    latency: float,
    attempts: int,
    usage: Usage | None = None,
) -> CandidateReply:
    """Build the reply for a failed request."""
    return CandidateReply(
        tool_calls=(),
        text=None,
        usage=usage or Usage(),
        latency_seconds=latency,
        attempts=attempts,
        failure=category,
        failure_detail=detail,
    )


def _as_dict(value: Any) -> dict[str, Any]:
    """Return a JSON object, or an empty one for any other value."""
    return value if isinstance(value, dict) else {}


def _read_tool_calls(
    message: dict[str, Any], text: str | None, tool_call_format: ToolCallFormat
) -> tuple[tuple[ToolCall, ...], str | None]:
    """Extract tool calls in the endpoint's format and return remaining text.

    Raises:
        ValueError: The tool calls cannot be parsed.
    """
    if tool_call_format == "native":
        calls = []
        for call in message.get("tool_calls") or ():
            function = call.get("function") if isinstance(call, dict) else None
            if not isinstance(function, dict) or not function.get("name"):
                raise ValueError("a tool call has no function name")
            arguments = parse_arguments(function.get("arguments") or "{}")
            if isinstance(arguments, str):
                raise ValueError(f"arguments of {function['name']} are not JSON")
            calls.append(ToolCall(str(function["name"]), arguments))
        return tuple(calls), text
    if text is None:
        return (), None
    if tool_call_format == "hermes":
        blocks = _HERMES_BLOCK.findall(text)
        if "<tool_call>" in text and not blocks:
            raise ValueError("unterminated <tool_call> block")
        calls = tuple(_parse_json_call(block) for block in blocks)
        remaining = _HERMES_BLOCK.sub("", text).strip() or None
        return calls, remaining
    stripped = text.strip()
    if not stripped.startswith("{"):
        return (), text
    return (_parse_json_call(stripped),), None


def _parse_json_call(raw: str) -> ToolCall:
    """Parse one `{"name", "arguments"}` JSON tool call.

    Raises:
        ValueError: The text is not such an object.
    """
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("a tool call is not valid JSON") from error
    if not isinstance(value, dict) or not isinstance(value.get("name"), str):
        raise ValueError("a tool call has no name")
    arguments = parse_arguments(value.get("arguments", value.get("parameters", {})))
    return ToolCall(value["name"], arguments)


def read_usage(usage: Any) -> Usage:
    """Read OpenAI-style usage, tolerating missing details."""
    if not isinstance(usage, dict):
        return Usage()
    prompt_details = usage.get("prompt_tokens_details") or {}
    completion_details = usage.get("completion_tokens_details") or {}
    return Usage(
        input_tokens=int(usage.get("prompt_tokens") or 0),
        cached_input_tokens=int(prompt_details.get("cached_tokens") or 0),
        output_tokens=int(usage.get("completion_tokens") or 0),
        reasoning_tokens=int(completion_details.get("reasoning_tokens") or 0),
    )
