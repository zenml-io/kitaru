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
import math
import os
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
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
    "truncated",
]

_MAX_DETAIL = 200
_RETRYABLE_STATUS = frozenset({408, 409, 429, 500, 502, 503, 504})
_HERMES_BLOCK = re.compile(r"<tool_call>\s*(.*?)\s*</tool_call>", re.DOTALL)
# Errors raised before the request reached the provider, so nothing was billed.
_NOT_SENT = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)


class MissingCredentialError(RuntimeError):
    """A candidate names an API key variable that is not set."""


@dataclass(frozen=True)
class Usage:
    """Token counts reported by the endpoint.

    `reported` is False when the endpoint gave no usage, so the counts are
    unknown rather than measured zeros. It does not take part in equality.
    """

    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    reported: bool = field(default=True, compare=False)


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
    unmetered_attempts: int = 0
    """Attempts the provider may have billed without reporting usage: a
    timeout or broken connection after the request was sent, or a reply
    without usage."""


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
    controls: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the chat-completions request body for a candidate.

    `controls` are the recorded call's `tool_choice`, `parallel_tool_calls`
    and `response_format`, already in chat-completions form.
    """
    settings = candidate.provider_settings
    body: dict[str, Any] = {"model": candidate.model, "messages": messages}
    if tools:
        body["tools"] = tools
    body.update(controls or {})
    if settings.reasoning_effort is not None:
        body["reasoning_effort"] = settings.reasoning_effort
    if settings.max_tokens is not None:
        body[settings.max_tokens_field] = settings.max_tokens
    if settings.temperature is not None:
        body["temperature"] = settings.temperature
    body.update(settings.extra_body)
    return body


def build_responses_body(
    candidate: Candidate,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    controls: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an OpenAI Responses API request from a chat-completions request.

    System and developer messages become `instructions`; assistant tool calls
    and tool results become `function_call` and `function_call_output` items.
    Nothing is stored on OpenAI's side.
    """
    settings = candidate.provider_settings
    instructions = []
    items: list[dict[str, Any]] = []
    for message in messages:
        role = message.get("role")
        text = _text(message.get("content"))
        if role in {"system", "developer"}:
            instructions.append(text)
        elif role == "tool":
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": message.get("tool_call_id"),
                    "output": text,
                }
            )
        else:
            if text:
                items.append({"role": role, "content": text})
            for call in message.get("tool_calls") or ():
                function = _as_dict(_as_dict(call).get("function"))
                arguments = function.get("arguments")
                items.append(
                    {
                        "type": "function_call",
                        "call_id": call.get("id"),
                        "name": function.get("name"),
                        "arguments": arguments
                        if isinstance(arguments, str)
                        else json.dumps(arguments or {}),
                    }
                )
    body: dict[str, Any] = {"model": candidate.model, "input": items, "store": False}
    if instructions:
        body["instructions"] = "\n\n".join(instructions)
    if tools:
        # The Responses API makes functions strict unless told otherwise,
        # while chat completions does not, so the recorded flag is explicit.
        body["tools"] = [
            {"type": "function", "strict": False, **_as_dict(tool.get("function"))}
            for tool in tools
        ]
    controls = controls or {}
    if "tool_choice" in controls:
        choice = controls["tool_choice"]
        body["tool_choice"] = (
            {"type": "function", "name": choice["function"]["name"]}
            if isinstance(choice, dict)
            else choice
        )
    if "parallel_tool_calls" in controls:
        body["parallel_tool_calls"] = controls["parallel_tool_calls"]
    text = dict(settings.extra_body.get("text") or {})
    if "response_format" in controls:
        text["format"] = _responses_format(controls["response_format"])
    if settings.reasoning_effort is not None:
        body["reasoning"] = {"effort": settings.reasoning_effort}
    if settings.max_tokens is not None:
        body["max_output_tokens"] = settings.max_tokens
    if settings.temperature is not None:
        body["temperature"] = settings.temperature
    body.update(settings.extra_body)
    if text:
        body["text"] = text
    return body


def _responses_format(response_format: Mapping[str, Any]) -> dict[str, Any]:
    """Convert a chat-completions `response_format` to a Responses `text.format`."""
    if response_format["type"] != "json_schema":
        return {"type": response_format["type"]}
    return {"type": "json_schema", **response_format["json_schema"]}


def _text(content: Any) -> str:
    """Flatten chat message content (a string or text parts) to a string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(part.get("text", ""))
            for part in content
            if isinstance(part, dict) and part.get("type") in {"text", "input_text"}
        )
    return ""


def replace_system_prompt(
    messages: list[dict[str, Any]], prompt: str
) -> list[dict[str, Any]]:
    """Replace the recorded system prompt, or add one when there is none.

    Every system or developer message is replaced by a single system message
    at the start, so the candidate sees only the prompt under test.
    """
    rest = [m for m in messages if m.get("role") not in {"system", "developer"}]
    return [{"role": "system", "content": prompt}, *rest]


async def send(
    client: httpx.AsyncClient,
    candidate: Candidate,
    *,
    api_key: str | None,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    controls: Mapping[str, Any] | None = None,
    may_retry: Callable[[], Awaitable[bool]] | None = None,
) -> CandidateReply:
    """Send one request, retrying transient failures, and classify the reply.

    `may_retry` is asked before each retry, so a retry can be refused when
    the spend cap has no room for another attempt.
    """
    settings = candidate.provider_settings
    responses = settings.api == "responses"
    url = candidate.base_url.rstrip("/") + (
        "/responses" if responses else "/chat/completions"
    )
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    body = (build_responses_body if responses else build_body)(
        candidate, messages, tools, controls
    )
    started = time.monotonic()
    attempts = 0
    unmetered = 0
    failure: FailureCategory = "http_error"
    detail = ""
    while attempts <= settings.max_retries:
        attempts += 1
        try:
            response = await client.post(
                url, json=body, headers=headers, timeout=settings.timeout_seconds
            )
        except httpx.TimeoutException as error:
            failure, detail = "timeout", f"no reply in {settings.timeout_seconds}s"
            unmetered += not isinstance(error, _NOT_SENT)
        except httpx.HTTPError as error:
            failure, detail = "http_error", type(error).__name__
            unmetered += not isinstance(error, _NOT_SENT)
        else:
            if response.status_code == 200:
                classify = _classify_responses if responses else _classify
                try:
                    reply = classify(
                        response,
                        settings.tool_call_format,
                        latency=time.monotonic() - started,
                        attempts=attempts,
                    )
                # Replies come from third-party endpoints, so a shape the
                # parsers do not expect is one failed call, not a failed run.
                except (TypeError, AttributeError, KeyError, ValueError) as error:
                    reply = _failure(
                        "tool_call_parse_error",
                        f"malformed reply: {error}",
                        latency=time.monotonic() - started,
                        attempts=attempts,
                    )
                return replace(
                    reply,
                    unmetered_attempts=unmetered + (not reply.usage.reported),
                )
            failure = "rate_limited" if response.status_code == 429 else "http_error"
            detail = f"HTTP {response.status_code}" + _error_message(response)
            if response.status_code not in _RETRYABLE_STATUS:
                break
        if attempts <= settings.max_retries:
            if may_retry is not None and not await may_retry():
                detail += "; not retried, the spend cap has no room"
                break
            await asyncio.sleep(min(2.0**attempts, 10.0))
    return replace(
        _failure(
            failure, detail, latency=time.monotonic() - started, attempts=attempts
        ),
        unmetered_attempts=unmetered,
    )


def _error_message(response: httpx.Response) -> str:
    """Return the provider's error message, which says how to fix the request.

    Whole error bodies can echo the request, so only the `error.message`
    field of a JSON body is kept; `_failure` bounds it.
    """
    try:
        payload = response.json()
    except ValueError:
        return ""
    error = payload.get("error") if isinstance(payload, dict) else None
    message = error.get("message") if isinstance(error, dict) else error
    if not isinstance(message, str) or not message.strip():
        return ""
    return ": " + " ".join(message.split())


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
    # A cut-off reply would otherwise count as a disagreement, mistaking a
    # token limit for the model's choice.
    if choice.get("finish_reason") == "length":
        return failed("truncated", "the reply hit the output token limit")
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


def _classify_responses(
    response: httpx.Response,
    _tool_call_format: ToolCallFormat,
    *,
    latency: float,
    attempts: int,
) -> CandidateReply:
    """Turn a successful Responses API reply into a reply or a failure."""
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

    if payload.get("status") == "incomplete":
        reason = _as_dict(payload.get("incomplete_details")).get("reason")
        if reason == "max_output_tokens":
            return failed("truncated", "the reply hit the output token limit")
        if reason == "content_filter":
            return failed("refusal", "content filter")
        return failed("empty_reply", f"the reply is incomplete: {reason}")
    calls: list[ToolCall] = []
    texts: list[str] = []
    output = payload.get("output")
    for item in output if isinstance(output, list) else ():
        item = _as_dict(item)
        if item.get("type") == "function_call":
            if not item.get("name"):
                return failed("tool_call_parse_error", "a tool call has no name")
            arguments = parse_arguments(item.get("arguments") or "{}")
            if not isinstance(arguments, dict):
                return failed(
                    "tool_call_parse_error",
                    f"arguments of {item['name']} are not a JSON object",
                )
            calls.append(ToolCall(str(item["name"]), arguments))
        elif item.get("type") == "message":
            for part in item.get("content") or ():
                part = _as_dict(part)
                if part.get("type") == "refusal":
                    return failed("refusal", str(part.get("refusal") or "refusal"))
                if part.get("type") == "output_text":
                    texts.append(str(part.get("text") or ""))
    text = "".join(texts) or None
    if not calls and not (text and text.strip()):
        return failed("empty_reply", "the reply has no text or tool calls")
    return CandidateReply(
        tool_calls=tuple(calls),
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
    """Build the reply for a failed request; without usage, usage is unknown."""
    return CandidateReply(
        tool_calls=(),
        text=None,
        usage=usage or Usage(reported=False),
        latency_seconds=latency,
        attempts=attempts,
        failure=category,
        # Refusal text comes from the endpoint, so it is bounded before it is
        # stored in the report.
        failure_detail=detail[:_MAX_DETAIL],
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
        raw_calls = message.get("tool_calls") or []
        if not isinstance(raw_calls, list):
            raise ValueError("tool_calls is not a list")
        calls = []
        for call in raw_calls:
            function = call.get("function") if isinstance(call, dict) else None
            if not isinstance(function, dict) or not function.get("name"):
                raise ValueError("a tool call has no function name")
            calls.append(
                ToolCall(
                    str(function["name"]),
                    _object_arguments(function["name"], function.get("arguments")),
                )
            )
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
    arguments = value.get("arguments", value.get("parameters"))
    return ToolCall(value["name"], _object_arguments(value["name"], arguments))


def _object_arguments(name: Any, arguments: Any) -> dict[str, Any]:
    """Parse tool-call arguments, which must be a JSON object.

    Raises:
        ValueError: The arguments are not a JSON object, so the call could
            not be executed.
    """
    parsed = parse_arguments(arguments if arguments not in (None, "") else "{}")
    if not isinstance(parsed, dict):
        raise ValueError(f"arguments of {name} are not a JSON object")
    return parsed


def read_usage(usage: Any) -> Usage:
    """Read OpenAI-style usage, tolerating missing details.

    Chat completions report `prompt_tokens` and `completion_tokens`; the
    Responses API reports `input_tokens` and `output_tokens`.
    """
    if not isinstance(usage, dict):
        return Usage(reported=False)
    if "input_tokens" in usage or "output_tokens" in usage:
        if not all(
            _is_token_count(usage.get(key)) for key in ("input_tokens", "output_tokens")
        ):
            return Usage(reported=False)
        input_details = _as_dict(usage.get("input_tokens_details"))
        output_details = _as_dict(usage.get("output_tokens_details"))
        return Usage(
            input_tokens=_count(usage.get("input_tokens")),
            cached_input_tokens=_count(input_details.get("cached_tokens")),
            output_tokens=_count(usage.get("output_tokens")),
            reasoning_tokens=_count(output_details.get("reasoning_tokens")),
        )
    if not all(
        _is_token_count(usage.get(key))
        for key in ("prompt_tokens", "completion_tokens")
    ):
        return Usage(reported=False)
    prompt_details = _as_dict(usage.get("prompt_tokens_details"))
    completion_details = _as_dict(usage.get("completion_tokens_details"))
    return Usage(
        input_tokens=_count(usage.get("prompt_tokens")),
        cached_input_tokens=_count(prompt_details.get("cached_tokens")),
        output_tokens=_count(usage.get("completion_tokens")),
        reasoning_tokens=_count(completion_details.get("reasoning_tokens")),
    )


def _is_token_count(value: Any) -> bool:
    """Return whether a provider supplied a usable nonnegative token count."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    return (
        isinstance(value, float)
        and math.isfinite(value)
        and value >= 0
        and value.is_integer()
    )


def _count(value: Any) -> int:
    """Read a token count; the endpoint's value must not lower the spend."""
    if not _is_token_count(value):
        return 0
    return int(value)
