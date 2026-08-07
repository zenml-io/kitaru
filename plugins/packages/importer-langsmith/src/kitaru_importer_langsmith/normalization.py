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
"""Shared normalization for official trace importers."""

import json
import re
from dataclasses import dataclass
from typing import Any

_FRAMEWORK_PATTERNS = (
    (re.compile(r"pydantic[._ -]?ai", re.IGNORECASE), "pydantic-ai"),
    (re.compile(r"langgraph", re.IGNORECASE), "langgraph"),
    (re.compile(r"openai[._ -]?agents?", re.IGNORECASE), "openai-agents"),
    (re.compile(r"google[._ -]?adk", re.IGNORECASE), "google-adk"),
    (
        re.compile(r"claude[._ -]?agent[._ -]?sdk|claudeagentsdk", re.IGNORECASE),
        "claude-agent-sdk",
    ),
)


@dataclass(frozen=True, slots=True)
class _TextMatch:
    """One text value and its RFC 9535 JSONPath selector."""

    selector: str
    text: str


def _get_child_selector(selector: str, key: str | int) -> str:
    """Append a child segment to a normalized JSONPath selector."""
    if isinstance(key, int):
        return f"{selector}[{key}]"
    return f"{selector}[{json.dumps(key, ensure_ascii=False)}]"


def _get_role(value: dict[str, Any]) -> str | None:
    """Return a normalized role from common message encodings."""
    candidates = [
        value.get("role"),
        value.get("type"),
        value.get("part_kind"),
        value.get("kind"),
    ]
    event_name = value.get("event.name")
    if isinstance(event_name, str):
        candidates.append(event_name.removeprefix("gen_ai.").removesuffix(".message"))
    identifier = value.get("id")
    if isinstance(identifier, list) and identifier:
        candidates.append(identifier[-1])
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        normalized = candidate.lower().replace("_", "-")
        if normalized in {"user", "human", "humanmessage", "user-prompt"}:
            return "user"
        if normalized in {"system", "systemmessage", "system-prompt"}:
            return "system"
        if normalized in {
            "assistant",
            "ai",
            "aimessage",
            "model",
            "response",
            "model-response",
        }:
            return "assistant"
    return None


def _get_content_match(
    value: Any, selector: str = "$", depth: int = 0
) -> _TextMatch | None:
    """Return a scalar text value and its selector from common content data."""
    if depth > 8:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return _TextMatch(selector=selector, text=stripped) if stripped else None
    if isinstance(value, list):
        matches = [
            match
            for index, item in enumerate(value)
            if (
                match := _get_content_match(
                    item, _get_child_selector(selector, index), depth + 1
                )
            )
            is not None
        ]
        return matches[-1] if matches else None
    if not isinstance(value, dict):
        return None
    for key in ("text", "content", "parts", "kwargs", "data"):
        if key in value and (
            match := _get_content_match(
                value[key], _get_child_selector(selector, key), depth + 1
            )
        ):
            return match
    return None


def _get_message_matches(
    value: Any, target_role: str, selector: str = "$", depth: int = 0
) -> list[_TextMatch]:
    """Collect scalar text and selectors for one nested message role."""
    if depth > 12:
        return []
    if isinstance(value, list):
        return [
            match
            for index, item in enumerate(value)
            for match in _get_message_matches(
                item,
                target_role,
                _get_child_selector(selector, index),
                depth + 1,
            )
        ]
    if not isinstance(value, dict):
        return []
    if _get_role(value) == target_role:
        for key in ("content", "text", "parts", "kwargs", "data"):
            if key in value and (
                match := _get_content_match(
                    value[key], _get_child_selector(selector, key), depth + 1
                )
            ):
                return [match]
    matches: list[_TextMatch] = []
    for key, child in value.items():
        matches.extend(
            _get_message_matches(
                child,
                target_role,
                _get_child_selector(selector, key),
                depth + 1,
            )
        )
    return matches


def get_input_text(value: Any) -> str | None:
    """Extract the primary user input text."""
    messages = _get_message_matches(value, "user")
    if messages:
        return messages[-1].text
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("prompt", "query", "question", "user_input", "message"):
            if key in value and (
                match := _get_content_match(value[key], _get_child_selector("$", key))
            ):
                return match.text
    return None


def get_output_text(value: Any) -> str | None:
    """Extract the primary assistant output text."""
    messages = _get_message_matches(value, "assistant")
    if messages:
        return messages[-1].text
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("answer", "result", "response", "output", "text", "content"):
            if key in value and (
                match := _get_content_match(value[key], _get_child_selector("$", key))
            ):
                return match.text
    return None


def _json_text(value: Any) -> str | None:
    """Serialize a structured tool value for display."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _get_system_prompt_match(value: Any) -> _TextMatch | None:
    """Return the latest system prompt and its selector from provider data."""
    messages = _get_message_matches(value, "system")
    if messages:
        return messages[-1]
    found: list[_TextMatch] = []

    def _collect(item: Any, selector: str = "$", depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                _collect(child, _get_child_selector(selector, index), depth + 1)
            return
        if not isinstance(item, dict):
            return
        for key in ("system_prompt", "system_instruction", "instructions"):
            if key in item and (
                match := _get_content_match(
                    item[key], _get_child_selector(selector, key), depth + 1
                )
            ):
                found.append(match)
        for key, child in item.items():
            _collect(child, _get_child_selector(selector, key), depth + 1)

    _collect(value)
    return found[-1] if found else None


def get_system_prompt(value: Any) -> str | None:
    """Extract the latest system prompt from provider data."""
    match = _get_system_prompt_match(value)
    return match.text if match is not None else None


def get_reasoning(value: Any) -> str | None:
    """Extract visible reasoning text from provider data."""
    found: list[str] = []

    def _collect(item: Any, depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(item, list):
            for child in item:
                _collect(child, depth + 1)
            return
        if not isinstance(item, dict):
            return
        kind_value = item.get("type") or item.get("part_kind")
        kind = str(kind_value).lower().replace("_", "-") if kind_value else ""
        if kind in {"reasoning", "reasoning-content", "thinking", "thought"}:
            for key in ("text", "content", "summary"):
                if key in item and (
                    match := _get_content_match(item[key], depth=depth + 1)
                ):
                    found.append(match.text)
        for key in ("reasoning", "reasoning_content", "thinking", "thought"):
            if key in item and (
                match := _get_content_match(item[key], depth=depth + 1)
            ):
                found.append(match.text)
        for child in item.values():
            _collect(child, depth + 1)

    _collect(value)
    return found[-1] if found else None


def detect_framework(value: Any) -> str | None:
    """Detect one supported framework from provider metadata."""
    evidence: list[str] = []

    def _collect(item: Any, depth: int = 0) -> None:
        if depth > 8 or len(evidence) >= 500:
            return
        if isinstance(item, dict):
            for key, child in item.items():
                evidence.append(str(key))
                _collect(child, depth + 1)
        elif isinstance(item, list):
            for child in item[:100]:
                _collect(child, depth + 1)
        elif isinstance(item, str):
            evidence.append(item)

    _collect(value)
    joined = "\n".join(evidence)
    matches = {
        framework
        for pattern, framework in _FRAMEWORK_PATTERNS
        if pattern.search(joined)
    }
    return next(iter(matches)) if len(matches) == 1 else None


def get_tool_payload_text(value: Any) -> str | None:
    """Return a displayable text representation of a tool payload."""
    return get_input_text(value) or _json_text(value)
