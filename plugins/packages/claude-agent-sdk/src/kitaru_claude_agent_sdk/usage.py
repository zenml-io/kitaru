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
"""Token usage mapping for Claude's public usage dictionaries."""

from collections.abc import Mapping
from typing import Any

from kitaru.api_models.v1.session import TokenUsage


def _get_integer(value: Mapping[str, Any], *names: str) -> int | None:
    """Read the first integer among alternate spellings of one usage field."""
    for name in names:
        candidate = value.get(name)
        if isinstance(candidate, int) and not isinstance(candidate, bool):
            return candidate
    return None


def _get_thinking_tokens(value: Mapping[str, Any]) -> int | None:
    """Read Claude's nested thinking count for Kitaru's reasoning field."""
    for name in ("output_tokens_details", "outputTokensDetails"):
        details = value.get(name)
        if isinstance(details, Mapping):
            return _get_integer(details, "thinking_tokens", "thinkingTokens")
    return None


def get_token_usage(value: Mapping[str, Any] | None) -> TokenUsage | None:
    """Map Claude's public usage dictionary to Kitaru token fields.

    Claude reports cache writes beside its input count, and Kitaru's input
    count includes them, while cache reads stay only in the cached field.

    Returns:
        Token usage, or None when Claude reported no usage.
    """
    if not value:
        return None

    input_tokens = _get_integer(value, "input_tokens", "inputTokens")
    cache_creation_tokens = _get_integer(
        value, "cache_creation_input_tokens", "cacheCreationInputTokens"
    )
    if cache_creation_tokens is not None:
        input_tokens = (input_tokens or 0) + cache_creation_tokens

    return TokenUsage(
        input_tokens=input_tokens,
        output_tokens=_get_integer(value, "output_tokens", "outputTokens"),
        cached_input_tokens=_get_integer(
            value, "cache_read_input_tokens", "cacheReadInputTokens"
        ),
        cache_write_input_tokens=cache_creation_tokens,
        reasoning_tokens=_get_thinking_tokens(value),
    )


def _get_remaining_tokens(total: int | None, recorded: list[int | None]) -> int | None:
    """Return how much of a total the recorded counts leave, never below zero."""
    if total is None:
        return None
    return max(total - sum(value or 0 for value in recorded), 0)


def get_remaining_usage(
    totals: TokenUsage, recorded: list[TokenUsage]
) -> TokenUsage | None:
    """Return the part of each total that the recorded usages leave uncovered.

    Returns:
        Remaining usage, or None when every remaining count is zero or unknown.
    """
    recorded_counts = [usage.model_dump() for usage in recorded]
    remainder = {
        name: _get_remaining_tokens(total, [counts[name] for counts in recorded_counts])
        for name, total in totals.model_dump().items()
    }
    return TokenUsage(**remainder) if any(remainder.values()) else None
