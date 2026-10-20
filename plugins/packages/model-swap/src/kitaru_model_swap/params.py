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
"""Validated analyzer parameters."""

import re
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

ToolCallFormat = Literal["native", "hermes", "json_content"]

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Price(_Params):
    """Token prices in USD per million tokens."""

    input_per_mtok: Decimal = Field(ge=0, description="Uncached input price.")
    output_per_mtok: Decimal = Field(
        ge=0, description="Output price, including reasoning tokens."
    )
    cached_input_per_mtok: Decimal | None = Field(
        default=None,
        ge=0,
        description="Cached input price; the input price when omitted.",
    )


class ProviderSettings(_Params):
    """Request settings that differ between providers serving a model."""

    tool_call_format: ToolCallFormat = Field(
        default="native",
        description=(
            "Where the endpoint returns tool calls: `native` reads the "
            "`tool_calls` field, `hermes` parses `<tool_call>` JSON blocks "
            "from the text, and `json_content` parses the whole text as one "
            '`{"name", "arguments"}` object.'
        ),
    )
    reasoning_effort: str | None = Field(
        default=None, description="Value sent as `reasoning_effort`."
    )
    max_tokens: int | None = Field(
        default=None, gt=0, description="Output token limit for each call."
    )
    max_tokens_field: Literal["max_completion_tokens", "max_tokens"] = Field(
        default="max_completion_tokens",
        description="Request field that carries `max_tokens`.",
    )
    temperature: float | None = Field(default=None, ge=0)
    extra_body: dict[str, Any] = Field(
        default_factory=dict,
        description="Extra request fields merged into every request body.",
    )
    timeout_seconds: float = Field(default=120, gt=0)
    max_retries: int = Field(
        default=1,
        ge=0,
        le=5,
        description="Retries after a timeout, rate limit or server error.",
    )


class Candidate(_Params):
    """One OpenAI-compatible model to send recorded calls to."""

    name: str = Field(min_length=1, max_length=80, description="Report label.")
    base_url: str = Field(
        min_length=1, description="Endpoint base URL, for example `.../v1`."
    )
    model: str = Field(min_length=1, description="Model name sent in requests.")
    api_key_env: str | None = Field(
        default=None,
        description=(
            "Name of the environment variable that holds the API key. Omit "
            "for endpoints without authentication, such as a local Ollama."
        ),
    )
    reference: bool = Field(
        default=False,
        description=(
            "Mark the model the agent uses today. It is compared like any "
            "other candidate, and its agreement with its own recording shows "
            "how much answers vary from run to run."
        ),
    )
    open_weights: bool | None = Field(
        default=None,
        description=(
            "Whether the model's weights are open, so it can be run by any "
            "provider or on your own hardware."
        ),
    )
    price: Price | None = Field(
        default=None, description="Token prices; looked up when omitted."
    )
    price_provider: str | None = Field(
        default=None,
        description="Provider id for the bundled price lookup, such as `openai`.",
    )
    provider_settings: ProviderSettings = Field(default_factory=ProviderSettings)

    @model_validator(mode="after")
    def _check_env_name(self) -> Self:
        """Reject an API key passed where a variable name belongs.

        Raises:
            ValueError: The value is not an environment variable name.

        Returns:
            The validated candidate.
        """
        if self.api_key_env is not None and not _ENV_NAME.match(self.api_key_env):
            raise ValueError(
                "api_key_env must name an environment variable, not hold a key"
            )
        return self


class ModelSwapParams(_Params):
    """Parameters of the model-swap analyzer."""

    candidates: list[Candidate] = Field(min_length=1, max_length=8)
    max_calls: int = Field(
        default=100, ge=1, le=2000, description="Recorded calls to sample."
    )
    dry_run: bool = Field(
        default=False, description="Estimate calls and spend without sending any."
    )
    max_spend_usd: Decimal | None = Field(
        default=None,
        gt=0,
        description="Stop sending calls before total estimated spend exceeds this.",
    )
    tool_schemas: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Tool name to JSON schema of its parameters.",
    )
    tool_descriptions: dict[str, str] = Field(default_factory=dict)
    use_recorded_tools: bool = Field(
        default=True,
        description="Use tool definitions recorded on the call.",
    )
    infer_missing_schemas: bool = Field(
        default=False,
        description=(
            "Infer a loose schema from recorded arguments when a tool has no "
            "schema. Such calls are reported apart from the headline numbers."
        ),
    )
    concurrency: int = Field(default=4, ge=1, le=32)
    time_budget_seconds: float = Field(
        default=600,
        gt=0,
        le=840,
        description=(
            "Stop sending calls after this many seconds so the analyzer "
            "returns its results before the task timeout."
        ),
    )
    sessions_per_month: float | None = Field(
        default=None,
        gt=0,
        description="Traffic used for monthly projections; derived when omitted.",
    )
    judge: Candidate | None = Field(
        default=None,
        description=(
            "Optional model asked whether differing answers or arguments are "
            "equivalent. Its verdicts are reported next to strict agreement."
        ),
    )
    max_disagreements: int = Field(default=200, ge=0, le=1000)

    @model_validator(mode="after")
    def _check_candidates(self) -> Self:
        """Require unique candidate names and at most one reference.

        Raises:
            ValueError: Names repeat or several candidates are references.

        Returns:
            The validated parameters.
        """
        names = [candidate.name for candidate in self.candidates]
        if len(names) != len(set(names)):
            raise ValueError("candidate names must be unique")
        if sum(candidate.reference for candidate in self.candidates) > 1:
            raise ValueError("at most one candidate can be the reference")
        return self
