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
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    model_validator,
)

ToolCallFormat = Literal["native", "hermes", "json_content"]

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class _Params(BaseModel):
    # Parameters are stored with the task, and a key pasted into the wrong
    # field must not be echoed back in a validation error.
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


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


OriginField = Literal["lab", "lab_country", "host", "host_region"]


class Origin(_Params):
    """Where a model comes from and where it runs, as the user describes it."""

    lab: str | None = Field(default=None, description="Lab that trained it.")
    lab_country: str | None = Field(
        default=None, description="Country of the lab, for example `US` or `CN`."
    )
    host: str | None = Field(default=None, description="Provider that serves it.")
    host_region: str | None = Field(
        default=None, description="Where it is served, for example `US` or `EU`."
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
    origin: Origin | None = Field(
        default=None,
        description="Lab and hosting details, for provenance rules.",
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
    def _check_endpoint(self) -> Self:
        """Keep credentials out of URLs and off unencrypted remote connections.

        Returns:
            The validated candidate.
        """
        check_endpoint(self.base_url, self.api_key_env)
        return self


DecisionService = Literal["jev", "clef", "openai_decisions"]

# Where each decision service answers, which variable usually holds its key,
# and which model it runs. Clef runs per Cloudflare account, so it has no
# default URL and therefore no default key either.
_DECISION_DEFAULTS: dict[str, dict[str, str | None]] = {
    "jev": {
        "base_url": "https://api.typesafe.ai",
        "api_key_env": "TYPESAFE_API_KEY",
        "model": "jev-latest",
    },
    "clef": {
        "base_url": None,
        "api_key_env": None,
        "model": "clef",
    },
    "openai_decisions": {
        "base_url": "https://api.openai.com/v1",
        "api_key_env": "OPENAI_API_KEY",
        "model": "gpt-6-luna",
    },
}


class DecisionModel(_Params):
    """A decision model: it answers typed questions with probabilities.

    Decision models pick from options instead of generating text, so they
    answer quickly, charge only for input, and say how sure they are.
    """

    kind: DecisionService = Field(
        description=(
            "`jev` (TypeSafe), `clef` (Cloudflare Workers AI), or "
            "`openai_decisions` (OpenAI Decisions API)."
        )
    )
    base_url: str = Field(
        description=(
            "Service root. Defaults per kind; for `clef`, the Workers AI run URL "
            "`https://api.cloudflare.com/client/v4/accounts/ACCOUNT_ID/ai/run/"
            "@cf/cloudflare/clef`."
        )
    )
    model: str = Field(min_length=1, description="Model; defaults per kind.")
    api_key_env: str = Field(description="Variable holding the key; defaults per kind.")
    price: Price | None = Field(
        default=None, description="Token prices, needed with `max_spend_usd`."
    )
    timeout_seconds: float = Field(default=30, gt=0)

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, value: Any) -> Any:
        """Fill the URL, key variable and model the service usually uses.

        A default key variable is only paired with the default URL, so a key
        is never sent to a URL the caller typed without naming the key.

        Raises:
            ValueError: A Clef judge has no URL, or a custom URL has no key
                variable.

        Returns:
            The values with defaults filled in.
        """
        if not isinstance(value, dict) or value.get("kind") not in _DECISION_DEFAULTS:
            return value
        given = {k: v for k, v in value.items() if v is not None}
        defaults = _DECISION_DEFAULTS[value["kind"]]
        if "base_url" in given and given["base_url"] != defaults["base_url"]:
            defaults = {**defaults, "api_key_env": None}
        filled = {k: v for k, v in defaults.items() if v is not None} | given
        if "base_url" not in filled:
            raise ValueError(
                "a clef decision model needs base_url, the Workers AI run URL "
                "for your account"
            )
        if "api_key_env" not in filled:
            raise ValueError(
                "a decision model with its own base_url needs api_key_env naming "
                "its key"
            )
        return filled

    @model_validator(mode="after")
    def _check_endpoint(self) -> Self:
        """Apply the same URL and key rules as candidates.

        Returns:
            The validated judge.
        """
        check_endpoint(self.base_url, self.api_key_env)
        return self


class DecisionJudge(DecisionModel):
    """A decision model that rules whether two answers are equivalent."""

    equivalent_at: float = Field(
        default=0.8,
        gt=0.5,
        le=1.0,
        description=(
            "Probability at or above which answers count as equivalent; at or "
            "below one minus this they count as different, and in between as "
            "unsure."
        ),
    )


class DecisionCandidate(DecisionModel):
    """A decision model tried in place of the model's decisions.

    It answers the decision parts of each recorded call: which tool to call
    (or to answer in text) and which value each enum or boolean argument
    takes. Parts that need generated text or free values are left to an LLM.
    """

    name: str = Field(min_length=1, max_length=80, description="Report label.")


def check_endpoint(base_url: str, api_key_env: str | None) -> None:
    """Keep credentials out of URLs and off unencrypted remote connections.

    Raises:
        ValueError: The URL carries credentials or is not http(s), the key
            variable is not a variable name or is a Kitaru variable, or a key
            would be sent to a remote host over plain http.
    """
    url = urlsplit(base_url)
    if url.scheme not in {"http", "https"} or not url.hostname:
        raise ValueError("base_url must be an http(s) URL")
    if url.username or url.password:
        raise ValueError("base_url must not contain credentials")
    if api_key_env is None:
        return
    if not _ENV_NAME.match(api_key_env):
        raise ValueError(
            "api_key_env must name an environment variable, not hold a key"
        )
    # Kitaru variables hold the worker's own credentials for the server.
    if api_key_env.upper().startswith("KITARU_"):
        raise ValueError("api_key_env must not name a KITARU_ variable")
    if url.scheme != "https" and url.hostname not in _LOCAL_HOSTS:
        raise ValueError("an API key is only sent over https to remote hosts")


def _judge_kind(value: Any) -> str:
    """Tell a decision judge (it has `kind`) from a chat-model judge."""
    if isinstance(value, dict):
        return "decision" if "kind" in value else "chat"
    return "decision" if isinstance(value, DecisionJudge) else "chat"


class ModelSwapParams(_Params):
    """Parameters of the model-swap analyzer."""

    candidates: list[Candidate] = Field(default_factory=list, max_length=8)
    decision_candidates: list[DecisionCandidate] = Field(
        default_factory=list,
        max_length=4,
        description=(
            "Decision models that answer the decision parts of each call, to "
            "find which calls could move off an LLM."
        ),
    )
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
    judge: (
        Annotated[
            Annotated[DecisionJudge, Tag("decision")]
            | Annotated[Candidate, Tag("chat")],
            Discriminator(_judge_kind),
        ]
        | None
    ) = Field(
        default=None,
        description=(
            "Optional judge asked whether differing answers or arguments are "
            "equivalent: a decision model (`kind`) or an OpenAI-compatible chat "
            "model. Its verdicts are reported next to strict agreement and "
            "never replace the user's."
        ),
    )
    max_disagreements: int = Field(default=200, ge=0, le=500)
    min_mix_calls: int = Field(
        default=20,
        ge=1,
        le=1000,
        description=(
            "Answered calls a candidate needs on a step before the best mix "
            "can move that step to it; below this the step stays on today's "
            "model."
        ),
    )
    repeats: int = Field(
        default=1,
        ge=1,
        le=5,
        description=(
            "Times each call is sent to each candidate. Above 1, the report "
            "shows how often any and all attempts match, to read noise."
        ),
    )
    excluded_origins: dict[OriginField, list[str]] = Field(
        default_factory=dict,
        description=(
            "Origins a candidate must not have, for example "
            '`{"lab_country": ["CN"]}`. Excluded candidates are still '
            "compared but never recommended."
        ),
    )

    def is_excluded(self, candidate: Candidate) -> bool:
        """Tell whether a candidate's declared origin is excluded."""
        origin = candidate.origin
        if origin is None:
            return False
        values: dict[str, str | None] = {
            "lab": origin.lab,
            "lab_country": origin.lab_country,
            "host": origin.host,
            "host_region": origin.host_region,
        }
        return any(
            (value := values[field]) is not None
            and value.casefold() in {item.casefold() for item in excluded}
            for field, excluded in self.excluded_origins.items()
        )

    @model_validator(mode="after")
    def _check_candidates(self) -> Self:
        """Require a candidate, unique names, and at most one reference.

        Raises:
            ValueError: There is no candidate, names repeat, or several
                candidates are references.

        Returns:
            The validated parameters.
        """
        if not self.candidates and not self.decision_candidates:
            raise ValueError("give at least one of candidates or decision_candidates")
        names = [c.name for c in self.candidates] + [
            c.name for c in self.decision_candidates
        ]
        if len(names) != len(set(names)):
            raise ValueError("candidate names must be unique")
        if sum(candidate.reference for candidate in self.candidates) > 1:
            raise ValueError("at most one candidate can be the reference")
        return self
