"""Public serializable models for the Gemini Interactions adapter."""

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.checkpoint import _raise_if_checkpoint_output_handle

from ._constants import ANTIGRAVITY_AGENT_ID

GeminiInteractionKind = Literal["start", "resume", "function_result", "poll"]
GeminiInteractionInput = str | dict[str, Any] | list[dict[str, Any]] | None


class GeminiInteractionRequest(BaseModel):
    """Serializable input to one Google/Gemini Interactions API turn."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = 1
    kind: GeminiInteractionKind
    input: GeminiInteractionInput = None
    model: str | None = None
    agent: str | None = None
    previous_interaction_id: str | None = None
    interaction_id: str | None = None
    function_call_id: str | None = None
    function_name: str | None = None
    function_result_payload: Any | None = Field(default=None, alias="function_result")
    environment: str | dict[str, Any] | None = None
    tools: list[dict[str, Any]] = Field(default_factory=list)
    system_instruction: str | None = None
    generation_config: dict[str, Any] = Field(default_factory=dict)
    response_format: dict[str, Any] | None = None
    background: bool = False
    store: bool = True
    timeout_s: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def target_kind(self) -> Literal["agent", "model", "poll"]:
        """Return the Gemini target family for metadata and analytics."""
        if self.kind == "poll":
            return "poll"
        return "agent" if self.agent is not None else "model"

    @property
    def has_function_result_payload(self) -> bool:
        """Return whether this request explicitly carries a function result."""
        return "function_result_payload" in self.model_fields_set

    @property
    def has_function_result_only_fields(self) -> bool:
        """Return whether any function-result-only field was explicit."""
        return any(
            field_name in self.model_fields_set
            for field_name in (
                "function_call_id",
                "function_name",
                "function_result_payload",
            )
        )

    @classmethod
    def start(
        cls,
        input: str | dict[str, Any] | list[dict[str, Any]],
        *,
        model: str | None = None,
        agent: str | None = None,
        environment: str | dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        system_instruction: str | None = None,
        generation_config: dict[str, Any] | None = None,
        response_format: dict[str, Any] | None = None,
        background: bool = False,
        store: bool = True,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        """Build a request for a fresh interaction."""
        return cls._create(
            kind="start",
            input=input,
            model=model,
            agent=agent,
            environment=environment,
            tools=tools,
            system_instruction=system_instruction,
            generation_config=generation_config,
            response_format=response_format,
            background=background,
            store=store,
            timeout_s=timeout_s,
            metadata=metadata,
        )

    @classmethod
    def resume(
        cls,
        input: str | dict[str, Any] | list[dict[str, Any]],
        previous_interaction_id: str,
        *,
        model: str | None = None,
        agent: str | None = None,
        environment: str | dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        system_instruction: str | None = None,
        generation_config: dict[str, Any] | None = None,
        response_format: dict[str, Any] | None = None,
        background: bool = False,
        store: bool = True,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        """Build a request that continues server-side interaction history."""
        return cls._create(
            kind="resume",
            input=input,
            model=model,
            agent=agent,
            previous_interaction_id=previous_interaction_id,
            environment=environment,
            tools=tools,
            system_instruction=system_instruction,
            generation_config=generation_config,
            response_format=response_format,
            background=background,
            store=store,
            timeout_s=timeout_s,
            metadata=metadata,
        )

    @classmethod
    def function_result(
        cls,
        *,
        previous_interaction_id: str,
        function_call_id: str,
        function_result: Any,
        function_name: str | None = None,
        model: str | None = None,
        agent: str | None = None,
        environment: str | dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        system_instruction: str | None = None,
        generation_config: dict[str, Any] | None = None,
        response_format: dict[str, Any] | None = None,
        background: bool = False,
        store: bool = True,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        """Build a request that answers a prior ``requires_action`` call."""
        return cls._create(
            kind="function_result",
            input=[
                {
                    "type": "function_result",
                    "call_id": function_call_id,
                    "name": function_name,
                    "result": function_result,
                }
            ],
            model=model,
            agent=agent,
            previous_interaction_id=previous_interaction_id,
            function_call_id=function_call_id,
            function_name=function_name,
            function_result=function_result,
            environment=environment,
            tools=tools,
            system_instruction=system_instruction,
            generation_config=generation_config,
            response_format=response_format,
            background=background,
            store=store,
            timeout_s=timeout_s,
            metadata=metadata,
        )

    @classmethod
    def poll(
        cls,
        interaction_id: str,
        *,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        """Build a request that fetches an existing background interaction."""
        return cls(
            kind="poll",
            interaction_id=interaction_id,
            timeout_s=timeout_s,
            metadata=_default_dict(metadata),
        )

    @classmethod
    def antigravity(
        cls,
        input: str | dict[str, Any] | list[dict[str, Any]],
        *,
        previous_interaction_id: str | None = None,
        environment: str | dict[str, Any] | None = "remote",
        agent: str = ANTIGRAVITY_AGENT_ID,
        tools: list[dict[str, Any]] | None = None,
        system_instruction: str | None = None,
        generation_config: dict[str, Any] | None = None,
        response_format: dict[str, Any] | None = None,
        background: bool = False,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        """Build an Antigravity managed-agent request.

        Antigravity remains one coarse Gemini interaction checkpoint. Kitaru
        does not claim to snapshot Google's hosted agent sandbox.
        """
        if background:
            raise ValueError(
                "GeminiInteractionRequest.antigravity() does not support "
                "background=True; Google's Antigravity preview requires "
                "synchronous requests. Use timeout_s to bound the provider call."
            )
        combined_metadata = {
            "adapter_preview": True,
            "agent_family": "antigravity",
            **(metadata or {}),
        }
        if previous_interaction_id is None:
            return cls.start(
                input,
                agent=agent,
                environment=environment,
                store=True,
                tools=tools,
                system_instruction=system_instruction,
                generation_config=generation_config,
                response_format=response_format,
                background=background,
                timeout_s=timeout_s,
                metadata=combined_metadata,
            )
        return cls.resume(
            input,
            previous_interaction_id,
            agent=agent,
            environment=environment,
            store=True,
            tools=tools,
            system_instruction=system_instruction,
            generation_config=generation_config,
            response_format=response_format,
            background=background,
            timeout_s=timeout_s,
            metadata=combined_metadata,
        )

    @classmethod
    def _create(
        cls,
        *,
        kind: Literal["start", "resume", "function_result"],
        input: str | dict[str, Any] | list[dict[str, Any]],
        model: str | None = None,
        agent: str | None = None,
        previous_interaction_id: str | None = None,
        function_call_id: str | None = None,
        function_name: str | None = None,
        function_result: Any | None = None,
        environment: str | dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        system_instruction: str | None = None,
        generation_config: dict[str, Any] | None = None,
        response_format: dict[str, Any] | None = None,
        background: bool = False,
        store: bool = True,
        timeout_s: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "GeminiInteractionRequest":
        data: dict[str, Any] = {
            "kind": kind,
            "input": input,
            "model": model,
            "agent": agent,
            "previous_interaction_id": previous_interaction_id,
            "environment": environment,
            "tools": _default_list(tools),
            "system_instruction": system_instruction,
            "generation_config": _default_dict(generation_config),
            "response_format": response_format,
            "background": background,
            "store": store,
            "timeout_s": timeout_s,
            "metadata": _default_dict(metadata),
        }
        if kind == "function_result":
            data.update(
                {
                    "function_call_id": function_call_id,
                    "function_name": function_name,
                    "function_result": function_result,
                }
            )
        return cls(**data)

    @field_validator(
        "input",
        "previous_interaction_id",
        "interaction_id",
        "function_call_id",
        "function_name",
        "function_result_payload",
        "environment",
        "model",
        "agent",
        "tools",
        "system_instruction",
        "generation_config",
        "response_format",
        "metadata",
        mode="before",
    )
    @classmethod
    def _reject_checkpoint_handles(cls, value: Any) -> Any:
        _reject_checkpoint_output_handles(value)
        return value

    @field_validator("model", "agent", "previous_interaction_id", "interaction_id")
    @classmethod
    def _validate_non_empty_strings(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Gemini interaction identifiers must be non-empty.")
        return value

    @field_validator("timeout_s")
    @classmethod
    def _validate_timeout(cls, value: float | None) -> float | None:
        if value is not None and value <= 0:
            raise ValueError("timeout_s must be positive when provided.")
        return value

    @model_validator(mode="after")
    def _validate_kind_contract(self) -> "GeminiInteractionRequest":
        target_count = int(self.model is not None) + int(self.agent is not None)
        if self.kind != "poll" and target_count != 1:
            raise ValueError(
                "Gemini interaction requests must set exactly one of model or agent."
            )
        if self.kind == "poll":
            if self.interaction_id is None:
                raise ValueError("kind='poll' requires interaction_id.")
            nullable_forbidden = [
                self.previous_interaction_id,
                self.function_call_id,
                self.function_name,
            ]
            create_only_fields_set = any(
                (
                    self.input is not None,
                    self.model is not None,
                    self.agent is not None,
                    bool(self.tools),
                    self.system_instruction is not None,
                    bool(self.generation_config),
                    self.response_format is not None,
                    self.environment is not None,
                    self.has_function_result_only_fields,
                )
            )
            if (
                any(value is not None for value in nullable_forbidden)
                or create_only_fields_set
            ):
                raise ValueError(
                    "kind='poll' fetches an existing interaction and forbids input, "
                    "model, agent, tools, environment, generation config, "
                    "previous_interaction_id, and function-result fields."
                )
            if self.background:
                raise ValueError("kind='poll' does not create a background job.")
            return self
        if self.kind == "start":
            forbidden = [self.previous_interaction_id, self.interaction_id]
            if any(value is not None for value in forbidden):
                raise ValueError(
                    "kind='start' forbids previous_interaction_id, interaction_id, "
                    "and function-result fields."
                )
            if self.has_function_result_only_fields:
                raise ValueError(
                    "kind='start' forbids previous_interaction_id, interaction_id, "
                    "and function-result fields."
                )
        if self.kind == "resume":
            if self.previous_interaction_id is None:
                raise ValueError("kind='resume' requires previous_interaction_id.")
            if self.has_function_result_only_fields:
                raise ValueError("kind='resume' forbids function-result fields.")
        if self.kind == "function_result":
            if self.previous_interaction_id is None:
                raise ValueError(
                    "kind='function_result' requires previous_interaction_id."
                )
            if self.function_call_id is None:
                raise ValueError("kind='function_result' requires function_call_id.")
            if not self.has_function_result_payload:
                raise ValueError("kind='function_result' requires function_result.")
        if self.input is None:
            raise ValueError(f"kind={self.kind!r} requires input.")
        if self.background and not self.store:
            raise ValueError("background=True requires store=True.")
        return self


class GeminiInteractionStepSummary(BaseModel):
    """Metadata-first summary for one Gemini interaction content/step item."""

    model_config = ConfigDict(extra="forbid")

    index: int
    step_id: str | None = None
    type: str | None = None
    status: str | None = None
    call_id: str | None = None
    tool_name: str | None = None
    text_preview: str | None = None
    raw_keys: list[str] = Field(default_factory=list)


class GeminiInteractionResult(BaseModel):
    """Serializable output from one stable Gemini interaction boundary."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: str
    interaction_id: str | None = None
    previous_interaction_id: str | None = None
    output_text: str | None = None
    model: str | None = None
    agent: str | None = None
    environment_id: str | None = None
    steps: list[GeminiInteractionStepSummary] = Field(default_factory=list)
    usage: dict[str, Any] | None = None
    duration_ms: float | None = None
    poll_count: int = 0
    sdk_version: str = "unknown"
    input_artifact_name: str | None = None
    request_manifest_artifact_name: str | None = None
    raw_interaction_artifact_name: str | None = None
    steps_artifact_name: str | None = None
    output_artifact_name: str | None = None
    usage_artifact_name: str | None = None
    event_log_artifact_name: str | None = None
    run_summary_artifact_name: str | None = None
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def _default_list(value: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [] if value is None else value


def _default_dict(value: dict[str, Any] | None) -> dict[str, Any]:
    return {} if value is None else value


def _reject_checkpoint_output_handles(value: Any) -> None:
    active_containers: set[int] = set()
    validated_containers: set[int] = set()
    values_visited = 0
    max_values = 10_000
    stack: list[tuple[Any, bool]] = [(value, True)]

    while stack:
        nested_value, entering = stack.pop()
        if not entering:
            value_id = id(nested_value)
            active_containers.remove(value_id)
            validated_containers.add(value_id)
            continue

        values_visited += 1
        if values_visited > max_values:
            raise ValueError(
                "GeminiInteractionRequest data is too large or deeply nested."
            )
        _raise_if_checkpoint_output_handle(
            nested_value,
            field_name="GeminiInteractionRequest",
            expected="materialized Gemini interaction request data",
        )
        if not _is_traversable_container(nested_value):
            continue

        value_id = id(nested_value)
        if value_id in validated_containers:
            continue
        if value_id in active_containers:
            raise ValueError("GeminiInteractionRequest data cannot be cyclic.")
        active_containers.add(value_id)
        stack.append((nested_value, False))
        if isinstance(nested_value, Mapping):
            children = list(nested_value.values())
        else:
            children = list(nested_value)
        stack.extend((child, True) for child in reversed(children))


def _is_traversable_container(value: Any) -> bool:
    return isinstance(value, Mapping) or (
        isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray)
    )
