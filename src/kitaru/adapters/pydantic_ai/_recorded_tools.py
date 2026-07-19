"""Conservative recorded tool responses for PydanticAI imported replay."""

from __future__ import annotations

import inspect
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from threading import Lock
from typing import Any, Never, get_type_hints

from pydantic import TypeAdapter, ValidationError
from pydantic_ai import RunContext
from pydantic_ai.tools import Tool, ToolDenied
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset
from pydantic_ai.toolsets.abstract import ToolsetTool

from kitaru._agent_registration import hash_registration_value
from kitaru._config._agents import (
    RegisteredToolEffect,
    _PydanticAIReplayManifest,
    _RegisteredPydanticAITool,
)
from kitaru.errors import KitaruStateError
from kitaru.imports._langfuse import strict_json_loads
from kitaru.imports._pydantic_ai_replay import (
    PreparedImportedReplayEvidence,
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ReplayEvidencePart,
    ReplayPartKind,
)
from kitaru.imports._replay_evidence import (
    ImportedReplayUnsupportedReason,
    canonical_json,
    contains_redaction,
    sha256_text_sequence,
)
from kitaru.replay import _MAX_IMPORTED_REPLAY_TOOL_DECISIONS

from ._toolset import KitaruToolset


class RecordedResponseDecision(StrEnum):
    """Result of matching one candidate tool call against recorded evidence."""

    HIT = "hit"
    BLOCKED = "blocked"


class RecordedResponseBlockReason(StrEnum):
    """Fail-closed reason attached to a blocked candidate tool call."""

    TOOL_NOT_RECORDED = "tool_not_recorded"
    CONTRACT_MISMATCH = "contract_mismatch"
    ARGUMENTS_INVALID = "arguments_invalid"
    ARGUMENT_MISMATCH = "argument_mismatch"
    OCCURRENCE_UNAVAILABLE = "occurrence_unavailable"
    OCCURRENCE_REORDERED = "occurrence_reordered"
    RETRY_UNSUPPORTED = "retry_unsupported"


@dataclass(frozen=True)
class RecordedResponseEvent:
    """One append-only recorded-response serving decision without user content."""

    index: int
    decision: RecordedResponseDecision
    candidate_tool_name: str
    logical_tool_id: str
    candidate_call_id: str
    retry_index: int
    arguments_sha256: str | None
    source_call_id: str | None = None
    source_observation_id: str | None = None
    source_sequence: int | None = None
    source_occurrence: int | None = None
    block_reason: RecordedResponseBlockReason | None = None


class RecordedResponseReport:
    """Thread-safe append-only evidence from one recorded-response runtime."""

    def __init__(self) -> None:
        self._events: list[RecordedResponseEvent] = []
        self._event_count = 0
        self._hit_count = 0
        self._blocked_count = 0
        self._lock = Lock()

    @property
    def events(self) -> tuple[RecordedResponseEvent, ...]:
        """Return an immutable snapshot in decision order."""

        with self._lock:
            return tuple(self._events)

    @property
    def hit_count(self) -> int:
        """Return all hits, including events beyond the detail limit."""

        with self._lock:
            return self._hit_count

    @property
    def blocked_count(self) -> int:
        """Return all blocks, including events beyond the detail limit."""

        with self._lock:
            return self._blocked_count

    def _append(self, **values: Any) -> RecordedResponseEvent:
        with self._lock:
            event = RecordedResponseEvent(index=self._event_count, **values)
            self._event_count += 1
            if event.decision is RecordedResponseDecision.HIT:
                self._hit_count += 1
            else:
                self._blocked_count += 1
            if len(self._events) < _MAX_IMPORTED_REPLAY_TOOL_DECISIONS:
                self._events.append(event)
            return event


class RecordedResponseCompilationError(KitaruStateError):
    """Typed failure raised before original PydanticAI tool sources are replaced."""

    def __init__(
        self,
        reason: ImportedReplayUnsupportedReason,
        message: str,
        *,
        execution_id: str,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.execution_id = execution_id


@dataclass(frozen=True)
class _RecordedOccurrence:
    logical_tool_id: str
    source_call_id: str
    source_observation_id: str
    source_sequence: int
    source_occurrence: int
    canonical_arguments: str
    result: Any


class _RecordedArgumentsValidator:
    """Parse strict JSON while preserving exact argument types for reporting."""

    def validate_json(
        self,
        value: str | bytes | bytearray,
        *,
        allow_partial: Any = False,
        **kwargs: Any,
    ) -> Any:
        del allow_partial, kwargs
        decoded = strict_json_loads(
            bytes(value).decode() if isinstance(value, (bytes, bytearray)) else value
        )
        if not isinstance(decoded, dict):
            raise ValueError("Recorded tool arguments must be a JSON object.")
        return decoded

    def validate_python(
        self,
        value: Any,
        *,
        allow_partial: Any = False,
        **kwargs: Any,
    ) -> Any:
        del allow_partial, kwargs
        if not isinstance(value, dict):
            raise ValueError("Recorded tool arguments must be a mapping.")
        return value


class _RecordedFunctionToolset(FunctionToolset[Any]):
    """Function toolset that forces strict candidate argument validation."""

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools = await super().get_tools(ctx)
        return {
            name: replace(
                tool,
                args_validator=_RecordedArgumentsValidator(),
                args_validator_func=None,
            )
            for name, tool in tools.items()
        }


@dataclass(frozen=True)
class _CandidateTool:
    contract: _RegisteredPydanticAITool
    declared_name: str
    tool: Tool[Any]
    result_adapter: TypeAdapter[Any]


class RecordedResponseRuntime:
    """Single-use recorded occurrences and complete PydanticAI tool replacement."""

    def __init__(
        self,
        *,
        execution_id: str,
        candidates: Mapping[str, _CandidateTool],
        occurrences: Sequence[_RecordedOccurrence],
        incompatible_occurrence_count: int,
        blockers: Mapping[str, RecordedResponseBlockReason],
    ) -> None:
        self.execution_id = execution_id
        self.report = RecordedResponseReport()
        self._candidates = dict(candidates)
        self._occurrences = tuple(occurrences)
        self._incompatible_occurrence_count = incompatible_occurrence_count
        self._blockers = dict(blockers)
        self._next_occurrence = 0
        self._lock = Lock()
        self._replacement_toolset = _RecordedFunctionToolset(
            [
                self._replacement_tool(candidate)
                for candidate in sorted(
                    self._candidates.values(),
                    key=lambda value: value.declared_name,
                )
            ]
        )

    @property
    def eligible_occurrence_count(self) -> int:
        """Return the frozen recorded-response denominator."""

        return len(self._occurrences) + self._incompatible_occurrence_count

    @property
    def incompatible_occurrence_count(self) -> int:
        """Return source occurrences rejected during contract compilation."""

        return self._incompatible_occurrence_count

    @property
    def candidate_tool_contract_compatible(self) -> bool:
        """Return whether every source occurrence bound to a candidate contract."""

        return self._incompatible_occurrence_count == 0

    @property
    def remaining_occurrence_count(self) -> int:
        """Return recorded responses the candidate did not consume."""

        with self._lock:
            return len(self._occurrences) - self._next_occurrence

    @property
    def replacement_toolsets(self) -> tuple[AbstractToolset[Any], ...]:
        """Return the only tool sources that may be installed for this runtime."""

        return (self._replacement_toolset,)

    @contextmanager
    def install(self, agent: Any) -> Iterator[None]:
        """Replace every agent tool source and reject streaming for this runtime."""

        with agent._replace_tool_sources(
            self.replacement_toolsets,
            streaming_supported=False,
        ):
            yield

    def _replacement_tool(self, candidate: _CandidateTool) -> Tool[Any]:
        async def _serve(ctx: RunContext[Any], **arguments: Any) -> Any:
            return self._serve(
                candidate=candidate,
                ctx=ctx,
                arguments=arguments,
            )

        function_schema = candidate.tool.function_schema
        if function_schema is None:
            raise RecordedResponseCompilationError(
                ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
                "A candidate tool has no PydanticAI function schema.",
                execution_id=self.execution_id,
            )
        return replace(
            candidate.tool,
            function=_serve,
            takes_ctx=True,
            prepare=None,
            args_validator=None,
            requires_approval=False,
            defer_loading=False,
            function_schema=replace(
                function_schema,
                function=_serve,
                validator=_RecordedArgumentsValidator(),
                takes_ctx=True,
                is_async=True,
            ),
        )

    def _blocked(
        self,
        *,
        candidate: _CandidateTool,
        ctx: RunContext[Any],
        reason: RecordedResponseBlockReason,
        arguments_sha256: str | None,
        occurrence: _RecordedOccurrence | None = None,
    ) -> ToolDenied:
        self.report._append(
            decision=RecordedResponseDecision.BLOCKED,
            candidate_tool_name=candidate.declared_name,
            logical_tool_id=candidate.contract.logical_id,
            candidate_call_id=ctx.tool_call_id,
            retry_index=ctx.retry,
            arguments_sha256=arguments_sha256,
            source_call_id=occurrence.source_call_id if occurrence else None,
            source_observation_id=(
                occurrence.source_observation_id if occurrence else None
            ),
            source_sequence=occurrence.source_sequence if occurrence else None,
            source_occurrence=occurrence.source_occurrence if occurrence else None,
            block_reason=reason,
        )
        return ToolDenied("The recorded response is unavailable for this tool call.")

    def _serve(
        self,
        *,
        candidate: _CandidateTool,
        ctx: RunContext[Any],
        arguments: Mapping[str, Any],
    ) -> Any:
        try:
            rendered_arguments = canonical_json(dict(arguments))
            arguments_sha256 = sha256_text_sequence((rendered_arguments,))
            function_schema = candidate.tool.function_schema
            assert function_schema is not None
            function_schema.validator.validate_json(
                rendered_arguments,
                strict=True,
                allow_partial=False,
            )
        except (TypeError, ValueError, ValidationError):
            return self._blocked(
                candidate=candidate,
                ctx=ctx,
                reason=RecordedResponseBlockReason.ARGUMENTS_INVALID,
                arguments_sha256=None,
            )
        if ctx.retry != 0:
            return self._blocked(
                candidate=candidate,
                ctx=ctx,
                reason=RecordedResponseBlockReason.RETRY_UNSUPPORTED,
                arguments_sha256=arguments_sha256,
            )

        logical_id = candidate.contract.logical_id
        blocker = self._blockers.get(logical_id)
        if blocker is not None:
            return self._blocked(
                candidate=candidate,
                ctx=ctx,
                reason=blocker,
                arguments_sha256=arguments_sha256,
            )

        with self._lock:
            occurrence_index = self._next_occurrence
            if occurrence_index >= len(self._occurrences):
                occurrence = None
            else:
                occurrence = self._occurrences[occurrence_index]

            if occurrence is None:
                reason = RecordedResponseBlockReason.OCCURRENCE_UNAVAILABLE
            elif occurrence.logical_tool_id != logical_id:
                reason = RecordedResponseBlockReason.OCCURRENCE_REORDERED
            elif occurrence.canonical_arguments != rendered_arguments:
                reason = RecordedResponseBlockReason.ARGUMENT_MISMATCH
            else:
                self._next_occurrence += 1
                reason = None

        if reason is not None:
            return self._blocked(
                candidate=candidate,
                ctx=ctx,
                reason=reason,
                arguments_sha256=arguments_sha256,
                occurrence=occurrence,
            )

        assert occurrence is not None
        self.report._append(
            decision=RecordedResponseDecision.HIT,
            candidate_tool_name=candidate.declared_name,
            logical_tool_id=logical_id,
            candidate_call_id=ctx.tool_call_id,
            retry_index=ctx.retry,
            arguments_sha256=arguments_sha256,
            source_call_id=occurrence.source_call_id,
            source_observation_id=occurrence.source_observation_id,
            source_sequence=occurrence.source_sequence,
            source_occurrence=occurrence.source_occurrence,
            block_reason=None,
        )
        return occurrence.result


def _fail(
    reason: ImportedReplayUnsupportedReason,
    message: str,
    *,
    evidence: PreparedImportedReplayEvidence,
) -> Never:
    raise RecordedResponseCompilationError(
        reason,
        message,
        execution_id=evidence.identity.execution_id,
    )


def _function_tools(
    toolsets: Sequence[AbstractToolset[Any]],
    *,
    evidence: PreparedImportedReplayEvidence,
) -> dict[str, Tool[Any]]:
    tools: dict[str, Tool[Any]] = {}
    for toolset in toolsets:
        component = toolset.wrapped if isinstance(toolset, KitaruToolset) else toolset
        if not isinstance(component, FunctionToolset):
            _fail(
                ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
                "Recorded responses require complete registered function-tool contracts.",
                evidence=evidence,
            )
        for name, tool in component.tools.items():
            if name in tools:
                _fail(
                    ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
                    "Candidate function-tool names are ambiguous.",
                    evidence=evidence,
                )
            tools[name] = tool
    return tools


def _candidate_tools(
    *,
    manifest: _PydanticAIReplayManifest,
    toolsets: Sequence[AbstractToolset[Any]],
    evidence: PreparedImportedReplayEvidence,
) -> dict[str, _CandidateTool]:
    if manifest.unresolved_tool_sources:
        _fail(
            ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
            "Recorded responses require resolved contracts for every candidate tool source.",
            evidence=evidence,
        )
    actual = _function_tools(toolsets, evidence=evidence)
    candidates: dict[str, _CandidateTool] = {}
    used_names: set[str] = set()
    for contract in manifest.tools:
        declared_names = [
            name
            for name in actual
            if name in contract.aliases and name not in used_names
        ]
        if len(declared_names) != 1:
            _fail(
                ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
                "A candidate tool cannot be bound uniquely to its registered contract.",
                evidence=evidence,
            )
        declared_name = declared_names[0]
        tool = actual[declared_name]
        function_schema = tool.function_schema
        if (
            function_schema is None
            or hash_registration_value(function_schema.json_schema)
            != contract.input_schema_hash
            or hash_registration_value(function_schema.return_schema)
            != contract.output_schema_hash
        ):
            _fail(
                ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
                "A candidate tool schema differs from its registered contract.",
                evidence=evidence,
            )
        used_names.add(declared_name)
        candidates[contract.logical_id] = _CandidateTool(
            contract=contract,
            declared_name=declared_name,
            tool=tool,
            result_adapter=_result_adapter(tool),
        )
    if used_names != set(actual):
        _fail(
            ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
            "Candidate function tools differ from the registered manifest.",
            evidence=evidence,
        )
    return candidates


def _contract_aliases(
    manifest: _PydanticAIReplayManifest,
) -> dict[str, _RegisteredPydanticAITool]:
    return {
        alias: contract for contract in manifest.tools for alias in contract.aliases
    }


def _contracts_match(
    source: _RegisteredPydanticAITool,
    candidate: _RegisteredPydanticAITool,
    *,
    source_name: str,
) -> bool:
    return (
        source.logical_id == candidate.logical_id
        and source_name in candidate.aliases
        and source.input_schema_hash == candidate.input_schema_hash
        and source.output_schema_hash == candidate.output_schema_hash
        and source.implementation_identity == candidate.implementation_identity
        and source.effect == candidate.effect
        and source.effect is not RegisteredToolEffect.UNKNOWN
        and source.argument_normalizer_revision
        == candidate.argument_normalizer_revision
    )


def _strict_arguments(
    part: ReplayEvidencePart,
    *,
    candidate: _CandidateTool,
) -> str:
    value = part.content
    if isinstance(value, str):
        value = strict_json_loads(value)
    if not isinstance(value, dict) or contains_redaction(value):
        raise ValueError("Recorded tool arguments are not an unredacted JSON object.")
    function_schema = candidate.tool.function_schema
    assert function_schema is not None
    rendered = canonical_json(value)
    function_schema.validator.validate_json(
        rendered,
        strict=True,
        allow_partial=False,
    )
    return rendered


def _strict_result(
    result: Any,
    *,
    candidate: _CandidateTool,
) -> Any:
    if contains_redaction(result):
        raise ValueError("Recorded tool result is redacted.")
    candidate.result_adapter.validate_python(result, strict=True)
    return result


def _result_adapter(tool: Tool[Any]) -> TypeAdapter[Any]:
    try:
        return_annotation = get_type_hints(tool.function).get(
            "return",
            Any,
        )
    except (NameError, TypeError):
        return_annotation = inspect.signature(tool.function).return_annotation
        if return_annotation is inspect.Signature.empty:
            return_annotation = Any
    return TypeAdapter(return_annotation)


def _parts(
    evidence: PreparedImportedReplayEvidence,
) -> tuple[ReplayEvidencePart, ...]:
    parts = tuple(
        part
        for observation in evidence.replay_bundle.observations
        for part in observation.parts
    )
    positions = [(part.sequence, part.occurrence) for part in parts]
    if positions != sorted(positions) or len(positions) != len(set(positions)):
        _fail(
            ImportedReplayUnsupportedReason.RECORDED_RESPONSE_INVALID,
            "Recorded tool evidence is not uniquely ordered.",
            evidence=evidence,
        )
    return parts


def compile_recorded_responses(
    evidence: PreparedImportedReplayEvidence,
    *,
    source_manifest: _PydanticAIReplayManifest,
    candidate_manifest: _PydanticAIReplayManifest,
    candidate_toolsets: Sequence[AbstractToolset[Any]],
    after_boundary: ImportedReplayBoundary | None = None,
) -> RecordedResponseRuntime:
    """Compile single-use recorded occurrences without invoking any tool source."""

    if (
        source_manifest.driver_revision != candidate_manifest.driver_revision
        or source_manifest.preparation_revision
        != candidate_manifest.preparation_revision
        or source_manifest.driver_entrypoint != candidate_manifest.driver_entrypoint
        or source_manifest.resume_kinds != candidate_manifest.resume_kinds
        or source_manifest.argument_normalizer_revision
        != candidate_manifest.argument_normalizer_revision
        or source_manifest.unresolved_tool_sources
    ):
        _fail(
            ImportedReplayUnsupportedReason.TOOL_CONTRACT_INCOMPATIBLE,
            "Source and candidate replay-driver contracts are incompatible.",
            evidence=evidence,
        )

    candidates = _candidate_tools(
        manifest=candidate_manifest,
        toolsets=candidate_toolsets,
        evidence=evidence,
    )
    source_aliases = _contract_aliases(source_manifest)
    calls: dict[str, ReplayEvidencePart] = {}
    results: dict[str, ReplayEvidencePart] = {}
    duplicate_ids: set[str] = set()
    boundary_position: tuple[int, int] | None = None
    if (
        after_boundary is not None
        and after_boundary.kind is not ImportedReplayBoundaryKind.ROOT_INPUT
    ):
        assert after_boundary.sequence is not None
        assert after_boundary.occurrence is not None
        boundary_position = (
            after_boundary.sequence,
            after_boundary.occurrence,
        )
    for part in _parts(evidence):
        if (
            boundary_position is not None
            and (
                part.sequence,
                part.occurrence,
            )
            <= boundary_position
        ):
            continue
        if part.kind is ReplayPartKind.TOOL_CALL and part.call_id is not None:
            if part.call_id in calls:
                duplicate_ids.add(part.call_id)
            else:
                calls[part.call_id] = part
        elif part.kind is ReplayPartKind.TOOL_RESULT and part.call_id is not None:
            if part.call_id in results:
                duplicate_ids.add(part.call_id)
            else:
                results[part.call_id] = part
    if duplicate_ids:
        _fail(
            ImportedReplayUnsupportedReason.RECORDED_RESPONSE_INVALID,
            "Recorded tool evidence contains ambiguous duplicate identities.",
            evidence=evidence,
        )
    if set(results) - set(calls):
        _fail(
            ImportedReplayUnsupportedReason.RECORDED_RESPONSE_INVALID,
            "Recorded tool evidence contains an orphaned result.",
            evidence=evidence,
        )
    call_order = [
        call.call_id
        for call in sorted(
            calls.values(),
            key=lambda part: (part.sequence, part.occurrence),
        )
        if call.call_id in results
    ]
    result_order = [
        result.call_id
        for result in sorted(
            results.values(),
            key=lambda part: (part.sequence, part.occurrence),
        )
    ]
    if call_order != result_order:
        _fail(
            ImportedReplayUnsupportedReason.RECORDED_RESPONSE_INVALID,
            "Recorded tool results do not preserve call order.",
            evidence=evidence,
        )

    blockers: dict[str, RecordedResponseBlockReason] = {}
    occurrences: list[_RecordedOccurrence] = []
    incompatible_occurrence_count = 0
    for call in sorted(
        calls.values(), key=lambda part: (part.sequence, part.occurrence)
    ):
        assert call.call_id is not None
        source_contract = source_aliases.get(call.name or "")
        if source_contract is None:
            incompatible_occurrence_count += 1
            continue
        candidate = candidates.get(source_contract.logical_id)
        if candidate is None:
            incompatible_occurrence_count += 1
            continue
        logical_id = candidate.contract.logical_id
        result = results.get(call.call_id)
        if (
            not _contracts_match(
                source_contract,
                candidate.contract,
                source_name=call.name or "",
            )
            or result is None
            or (result.name is not None and result.name != call.name)
            or (result.sequence, result.occurrence) <= (call.sequence, call.occurrence)
        ):
            blockers[logical_id] = RecordedResponseBlockReason.CONTRACT_MISMATCH
            incompatible_occurrence_count += 1
            continue
        try:
            rendered_arguments = _strict_arguments(call, candidate=candidate)
            recorded_result = _strict_result(result.content, candidate=candidate)
        except (TypeError, ValueError, ValidationError):
            blockers[logical_id] = RecordedResponseBlockReason.CONTRACT_MISMATCH
            incompatible_occurrence_count += 1
            continue
        occurrences.append(
            _RecordedOccurrence(
                logical_tool_id=logical_id,
                source_call_id=call.call_id,
                source_observation_id=call.observation_id,
                source_sequence=call.sequence,
                source_occurrence=call.occurrence,
                canonical_arguments=rendered_arguments,
                result=recorded_result,
            )
        )

    recorded_tool_ids = {occurrence.logical_tool_id for occurrence in occurrences}
    for logical_id in candidates:
        if logical_id not in recorded_tool_ids and logical_id not in blockers:
            blockers[logical_id] = RecordedResponseBlockReason.TOOL_NOT_RECORDED

    return RecordedResponseRuntime(
        execution_id=evidence.identity.execution_id,
        candidates=candidates,
        occurrences=occurrences,
        incompatible_occurrence_count=incompatible_occurrence_count,
        blockers=blockers,
    )


__all__ = [
    "RecordedResponseBlockReason",
    "RecordedResponseCompilationError",
    "RecordedResponseDecision",
    "RecordedResponseEvent",
    "RecordedResponseReport",
    "RecordedResponseRuntime",
    "compile_recorded_responses",
]
