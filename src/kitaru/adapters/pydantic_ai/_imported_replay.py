"""Validated PydanticAI message history built from imported replay evidence."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Never

from pydantic import TypeAdapter, ValidationError
from pydantic_ai import messages as _messages

from kitaru.errors import KitaruStateError
from kitaru.imports._langfuse import strict_json_loads
from kitaru.imports._pydantic_ai_replay import (
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ImportedReplayMode,
    PreparedImportedReplayEvidence,
    ReplayEvidencePart,
    ReplayPartKind,
)
from kitaru.imports._replay_evidence import (
    ImportedReplayUnsupportedReason,
    ReplayReadinessStatus,
    contains_redaction,
)


class ImportedReplayFallbackPolicy(StrEnum):
    """Behavior available when a requested history boundary cannot be prepared."""

    ROOT_INPUT = "root_input"
    BLOCK = "block"


@dataclass(frozen=True)
class ImportedReplayMessageProvenance:
    """Source positions represented by one reconstructed PydanticAI message."""

    observation_ids: tuple[str, ...]
    first_sequence: int
    first_occurrence: int
    last_sequence: int
    last_occurrence: int


@dataclass(frozen=True)
class PreparedImportedReplayHistory:
    """One complete validated PydanticAI history prefix and its evidence identity."""

    evidence: PreparedImportedReplayEvidence
    mode: ImportedReplayMode
    boundary: ImportedReplayBoundary
    message_history: tuple[_messages.ModelMessage, ...]
    message_provenance: tuple[ImportedReplayMessageProvenance, ...]
    fallback_policy: ImportedReplayFallbackPolicy
    fallback_root_input: Any


class ImportedReplayPreparationError(KitaruStateError):
    """Typed failure raised before imported history reaches a candidate."""

    def __init__(
        self,
        reason: ImportedReplayUnsupportedReason,
        message: str,
        *,
        execution_id: str,
        boundary: ImportedReplayBoundary,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.execution_id = execution_id
        self.boundary = boundary


_MessageCategory = Literal["prompt_request", "tool_return_request", "model_response"]


@dataclass(frozen=True)
class _MessageGroup:
    category: _MessageCategory
    parts: tuple[ReplayEvidencePart, ...]


def _fail(
    reason: ImportedReplayUnsupportedReason,
    message: str,
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> Never:
    raise ImportedReplayPreparationError(
        reason,
        message,
        execution_id=evidence.identity.execution_id,
        boundary=boundary,
    )


def _part_category(part: ReplayEvidencePart) -> _MessageCategory:
    if part.kind in {ReplayPartKind.SYSTEM_PROMPT, ReplayPartKind.USER_PROMPT}:
        return "prompt_request"
    if part.kind is ReplayPartKind.TOOL_RESULT:
        return "tool_return_request"
    return "model_response"


def _ordered_positions(
    evidence: PreparedImportedReplayEvidence,
    *,
    boundary: ImportedReplayBoundary,
) -> tuple[ReplayEvidencePart, ...]:
    observations = evidence.replay_bundle.observations
    observation_positions = [
        (observation.sequence, observation.occurrence) for observation in observations
    ]
    if observation_positions != sorted(observation_positions) or len(
        observation_positions
    ) != len(set(observation_positions)):
        _fail(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
            "Imported replay observations are not uniquely ordered.",
            evidence=evidence,
            boundary=boundary,
        )

    parts: list[ReplayEvidencePart] = []
    prior: tuple[int, int] | None = None
    for observation in observations:
        part_occurrences: set[int] = set()
        prior_message_index: int | None = None
        for part in observation.parts:
            invalid_message_index = (
                isinstance(part.message_index, bool)
                or not isinstance(part.message_index, int)
                or part.message_index < 0
                or (
                    prior_message_index is not None
                    and part.message_index < prior_message_index
                )
            )
            if (
                part.observation_id != observation.provider_observation_id
                or part.sequence != observation.sequence
                or part.occurrence in part_occurrences
                or invalid_message_index
            ):
                _fail(
                    ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                    "Imported message provenance is inconsistent.",
                    evidence=evidence,
                    boundary=boundary,
                )
            part_occurrences.add(part.occurrence)
            prior_message_index = part.message_index
            position = (part.sequence, part.occurrence)
            if prior is not None and position <= prior:
                _fail(
                    ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                    "Imported message parts are not uniquely ordered.",
                    evidence=evidence,
                    boundary=boundary,
                )
            prior = position
            parts.append(part)
    return tuple(parts)


def _boundary_index(
    positions: Sequence[ReplayEvidencePart],
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> int:
    expected_kinds = {
        ImportedReplayBoundaryKind.MODEL_MESSAGE: {
            ReplayPartKind.MODEL_TEXT,
            ReplayPartKind.TOOL_CALL,
        },
        ImportedReplayBoundaryKind.TOOL_RESULT: {ReplayPartKind.TOOL_RESULT},
    }.get(boundary.kind)
    if expected_kinds is None:
        _fail(
            ImportedReplayUnsupportedReason.BOUNDARY_UNAVAILABLE,
            "History preparation requires a model-message or tool-result boundary.",
            evidence=evidence,
            boundary=boundary,
        )

    matches = [
        index
        for index, part in enumerate(positions)
        if (
            part.kind in expected_kinds
            and part.observation_id == boundary.observation_id
            and part.sequence == boundary.sequence
            and part.occurrence == boundary.occurrence
            and (
                boundary.kind is ImportedReplayBoundaryKind.MODEL_MESSAGE
                or part.call_id == boundary.call_id
            )
        )
    ]
    if len(matches) != 1:
        _fail(
            ImportedReplayUnsupportedReason.BOUNDARY_UNAVAILABLE,
            "The requested imported replay boundary is not uniquely available.",
            evidence=evidence,
            boundary=boundary,
        )
    return matches[0]


def _groups(
    parts: Sequence[ReplayEvidencePart],
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> tuple[_MessageGroup, ...]:
    groups: list[_MessageGroup] = []
    seen_keys: set[tuple[str, int]] = set()
    current_key: tuple[str, int] | None = None
    current_category: _MessageCategory | None = None
    current: list[ReplayEvidencePart] = []
    for part in parts:
        key = (part.observation_id, part.message_index)
        category = _part_category(part)
        if current and key != current_key:
            assert current_key is not None and current_category is not None
            groups.append(
                _MessageGroup(category=current_category, parts=tuple(current))
            )
            seen_keys.add(current_key)
            current = []
            current_category = None
        if key in seen_keys or (
            current_category is not None and category != current_category
        ):
            _fail(
                ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                "Imported evidence does not preserve unambiguous source message boundaries.",
                evidence=evidence,
                boundary=boundary,
            )
        current_key = key
        current_category = category
        current.append(part)
    if current:
        assert current_category is not None
        groups.append(_MessageGroup(category=current_category, parts=tuple(current)))
    return tuple(groups)


def _string_content(
    part: ReplayEvidencePart,
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> str:
    if not isinstance(part.content, str) or contains_redaction(part.content):
        _fail(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
            "The supported imported history boundary requires unredacted text parts.",
            evidence=evidence,
            boundary=boundary,
        )
    return part.content


def _tool_arguments(
    part: ReplayEvidencePart,
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> dict[str, Any]:
    value = part.content
    if isinstance(value, str):
        try:
            value = strict_json_loads(value)
        except (TypeError, ValueError) as exc:
            raise ImportedReplayPreparationError(
                ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                "Imported tool arguments are not strict JSON.",
                execution_id=evidence.identity.execution_id,
                boundary=boundary,
            ) from exc
    if not isinstance(value, dict) or contains_redaction(value):
        _fail(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
            "Imported tool arguments must be an unredacted JSON object.",
            evidence=evidence,
            boundary=boundary,
        )
    return value


def _message_provenance(group: _MessageGroup) -> ImportedReplayMessageProvenance:
    first = group.parts[0]
    last = group.parts[-1]
    return ImportedReplayMessageProvenance(
        observation_ids=tuple(
            dict.fromkeys(part.observation_id for part in group.parts)
        ),
        first_sequence=first.sequence,
        first_occurrence=first.occurrence,
        last_sequence=last.sequence,
        last_occurrence=last.occurrence,
    )


def _request_message(
    group: _MessageGroup,
    *,
    pending_calls: list[tuple[str, str]],
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> _messages.ModelRequest:
    request_parts: list[_messages.ModelRequestPart] = []
    if group.category == "prompt_request":
        if pending_calls:
            _fail(
                ImportedReplayUnsupportedReason.BOUNDARY_INCOMPLETE,
                "A prompt appears before the preceding tool exchange is complete.",
                evidence=evidence,
                boundary=boundary,
            )
        seen_user_prompt = False
        for part in group.parts:
            if (part.kind is ReplayPartKind.SYSTEM_PROMPT and seen_user_prompt) or (
                part.kind is ReplayPartKind.USER_PROMPT and seen_user_prompt
            ):
                _fail(
                    ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                    "The supported history boundary has ambiguous prompt grouping.",
                    evidence=evidence,
                    boundary=boundary,
                )
            if part.kind is ReplayPartKind.USER_PROMPT:
                seen_user_prompt = True
            content = _string_content(
                part,
                evidence=evidence,
                boundary=boundary,
            )
            if part.kind is ReplayPartKind.SYSTEM_PROMPT:
                request_parts.append(_messages.SystemPromptPart(content=content))
            else:
                request_parts.append(_messages.UserPromptPart(content=content))
    else:
        if not pending_calls or len(group.parts) != len(pending_calls):
            _fail(
                ImportedReplayUnsupportedReason.BOUNDARY_INCOMPLETE,
                "The imported tool exchange does not contain every required result.",
                evidence=evidence,
                boundary=boundary,
            )
        for part, (expected_call_id, expected_name) in zip(
            group.parts,
            pending_calls,
            strict=True,
        ):
            if (
                part.call_id != expected_call_id
                or (part.name is not None and part.name != expected_name)
                or contains_redaction(part.content)
            ):
                _fail(
                    ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                    "Imported tool results are missing, reordered, or mismatched.",
                    evidence=evidence,
                    boundary=boundary,
                )
            request_parts.append(
                _messages.ToolReturnPart(
                    tool_name=expected_name,
                    content=part.content,
                    tool_call_id=expected_call_id,
                )
            )
        pending_calls.clear()
    return _messages.ModelRequest(parts=request_parts)


def _response_message(
    group: _MessageGroup,
    *,
    pending_calls: list[tuple[str, str]],
    has_request: bool,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
) -> _messages.ModelResponse:
    if not has_request or pending_calls:
        _fail(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
            "Imported model messages do not follow a complete request.",
            evidence=evidence,
            boundary=boundary,
        )

    response_parts: list[_messages.ModelResponsePart] = []
    seen_call_ids: set[str] = set()
    seen_model_text = False
    for part in group.parts:
        if part.kind is ReplayPartKind.MODEL_TEXT:
            if seen_model_text or seen_call_ids:
                _fail(
                    ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                    "The supported history boundary has ambiguous model grouping.",
                    evidence=evidence,
                    boundary=boundary,
                )
            seen_model_text = True
            response_parts.append(
                _messages.TextPart(
                    content=_string_content(
                        part,
                        evidence=evidence,
                        boundary=boundary,
                    )
                )
            )
            continue
        if (
            part.kind is not ReplayPartKind.TOOL_CALL
            or part.call_id is None
            or part.name is None
            or part.call_id in seen_call_ids
        ):
            _fail(
                ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
                "Imported tool calls have incomplete or duplicate identities.",
                evidence=evidence,
                boundary=boundary,
            )
        seen_call_ids.add(part.call_id)
        arguments = _tool_arguments(
            part,
            evidence=evidence,
            boundary=boundary,
        )
        response_parts.append(
            _messages.ToolCallPart(
                tool_name=part.name,
                args=arguments,
                tool_call_id=part.call_id,
            )
        )
        pending_calls.append((part.call_id, part.name))
    return _messages.ModelResponse(parts=response_parts)


def _validated_messages(
    groups: Sequence[_MessageGroup],
    *,
    evidence: PreparedImportedReplayEvidence,
    boundary: ImportedReplayBoundary,
    allow_pending_calls: bool = False,
) -> tuple[_messages.ModelMessage, ...]:
    messages: list[_messages.ModelMessage] = []
    pending_calls: list[tuple[str, str]] = []
    has_request = False
    for group in groups:
        if group.category == "model_response":
            messages.append(
                _response_message(
                    group,
                    pending_calls=pending_calls,
                    has_request=has_request,
                    evidence=evidence,
                    boundary=boundary,
                )
            )
        else:
            messages.append(
                _request_message(
                    group,
                    pending_calls=pending_calls,
                    evidence=evidence,
                    boundary=boundary,
                )
            )
            has_request = True

    if pending_calls and not allow_pending_calls:
        _fail(
            ImportedReplayUnsupportedReason.BOUNDARY_INCOMPLETE,
            "The requested boundary leaves an incomplete tool exchange.",
            evidence=evidence,
            boundary=boundary,
        )
    try:
        validated = _messages.ModelMessagesTypeAdapter.validate_python(
            messages,
            strict=True,
        )
    except ValidationError as exc:
        raise ImportedReplayPreparationError(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID,
            "PydanticAI rejected the complete imported message prefix.",
            execution_id=evidence.identity.execution_id,
            boundary=boundary,
        ) from exc
    return tuple(validated)


def prepare_imported_root_input(
    evidence: PreparedImportedReplayEvidence,
) -> str | Sequence[_messages.UserContent]:
    """Extract the candidate prompt from one complete imported root input."""
    boundary = ImportedReplayBoundary(kind=ImportedReplayBoundaryKind.ROOT_INPUT)
    if (
        evidence.readiness.root_input_candidate_rerun.status
        is not ReplayReadinessStatus.READY
        or not evidence.replay_bundle.root_input_present
    ):
        _fail(
            ImportedReplayUnsupportedReason.ROOT_INPUT_MISSING,
            "Imported evidence does not contain a complete root input.",
            evidence=evidence,
            boundary=boundary,
        )
    root_input = evidence.replay_bundle.root_input
    if isinstance(root_input, str) and root_input:
        return root_input
    if isinstance(root_input, dict):
        prompt = root_input.get("prompt")
        if isinstance(prompt, str) and prompt:
            return prompt
        messages = root_input.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if not isinstance(message, dict) or message.get("role") != "user":
                    continue
                content = message.get("content")
                if isinstance(content, str) and content:
                    return content
                if isinstance(content, list) and content:
                    try:
                        validated = TypeAdapter(
                            list[_messages.UserContent]
                        ).validate_python(content, strict=True)
                    except ValidationError:
                        break
                    return validated
    _fail(
        ImportedReplayUnsupportedReason.ROOT_INPUT_MISSING,
        "Imported root input cannot be mapped to a PydanticAI user prompt.",
        evidence=evidence,
        boundary=boundary,
    )


def prepare_imported_replay_history(
    evidence: PreparedImportedReplayEvidence,
    *,
    boundary: ImportedReplayBoundary,
    fallback_policy: ImportedReplayFallbackPolicy = (
        ImportedReplayFallbackPolicy.ROOT_INPUT
    ),
) -> PreparedImportedReplayHistory:
    """Prepare one explicit complete history boundary without running a candidate."""

    readiness = (
        evidence.readiness.model_message_reconstruction
        if boundary.kind is ImportedReplayBoundaryKind.MODEL_MESSAGE
        else evidence.readiness.tool_result_boundary_reconstruction
    )
    if (
        boundary.kind is ImportedReplayBoundaryKind.ROOT_INPUT
        or readiness.status is ReplayReadinessStatus.UNSUPPORTED
    ):
        _fail(
            ImportedReplayUnsupportedReason.MESSAGE_HISTORY_UNAVAILABLE,
            "Imported evidence does not support the requested history boundary.",
            evidence=evidence,
            boundary=boundary,
        )
    if fallback_policy is ImportedReplayFallbackPolicy.ROOT_INPUT and (
        evidence.readiness.root_input_candidate_rerun.status
        is not ReplayReadinessStatus.READY
        or not evidence.replay_bundle.root_input_present
        or contains_redaction(evidence.replay_bundle.root_input)
    ):
        _fail(
            ImportedReplayUnsupportedReason.ROOT_INPUT_MISSING,
            "The selected fallback requires a complete unredacted root input.",
            evidence=evidence,
            boundary=boundary,
        )

    positions = _ordered_positions(evidence, boundary=boundary)
    index = _boundary_index(
        positions,
        evidence=evidence,
        boundary=boundary,
    )
    boundary_part = positions[index]
    boundary_message = (
        boundary_part.observation_id,
        boundary_part.message_index,
    )
    if any(
        (part.observation_id, part.message_index) == boundary_message
        for part in positions[index + 1 :]
    ):
        _fail(
            ImportedReplayUnsupportedReason.BOUNDARY_INCOMPLETE,
            "The requested boundary is not the end of its source message.",
            evidence=evidence,
            boundary=boundary,
        )

    selected_groups = _groups(
        positions[: index + 1],
        evidence=evidence,
        boundary=boundary,
    )
    messages = _validated_messages(
        selected_groups,
        evidence=evidence,
        boundary=boundary,
        allow_pending_calls=(
            boundary.kind is ImportedReplayBoundaryKind.MODEL_MESSAGE
            and boundary_part.kind is ReplayPartKind.TOOL_CALL
        ),
    )
    return PreparedImportedReplayHistory(
        evidence=evidence,
        mode=ImportedReplayMode.MESSAGE_HISTORY,
        boundary=boundary,
        message_history=messages,
        message_provenance=tuple(
            _message_provenance(group) for group in selected_groups
        ),
        fallback_policy=fallback_policy,
        fallback_root_input=(
            evidence.replay_bundle.root_input
            if fallback_policy is ImportedReplayFallbackPolicy.ROOT_INPUT
            else None
        ),
    )


__all__ = [
    "ImportedReplayFallbackPolicy",
    "ImportedReplayMessageProvenance",
    "ImportedReplayPreparationError",
    "PreparedImportedReplayHistory",
    "prepare_imported_replay_history",
    "prepare_imported_root_input",
]
