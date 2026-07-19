"""Deterministic history and recorded-tool tests for PydanticAI imported replay."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic_ai import Agent
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.capabilities import WebSearch
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.adapters.pydantic_ai._agent import (
    _imported_replay_member_evidence,
    _pydantic_ai_replay_manifest,
    _verified_replay_execution_ids,
)
from kitaru.adapters.pydantic_ai._imported_replay import (
    ImportedReplayFallbackPolicy,
    ImportedReplayPreparationError,
    prepare_imported_replay_history,
)
from kitaru.adapters.pydantic_ai._recorded_tools import (
    RecordedResponseBlockReason,
    RecordedResponseDecision,
    compile_recorded_responses,
)
from kitaru.errors import KitaruUsageError
from kitaru.imports import (
    ImportedEvidenceArtifactIdentity,
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ImportedReplayEvidenceIdentity,
    ImportedReplayMode,
    ImportedReplayUnsupportedReason,
    PreparedImportedReplayEvidence,
    ReplayPartKind,
    build_pydantic_ai_replay_evidence,
    build_raw_imported_evidence,
    read_langfuse_jsonl_records,
)
from kitaru.imports._normalization import normalize_langfuse_records
from kitaru.runtime import _flow_scope
from kitaru.scoring import ImportedReplayComparability

_FIXTURE = (
    Path(__file__).parent / "imports" / "fixtures" / "langfuse_replay_evidence.jsonl"
)


def _prepared_evidence() -> PreparedImportedReplayEvidence:
    records = [
        record
        for record in read_langfuse_jsonl_records(_FIXTURE)
        if record.row["traceId"] == "trace-alias"
    ]
    normalized = normalize_langfuse_records(
        records,
        project_id="source-project",
    )[0]
    raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    replay = build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )
    identity = ImportedReplayEvidenceIdentity(
        execution_id="imported-execution",
        project_id="agent-project",
        source_agent_version_id="source-pipeline",
        source_pipeline_id="source-pipeline",
        source_fingerprint="source-fingerprint",
        source_provider=raw.source.provider,
        source_project_id=raw.source.project_id,
        source_trace_id=raw.source.trace_id,
        raw_evidence=ImportedEvidenceArtifactIdentity(
            artifact_id="raw-artifact",
            schema_version=raw.schema_version,
            sha256=raw.raw_content_sha256,
        ),
        replay_bundle=ImportedEvidenceArtifactIdentity(
            artifact_id="replay-artifact",
            schema_version=replay.bundle.schema_version,
            sha256=replay.bundle.bundle_digest,
        ),
        replay_profile_version=replay.bundle.profile_version,
    )
    return PreparedImportedReplayEvidence(
        identity=identity,
        raw_evidence=raw,
        replay_bundle=replay.bundle,
        readiness=replay.readiness,
    )


def _boundary(
    evidence: PreparedImportedReplayEvidence,
    kind: ReplayPartKind,
    *,
    occurrence_index: int = 0,
) -> ImportedReplayBoundary:
    parts = [
        part
        for observation in evidence.replay_bundle.observations
        for part in observation.parts
        if part.kind is kind
    ]
    part = parts[occurrence_index]
    return ImportedReplayBoundary(
        kind=(
            ImportedReplayBoundaryKind.TOOL_RESULT
            if kind is ReplayPartKind.TOOL_RESULT
            else ImportedReplayBoundaryKind.MODEL_MESSAGE
        ),
        observation_id=part.observation_id,
        sequence=part.sequence,
        occurrence=part.occurrence,
        call_id=part.call_id,
    )


def _with_tool_arguments(
    evidence: PreparedImportedReplayEvidence,
    arguments: Any,
) -> PreparedImportedReplayEvidence:
    observations = []
    for observation in evidence.replay_bundle.observations:
        parts = tuple(
            part.model_copy(update={"content": arguments})
            if part.kind is ReplayPartKind.TOOL_CALL
            else part
            for part in observation.parts
        )
        observations.append(observation.model_copy(update={"parts": parts}))
    bundle = evidence.replay_bundle.model_copy(
        update={"observations": tuple(observations)}
    )
    return evidence.model_copy(update={"replay_bundle": bundle})


def test_prepares_complete_tool_result_boundary_with_pydantic_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _prepared_evidence()
    boundary = _boundary(evidence, ReplayPartKind.TOOL_RESULT)
    original_validate = pydantic_messages.ModelMessagesTypeAdapter.validate_python
    validation_calls = 0

    def validate_messages(*args: Any, **kwargs: Any) -> Any:
        nonlocal validation_calls
        validation_calls += 1
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(
        pydantic_messages.ModelMessagesTypeAdapter,
        "validate_python",
        validate_messages,
    )

    prepared = prepare_imported_replay_history(
        evidence,
        boundary=boundary,
    )

    assert validation_calls == 1
    assert prepared.boundary == boundary
    assert prepared.fallback_policy is ImportedReplayFallbackPolicy.ROOT_INPUT
    assert prepared.fallback_root_input == evidence.replay_bundle.root_input
    assert len(prepared.message_history) == 3
    assert isinstance(prepared.message_history[-1], pydantic_messages.ModelRequest)
    tool_return = prepared.message_history[-1].parts[0]
    assert isinstance(tool_return, pydantic_messages.ToolReturnPart)
    assert tool_return.tool_call_id == "call-1"
    assert tool_return.content == {"answer": "y"}
    assert prepared.message_provenance[-1].last_occurrence == boundary.occurrence


def test_prepares_complete_final_model_message_boundary() -> None:
    evidence = _prepared_evidence()
    boundary = _boundary(
        evidence,
        ReplayPartKind.MODEL_TEXT,
        occurrence_index=1,
    )

    prepared = prepare_imported_replay_history(
        evidence,
        boundary=boundary,
        fallback_policy=ImportedReplayFallbackPolicy.BLOCK,
    )

    assert len(prepared.message_history) == 4
    final = prepared.message_history[-1]
    assert isinstance(final, pydantic_messages.ModelResponse)
    assert final.parts == [pydantic_messages.TextPart(content="done")]
    assert prepared.fallback_root_input is None


def test_rejects_model_position_inside_incomplete_tool_exchange() -> None:
    evidence = _prepared_evidence()
    boundary = _boundary(evidence, ReplayPartKind.MODEL_TEXT)

    with pytest.raises(ImportedReplayPreparationError) as exc_info:
        prepare_imported_replay_history(evidence, boundary=boundary)

    assert exc_info.value.reason is ImportedReplayUnsupportedReason.BOUNDARY_INCOMPLETE


def test_rejects_reordered_tool_result_before_candidate_run() -> None:
    evidence = _prepared_evidence()
    observations = []
    for observation in evidence.replay_bundle.observations:
        parts = tuple(
            part.model_copy(update={"call_id": "different-call"})
            if part.kind is ReplayPartKind.TOOL_RESULT
            else part
            for part in observation.parts
        )
        observations.append(observation.model_copy(update={"parts": parts}))
    invalid = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={"observations": tuple(observations)}
            )
        }
    )
    boundary = _boundary(invalid, ReplayPartKind.TOOL_RESULT)

    with pytest.raises(ImportedReplayPreparationError) as exc_info:
        prepare_imported_replay_history(invalid, boundary=boundary)

    assert (
        exc_info.value.reason is ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID
    )


def test_source_message_indices_distinguish_separate_assistant_messages() -> None:
    evidence = _prepared_evidence()
    observation = evidence.replay_bundle.observations[0]
    parts = tuple(
        part.model_copy(update={"content": "thinking"})
        if part.kind is ReplayPartKind.MODEL_TEXT and part.occurrence == 1
        else part.model_copy(update={"message_index": 2})
        if part.kind is ReplayPartKind.TOOL_CALL
        else part.model_copy(update={"message_index": 3})
        if part.kind is ReplayPartKind.TOOL_RESULT
        else part.model_copy(update={"message_index": 4})
        if part.kind is ReplayPartKind.MODEL_TEXT
        else part
        for part in observation.parts
    )
    separated = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={
                    "observations": (observation.model_copy(update={"parts": parts}),)
                }
            )
        }
    )
    boundary = _boundary(separated, ReplayPartKind.MODEL_TEXT)

    prepared = prepare_imported_replay_history(separated, boundary=boundary)

    assert len(prepared.message_history) == 2
    response = prepared.message_history[-1]
    assert isinstance(response, pydantic_messages.ModelResponse)
    assert response.parts == [pydantic_messages.TextPart(content="thinking")]


def test_consecutive_same_role_messages_are_not_merged() -> None:
    evidence = _prepared_evidence()
    observation = evidence.replay_bundle.observations[0]
    user = observation.parts[0]
    first_response = observation.parts[1].model_copy(
        update={"content": "first", "occurrence": 1, "message_index": 1}
    )
    second_response = observation.parts[-1].model_copy(
        update={"content": "second", "occurrence": 2, "message_index": 2}
    )
    updated = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={
                    "observations": (
                        observation.model_copy(
                            update={"parts": (user, first_response, second_response)}
                        ),
                    )
                }
            )
        }
    )

    prepared = prepare_imported_replay_history(
        updated,
        boundary=_boundary(
            updated,
            ReplayPartKind.MODEL_TEXT,
            occurrence_index=1,
        ),
    )

    assert len(prepared.message_history) == 3
    contents: list[str] = []
    for message in prepared.message_history[1:]:
        assert isinstance(message, pydantic_messages.ModelResponse)
        part = message.parts[0]
        assert isinstance(part, pydantic_messages.TextPart)
        contents.append(part.content)
    assert contents == ["first", "second"]


def test_multi_part_tool_return_message_preserves_one_exact_boundary() -> None:
    evidence = _prepared_evidence()
    observation = evidence.replay_bundle.observations[0]
    user, model_text, source_call, source_result, final_text = observation.parts
    parts = (
        user,
        model_text.model_copy(update={"content": "calling tools"}),
        source_call,
        source_call.model_copy(update={"call_id": "call-2", "occurrence": 3}),
        source_result.model_copy(update={"occurrence": 4}),
        source_result.model_copy(update={"call_id": "call-2", "occurrence": 5}),
        final_text.model_copy(update={"occurrence": 6}),
    )
    updated = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={
                    "observations": (observation.model_copy(update={"parts": parts}),)
                }
            )
        }
    )

    prepared = prepare_imported_replay_history(
        updated,
        boundary=_boundary(
            updated,
            ReplayPartKind.TOOL_RESULT,
            occurrence_index=1,
        ),
    )

    request = prepared.message_history[-1]
    assert isinstance(request, pydantic_messages.ModelRequest)
    assert [
        part.tool_call_id
        for part in request.parts
        if isinstance(part, pydantic_messages.ToolReturnPart)
    ] == ["call-1", "call-2"]


def test_missing_source_message_boundary_is_rejected() -> None:
    evidence = _prepared_evidence()
    observation = evidence.replay_bundle.observations[0]
    parts = tuple(
        part.model_copy(update={"message_index": None})
        if part.kind is ReplayPartKind.MODEL_TEXT
        else part
        for part in observation.parts
    )
    ambiguous = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={
                    "observations": (observation.model_copy(update={"parts": parts}),)
                }
            )
        }
    )

    with pytest.raises(ImportedReplayPreparationError) as exc_info:
        prepare_imported_replay_history(
            ambiguous,
            boundary=_boundary(
                ambiguous,
                ReplayPartKind.MODEL_TEXT,
                occurrence_index=1,
            ),
        )

    assert (
        exc_info.value.reason is ImportedReplayUnsupportedReason.MESSAGE_HISTORY_INVALID
    )


def _toolset(*, effect: str = "read_only", integer_argument: bool = False) -> Any:
    toolset: FunctionToolset[Any] = FunctionToolset()
    metadata = {
        "kitaru_replay": {
            "logical_id": "support.lookup",
            "aliases": ["lookup"],
            "effect": effect,
        }
    }
    if integer_argument:

        @toolset.tool_plain(metadata=metadata)
        def lookup(q: int) -> dict[str, str]:
            raise AssertionError("the original candidate tool must not execute")

    else:

        @toolset.tool_plain(metadata=metadata)
        def lookup(q: str) -> dict[str, str]:
            raise AssertionError("the original candidate tool must not execute")

    return toolset


def _model(arguments: dict[str, Any]) -> FunctionModel:
    def respond(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        if (
            messages
            and isinstance(messages[-1], pydantic_messages.ModelRequest)
            and any(
                isinstance(part, pydantic_messages.ToolReturnPart)
                for part in messages[-1].parts
            )
        ):
            return pydantic_messages.ModelResponse(
                parts=[pydantic_messages.TextPart(content="complete")]
            )
        return pydantic_messages.ModelResponse(
            parts=[
                pydantic_messages.ToolCallPart(
                    tool_name="lookup",
                    args=arguments,
                    tool_call_id="candidate-call",
                )
            ]
        )

    return FunctionModel(respond)


def _disable_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    agent: KitaruAgent[Any, str],
) -> None:
    async def run_without_checkpoint(*, body: Any, **_kwargs: Any) -> Any:
        return await body()

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
        run_without_checkpoint,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        run_without_checkpoint,
    )
    monkeypatch.setattr(agent, "_should_track", lambda: False)


@pytest.mark.parametrize("effect", ["read_only", "write"])
def test_recorded_hit_never_invokes_read_or_write_callable(
    monkeypatch: pytest.MonkeyPatch,
    effect: str,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    toolset = _toolset(effect=effect)
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    agent = KitaruAgent(
        Agent(
            _model({"q": "x"}),
            name="recorded_hit",
            output_type=str,
            toolsets=[toolset],
        )
    )
    _disable_checkpoints(monkeypatch, agent)

    with _flow_scope(name="recorded_hit_flow"), runtime.install(agent):
        result = agent.run_sync("continue")

    assert result.output == "complete"
    assert len(runtime.report.events) == 1
    event = runtime.report.events[0]
    assert event.decision is RecordedResponseDecision.HIT
    assert event.source_call_id == "call-1"
    assert event.index == 0


def test_occurrence_is_consumed_once_and_miss_has_no_live_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    toolset = _toolset()
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    agent = KitaruAgent(
        Agent(
            _model({"q": "x"}),
            name="recorded_single_use",
            output_type=str,
            toolsets=[toolset],
        )
    )
    _disable_checkpoints(monkeypatch, agent)

    with _flow_scope(name="recorded_first"), runtime.install(agent):
        agent.run_sync("first")
    with _flow_scope(name="recorded_second"), runtime.install(agent):
        agent.run_sync("second")

    events = runtime.report.events
    assert [event.index for event in events] == [0, 1]
    assert [event.decision for event in events] == [
        RecordedResponseDecision.HIT,
        RecordedResponseDecision.BLOCKED,
    ]
    assert events[-1].block_reason is RecordedResponseBlockReason.OCCURRENCE_UNAVAILABLE


def test_recorded_responses_enforce_order_across_tools() -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    observation = evidence.replay_bundle.observations[0]
    source_call = next(
        part for part in observation.parts if part.kind is ReplayPartKind.TOOL_CALL
    )
    source_result = next(
        part for part in observation.parts if part.kind is ReplayPartKind.TOOL_RESULT
    )
    parts = tuple(
        part.model_copy(update={"name": "first"})
        if part is source_call or part is source_result
        else part.model_copy(update={"occurrence": 6})
        if part.kind is ReplayPartKind.MODEL_TEXT and part.content == "done"
        else part
        for part in observation.parts
    )
    parts = (
        *parts[:-1],
        source_call.model_copy(
            update={"name": "second", "call_id": "call-2", "occurrence": 4}
        ),
        source_result.model_copy(
            update={"name": "second", "call_id": "call-2", "occurrence": 5}
        ),
        parts[-1],
    )
    evidence = evidence.model_copy(
        update={
            "replay_bundle": evidence.replay_bundle.model_copy(
                update={
                    "observations": (observation.model_copy(update={"parts": parts}),)
                }
            )
        }
    )
    toolset: FunctionToolset[Any] = FunctionToolset()

    def metadata(logical_id: str) -> dict[str, Any]:
        return {
            "kitaru_replay": {
                "logical_id": logical_id,
                "aliases": [logical_id],
                "effect": "read_only",
            }
        }

    @toolset.tool_plain(metadata=metadata("first"))
    def first(q: str) -> dict[str, str]:
        raise AssertionError("the original first tool must not execute")

    @toolset.tool_plain(metadata=metadata("second"))
    def second(q: str) -> dict[str, str]:
        raise AssertionError("the original second tool must not execute")

    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    context: Any = SimpleNamespace(tool_call_id="candidate-call", retry=0)

    runtime._serve(
        candidate=runtime._candidates["second"],
        ctx=context,
        arguments={"q": "x"},
    )
    runtime._serve(
        candidate=runtime._candidates["first"],
        ctx=context,
        arguments={"q": "x"},
    )

    assert [event.decision for event in runtime.report.events] == [
        RecordedResponseDecision.BLOCKED,
        RecordedResponseDecision.HIT,
    ]
    assert (
        runtime.report.events[0].block_reason
        is RecordedResponseBlockReason.OCCURRENCE_REORDERED
    )
    assert runtime.remaining_occurrence_count == 1


def test_argument_miss_blocks_without_consuming_the_expected_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    toolset = _toolset()
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    wrong_agent = KitaruAgent(
        Agent(
            _model({"q": "different"}),
            name="recorded_argument_miss",
            output_type=str,
            toolsets=[toolset],
        )
    )
    matching_agent = KitaruAgent(
        Agent(
            _model({"q": "x"}),
            name="recorded_argument_hit",
            output_type=str,
            toolsets=[toolset],
        )
    )
    _disable_checkpoints(monkeypatch, wrong_agent)
    _disable_checkpoints(monkeypatch, matching_agent)

    with _flow_scope(name="recorded_argument_miss"), runtime.install(wrong_agent):
        wrong_agent.run_sync("miss")
    with _flow_scope(name="recorded_argument_hit"), runtime.install(matching_agent):
        matching_agent.run_sync("hit")

    assert [event.decision for event in runtime.report.events] == [
        RecordedResponseDecision.BLOCKED,
        RecordedResponseDecision.HIT,
    ]
    assert (
        runtime.report.events[0].block_reason
        is RecordedResponseBlockReason.ARGUMENT_MISMATCH
    )


def test_argument_type_drift_is_blocked_without_coercion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "40"})
    toolset = _toolset(integer_argument=True)
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    agent = KitaruAgent(
        Agent(
            _model({"q": 40}),
            name="recorded_type_drift",
            output_type=str,
            toolsets=[toolset],
        )
    )
    _disable_checkpoints(monkeypatch, agent)

    with _flow_scope(name="recorded_type_drift"), runtime.install(agent):
        agent.run_sync("continue")

    assert runtime.report.events[0].decision is RecordedResponseDecision.BLOCKED
    assert (
        runtime.report.events[0].block_reason
        is RecordedResponseBlockReason.CONTRACT_MISMATCH
    )


def test_candidate_argument_type_miss_is_reported_and_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": 40})
    toolset = _toolset(integer_argument=True)
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    agent = KitaruAgent(
        Agent(
            _model({"q": "40"}),
            name="recorded_candidate_type_miss",
            output_type=str,
            toolsets=[toolset],
        )
    )
    _disable_checkpoints(monkeypatch, agent)

    with _flow_scope(name="recorded_candidate_type_miss"), runtime.install(agent):
        agent.run_sync("continue")

    assert runtime.report.events[0].decision is RecordedResponseDecision.BLOCKED
    assert (
        runtime.report.events[0].block_reason
        is RecordedResponseBlockReason.ARGUMENTS_INVALID
    )


@pytest.mark.parametrize(
    "case",
    ["unknown_source_tool", "missing_candidate_contract", "invalid_result"],
)
def test_incompatible_recorded_occurrences_remain_misses_without_candidate_calls(
    case: str,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    observation = evidence.replay_bundle.observations[0]
    if case == "unknown_source_tool":
        parts = tuple(
            part.model_copy(update={"name": "unknown"})
            if part.kind in {ReplayPartKind.TOOL_CALL, ReplayPartKind.TOOL_RESULT}
            else part
            for part in observation.parts
        )
        evidence = evidence.model_copy(
            update={
                "replay_bundle": evidence.replay_bundle.model_copy(
                    update={
                        "observations": (
                            observation.model_copy(update={"parts": parts}),
                        )
                    }
                )
            }
        )
    elif case == "invalid_result":
        parts = tuple(
            part.model_copy(update={"content": ["invalid"]})
            if part.kind is ReplayPartKind.TOOL_RESULT
            else part
            for part in observation.parts
        )
        evidence = evidence.model_copy(
            update={
                "replay_bundle": evidence.replay_bundle.model_copy(
                    update={
                        "observations": (
                            observation.model_copy(update={"parts": parts}),
                        )
                    }
                )
            }
        )

    source_toolset = _toolset()
    source_manifest = _pydantic_ai_replay_manifest([source_toolset])
    candidate_toolsets = (
        [] if case == "missing_candidate_contract" else [source_toolset]
    )
    candidate_manifest = _pydantic_ai_replay_manifest(candidate_toolsets)
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=source_manifest,
        candidate_manifest=candidate_manifest,
        candidate_toolsets=candidate_toolsets,
    )

    replay_plan = SimpleNamespace(
        mode=ImportedReplayMode.ROOT_INPUT,
        boundary=ImportedReplayBoundary(kind=ImportedReplayBoundaryKind.ROOT_INPUT),
    )
    member = _imported_replay_member_evidence(
        spec=cast(
            Any,
            SimpleNamespace(
                experiment_id="experiment",
                candidate_agent_version_id="candidate-version",
            ),
        ),
        trial=cast(
            Any,
            SimpleNamespace(
                target_execution_id="target",
                repeat_index=0,
                parent_execution_id="parent",
                root_execution_id="root",
            ),
        ),
        replay_plan=cast(Any, replay_plan),
        evidence=evidence,
        capture=cast(Any, SimpleNamespace(execution_id="child")),
        runtime=runtime,
        candidate_status="completed",
    )

    assert runtime.report.events == ()
    assert runtime.eligible_occurrence_count == 1
    assert runtime.incompatible_occurrence_count == 1
    assert runtime.candidate_tool_contract_compatible is False
    assert member.candidate_tool_contract_compatible is False
    assert member.recorded_response_misses == 1
    assert member.path_diverged is True
    assert member.comparability is ImportedReplayComparability.DEGRADED


def test_agent_level_provider_native_capability_is_removed_before_model_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_native_tools: list[Any] = []

    def respond(
        _messages: list[pydantic_messages.ModelMessage],
        info: Any,
    ) -> pydantic_messages.ModelResponse:
        observed_native_tools.extend(info.model_request_parameters.native_tools)
        return pydantic_messages.ModelResponse(
            parts=[pydantic_messages.TextPart(content="complete")]
        )

    agent = KitaruAgent(
        Agent(
            FunctionModel(respond),
            name="provider_native_default_deny",
            output_type=str,
            capabilities=[WebSearch()],
        )
    )
    _disable_checkpoints(monkeypatch, agent)

    with (
        _flow_scope(name="provider_native_default_deny"),
        agent._replace_tool_sources(()),
    ):
        result = agent.run_sync("continue")

    assert result.output == "complete"
    assert observed_native_tools == []


def test_imported_recovery_uses_frozen_member_order_not_backend_order() -> None:
    members = [
        SimpleNamespace(child_execution_id="child-first", candidate_status="completed"),
        SimpleNamespace(
            child_execution_id="child-second", candidate_status="completed"
        ),
    ]
    record = SimpleNamespace(
        counts=SimpleNamespace(verified=2),
        imported_replay_evidence=object(),
        imported_replay_members=members,
        unverified_children=[],
    )
    result = SimpleNamespace(
        record=record,
        submission=SimpleNamespace(results=[]),
        runs=SimpleNamespace(
            list=lambda **_kwargs: SimpleNamespace(
                items=[
                    SimpleNamespace(id="child-second"),
                    SimpleNamespace(id="child-first"),
                ]
            )
        ),
    )

    assert _verified_replay_execution_ids(cast(Any, result)) == [
        "child-first",
        "child-second",
    ]


@pytest.mark.anyio
async def test_recorded_responses_reject_streaming_before_model_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _with_tool_arguments(_prepared_evidence(), {"q": "x"})
    toolset = _toolset()
    manifest = _pydantic_ai_replay_manifest([toolset])
    runtime = compile_recorded_responses(
        evidence,
        source_manifest=manifest,
        candidate_manifest=manifest,
        candidate_toolsets=[toolset],
    )
    model_calls = 0

    def respond(*_args: Any, **_kwargs: Any) -> pydantic_messages.ModelResponse:
        nonlocal model_calls
        model_calls += 1
        return pydantic_messages.ModelResponse(
            parts=[pydantic_messages.TextPart(content="must not run")]
        )

    agent = KitaruAgent(
        Agent(
            FunctionModel(respond),
            name="recorded_streaming",
            output_type=str,
            toolsets=[toolset],
        )
    )
    monkeypatch.setattr(agent, "_should_track", lambda: False)

    with (
        runtime.install(agent),
        pytest.raises(KitaruUsageError, match="do not support streaming"),
    ):
        async with agent.run_stream("continue"):
            pass

    assert model_calls == 0
    assert runtime.report.events == ()
