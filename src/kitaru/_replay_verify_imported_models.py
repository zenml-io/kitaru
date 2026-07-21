"""Neutral data shapes for imported-input replay verification.

These records describe evidence imported from an external trace source plus the
local-runner contract needed to compare a baseline and candidate. They do not
require a Kitaru execution id because imported traces are not Kitaru checkpoint
replays.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Literal

ImportedEligibilityState = Literal[
    "eligible",
    "partial",
    "ineligible",
    "non_comparable",
    "unsafe_ineligible",
]
ImportedVerdictState = Literal["ship", "caution", "hold"]
SafetyStatus = Literal["safe", "mocked", "unsafe_ineligible"]
RecordedCallKind = Literal["llm", "tool", "retrieval", "evaluator", "other"]

IMPORTED_ELIGIBLE: ImportedEligibilityState = "eligible"
IMPORTED_PARTIAL: ImportedEligibilityState = "partial"
IMPORTED_INELIGIBLE: ImportedEligibilityState = "ineligible"
IMPORTED_NON_COMPARABLE: ImportedEligibilityState = "non_comparable"
IMPORTED_UNSAFE_INELIGIBLE: ImportedEligibilityState = "unsafe_ineligible"

IMPORTED_VERDICT_SHIP: ImportedVerdictState = "ship"
IMPORTED_VERDICT_CAUTION: ImportedVerdictState = "caution"
IMPORTED_VERDICT_HOLD: ImportedVerdictState = "hold"

SAFETY_STATUS_SAFE: SafetyStatus = "safe"
SAFETY_STATUS_MOCKED: SafetyStatus = "mocked"
SAFETY_STATUS_UNSAFE_INELIGIBLE: SafetyStatus = "unsafe_ineligible"

IMPORTED_INPUT_EXECUTION_MODE = (
    "imported_input_fresh_execution_not_deterministic_checkpoint_replay"
)
FIXTURE_HARNESS_EXECUTION_MODE = "fixture_harness_execution_not_real_agent_comparison"
DEFAULT_COMPARISON_FIELDS = (
    "policy_label",
    "risk_status",
    "tool_names",
    "retrieval_document_ids",
)


def execution_mode_detail(execution_mode: str) -> str:
    """Return user-facing wording for imported verification execution modes."""
    if execution_mode == IMPORTED_INPUT_EXECUTION_MODE:
        return "Imported-input fresh execution; not deterministic checkpoint replay."
    if execution_mode == FIXTURE_HARNESS_EXECUTION_MODE:
        return "Fixture evidence; not a real-agent candidate comparison."
    return "Custom imported verification execution mode."


@dataclass(frozen=True)
class ImportedCaseSourceRef:
    """Where the imported case came from."""

    source_system: str
    source_id: str
    observation_ids: list[str] = field(default_factory=list)
    url: str | None = None
    observed_at: str | None = None
    raw_source_ref: str | None = None


@dataclass(frozen=True)
class TenantContext:
    """Tenant and permission evidence recovered from the source trace."""

    tenant_id: str | None = None
    workspace_id: str | None = None
    user_id: str | None = None
    role: str | None = None
    permission_scope: str | None = None


@dataclass(frozen=True)
class RetrievalContext:
    """Recovered retrieval evidence for RAG cases."""

    query: str | None = None
    retriever_name: str | None = None
    corpus_index_version: str | None = None
    top_k: int | None = None
    returned_document_ids: list[str] = field(default_factory=list)
    returned_chunk_ids: list[str] = field(default_factory=list)
    chunk_hashes: list[str] = field(default_factory=list)
    tenant_id: str | None = None
    permission_scope: str | None = None
    retrieval_timestamp: str | None = None
    reranker_name: str | None = None
    reranker_version: str | None = None
    reranker_settings: dict[str, Any] = field(default_factory=dict)
    low_confidence_expected: bool = False


@dataclass(frozen=True)
class RecordedCall:
    """One model, tool, retrieval, or evaluator observation from the source."""

    kind: RecordedCallKind
    name: str
    input_payload: Any = None
    output_payload: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    observation_id: str | None = None
    started_at: str | None = None
    model: str | None = None
    usage: Any = None
    cost: float | None = None
    latency: float | None = None
    node: str | None = None
    call_index: int | None = None


@dataclass(frozen=True)
class ReplayTraceContract:
    """Recovered app/runtime fields that make a trace comparable."""

    trace_contract_version: str | None = None
    app_name: str | None = None
    app_version: str | None = None
    model: str | None = None
    prompt_version: str | None = None
    prompt_hash: str | None = None
    available_tools: list[str] | None = None
    application_tool_names: list[str] = field(default_factory=list)
    side_effect_policy: str | None = None
    tool_registry_version: str | None = None
    recorded_response_control: dict[str, Any] = field(default_factory=dict)
    raw_config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunnerContract:
    """Local code the verifier may call for eligible imported cases."""

    entrypoint: str | None
    execution_mode: str = IMPORTED_INPUT_EXECUTION_MODE
    baseline_id: str | None = None
    candidate_id: str | None = None
    comparison_fields: tuple[str, ...] = DEFAULT_COMPARISON_FIELDS


@dataclass(frozen=True)
class FidelityReport:
    """Validation result for source evidence completeness and comparability."""

    recovered_fields: dict[str, bool]
    missing_fields: list[str]
    eligibility: ImportedEligibilityState
    score: float
    level: Literal["high", "medium", "low"]
    reasons: list[str]
    verdict: ImportedVerdictState


@dataclass(frozen=True)
class SafetyAssessment:
    """Validation result for side-effect and permission safety."""

    status: SafetyStatus
    safe_tools: list[str] = field(default_factory=list)
    mocked_or_blocked_tools: list[str] = field(default_factory=list)
    unsafe_tools: list[str] = field(default_factory=list)
    observed_tool_names: list[str] = field(default_factory=list)
    live_execution_blocked: bool = True
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ImportedReplayCase:
    """One imported-input case ready for validation and possible comparison."""

    case_id: str
    source_ref: ImportedCaseSourceRef
    root_input: Any
    observed_output: Any
    recorded_calls: list[RecordedCall]
    trace_contract: ReplayTraceContract
    runner_contract: RunnerContract | None = None
    tenant_context: TenantContext | None = None
    retrieval_context: RetrievalContext | None = None
    cohort: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    raw_source_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ImportedCaseValidation:
    """Combined validation for one imported case."""

    case_id: str
    fidelity: FidelityReport
    safety: SafetyAssessment
    candidate_execution_allowed: bool
    stop_reasons: list[str]


@dataclass(frozen=True)
class ImportedVerificationReport:
    """Product-shaped report container for imported verification results."""

    name: str
    created_at: str
    source_system: str
    execution_mode: str
    cases: list[ImportedCaseValidation]
    summary: dict[str, Any]
    runner_contract: RunnerContract | None = None
    report_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report payload."""
        return to_plain_data(self)


def imported_case_from_mapping(raw: Mapping[str, Any]) -> ImportedReplayCase:
    """Convert a neutral or trust-boundary JSONL mapping into a case object."""
    if isinstance(raw.get("source_ref"), Mapping):
        return _neutral_case_from_mapping(raw)
    config = _mapping(raw.get("config_hints"))
    source_ref = ImportedCaseSourceRef(
        source_system=str(raw.get("source_system") or "unknown"),
        source_id=_optional_str(raw.get("trace_id") or raw.get("source_id")) or "",
        observation_ids=_string_list(raw.get("observation_ids")),
        url=_optional_str(raw.get("trace_url") or raw.get("source_url")),
        observed_at=_optional_str(
            raw.get("observed_timestamp") or raw.get("observed_at")
        ),
        raw_source_ref=_optional_str(raw.get("raw_source_ref")),
    )
    trace_contract = ReplayTraceContract(
        trace_contract_version=_optional_str(config.get("trace_contract_version")),
        app_name=_optional_str(config.get("app_name")),
        app_version=_optional_str(config.get("app_version")),
        model=_optional_str(config.get("model")),
        prompt_version=_optional_str(config.get("prompt_version")),
        prompt_hash=_optional_str(config.get("prompt_hash")),
        available_tools=_optional_string_list_or_none(
            config.get("available_tool_names", raw.get("available_tools"))
        ),
        application_tool_names=_string_list(config.get("application_tool_names")),
        side_effect_policy=_optional_str(config.get("side_effect_policy")),
        tool_registry_version=_optional_str(config.get("tool_registry_version")),
        recorded_response_control=_mapping(config.get("recorded_response_control")),
        raw_config=dict(config),
    )
    runner_entrypoint = raw.get(
        "local_runner_entrypoint", config.get("local_runner_entrypoint")
    )
    runner_contract = RunnerContract(entrypoint=_optional_str(runner_entrypoint))
    manifest_case = _mapping(raw.get("manifest_case"))
    tenant_context = tenant_context_from_mapping(
        _mapping(raw.get("tenant_context", manifest_case.get("tenant_context")))
    )
    retrieval_context = retrieval_context_from_mapping(
        raw.get("retrieval_context", config.get("retrieval_metadata"))
    )
    return ImportedReplayCase(
        case_id=str(raw.get("case_id") or source_ref.source_id),
        source_ref=source_ref,
        root_input=raw.get("root_input", raw.get("input_payload")),
        observed_output=raw.get("observed_output"),
        recorded_calls=[
            recorded_call_from_mapping(item) for item in raw.get("recorded_calls", [])
        ],
        trace_contract=trace_contract,
        runner_contract=runner_contract,
        tenant_context=tenant_context,
        retrieval_context=retrieval_context,
        cohort=_optional_str(raw.get("cohort")),
        labels={
            str(key): str(value) for key, value in _mapping(raw.get("labels")).items()
        },
        raw_source_payload=dict(raw),
    )


def _neutral_case_from_mapping(raw: Mapping[str, Any]) -> ImportedReplayCase:
    source_ref_raw = _mapping(raw.get("source_ref"))
    trace_contract_raw = _mapping(raw.get("trace_contract"))
    runner_contract_raw = _mapping(raw.get("runner_contract"))
    source_ref = ImportedCaseSourceRef(
        source_system=str(source_ref_raw.get("source_system") or "unknown"),
        source_id=_optional_str(source_ref_raw.get("source_id")) or "",
        observation_ids=_string_list(source_ref_raw.get("observation_ids")),
        url=_optional_str(source_ref_raw.get("url")),
        observed_at=_optional_str(source_ref_raw.get("observed_at")),
        raw_source_ref=_optional_str(source_ref_raw.get("raw_source_ref")),
    )
    runner_contract = None
    if runner_contract_raw:
        comparison_fields = runner_contract_raw.get("comparison_fields")
        runner_contract = RunnerContract(
            entrypoint=_optional_str(runner_contract_raw.get("entrypoint")),
            execution_mode=str(
                runner_contract_raw.get("execution_mode")
                or IMPORTED_INPUT_EXECUTION_MODE
            ),
            baseline_id=_optional_str(runner_contract_raw.get("baseline_id")),
            candidate_id=_optional_str(runner_contract_raw.get("candidate_id")),
            comparison_fields=tuple(_string_list(comparison_fields))
            if isinstance(comparison_fields, list)
            else DEFAULT_COMPARISON_FIELDS,
        )
    return ImportedReplayCase(
        case_id=str(raw.get("case_id") or source_ref.source_id),
        source_ref=source_ref,
        root_input=raw.get("root_input"),
        observed_output=raw.get("observed_output"),
        recorded_calls=[
            recorded_call_from_mapping(item) for item in raw.get("recorded_calls", [])
        ],
        trace_contract=ReplayTraceContract(
            trace_contract_version=_optional_str(
                trace_contract_raw.get("trace_contract_version")
            ),
            app_name=_optional_str(trace_contract_raw.get("app_name")),
            app_version=_optional_str(trace_contract_raw.get("app_version")),
            model=_optional_str(trace_contract_raw.get("model")),
            prompt_version=_optional_str(trace_contract_raw.get("prompt_version")),
            prompt_hash=_optional_str(trace_contract_raw.get("prompt_hash")),
            available_tools=_optional_string_list_or_none(
                trace_contract_raw.get("available_tools")
            ),
            application_tool_names=_string_list(
                trace_contract_raw.get("application_tool_names")
            ),
            side_effect_policy=_optional_str(
                trace_contract_raw.get("side_effect_policy")
            ),
            tool_registry_version=_optional_str(
                trace_contract_raw.get("tool_registry_version")
            ),
            recorded_response_control=_mapping(
                trace_contract_raw.get("recorded_response_control")
            ),
            raw_config=_mapping(trace_contract_raw.get("raw_config")),
        ),
        runner_contract=runner_contract,
        tenant_context=tenant_context_from_mapping(_mapping(raw.get("tenant_context"))),
        retrieval_context=retrieval_context_from_mapping(raw.get("retrieval_context")),
        cohort=_optional_str(raw.get("cohort")),
        labels={
            str(key): str(value) for key, value in _mapping(raw.get("labels")).items()
        },
        raw_source_payload=_neutral_raw_source_payload(raw),
    )


def _neutral_raw_source_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(raw.get("raw_source_payload"))
    for key in ("source_import_reasons", "source_import_summary"):
        if key in raw and key not in payload:
            payload[key] = raw[key]
    return payload


def recorded_call_from_mapping(raw: Mapping[str, Any]) -> RecordedCall:
    """Convert a raw recorded-call mapping into a dataclass."""
    kind = str(raw.get("kind") or "other").lower()
    if kind not in {"llm", "tool", "retrieval", "evaluator", "other"}:
        kind = "other"
    return RecordedCall(
        kind=kind,
        name=str(raw.get("name") or raw.get("tool_name") or "unknown"),
        input_payload=raw.get("input", raw.get("input_payload")),
        output_payload=raw.get("output", raw.get("output_payload")),
        metadata=_mapping(raw.get("metadata")),
        observation_id=_optional_str(raw.get("observation_id")),
        started_at=_optional_str(raw.get("start_time") or raw.get("started_at")),
        model=_optional_str(raw.get("model")),
        usage=raw.get("usage"),
        cost=_optional_float(raw.get("cost")),
        latency=_optional_float(raw.get("latency")),
    )


def tenant_context_from_mapping(raw: Mapping[str, Any]) -> TenantContext | None:
    """Convert a tenant mapping into a dataclass when any field exists."""
    if not raw:
        return None
    return TenantContext(
        tenant_id=_optional_str(raw.get("tenant_id")),
        workspace_id=_optional_str(raw.get("workspace_id")),
        user_id=_optional_str(raw.get("user_id")),
        role=_optional_str(raw.get("role")),
        permission_scope=_optional_str(raw.get("permission_scope")),
    )


def retrieval_context_from_mapping(raw: Any) -> RetrievalContext | None:
    """Convert retrieval metadata into a dataclass when present."""
    if not isinstance(raw, Mapping) or not raw:
        return None
    return RetrievalContext(
        query=_optional_str(raw.get("query")),
        retriever_name=_optional_str(raw.get("retriever_name")),
        corpus_index_version=_optional_str(raw.get("corpus_index_version")),
        top_k=_optional_int(raw.get("top_k")),
        returned_document_ids=_string_list(raw.get("returned_document_ids")),
        returned_chunk_ids=_string_list(raw.get("returned_chunk_ids")),
        chunk_hashes=_string_list(raw.get("chunk_hashes")),
        tenant_id=_optional_str(raw.get("tenant_id")),
        permission_scope=_optional_str(raw.get("permission_scope")),
        retrieval_timestamp=_optional_str(raw.get("retrieval_timestamp")),
        reranker_name=_optional_str(raw.get("reranker_name")),
        reranker_version=_optional_str(raw.get("reranker_version")),
        reranker_settings=_mapping(raw.get("reranker_settings")),
        low_confidence_expected=bool(raw.get("low_confidence_expected", False)),
    )


def to_plain_data(value: Any) -> Any:
    """Convert dataclasses and nested containers into JSON-safe plain data."""
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_plain_data(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {str(key): to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [to_plain_data(item) for item in value]
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _optional_string_list_or_none(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        return None
    return [str(item) for item in value]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
