"""Replay planning utilities.

Translates Kitaru's unified replay override surface into ZenML replay inputs
consumed by ``Pipeline.replay(...)`` plus a small runtime context consumed by
Kitaru checkpoints during the live replay tail.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from zenml.models import PipelineRunResponse, PipelineRunUpdate, StepRunResponse

from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay_context import ReplayRuntimeContext

_TOOL_CHECKPOINT_SUFFIX = re.compile(r"_tool(?:_\d+)?$")
_MODEL_REQUEST_CHECKPOINT_SUFFIX = re.compile(r"^(?P<base>.+_model_request)(?:_\d+)?$")
_ALLOWED_OVERRIDE_FIELDS = frozenset({"input", "output", "model", "code"})
logger = logging.getLogger(__name__)

REPLAY_RESERVED_KWARGS = frozenset(
    {
        "at",
        "flow_overrides",
        "checkpoint_overrides",
        "invocation_overrides",
        "skip",
        "tag",
        "wait",
        "on_error",
        "stack",
        "image",
        "cache",
        "retries",
    }
)


@dataclass(frozen=True)
class ReplayPlanDocument:
    """Serializable description of the replay request and matched targets."""

    flow_overrides: dict[str, Any] = field(default_factory=dict)
    checkpoint_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    invocation_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    skip: list[str] = field(default_factory=list)
    matched_targets: dict[str, list[str]] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "flow_overrides": self.flow_overrides,
            "checkpoint_overrides": self.checkpoint_overrides,
            "invocation_overrides": self.invocation_overrides,
            "skip": list(self.skip),
        }
        if self.matched_targets:
            payload["matched_targets"] = {
                key: list(value) for key, value in self.matched_targets.items()
            }
        return payload


@dataclass(frozen=True)
class ReplayResultRow:
    """One successfully submitted replay child."""

    original_exec_ref: str
    original_exec_id: str
    replay_exec_id: str
    status: Literal["submitted", "completed", "failed"]
    compare_url: str | None = None
    handle: Any | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "original_exec_ref": self.original_exec_ref,
            "original_exec_id": self.original_exec_id,
            "replay_exec_id": self.replay_exec_id,
            "status": self.status,
            "compare_url": self.compare_url,
        }


@dataclass(frozen=True)
class ReplayFailureRow:
    """One parent that failed before or during replay submission/completion."""

    original_exec_ref: str
    original_exec_id: str | None
    reason: str

    def to_json(self) -> dict[str, Any]:
        return {
            "original_exec_ref": self.original_exec_ref,
            "original_exec_id": self.original_exec_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ReplaySkippedRow:
    """One parent intentionally skipped by batch collect-mode validation."""

    original_exec_ref: str
    original_exec_id: str | None
    reason: str

    def to_json(self) -> dict[str, Any]:
        return {
            "original_exec_ref": self.original_exec_ref,
            "original_exec_id": self.original_exec_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ReplaySummary:
    """Aggregate replay submission counts."""

    submitted: int
    completed: int
    failed: int
    skipped: int

    def to_json(self) -> dict[str, int]:
        return {
            "submitted": self.submitted,
            "completed": self.completed,
            "failed": self.failed,
            "skipped": self.skipped,
        }


@dataclass(frozen=True)
class ReplaySubmission:
    """Shared replay result returned by SDK, CLI, and MCP surfaces."""

    submission_id: str
    tag: str | None
    at: str
    wait: bool
    plan: ReplayPlanDocument
    results: list[ReplayResultRow]
    failures: list[ReplayFailureRow]
    skipped: list[ReplaySkippedRow]
    summary: ReplaySummary
    compare_url: str | None = None

    @classmethod
    def create(
        cls,
        *,
        tag: str | None,
        at: str,
        wait: bool,
        plan: ReplayPlanDocument,
        results: list[ReplayResultRow] | None = None,
        failures: list[ReplayFailureRow] | None = None,
        skipped: list[ReplaySkippedRow] | None = None,
        compare_url: str | None = None,
        submission_id: str | None = None,
    ) -> ReplaySubmission:
        resolved_results = list(results or [])
        resolved_failures = list(failures or [])
        resolved_skipped = list(skipped or [])
        summary = ReplaySummary(
            submitted=len(resolved_results),
            completed=sum(1 for row in resolved_results if row.status == "completed"),
            failed=len(resolved_failures)
            + sum(1 for row in resolved_results if row.status == "failed"),
            skipped=len(resolved_skipped),
        )
        return cls(
            submission_id=submission_id or new_replay_submission_id(),
            tag=tag,
            at=at,
            wait=wait,
            plan=plan,
            results=resolved_results,
            failures=resolved_failures,
            skipped=resolved_skipped,
            summary=summary,
            compare_url=compare_url,
        )

    def wait_for_handles(self) -> ReplaySubmission:
        """Block on live handles and return a new submission without mutating rows."""
        for row in self.results:
            handle = row.handle
            if handle is not None and callable(getattr(handle, "wait", None)):
                handle.wait()
        return self

    def to_json(self) -> dict[str, Any]:
        """Serialize the replay submission without live SDK handles."""
        return {
            "submission_id": self.submission_id,
            "tag": self.tag,
            "at": self.at,
            "wait": self.wait,
            "plan": self.plan.to_json(),
            "results": [row.to_json() for row in self.results],
            "failures": [row.to_json() for row in self.failures],
            "skipped": [row.to_json() for row in self.skipped],
            "summary": self.summary.to_json(),
            "compare_url": self.compare_url,
        }


def safe_compare_url_for_executions(exec_ids: Sequence[str]) -> str | None:
    """Build a compare URL without letting URL lookup break replay results."""
    try:
        from kitaru.diff import compare_url_for_executions

        return compare_url_for_executions(exec_ids)
    except Exception:
        logger.debug("Failed to build replay compare URL.", exc_info=True)
        return None


def _safe_apply_replay_tag(replay_exec_id: str, tag: str | None) -> None:
    """Best-effort native run tag application for replay children."""
    if not tag:
        return
    try:
        from zenml.client import Client

        client = Client()
        update_run = getattr(getattr(client, "zen_store", None), "update_run", None)
        if callable(update_run):
            # Prefer ZenML's store-level run update; it expects a model, not a
            # plain dict, so that backend code can read fields such as add_tags.
            try:
                update_run(
                    run_id=replay_exec_id,
                    run_update=PipelineRunUpdate(add_tags=[tag]),
                )
                return
            except Exception:
                logger.debug("ZenML run tag update failed.", exc_info=True)
        add_run_tags = getattr(client, "add_run_tags", None)
        if callable(add_run_tags):
            add_run_tags(replay_exec_id, [tag])
    except Exception:
        logger.debug(
            "Failed to apply replay tag %s to %s.",
            tag,
            replay_exec_id,
            exc_info=True,
        )


def safe_persist_replay_submission_metadata(
    *,
    replay_exec_id: str,
    original_exec_id: str,
    submission_id: str,
    tag: str | None,
) -> None:
    """Best-effort replay correlation metadata and tag persistence."""
    try:
        from kitaru.logging import log_to_execution

        metadata: dict[str, Any] = {
            "submission_id": submission_id,
            "original_exec_id": original_exec_id,
        }
        if tag:
            metadata["replay_tag"] = tag
        log_to_execution(replay_exec_id, **metadata)
    except Exception:
        logger.debug(
            "Failed to persist replay metadata for %s.",
            replay_exec_id,
            exc_info=True,
        )
    _safe_apply_replay_tag(replay_exec_id, tag)


def new_replay_submission_id() -> str:
    """Return a non-sensitive replay submission correlation ID."""
    return f"rs-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for ``Pipeline.replay(...)``."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]
    runtime_context: ReplayRuntimeContext
    document: ReplayPlanDocument = field(default_factory=ReplayPlanDocument)


ReplayAtStatus = Literal["present", "missing", "ambiguous", "no_checkpoints"]


@dataclass(frozen=True)
class _Checkpoint:
    """Checkpoint metadata used during replay planning."""

    invocation_id: str
    call_id: str
    name: str
    step: StepRunResponse
    started_at: datetime | None
    checkpoint_type: str | None

    @property
    def target_keys(self) -> set[str]:
        return {self.invocation_id, self.call_id, self.name}


def _checkpoint_invocation_id(step: StepRunResponse) -> str:
    invocation_id = getattr(getattr(step, "spec", None), "invocation_id", None)
    if not isinstance(invocation_id, str) or not invocation_id:
        return step.name
    return invocation_id


def _checkpoint_type(step: StepRunResponse) -> str | None:
    step_type = getattr(step, "type", None)
    if step_type is None:
        return None
    value = getattr(step_type, "value", step_type)
    return str(value) if value is not None else None


def _checkpoints(run: PipelineRunResponse) -> list[_Checkpoint]:
    checkpoints: list[_Checkpoint] = []
    for step in run.steps.values():
        checkpoints.append(
            _Checkpoint(
                invocation_id=_checkpoint_invocation_id(step),
                call_id=str(step.id),
                name=_normalize_checkpoint_name(step.name),
                step=step,
                started_at=getattr(step, "start_time", None),
                checkpoint_type=_checkpoint_type(step),
            )
        )
    return checkpoints


def _available_checkpoint_selectors(checkpoints: Sequence[_Checkpoint]) -> str:
    names = sorted({checkpoint.name for checkpoint in checkpoints})
    if not names:
        return "none"
    return ", ".join(names)


def replay_at_status(
    *,
    run: PipelineRunResponse,
    at: str,
) -> ReplayAtStatus:
    """Return whether ``at`` resolves for a source execution."""
    checkpoints = _checkpoints(run)
    if not checkpoints:
        return "no_checkpoints"

    matches = [checkpoint for checkpoint in checkpoints if at in checkpoint.target_keys]
    if len(matches) == 1:
        return "present"
    if len(matches) > 1:
        return "ambiguous"
    return "missing"


def replay_at_skip_reason(*, run: PipelineRunResponse, at: str) -> str:
    """Return a human-readable skip/failure reason for a replay ``at`` selector."""
    status = replay_at_status(run=run, at=at)
    if status == "no_checkpoints":
        return f"Execution '{run.id}' has no checkpoint history to replay."
    if status == "missing":
        checkpoints = _checkpoints(run)
        return (
            f"Unknown checkpoint selector '{at}'. Available checkpoints: "
            f"{_available_checkpoint_selectors(checkpoints)}."
        )
    call_ids = ", ".join(
        sorted(
            {
                checkpoint.call_id
                for checkpoint in _checkpoints(run)
                if at in checkpoint.target_keys
            }
        )
    )
    return (
        f"Replay selector '{at}' is ambiguous. Use a checkpoint call ID instead. "
        f"Matching call IDs: {call_ids}."
    )


def _resolve_checkpoint_selector(
    selector: str,
    checkpoints: Sequence[_Checkpoint],
) -> _Checkpoint:
    matches = [
        checkpoint for checkpoint in checkpoints if selector in checkpoint.target_keys
    ]

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        call_ids = ", ".join(sorted({match.call_id for match in matches}))
        raise KitaruStateError(
            "Replay selector is ambiguous for checkpoint "
            f"'{selector}'. Use a checkpoint call ID instead. "
            f"Matching call IDs: {call_ids}."
        )

    raise KitaruStateError(
        f"Unknown checkpoint selector '{selector}'. Available checkpoints: "
        f"{_available_checkpoint_selectors(checkpoints)}."
    )


def _resolve_checkpoint_scope_selector(
    selector: str,
    checkpoints: Sequence[_Checkpoint],
) -> list[_Checkpoint]:
    model_request_base = _unsuffixed_model_request_base(selector)
    if model_request_base is not None:
        matches = [
            checkpoint
            for checkpoint in checkpoints
            if _is_llm_checkpoint(checkpoint)
            and _model_request_base(checkpoint.name) == model_request_base
        ]
        if matches:
            return matches

    exact_matches = [
        checkpoint for checkpoint in checkpoints if checkpoint.name == selector
    ]
    if exact_matches and not _is_unsuffixed_tool_selector(selector):
        return exact_matches
    if _is_suffixed_tool_selector(selector):
        raise KitaruStateError(
            f"Unknown checkpoint override target '{selector}'. Available checkpoints: "
            f"{_available_checkpoint_selectors(checkpoints)}."
        )

    normalized_selector = _normalized_tool_name(selector)
    matches = [
        checkpoint
        for checkpoint in checkpoints
        if _normalized_tool_name(checkpoint.name) == normalized_selector
    ]
    if matches:
        return matches
    raise KitaruStateError(
        f"Unknown checkpoint override target '{selector}'. Available checkpoints: "
        f"{_available_checkpoint_selectors(checkpoints)}."
    )


def _is_unsuffixed_tool_selector(selector: str) -> bool:
    return selector.endswith("_tool")


def _is_suffixed_tool_selector(selector: str) -> bool:
    return re.search(r"_tool_\d+$", selector) is not None


def _normalized_tool_name(name: str) -> str:
    """Normalize adapter tool checkpoint names for replay selectors."""
    if _TOOL_CHECKPOINT_SUFFIX.search(name):
        return _TOOL_CHECKPOINT_SUFFIX.sub("", name)
    return name.removesuffix("_tool")


def _model_request_base(name: str) -> str | None:
    """Return the adapter-generated model request family name, if present."""
    match = _MODEL_REQUEST_CHECKPOINT_SUFFIX.match(name)
    if match is None:
        return None
    return match.group("base")


def _unsuffixed_model_request_base(selector: str) -> str | None:
    """Return a model-request family selector only for unsuffixed base names."""
    base = _model_request_base(selector)
    if base is None or base != selector:
        return None
    return base


def _iter_step_input_specs(step: StepRunResponse) -> Iterator[tuple[str, Any]]:
    step_spec = getattr(step, "spec", None)
    if step_spec is None:
        return

    inputs_v2 = getattr(step_spec, "inputs_v2", None)
    if isinstance(inputs_v2, Mapping):
        for input_name, input_specs in inputs_v2.items():
            for input_spec in input_specs:
                yield input_name, input_spec
        return

    legacy_inputs = getattr(step_spec, "inputs", None)
    if not isinstance(legacy_inputs, Mapping):
        return

    for input_name, raw_input_specs in legacy_inputs.items():
        if isinstance(raw_input_specs, Sequence) and not isinstance(
            raw_input_specs, (str, bytes)
        ):
            iterable: Iterable[Any] = raw_input_specs
        else:
            iterable = [raw_input_specs]
        for input_spec in iterable:
            yield input_name, input_spec


def _single_checkpoint_output_name(checkpoint: _Checkpoint) -> str:
    try:
        output_names = list(checkpoint.step.regular_outputs)
    except Exception:
        outputs = getattr(checkpoint.step, "outputs", None)
        output_names = list(outputs or {})

    if not output_names:
        raise KitaruStateError(
            f"Checkpoint '{checkpoint.name}' does not expose replayable outputs."
        )
    if len(output_names) > 1:
        raise KitaruUsageError(
            "Checkpoint output overrides currently require single-output "
            f"checkpoints. Checkpoint '{checkpoint.name}' has outputs: "
            f"{', '.join(output_names)}."
        )
    return output_names[0]


def _find_downstream_consumers(
    *,
    source: _Checkpoint,
    checkpoints: Sequence[_Checkpoint],
) -> list[tuple[str, str]]:
    output_name = _single_checkpoint_output_name(source)

    consumers: list[tuple[str, str]] = []
    for checkpoint in checkpoints:
        for input_name, input_spec in _iter_step_input_specs(checkpoint.step):
            upstream_name = getattr(input_spec, "step_name", None)
            upstream_output_name = getattr(input_spec, "output_name", None)
            if upstream_name != source.invocation_id:
                continue
            if upstream_output_name != output_name:
                continue
            consumers.append((checkpoint.invocation_id, input_name))

    if not consumers:
        raise KitaruStateError(
            "Checkpoint output override target has no downstream consumer in "
            f"this execution: {source.name}. Output overrides currently work "
            "by replacing inputs to later checkpoints, and this checkpoint has "
            "no later input to replace. For a side-effectful terminal "
            "checkpoint, guard the side effect with `kitaru.is_replay()` or "
            "move the side effect into a checkpoint whose output is consumed "
            "later."
        )

    return consumers


def _build_children_by_invocation(
    checkpoints: Sequence[_Checkpoint],
) -> dict[str, set[str]]:
    checkpoints_by_invocation = {
        checkpoint.invocation_id: checkpoint for checkpoint in checkpoints
    }
    children_by_invocation: dict[str, set[str]] = {
        invocation_id: set() for invocation_id in checkpoints_by_invocation
    }

    for checkpoint in checkpoints:
        upstream_steps: Sequence[str] = (
            getattr(getattr(checkpoint.step, "spec", None), "upstream_steps", None)
            or ()
        )
        for upstream_invocation_id in upstream_steps:
            if upstream_invocation_id not in checkpoints_by_invocation:
                continue
            children_by_invocation[upstream_invocation_id].add(checkpoint.invocation_id)

    return children_by_invocation


def _collect_descendants(
    *,
    roots: set[str],
    children_by_invocation: Mapping[str, set[str]],
) -> set[str]:
    descendants: set[str] = set()
    to_visit = list(roots)

    while to_visit:
        invocation_id = to_visit.pop()
        for child_invocation_id in children_by_invocation.get(invocation_id, set()):
            if child_invocation_id in descendants:
                continue
            descendants.add(child_invocation_id)
            to_visit.append(child_invocation_id)

    return descendants


def _checkpoint_input_slot_names(checkpoint: _Checkpoint) -> set[str]:
    if checkpoint.checkpoint_type == "tool_call":
        return {"tool_args"}
    if checkpoint.checkpoint_type == "llm_call":
        return {"messages", "input", "user_prompt", "message_history"}
    names: set[str] = set()
    for input_name, _ in _iter_step_input_specs(checkpoint.step):
        names.add(input_name)
    return names or {"input"}


def _normalize_checkpoint_input_value(
    checkpoint: _Checkpoint,
    value: Any,
) -> dict[str, Any]:
    if isinstance(value, Mapping):
        slot_names = _checkpoint_input_slot_names(checkpoint)
        if slot_names.intersection(value.keys()):
            return dict(value)
        if checkpoint.checkpoint_type == "tool_call":
            return {"tool_args": value}
        if checkpoint.checkpoint_type == "llm_call":
            return {"messages": value}
        if len(slot_names) == 1:
            slot_name = next(iter(slot_names))
            return {slot_name: value}
        return dict(value)
    slot_names = _checkpoint_input_slot_names(checkpoint)
    if len(slot_names) == 1:
        return {next(iter(slot_names)): value}
    raise KitaruUsageError(
        f"Checkpoint input override for '{checkpoint.name}' must be a mapping "
        f"with one of: {', '.join(sorted(slot_names))}."
    )


def _resolve_skip_invocation_ids(
    skip: Sequence[str] | None,
    checkpoints: Sequence[_Checkpoint],
) -> set[str]:
    if not skip:
        return set()

    invocation_ids: set[str] = set()
    for selector in skip:
        if not selector or not str(selector).strip():
            raise KitaruUsageError(
                "Every entry in `skip` must be a non-empty checkpoint selector."
            )
        checkpoint = _resolve_checkpoint_selector(str(selector).strip(), checkpoints)
        invocation_ids.add(checkpoint.invocation_id)
    return invocation_ids


def _compute_live_steps(
    *,
    at_checkpoint: _Checkpoint,
    checkpoints: Sequence[_Checkpoint],
    children_by_invocation: Mapping[str, set[str]],
) -> set[str]:
    live_roots = {at_checkpoint.invocation_id}
    live = live_roots | _collect_descendants(
        roots=live_roots,
        children_by_invocation=children_by_invocation,
    )

    at_started_at = at_checkpoint.started_at
    if at_started_at is None:
        return live

    skipped_before_at = {
        checkpoint.invocation_id
        for checkpoint in checkpoints
        if checkpoint.started_at is not None and checkpoint.started_at < at_started_at
    }

    for checkpoint in checkpoints:
        if checkpoint.invocation_id in live:
            continue
        if checkpoint.started_at is None:
            continue
        if checkpoint.started_at < at_started_at:
            continue

        upstream_steps: Sequence[str] = (
            getattr(
                getattr(checkpoint.step, "spec", None),
                "upstream_steps",
                None,
            )
            or ()
        )
        if upstream_steps:
            if all(upstream in skipped_before_at for upstream in upstream_steps):
                continue
            if not any(
                upstream in live or upstream == at_checkpoint.invocation_id
                for upstream in upstream_steps
            ):
                continue

        live.add(checkpoint.invocation_id)
        live |= _collect_descendants(
            roots={checkpoint.invocation_id},
            children_by_invocation=children_by_invocation,
        )

    return live


def _coerce_flow_overrides(flow_overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    if not flow_overrides:
        return {}
    if not isinstance(flow_overrides, Mapping):
        raise KitaruUsageError("`flow_overrides` must be a mapping.")
    return {str(key): value for key, value in flow_overrides.items()}


def build_replay_request_document(
    *,
    flow_overrides: Mapping[str, Any] | None = None,
    checkpoint_overrides: Mapping[str, Any] | None = None,
    invocation_overrides: Mapping[str, Any] | None = None,
    skip: Sequence[str] | None = None,
) -> ReplayPlanDocument:
    """Validate and serialize the public replay request shape."""
    return ReplayPlanDocument(
        flow_overrides=_coerce_flow_overrides(flow_overrides),
        checkpoint_overrides=_coerce_override_mapping(
            "checkpoint_overrides", checkpoint_overrides
        ),
        invocation_overrides=_coerce_override_mapping(
            "invocation_overrides", invocation_overrides
        ),
        skip=[str(item) for item in (skip or [])],
    )


def _coerce_override_entry(scope: str, selector: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise KitaruUsageError(
            f"{scope} override for '{selector}' must be an object with one or more "
            "of: input, output, model, code."
        )
    entry = {str(key): item for key, item in value.items()}
    unknown = set(entry) - _ALLOWED_OVERRIDE_FIELDS
    if unknown:
        raise KitaruUsageError(
            f"Unknown replay override field(s) for '{selector}': "
            f"{', '.join(sorted(unknown))}. Allowed fields: "
            f"{', '.join(sorted(_ALLOWED_OVERRIDE_FIELDS))}."
        )
    if "input" in entry and "output" in entry:
        raise KitaruUsageError(
            f"Replay override for '{selector}' cannot include both input and output."
        )
    return entry


def _coerce_override_mapping(
    name: str,
    overrides: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if not overrides:
        return {}
    if not isinstance(overrides, Mapping):
        raise KitaruUsageError(f"`{name}` must be a mapping.")
    return {
        str(selector): _coerce_override_entry(name, str(selector), value)
        for selector, value in overrides.items()
    }


def _is_llm_checkpoint(checkpoint: _Checkpoint) -> bool:
    return str(checkpoint.checkpoint_type or "").lower() == "llm_call"


def _is_tool_checkpoint(checkpoint: _Checkpoint) -> bool:
    return str(checkpoint.checkpoint_type or "").lower() == "tool_call"


def _validate_model_override(checkpoint: _Checkpoint) -> None:
    if _is_llm_checkpoint(checkpoint):
        return
    raise KitaruUsageError(
        f"Model override target '{checkpoint.invocation_id}' is not an LLM "
        "checkpoint and cannot honor a model swap."
    )


def _validate_code_override(checkpoint: _Checkpoint) -> None:
    if _is_tool_checkpoint(checkpoint):
        return
    raise KitaruUsageError(
        f"Code override target '{checkpoint.invocation_id}' is not a tool "
        "checkpoint and cannot honor a code swap."
    )


def _put_target_override(
    effective: dict[str, dict[str, Any]],
    checkpoint: _Checkpoint,
    entry: Mapping[str, Any],
) -> None:
    target = effective.setdefault(checkpoint.invocation_id, {})
    if "input" in entry and "output" in target:
        raise KitaruUsageError(
            f"Replay target '{checkpoint.invocation_id}' cannot combine input "
            "and output."
        )
    if "output" in entry and "input" in target:
        raise KitaruUsageError(
            f"Replay target '{checkpoint.invocation_id}' cannot combine input "
            "and output."
        )
    target.update(entry)


def _build_effective_overrides(
    *,
    checkpoint_overrides: Mapping[str, Any] | None,
    invocation_overrides: Mapping[str, Any] | None,
    checkpoints: Sequence[_Checkpoint],
) -> tuple[dict[str, dict[str, Any]], ReplayPlanDocument]:
    checkpoint_entries = _coerce_override_mapping(
        "checkpoint_overrides", checkpoint_overrides
    )
    invocation_entries = _coerce_override_mapping(
        "invocation_overrides", invocation_overrides
    )

    effective: dict[str, dict[str, Any]] = {}
    matched_targets: dict[str, list[str]] = {}

    for selector, entry in checkpoint_entries.items():
        matches = _resolve_checkpoint_scope_selector(selector, checkpoints)
        matched_targets[f"checkpoint:{selector}"] = [
            checkpoint.invocation_id for checkpoint in matches
        ]
        for checkpoint in matches:
            _put_target_override(effective, checkpoint, entry)

    for selector, entry in invocation_entries.items():
        checkpoint = _resolve_checkpoint_selector(selector, checkpoints)
        matched_targets[f"invocation:{selector}"] = [checkpoint.invocation_id]
        _put_target_override(effective, checkpoint, entry)

    for invocation_id, entry in effective.items():
        if "model" not in entry and "code" not in entry:
            continue
        checkpoint = next(
            checkpoint
            for checkpoint in checkpoints
            if checkpoint.invocation_id == invocation_id
        )
        if "model" in entry:
            _validate_model_override(checkpoint)
        if "code" in entry:
            _validate_code_override(checkpoint)

    document = ReplayPlanDocument(
        checkpoint_overrides={
            key: dict(value) for key, value in checkpoint_entries.items()
        },
        invocation_overrides={
            key: dict(value) for key, value in invocation_entries.items()
        },
        matched_targets=matched_targets,
    )
    return effective, document


def _runtime_override_keys(checkpoint: _Checkpoint) -> set[str]:
    # Include all stable recorded identities so runtime consumers can resolve by
    # whichever identity their adapter has available. The planner still keys the
    # public document and ZenML overrides by invocation ID.
    return checkpoint.target_keys


def _has_runtime_only_overrides(plan: ReplayPlan) -> bool:
    return bool(
        plan.runtime_context.code_overrides or plan.runtime_context.model_overrides
    )


def plan_requires_runtime_transport(plan: ReplayPlan) -> bool:
    """Return whether this replay plan needs KITARU_REPLAY_CONTEXT transport."""
    return _has_runtime_only_overrides(plan)


def build_replay_plan(
    *,
    run: PipelineRunResponse,
    at: str,
    flow_overrides: Mapping[str, Any] | None = None,
    checkpoint_overrides: Mapping[str, Any] | None = None,
    invocation_overrides: Mapping[str, Any] | None = None,
    skip: Sequence[str] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed or paused execution.

    Checkpoints before ``at`` are skipped. ``at`` and its downstream descendants
    re-execute unless an ``output`` override injects a value or ``skip`` asks to
    play back a recorded result. Checkpoint-scope overrides intentionally fan
    out to every matching invocation; invocation-scope overrides target exactly
    one invocation/call ID and win over checkpoint-scope values.
    """
    checkpoints = _checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
        )

    if not at or not at.strip():
        raise KitaruUsageError("`at` must be a non-empty checkpoint selector.")

    at_checkpoint = _resolve_checkpoint_selector(at, checkpoints)
    children_by_invocation = _build_children_by_invocation(checkpoints)
    all_steps = {checkpoint.invocation_id for checkpoint in checkpoints}
    checkpoints_by_invocation = {
        checkpoint.invocation_id: checkpoint for checkpoint in checkpoints
    }

    effective_overrides, document = _build_effective_overrides(
        checkpoint_overrides=checkpoint_overrides,
        invocation_overrides=invocation_overrides,
        checkpoints=checkpoints,
    )
    explicit_skip = _resolve_skip_invocation_ids(skip, checkpoints)
    override_targets = set(effective_overrides)
    skip_overlap = explicit_skip & override_targets
    if skip_overlap:
        joined = ", ".join(sorted(skip_overlap))
        raise KitaruUsageError(
            f"Cannot skip and override the same replay target: {joined}."
        )

    live = _compute_live_steps(
        at_checkpoint=at_checkpoint,
        checkpoints=checkpoints,
        children_by_invocation=children_by_invocation,
    )
    steps_to_skip = all_steps - live

    step_input_overrides: dict[str, dict[str, Any]] = {}
    runtime_input_overrides: dict[str, dict[str, Any]] = {}
    output_mocks: dict[str, Any] = {}
    code_overrides: dict[str, str] = {}
    model_overrides: dict[str, str] = {}

    for invocation_id, entry in effective_overrides.items():
        checkpoint = checkpoints_by_invocation[invocation_id]
        if "input" in entry:
            normalized = _normalize_checkpoint_input_value(checkpoint, entry["input"])
            live.add(invocation_id)
            live |= _collect_descendants(
                roots={invocation_id},
                children_by_invocation=children_by_invocation,
            )
            step_input_overrides.setdefault(invocation_id, {}).update(normalized)
            runtime_input_overrides[checkpoint.call_id] = normalized

        if "code" in entry:
            live.add(invocation_id)
            live |= _collect_descendants(
                roots={invocation_id},
                children_by_invocation=children_by_invocation,
            )
            for key in _runtime_override_keys(checkpoint):
                code_overrides[key] = str(entry["code"])

        if "model" in entry:
            live.add(invocation_id)
            live |= _collect_descendants(
                roots={invocation_id},
                children_by_invocation=children_by_invocation,
            )
            for key in _runtime_override_keys(checkpoint):
                model_overrides[key] = str(entry["model"])

    for invocation_id, entry in effective_overrides.items():
        if "output" not in entry:
            continue
        source = checkpoints_by_invocation[invocation_id]
        value = entry["output"]
        live.discard(source.invocation_id)
        for consumer_invocation_id, input_name in _find_downstream_consumers(
            source=source,
            checkpoints=checkpoints,
        ):
            live.add(consumer_invocation_id)
            live |= _collect_descendants(
                roots={consumer_invocation_id},
                children_by_invocation=children_by_invocation,
            )
            step_input_overrides.setdefault(consumer_invocation_id, {})[input_name] = (
                value
            )
        for key in _runtime_override_keys(source):
            output_mocks[key] = value

    live -= explicit_skip
    steps_to_skip = all_steps - live

    overlap = steps_to_skip & set(step_input_overrides)
    if overlap:
        steps_to_skip -= overlap

    runtime_context = ReplayRuntimeContext(
        at=at,
        output_mocks=output_mocks,
        code_overrides=code_overrides,
        model_overrides=model_overrides,
        input_overrides=runtime_input_overrides,
    )

    normalized_flow_overrides = _coerce_flow_overrides(flow_overrides)
    resolved_document = ReplayPlanDocument(
        flow_overrides=normalized_flow_overrides,
        checkpoint_overrides=document.checkpoint_overrides,
        invocation_overrides=document.invocation_overrides,
        skip=[str(item) for item in (skip or [])],
        matched_targets=document.matched_targets,
    )

    return ReplayPlan(
        original_run_id=str(run.id),
        steps_to_skip=steps_to_skip,
        input_overrides=normalized_flow_overrides,
        step_input_overrides=step_input_overrides,
        runtime_context=runtime_context,
        document=resolved_document,
    )


__all__ = [
    "REPLAY_RESERVED_KWARGS",
    "ReplayFailureRow",
    "ReplayPlan",
    "ReplayPlanDocument",
    "ReplayResultRow",
    "ReplaySkippedRow",
    "ReplaySubmission",
    "ReplaySummary",
    "build_replay_plan",
    "build_replay_request_document",
    "new_replay_submission_id",
    "plan_requires_runtime_transport",
    "replay_at_skip_reason",
    "replay_at_status",
    "safe_compare_url_for_executions",
    "safe_persist_replay_submission_metadata",
]
