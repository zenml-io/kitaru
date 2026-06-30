"""Load SupportDecision results from completed demo executions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from support_agent import (
    FINAL_DECISION_CHECKPOINT,
    MODEL_CHECKPOINT_PREFIX,
    SupportDecision,
)

from kitaru import KitaruClient

DECISION_FIELDS = ("risk_status", "required_action")
_FLOW_RESULT_REF_METADATA_KEY = "kitaru_flow_result_ref_v1"
_PREFERRED_ARTIFACT_NAMES = (
    "support_decision",
    "output",
    "kitaru_flow_result",
)


@dataclass(frozen=True)
class DecisionDiff:
    """Field-level comparison between two support decisions."""

    changes: dict[str, tuple[Any, Any]]

    @property
    def changed(self) -> bool:
        return bool(self.changes)

    def __str__(self) -> str:
        if not self.changes:
            return "no decision change"
        return "; ".join(
            f"{field}: {before!r} -> {after!r}"
            for field, (before, after) in self.changes.items()
        )


def decision_summary(decision: dict[str, Any] | None) -> str:
    """Return a compact one-line support decision summary."""
    if decision is None:
        return "decision unavailable"
    return (
        f"risk={decision.get('risk_status', '?')} "
        f"action={decision.get('required_action', '?')} "
        f"label={decision.get('policy_label', '?')}"
    )


def diff_decisions(original: dict[str, Any], replay: dict[str, Any]) -> DecisionDiff:
    """Compare the decision fields that matter for this demo."""
    return DecisionDiff(
        {
            field: (original.get(field), replay.get(field))
            for field in DECISION_FIELDS
            if original.get(field) != replay.get(field)
        }
    )


def _extract_support_decision(value: Any) -> dict[str, Any] | None:
    """Normalize a loaded artifact or flow result into a SupportDecision dict."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if isinstance(value, dict) and "risk_status" in value:
        return value
    if isinstance(value, SupportDecision):
        return value.model_dump()
    output = getattr(value, "output", None)
    if output is not None and output is not value:
        extracted = _extract_support_decision(output)
        if extracted is not None:
            return extracted
    for part in getattr(value, "parts", None) or ():
        if getattr(part, "tool_name", None) != "final_result":
            continue
        args = getattr(part, "args", None)
        parsed = json.loads(args) if isinstance(args, str) else args
        if isinstance(parsed, dict) and "risk_status" in parsed:
            return parsed
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict) and "risk_status" in dumped:
            return dumped
    return None


def _load_artifact_value(client: KitaruClient, artifact_id: str) -> Any:
    """Load one artifact version through the SDK hydration path."""
    return client.artifacts.get(artifact_id).load()


def load_support_decision(client: KitaruClient, exec_id: str) -> dict[str, Any]:
    """Load the SupportDecision dict from a completed execution.

    On remote artifact stores, PydanticAI model-response checkpoints may not
    materialize locally. Prefer the linked flow-result artifact Kitaru persists
    for ``support_copilot_flow`` return values, then fall back to explicit
    publish/report checkpoints.
    """
    run = client.executions.get(exec_id)
    load_errors: list[str] = []

    flow_result_ref = run.metadata.get(_FLOW_RESULT_REF_METADATA_KEY)
    if isinstance(flow_result_ref, str) and flow_result_ref:
        try:
            extracted = _extract_support_decision(
                _load_artifact_value(client, flow_result_ref)
            )
            if extracted is not None:
                return extracted
        except Exception as exc:
            load_errors.append(f"flow_result_ref={flow_result_ref}: {exc}")

    for artifact in run.list_artifacts():
        if (
            artifact.name not in _PREFERRED_ARTIFACT_NAMES
            and artifact.direction != "output"
        ):
            continue
        try:
            extracted = _extract_support_decision(
                _load_artifact_value(client, artifact.artifact_id)
            )
            if extracted is not None:
                return extracted
        except Exception as exc:
            load_errors.append(
                f"artifact {artifact.name!r} ({artifact.artifact_id}): {exc}"
            )

    checkpoint_by_name = {checkpoint.name: checkpoint for checkpoint in run.checkpoints}
    checkpoint_names = [
        FINAL_DECISION_CHECKPOINT,
        *[
            checkpoint.name
            for checkpoint in run.checkpoints
            if checkpoint.name.startswith(MODEL_CHECKPOINT_PREFIX)
        ][::-1],
    ]
    for name in checkpoint_names:
        checkpoint = checkpoint_by_name.get(name)
        if checkpoint is None:
            continue
        for artifact in checkpoint.artifacts:
            if artifact.direction not in (None, "output"):
                continue
            try:
                extracted = _extract_support_decision(
                    _load_artifact_value(client, artifact.artifact_id)
                )
                if extracted is not None:
                    return extracted
            except Exception as exc:
                load_errors.append(
                    f"checkpoint {name!r} artifact {artifact.name!r}: {exc}"
                )

    detail = (
        f"Checkpoints present: {[checkpoint.name for checkpoint in run.checkpoints]}."
    )
    if load_errors:
        detail += " Load errors: " + "; ".join(load_errors[:5])
        if len(load_errors) > 5:
            detail += f" (+{len(load_errors) - 5} more)"
    raise RuntimeError(
        f"Could not load SupportDecision from execution {exec_id!r}. {detail}"
    )
