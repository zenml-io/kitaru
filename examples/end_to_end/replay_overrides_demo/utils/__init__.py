"""Small reporting helpers for the replay overrides demo."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from support_agent import (
    FINAL_DECISION_CHECKPOINT,
    MODEL_CHECKPOINT_PREFIX,
    REPORTING_CHECKPOINT,
    SupportDecision,
)

from kitaru import KitaruClient

DECISION_FIELDS = ("risk_status", "required_action")


@dataclass(frozen=True)
class DecisionDiff:
    """Field-level comparison between two support decisions."""

    changes: dict[str, tuple[Any, Any]]

    @property
    def changed(self) -> bool:
        return bool(self.changes)

    def to_json(self) -> dict[str, dict[str, Any]]:
        return {
            field: {"original": before, "replay": after}
            for field, (before, after) in self.changes.items()
        }

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


def load_support_decision(client: KitaruClient, exec_id: str) -> dict[str, Any]:
    """Load the SupportDecision dict from a completed execution."""
    run = client.executions.get(exec_id)

    def _extract(value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict) and "risk_status" in value:
            return value
        if isinstance(value, SupportDecision):
            return value.model_dump()
        output = getattr(value, "output", None)
        if output is not None and output is not value:
            extracted = _extract(output)
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

    checkpoint_by_name = {checkpoint.name: checkpoint for checkpoint in run.checkpoints}
    names = [
        REPORTING_CHECKPOINT,
        FINAL_DECISION_CHECKPOINT,
        *[
            checkpoint.name
            for checkpoint in run.checkpoints
            if checkpoint.name.startswith(MODEL_CHECKPOINT_PREFIX)
        ][::-1],
    ]
    for name in names:
        checkpoint = checkpoint_by_name.get(name)
        if checkpoint is None:
            continue
        for artifact in checkpoint.artifacts:
            if getattr(artifact, "direction", None) not in (None, "output"):
                continue
            try:
                extracted = _extract(artifact.load())
            except Exception:
                continue
            if extracted is not None:
                return extracted
    raise RuntimeError(
        f"Could not load SupportDecision from execution {exec_id!r}. "
        f"Checkpoints present: {[checkpoint.name for checkpoint in run.checkpoints]}."
    )


def write_json(path: Path, payload: dict[str, Any]) -> str:
    """Write JSON and return the absolute path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(path.resolve())
