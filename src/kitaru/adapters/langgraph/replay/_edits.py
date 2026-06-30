from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_UNSET: Any = object()


@dataclass(frozen=True)
class Edit:
    target: str
    model: str | None = None
    value: Any = _UNSET
    params: dict[str, Any] = field(default_factory=dict)


def edit(
    target: str, *, model: str | None = None, value: Any = _UNSET, **params: Any
) -> Edit:
    return Edit(target=target, model=model, value=value, params=dict(params))


def _matches(target: str, *, node: str, call_index: int | None) -> bool:
    if target == node:
        return True
    if target == f"model_call:{node}":
        return True
    return call_index is not None and target == f"tool_call:{node}:{call_index}"


def resolve_edits(
    *,
    node: str,
    call_index: int | None,
    edits: list[Edit],
    variant: dict[str, Any] | None,
    recorded: dict[str, Any],
) -> dict[str, Any]:
    """Effective params for a checkpoint: call > variant > recorded."""
    effective: dict[str, Any] = dict(recorded)
    if variant:
        effective.update({k: v for k, v in variant.items() if v is not None})
    for e in edits:
        if not _matches(e.target, node=node, call_index=call_index):
            continue
        if e.model is not None:
            effective["model"] = e.model
        if e.value is not _UNSET:
            effective["value"] = e.value
        effective.update(e.params)
    return effective
