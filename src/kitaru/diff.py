"""Execution diff utilities for replay comparison."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from urllib.parse import quote

from kitaru._client._models import CheckpointCall, Execution
from kitaru.client import KitaruClient

DEFAULT_COMPARE_FLOW_VERSION = "local"


@dataclass(frozen=True)
class CheckpointDiff:
    """Per-checkpoint comparison between an original and replay execution."""

    name: str
    original_call_id: str | None
    replay_call_id: str | None
    status_match: bool
    duration_delta_ms: float | None
    token_delta: dict[str, int] | None
    artifact_hashes: dict[str, tuple[str | None, str | None]]


@dataclass(frozen=True)
class ExecutionDiff:
    """Diff between an original execution and one or more replays."""

    original_exec_id: str
    compared: list[tuple[str, list[CheckpointDiff]]] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CohortDiff:
    """Diff matrix for a cohort of original executions and their replays."""

    rows: list[ExecutionDiff] = field(default_factory=list)


def _client_server_url(client: KitaruClient) -> str | None:
    try:
        zen_store = client._client().zen_store
        url = getattr(zen_store, "url", None)
    except Exception:
        return None
    if not isinstance(url, str) or not url.strip():
        return None
    return url.strip().rstrip("/")


def build_compare_url(
    *,
    server_url: str | None,
    flow_id: str | None,
    original_exec_id: str,
    replay_exec_id: str,
    flow_version: str = DEFAULT_COMPARE_FLOW_VERSION,
) -> str | None:
    """Build the Kitaru UI compare URL for an original vs one replay execution.

    Example::

        {server}/flows/{flow_id}/v/local/compare?executions={original},{replay}
    """
    if server_url is None or flow_id is None:
        return None

    original = str(original_exec_id).strip()
    replay = str(replay_exec_id).strip()
    if not original or not replay:
        return None

    flow_segment = quote(str(flow_id), safe="")
    version_segment = quote(
        flow_version.strip() or DEFAULT_COMPARE_FLOW_VERSION,
        safe="",
    )
    execution_segment = quote(f"{original},{replay}", safe=",")
    return (
        f"{server_url.rstrip('/')}/flows/{flow_segment}/v/{version_segment}/compare"
        f"?executions={execution_segment}"
    )


def build_compare_urls(
    *,
    server_url: str | None,
    flow_id: str | None,
    original_exec_id: str,
    replay_exec_ids: Sequence[str],
    flow_version: str = DEFAULT_COMPARE_FLOW_VERSION,
) -> list[str]:
    """Build one UI compare URL per original-vs-replay pair."""
    urls: list[str] = []
    for replay_exec_id in replay_exec_ids:
        url = build_compare_url(
            server_url=server_url,
            flow_id=flow_id,
            original_exec_id=original_exec_id,
            replay_exec_id=replay_exec_id,
            flow_version=flow_version,
        )
        if url is not None:
            urls.append(url)
    return urls


def _checkpoint_duration_ms(checkpoint: CheckpointCall) -> float | None:
    if checkpoint.started_at is None or checkpoint.ended_at is None:
        return None
    return (checkpoint.ended_at - checkpoint.started_at).total_seconds() * 1000


def _token_totals(checkpoint: CheckpointCall) -> dict[str, int]:
    totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    for record in checkpoint.llm_usage_records:
        for key in totals:
            value = record.get(key)
            if isinstance(value, int):
                totals[key] += value
    if not any(totals.values()):
        return {}
    return totals


def _artifact_content_hash(artifact_id: str, client: KitaruClient) -> str | None:
    try:
        artifact = client._get_artifact_version(artifact_id, hydrate=True)
        value = artifact.load()
        payload = json.dumps(value, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return None


def _artifact_hashes(
    checkpoint: CheckpointCall,
    client: KitaruClient,
) -> dict[str, str | None]:
    hashes: dict[str, str | None] = {}
    for artifact in checkpoint.artifacts:
        role = artifact.name or artifact.kind or "artifact"
        hashes[role] = _artifact_content_hash(artifact.artifact_id, client)
    return hashes


def _align_checkpoints(
    original: Execution,
    replay: Execution,
) -> list[tuple[CheckpointCall | None, CheckpointCall | None]]:
    replay_by_original: dict[str, CheckpointCall] = {}
    replay_by_name: dict[str, list[CheckpointCall]] = {}
    for checkpoint in replay.checkpoints:
        if checkpoint.original_call_id:
            replay_by_original[checkpoint.original_call_id] = checkpoint
        replay_by_name.setdefault(checkpoint.name, []).append(checkpoint)

    pairs: list[tuple[CheckpointCall | None, CheckpointCall | None]] = []
    used_replay_ids: set[str] = set()
    for original_cp in original.checkpoints:
        matched = replay_by_original.get(original_cp.call_id)
        if matched is None:
            candidates = replay_by_name.get(original_cp.name, [])
            for candidate in candidates:
                if candidate.call_id not in used_replay_ids:
                    matched = candidate
                    break
        if matched is not None:
            used_replay_ids.add(matched.call_id)
        pairs.append((original_cp, matched))

    for checkpoint in replay.checkpoints:
        if checkpoint.call_id in used_replay_ids:
            continue
        pairs.append((None, checkpoint))

    return pairs


def _compare_checkpoints(
    *,
    original_cp: CheckpointCall | None,
    replay_cp: CheckpointCall | None,
    client: KitaruClient,
) -> CheckpointDiff:
    name = (original_cp or replay_cp).name if (original_cp or replay_cp) else "unknown"
    original_duration = (
        _checkpoint_duration_ms(original_cp) if original_cp is not None else None
    )
    replay_duration = (
        _checkpoint_duration_ms(replay_cp) if replay_cp is not None else None
    )
    duration_delta = None
    if original_duration is not None and replay_duration is not None:
        duration_delta = replay_duration - original_duration

    original_tokens = _token_totals(original_cp) if original_cp is not None else {}
    replay_tokens = _token_totals(replay_cp) if replay_cp is not None else {}
    token_delta: dict[str, int] | None = None
    if original_tokens or replay_tokens:
        token_delta = {
            key: replay_tokens.get(key, 0) - original_tokens.get(key, 0)
            for key in ("prompt_tokens", "completion_tokens", "total_tokens")
        }

    original_hashes = (
        _artifact_hashes(original_cp, client) if original_cp is not None else {}
    )
    replay_hashes = _artifact_hashes(replay_cp, client) if replay_cp is not None else {}
    roles = sorted(set(original_hashes) | set(replay_hashes))
    artifact_hashes = {
        role: (original_hashes.get(role), replay_hashes.get(role)) for role in roles
    }

    status_match = (
        original_cp is not None
        and replay_cp is not None
        and original_cp.status == replay_cp.status
    )

    return CheckpointDiff(
        name=name,
        original_call_id=original_cp.call_id if original_cp is not None else None,
        replay_call_id=replay_cp.call_id if replay_cp is not None else None,
        status_match=status_match,
        duration_delta_ms=duration_delta,
        token_delta=token_delta,
        artifact_hashes=artifact_hashes,
    )


def diff(
    original: str,
    *executions: str,
) -> ExecutionDiff:
    """Compare an original execution against replay executions.

    When ``executions`` is omitted, all runs whose ``original_exec_id`` matches
    ``original`` are included.
    """
    client = KitaruClient()
    original_execution = client.executions.get(original)

    compared_exec_ids: list[str]
    if executions:
        compared_exec_ids = list(executions)
    else:
        flow_name = original_execution.flow_name
        candidates = client.executions.list(flow=flow_name, limit=200)
        compared_exec_ids = [
            candidate.exec_id
            for candidate in candidates
            if candidate.original_exec_id == original
        ]

    compared: list[tuple[str, list[CheckpointDiff]]] = []
    for replay_exec_id in compared_exec_ids:
        replay_execution = client.executions.get(replay_exec_id)
        pairs = _align_checkpoints(original_execution, replay_execution)
        checkpoint_diffs = [
            _compare_checkpoints(
                original_cp=original_cp,
                replay_cp=replay_cp,
                client=client,
            )
            for original_cp, replay_cp in pairs
        ]
        compared.append((replay_exec_id, checkpoint_diffs))

    compare_urls = build_compare_urls(
        server_url=_client_server_url(client),
        flow_id=original_execution.flow_id,
        original_exec_id=original,
        replay_exec_ids=compared_exec_ids,
    )

    return ExecutionDiff(
        original_exec_id=original,
        compared=compared,
        urls=compare_urls,
    )


def diff_cohort(
    exec_ids: Sequence[str],
) -> CohortDiff:
    """Compare many original executions against their discovered replays.

    Each entry in ``exec_ids`` is passed to ``diff(...)`` with auto-discovery of
    replay executions linked via ``original_exec_id``.
    """
    return CohortDiff(rows=[diff(exec_id) for exec_id in exec_ids])


__all__ = [
    "CheckpointDiff",
    "CohortDiff",
    "DEFAULT_COMPARE_FLOW_VERSION",
    "ExecutionDiff",
    "build_compare_url",
    "build_compare_urls",
    "diff",
    "diff_cohort",
]
