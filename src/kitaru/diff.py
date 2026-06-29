"""Execution diff utilities for replay comparison."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from kitaru._client._models import CheckpointCall, Execution
from kitaru._ui_urls import resolve_ui_base_url
from kitaru.client import KitaruClient

DEFAULT_COMPARE_FLOW_VERSION = "local"
_AUTO_DISCOVERY_LIMIT = 200


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
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CohortDiff:
    """Diff matrix for a cohort of original executions and their replays."""

    rows: list[ExecutionDiff] = field(default_factory=list)


def _client_server_url(client: KitaruClient) -> str | None:
    """Return an HTTP(S) dashboard base URL for UI compare links."""
    return resolve_ui_base_url(client)


def compare_urls_for_replay(
    client: KitaruClient,
    *,
    original_exec_id: str,
    replay_exec_id: str,
) -> list[str]:
    """Build UI compare URLs for one original execution and its replay."""
    original_execution = client.executions.get(original_exec_id)
    compare_url = build_compare_url(
        server_url=_client_server_url(client),
        flow_id=original_execution.flow_id,
        original_exec_id=original_exec_id,
        replay_exec_id=replay_exec_id,
        flow_version=_resolve_compare_flow_version(original_execution),
    )
    if compare_url is None:
        return []
    return [compare_url]


def build_compare_url_for_executions(
    *,
    server_url: str | None,
    flow_id: str | None,
    exec_ids: Sequence[str],
    flow_version: str = DEFAULT_COMPARE_FLOW_VERSION,
) -> str | None:
    """Build a Kitaru UI compare URL for two or more executions.

    Example::

        {server}/flows/{flow_id}/v/local/compare?executions={id1},{id2},{id3}
    """
    normalized = [str(exec_id).strip() for exec_id in exec_ids if str(exec_id).strip()]
    if len(normalized) < 2:
        return None
    if server_url is None or flow_id is None:
        return None

    flow_segment = quote(str(flow_id), safe="")
    version_segment = quote(
        flow_version.strip() or DEFAULT_COMPARE_FLOW_VERSION,
        safe="",
    )
    execution_segment = quote(",".join(normalized), safe=",")
    return (
        f"{server_url.rstrip('/')}/flows/{flow_segment}/v/{version_segment}/compare"
        f"?executions={execution_segment}"
    )


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
    return build_compare_url_for_executions(
        server_url=server_url,
        flow_id=flow_id,
        exec_ids=[original_exec_id, replay_exec_id],
        flow_version=flow_version,
    )


def compare_url_for_executions(
    exec_ids: Sequence[str],
    *,
    client: KitaruClient | None = None,
) -> str | None:
    """Build one UI compare URL from explicit execution IDs (2 or more)."""
    normalized = [str(exec_id).strip() for exec_id in exec_ids if str(exec_id).strip()]
    if len(normalized) < 2:
        return None

    resolved_client = client or KitaruClient()
    anchor = resolved_client.executions.get(normalized[0])
    return build_compare_url_for_executions(
        server_url=_client_server_url(resolved_client),
        flow_id=anchor.flow_id,
        exec_ids=normalized,
        flow_version=_resolve_compare_flow_version(anchor),
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
    checkpoint = original_cp or replay_cp
    name = checkpoint.name if checkpoint is not None else "unknown"
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


def _resolve_compare_flow_version(execution: Execution) -> str:
    metadata = execution.metadata if isinstance(execution.metadata, Mapping) else {}
    nested = metadata.get("kitaru_deployment")
    nested_mapping = nested if isinstance(nested, Mapping) else {}
    for key in (
        "deployment_version",
        "kitaru_deployment_version",
    ):
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    nested_version = nested_mapping.get("version")
    if nested_version is not None and str(nested_version).strip():
        return str(nested_version).strip()
    return DEFAULT_COMPARE_FLOW_VERSION


def serialize_checkpoint_diff(item: CheckpointDiff) -> dict[str, Any]:
    return {
        "name": item.name,
        "original_call_id": item.original_call_id,
        "replay_call_id": item.replay_call_id,
        "status_match": item.status_match,
        "duration_delta_ms": item.duration_delta_ms,
        "token_delta": item.token_delta,
        "artifact_hashes": {
            role: {"original": left, "replay": right}
            for role, (left, right) in item.artifact_hashes.items()
        },
    }


def serialize_execution_diff(item: ExecutionDiff) -> dict[str, Any]:
    return {
        "original_exec_id": item.original_exec_id,
        "compared": [
            {
                "replay_exec_id": replay_exec_id,
                "checkpoints": [
                    serialize_checkpoint_diff(checkpoint)
                    for checkpoint in checkpoint_diffs
                ],
            }
            for replay_exec_id, checkpoint_diffs in item.compared
        ],
        "urls": list(item.urls),
        "warnings": list(item.warnings),
    }


def serialize_cohort_diff(item: CohortDiff) -> dict[str, Any]:
    return serialize_diff_matrix(item)


def serialize_diff_matrix(item: CohortDiff) -> dict[str, Any]:
    return {
        "rows": [serialize_execution_diff(row) for row in item.rows],
    }


def _diff_impl(
    original: str,
    *executions: str,
) -> ExecutionDiff:
    client = KitaruClient()
    original_execution = client.executions.get(original)

    compared_exec_ids: list[str]
    warnings: list[str] = []
    if executions:
        compared_exec_ids = list(executions)
    else:
        flow_name = original_execution.flow_name
        candidates = client.executions.list(flow=flow_name, limit=_AUTO_DISCOVERY_LIMIT)
        if len(candidates) == _AUTO_DISCOVERY_LIMIT:
            warnings.append(
                "Replay auto-discovery inspected the newest 200 executions for "
                f"flow {flow_name}. Older replay children may exist. Pass replay "
                "execution IDs explicitly to diff all known replays."
            )
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

    server_url = _client_server_url(client)
    flow_id = original_execution.flow_id
    flow_version = _resolve_compare_flow_version(original_execution)
    if compared_exec_ids:
        multi_url = build_compare_url_for_executions(
            server_url=server_url,
            flow_id=flow_id,
            exec_ids=[original, *compared_exec_ids],
            flow_version=flow_version,
        )
        compare_urls = [multi_url] if multi_url else []
    else:
        compare_urls = []

    return ExecutionDiff(
        original_exec_id=original,
        compared=compared,
        urls=compare_urls,
        warnings=warnings,
    )


def diff(
    original: str,
    *executions: str,
) -> ExecutionDiff:
    """Compare an original execution against replay executions.

    When ``executions`` is omitted, all runs whose ``original_exec_id`` matches
    ``original`` are included.
    """
    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.DIFF_REQUESTED,
        {
            "auto_discover": not bool(executions),
            "replay_count": len(executions),
        },
    )
    return _diff_impl(original, *executions)


def _build_diff_matrix(exec_ids: Sequence[str] | Any) -> CohortDiff:
    from kitaru.analytics import AnalyticsEvent, track
    from kitaru.cohort import coerce_exec_ids

    resolved_ids = coerce_exec_ids(exec_ids)
    track(
        AnalyticsEvent.DIFF_REQUESTED,
        {
            "auto_discover": True,
            "replay_count": len(resolved_ids),
            "cohort": True,
        },
    )
    return CohortDiff(rows=[_diff_impl(exec_id) for exec_id in resolved_ids])


def diff_cohort(
    exec_ids: Sequence[str] | Any,
) -> CohortDiff:
    """Compare many original executions against their discovered replays.

    Each entry in ``exec_ids`` is diffed with auto-discovery of replay
    executions linked via ``original_exec_id``.
    """
    return _build_diff_matrix(exec_ids)


def diff_matrix(exec_ids: Sequence[str] | Any) -> CohortDiff:
    """Compare many original executions against their discovered replays."""
    return _build_diff_matrix(exec_ids)


__all__ = [
    "DEFAULT_COMPARE_FLOW_VERSION",
    "CheckpointDiff",
    "CohortDiff",
    "ExecutionDiff",
    "build_compare_url",
    "build_compare_url_for_executions",
    "build_compare_urls",
    "compare_url_for_executions",
    "compare_urls_for_replay",
    "diff",
    "diff_cohort",
    "diff_matrix",
    "serialize_checkpoint_diff",
    "serialize_cohort_diff",
    "serialize_diff_matrix",
    "serialize_execution_diff",
]
