"""Client-side cohort selection for batch replay experiments."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal

from kitaru._client._models import CheckpointCall, Execution, ExecutionStatus
from kitaru.errors import KitaruUsageError

if TYPE_CHECKING:
    from kitaru.client import KitaruClient

_SUPPORTED_ORDER_FIELDS = frozenset(
    {"started_at", "display_cost_usd", "total_tokens", "duration"}
)


def coerce_exec_ids(executions: CohortResult | Sequence[str]) -> list[str]:
    """Return exec IDs from a resolved cohort or plain sequence."""
    if isinstance(executions, CohortResult):
        return list(executions.exec_ids)
    return [str(item) for item in executions]


@dataclass(frozen=True)
class CohortQuery:
    """Declarative cohort selection query."""

    flow: str
    at: str
    deployment: str | None = None
    deployment_version: int | None = None
    order_by: str = "-started_at"
    limit: int = 50
    originals_only: bool = True
    status: str | Sequence[str] = "completed"
    since: datetime | str | None = None
    until: datetime | str | None = None
    _client: KitaruClient | None = field(default=None, repr=False, compare=False)

    def resolve(self, *, max_scan: int = 500) -> CohortResult:
        """Resolve the query to a frozen exec-id list and selection metadata."""
        from kitaru.client import KitaruClient

        client = self._client or KitaruClient()
        return _resolve_cohort(client, self, max_scan=max_scan)


@dataclass(frozen=True)
class CohortResult:
    """Resolved cohort snapshot."""

    exec_ids: list[str]
    flow: str
    at: str
    deployment: str | None
    deployment_version: int | None
    order_by: str
    scanned: int
    matched: int
    partial: bool
    filtered: dict[str, int]
    ranked: tuple[tuple[str, float | None], ...] = ()

    def __len__(self) -> int:
        return len(self.exec_ids)

    def __iter__(self) -> Iterator[str]:
        return iter(self.exec_ids)

    def __getitem__(self, index: int) -> str:
        return self.exec_ids[index]

    def to_json(self) -> dict[str, Any]:
        """Serialize the cohort snapshot for audit artifacts."""
        return {
            "flow": self.flow,
            "at": self.at,
            "deployment": self.deployment,
            "deployment_version": self.deployment_version,
            "order_by": self.order_by,
            "exec_ids": list(self.exec_ids),
            "scanned": self.scanned,
            "matched": self.matched,
            "partial": self.partial,
            "filtered": dict(self.filtered),
            "ranked": [
                {"exec_id": exec_id, "sort_value": value}
                for exec_id, value in self.ranked
            ],
        }


def cohort(
    *,
    flow: str,
    at: str,
    deployment: str | None = None,
    deployment_version: int | None = None,
    order_by: str = "-started_at",
    limit: int = 50,
    originals_only: bool = True,
    status: str | Sequence[str] = "completed",
    since: datetime | str | None = None,
    until: datetime | str | None = None,
    client: KitaruClient | None = None,
) -> CohortQuery:
    """Build a cohort selection query for ``resolve()``."""
    return CohortQuery(
        flow=flow,
        at=at,
        deployment=deployment,
        deployment_version=deployment_version,
        order_by=order_by,
        limit=limit,
        originals_only=originals_only,
        status=status,
        since=since,
        until=until,
        _client=client,
    )


def _checkpoint_selectors(checkpoint: CheckpointCall) -> set[str]:
    selectors = {checkpoint.name, checkpoint.call_id}
    invocation_id = getattr(checkpoint, "invocation_id", None)
    if isinstance(invocation_id, str) and invocation_id:
        selectors.add(invocation_id)
    return selectors


def execution_replay_at_status(
    *, execution: Execution, at: str
) -> Literal[
    "present",
    "missing",
    "ambiguous",
    "no_checkpoints",
]:
    """Return whether ``at`` resolves on a hydrated ``Execution``."""
    checkpoints = execution.checkpoints
    if not checkpoints:
        return "no_checkpoints"

    matches = [
        checkpoint
        for checkpoint in checkpoints
        if at in _checkpoint_selectors(checkpoint)
    ]
    if len(matches) == 1:
        return "present"
    if len(matches) > 1:
        return "ambiguous"
    return "missing"


def _execution_replay_at_status_resolved(
    client: KitaruClient,
    execution: Execution,
    at: str,
) -> Literal["present", "missing", "ambiguous", "no_checkpoints"]:
    """Resolve replay anchor presence, hydrating list summaries when needed."""
    status = execution_replay_at_status(execution=execution, at=at)
    if status != "no_checkpoints":
        return status
    hydrated = client.executions.get(execution.exec_id)
    return execution_replay_at_status(execution=hydrated, at=at)


def _parse_order_by(order_by: str) -> tuple[str, bool]:
    normalized = order_by.strip()
    if not normalized:
        raise KitaruUsageError("`order_by` must be a non-empty field name.")
    if normalized.startswith("-"):
        field_name = normalized[1:].strip()
        if not field_name:
            raise KitaruUsageError(
                f"Invalid `order_by` value {order_by!r}. Use a field name such as "
                "'-display_cost_usd'."
            )
        descending = True
    else:
        field_name = normalized
        descending = False
    if field_name not in _SUPPORTED_ORDER_FIELDS:
        supported = ", ".join(sorted(_SUPPORTED_ORDER_FIELDS))
        raise KitaruUsageError(
            f"Unsupported order_by field {field_name!r}. Supported fields: {supported}."
        )
    return field_name, descending


def _parse_instant(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10:
        return datetime.combine(date.fromisoformat(text), datetime.min.time())
    return datetime.fromisoformat(text)


def _coerce_statuses(status: str | Sequence[str]) -> frozenset[ExecutionStatus]:
    values = [status] if isinstance(status, str) else list(status)
    allowed: set[ExecutionStatus] = set()
    for item in values:
        normalized = str(item).strip().lower()
        if normalized == "completed":
            allowed.add(ExecutionStatus.COMPLETED)
        elif normalized == "failed":
            allowed.add(ExecutionStatus.FAILED)
        elif normalized == "running":
            allowed.add(ExecutionStatus.RUNNING)
        elif normalized == "waiting":
            allowed.add(ExecutionStatus.WAITING)
        elif normalized == "cancelled":
            allowed.add(ExecutionStatus.CANCELLED)
        else:
            raise KitaruUsageError(
                f"Unsupported cohort status filter {item!r}. "
                "Use completed, failed, running, waiting, or cancelled."
            )
    if not allowed:
        raise KitaruUsageError("At least one status filter is required.")
    return frozenset(allowed)


def _execution_deployment_version(metadata: Mapping[str, Any]) -> int | str | None:
    nested = metadata.get("kitaru_deployment")
    nested_mapping = nested if isinstance(nested, Mapping) else {}
    for key in ("deployment_version", "kitaru_deployment_version"):
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return value
    nested_version = nested_mapping.get("version")
    if nested_version is not None and str(nested_version).strip():
        return nested_version
    return None


def _deployment_versions_match(
    execution_version: int | str | None,
    target_version: int | None,
) -> bool:
    if target_version is None:
        return True
    if execution_version is None:
        return False
    return str(execution_version).strip() == str(target_version).strip()


def _sort_value(execution: Execution, field_name: str) -> float | None:
    if field_name == "started_at":
        if execution.started_at is None:
            return None
        return execution.started_at.timestamp()
    if field_name == "duration":
        if execution.started_at is None or execution.ended_at is None:
            return None
        return (execution.ended_at - execution.started_at).total_seconds()
    summary = execution.llm_usage_summary or {}
    if field_name == "display_cost_usd":
        value = summary.get("display_cost_usd")
    elif field_name == "total_tokens":
        value = summary.get("total_tokens")
    else:
        return None
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rank_execution(
    execution: Execution,
    *,
    field_name: str,
    descending: bool,
) -> tuple[tuple[int, float], tuple[int, float], str]:
    primary = _sort_value(execution, field_name)
    if primary is None:
        primary_key = (1, 0.0)
    else:
        primary_key = (0, -primary if descending else primary)

    started = _sort_value(execution, "started_at")
    tie_started = (1, 0.0) if started is None else (0, -started)

    return primary_key, tie_started, execution.exec_id


def _within_window(
    execution: Execution,
    *,
    since: datetime | None,
    until: datetime | None,
) -> bool:
    if since is None and until is None:
        return True
    if execution.started_at is None:
        return False
    if since is not None and execution.started_at < since:
        return False
    return not (until is not None and execution.started_at > until)


def _resolve_target_deployment_version(
    client: KitaruClient,
    *,
    flow: str,
    deployment: str | None,
    deployment_version: int | None,
) -> int | None:
    if deployment_version is not None:
        return deployment_version
    if deployment is None:
        return None
    record = client.deployments.get(flow=flow, tag=deployment)
    return record.version


def _validate_query(query: CohortQuery) -> None:
    if not query.flow or not query.flow.strip():
        raise KitaruUsageError("`flow` is required.")
    if not query.at or not query.at.strip():
        raise KitaruUsageError("`at` is required.")
    if query.deployment is not None and query.deployment_version is not None:
        raise KitaruUsageError(
            "Pass either `deployment` or `deployment_version`, not both."
        )
    if isinstance(query.limit, bool) or query.limit < 1:
        raise KitaruUsageError("`limit` must be >= 1.")


def _resolve_cohort(
    client: KitaruClient,
    query: CohortQuery,
    *,
    max_scan: int,
) -> CohortResult:
    from kitaru.analytics import AnalyticsEvent, track

    _validate_query(query)
    if isinstance(max_scan, bool) or max_scan < 1:
        raise KitaruUsageError("`max_scan` must be >= 1.")

    field_name, descending = _parse_order_by(query.order_by)
    allowed_statuses = _coerce_statuses(query.status)
    since = _parse_instant(query.since)
    until = _parse_instant(query.until)
    target_version = _resolve_target_deployment_version(
        client,
        flow=query.flow,
        deployment=query.deployment,
        deployment_version=query.deployment_version,
    )

    filtered = {
        "status": 0,
        "originals": 0,
        "deployment": 0,
        "checkpoint": 0,
        "window": 0,
        "ambiguous_checkpoint": 0,
    }
    candidates: list[Execution] = []
    scanned = 0
    page = 1
    page_size = max(50, query.limit * 5)

    while scanned < max_scan:
        executions = client.executions.list(
            flow=query.flow,
            page=page,
            size=page_size,
        )
        if not executions:
            break

        for execution in executions:
            if scanned >= max_scan:
                break
            scanned += 1

            if execution.status not in allowed_statuses:
                filtered["status"] += 1
                continue
            if query.originals_only and execution.original_exec_id is not None:
                filtered["originals"] += 1
                continue
            if not _deployment_versions_match(
                _execution_deployment_version(execution.metadata),
                target_version,
            ):
                filtered["deployment"] += 1
                continue
            if not _within_window(execution, since=since, until=until):
                filtered["window"] += 1
                continue

            at_status = _execution_replay_at_status_resolved(
                client,
                execution,
                query.at,
            )
            if at_status == "present":
                candidates.append(execution)
            elif at_status == "ambiguous":
                filtered["ambiguous_checkpoint"] += 1
            else:
                filtered["checkpoint"] += 1

        if len(executions) < page_size:
            break
        page += 1

    ranked_candidates = sorted(
        candidates,
        key=lambda execution: _rank_execution(
            execution,
            field_name=field_name,
            descending=descending,
        ),
    )
    selected = ranked_candidates[: query.limit]
    exec_ids = [execution.exec_id for execution in selected]
    ranked = tuple(
        (execution.exec_id, _sort_value(execution, field_name))
        for execution in selected
    )

    partial = scanned >= max_scan and len(selected) < query.limit
    result = CohortResult(
        exec_ids=exec_ids,
        flow=query.flow,
        at=query.at,
        deployment=query.deployment,
        deployment_version=query.deployment_version or target_version,
        order_by=query.order_by,
        scanned=scanned,
        matched=len(exec_ids),
        partial=partial,
        filtered=filtered,
        ranked=ranked,
    )

    track(
        AnalyticsEvent.COHORT_RESOLVED,
        {
            "flow": query.flow,
            "matched": result.matched,
            "scanned": result.scanned,
            "partial": result.partial,
            "limit": query.limit,
        },
    )

    if result.matched == 0:
        raise KitaruUsageError(
            "Cohort selection matched 0 executions. "
            f"flow={query.flow!r}, at={query.at!r}, "
            f"deployment={query.deployment!r}, "
            f"deployment_version={query.deployment_version!r}, "
            f"scanned={scanned}, filtered={filtered}."
        )

    return result


__all__ = [
    "CohortQuery",
    "CohortResult",
    "coerce_exec_ids",
    "cohort",
    "execution_replay_at_status",
]
