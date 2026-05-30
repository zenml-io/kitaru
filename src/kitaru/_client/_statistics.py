"""Execution statistics helpers for the Kitaru client."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from kitaru._client._mappers import (
    _RAW_STATUS_TO_PUBLIC_STATUS,
    _RAW_STATUSES_BY_PUBLIC_STATUS,
    _to_public_status,
)
from kitaru._client._models import (
    ExecutionStatistics,
    ExecutionStatisticsDimension,
    ExecutionStatisticsGroup,
    ExecutionStatisticsGrouping,
    ExecutionStatisticsTimeGranularity,
    ExecutionStatus,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruUsageError,
)

if TYPE_CHECKING:
    from kitaru.client import KitaruClient

JsonScalar = str | int | float | bool | None
GroupMergeKey = tuple[tuple[str, JsonScalar], ...]

_RAW_STATUS_COUNT = len(_RAW_STATUS_TO_PUBLIC_STATUS)


@dataclass(frozen=True)
class _ZenMLStatisticsModels:
    """Lazily imported ZenML statistics model classes."""

    metadata_grouping: Any
    pipeline_run_filter: Any
    run_statistics_request: Any
    simple_grouping: Any
    statistics_grouping_type: Any
    statistics_time_granularity: Any
    time_grouping: Any


@dataclass(frozen=True)
class _RunStatisticsRequestParts:
    """Validated request data that is reused after the backend returns."""

    request: Any
    groupings: list[ExecutionStatisticsGrouping]
    max_groups: int


def _statistics_model_import_error(exc: Exception) -> KitaruFeatureNotAvailableError:
    """Build a clear error when the active ZenML lacks statistics support."""
    return KitaruFeatureNotAvailableError(
        "Execution statistics require ZenML 0.94.5 or newer on the active "
        "Kitaru runtime. Upgrade the client/server environment and retry."
    )


def _load_zenml_statistics_models() -> _ZenMLStatisticsModels:
    """Load ZenML statistics models lazily for server-skew error handling."""
    try:
        from zenml.models import (  # type: ignore[attr-defined]
            MetadataGrouping,
            RunStatisticsRequest,
            SimpleGrouping,
            StatisticsGroupingType,
            StatisticsTimeGranularity,
            TimeGrouping,
        )
        from zenml.models.v2.core.pipeline_run import PipelineRunFilter
    except ImportError as exc:
        raise _statistics_model_import_error(exc) from exc

    return _ZenMLStatisticsModels(
        metadata_grouping=MetadataGrouping,
        pipeline_run_filter=PipelineRunFilter,
        run_statistics_request=RunStatisticsRequest,
        simple_grouping=SimpleGrouping,
        statistics_grouping_type=StatisticsGroupingType,
        statistics_time_granularity=StatisticsTimeGranularity,
        time_grouping=TimeGrouping,
    )


def coerce_execution_statistics_grouping(
    grouping: ExecutionStatisticsGrouping | str,
) -> ExecutionStatisticsGrouping:
    """Normalize one public grouping object or string shorthand."""
    if isinstance(grouping, ExecutionStatisticsGrouping):
        return grouping
    if not isinstance(grouping, str):
        raise KitaruUsageError(
            "Execution statistics groupings must be strings or "
            "ExecutionStatisticsGrouping objects."
        )

    token = grouping.strip()
    if not token:
        raise KitaruUsageError("Execution statistics grouping tokens cannot be empty.")

    try:
        if token in {"status", "flow", "stack", "tag"}:
            return ExecutionStatisticsGrouping(token)
        if token.startswith("time:"):
            granularity = token.removeprefix("time:").strip()
            return ExecutionStatisticsGrouping(
                ExecutionStatisticsDimension.TIME,
                time_granularity=granularity,
            )
        if token.startswith("metadata:"):
            metadata_key = token.removeprefix("metadata:").strip()
            return ExecutionStatisticsGrouping(
                ExecutionStatisticsDimension.METADATA,
                metadata_key=metadata_key,
            )
    except ValueError as exc:
        raise KitaruUsageError(str(exc)) from exc

    raise KitaruUsageError(
        f"Unsupported execution statistics grouping {grouping!r}. Expected one "
        "of: status, flow, stack, tag, time:<hour|day|week|month>, "
        "metadata:<key>."
    )


def normalize_execution_statistics_groupings(
    group_by: Sequence[ExecutionStatisticsGrouping | str],
) -> list[ExecutionStatisticsGrouping]:
    """Normalize and validate a public grouping sequence."""
    groupings = [
        coerce_execution_statistics_grouping(grouping) for grouping in group_by
    ]
    names = [grouping.name for grouping in groupings]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        joined = ", ".join(repr(name) for name in duplicate_names)
        raise KitaruUsageError(
            "Duplicate execution statistics grouping output names are not "
            f"allowed: {joined}."
        )

    time_grouping_count = sum(
        1
        for grouping in groupings
        if grouping.dimension is ExecutionStatisticsDimension.TIME
    )
    if time_grouping_count > 1:
        raise KitaruUsageError("At most one time statistics grouping is allowed.")

    return groupings


def validate_statistics_max_groups(
    max_groups: int,
    *,
    label: str = "max_groups",
) -> int:
    """Validate the public max-groups limit."""
    display_label = f"`{label}`"
    if not isinstance(max_groups, int) or isinstance(max_groups, bool):
        raise KitaruUsageError(f"{display_label} must be an integer.")
    if max_groups < 1 or max_groups > 10_000:
        raise KitaruUsageError(f"{display_label} must be between 1 and 10000.")
    return max_groups


def normalize_execution_statistics_tags(
    tags: Sequence[str] | None,
) -> list[str] | None:
    """Normalize tag filters while rejecting accidental string fan-out."""
    if tags is None:
        return None
    if isinstance(tags, (str, bytes, bytearray)):
        raise KitaruUsageError(
            "`tags` must be a sequence of tag strings, not a string."
        )

    normalized_tags: list[str] = []
    for tag in tags:
        if not isinstance(tag, str):
            raise KitaruUsageError("`tags` must contain only strings.")
        normalized_tag = tag.strip()
        if not normalized_tag:
            raise KitaruUsageError("`tags` cannot contain empty strings.")
        normalized_tags.append(normalized_tag)
    return normalized_tags


def _backend_max_groups(
    *,
    groupings: Sequence[ExecutionStatisticsGrouping],
    public_max_groups: int,
    status_filter: ExecutionStatus | None,
) -> int:
    """Return a backend limit large enough for Kitaru status merging."""
    has_status_grouping = any(
        grouping.dimension is ExecutionStatisticsDimension.STATUS
        for grouping in groupings
    )
    if not has_status_grouping:
        return public_max_groups

    raw_status_count = (
        len(_RAW_STATUSES_BY_PUBLIC_STATUS[status_filter])
        if status_filter is not None
        else _RAW_STATUS_COUNT
    )
    return min(10_000, public_max_groups * raw_status_count)


def _coerce_public_status(
    status: ExecutionStatus | str | None,
) -> ExecutionStatus | None:
    """Normalize a public status filter."""
    if status is None:
        return None
    if isinstance(status, ExecutionStatus):
        return status
    normalized = status.strip().lower()
    try:
        return ExecutionStatus(normalized)
    except ValueError as exc:
        expected = ", ".join(item.value for item in ExecutionStatus)
        raise KitaruUsageError(
            f"Unsupported status filter {status!r}. Expected one of: {expected}."
        ) from exc


def _status_filter_value(public_status: ExecutionStatus | None) -> str | None:
    """Map a public status filter to the backend string-filter syntax."""
    if public_status is None:
        return None

    raw_statuses = _RAW_STATUSES_BY_PUBLIC_STATUS[public_status]
    if len(raw_statuses) == 1:
        return raw_statuses[0]
    return f"oneof:{json.dumps(list(raw_statuses), separators=(',', ':'))}"


def _zenml_grouping(
    grouping: ExecutionStatisticsGrouping,
    models: _ZenMLStatisticsModels,
) -> Any:
    """Map one public grouping to its upstream ZenML grouping model."""
    dimension = grouping.dimension
    grouping_name = grouping.name
    assert grouping_name is not None

    if dimension is ExecutionStatisticsDimension.STATUS:
        return models.simple_grouping(
            type=models.statistics_grouping_type.STATUS,
            name=grouping_name,
        )
    if dimension is ExecutionStatisticsDimension.FLOW:
        return models.simple_grouping(
            type=models.statistics_grouping_type.PIPELINE,
            name=grouping_name,
        )
    if dimension is ExecutionStatisticsDimension.STACK:
        return models.simple_grouping(
            type=models.statistics_grouping_type.STACK,
            name=grouping_name,
        )
    if dimension is ExecutionStatisticsDimension.TAG:
        return models.simple_grouping(
            type=models.statistics_grouping_type.TAG,
            name=grouping_name,
        )
    if dimension is ExecutionStatisticsDimension.TIME:
        assert grouping.time_granularity is not None
        granularity = ExecutionStatisticsTimeGranularity(str(grouping.time_granularity))
        return models.time_grouping(
            type=models.statistics_grouping_type.TIME,
            name=grouping_name,
            granularity=models.statistics_time_granularity(granularity.value),
        )
    if dimension is ExecutionStatisticsDimension.METADATA:
        assert grouping.metadata_key is not None
        return models.metadata_grouping(
            type=models.statistics_grouping_type.METADATA,
            name=grouping_name,
            metadata_key=grouping.metadata_key,
        )

    raise KitaruUsageError(
        f"Unsupported execution statistics dimension: {dimension!r}."
    )


def _build_run_statistics_request_parts(
    *,
    project: str | None,
    group_by: Sequence[ExecutionStatisticsGrouping | str],
    flow: str | None,
    status: ExecutionStatus | str | None,
    stack: str | None,
    tags: Sequence[str] | None,
    max_groups: int,
) -> _RunStatisticsRequestParts:
    """Build validated ZenML request data for execution statistics."""
    models = _load_zenml_statistics_models()
    normalized_groupings = normalize_execution_statistics_groupings(group_by)
    validated_max_groups = validate_statistics_max_groups(max_groups)
    normalized_tags = normalize_execution_statistics_tags(tags)
    public_status_filter = _coerce_public_status(status)

    try:
        run_filter = models.pipeline_run_filter(
            project=project,
            pipeline=flow,
            stack=stack,
            tags=normalized_tags,
            status=_status_filter_value(public_status_filter),
        )
        request = models.run_statistics_request(
            filter=run_filter,
            groupings=[
                _zenml_grouping(grouping, models) for grouping in normalized_groupings
            ],
            metrics=[],
            max_groups=_backend_max_groups(
                groupings=normalized_groupings,
                public_max_groups=validated_max_groups,
                status_filter=public_status_filter,
            ),
        )
    except ValidationError as exc:
        raise KitaruUsageError(f"Invalid execution statistics request: {exc}") from exc

    return _RunStatisticsRequestParts(
        request=request,
        groupings=normalized_groupings,
        max_groups=validated_max_groups,
    )


def build_run_statistics_request(
    *,
    project: str | None,
    group_by: Sequence[ExecutionStatisticsGrouping | str],
    flow: str | None,
    status: ExecutionStatus | str | None,
    stack: str | None,
    tags: Sequence[str] | None,
    max_groups: int,
) -> Any:
    """Build the ZenML run-statistics request for public execution statistics."""
    return _build_run_statistics_request_parts(
        project=project,
        group_by=group_by,
        flow=flow,
        status=status,
        stack=stack,
        tags=tags,
        max_groups=max_groups,
    ).request


def _normalize_raw_status_value(value: Any) -> str:
    """Return the raw backend status string from an upstream group key value."""
    return str(getattr(value, "value", value))


def _public_group_keys(
    raw_keys: dict[str, Any],
    *,
    status_grouping_names: set[str],
) -> dict[str, JsonScalar]:
    """Map backend group keys to public Kitaru group keys."""
    public_keys: dict[str, JsonScalar] = {}
    for key, value in raw_keys.items():
        if key in status_grouping_names and value is not None:
            status_value = _normalize_raw_status_value(value)
            try:
                public_keys[key] = _to_public_status(status_value).value
            except Exception as exc:
                raise KitaruBackendError(
                    f"Backend returned unsupported execution status {status_value!r}."
                ) from exc
        else:
            public_keys[key] = value
    return public_keys


def _group_sort_key(
    group: ExecutionStatisticsGroup,
    *,
    time_grouping_name: str | None,
) -> tuple[str, tuple[tuple[str, str], ...], int]:
    """Sort statistics groups deterministically."""
    if time_grouping_name is not None:
        time_value = repr(group.keys.get(time_grouping_name))
        other_items = tuple(
            (key, repr(value))
            for key, value in sorted(group.keys.items())
            if key != time_grouping_name
        )
        return (time_value, other_items, -group.execution_count)

    key_items = tuple((key, repr(value)) for key, value in sorted(group.keys.items()))
    return ("", key_items, -group.execution_count)


def _map_run_statistics_response_with_groupings(
    response: Any,
    *,
    normalized_groupings: Sequence[ExecutionStatisticsGrouping],
    max_groups: int,
) -> ExecutionStatistics:
    """Map a run-statistics response using already-normalized groupings."""
    status_grouping_names = {
        grouping.name
        for grouping in normalized_groupings
        if grouping.dimension is ExecutionStatisticsDimension.STATUS
        and grouping.name is not None
    }
    time_grouping_name = next(
        (
            grouping.name
            for grouping in normalized_groupings
            if grouping.dimension is ExecutionStatisticsDimension.TIME
        ),
        None,
    )

    merged_counts: dict[GroupMergeKey, int] = {}
    merged_keys: dict[GroupMergeKey, dict[str, JsonScalar]] = {}

    for group in getattr(response, "groups", []):
        raw_keys = dict(getattr(group, "group_keys", {}))
        public_keys = _public_group_keys(
            raw_keys,
            status_grouping_names=status_grouping_names,
        )
        merge_key = tuple(sorted(public_keys.items()))
        merged_keys.setdefault(merge_key, public_keys)
        merged_counts[merge_key] = merged_counts.get(merge_key, 0) + int(
            getattr(group, "run_count", 0)
        )

    groups = [
        ExecutionStatisticsGroup(
            keys=merged_keys[merge_key],
            execution_count=execution_count,
        )
        for merge_key, execution_count in merged_counts.items()
    ]
    groups.sort(
        key=lambda group: _group_sort_key(
            group,
            time_grouping_name=time_grouping_name,
        )
    )

    truncated = bool(getattr(response, "truncated", False))
    if len(groups) > max_groups:
        groups = groups[:max_groups]
        truncated = True

    return ExecutionStatistics(groups=groups, truncated=truncated)


def map_run_statistics_response(
    response: Any,
    *,
    group_by: Sequence[ExecutionStatisticsGrouping | str] = (),
    max_groups: int,
) -> ExecutionStatistics:
    """Map an upstream run-statistics response to Kitaru DTOs."""
    validated_max_groups = validate_statistics_max_groups(max_groups)
    normalized_groupings = normalize_execution_statistics_groupings(group_by)
    return _map_run_statistics_response_with_groupings(
        response,
        normalized_groupings=normalized_groupings,
        max_groups=validated_max_groups,
    )


def _is_statistics_endpoint_missing_error(exc: Exception) -> bool:
    """Return whether a backend error looks like an old statistics endpoint."""
    message = str(exc).lower()
    missing_markers = (
        "404",
        "not found",
        "no route",
        "route not",
        "method not allowed",
    )
    statistics_markers = ("statistics", "run statistics", "runs/statistics")
    return any(marker in message for marker in missing_markers) and any(
        marker in message for marker in statistics_markers
    )


def get_execution_statistics(
    *,
    client: KitaruClient,
    group_by: Sequence[ExecutionStatisticsGrouping | str] = (),
    flow: str | None = None,
    status: ExecutionStatus | str | None = None,
    stack: str | None = None,
    tags: Sequence[str] | None = None,
    max_groups: int = 1000,
) -> ExecutionStatistics:
    """Fetch grouped execution statistics from the active ZenML store."""
    request_parts = _build_run_statistics_request_parts(
        project=client._project,
        group_by=group_by,
        flow=flow,
        status=status,
        stack=stack,
        tags=tags,
        max_groups=max_groups,
    )

    try:
        zen_store = client._client().zen_store
        get_run_statistics = zen_store.get_run_statistics
    except AttributeError as exc:
        raise _statistics_model_import_error(exc) from exc

    try:
        response = get_run_statistics(request_parts.request)
    except AttributeError as exc:
        raise _statistics_model_import_error(exc) from exc
    except Exception as exc:
        if _is_statistics_endpoint_missing_error(exc):
            raise _statistics_model_import_error(exc) from exc
        raise KitaruBackendError(
            f"Failed to fetch execution statistics: {exc}"
        ) from exc

    return _map_run_statistics_response_with_groupings(
        response,
        normalized_groupings=request_parts.groupings,
        max_groups=request_parts.max_groups,
    )


__all__ = [
    "build_run_statistics_request",
    "coerce_execution_statistics_grouping",
    "get_execution_statistics",
    "map_run_statistics_response",
    "normalize_execution_statistics_groupings",
    "normalize_execution_statistics_tags",
    "validate_statistics_max_groups",
]
