"""Execution statistics helpers for the Kitaru client."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from kitaru._client._mappers import (
    _RAW_STATUSES_BY_PUBLIC_STATUS,
    _coerce_status_filter,
    _to_public_status,
)
from kitaru._client._models import (
    ExecutionStatistics,
    ExecutionStatisticsDimension,
    ExecutionStatisticsGroup,
    ExecutionStatisticsGrouping,
    ExecutionStatisticsMetric,
    ExecutionStatisticsMetricAggregation,
    ExecutionStatisticsMetricSource,
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
GroupEntry = tuple[dict[str, Any], int, dict[str, float | None]]


@dataclass(frozen=True)
class _ZenMLStatisticsModels:
    """Lazily imported ZenML statistics model classes."""

    metadata_grouping: Any
    metadata_metric: Any
    pipeline_run_filter: Any
    run_statistics_request: Any
    simple_grouping: Any
    simple_metric: Any
    statistics_aggregation: Any
    statistics_grouping_type: Any
    statistics_metric_source: Any
    statistics_time_granularity: Any
    time_grouping: Any


@dataclass(frozen=True)
class _RunStatisticsRequestParts:
    """Validated request data that is reused after the backend returns."""

    request: Any
    groupings: list[ExecutionStatisticsGrouping]
    backend_groupings: list[ExecutionStatisticsGrouping]
    metrics: list[ExecutionStatisticsMetric]
    max_groups: int
    public_status_filter: ExecutionStatus | None
    status_grouping_name: str | None


@dataclass
class _StatisticsGroupAccumulator:
    """Incrementally build one public statistics group from backend groups."""

    keys: dict[str, JsonScalar]
    metrics_by_name: Mapping[str, ExecutionStatisticsMetric]
    execution_count: int = 0
    metrics: dict[str, float | None] = field(default_factory=dict)
    metric_weights: dict[str, int] = field(default_factory=dict)
    ambiguous_metadata_avg_metrics: set[str] = field(default_factory=set)

    def add_backend_group(
        self,
        *,
        run_count: int,
        metrics: Mapping[str, float | None],
    ) -> None:
        """Merge one backend statistics group into this public group."""
        previous_count = self.execution_count
        self.execution_count += run_count
        for metric_name, value in metrics.items():
            self._add_metric_value(
                metric_name=metric_name,
                value=value,
                run_count=run_count,
                previous_count=previous_count,
            )

    def to_public_group(self) -> ExecutionStatisticsGroup:
        """Return the public DTO for this accumulated group."""
        return ExecutionStatisticsGroup(
            keys=self.keys,
            execution_count=self.execution_count,
            metrics=self.metrics,
        )

    def _add_metric_value(
        self,
        *,
        metric_name: str,
        value: float | None,
        run_count: int,
        previous_count: int,
    ) -> None:
        """Merge one metric value, preserving safe semantics for each aggregation."""
        if value is None:
            self.metrics.setdefault(metric_name, None)
            return

        metric = self.metrics_by_name.get(metric_name)
        aggregation = (
            ExecutionStatisticsMetricAggregation(metric.aggregation)
            if metric is not None
            else ExecutionStatisticsMetricAggregation.SUM
        )
        metric_source = (
            ExecutionStatisticsMetricSource(metric.source)
            if metric is not None
            else None
        )
        if metric_name in self.ambiguous_metadata_avg_metrics:
            self.metrics[metric_name] = None
            return

        current_value = self.metrics.get(metric_name)
        if (
            aggregation is ExecutionStatisticsMetricAggregation.AVG
            and metric_source is ExecutionStatisticsMetricSource.METADATA
            and current_value is not None
        ):
            self.metrics[metric_name] = None
            self.ambiguous_metadata_avg_metrics.add(metric_name)
            return

        self.metrics[metric_name] = self._merge_metric_value(
            metric_name=metric_name,
            current_value=current_value,
            new_value=float(value),
            aggregation=aggregation,
            run_count=run_count,
        )
        self._update_metric_weight(
            metric_name=metric_name,
            aggregation=aggregation,
            run_count=run_count,
            previous_count=previous_count,
        )

    def _merge_metric_value(
        self,
        *,
        metric_name: str,
        current_value: float | None,
        new_value: float,
        aggregation: ExecutionStatisticsMetricAggregation,
        run_count: int,
    ) -> float:
        """Return the merged metric value for one aggregation operator."""
        if current_value is None:
            return new_value
        if aggregation is ExecutionStatisticsMetricAggregation.SUM:
            return current_value + new_value
        if aggregation is ExecutionStatisticsMetricAggregation.MIN:
            return min(current_value, new_value)
        if aggregation is ExecutionStatisticsMetricAggregation.MAX:
            return max(current_value, new_value)
        if aggregation is ExecutionStatisticsMetricAggregation.AVG:
            current_weight = self.metric_weights.get(metric_name, 0)
            total_weight = current_weight + run_count
            if total_weight <= 0:
                return new_value
            return ((current_value * current_weight) + (new_value * run_count)) / (
                total_weight
            )

        raise KitaruUsageError(
            f"Unsupported execution statistics metric aggregation: {aggregation!r}."
        )

    def _update_metric_weight(
        self,
        *,
        metric_name: str,
        aggregation: ExecutionStatisticsMetricAggregation,
        run_count: int,
        previous_count: int,
    ) -> None:
        """Track denominator information needed for later average merges."""
        if aggregation is ExecutionStatisticsMetricAggregation.AVG:
            self.metric_weights[metric_name] = (
                self.metric_weights.get(metric_name, 0) + run_count
            )
        else:
            self.metric_weights[metric_name] = previous_count + run_count


def _statistics_model_import_error(exc: Exception) -> KitaruFeatureNotAvailableError:
    """Build a clear error when the active ZenML lacks statistics support."""
    return KitaruFeatureNotAvailableError(
        "Execution statistics require ZenML 0.94.6 or newer on the active "
        "Kitaru runtime. Upgrade the client/server environment and retry."
    )


def _load_zenml_statistics_models() -> _ZenMLStatisticsModels:
    """Load ZenML statistics models lazily for server-skew error handling."""
    try:
        from zenml.models import (  # type: ignore[attr-defined]
            MetadataGrouping,
            MetadataMetric,
            RunStatisticsRequest,
            SimpleGrouping,
            SimpleMetric,
            StatisticsAggregation,
            StatisticsGroupingType,
            StatisticsMetricSource,
            StatisticsTimeGranularity,
            TimeGrouping,
        )
        from zenml.models.v2.core.pipeline_run import PipelineRunFilter
    except ImportError as exc:
        raise _statistics_model_import_error(exc) from exc

    return _ZenMLStatisticsModels(
        metadata_grouping=MetadataGrouping,
        metadata_metric=MetadataMetric,
        pipeline_run_filter=PipelineRunFilter,
        run_statistics_request=RunStatisticsRequest,
        simple_grouping=SimpleGrouping,
        simple_metric=SimpleMetric,
        statistics_aggregation=StatisticsAggregation,
        statistics_grouping_type=StatisticsGroupingType,
        statistics_metric_source=StatisticsMetricSource,
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


def _validate_unique_names(
    names: Iterable[str],
    *,
    description: str,
) -> None:
    """Reject duplicate public output names in linear time."""
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        if name in seen:
            duplicates.add(name)
        else:
            seen.add(name)
    if duplicates:
        joined = ", ".join(repr(name) for name in sorted(duplicates))
        raise KitaruUsageError(f"Duplicate {description} are not allowed: {joined}.")


def normalize_execution_statistics_groupings(
    group_by: Sequence[ExecutionStatisticsGrouping | str],
) -> list[ExecutionStatisticsGrouping]:
    """Normalize and validate a public grouping sequence."""
    groupings = [
        coerce_execution_statistics_grouping(grouping) for grouping in group_by
    ]
    _validate_unique_names(
        (grouping.name for grouping in groupings if grouping.name is not None),
        description="execution statistics grouping output names",
    )

    time_grouping_count = sum(
        1
        for grouping in groupings
        if grouping.dimension is ExecutionStatisticsDimension.TIME
    )
    if time_grouping_count > 1:
        raise KitaruUsageError("At most one time statistics grouping is allowed.")

    return groupings


def coerce_execution_statistics_metric(
    metric: ExecutionStatisticsMetric | Mapping[str, Any] | str,
) -> ExecutionStatisticsMetric:
    """Normalize one public metric object, mapping, or CLI string shorthand."""
    if isinstance(metric, ExecutionStatisticsMetric):
        return metric
    if isinstance(metric, Mapping):
        return ExecutionStatisticsMetric(**dict(metric))
    if not isinstance(metric, str):
        raise KitaruUsageError(
            "Execution statistics metrics must be strings, mappings, or "
            "ExecutionStatisticsMetric objects."
        )

    token = metric.strip()
    if not token:
        raise KitaruUsageError("Execution statistics metric tokens cannot be empty.")

    parts = [part.strip() for part in token.split(":")]
    if len(parts) == 3:
        name, source, aggregation = parts
        return ExecutionStatisticsMetric(
            name=name,
            source=source,
            aggregation=aggregation,
        )
    if len(parts) == 4 and parts[1] == ExecutionStatisticsMetricSource.METADATA.value:
        name, source, metadata_key, aggregation = parts
        return ExecutionStatisticsMetric(
            name=name,
            source=source,
            aggregation=aggregation,
            metadata_key=metadata_key,
        )

    raise KitaruUsageError(
        f"Unsupported execution statistics metric {metric!r}. Expected "
        "<name>:<source>:<avg|sum|min|max> for built-in sources or "
        "<name>:metadata:<metadata_key>:<avg|sum|min|max> for metadata."
    )


def normalize_execution_statistics_metrics(
    metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str],
) -> list[ExecutionStatisticsMetric]:
    """Normalize and validate a public metric sequence."""
    normalized_metrics = [
        coerce_execution_statistics_metric(metric) for metric in metrics
    ]
    _validate_unique_names(
        (metric.name for metric in normalized_metrics),
        description="execution statistics metric names",
    )
    return normalized_metrics


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


def _status_filter_value(public_status: ExecutionStatus | None) -> str | None:
    """Map a public status filter to the backend string-filter syntax."""
    if public_status is None:
        return None

    raw_statuses = _RAW_STATUSES_BY_PUBLIC_STATUS[public_status]
    if len(raw_statuses) == 1:
        return raw_statuses[0]
    return f"oneof:{json.dumps(list(raw_statuses), separators=(',', ':'))}"


def _status_grouping_name(
    groupings: Sequence[ExecutionStatisticsGrouping],
) -> str | None:
    """Return the public status grouping output name, if present."""
    return next(
        (
            grouping.name
            for grouping in groupings
            if grouping.dimension is ExecutionStatisticsDimension.STATUS
        ),
        None,
    )


def _backend_groupings_for_status_split(
    groupings: Sequence[ExecutionStatisticsGrouping],
) -> list[ExecutionStatisticsGrouping]:
    """Return backend groupings with public status removed."""
    return [
        grouping
        for grouping in groupings
        if grouping.dimension is not ExecutionStatisticsDimension.STATUS
    ]


def _public_status_buckets_to_query(
    status_filter: ExecutionStatus | None,
) -> tuple[ExecutionStatus, ...]:
    """Return the public status buckets needed for an exact status query."""
    if status_filter is not None:
        return (status_filter,)
    return tuple(ExecutionStatus)


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


def _zenml_metric(
    metric: ExecutionStatisticsMetric,
    models: _ZenMLStatisticsModels,
) -> Any:
    """Map one public metric to its upstream ZenML metric model."""
    metric_aggregation = ExecutionStatisticsMetricAggregation(metric.aggregation)
    metric_source = ExecutionStatisticsMetricSource(metric.source)
    aggregation = models.statistics_aggregation(metric_aggregation.value)
    source = models.statistics_metric_source(metric_source.value)

    if metric_source is ExecutionStatisticsMetricSource.METADATA:
        assert metric.metadata_key is not None
        return models.metadata_metric(
            name=metric.name,
            aggregation=aggregation,
            source=source,
            metadata_key=metric.metadata_key,
        )

    return models.simple_metric(
        name=metric.name,
        aggregation=aggregation,
        source=source,
    )


def _build_run_statistics_request_parts(
    *,
    project: str | None,
    group_by: Sequence[ExecutionStatisticsGrouping | str],
    metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str],
    flow: str | None,
    status: ExecutionStatus | str | None,
    stack: str | None,
    tags: Sequence[str] | None,
    max_groups: int,
) -> _RunStatisticsRequestParts:
    """Build validated ZenML request data for execution statistics."""
    models = _load_zenml_statistics_models()
    normalized_groupings = normalize_execution_statistics_groupings(group_by)
    normalized_metrics = normalize_execution_statistics_metrics(metrics)
    validated_max_groups = validate_statistics_max_groups(max_groups)
    normalized_tags = normalize_execution_statistics_tags(tags)
    public_status_filter = _coerce_status_filter(status)
    status_grouping_name = _status_grouping_name(normalized_groupings)
    backend_groupings = (
        _backend_groupings_for_status_split(normalized_groupings)
        if status_grouping_name is not None
        else list(normalized_groupings)
    )

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
                _zenml_grouping(grouping, models) for grouping in backend_groupings
            ],
            metrics=[_zenml_metric(metric, models) for metric in normalized_metrics],
            max_groups=validated_max_groups,
        )
    except ValidationError as exc:
        raise KitaruUsageError(f"Invalid execution statistics request: {exc}") from exc

    return _RunStatisticsRequestParts(
        request=request,
        groupings=normalized_groupings,
        backend_groupings=backend_groupings,
        metrics=normalized_metrics,
        max_groups=validated_max_groups,
        public_status_filter=public_status_filter,
        status_grouping_name=status_grouping_name,
    )


def build_run_statistics_request(
    *,
    project: str | None,
    group_by: Sequence[ExecutionStatisticsGrouping | str],
    metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str] = (),
    flow: str | None,
    status: ExecutionStatus | str | None,
    stack: str | None,
    tags: Sequence[str] | None,
    max_groups: int,
) -> Any:
    """Build one ZenML run-statistics request for public execution statistics."""
    normalized_groupings = normalize_execution_statistics_groupings(group_by)
    if _status_grouping_name(normalized_groupings) is not None:
        raise KitaruUsageError(
            "`build_run_statistics_request` cannot build status-grouped "
            "statistics because exact public status grouping requires multiple "
            "backend requests. Use `get_execution_statistics(...)` instead."
        )

    return _build_run_statistics_request_parts(
        project=project,
        group_by=normalized_groupings,
        metrics=metrics,
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
                public_keys[key] = ExecutionStatus(status_value).value
            except ValueError:
                try:
                    public_keys[key] = _to_public_status(status_value).value
                except Exception as exc:
                    message = (
                        "Backend returned unsupported execution status "
                        f"{status_value!r}."
                    )
                    raise KitaruBackendError(message) from exc
        else:
            public_keys[key] = value
    return public_keys


def _group_sort_key(
    group: ExecutionStatisticsGroup,
    *,
    time_grouping_name: str | None,
) -> tuple[Any, ...]:
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
    return (-group.execution_count, key_items)


def _map_run_statistics_group_entries(
    group_entries: Iterable[GroupEntry],
    *,
    normalized_groupings: Sequence[ExecutionStatisticsGrouping],
    normalized_metrics: Sequence[ExecutionStatisticsMetric],
    max_groups: int,
    truncated: bool,
) -> ExecutionStatistics:
    """Map and merge backend statistics groups using normalized public groupings."""
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

    metrics_by_name = {metric.name: metric for metric in normalized_metrics}
    accumulators: dict[GroupMergeKey, _StatisticsGroupAccumulator] = {}

    for raw_keys, run_count, raw_metrics in group_entries:
        public_keys = _public_group_keys(
            raw_keys,
            status_grouping_names=status_grouping_names,
        )
        merge_key = tuple(sorted(public_keys.items()))
        accumulator = accumulators.setdefault(
            merge_key,
            _StatisticsGroupAccumulator(
                keys=public_keys,
                metrics_by_name=metrics_by_name,
            ),
        )
        accumulator.add_backend_group(run_count=run_count, metrics=raw_metrics)

    groups = [accumulator.to_public_group() for accumulator in accumulators.values()]
    groups.sort(
        key=lambda group: _group_sort_key(
            group,
            time_grouping_name=time_grouping_name,
        )
    )

    if len(groups) > max_groups:
        if time_grouping_name is not None:
            groups = groups[-max_groups:]
        else:
            groups = groups[:max_groups]
        truncated = True

    return ExecutionStatistics(groups=groups, truncated=truncated)


def _map_run_statistics_response_with_groupings(
    response: Any,
    *,
    normalized_groupings: Sequence[ExecutionStatisticsGrouping],
    normalized_metrics: Sequence[ExecutionStatisticsMetric],
    max_groups: int,
) -> ExecutionStatistics:
    """Map a run-statistics response using already-normalized groupings."""
    return _map_run_statistics_group_entries(
        (
            (
                dict(getattr(group, "group_keys", {})),
                int(getattr(group, "run_count", 0)),
                dict(getattr(group, "metrics", {})),
            )
            for group in getattr(response, "groups", [])
        ),
        normalized_groupings=normalized_groupings,
        normalized_metrics=normalized_metrics,
        max_groups=max_groups,
        truncated=bool(getattr(response, "truncated", False)),
    )


def map_run_statistics_response(
    response: Any,
    *,
    group_by: Sequence[ExecutionStatisticsGrouping | str] = (),
    metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str] = (),
    max_groups: int,
) -> ExecutionStatistics:
    """Map an upstream run-statistics response to Kitaru DTOs."""
    validated_max_groups = validate_statistics_max_groups(max_groups)
    normalized_groupings = normalize_execution_statistics_groupings(group_by)
    normalized_metrics = normalize_execution_statistics_metrics(metrics)
    return _map_run_statistics_response_with_groupings(
        response,
        normalized_groupings=normalized_groupings,
        normalized_metrics=normalized_metrics,
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
    metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str] = (),
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
        metrics=metrics,
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

    def fetch_response(request: Any) -> Any:
        try:
            return get_run_statistics(request)
        except AttributeError as exc:
            raise _statistics_model_import_error(exc) from exc
        except Exception as exc:
            if _is_statistics_endpoint_missing_error(exc):
                raise _statistics_model_import_error(exc) from exc
            raise KitaruBackendError(
                f"Failed to fetch execution statistics: {exc}"
            ) from exc

    if request_parts.status_grouping_name is None:
        response = fetch_response(request_parts.request)
        return _map_run_statistics_response_with_groupings(
            response,
            normalized_groupings=request_parts.groupings,
            normalized_metrics=request_parts.metrics,
            max_groups=request_parts.max_groups,
        )

    group_entries: list[GroupEntry] = []
    truncated = False
    for public_status in _public_status_buckets_to_query(
        request_parts.public_status_filter
    ):
        if request_parts.public_status_filter is not None:
            bucket_request = request_parts.request
        else:
            bucket_request = _build_run_statistics_request_parts(
                project=client._project,
                group_by=request_parts.backend_groupings,
                metrics=request_parts.metrics,
                flow=flow,
                status=public_status,
                stack=stack,
                tags=tags,
                max_groups=request_parts.max_groups,
            ).request
        response = fetch_response(bucket_request)
        truncated = truncated or bool(getattr(response, "truncated", False))

        for group in getattr(response, "groups", []):
            raw_keys = dict(getattr(group, "group_keys", {}))
            raw_keys[request_parts.status_grouping_name] = public_status.value
            group_entries.append(
                (
                    raw_keys,
                    int(getattr(group, "run_count", 0)),
                    dict(getattr(group, "metrics", {})),
                )
            )

    return _map_run_statistics_group_entries(
        group_entries,
        normalized_groupings=request_parts.groupings,
        normalized_metrics=request_parts.metrics,
        max_groups=request_parts.max_groups,
        truncated=truncated,
    )


__all__ = [
    "build_run_statistics_request",
    "coerce_execution_statistics_grouping",
    "coerce_execution_statistics_metric",
    "get_execution_statistics",
    "map_run_statistics_response",
    "normalize_execution_statistics_groupings",
    "normalize_execution_statistics_metrics",
    "normalize_execution_statistics_tags",
    "validate_statistics_max_groups",
]
