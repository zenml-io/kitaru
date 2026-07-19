"""Resolve Langfuse JSONL exports and trace URIs into one record stream."""

import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable
from urllib.parse import unquote, urlsplit

from kitaru.imports._langfuse import (
    LangfuseImportError,
    LangfuseSourceRecord,
    read_langfuse_jsonl_records,
)
from kitaru.imports._replay_evidence import canonical_json

_MINIMUM_LANGFUSE_VERSION = (4, 7, 0)
_SUPPORTED_LANGFUSE_MAJOR = 4
_DEFAULT_LANGFUSE_BASE_URL = "https://cloud.langfuse.com"
_OBSERVATION_FIELDS = (
    "core",
    "basic",
    "time",
    "io",
    "metadata",
    "model",
    "usage",
    "prompt",
    "metrics",
    "trace_context",
)
_OBSERVATION_FIELDS_PARAMETER = ",".join(_OBSERVATION_FIELDS)
_OBSERVATION_PAGE_LIMIT = 100
_MAX_OBSERVATION_PAGES = 100
_MAX_TRACE_OBSERVATIONS = 10_000
_MAX_OBSERVATION_CANONICAL_BYTES = 1 * 1024 * 1024
_MAX_TRACE_CANONICAL_BYTES = 64 * 1024 * 1024
_TRACE_URI_HELP = "Expected 'langfuse://trace/<non-empty-id>'."


class LangfuseSourceKind(StrEnum):
    """Supported inputs to the Langfuse importer."""

    JSONL = "jsonl"
    TRACE_URI = "trace_uri"


@dataclass(frozen=True)
class LangfuseFetchProvenance:
    """Non-secret facts describing a Langfuse observations query."""

    api_resource: str
    base_url: str
    field_groups: tuple[str, ...]
    page_count: int
    base_url_source: str = "default"


@dataclass(frozen=True)
class ResolvedLangfuseSource:
    """A deterministic source record stream ready for normalization."""

    kind: LangfuseSourceKind
    authoritative_project_id: str
    selected_trace_id: str | None
    records: Iterable[LangfuseSourceRecord]
    fetch_provenance: LangfuseFetchProvenance | None = None


@runtime_checkable
class _SerializableObservation(Protocol):
    def model_dump(
        self,
        *,
        mode: str,
        by_alias: bool,
        exclude_none: bool,
    ) -> dict[str, Any]: ...


def resolve_langfuse_source(
    source: str | Path,
    *,
    source_project_id: str | None = None,
    trace_ids: Sequence[str] | None = None,
) -> ResolvedLangfuseSource:
    """Resolve a JSONL path or one Langfuse trace URI without writing upstream."""

    kind, trace_id = parse_langfuse_source(source)
    if kind is LangfuseSourceKind.JSONL:
        project_id = _required_project_id(source_project_id)
        return ResolvedLangfuseSource(
            kind=LangfuseSourceKind.JSONL,
            authoritative_project_id=project_id,
            selected_trace_id=None,
            records=read_langfuse_jsonl_records(source),
        )

    assert trace_id is not None
    requested_trace_ids = tuple(trace_ids) if trace_ids is not None else None
    if requested_trace_ids is not None and requested_trace_ids != (trace_id,):
        raise LangfuseImportError(
            f"The source URI selects trace {trace_id!r}; trace_ids must be omitted "
            "or contain that exact trace ID once."
        )

    records, project_id, provenance = _fetch_langfuse_trace(trace_id)
    if source_project_id is not None:
        declared_project_id = _nonempty(source_project_id, "source_project_id")
        if declared_project_id != project_id:
            raise LangfuseImportError(
                "source_project_id does not match the project ID returned by Langfuse."
            )
    return ResolvedLangfuseSource(
        kind=LangfuseSourceKind.TRACE_URI,
        authoritative_project_id=project_id,
        selected_trace_id=trace_id,
        records=records,
        fetch_provenance=provenance,
    )


def parse_langfuse_source(
    source: str | Path,
) -> tuple[LangfuseSourceKind, str | None]:
    """Classify and validate one Langfuse import source."""
    if isinstance(source, Path):
        return LangfuseSourceKind.JSONL, None

    parsed = urlsplit(source)
    if not parsed.scheme or "://" not in source:
        return LangfuseSourceKind.JSONL, None
    if parsed.scheme != "langfuse":
        raise LangfuseImportError(
            f"Unsupported Langfuse import source scheme {parsed.scheme!r}. "
            + _TRACE_URI_HELP
        )
    if (
        parsed.netloc != "trace"
        or not parsed.path.startswith("/")
        or parsed.query
        or parsed.fragment
    ):
        raise LangfuseImportError(f"Invalid Langfuse trace URI. {_TRACE_URI_HELP}")

    path_parts = parsed.path.removeprefix("/").split("/")
    if len(path_parts) != 1:
        raise LangfuseImportError(f"Invalid Langfuse trace URI. {_TRACE_URI_HELP}")
    trace_id = unquote(path_parts[0])
    if not trace_id or trace_id != trace_id.strip() or "/" in trace_id:
        raise LangfuseImportError(f"Invalid Langfuse trace URI. {_TRACE_URI_HELP}")
    return LangfuseSourceKind.TRACE_URI, trace_id


def _fetch_langfuse_trace(
    trace_id: str,
) -> tuple[
    tuple[LangfuseSourceRecord, ...],
    str,
    LangfuseFetchProvenance,
]:
    public_key = _required_environment_value("LANGFUSE_PUBLIC_KEY")
    secret_key = _required_environment_value("LANGFUSE_SECRET_KEY")
    base_url, base_url_source = _langfuse_base_url()
    client_type = _load_langfuse_client_type()
    client = client_type(
        public_key=public_key,
        secret_key=secret_key,
        base_url=base_url,
        tracing_enabled=False,
    )

    retained_rows: list[tuple[dict[str, Any], str]] = []
    canonical_bytes = 0
    observation_ids: set[str] = set()
    project_ids: set[str] = set()
    returned_trace_ids: set[str] = set()
    cursor: str | None = None
    seen_cursors: set[str] = set()
    page_count = 0
    while True:
        if page_count >= _MAX_OBSERVATION_PAGES:
            raise LangfuseImportError(
                "Langfuse observations pagination exceeded the supported page limit."
            )
        try:
            response = client.api.observations.get_many(
                trace_id=trace_id,
                fields=_OBSERVATION_FIELDS_PARAMETER,
                limit=_OBSERVATION_PAGE_LIMIT,
                cursor=cursor,
                parse_io_as_json=False,
            )
        except Exception as exc:
            raise LangfuseImportError(
                f"Langfuse observations request failed ({type(exc).__name__})."
            ) from exc
        page_count += 1
        try:
            page_data = response.data
            next_cursor = response.meta.cursor
        except AttributeError as exc:
            raise LangfuseImportError(
                "The Langfuse SDK returned an unsupported observations v2 response."
            ) from exc
        if not isinstance(page_data, Sequence) or isinstance(page_data, str):
            raise LangfuseImportError(
                "The Langfuse SDK returned an unsupported observations v2 response."
            )
        if len(retained_rows) + len(page_data) > _MAX_TRACE_OBSERVATIONS:
            raise LangfuseImportError(
                "Langfuse trace exceeds the supported observation limit."
            )
        for observation in page_data:
            row = _observation_row(observation)
            observation_id = _required_row_string(row, "id")
            if observation_id in observation_ids:
                raise LangfuseImportError(
                    "Langfuse returned duplicate observation IDs across fetched pages."
                )
            project_id = _required_row_string(row, "projectId")
            returned_trace_id = _required_row_string(row, "traceId")
            raw_text = _canonical_row(row)
            row_bytes = _bounded_utf8_size(
                raw_text,
                stop_after=_MAX_OBSERVATION_CANONICAL_BYTES,
            )
            if row_bytes > _MAX_OBSERVATION_CANONICAL_BYTES:
                raise LangfuseImportError(
                    "Langfuse observation exceeds the supported "
                    "per-observation canonical-byte limit."
                )
            if canonical_bytes + row_bytes > _MAX_TRACE_CANONICAL_BYTES:
                raise LangfuseImportError(
                    "Langfuse trace exceeds the supported cumulative "
                    "canonical-byte limit."
                )
            observation_ids.add(observation_id)
            project_ids.add(project_id)
            returned_trace_ids.add(returned_trace_id)
            retained_rows.append((row, raw_text))
            canonical_bytes += row_bytes

        if next_cursor is None:
            break
        if not isinstance(next_cursor, str) or not next_cursor:
            raise LangfuseImportError(
                "The Langfuse SDK returned an invalid observations cursor."
            )
        if next_cursor in seen_cursors:
            raise LangfuseImportError(
                "Langfuse observations pagination returned a repeated cursor."
            )
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    if not retained_rows:
        raise LangfuseImportError(f"Langfuse trace {trace_id!r} has no observations.")

    if returned_trace_ids != {trace_id}:
        raise LangfuseImportError(
            "Langfuse returned observations from a different or mixed trace."
        )
    if len(project_ids) != 1:
        raise LangfuseImportError(
            "Langfuse returned observations with mixed project IDs."
        )

    retained_rows.sort(
        key=lambda item: (
            _required_row_string(item[0], "startTime"),
            _required_row_string(item[0], "id"),
        )
    )
    records = tuple(
        LangfuseSourceRecord(
            raw_text=raw_text,
            row=row,
            line_number=index,
            source_order=index - 1,
        )
        for index, (row, raw_text) in enumerate(retained_rows, start=1)
    )
    return (
        records,
        project_ids.pop(),
        LangfuseFetchProvenance(
            api_resource="observations_v2",
            base_url=base_url,
            field_groups=_OBSERVATION_FIELDS,
            page_count=page_count,
            base_url_source=base_url_source,
        ),
    )


def _observation_row(observation: Any) -> dict[str, Any]:
    if isinstance(observation, Mapping):
        row = dict(observation)
    elif isinstance(observation, _SerializableObservation):
        row = observation.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    else:
        raise LangfuseImportError(
            "Langfuse returned an unsupported observation response object."
        )
    if not isinstance(row, dict):
        raise LangfuseImportError(
            "Langfuse returned an unsupported observation response object."
        )
    return cast(dict[str, Any], row)


def _bounded_utf8_size(value: str, *, stop_after: int) -> int:
    """Count UTF-8 bytes in bounded chunks and stop once a limit is exceeded."""
    chunk_size = 64 * 1024
    total = 0
    for start in range(0, len(value), chunk_size):
        total += len(value[start : start + chunk_size].encode("utf-8"))
        if total > stop_after:
            return total
    return total


def _canonical_row(row: Mapping[str, Any]) -> str:
    try:
        return canonical_json(row) + "\n"
    except (RecursionError, TypeError, ValueError) as exc:
        raise LangfuseImportError(
            "Langfuse returned an observation that cannot be serialized as JSON."
        ) from exc


def _required_row_string(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise LangfuseImportError(
            f"Langfuse observation is missing a non-empty {key} field."
        )
    return value.strip()


def _required_project_id(value: str | None) -> str:
    if value is None:
        raise LangfuseImportError(
            "source_project_id is required for Langfuse JSONL sources."
        )
    return _nonempty(value, "source_project_id")


def _nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LangfuseImportError(f"{name} must be a non-empty string.")
    return value.strip()


def _required_environment_value(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise LangfuseImportError(f"{name} is required to fetch a langfuse:// trace.")
    return value.strip()


def _langfuse_base_url() -> tuple[str, str]:
    base_url = os.environ.get("LANGFUSE_BASE_URL", "").strip().rstrip("/")
    host = os.environ.get("LANGFUSE_HOST", "").strip().rstrip("/")
    if base_url and host and base_url != host:
        raise LangfuseImportError(
            "LANGFUSE_BASE_URL and LANGFUSE_HOST are both set to different "
            "values. Keep one or make them match."
        )
    if base_url:
        selected = base_url
        selected_from = "LANGFUSE_BASE_URL"
    elif host:
        selected = host
        selected_from = "LANGFUSE_HOST"
    else:
        selected = _DEFAULT_LANGFUSE_BASE_URL
        selected_from = "default"
    parsed = urlsplit(selected)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise LangfuseImportError(
            "The Langfuse base URL must be an HTTP(S) URL without credentials, "
            "a query, or a fragment."
        )
    return selected, selected_from


def _load_langfuse_client_type() -> type[Any]:
    try:
        module = import_module("langfuse")
        installed_version = version("langfuse")
    except (ModuleNotFoundError, PackageNotFoundError) as exc:
        raise LangfuseImportError(
            "Fetching langfuse:// traces requires the optional Langfuse SDK. "
            "Install it with \"uv add 'kitaru[langfuse]'\" or "
            "\"pip install 'kitaru[langfuse]'\"."
        ) from exc

    parsed_version = _release_tuple(installed_version)
    if (
        parsed_version is None
        or parsed_version < _MINIMUM_LANGFUSE_VERSION
        or parsed_version[0] != _SUPPORTED_LANGFUSE_MAJOR
    ):
        raise LangfuseImportError(
            "Fetching langfuse:// traces requires langfuse>=4.7.0,<5; "
            f"found {installed_version!r}."
        )
    try:
        return cast(type[Any], module.Langfuse)
    except AttributeError as exc:
        raise LangfuseImportError(
            "The installed Langfuse SDK does not expose the supported v4 client."
        ) from exc


def _release_tuple(value: str) -> tuple[int, int, int] | None:
    match = re.fullmatch(
        r"(\d+)\.(\d+)\.(\d+)"
        r"(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?"
        r"(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?",
        value,
    )
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return (major, minor, patch)
