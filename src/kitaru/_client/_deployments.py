"""Helpers for Kitaru deployment snapshots."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, unquote

from kitaru._client._models import Deployment
from kitaru.errors import KitaruUsageError

_SNAPSHOT_NAME_PATTERN = re.compile(r"^kitaru::(?P<flow>.+)::v(?P<version>[1-9]\d*)$")
_DEPLOYMENT_TAG_MARKER = "kitaru:deployment"
_DEPLOYMENT_NAMESPACE_PREFIX = f"{_DEPLOYMENT_TAG_MARKER}:"
_DEPLOYMENT_PUBLIC_TAG_PREFIX = f"{_DEPLOYMENT_NAMESPACE_PREFIX}tag:"
_DEPLOYMENT_PUBLIC_TAG_EXCLUSIVE_SUFFIX = ":exclusive"
_DEPLOYMENT_PUBLIC_TAG_SHARED_SUFFIX = ":shared"

DEFAULT_DEPLOYMENT_TAG = "default"


@dataclass(frozen=True)
class DeploymentSnapshotName:
    """Parsed Kitaru-owned snapshot name."""

    flow: str
    version: int


def validate_deployment_flow(flow: str) -> str:
    """Validate and normalize a deployment flow selector."""
    normalized = flow.strip()
    if not normalized:
        raise KitaruUsageError("`flow` must be a non-empty string.")
    if "::" in normalized:
        raise KitaruUsageError("Deployment flow names cannot contain `::`.")
    return normalized


def validate_deployment_tag(tag: str) -> str:
    """Validate and normalize a deployment tag."""
    normalized = tag.strip()
    if not normalized:
        raise KitaruUsageError("Deployment tags must be non-empty strings.")
    if normalized.startswith(_DEPLOYMENT_NAMESPACE_PREFIX):
        raise KitaruUsageError("Deployment tags cannot use the `kitaru:` namespace.")
    return normalized


def validate_deployment_version(version: int) -> int:
    """Validate a deployment version selector."""
    if isinstance(version, bool) or version < 1:
        raise KitaruUsageError("Deployment version must be >= 1.")
    return version


def resolve_deployment_exclusive(tag: str, exclusive: bool) -> bool:
    """Return the effective exclusivity, forcing ``default`` to always be exclusive."""
    return True if tag == DEFAULT_DEPLOYMENT_TAG else bool(exclusive)


def is_default_deployment_tag(tag: str) -> bool:
    """Return whether ``tag`` is the reserved default deployment tag."""
    return tag == DEFAULT_DEPLOYMENT_TAG


def build_deployment_snapshot_name(flow: str, version: int) -> str:
    """Build the Kitaru-owned ZenML snapshot name for a deployment version."""
    normalized_flow = validate_deployment_flow(flow)
    validate_deployment_version(version)
    return f"kitaru::{normalized_flow}::v{version}"


def parse_deployment_snapshot_name(name: str | None) -> DeploymentSnapshotName | None:
    """Parse a Kitaru-owned snapshot name, returning ``None`` for other names."""
    if not isinstance(name, str):
        return None
    match = _SNAPSHOT_NAME_PATTERN.match(name)
    if match is None:
        return None
    flow = match.group("flow")
    if "::" in flow:
        return None
    return DeploymentSnapshotName(flow=flow, version=int(match.group("version")))


def next_deployment_version(
    snapshots: list[Any],
    *,
    flow: str,
) -> int:
    """Return the next deployment version for ``flow`` from existing snapshots."""
    normalized_flow = validate_deployment_flow(flow)
    versions = [
        parsed.version
        for snapshot in snapshots
        if (parsed := parse_deployment_snapshot_name(getattr(snapshot, "name", None)))
        is not None
        and parsed.flow == normalized_flow
    ]
    if not versions:
        return 1
    return max(versions) + 1


def deployment_snapshot_marker_tag() -> str:
    """Return the native ZenML tag used to mark Kitaru deployment snapshots."""
    return _DEPLOYMENT_TAG_MARKER


def deployment_public_tag(tag: str, *, exclusive: bool) -> str:
    """Build the native ZenML tag that stores public deployment tag state."""
    normalized_tag = validate_deployment_tag(tag)
    encoded = quote(normalized_tag, safe="")
    suffix = (
        _DEPLOYMENT_PUBLIC_TAG_EXCLUSIVE_SUFFIX
        if exclusive
        else _DEPLOYMENT_PUBLIC_TAG_SHARED_SUFFIX
    )
    return f"{_DEPLOYMENT_PUBLIC_TAG_PREFIX}{encoded}{suffix}"


def _parse_deployment_public_tag(native_tag: str) -> tuple[str, bool] | None:
    """Parse a native ZenML deployment tag into public tag state."""
    if not native_tag.startswith(_DEPLOYMENT_PUBLIC_TAG_PREFIX):
        return None

    remainder = native_tag.removeprefix(_DEPLOYMENT_PUBLIC_TAG_PREFIX)
    exclusive = False
    if remainder.endswith(_DEPLOYMENT_PUBLIC_TAG_EXCLUSIVE_SUFFIX):
        encoded = remainder.removesuffix(_DEPLOYMENT_PUBLIC_TAG_EXCLUSIVE_SUFFIX)
        exclusive = True
    elif remainder.endswith(_DEPLOYMENT_PUBLIC_TAG_SHARED_SUFFIX):
        encoded = remainder.removesuffix(_DEPLOYMENT_PUBLIC_TAG_SHARED_SUFFIX)
    else:
        return None

    tag = unquote(encoded)
    if not tag:
        return None
    return tag, exclusive


def deployment_native_tags(tags: Mapping[str, bool] | None) -> list[str]:
    """Build native ZenML tags for a deployment snapshot."""
    native_tags = [_DEPLOYMENT_TAG_MARKER]
    for tag, exclusive in (tags or {}).items():
        normalized_tag = validate_deployment_tag(tag)
        native_tags.append(
            deployment_public_tag(
                normalized_tag,
                exclusive=resolve_deployment_exclusive(normalized_tag, exclusive),
            )
        )
    return native_tags


def _tag_names(snapshot: Any) -> set[str]:
    """Extract native tag names from a ZenML snapshot response-like object."""
    raw_tags = getattr(snapshot, "tags", None)
    resources = getattr(snapshot, "resources", None)
    if raw_tags is None and resources is not None:
        raw_tags = getattr(resources, "tags", None)

    tag_names: set[str] = set()
    for raw_tag in raw_tags or []:
        if isinstance(raw_tag, str):
            tag_name = raw_tag.strip()
        else:
            tag_name = str(getattr(raw_tag, "name", "")).strip()
        if tag_name:
            tag_names.add(tag_name)
    return tag_names


def deployment_tags_from_snapshot(snapshot: Any) -> dict[str, bool]:
    """Extract public deployment tags from a ZenML snapshot response."""
    public_tags: dict[str, bool] = {}
    for native_tag in _tag_names(snapshot):
        parsed = _parse_deployment_public_tag(native_tag)
        if parsed is None:
            continue
        tag, exclusive = parsed
        public_tags[tag] = exclusive
    return public_tags


def _mapping_from_model(value: Any) -> dict[str, Any]:
    """Return a best-effort dict from a pydantic model, mapping, or object."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    return {}


def _deployment_extra(snapshot: Any) -> dict[str, Any]:
    """Extract Kitaru-specific deployment metadata from snapshot-like objects."""
    direct = getattr(snapshot, "kitaru_deployment", None)
    if isinstance(direct, Mapping):
        return dict(direct)

    metadata = getattr(snapshot, "metadata", None)
    metadata_mapping = _mapping_from_model(metadata)

    for key in ("kitaru_deployment", "kitaru_deployment_metadata"):
        raw = metadata_mapping.get(key)
        if isinstance(raw, Mapping):
            return dict(raw)

    run_metadata = getattr(snapshot, "run_metadata", None)
    if isinstance(run_metadata, Mapping):
        raw = run_metadata.get("kitaru_deployment")
        if isinstance(raw, Mapping):
            return dict(raw)

    client_environment = metadata_mapping.get("client_environment")
    if isinstance(client_environment, Mapping):
        raw = client_environment.get("KITARU_DEPLOYMENT_METADATA")
        if isinstance(raw, Mapping):
            return dict(raw)

    return {}


def _optional_string(value: Any) -> str | None:
    """Return a stripped string or ``None``."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _optional_bool(value: Any) -> bool | None:
    """Return a bool only for unambiguous bool-like values."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _created_at(snapshot: Any) -> datetime | None:
    """Extract a creation timestamp from a snapshot response."""
    direct = getattr(snapshot, "created", None)
    if isinstance(direct, datetime):
        return direct
    body = getattr(snapshot, "body", None)
    body_created = getattr(body, "created", None)
    if isinstance(body_created, datetime):
        return body_created
    return None


def _schema(snapshot: Any, extra: Mapping[str, Any]) -> dict[str, Any] | None:
    """Extract the deployment invocation schema."""
    raw_schema = extra.get("schema")
    if isinstance(raw_schema, Mapping):
        return dict(raw_schema)

    metadata = _mapping_from_model(getattr(snapshot, "metadata", None))
    raw_schema = metadata.get("config_schema")
    if isinstance(raw_schema, Mapping):
        return dict(raw_schema)
    return None


def _stack(snapshot: Any, extra: Mapping[str, Any]) -> str | None:
    """Extract stack name/id from deployment metadata or snapshot resources."""
    raw_stack = _optional_string(extra.get("stack"))
    if raw_stack is not None:
        return raw_stack

    resources = getattr(snapshot, "resources", None)
    stack = getattr(resources, "stack", None) if resources is not None else None
    if stack is None:
        stack = getattr(snapshot, "stack", None)
    return _optional_string(getattr(stack, "name", None) or getattr(stack, "id", None))


def map_deployment_snapshot(snapshot: Any) -> Deployment | None:
    """Map a ZenML snapshot response to a Kitaru deployment, if owned by Kitaru."""
    parsed_name = parse_deployment_snapshot_name(getattr(snapshot, "name", None))
    if parsed_name is None:
        return None

    extra = _deployment_extra(snapshot)
    return Deployment(
        deployment_id=str(snapshot.id),
        flow=parsed_name.flow,
        version=parsed_name.version,
        tags=deployment_tags_from_snapshot(snapshot),
        commit_sha=_optional_string(extra.get("commit_sha")),
        commit_dirty=_optional_bool(extra.get("commit_dirty")),
        image_digest=_optional_string(extra.get("image_digest")),
        created_at=_created_at(snapshot),
        schema=_schema(snapshot, extra),
        stack=_stack(snapshot, extra),
    )


__all__ = [
    "DEFAULT_DEPLOYMENT_TAG",
    "DeploymentSnapshotName",
    "build_deployment_snapshot_name",
    "deployment_native_tags",
    "deployment_public_tag",
    "deployment_snapshot_marker_tag",
    "deployment_tags_from_snapshot",
    "is_default_deployment_tag",
    "map_deployment_snapshot",
    "next_deployment_version",
    "parse_deployment_snapshot_name",
    "resolve_deployment_exclusive",
    "validate_deployment_flow",
    "validate_deployment_tag",
    "validate_deployment_version",
]
