"""Shared SDK helpers for Kitaru deployment interfaces."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    is_default_deployment_tag,
    resolve_deployment_exclusive,
    validate_deployment_flow,
    validate_deployment_tag,
    validate_deployment_version,
)
from kitaru._client._models import Deployment as DeploymentRecord
from kitaru.errors import KitaruUsageError

if TYPE_CHECKING:
    from kitaru.client import KitaruClient
    from kitaru.flow import FlowHandle

_KNOWN_DEPLOYMENTS: set[tuple[str, int]] = set()


def deployment_key(flow: str, version: int) -> tuple[str, int]:
    """Return a normalized in-process deployment identity key."""
    return validate_deployment_flow(flow), validate_deployment_version(version)


def mark_deployment_known(deployment: DeploymentRecord) -> None:
    """Remember that this process has observed a deployment version."""
    _KNOWN_DEPLOYMENTS.add(deployment_key(deployment.flow, deployment.version))


def is_deployment_known(flow: str, version: int) -> bool:
    """Return whether this process has observed a deployment version."""
    return deployment_key(flow, version) in _KNOWN_DEPLOYMENTS


def validate_deployment_selector(
    *,
    version: int | None = None,
    tag: str | None = None,
    default_tag: str | None = None,
    require_one: bool = False,
) -> tuple[int | None, str | None]:
    """Validate mutually exclusive deployment selectors.

    When both selectors are None, ``default_tag`` (if given) is used as the
    resolved tag. ``require_one`` raises if neither selector resolves.
    """
    if version is not None and tag is not None:
        raise KitaruUsageError("`version` and `tag` are mutually exclusive.")
    normalized_version = (
        validate_deployment_version(version) if version is not None else None
    )
    normalized_tag = validate_deployment_tag(tag) if tag is not None else None
    if (
        normalized_version is None
        and normalized_tag is None
        and default_tag is not None
    ):
        normalized_tag = validate_deployment_tag(default_tag)
    if require_one and normalized_version is None and normalized_tag is None:
        raise KitaruUsageError("Exactly one of `version` or `tag` is required.")
    return normalized_version, normalized_tag


def deployment_tags_for_create(
    *,
    is_first_deploy: bool,
    tags: Mapping[str, bool] | None = None,
) -> dict[str, bool]:
    """Return normalized deployment tags, adding first-deploy default if needed."""
    normalized: dict[str, bool] = {}
    for tag, exclusive in (tags or {}).items():
        normalized_tag = validate_deployment_tag(tag)
        normalized[normalized_tag] = resolve_deployment_exclusive(
            normalized_tag,
            exclusive,
        )

    if is_first_deploy:
        normalized[DEFAULT_DEPLOYMENT_TAG] = True
    return normalized


def validate_remove_deployment_tag(tag: str) -> str:
    """Validate a tag removal request."""
    normalized_tag = validate_deployment_tag(tag)
    if is_default_deployment_tag(normalized_tag):
        raise KitaruUsageError(
            "The reserved `default` deployment tag cannot be removed."
        )
    return normalized_tag


def warn_if_deployment_drifted(
    deployment: DeploymentRecord,
    *,
    known_before_resolution: bool,
) -> None:
    """Warn when invoking a deployment version first observed during invocation."""
    if known_before_resolution:
        return
    warnings.warn(
        "Invoking deployment "
        f"{deployment.flow!r} v{deployment.version}, which was not previously "
        "known in this Python process. If another process moved deployment tags "
        "or replaced versions, confirm this is the version you intended.",
        UserWarning,
        stacklevel=3,
    )


@dataclass(frozen=True)
class Deployment:
    """SDK-facing deployment facade with convenience operations."""

    _record: DeploymentRecord = field(repr=False)
    _client: KitaruClient = field(repr=False, compare=False)

    @property
    def deployment_id(self) -> str:
        """Backend snapshot/deployment identifier."""
        return self._record.deployment_id

    @property
    def flow(self) -> str:
        """Flow name."""
        return self._record.flow

    @property
    def version(self) -> int:
        """Deployment version."""
        return self._record.version

    @property
    def tags(self) -> dict[str, bool]:
        """Public deployment tags mapped to exclusivity flags."""
        return dict(self._record.tags)

    @property
    def commit_sha(self) -> str | None:
        """Best-effort source commit SHA."""
        return self._record.commit_sha

    @property
    def commit_dirty(self) -> bool | None:
        """Best-effort source dirty flag."""
        return self._record.commit_dirty

    @property
    def image_digest(self) -> str | None:
        """Best-effort image digest."""
        return self._record.image_digest

    @property
    def created_at(self) -> datetime | None:
        """Creation timestamp when provided by the backend."""
        return self._record.created_at

    @property
    def schema(self) -> dict[str, Any] | None:
        """Best-effort deployment input schema."""
        return dict(self._record.schema) if self._record.schema else None

    @property
    def stack(self) -> str | None:
        """Stack name or ID associated with the deployment snapshot."""
        return self._record.stack

    def invoke(self, **flow_inputs: Any) -> FlowHandle:
        """Invoke this pinned deployment version."""
        return self._client.deployments.invoke(
            flow=self.flow,
            version=self.version,
            inputs=flow_inputs,
        )

    def add_tag(self, tag: str, *, exclusive: bool = False) -> Deployment:
        """Attach a public tag to this deployment version."""
        return self._client.deployments.tag(
            flow=self.flow,
            version=self.version,
            tag=tag,
            exclusive=exclusive,
        )

    def remove_tag(self, tag: str) -> Deployment:
        """Remove a public tag from this deployment version."""
        validate_remove_deployment_tag(tag)
        return self._client.deployments.untag(
            flow=self.flow,
            version=self.version,
            tag=tag,
        )

    def delete(self) -> None:
        """Delete this deployment version if no exclusive tag protects it."""
        self._client.deployments.delete(flow=self.flow, version=self.version)


__all__ = [
    "Deployment",
    "deployment_tags_for_create",
    "is_deployment_known",
    "mark_deployment_known",
    "validate_deployment_selector",
    "validate_remove_deployment_tag",
    "warn_if_deployment_drifted",
]
