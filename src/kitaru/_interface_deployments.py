"""Shared SDK helpers for Kitaru deployment interfaces."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import ValidationError
from zenml.pipelines.run_utils import validate_stack_is_runnable_from_server

from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    is_default_deployment_tag,
    resolve_deployment_exclusive,
    validate_deployment_flow,
    validate_deployment_tag,
    validate_deployment_version,
)
from kitaru._client._models import Deployment as DeploymentRecord
from kitaru.config import ImageInput, KitaruConfig
from kitaru.errors import (
    KitaruError,
    KitaruStackNotRemoteExecutableStateError,
    KitaruStackNotRemoteExecutableUsageError,
    KitaruUsageError,
)

if TYPE_CHECKING:
    from kitaru.client import KitaruClient
    from kitaru.flow import FlowHandle

DEPLOYMENT_CONTROL_INPUT_KEYS = frozenset(
    {"cache", "image", "publish_default_on_first_deploy", "retries", "stack", "tags"}
)

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


DeploymentSelectorSource = Literal["version", "tag", "implicit_default"]


@dataclass(frozen=True)
class DeploymentSelector:
    """Normalized deployment selector plus how it was chosen."""

    version: int | None
    tag: str | None
    source: DeploymentSelectorSource | None


def resolve_deployment_selector(
    *,
    version: int | None = None,
    tag: str | None = None,
    default_tag: str | None = None,
    require_one: bool = False,
) -> DeploymentSelector:
    """Validate selectors and preserve whether default routing was implicit."""
    if version is not None and tag is not None:
        raise KitaruUsageError("`version` and `tag` are mutually exclusive.")

    normalized_version = (
        validate_deployment_version(version) if version is not None else None
    )
    normalized_tag = validate_deployment_tag(tag) if tag is not None else None
    source: DeploymentSelectorSource | None = None

    if normalized_version is not None:
        source = "version"
    elif normalized_tag is not None:
        source = "tag"
    elif default_tag is not None:
        normalized_tag = validate_deployment_tag(default_tag)
        source = "implicit_default"

    if require_one and normalized_version is None and normalized_tag is None:
        raise KitaruUsageError("Exactly one of `version` or `tag` is required.")

    return DeploymentSelector(
        version=normalized_version,
        tag=normalized_tag,
        source=source,
    )


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
    selector = resolve_deployment_selector(
        version=version,
        tag=tag,
        default_tag=default_tag,
        require_one=require_one,
    )
    return selector.version, selector.tag


def deployment_tags_for_create(
    *,
    is_first_deploy: bool,
    tags: Mapping[str, bool] | None = None,
    publish_default_on_first_deploy: bool = True,
) -> dict[str, bool]:
    """Return normalized deployment tags, adding first-deploy default if needed."""
    normalized: dict[str, bool] = {}
    for tag, exclusive in (tags or {}).items():
        normalized_tag = validate_deployment_tag(tag)
        normalized[normalized_tag] = resolve_deployment_exclusive(
            normalized_tag,
            exclusive,
        )

    if is_first_deploy and publish_default_on_first_deploy:
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


def validate_deployment_input_keys(
    inputs: Mapping[str, Any],
    *,
    input_label: str = "`inputs`",
) -> None:
    """Reject flow input keys that collide with deployment-control kwargs."""
    collisions = DEPLOYMENT_CONTROL_INPUT_KEYS & set(inputs)
    if collisions:
        raise ValueError(
            f"{input_label} contains reserved deployment option key(s): "
            + ", ".join(sorted(collisions))
        )


def _deployment_image_validation_detail(exc: ValidationError) -> str | None:
    """Return a concise first validation detail for image-shape errors."""
    errors = exc.errors(include_url=False)
    if not errors:
        detail = str(exc).strip()
        return detail or None

    first_error = errors[0]
    ctx = first_error.get("ctx") or {}
    ctx_error = ctx.get("error")
    if isinstance(ctx_error, Exception):
        detail = str(ctx_error).strip()
    else:
        detail = str(first_error.get("msg") or "").strip()
        if detail.lower().startswith("value error, "):
            detail = detail[len("Value error, ") :]

    location = ".".join(
        str(part) for part in first_error.get("loc", ()) if str(part) != "image"
    )
    if location and detail:
        return f"{location}: {detail}"
    return detail or None


def _normalize_deployment_image(
    image: ImageInput | None,
    *,
    image_label: str,
) -> Any | None:
    """Normalize a deploy-time image override through shared config validation."""
    if image is None:
        return None

    try:
        return KitaruConfig.model_validate({"image": image}).image
    except ValidationError as exc:
        detail = _deployment_image_validation_detail(exc)
    except (KitaruUsageError, TypeError) as exc:
        detail = str(exc).strip() or None

    message = (
        f"{image_label} must be either a base image string or an image settings object."
    )
    if detail:
        message += f" Details: {detail}"
    raise KitaruUsageError(message)


def _deployment_operation_subject(
    *,
    operation: Literal["deploy", "invoke", "curl"],
    flow: str,
    version: int | None = None,
) -> str:
    """Return a human-readable subject for deployment validation errors."""
    normalized_flow = validate_deployment_flow(flow)
    if operation == "deploy":
        return f"Flow {normalized_flow!r}"
    if version is None:
        raise KitaruUsageError(
            f"`version` is required when validating deployment {operation}."
        )
    return f"Deployment {normalized_flow!r} v{validate_deployment_version(version)}"


def _stack_label(stack: Any) -> str:
    """Return the best available stack label for user-facing errors."""
    name = getattr(stack, "name", None)
    if isinstance(name, str) and name:
        return name
    stack_id = getattr(stack, "id", None)
    if stack_id is not None:
        return str(stack_id)
    return "<unknown>"


def _stack_validation_detail(exc: ValueError) -> str | None:
    """Return safe stack-validation details, filtering backend-internal phrasing."""
    detail = str(exc).strip()
    if not detail:
        return None
    if "unable to run pipeline from the server" in detail.lower():
        return None
    if not detail.endswith((".", "!", "?")):
        detail += "."
    return detail


_REMOTE_EXECUTABLE_HINT = (
    "Choose a stack the Kitaru server can execute remotely (for example "
    "Kubernetes, Vertex, SageMaker, or AzureML)"
)

_DEPLOY_MESSAGE = (
    "{subject} cannot be deployed with stack {stack_name!r} because that "
    "stack is not one the Kitaru server can execute remotely. "
    + _REMOTE_EXECUTABLE_HINT
    + " and try again."
)
_INVOKE_MESSAGE = (
    "{subject} was created from stack {stack_name!r}, which the Kitaru "
    "server cannot execute remotely. Rebuild the deployment using a stack "
    "the Kitaru server can execute remotely before invoking it."
)
_CURL_MESSAGE = (
    "{subject} was created from stack {stack_name!r}, which the Kitaru "
    "server cannot execute remotely. Rebuild the deployment using a stack "
    "the Kitaru server can execute remotely before generating a curl "
    "command for it."
)

_OPERATION_ERRORS: dict[
    Literal["deploy", "invoke", "curl"],
    tuple[type[KitaruError], str],
] = {
    "deploy": (KitaruStackNotRemoteExecutableUsageError, _DEPLOY_MESSAGE),
    "invoke": (KitaruStackNotRemoteExecutableStateError, _INVOKE_MESSAGE),
    "curl": (KitaruStackNotRemoteExecutableStateError, _CURL_MESSAGE),
}


def ensure_stack_is_server_runnable(
    *,
    zen_store: Any,
    stack: Any,
    operation: Literal["deploy", "invoke", "curl"],
    flow: str,
    version: int | None = None,
) -> None:
    """Validate that a stack can be executed by the server-backed runtime."""
    try:
        validate_stack_is_runnable_from_server(zen_store=zen_store, stack=stack)
    except ValueError as exc:
        exc_cls, template = _OPERATION_ERRORS[operation]
        subject = _deployment_operation_subject(
            operation=operation,
            flow=flow,
            version=version,
        )
        detail = _stack_validation_detail(exc)
        detail_suffix = f" Details: {detail}" if detail else ""
        message = template.format(
            subject=subject,
            stack_name=_stack_label(stack),
        )
        raise exc_cls(f"{message}{detail_suffix}") from exc


def build_deployment_deploy_kwargs(
    *,
    stack: str | None,
    image: ImageInput | None = None,
    cache: bool | None,
    retries: int | None,
    tags: Mapping[str, bool],
    inputs: Mapping[str, Any],
    input_label: str = "`inputs`",
    image_label: str = "`image`",
) -> dict[str, Any]:
    """Build keyword arguments for ``flow.deploy(...)`` without noisy Nones."""
    validate_deployment_input_keys(inputs, input_label=input_label)
    deploy_kwargs: dict[str, Any] = {**dict(inputs), "tags": dict(tags)}
    normalized_image = _normalize_deployment_image(image, image_label=image_label)
    if stack is not None:
        deploy_kwargs["stack"] = stack
    if normalized_image is not None:
        deploy_kwargs["image"] = normalized_image
    if cache is not None:
        deploy_kwargs["cache"] = cache
    if retries is not None:
        deploy_kwargs["retries"] = retries
    return deploy_kwargs


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
    "DEPLOYMENT_CONTROL_INPUT_KEYS",
    "Deployment",
    "DeploymentSelector",
    "DeploymentSelectorSource",
    "build_deployment_deploy_kwargs",
    "deployment_tags_for_create",
    "ensure_stack_is_server_runnable",
    "is_deployment_known",
    "mark_deployment_known",
    "resolve_deployment_selector",
    "validate_deployment_input_keys",
    "validate_deployment_selector",
    "validate_remove_deployment_tag",
    "warn_if_deployment_drifted",
]
