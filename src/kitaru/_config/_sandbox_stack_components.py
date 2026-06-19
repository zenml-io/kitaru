"""Sandbox stack-component construction helpers."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from functools import cache
from typing import Any, Literal
from uuid import UUID

from zenml.enums import StackComponentType
from zenml.models.v2.misc.info_models import ComponentInfo

# Keep this literal Kitaru-owned so importing stack helpers does not import
# ZenML sandbox modules at process startup.
LOCAL_SANDBOX_FLAVOR = "local"
SANDBOX_COMPONENT_KIND: Literal["sandbox"] = "sandbox"
SANDBOX_RECURSIVE_DELETE_ENTRY: tuple[StackComponentType, Literal["sandbox"]] = (
    StackComponentType.SANDBOX,
    SANDBOX_COMPONENT_KIND,
)


@dataclass(frozen=True)
class SandboxValidationMetadata:
    """Validation metadata for a ZenML sandbox stack-component flavor."""

    component_type: StackComponentType
    config_class: type[Any]
    docs_url: str | None


@cache
def local_sandbox_validation_metadata(
    *,
    flavor: str,
) -> SandboxValidationMetadata | None:
    """Return metadata for ZenML's built-in local sandbox flavor, if requested."""
    if flavor != LOCAL_SANDBOX_FLAVOR:
        return None

    from zenml.sandboxes.local_sandbox import LocalSandboxFlavor

    sandbox_flavor = LocalSandboxFlavor()
    return SandboxValidationMetadata(
        component_type=sandbox_flavor.type,
        config_class=sandbox_flavor.config_class,
        docs_url=sandbox_flavor.docs_url,
    )


def add_remote_sandbox_component_info(
    components: MutableMapping[StackComponentType, list[UUID | ComponentInfo]],
    *,
    sandbox_flavor: str | None,
    configuration: Mapping[str, Any],
) -> None:
    """Attach one sandbox component to a one-shot ZenML stack request."""
    if sandbox_flavor is None:
        return

    components[StackComponentType.SANDBOX] = [
        ComponentInfo(
            flavor=sandbox_flavor,
            configuration=dict(configuration),
        )
    ]


def create_local_sandbox_component(
    client: Any,
    *,
    name: str,
    flavor: str,
    configuration: Mapping[str, Any],
) -> Any:
    """Create the sandbox stack component for a local Kitaru stack."""
    return client.create_stack_component(
        name=name,
        flavor=flavor,
        component_type=StackComponentType.SANDBOX,
        configuration=dict(configuration),
    )
