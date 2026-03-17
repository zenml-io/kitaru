"""Frozen execution-spec exports."""

from kitaru._config._core import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    FrozenExecutionSpec,
    _parse_run_uuid,
    build_frozen_execution_spec,
    persist_frozen_execution_spec_impl,
)
from kitaru._config._models import ModelRegistryConfig

FrozenExecutionSpec.model_rebuild(
    _types_namespace={"ModelRegistryConfig": ModelRegistryConfig}
)

__all__ = [
    "FROZEN_EXECUTION_SPEC_METADATA_KEY",
    "FrozenExecutionSpec",
    "_parse_run_uuid",
    "build_frozen_execution_spec",
    "persist_frozen_execution_spec_impl",
]
