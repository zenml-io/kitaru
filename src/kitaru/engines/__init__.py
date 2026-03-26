"""Pluggable engine infrastructure for Kitaru backends.

All imports here are from internal, import-light modules. No backend
implementation module (e.g. ``kitaru.engines.zenml``) is imported at
package level — backends are loaded lazily by the registry on demand.
"""

from kitaru.engines._protocols import ExecutionEngineBackend
from kitaru.engines._registry import (
    available_engine_names,
    get_engine_backend,
    resolve_engine_name,
)
from kitaru.engines._types import (
    CheckpointGraphNode,
    CheckpointInputBinding,
    ExecutionGraphSnapshot,
)

__all__ = [
    "CheckpointGraphNode",
    "CheckpointInputBinding",
    "ExecutionEngineBackend",
    "ExecutionGraphSnapshot",
    "available_engine_names",
    "get_engine_backend",
    "resolve_engine_name",
]
