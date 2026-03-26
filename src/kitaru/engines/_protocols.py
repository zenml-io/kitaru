"""Engine backend protocol definitions.

These protocols define the contract that each backend engine must satisfy.
Phase 3 introduces only the narrow ``ExecutionEngineBackend`` protocol
for execution graph snapshot mapping; broader protocols for flow execution,
checkpoint dispatch, and client operations will be added in later phases.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from kitaru.engines._types import ExecutionGraphSnapshot


@runtime_checkable
class ExecutionEngineBackend(Protocol):
    """Backend that can convert native run objects into neutral snapshots.

    Each backend implements this to translate its native execution/run
    representation into the backend-neutral ``ExecutionGraphSnapshot``
    consumed by Kitaru's replay planner.
    """

    @property
    def name(self) -> str: ...

    def execution_graph_from_run(self, run: Any) -> ExecutionGraphSnapshot: ...
