"""Engine backend protocol definitions.

These protocols define the contract that each backend engine must satisfy.
``ExecutionEngineBackend`` covers snapshot mapping and definition creation;
``EngineFlowDefinition`` and ``EngineCheckpointDefinition`` cover the
backend-neutral wrappers that ``flow.py`` and ``checkpoint.py`` store
after definition creation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

from kitaru.engines._types import ExecutionGraphSnapshot


@runtime_checkable
class EngineFlowDefinition(Protocol):
    """Backend-owned flow definition wrapper.

    Each engine returns an implementation from ``create_flow_definition``.
    It owns the backend-native flow object and delegates run/replay.
    """

    @property
    def source_object(self) -> Any:
        """Raw backend-native object registered under the source alias."""
        ...

    def run(
        self,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        cache: bool,
        retries: int,
        image: Any,
    ) -> Any:
        """Execute the flow with the given options."""
        ...

    def replay(
        self,
        *,
        source_run_id: Any,
        cache: bool,
        retries: int,
        image: Any,
        steps_to_skip: set[str],
        input_overrides: dict[str, Any] | None,
        step_input_overrides: dict[str, dict[str, Any]] | None,
    ) -> Any:
        """Replay a prior execution from a checkpoint boundary."""
        ...


@runtime_checkable
class EngineCheckpointDefinition(Protocol):
    """Backend-owned checkpoint definition wrapper.

    Each engine returns an implementation from
    ``create_checkpoint_definition``. It owns the backend-native step
    object and delegates call/submit/map/product.
    """

    @property
    def source_object(self) -> Any:
        """Raw backend-native object registered under the source alias."""
        ...

    def call(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Synchronously call the checkpoint."""
        ...

    def submit(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Submit the checkpoint for concurrent execution."""
        ...

    def map(
        self,
        *args: Any,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Map the checkpoint across multiple inputs."""
        ...

    def product(
        self,
        *args: Any,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Map the checkpoint as a cartesian product."""
        ...


@runtime_checkable
class ExecutionEngineBackend(Protocol):
    """Backend that manages flow/checkpoint definitions and snapshots.

    Each backend implements this to create backend-native definitions
    and to convert native run objects into ``ExecutionGraphSnapshot``.
    """

    @property
    def name(self) -> str: ...

    def execution_graph_from_run(self, run: Any) -> ExecutionGraphSnapshot: ...

    def create_flow_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
    ) -> EngineFlowDefinition: ...

    def create_checkpoint_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
        retries: int,
        checkpoint_type: str | None,
        runtime: Any,
    ) -> EngineCheckpointDefinition: ...
