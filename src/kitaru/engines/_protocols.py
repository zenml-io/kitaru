"""Engine backend protocol definitions.

These protocols define the contract that each backend engine must satisfy.
``ExecutionEngineBackend`` covers snapshot mapping, definition creation,
and runtime session creation; ``EngineFlowDefinition`` and
``EngineCheckpointDefinition`` cover the backend-neutral wrappers that
``flow.py`` and ``checkpoint.py`` store after definition creation;
``RuntimeSession`` covers in-flow primitive dispatch (wait, save, load, log).
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
        frozen_execution_spec: Any = None,
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
        frozen_execution_spec: Any = None,
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
class RuntimeSession(Protocol):
    """Backend-owned runtime session for in-flow primitive dispatch.

    Each backend returns an implementation from ``create_runtime_session``.
    The session is installed by ``_flow_scope()`` and used by ``wait()``,
    ``save()``, ``load()``, and ``log()`` to dispatch to the active backend.
    """

    def wait(
        self,
        *,
        schema: Any = None,
        name: str | None = None,
        question: str | None = None,
        timeout: int,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Suspend the flow until input is provided."""
        ...

    def save_artifact(
        self,
        name: str,
        value: Any,
        *,
        type: str,
        tags: list[str] | None = None,
    ) -> None:
        """Persist a named artifact inside the current checkpoint."""
        ...

    def load_artifact(self, exec_id: str, name: str) -> Any:
        """Load a named artifact from a previous execution."""
        ...

    def log_metadata(self, metadata: dict[str, Any]) -> None:
        """Attach structured metadata to the current checkpoint or execution."""
        ...


@runtime_checkable
class ExecutionEngineBackend(Protocol):
    """Backend that manages flow/checkpoint definitions, snapshots, and sessions.

    Each backend implements this to create backend-native definitions,
    convert native run objects into ``ExecutionGraphSnapshot``, and
    provide runtime sessions for in-flow primitive dispatch.
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

    def create_runtime_session(self) -> RuntimeSession: ...
