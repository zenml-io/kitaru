"""ZenML execution engine backend for Kitaru.

Owns all ZenML-native flow and checkpoint definition creation. This module
is lazily imported by the engine registry on first backend access — importing
``kitaru.engines`` does not import this file.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from zenml.config.constants import DOCKER_SETTINGS_KEY
from zenml.config.docker_settings import DockerSettings
from zenml.config.retry_config import StepRetryConfig
from zenml.enums import StepType
from zenml.pipelines.pipeline_decorator import pipeline
from zenml.steps.step_decorator import step

from kitaru.config import ImageSettings, image_settings_to_docker_settings
from kitaru.engines._types import ExecutionGraphSnapshot
from kitaru.engines.zenml.snapshots import (
    execution_graph_from_run as _snapshot_mapper,
)

# -- Retry conversion (shared by flow and checkpoint) -------------------------


def _to_retry_config(retries: int) -> StepRetryConfig | None:
    """Convert a retry count to ZenML retry config."""
    if retries == 0:
        return None
    return StepRetryConfig(max_retries=retries)


# -- Docker settings ----------------------------------------------------------


def _build_settings(
    image: ImageSettings | None,
) -> dict[str, DockerSettings]:
    """Build ZenML settings payload for flow execution."""
    return {DOCKER_SETTINGS_KEY: image_settings_to_docker_settings(image)}


# -- Checkpoint helpers --------------------------------------------------------


def _build_checkpoint_extra(checkpoint_type: str | None) -> dict[str, Any]:
    """Build namespaced step metadata for dashboard rendering."""
    payload: dict[str, Any] = {"boundary": "checkpoint"}
    if checkpoint_type is not None:
        payload["type"] = checkpoint_type
    return {"kitaru": payload}


_KNOWN_STEP_TYPES: dict[str, StepType] = {
    "llm_call": StepType.LLM_CALL,
    "tool_call": StepType.TOOL_CALL,
}


def _to_step_type(checkpoint_type: str | None) -> StepType | None:
    """Map well-known checkpoint types to ZenML's StepType enum."""
    if checkpoint_type is None:
        return None
    return _KNOWN_STEP_TYPES.get(checkpoint_type)


# -- Definition wrappers -------------------------------------------------------


class ZenMLFlowDefinition:
    """Flow definition backed by a ZenML Pipeline object."""

    __slots__ = ("_pipeline",)

    def __init__(self, pipeline_obj: Any) -> None:
        self._pipeline = pipeline_obj

    @property
    def source_object(self) -> Any:
        return self._pipeline

    def _configured(
        self,
        cache: bool,
        retries: int,
        image: ImageSettings | None,
    ) -> Any:
        """Return a pipeline configured with execution options."""
        return self._pipeline.with_options(
            enable_cache=cache,
            retry=_to_retry_config(retries),
            settings=_build_settings(image),
        )

    @staticmethod
    def _persist_spec(run: Any, frozen_execution_spec: Any) -> None:
        """Persist frozen execution spec on a successful run if provided."""
        if run is not None and frozen_execution_spec is not None:
            from kitaru.config import persist_frozen_execution_spec

            persist_frozen_execution_spec(
                run_id=run.id,
                frozen_execution_spec=frozen_execution_spec,
            )

    def run(
        self,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        cache: bool,
        retries: int,
        image: ImageSettings | None,
        frozen_execution_spec: Any = None,
    ) -> Any:
        run = self._configured(cache, retries, image)(*args, **kwargs)
        self._persist_spec(run, frozen_execution_spec)
        return run

    def replay(
        self,
        *,
        source_run_id: Any,
        cache: bool,
        retries: int,
        image: ImageSettings | None,
        steps_to_skip: set[str],
        input_overrides: dict[str, Any] | None,
        step_input_overrides: dict[str, dict[str, Any]] | None,
        frozen_execution_spec: Any = None,
    ) -> Any:
        run = self._configured(cache, retries, image).replay(
            pipeline_run=source_run_id,
            skip=steps_to_skip,
            skip_successful_steps=False,
            input_overrides=input_overrides,
            step_input_overrides=step_input_overrides,
        )
        self._persist_spec(run, frozen_execution_spec)
        return run


class ZenMLCheckpointDefinition:
    """Checkpoint definition backed by a ZenML step object."""

    __slots__ = ("_step",)

    def __init__(self, step_obj: Any) -> None:
        self._step = step_obj

    @property
    def source_object(self) -> Any:
        return self._step

    def call(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        return self._step(*args, id=id, after=after, **kwargs)

    def submit(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        return self._step.submit(*args, id=id, after=after, **kwargs)

    def map(
        self,
        *args: Any,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        return self._step.map(*args, after=after, **kwargs)

    def product(
        self,
        *args: Any,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        return self._step.product(*args, after=after, **kwargs)


# -- Runtime session -----------------------------------------------------------


class ZenMLRuntimeSession:
    """ZenML-backed runtime session for in-flow primitive dispatch.

    Each method delegates to a helper in the corresponding primitive module
    via method-local imports, preserving existing test patch points.
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
        from kitaru.wait import _wait_via_zenml

        return _wait_via_zenml(
            schema=schema,
            name=name,
            question=question,
            timeout=timeout,
            metadata=metadata,
        )

    def save_artifact(
        self,
        name: str,
        value: Any,
        *,
        type: str,
        tags: list[str] | None = None,
    ) -> None:
        from kitaru.artifacts import _save_via_zenml

        _save_via_zenml(name, value, type=type, tags=tags)

    def load_artifact(self, exec_id: str, name: str) -> Any:
        from kitaru.artifacts import _load_via_zenml

        return _load_via_zenml(exec_id, name)

    def log_metadata(self, metadata: dict[str, Any]) -> None:
        from kitaru.logging import _log_via_zenml

        _log_via_zenml(metadata)


# -- Backend class -------------------------------------------------------------


class ZenMLExecutionEngineBackend:
    """Full ZenML engine backend with flow/checkpoint definition creation."""

    @property
    def name(self) -> str:
        return "zenml"

    def execution_graph_from_run(self, run: Any) -> ExecutionGraphSnapshot:
        return _snapshot_mapper(run)

    def create_flow_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
    ) -> ZenMLFlowDefinition:
        pipeline_obj = pipeline(dynamic=True, name=registration_name)(entrypoint)
        return ZenMLFlowDefinition(pipeline_obj)

    def create_checkpoint_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
        retries: int,
        checkpoint_type: str | None,
        runtime: Any,
    ) -> ZenMLCheckpointDefinition:
        step_obj = step(
            name=registration_name,
            retry=_to_retry_config(retries),
            extra=_build_checkpoint_extra(checkpoint_type),
            step_type=_to_step_type(checkpoint_type),
            runtime=runtime,
        )(entrypoint)
        return ZenMLCheckpointDefinition(step_obj)

    def create_runtime_session(self) -> ZenMLRuntimeSession:
        return ZenMLRuntimeSession()
