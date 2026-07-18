"""Focused contracts for experiment persistence and replay preplanning."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

from zenml.models import PipelineRunResponse

from kitaru._agent_registration import RegisteredAgentVersionBinding
from kitaru._config._agents import (
    _AgentMetadata,
    _AgentMetadataEnvelope,
    _AgentVersionManifest,
)
from kitaru._experiments import (
    freeze_replay_attempt,
    preplan_replay_attempt,
)


def _manifest(version_id: str = "pipeline-id") -> _AgentVersionManifest:
    return _AgentVersionManifest(
        schema_version=1,
        agent_version_id=version_id,
        pipeline_id=version_id,
        pipeline_name=f"support-agent--{version_id}",
        fingerprint=f"sha256:{version_id}",
        git_sha="7f192aa456789",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash=f"sha256:config-{version_id}",
        worldview_hash=f"sha256:worldview-{version_id}",
        entrypoint="evals.register:kagent",
        registered_at="2026-07-17T08:30:00Z",
        source="registration",
    )


def _binding(version_id: str = "pipeline-id") -> RegisteredAgentVersionBinding:
    return RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=_manifest(version_id),
    )


def _step(
    name: str,
    *,
    invocation_id: str | None = None,
    started_at: datetime | None = None,
    upstream_steps: list[str] | None = None,
) -> Any:
    timestamp = started_at or datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        type=None,
        start_time=timestamp,
        end_time=timestamp + timedelta(seconds=1),
        spec=SimpleNamespace(
            invocation_id=invocation_id or name,
            upstream_steps=upstream_steps or [],
            inputs_v2={},
        ),
        outputs={"output": [object()]},
        regular_outputs={"output": object()},
        run_metadata={},
    )


def _run(
    run_id: str,
    *steps: Any,
    project_id: str = "project-id",
    status: str = "completed",
    original_run: Any | None = None,
) -> PipelineRunResponse:
    return cast(
        PipelineRunResponse,
        SimpleNamespace(
            id=run_id,
            project_id=project_id,
            status=SimpleNamespace(value=status),
            original_run=original_run,
            orchestrator_environment={},
            steps={step.name: step for step in steps},
            config=SimpleNamespace(parameters={}),
        ),
    )


class _RunClient:
    def __init__(self, runs: dict[str, Any]) -> None:
        self.runs = runs
        self.get_calls: list[str] = []

    def get_pipeline_run(
        self,
        *,
        name_id_or_prefix: str,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> Any:
        assert allow_name_prefix_match is False
        assert hydrate is True
        self.get_calls.append(name_id_or_prefix)
        return self.runs[name_id_or_prefix]


def _draft(
    *,
    idempotency_key: str = "request-1",
    name: str | None = None,
    suite_key: str | None = None,
    flow_overrides: dict[str, Any] | None = None,
    created_at: str = "2026-07-17T09:00:00Z",
) -> Any:
    run = _run("run-1", _step("at"))
    return preplan_replay_attempt(
        ["run-1"],
        binding=_binding(),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key=idempotency_key,
        repeats=1,
        wait=False,
        name=name,
        suite_key=suite_key,
        flow_overrides=flow_overrides,
        created_at=created_at,
        client=_RunClient({"run-1": run}),
        pipeline_verifier=lambda _client, _binding: None,
    )


def _plan(**kwargs: Any) -> Any:
    return freeze_replay_attempt(_draft(**kwargs))


def _base_envelope() -> _AgentMetadataEnvelope:
    manifest = _manifest()
    return _AgentMetadataEnvelope(
        schema_version=1,
        agent=_AgentMetadata(agent_id="project-id", name="support-agent"),
        agent_version_order=["pipeline-id"],
        agent_versions={"pipeline-id": manifest},
    )


def _stored_metadata(envelope: _AgentMetadataEnvelope) -> dict[str, Any]:
    return {
        "foreign": {"preserve": True},
        "kitaru": {
            **envelope.model_dump(mode="json"),
            "future_key": {"preserve": [1, 2, 3]},
        },
    }


class _ProjectClient:
    def __init__(self, envelope: _AgentMetadataEnvelope) -> None:
        self.metadata = _stored_metadata(envelope)
        self.update_calls: list[dict[str, Any]] = []
        self.raise_after_first_commit = False
        self._raised = False
        self.list_run_calls: list[dict[str, Any]] = []

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> Any:
        assert selector == "project-id"
        assert allow_name_prefix_match is False
        assert hydrate is True
        return SimpleNamespace(
            id="project-id",
            name="support-agent",
            project_metadata=deepcopy(self.metadata),
        )

    def update_project(
        self,
        selector: str,
        *,
        project_metadata: dict[str, Any],
    ) -> Any:
        assert selector == "project-id"
        self.metadata = deepcopy(project_metadata)
        self.update_calls.append(deepcopy(project_metadata))
        if self.raise_after_first_commit and not self._raised:
            self._raised = True
            raise RuntimeError("response lost after commit")
        return self.get_project(
            "project-id",
            allow_name_prefix_match=False,
            hydrate=True,
        )

    def list_pipeline_runs(self, **kwargs: Any) -> Any:
        self.list_run_calls.append(kwargs)
        return SimpleNamespace(items=[])


class _Artifact:
    def __init__(self, artifact_id: str, name: str, value: list[str]) -> None:
        self.id = artifact_id
        self.name = name
        self._value = list(value)

    def load(self) -> list[str]:
        return list(self._value)


class _ArtifactClient:
    def __init__(self) -> None:
        self.active_project = SimpleNamespace(id="project-id")
        self.artifacts: list[_Artifact] = []

    def list_artifact_versions(self, **kwargs: Any) -> Any:
        name = str(kwargs["name"]).removeprefix("equals:")
        return SimpleNamespace(
            items=[artifact for artifact in self.artifacts if artifact.name == name]
        )

    def get_artifact_version(self, *, name_id_or_prefix: str, **_: Any) -> _Artifact:
        return next(
            artifact for artifact in self.artifacts if artifact.id == name_id_or_prefix
        )
