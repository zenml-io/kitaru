"""Safety probes for registered PydanticAI imported replay runs."""

import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from pydantic_ai import Agent
from pydantic_ai import mcp as pydantic_ai_mcp
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets import FunctionToolset
from zenml.client import Client

from kitaru._agent_registration import verify_hydrated_submitted_run_binding
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.experiments import RegressionLimits
from kitaru.imports import ImportedReplayMode, ReplayPartKind
from kitaru.replay import ExperimentReplayContext
from kitaru.runtime import _flow_scope
from kitaru.scoring import ExperimentVerdict, Score, scorer
from tests.test_pydantic_ai_imported_replay_runtime import (
    _boundary,
    _disable_checkpoints,
    _model,
    _prepared_evidence,
    _toolset,
    _with_tool_arguments,
)

_REGISTERED_IMPORTED_REPLAY_AGENT: KitaruAgent[Any, str] | None = None


@scorer(capability="pure", name="ground-truth")
def _ground_truth_objective(_: object) -> Score:
    return Score(value=1.0)


def _ground_truth_protection(_: object) -> Score:
    return Score(value=1.0)


def _sentinel_tool(name: str, calls: list[str]) -> Any:
    def tool() -> str:
        calls.append(name)
        return f"recorded:{name}"

    tool.__name__ = name
    return tool


def test_one_run_replaces_direct_tool_toolsets_and_mcp_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_calls: list[str] = []

    def direct_tool() -> str:
        original_calls.append("direct_tool")
        raise AssertionError("the registered direct tool must not run")

    registered_toolset = FunctionToolset()

    @registered_toolset.tool_plain
    def toolset_tool() -> str:
        original_calls.append("toolset_tool")
        raise AssertionError("the registered FunctionToolset must not run")

    per_run_toolset = FunctionToolset()

    @per_run_toolset.tool_plain
    def per_run_tool() -> str:
        original_calls.append("per_run_tool")
        raise AssertionError("the per-run toolset must not run")

    mcp_server_stdio = vars(pydantic_ai_mcp)["MCPServerStdio"]
    unavailable_mcp = mcp_server_stdio(
        "/this/path/must-not-be-opened/by-an-imported-replay", args=[]
    )
    tool_names = ("direct_tool", "toolset_tool", "mcp_tool", "per_run_tool")
    wrapped = KitaruAgent(
        Agent(
            TestModel(call_tools=list(tool_names)),
            name=f"tool_replacement_{uuid4().hex[:8]}",
            output_type=str,
            tools=[cast(Any, direct_tool)],
            toolsets=[registered_toolset, unavailable_mcp],
        )
    )
    replacement_calls: list[str] = []
    replacement = FunctionToolset(
        [_sentinel_tool(name, replacement_calls) for name in tool_names]
    )

    _disable_checkpoints(monkeypatch, wrapped)

    with (
        _flow_scope(name=f"tool_replacement_probe_{uuid4().hex[:8]}"),
        wrapped._replace_tool_sources(cast(Any, [replacement])),
    ):
        result = wrapped.run_sync(
            "use every available tool", toolsets=cast(Any, [per_run_toolset])
        )

    assert isinstance(result.output, str)
    assert original_calls == []
    assert sorted(replacement_calls) == sorted(tool_names)


def _initialize_entrypoint_repository(repository_root: Path) -> None:
    entrypoint_module = repository_root / "registered_imported_replay_agent.py"
    entrypoint_module.write_text(
        "from tests.test_pydantic_ai_imported_replay import "
        "_REGISTERED_IMPORTED_REPLAY_AGENT\n"
    )
    (repository_root / ".gitignore").write_text(".kitaru/\n")
    subprocess.run(["git", "init", "-q", str(repository_root)], check=True)
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository_root),
            "add",
            ".gitignore",
            entrypoint_module.name,
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "commit", "-qm", "test entrypoint"],
        check=True,
    )


def test_registered_entrypoint_creates_normal_candidate_execution(
    primed_zenml: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del primed_zenml
    global _REGISTERED_IMPORTED_REPLAY_AGENT

    repository_root = Path(Client.find_repository())
    _initialize_entrypoint_repository(repository_root)
    monkeypatch.syspath_prepend(repository_root)
    importlib.invalidate_caches()
    sys.modules.pop("registered_imported_replay_agent", None)

    agent = KitaruAgent(
        Agent(
            TestModel(),
            name=f"imported_replay_candidate_{uuid4().hex[:8]}",
            output_type=str,
        )
    )
    _REGISTERED_IMPORTED_REPLAY_AGENT = agent
    try:
        registration = agent.register(
            entrypoint=(
                "registered_imported_replay_agent:_REGISTERED_IMPORTED_REPLAY_AGENT"
            )
        )
        module = importlib.import_module("registered_imported_replay_agent")
        entrypoint = module._REGISTERED_IMPORTED_REPLAY_AGENT
        assert entrypoint is agent

        lineage = ExperimentReplayContext(
            experiment_id=f"experiment-{uuid4().hex}",
            target_execution_id=f"imported-{uuid4().hex}",
            repeat_index=0,
            parent_execution_id=f"parent-{uuid4().hex}",
            root_execution_id=f"root-{uuid4().hex}",
            lineage_kind="imported_replay",
        )
        with entrypoint._capture_imported_replay_execution(lineage) as capture:
            result = entrypoint.run_sync("run the registered candidate")
    finally:
        _REGISTERED_IMPORTED_REPLAY_AGENT = None

    assert isinstance(result.output, str)
    assert capture.context is lineage
    assert capture.execution_id is not None
    run = Client().get_pipeline_run(
        capture.execution_id,
        allow_name_prefix_match=False,
        hydrate=True,
        project=registration.agent.agent_id,
    )
    verified = verify_hydrated_submitted_run_binding(
        run,
        binding=agent._registered_state.binding,  # type: ignore[union-attr]
    )
    assert str(verified.id) == capture.execution_id
    assert str(verified.pipeline.id) == registration.agent_version.pipeline_id


def test_imported_experiment_ground_truth_through_registered_candidate(
    primed_zenml: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise root, history, suite repeat, and idempotent recovery together."""
    del primed_zenml
    global _REGISTERED_IMPORTED_REPLAY_AGENT

    repository_root = Path(Client.find_repository())
    _initialize_entrypoint_repository(repository_root)
    monkeypatch.syspath_prepend(repository_root)
    importlib.invalidate_caches()
    sys.modules.pop("registered_imported_replay_agent", None)

    toolset = _toolset(effect="write")
    agent = KitaruAgent(
        Agent(
            _model({"q": "x"}),
            name=f"imported_experiment_{uuid4().hex[:8]}",
            output_type=str,
            toolsets=[toolset],
        )
    )
    agent.protection("safe-output", capability="pure")(_ground_truth_protection)
    _REGISTERED_IMPORTED_REPLAY_AGENT = agent
    try:
        agent.register(
            entrypoint=(
                "registered_imported_replay_agent:_REGISTERED_IMPORTED_REPLAY_AGENT"
            )
        )
        state = agent._registered_state
        assert state is not None
        evidence = _prepared_evidence()
        evidence = evidence.model_copy(
            update={
                "identity": evidence.identity.model_copy(
                    update={
                        "execution_id": "imported-ground-truth",
                        "project_id": state.binding.project_id,
                        "source_agent_version_id": (
                            state.binding.manifest.agent_version_id
                        ),
                        "source_pipeline_id": state.binding.manifest.pipeline_id,
                        "source_fingerprint": state.binding.manifest.fingerprint,
                    }
                )
            }
        )

        def load_evidence(*_args: Any, **_kwargs: Any) -> Any:
            return evidence

        monkeypatch.setattr(
            "kitaru.imports._replay_loading.load_imported_replay_evidence",
            load_evidence,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._agent.load_imported_replay_evidence",
            load_evidence,
        )

        root = agent.replay(
            "imported-ground-truth",
            imported_mode=ImportedReplayMode.ROOT_INPUT,
            on_error="fail",
            idempotency_key="imported-root",
            scorers=[_ground_truth_objective],
        )
        boundary = _boundary(evidence, ReplayPartKind.TOOL_RESULT)
        history = agent.replay(
            "imported-ground-truth",
            imported_mode=ImportedReplayMode.MESSAGE_HISTORY,
            imported_boundary=boundary,
            on_error="fail",
            idempotency_key="imported-history",
            scorers=[_ground_truth_objective],
        )
        repeated = agent.replay(
            experiment=history.spec.experiment_id,
            idempotency_key="imported-history-repeat",
            repeats=2,
            scorers=[_ground_truth_objective],
            limits=RegressionLimits(max_trials=2),
        )
        retried = agent.replay(
            experiment=history.spec.experiment_id,
            idempotency_key="imported-history-repeat",
            repeats=2,
            scorers=[_ground_truth_objective],
            limits=RegressionLimits(max_trials=2),
        )
        evidence = _with_tool_arguments(evidence, {"q": "mutated"})
        blocked = agent.replay(
            "imported-ground-truth",
            imported_mode=ImportedReplayMode.ROOT_INPUT,
            on_error="fail",
            idempotency_key="imported-blocked-mutation",
            repeats=2,
            scorers=[_ground_truth_objective],
        )
    finally:
        _REGISTERED_IMPORTED_REPLAY_AGENT = None

    assert root.record.status == "completed"
    assert root.record.imported_replay_evidence is not None
    assert root.record.imported_replay_evidence.recorded_response_hits == 1
    assert isinstance(root.record.imported_replay_members[0].decisions, tuple)
    assert isinstance(
        root.record.imported_replay_members[0].model_dump(mode="json")["decisions"],
        list,
    )
    assert root.record.imported_replay_members[0].parent_execution_id == (
        "imported-ground-truth"
    )
    assert root.record.imported_replay_members[0].root_execution_id == (
        "imported-ground-truth"
    )
    assert root.verdict is not None
    assert root.verdict.verdict is ExperimentVerdict.HOLD

    assert history.record.status == "completed"
    assert history.record.imported_replay_evidence is not None
    assert history.record.imported_replay_evidence.complete_prefixes == 1
    assert history.record.imported_replay_evidence.recorded_response_misses == 0
    assert history.verdict is not None
    assert history.verdict.verdict is ExperimentVerdict.PASS

    assert repeated.record.counts.verified == 2
    assert len(repeated.record.imported_replay_members) == 2
    assert repeated.spec.regression_limits is not None
    assert repeated.spec.regression_limits.max_trials == 2
    assert repeated.verdict is not None
    assert repeated.verdict.verdict is ExperimentVerdict.PASS
    assert retried.record == repeated.record
    assert retried.submission.results == []

    assert blocked.record.status == "completed"
    assert blocked.record.counts.verified == 2
    assert blocked.record.counts.failed == 0
    assert blocked.record.imported_replay_evidence is not None
    assert blocked.record.imported_replay_evidence.blocked_calls > 0
    assert blocked.verdict is not None
    assert blocked.verdict.verdict is ExperimentVerdict.HOLD


def test_failed_imported_candidate_cannot_complete_or_pass(
    primed_zenml: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del primed_zenml
    global _REGISTERED_IMPORTED_REPLAY_AGENT

    repository_root = Path(Client.find_repository())
    _initialize_entrypoint_repository(repository_root)
    monkeypatch.syspath_prepend(repository_root)
    importlib.invalidate_caches()
    sys.modules.pop("registered_imported_replay_agent", None)

    def fail_after_recorded_response(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        if (
            messages
            and isinstance(messages[-1], pydantic_messages.ModelRequest)
            and any(
                isinstance(part, pydantic_messages.ToolReturnPart)
                for part in messages[-1].parts
            )
        ):
            raise RuntimeError("candidate failed after recorded response")
        return pydantic_messages.ModelResponse(
            parts=[
                pydantic_messages.ToolCallPart(
                    tool_name="lookup",
                    args={"q": "x"},
                    tool_call_id="candidate-call",
                )
            ]
        )

    toolset = _toolset(effect="write")
    agent = KitaruAgent(
        Agent(
            FunctionModel(fail_after_recorded_response),
            name=f"failed_imported_candidate_{uuid4().hex[:8]}",
            output_type=str,
            toolsets=[toolset],
        )
    )
    agent.protection("safe-output", capability="pure")(_ground_truth_protection)
    _REGISTERED_IMPORTED_REPLAY_AGENT = agent
    try:
        agent.register(
            entrypoint=(
                "registered_imported_replay_agent:_REGISTERED_IMPORTED_REPLAY_AGENT"
            )
        )
        state = agent._registered_state
        assert state is not None
        evidence = _prepared_evidence()
        evidence = evidence.model_copy(
            update={
                "identity": evidence.identity.model_copy(
                    update={
                        "execution_id": "imported-failed-candidate",
                        "project_id": state.binding.project_id,
                        "source_agent_version_id": (
                            state.binding.manifest.agent_version_id
                        ),
                        "source_pipeline_id": state.binding.manifest.pipeline_id,
                        "source_fingerprint": state.binding.manifest.fingerprint,
                    }
                )
            }
        )

        monkeypatch.setattr(
            "kitaru.imports._replay_loading.load_imported_replay_evidence",
            lambda *_args, **_kwargs: evidence,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._agent.load_imported_replay_evidence",
            lambda *_args, **_kwargs: evidence,
        )

        result = agent.replay(
            "imported-failed-candidate",
            imported_mode=ImportedReplayMode.ROOT_INPUT,
            on_error="collect",
            idempotency_key="imported-failed-candidate",
            scorers=[_ground_truth_objective],
        )
    finally:
        _REGISTERED_IMPORTED_REPLAY_AGENT = None

    assert result.record.status == "failed"
    assert result.record.counts.submitted == 1
    assert result.record.counts.verified == 0
    assert result.record.counts.unverified == 1
    assert result.record.imported_replay_evidence is not None
    assert result.record.imported_replay_evidence.recorded_response_hits == 1
    assert result.record.imported_replay_members[0].candidate_status == "failed"
    assert result.verdict is not None
    assert result.verdict.verdict is ExperimentVerdict.HOLD
