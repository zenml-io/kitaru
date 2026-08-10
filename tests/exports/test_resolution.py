"""Tests for private experiment export resolution."""

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.exports.models import ExportError, RewardSelector
from kitaru.exports.resolve import resolve_export
from kitaru.exports.source import inventory_source


def _id() -> uuid.UUID:
    return uuid.uuid4()


class _GetResource:
    def __init__(self, values: dict[Any, Any]) -> None:
        self.values = values
        self.calls: list[Any] = []

    async def get(self, key: Any) -> Any:
        self.calls.append(key)
        return self.values[key]


class _Sessions:
    def __init__(self, summaries: list[Any], full: dict[uuid.UUID, Any]) -> None:
        self.summaries = summaries
        self.full = full
        self.params: list[Any] = []

    async def iter(self, params: Any) -> Any:
        self.params.append(params)
        for session in self.summaries:
            yield session

    async def get_with_nodes(self, session_id: uuid.UUID) -> Any:
        return self.full[session_id]


class _Evaluators:
    def __init__(self, evaluator: Any, version: Any) -> None:
        self.evaluator = evaluator
        self.version = version

    async def iter(self, params: Any) -> Any:
        _ = params
        yield self.evaluator

    async def get_version(self, evaluator_id: uuid.UUID, version: int) -> Any:
        assert evaluator_id == self.evaluator.id
        assert version == self.version.version
        return self.version


class _Blobs:
    def __init__(self, content: bytes) -> None:
        self.content = content

    async def download(self, blob_id: uuid.UUID) -> bytes:
        _ = blob_id
        return self.content


def _client(
    *, inputs: Any = None, override: Any = None, policy_type: str = "passthrough"
) -> tuple[Any, dict[str, uuid.UUID]]:
    ids = {
        name: _id()
        for name in (
            "experiment",
            "cohort_version",
            "cohort",
            "agent",
            "agent_version",
            "session",
            "evaluator",
            "evaluator_version",
            "blob",
        )
    }
    experiment = SimpleNamespace(
        id=ids["experiment"],
        agent_id=ids["agent"],
        override=override,
        tool_policy=SimpleNamespace(
            default=SimpleNamespace(type=policy_type), tools={}
        ),
        evaluators=[
            SimpleNamespace(evaluator="quality", version=3, params={"threshold": 0.5})
        ],
    )
    cohort_version = SimpleNamespace(
        id=ids["cohort_version"],
        cohort_id=ids["cohort"],
        session_count=1,
    )
    cohort = SimpleNamespace(id=ids["cohort"], agent_id=ids["agent"])
    agent_version = SimpleNamespace(
        id=ids["agent_version"],
        agent_id=ids["agent"],
        run_spec=SimpleNamespace(
            command="python agent.py",
            working_dir=None,
            env={"MODEL": "test"},
            secret_ids=[_id()],
            timeout_seconds=60,
        ),
    )
    summary = SimpleNamespace(id=ids["session"])
    full_session = SimpleNamespace(
        id=ids["session"],
        agent_id=ids["agent"],
        inputs={"prompt": "hello"} if inputs is None else inputs,
    )
    full = SimpleNamespace(session=full_session, nodes=[])
    evaluator = SimpleNamespace(id=ids["evaluator"], name="quality")
    version = SimpleNamespace(
        id=ids["evaluator_version"],
        evaluator_id=ids["evaluator"],
        version=3,
        source=SimpleNamespace(
            type="script", blob_id=ids["blob"], entrypoint="evaluate"
        ),
    )
    client = SimpleNamespace(
        experiments=_GetResource({ids["experiment"]: experiment}),
        cohort_versions=_GetResource({ids["cohort_version"]: cohort_version}),
        cohorts=_GetResource({ids["cohort"]: cohort}),
        agent_versions=_GetResource({ids["agent_version"]: agent_version}),
        sessions=_Sessions([summary], {ids["session"]: full}),
        evaluators=_Evaluators(evaluator, version),
        blobs=_Blobs(b"def evaluate(session): return []\n"),
    )
    return client, ids


async def test_resolve_export_uses_exact_reads_and_materializes_scripts(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()

    resolved = await resolve_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(tmp_path),
    )

    assert [full.session.id for full in resolved.sessions] == [ids["session"]]
    assert resolved.evaluators[0].script == b"def evaluate(session): return []\n"
    assert resolved.evaluators[0].params == {"threshold": 0.5}
    assert client.experiments.calls == [ids["experiment"]]
    assert client.cohort_versions.calls == [ids["cohort_version"]]
    assert client.agent_versions.calls == [ids["agent_version"]]


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"policy_type": "history"}, "unsupported_tool_policy"),
        ({"override": SimpleNamespace(prompt="changed")}, "unsupported_override"),
        ({"inputs": {"value": "x" * 32768}}, "inputs_too_large"),
    ],
)
async def test_resolve_export_rejects_unsupported_semantics(
    tmp_path: Path, kwargs: dict[str, Any], code: str
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client(**kwargs)

    with pytest.raises(ExportError, match=code):
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )


async def test_resolve_export_rejects_cohort_count_mismatch(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    client.cohort_versions.values[ids["cohort_version"]].session_count = 2

    with pytest.raises(ExportError, match="cohort_count_mismatch"):
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )


async def test_resolve_export_rejects_wrong_agent_and_missing_run_spec(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    client.cohorts.values[ids["cohort"]].agent_id = _id()

    with pytest.raises(ExportError, match="agent_mismatch"):
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    client, ids = _client()
    client.agent_versions.values[ids["agent_version"]].run_spec = None
    with pytest.raises(ExportError, match="missing_run_spec"):
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )


async def test_resolve_export_requires_pinned_evaluator(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    client.experiments.values[ids["experiment"]].evaluators[0].version = None

    with pytest.raises(ExportError, match="unpinned_evaluator"):
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )


async def test_resolve_export_materializes_exact_package_requirement(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    version = client.evaluators.version
    version.source = SimpleNamespace(
        type="package",
        requirement="example-evaluator==1.2.3",
        entrypoint="package:evaluate",
    )

    resolved = await resolve_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:passed"),
        source=inventory_source(tmp_path),
    )

    assert resolved.evaluators[0].script is None
    assert len(resolved.evaluators[0].source_sha256) == 64


def test_reward_selector_requires_supported_result() -> None:
    with pytest.raises(ExportError, match="invalid_reward_selector"):
        RewardSelector.parse("quality:correctness:value")

    selector = RewardSelector.parse("quality:correctness:score")
    assert selector.evaluator == "quality"
    assert selector.result == "correctness"
    assert selector.field == "score"
