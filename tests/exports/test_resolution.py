"""Tests for private experiment export resolution."""

import pickle
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.exports.models import EnvironmentPolicy, ExportError, RewardSelector
from kitaru.exports.resolve import resolve_export, resolve_remote_export
from kitaru.exports.source import inventory_source


def _id() -> uuid.UUID:
    return uuid.uuid4()


def _write_source(root: Path) -> None:
    (root / "agent.py").write_text("print('ok')\n")
    (root / "pyproject.toml").write_text(
        '[project]\nname = "fixture-agent"\nversion = "1.0.0"\n'
        'requires-python = ">=3.11"\ndependencies = []\n'
    )


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


class _Secrets:
    def __init__(self, values: dict[uuid.UUID, Any]) -> None:
        self.values = values
        self.calls: list[tuple[uuid.UUID, bool]] = []

    async def get(self, secret_id: uuid.UUID, *, include_values: bool = False) -> Any:
        self.calls.append((secret_id, include_values))
        value = self.values[secret_id]
        if isinstance(value, Exception):
            raise value
        return value


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
            "secret",
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
            secret_ids=[ids["secret"]],
            timeout_seconds=60,
        ),
    )
    summary = SimpleNamespace(id=ids["session"])
    full_session = SimpleNamespace(
        id=ids["session"],
        agent_id=ids["agent"],
        inputs={"prompt": "hello"} if inputs is None else inputs,
        outputs="safe output",
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
        secrets=_Secrets(
            {
                ids["secret"]: SimpleNamespace(
                    id=ids["secret"],
                    name="provider-credential",
                    values={
                        "MODEL_API_KEY": SimpleNamespace(
                            get_secret_value=lambda: "sentinel-secret-value"
                        )
                    },
                )
            }
        ),
    )
    return client, ids


async def test_resolve_export_uses_exact_reads_and_materializes_scripts(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
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
    assert client.secrets.calls == [(ids["secret"], True)]
    assert resolved.agent_version.run_spec is not None
    assert resolved.agent_version.run_spec.secret_ids == []
    assert resolved.required_environment_names == ("MODEL_API_KEY",)
    assert resolved.command_argv == ("python", "agent.py")


async def test_resolve_export_sanitizes_nested_exact_values_and_freezes_environment(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
    client, ids = _client(inputs={"nested": ["prefix sentinel-secret-value suffix"]})
    client.agent_versions.values[ids["agent_version"]].run_spec.env = {
        "MODE": "test",
        "MODEL_API_KEY": "stale configuration",
    }

    resolved = await resolve_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(tmp_path),
    )

    assert resolved.sessions[0].session.inputs == {
        "nested": ["prefix [REDACTED] suffix"]
    }
    assert resolved.agent_version.run_spec is not None
    assert resolved.agent_version.run_spec.env == {"MODE": "test"}
    assert resolved.required_environment_names == ("MODEL_API_KEY",)
    assert "sentinel-secret-value" not in repr(resolved)
    assert "provider-credential" not in repr(resolved)


async def test_resolve_export_runtime_only_moves_registered_environment_to_requirements(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
    client, ids = _client()

    resolved = await resolve_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(tmp_path),
        environment_policy=EnvironmentPolicy(mode="runtime_only"),
    )

    assert resolved.agent_version.run_spec is not None
    assert resolved.agent_version.run_spec.env == {}
    assert resolved.required_environment_names == ("MODEL", "MODEL_API_KEY")
    assert {
        requirement.name: requirement.source
        for requirement in resolved.runtime_environment
    } == {
        "MODEL": "registered_environment",
        "MODEL_API_KEY": "attached_secret",
    }


async def test_remote_resolution_is_source_free_and_ephemeral() -> None:
    client, ids = _client(inputs={"token": "sentinel-secret-value"})

    remote = await resolve_remote_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:score"),
    )

    assert remote.sessions[0].session.inputs == {"token": "[REDACTED]"}
    assert "sentinel-secret-value" not in repr(remote)
    assert "provider-credential" not in repr(remote)
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(remote)


@pytest.mark.parametrize("value", ["", "short"])
async def test_resolve_export_rejects_secret_values_that_cannot_be_matched_safely(
    tmp_path: Path, value: str
) -> None:
    _write_source(tmp_path)
    client, ids = _client()
    secret = client.secrets.values[ids["secret"]]
    secret.values["MODEL_API_KEY"] = SimpleNamespace(get_secret_value=lambda: value)

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == "unsafe_secret_value"
    if value:
        assert value not in raised.value.message


async def test_resolve_export_hides_secret_identity_when_authorization_fails(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
    client, ids = _client()
    client.secrets.values[ids["secret"]] = PermissionError("denied provider-credential")

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == "secret_resolution_failed"
    assert "provider-credential" not in str(raised.value)


@pytest.mark.parametrize(
    ("command", "code"),
    [
        ("python agent.py && echo unsafe", "unsupported_run_command"),
        ("python -c 'print(1)'", "unsupported_run_command"),
        ("python agent.py sentinel-secret-value", "protected_value_in_command"),
    ],
)
async def test_resolve_export_rejects_unsafe_run_commands(
    tmp_path: Path, command: str, code: str
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    client.agent_versions.values[ids["agent_version"]].run_spec.command = command

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == code
    assert "sentinel-secret-value" not in str(raised.value)


async def test_resolve_export_deduplicates_secret_reads_but_preserves_requirements(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
    client, ids = _client()
    run_spec = client.agent_versions.values[ids["agent_version"]].run_spec
    run_spec.secret_ids = [ids["secret"], ids["secret"]]

    resolved = await resolve_export(
        client,
        experiment_id=ids["experiment"],
        cohort_version_id=ids["cohort_version"],
        agent_version_id=ids["agent_version"],
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(tmp_path),
    )

    assert client.secrets.calls == [(ids["secret"], True)]
    assert resolved.required_environment_names == ("MODEL_API_KEY",)


async def test_resolve_export_enforces_attached_secret_budgets(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    run_spec = client.agent_versions.values[ids["agent_version"]].run_spec
    run_spec.secret_ids = [ids["secret"]] * 101

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "too_many_attached_secrets"

    client, ids = _client()
    secret = client.secrets.values[ids["secret"]]
    oversized = "x" * (1024 * 1024 + 1)
    secret.values["MODEL_API_KEY"] = SimpleNamespace(get_secret_value=lambda: oversized)
    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "protected_values_too_large"


async def test_resolve_export_rejects_reserved_runtime_names(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    secret = client.secrets.values[ids["secret"]]
    secret.values = {
        "KITARU_TASK_INPUTS": SimpleNamespace(
            get_secret_value=lambda: "sentinel-secret-value"
        )
    }

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == "reserved_environment_name"


@pytest.mark.parametrize(
    ("path", "content", "code"),
    [
        ("sentinel-secret-value.py", "print('ok')\n", "protected_value_in_path"),
        ("agent.py", "TOKEN = 'sentinel-secret-value'\n", "protected_value_in_source"),
    ],
)
async def test_resolve_export_fails_closed_for_protected_source_material(
    tmp_path: Path, path: str, content: str, code: str
) -> None:
    (tmp_path / path).write_text(content)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="agent"\nversion="1"\ndependencies=[]\n'
    )
    client, ids = _client()

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == code
    assert "sentinel-secret-value" not in str(raised.value)


async def test_resolve_export_rejects_protected_dependency_and_evaluator_material(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="agent"\nversion="1"\n'
        'dependencies=["sentinel-secret-value==1"]\n'
    )
    client, ids = _client()

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "protected_value_in_dependency"
    assert "sentinel-secret-value" not in str(raised.value)

    _write_source(tmp_path)
    client, ids = _client()
    client.blobs.content = b"TOKEN = 'sentinel-secret-value'\n"
    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "protected_value_in_evaluator"
    assert "sentinel-secret-value" not in str(raised.value)


async def test_resolve_export_rejects_duplicate_evaluator_names(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    duplicate = SimpleNamespace(
        evaluator="quality", version=3, params={"threshold": 0.9}
    )
    client.experiments.values[ids["experiment"]].evaluators.append(duplicate)

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )

    assert raised.value.code == "duplicate_evaluator_name"


async def test_resolve_export_enforces_session_and_evaluator_blob_budgets(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    client, ids = _client()
    full = client.sessions.full[ids["session"]]
    full.session.outputs = "x" * (16 * 1024 * 1024)

    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "session_too_large"

    client, ids = _client()
    client.blobs.content = b"x" * (10 * 1024 * 1024 + 1)
    with pytest.raises(ExportError) as raised:
        await resolve_export(
            client,
            experiment_id=ids["experiment"],
            cohort_version_id=ids["cohort_version"],
            agent_version_id=ids["agent_version"],
            reward=RewardSelector.parse("quality:correctness:score"),
            source=inventory_source(tmp_path),
        )
    assert raised.value.code == "evaluator_too_large"


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
    _write_source(tmp_path)
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
