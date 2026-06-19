from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from kitaru._config._sandbox import run_sandbox_command
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruUsageError,
)


@dataclass
class FakeStackModel:
    id: str = "stack-1"
    name: str = "dev"


@dataclass
class FakeOutput:
    stdout: str = "out"
    stderr: str = "err"
    exit_code: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False


class FakeProcess:
    def __init__(
        self,
        output: FakeOutput | None = None,
        *,
        collect_error: Exception | None = None,
    ) -> None:
        self.output = output or FakeOutput()
        self.collect_error = collect_error
        self.collect_calls = 0
        self.collect_max_chars: list[int] = []

    def collect(self, *, max_chars: int) -> FakeOutput:
        self.collect_calls += 1
        self.collect_max_chars.append(max_chars)
        if self.collect_error is not None:
            raise self.collect_error
        return self.output


class FakeSession:
    def __init__(
        self,
        process: FakeProcess | None = None,
        *,
        exec_error: Exception | None = None,
        destroy_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.id = "session-1"
        self.process = process or FakeProcess()
        self.exec_error = exec_error
        self.destroy_error = destroy_error
        self.close_error = close_error
        self.exec_calls: list[dict[str, Any]] = []
        self.destroy_calls = 0
        self.close_calls = 0

    def exec(
        self,
        command: str | list[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> FakeProcess:
        self.exec_calls.append({"command": command, "cwd": cwd, "env": env})
        if self.exec_error is not None:
            raise self.exec_error
        return self.process

    def destroy(self) -> None:
        self.destroy_calls += 1
        if self.destroy_error is not None:
            raise self.destroy_error

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class FakeSandbox:
    def __init__(
        self,
        session: FakeSession | None = None,
        *,
        create_error: Exception | None = None,
        id: str | None = "sandbox-1",
        name: str = "sandbox-dev",
    ) -> None:
        self.id = id
        self.name = name
        self.session = session or FakeSession()
        self.create_error = create_error
        self.create_settings: list[Any] = []

    def create_session(self, settings: Any | None = None) -> FakeSession:
        self.create_settings.append(settings)
        if self.create_error is not None:
            raise self.create_error
        return self.session


class FakeActiveStack:
    def __init__(self, sandboxes: dict[str, Any] | None = None) -> None:
        self.sandboxes = (
            {"sandbox-dev": FakeSandbox()} if sandboxes is None else sandboxes
        )


class FakeActiveStackWithoutSandboxApi:
    pass


class FakeSandboxWithoutCreateSession:
    id = "sandbox-1"
    name = "broken"


@dataclass
class FakeClient:
    active_stack: Any
    active_stack_model: FakeStackModel = field(default_factory=FakeStackModel)


def _client_factory(client: FakeClient) -> Any:
    return lambda: client


def _assert_secret_not_in_public_exception(
    exc: BaseException,
    *secret_values: str,
) -> str:
    """Assert a public exception does not expose supplied secret values."""
    message = str(exc)
    formatted = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    for secret_value in secret_values:
        assert secret_value not in message
        assert secret_value not in formatted
    assert exc.__cause__ is None
    return message


def test_run_sandbox_command_returns_stable_result_shape() -> None:
    process = FakeProcess(FakeOutput(stdout="hello\n", stderr="", exit_code=0))
    session = FakeSession(process)
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        ["python", "-c", "print('hello')"],
        cwd="/workspace",
        env={"TOKEN": "secret", "MODE": "test"},
        max_chars=123,
        client_factory=_client_factory(client),
    )

    assert sandbox.create_settings == [None]
    assert session.exec_calls == [
        {
            "command": ["python", "-c", "print('hello')"],
            "cwd": "/workspace",
            "env": {"TOKEN": "secret", "MODE": "test"},
        }
    ]
    assert process.collect_calls == 1
    assert process.collect_max_chars == [123]
    assert session.destroy_calls == 1
    assert session.close_calls == 0
    assert result.model_dump(mode="json") == {
        "command": ["python", "-c", "print('hello')"],
        "cwd": "/workspace",
        "stdout": "hello\n",
        "stderr": "",
        "exit_code": 0,
        "stdout_truncated": False,
        "stderr_truncated": False,
        "stack_id": "stack-1",
        "stack_name": "dev",
        "sandbox_id": "sandbox-1",
        "sandbox_name": "local",
        "session_id": "session-1",
        "cleanup": "destroy",
        "cleanup_succeeded": True,
        "cleanup_error": None,
    }
    assert "TOKEN" not in result.model_dump(mode="json")


def test_run_sandbox_command_returns_non_zero_exit_result() -> None:
    process = FakeProcess(FakeOutput(stdout="", stderr="nope", exit_code=7))
    session = FakeSession(process)
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command("false", client_factory=_client_factory(client))

    assert result.exit_code == 7
    assert result.stderr == "nope"
    assert result.cleanup_succeeded is True


def test_run_sandbox_command_returns_result_when_session_lacks_id() -> None:
    process = FakeProcess(FakeOutput(stdout="done", stderr="warn", exit_code=3))
    session = FakeSession(process)
    del session.id
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command("check", client_factory=_client_factory(client))

    assert result.stdout == "done"
    assert result.stderr == "warn"
    assert result.exit_code == 3
    assert result.session_id is None
    assert result.cleanup_succeeded is True
    assert session.destroy_calls == 1
    assert session.close_calls == 0


def test_run_sandbox_command_copies_truncation_flags() -> None:
    process = FakeProcess(
        FakeOutput(
            stdout="x",
            stderr="y",
            exit_code=0,
            stdout_truncated=True,
            stderr_truncated=True,
        )
    )
    session = FakeSession(process)
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command("emit", client_factory=_client_factory(client))

    assert result.stdout_truncated is True
    assert result.stderr_truncated is True


def test_run_sandbox_command_requires_one_active_sandbox() -> None:
    client = FakeClient(active_stack=FakeActiveStack({}))

    with pytest.raises(KitaruStateError, match="no sandbox component"):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    client = FakeClient(
        active_stack=FakeActiveStack(
            {
                "local": FakeSandbox(),
                "remote-gpu": FakeSandbox(name="remote-gpu"),
            }
        )
    )

    with pytest.raises(KitaruStateError, match="multiple sandbox components"):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))


def test_run_sandbox_command_maps_missing_sandbox_api_to_feature_error() -> None:
    client = FakeClient(active_stack=FakeActiveStackWithoutSandboxApi())

    with pytest.raises(KitaruFeatureNotAvailableError, match="not available"):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    client = FakeClient(
        active_stack=FakeActiveStack({"broken": FakeSandboxWithoutCreateSession()})
    )

    with pytest.raises(KitaruFeatureNotAvailableError, match="not available"):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))


def test_run_sandbox_command_close_cleanup_policy() -> None:
    session = FakeSession()
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        "echo hi",
        cleanup="close",
        client_factory=_client_factory(client),
    )

    assert result.cleanup == "close"
    assert result.cleanup_succeeded is True
    assert session.close_calls == 1
    assert session.destroy_calls == 0


def test_run_sandbox_command_reports_unsupported_destroy_and_closes() -> None:
    session = FakeSession(destroy_error=NotImplementedError("destroy unsupported"))
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command("echo hi", client_factory=_client_factory(client))

    assert result.cleanup == "destroy"
    assert result.cleanup_succeeded is False
    assert "destroy is not supported" in cast(str, result.cleanup_error)
    assert session.destroy_calls == 1
    assert session.close_calls == 1


def test_run_sandbox_command_raises_backend_error_for_unexpected_cleanup_failure() -> (
    None
):
    session = FakeSession(destroy_error=RuntimeError("provider exploded"))
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(
        KitaruBackendError, match="destroying the sandbox session failed"
    ):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    assert session.destroy_calls == 1
    assert session.close_calls == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"command": ""}, "non-empty string"),
        ({"command": []}, "cannot be empty"),
        ({"command": ["echo", 1]}, "non-empty strings"),
        ({"command": "echo", "max_chars": -1}, ">= 0"),
        ({"command": "echo", "max_chars": 1.2}, "integer"),
        ({"command": "echo", "max_chars": True}, "integer"),
        ({"command": "echo", "env": {"A": 1}}, "keys and values"),
        ({"command": "echo", "cleanup": "keep"}, "destroy.*close"),
    ],
)
def test_run_sandbox_command_validates_inputs(
    kwargs: dict[str, Any],
    message: str,
) -> None:
    client = FakeClient(active_stack=FakeActiveStack())

    with pytest.raises(KitaruUsageError, match=message):
        run_sandbox_command(
            client_factory=_client_factory(client),
            **kwargs,
        )


@pytest.mark.parametrize(
    ("sandbox", "expected_destroy_calls"),
    [
        (FakeSandbox(create_error=RuntimeError("create failed")), 0),
        (
            FakeSandbox(FakeSession(exec_error=RuntimeError("exec failed"))),
            1,
        ),
        (
            FakeSandbox(
                FakeSession(FakeProcess(collect_error=RuntimeError("collect failed")))
            ),
            1,
        ),
    ],
)
def test_run_sandbox_command_maps_backend_failures(
    sandbox: FakeSandbox,
    expected_destroy_calls: int,
) -> None:
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError, match="Sandbox command execution failed"):
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    assert sandbox.session.destroy_calls == expected_destroy_calls


@pytest.mark.parametrize(
    ("session", "provider_error"),
    [
        (
            FakeSession(
                exec_error=RuntimeError("exec failed"),
                destroy_error=RuntimeError("destroy failed"),
            ),
            "exec failed",
        ),
        (
            FakeSession(
                FakeProcess(collect_error=RuntimeError("collect failed")),
                destroy_error=RuntimeError("destroy failed"),
            ),
            "collect failed",
        ),
    ],
)
def test_run_sandbox_command_reports_cleanup_warning_after_command_failure(
    session: FakeSession,
    provider_error: str,
) -> None:
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    message = str(exc_info.value)
    assert "Sandbox command execution failed" in message
    assert provider_error in message
    assert "Cleanup warning" in message
    assert "destroy failed" in message
    assert session.destroy_calls == 1


def test_run_sandbox_command_redacts_provider_and_cleanup_error_secrets() -> None:
    env = {"API_TOKEN": "env-secret-123"}
    session = FakeSession(
        exec_error=RuntimeError(
            "exec failed with env-secret-123 password=provider-secret-456 "
            "OPENAI_API_KEY=openai-secret-456 "
            "AWS_SECRET_ACCESS_KEY=aws-secret-789 "
            "GITHUB_TOKEN=github-secret-000 "
            'MY_PASSWORD="password secret with spaces" '
            "api_key='quoted,provider;secret' "
            "Authorization: Basic basic-secret-111"
        ),
        destroy_error=RuntimeError(
            "destroy failed with env-secret-123 access_key=cleanup-secret-000 "
            'private_key="cleanup secret with spaces"'
        ),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(
            ["printenv", "API_TOKEN"],
            env=env,
            client_factory=_client_factory(client),
        )

    assert exc_info.value.__cause__ is None
    message = str(exc_info.value)
    assert "<redacted>" in message
    assert "env-secret-123" not in message
    assert "provider-secret-456" not in message
    assert "openai-secret-456" not in message
    assert "aws-secret-789" not in message
    assert "github-secret-000" not in message
    assert "password secret with spaces" not in message
    assert "basic-secret-111" not in message
    assert "bearer-secret-789" not in message
    assert "quoted,provider;secret" not in message
    assert "cleanup-secret-000" not in message
    assert "cleanup secret with spaces" not in message
    assert session.exec_calls == [
        {"command": ["printenv", "API_TOKEN"], "cwd": None, "env": env}
    ]
    assert session.destroy_calls == 1


def test_run_sandbox_command_redacts_string_command_from_provider_errors() -> None:
    command = "python -c \"print('leaky-command-string')\""
    session = FakeSession(
        exec_error=RuntimeError(f"provider failed while running {command}")
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(command, client_factory=_client_factory(client))

    message = _assert_secret_not_in_public_exception(exc_info.value, command)
    assert "<redacted>" in message
    assert session.exec_calls == [{"command": command, "cwd": None, "env": None}]


def test_run_sandbox_command_redacts_list_command_from_cleanup_errors() -> None:
    command = ["python", "-c", "print('leaky-list-command')"]
    command_text = str(command)
    command_item = "print('leaky-list-command')"
    session = FakeSession(
        exec_error=RuntimeError("exec failed"),
        destroy_error=RuntimeError(
            f"cleanup failed after {command_text}; binary=python flag=-c "
            f"script={command_item}"
        ),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(command, client_factory=_client_factory(client))

    message = _assert_secret_not_in_public_exception(
        exc_info.value,
        command_text,
        command_item,
    )
    assert "<redacted>" in message
    assert "Cleanup warning" in message
    assert "binary=python" in message
    assert "flag=-c" in message
    assert session.exec_calls == [{"command": command, "cwd": None, "env": None}]
    assert session.destroy_calls == 1


def test_run_sandbox_command_redacts_env_values_from_cleanup_close_failures() -> None:
    secret_value = "close-token-from-tool-env"
    session = FakeSession(close_error=RuntimeError(f"close failed with {secret_value}"))
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(
        KitaruBackendError, match="closing the sandbox session failed"
    ) as exc_info:
        run_sandbox_command(
            "echo hi",
            cleanup="close",
            env={"API_TOKEN": secret_value},
            client_factory=_client_factory(client),
        )

    message = _assert_secret_not_in_public_exception(exc_info.value, secret_value)
    assert message.endswith("close failed with <redacted>")


def test_run_sandbox_command_redacts_env_values_from_backend_failures() -> None:
    secret_value = "static-token-from-tool-env"
    public_value = "demo-mode"
    sandbox = FakeSandbox(
        FakeSession(
            exec_error=RuntimeError(
                f"provider rejected token {secret_value} in mode {public_value}"
            )
        )
    )
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(
            "echo hi",
            env={"API_TOKEN": secret_value, "MODE": public_value},
            client_factory=_client_factory(client),
        )

    message = _assert_secret_not_in_public_exception(
        exc_info.value,
        secret_value,
        public_value,
    )
    assert message.endswith("provider rejected token <redacted> in mode <redacted>")


def test_public_sandbox_imports() -> None:
    import kitaru
    import kitaru.config as config

    assert kitaru.run_sandbox_command is config.run_sandbox_command
    assert kitaru.SandboxCommandResult is config.SandboxCommandResult
    assert "run_sandbox_command" in kitaru.__all__
    assert "SandboxCommandResult" in kitaru.__all__
