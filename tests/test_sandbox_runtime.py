from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
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
        self.kill_calls = 0

    def collect(self, *, max_chars: int) -> FakeOutput:
        self.collect_calls += 1
        self.collect_max_chars.append(max_chars)
        if self.collect_error is not None:
            raise self.collect_error
        return self.output

    def kill(self) -> None:
        self.kill_calls += 1


class BlockingFakeProcess(FakeProcess):
    def __init__(self) -> None:
        super().__init__(FakeOutput(stdout="late"))
        self.kill_event = Event()

    def collect(self, *, max_chars: int) -> FakeOutput:
        self.collect_calls += 1
        self.collect_max_chars.append(max_chars)
        self.kill_event.wait(timeout=1.0)
        return self.output

    def kill(self) -> None:
        super().kill()
        self.kill_event.set()


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
        "timed_out": False,
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


def test_run_sandbox_command_times_out_and_cleans_up() -> None:
    process = BlockingFakeProcess()
    session = FakeSession(process)
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        "sleep 999999",
        timeout_seconds=0.01,
        client_factory=_client_factory(client),
    )

    assert process.collect_calls == 1
    assert process.kill_calls == 1
    assert session.destroy_calls == 1
    assert result.timed_out is True
    assert result.exit_code == -1
    assert "timed out after 0.01 seconds" in result.stderr
    assert result.stdout == ""
    assert result.cleanup_succeeded is True


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


def test_run_sandbox_command_redacts_provider_error_secrets() -> None:
    env_secret = "env-token-value-123"
    non_secret_env_value = "plain env value with spaces"
    quoted_secret = "quoted secret value, with comma"
    basic_auth_token = "basic-token-value-123"
    openai_key = "sk-testtokenvalue1234567890"
    github_token = "github-token-value-123"
    process = FakeProcess(
        collect_error=RuntimeError(
            "provider failed with Authorization: Bearer "
            f"{openai_key}; Authorization: Basic {basic_auth_token}; "
            f"OPENAI_API_KEY={env_secret}; MODE={non_secret_env_value}; "
            f'PASSWORD="{quoted_secret}"; token={github_token}'
        )
    )
    session = FakeSession(process)
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(
            "echo hi",
            env={"OPENAI_API_KEY": env_secret, "MODE": non_secret_env_value},
            client_factory=_client_factory(client),
        )

    message = str(exc_info.value)
    assert "[REDACTED]" in message
    assert env_secret not in message
    assert non_secret_env_value not in message
    assert quoted_secret not in message
    assert basic_auth_token not in message
    assert openai_key not in message
    assert github_token not in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_run_sandbox_command_reports_cleanup_failure_after_command_failure() -> None:
    command_secret = "command-env-value with spaces"
    cleanup_secret = "cleanup-secret-looking-value-123"
    process = FakeProcess(collect_error=RuntimeError("collect failed"))
    session = FakeSession(
        process,
        destroy_error=RuntimeError(
            f"cleanup failed Authorization: Basic {cleanup_secret}"
        ),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(
            "echo hi",
            env={"MODE": command_secret, "CLEANUP_DETAIL": cleanup_secret},
            client_factory=_client_factory(client),
        )

    message = str(exc_info.value)
    assert "Sandbox command execution failed" in message
    assert "collect failed" in message
    assert "Cleanup warning" in message
    assert "Sandbox cleanup after command failure failed" in message
    assert "[REDACTED]" in message
    assert command_secret not in message
    assert cleanup_secret not in message
    assert session.destroy_calls == 1
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_run_sandbox_command_redacts_cleanup_failure_secrets() -> None:
    secret = "cleanup-secret-value-123"
    session = FakeSession(
        destroy_error=RuntimeError(f"destroy failed PASSWORD={secret}")
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command(
            "echo hi",
            env={"PASSWORD": secret},
            client_factory=_client_factory(client),
        )

    message = str(exc_info.value)
    assert "[REDACTED]" in message
    assert secret not in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_unsupported_destroy_after_failure_does_not_promise_result() -> None:
    process = FakeProcess(collect_error=RuntimeError("collect failed"))
    session = FakeSession(
        process,
        destroy_error=NotImplementedError("destroy unsupported"),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    with pytest.raises(KitaruBackendError) as exc_info:
        run_sandbox_command("echo hi", client_factory=_client_factory(client))

    message = str(exc_info.value)
    assert "best-effort close was attempted" in message
    assert "command result is still available" not in message
    assert session.destroy_calls == 1
    assert session.close_calls == 1


def test_run_sandbox_command_redacts_timeout_cleanup_error() -> None:
    secret = "sk-timeoutsecret1234567890"
    process = BlockingFakeProcess()
    session = FakeSession(process, destroy_error=RuntimeError(f"Bearer {secret}"))
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        "sleep 999999",
        timeout_seconds=0.01,
        client_factory=_client_factory(client),
    )

    assert result.cleanup_succeeded is False
    assert result.cleanup_error is not None
    assert "[REDACTED]" in result.cleanup_error
    assert secret not in result.cleanup_error


def test_unsupported_destroy_after_timeout_does_not_promise_result() -> None:
    process = BlockingFakeProcess()
    session = FakeSession(
        process,
        destroy_error=NotImplementedError("destroy unsupported"),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        "sleep 999999",
        timeout_seconds=0.01,
        client_factory=_client_factory(client),
    )

    assert result.cleanup_succeeded is False
    assert result.cleanup_error is not None
    assert "best-effort close was attempted" in result.cleanup_error
    assert "command result is still available" not in result.cleanup_error
    assert session.destroy_calls == 1
    assert session.close_calls == 1


def test_run_sandbox_command_redacts_unsupported_destroy_cleanup_errors() -> None:
    destroy_secret = "destroy-secret-value-123"
    close_secret = "sk-closesecret1234567890"
    session = FakeSession(
        destroy_error=NotImplementedError(f"SECRET={destroy_secret}"),
        close_error=RuntimeError(f"Authorization: Bearer {close_secret}"),
    )
    sandbox = FakeSandbox(session)
    client = FakeClient(active_stack=FakeActiveStack({"local": sandbox}))

    result = run_sandbox_command(
        "echo hi",
        env={"SECRET": destroy_secret},
        client_factory=_client_factory(client),
    )

    assert result.cleanup_succeeded is False
    assert result.cleanup_error is not None
    assert "[REDACTED]" in result.cleanup_error
    assert destroy_secret not in result.cleanup_error
    assert close_secret not in result.cleanup_error


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"command": ""}, "non-empty string"),
        ({"command": []}, "cannot be empty"),
        ({"command": ["echo", 1]}, "non-empty strings"),
        ({"command": "echo", "max_chars": -1}, ">= 0"),
        ({"command": "echo", "max_chars": 1.2}, "integer"),
        ({"command": "echo", "max_chars": True}, "integer"),
        ({"command": "echo", "timeout_seconds": 0}, "greater than 0"),
        ({"command": "echo", "timeout_seconds": float("nan")}, "finite number"),
        ({"command": "echo", "timeout_seconds": True}, "finite number"),
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
    assert "[REDACTED]" in message
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


def test_public_sandbox_imports() -> None:
    import kitaru
    import kitaru.config as config

    assert kitaru.run_sandbox_command is config.run_sandbox_command
    assert kitaru.SandboxCommandResult is config.SandboxCommandResult
    assert "run_sandbox_command" in kitaru.__all__
    assert "SandboxCommandResult" in kitaru.__all__
