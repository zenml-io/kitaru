"""Sandbox session support for Kitaru.

Monty-backed sandbox sessions provide stateful Python execution that can pause
and resume around ``kitaru.wait()``.
"""

import base64
import re
import time
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from zenml.client import Client
from zenml.enums import MetadataResourceTypes
from zenml.models.v2.misc.run_metadata import RunMetadataResource

from kitaru.artifacts import save
from kitaru.checkpoint import checkpoint
from kitaru.config import ResolvedSandboxConfig, SandboxProviderKind
from kitaru.errors import (
    KitaruContextError,
    KitaruFeatureNotAvailableError,
    KitaruSandboxCapabilityError,
    KitaruSandboxExecutionError,
    KitaruSandboxNotConfiguredError,
    KitaruSandboxProviderError,
    KitaruSandboxSessionError,
)
from kitaru.logging import log
from kitaru.runtime import (
    _get_current_checkpoint,
    _get_current_execution_id,
    _get_current_flow,
    _get_current_sandbox_config,
    _get_current_sandbox_manager,
    _is_inside_checkpoint,
    _is_inside_flow,
    _next_llm_call_name,
    _set_current_sandbox_manager,
)

_SANDBOX_STATE_METADATA_KEY = "kitaru_sandbox_state"
_LOCAL_EXECUTION_SCOPE_ID = "__local_execution__"
_LOCAL_CHECKPOINT_SCOPE_ID = "__local_checkpoint__"
_SESSION_NAME_PATTERN = re.compile(r"\W+")


class SandboxCapabilities(BaseModel):
    """Feature flags exposed by a sandbox provider."""

    supports_shell: bool
    supports_filesystem: bool
    supports_network: bool
    supports_stateful_python: bool
    supports_pause_resume: bool
    supports_snapshots: bool


class SandboxExecutionResult(BaseModel):
    """Structured result returned from sandbox execution."""

    call_name: str
    provider: SandboxProviderKind
    session_name: str
    value: Any | None = None
    stdout: str = ""
    stderr: str = ""
    duration_ms: float
    stateful: bool = True

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _SandboxBackendSession(Protocol):
    """Provider-specific backend session interface."""

    def run_code(
        self,
        code: str,
        *,
        inputs: Mapping[str, Any] | None = None,
    ) -> SandboxExecutionResult: ...

    def run_command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> SandboxExecutionResult: ...

    def dump(self) -> bytes | None: ...

    def close(self) -> None: ...


class _SandboxProvider(Protocol):
    """Internal provider registry contract."""

    def capabilities(self) -> SandboxCapabilities: ...

    def open_session(
        self,
        *,
        session_name: str,
        scope: Literal["execution", "checkpoint"],
        config: ResolvedSandboxConfig,
        restored_snapshot: bytes | None = None,
    ) -> _SandboxBackendSession: ...


@dataclass
class _SandboxSessionRecord:
    """Internal tracked state for one session."""

    scope: Literal["execution", "checkpoint"]
    scope_id: str
    session_name: str
    backend: _SandboxBackendSession | None
    status: Literal["active", "paused", "closed"]
    call_index: int = 0
    snapshot: bytes | None = None


class SandboxSession:
    """Public session handle used inside flows and checkpoints."""

    def __init__(
        self,
        *,
        manager: "_SandboxRuntimeManager",
        name: str,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
    ) -> None:
        self.name = name
        self.scope = scope
        self._scope_id = scope_id
        self._manager = manager

    @property
    def capabilities(self) -> SandboxCapabilities:
        """Return the provider capability flags for this session."""
        return self._manager.capabilities

    def run_code(
        self,
        code: str,
        *,
        inputs: Mapping[str, Any] | None = None,
        name: str | None = None,
    ) -> SandboxExecutionResult:
        """Execute Python code in the sandbox session."""
        call_name = _normalize_call_name(name) or _next_llm_call_name("sandbox")
        if _is_inside_checkpoint():
            return self._manager.run_code(
                scope=self.scope,
                scope_id=self._scope_id,
                session_name=self.name,
                code=code,
                inputs=inputs,
                call_name=call_name,
            )

        request = _SandboxCodeRequest(
            scope=self.scope,
            scope_id=self._scope_id,
            session_name=self.name,
            code=code,
            inputs=dict(inputs or {}),
            call_name=call_name,
        )
        result = _sandbox_run_code_checkpoint_call(request)
        if isinstance(result, SandboxExecutionResult):
            return result

        load = getattr(result, "load", None)
        if callable(load):
            result = load()

        return SandboxExecutionResult.model_validate(result)

    def run_command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> SandboxExecutionResult:
        """Execute a shell command in the sandbox session."""
        return self._manager.run_command(
            scope=self.scope,
            scope_id=self._scope_id,
            session_name=self.name,
            command=command,
            cwd=cwd,
            env=env,
        )

    def pause(self) -> None:
        """Pause this sandbox session."""
        self._manager.pause_session(
            scope=self.scope,
            scope_id=self._scope_id,
            session_name=self.name,
        )

    def resume(self) -> None:
        """Resume this sandbox session if it is paused."""
        self._manager.resume_session(
            scope=self.scope,
            scope_id=self._scope_id,
            session_name=self.name,
        )

    def close(self) -> None:
        """Close this sandbox session and mark the handle unusable."""
        self._manager.close_session(
            scope=self.scope,
            scope_id=self._scope_id,
            session_name=self.name,
        )


class _SandboxCodeRequest(BaseModel):
    """Synthetic-checkpoint payload for flow-scope sandbox calls."""

    scope: Literal["execution", "checkpoint"]
    scope_id: str
    session_name: str
    code: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    call_name: str


class _MontyBackendSession:
    """Monty-backed stateful Python REPL session."""

    def __init__(
        self,
        *,
        session_name: str,
        config: ResolvedSandboxConfig,
        restored_snapshot: bytes | None = None,
    ) -> None:
        monty_module = _require_monty()
        limits = {
            "max_duration_secs": config.monty.max_duration_secs,
            "max_memory_mb": config.monty.max_memory_mb,
        }
        self._session_name = session_name
        self._config = config
        self._repl: Any
        if restored_snapshot is not None:
            self._repl = monty_module.MontyRepl.load(restored_snapshot)
        else:
            self._repl = monty_module.MontyRepl(limits=limits)
        self._runtime_error = monty_module.MontyRuntimeError
        self._syntax_error = monty_module.MontySyntaxError
        self._typing_error = monty_module.MontyTypingError

    def run_code(
        self,
        code: str,
        *,
        inputs: Mapping[str, Any] | None = None,
    ) -> SandboxExecutionResult:
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        def _print_callback(stream: str, chunk: str) -> None:
            if stream == "stderr":
                stderr_chunks.append(chunk)
            else:
                stdout_chunks.append(chunk)

        started_at = time.perf_counter()
        try:
            value = self._repl.feed_run(
                code,
                inputs=dict(inputs) if inputs is not None else None,
                print_callback=_print_callback,
            )
        except Exception as exc:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            display = _monty_error_display(exc)
            error_message = display or str(exc)
            if isinstance(
                exc,
                (
                    self._runtime_error,
                    self._syntax_error,
                    self._typing_error,
                ),
            ):
                raise KitaruSandboxExecutionError(error_message) from exc
            raise KitaruSandboxProviderError(error_message) from exc

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        return SandboxExecutionResult(
            call_name="sandbox",
            provider=SandboxProviderKind.MONTY,
            session_name=self._session_name,
            value=value,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
            duration_ms=duration_ms,
            stateful=True,
        )

    def run_command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> SandboxExecutionResult:
        del cwd, env
        raise KitaruSandboxCapabilityError(
            "The Monty sandbox provider supports Python code execution only. "
            f"It cannot run shell commands like {command!r}."
        )

    def dump(self) -> bytes | None:
        """Serialize the in-memory REPL state."""
        try:
            return cast(bytes, self._repl.dump())
        except Exception as exc:
            raise KitaruSandboxProviderError(
                f"Failed to snapshot Monty sandbox state: {exc}"
            ) from exc

    def close(self) -> None:
        """Close the session handle."""
        self._repl = None


class _MontyProvider:
    """Internal Monty provider entrypoint."""

    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            supports_shell=False,
            supports_filesystem=False,
            supports_network=False,
            supports_stateful_python=True,
            supports_pause_resume=True,
            supports_snapshots=True,
        )

    def open_session(
        self,
        *,
        session_name: str,
        scope: Literal["execution", "checkpoint"],
        config: ResolvedSandboxConfig,
        restored_snapshot: bytes | None = None,
    ) -> _SandboxBackendSession:
        del scope
        return _MontyBackendSession(
            session_name=session_name,
            config=config,
            restored_snapshot=restored_snapshot,
        )


_PROVIDER_REGISTRY: dict[SandboxProviderKind, _SandboxProvider] = {
    SandboxProviderKind.MONTY: _MontyProvider(),
}


class _SandboxRuntimeManager:
    """Owns live sandbox sessions for the current flow execution."""

    def __init__(
        self,
        config: ResolvedSandboxConfig,
        execution_id: str | None,
    ) -> None:
        self._config = config
        self._execution_id = execution_id
        self._provider = _provider_for_config(config)
        self._records: dict[tuple[str, str, str], _SandboxSessionRecord] = {}

    @property
    def capabilities(self) -> SandboxCapabilities:
        return self._provider.capabilities()

    def open_session_handle(
        self,
        *,
        name: str,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
    ) -> SandboxSession:
        self._ensure_record(scope=scope, scope_id=scope_id, session_name=name)
        return SandboxSession(
            manager=self,
            name=name,
            scope=scope,
            scope_id=scope_id,
        )

    def run_code(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
        code: str,
        inputs: Mapping[str, Any] | None,
        call_name: str | None,
    ) -> SandboxExecutionResult:
        record = self._ensure_record(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        if record.status == "paused":
            self._restore_record(record)
        if record.status == "closed":
            raise KitaruSandboxSessionError(
                f"Sandbox session {session_name!r} is closed."
            )
        if record.backend is None:
            raise KitaruSandboxSessionError(
                f"Sandbox session {session_name!r} is not available."
            )

        record.call_index += 1
        resolved_call_name = call_name or _next_llm_call_name("sandbox")
        input_artifact = _save_with_fallback(
            f"{session_name}_{resolved_call_name}_input",
            {"code": code, "inputs": dict(inputs or {})},
            artifact_type="input",
        )
        try:
            result = record.backend.run_code(code, inputs=inputs)
            result.call_name = resolved_call_name
            summary = {
                "provider": self._config.provider,
                "session_name": session_name,
                "scope": scope,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration_ms": result.duration_ms,
                "stateful": result.stateful,
                "status": "completed",
                "input_artifact": input_artifact,
            }
            if result.value is not None:
                _save_with_fallback(
                    f"{session_name}_{resolved_call_name}_value",
                    result.value,
                    artifact_type="output",
                )
            _save_with_fallback(
                f"{session_name}_{resolved_call_name}_summary",
                summary,
                artifact_type="blob",
            )
            log(
                sandbox_calls={
                    resolved_call_name: {
                        "provider": self._config.provider,
                        "session_name": session_name,
                        "scope": scope,
                        "duration_ms": result.duration_ms,
                        "status": "completed",
                        "stdout_chars": len(result.stdout),
                        "stderr_chars": len(result.stderr),
                    }
                }
            )
            self._persist_record(record)
            return result
        except Exception as exc:
            summary = {
                "provider": self._config.provider,
                "session_name": session_name,
                "scope": scope,
                "stdout": "",
                "stderr": str(exc),
                "duration_ms": None,
                "stateful": True,
                "status": "failed",
                "input_artifact": input_artifact,
            }
            _save_with_fallback(
                f"{session_name}_{resolved_call_name}_summary",
                summary,
                artifact_type="blob",
            )
            log(
                sandbox_calls={
                    resolved_call_name: {
                        "provider": self._config.provider,
                        "session_name": session_name,
                        "scope": scope,
                        "duration_ms": None,
                        "status": "failed",
                        "stdout_chars": 0,
                        "stderr_chars": len(str(exc)),
                    }
                }
            )
            raise

    def run_command(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
        command: str,
        cwd: str | None,
        env: Mapping[str, str] | None,
    ) -> SandboxExecutionResult:
        record = self._ensure_record(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        if record.backend is None:
            raise KitaruSandboxSessionError(
                f"Sandbox session {session_name!r} is not available."
            )
        return record.backend.run_command(command, cwd=cwd, env=env)

    def pause_session(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
    ) -> None:
        record = self._ensure_record(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        self._pause_record(record)

    def resume_session(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
    ) -> None:
        record = self._ensure_record(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        self._restore_record(record)

    def close_session(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
    ) -> None:
        record = self._ensure_record(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        self._close_record(record)

    def close_checkpoint_scope(self, checkpoint_id: str | None) -> None:
        resolved_checkpoint_id = checkpoint_id or _LOCAL_CHECKPOINT_SCOPE_ID
        for key, record in list(self._records.items()):
            if (
                record.scope == "checkpoint"
                and record.scope_id == resolved_checkpoint_id
            ):
                self._close_record(record)
                self._records.pop(key, None)

    def close_execution_scope(self, execution_id: str | None) -> None:
        resolved_execution_id = execution_id or _LOCAL_EXECUTION_SCOPE_ID
        for key, record in list(self._records.items()):
            if record.scope == "execution" and record.scope_id == resolved_execution_id:
                self._close_record(record)
                self._records.pop(key, None)

    def before_wait(self, execution_id: str | None) -> None:
        resolved_execution_id = execution_id or _LOCAL_EXECUTION_SCOPE_ID
        for record in self._records.values():
            if record.scope == "execution" and record.scope_id == resolved_execution_id:
                self._pause_record(record)

    def after_wait(self, execution_id: str | None) -> None:
        resolved_execution_id = execution_id or _LOCAL_EXECUTION_SCOPE_ID
        for record in self._records.values():
            if record.scope == "execution" and record.scope_id == resolved_execution_id:
                self._restore_record(record)

    def _record_key(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
    ) -> tuple[str, str, str]:
        return scope, scope_id, session_name

    def _ensure_record(
        self,
        *,
        scope: Literal["execution", "checkpoint"],
        scope_id: str,
        session_name: str,
    ) -> _SandboxSessionRecord:
        key = self._record_key(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
        )
        record = self._records.get(key)
        if record is not None:
            if record.status == "closed":
                raise KitaruSandboxSessionError(
                    f"Sandbox session {session_name!r} is closed."
                )
            return record

        restored_snapshot: bytes | None = None
        call_index = 0
        if scope == "execution":
            persisted = self._load_persisted_session_state(session_name)
            if persisted is not None:
                restored_snapshot = _decode_snapshot(persisted.get("snapshot"))
                call_index = int(persisted.get("call_index") or 0)

        backend = self._provider.open_session(
            session_name=session_name,
            scope=scope,
            config=self._config,
            restored_snapshot=restored_snapshot,
        )
        record = _SandboxSessionRecord(
            scope=scope,
            scope_id=scope_id,
            session_name=session_name,
            backend=backend,
            status="active",
            call_index=call_index,
        )
        self._records[key] = record
        self._persist_record(record)
        return record

    def _pause_record(self, record: _SandboxSessionRecord) -> None:
        if record.status == "closed" or record.status == "paused":
            return
        if record.backend is not None:
            record.snapshot = record.backend.dump()
            record.backend.close()
            record.backend = None
        record.status = "paused"
        self._persist_record(record)

    def _restore_record(self, record: _SandboxSessionRecord) -> None:
        if record.status == "closed":
            raise KitaruSandboxSessionError(
                f"Sandbox session {record.session_name!r} is closed."
            )
        if record.status == "active":
            return
        if record.snapshot is None and record.scope == "execution":
            persisted = self._load_persisted_session_state(record.session_name)
            if persisted is not None:
                record.snapshot = _decode_snapshot(persisted.get("snapshot"))
                record.call_index = int(
                    persisted.get("call_index") or record.call_index
                )
        record.backend = self._provider.open_session(
            session_name=record.session_name,
            scope=record.scope,
            config=self._config,
            restored_snapshot=record.snapshot,
        )
        record.snapshot = None
        record.status = "active"
        self._persist_record(record)

    def _close_record(self, record: _SandboxSessionRecord) -> None:
        if record.status == "closed":
            return
        if record.backend is not None:
            record.backend.close()
        record.backend = None
        record.snapshot = None
        record.status = "closed"
        self._persist_record(record)

    def _load_persisted_session_state(self, session_name: str) -> dict[str, Any] | None:
        state = _load_execution_state(self._execution_id)
        sessions = state.get("sessions", {})
        raw_session = sessions.get(session_name)
        if isinstance(raw_session, dict):
            return raw_session
        return None

    def _persist_record(self, record: _SandboxSessionRecord) -> None:
        if record.scope != "execution":
            return
        state = _load_execution_state(self._execution_id)
        sessions = dict(state.get("sessions", {}))
        sessions[record.session_name] = {
            "provider": self._config.provider,
            "status": record.status,
            "scope": record.scope,
            "call_index": record.call_index,
            "encoding": "zlib+base64",
            "snapshot": _encode_snapshot(record.snapshot),
        }
        state["version"] = 1
        state["sessions"] = sessions
        _persist_execution_state(self._execution_id, state)


@checkpoint(type="sandbox_call")
def _sandbox_run_code_checkpoint_call(
    request: _SandboxCodeRequest,
) -> dict[str, Any]:
    """Execute a flow-scope sandbox call inside a synthetic checkpoint."""
    manager = _require_runtime_manager()
    return manager.run_code(
        scope=request.scope,
        scope_id=request.scope_id,
        session_name=request.session_name,
        code=request.code,
        inputs=request.inputs,
        call_name=request.call_name,
    ).model_dump(mode="json")


def _require_monty() -> Any:
    """Import the optional Monty dependency with a helpful install hint."""
    try:
        import pydantic_monty

        return pydantic_monty
    except ImportError as exc:
        raise KitaruFeatureNotAvailableError(
            "Monty sandbox support requires the optional sandbox dependency. "
            "Install it with `uv sync --extra sandbox`."
        ) from exc


def _provider_for_config(config: ResolvedSandboxConfig) -> _SandboxProvider:
    """Resolve the registered provider implementation for a config."""
    provider = _PROVIDER_REGISTRY.get(config.provider)
    if provider is None:
        raise KitaruSandboxProviderError(
            f"No sandbox provider is registered for {config.provider.value!r}."
        )
    return provider


def _ensure_runtime_manager() -> _SandboxRuntimeManager:
    """Return the active runtime sandbox manager, creating it lazily."""
    existing = _get_current_sandbox_manager()
    if existing is not None:
        return cast(_SandboxRuntimeManager, existing)
    config = _get_current_sandbox_config()
    if config is None:
        raise KitaruSandboxNotConfiguredError(
            "No sandbox provider is configured for this flow execution. Configure "
            "one with `kitaru.configure(sandbox=...)`, `KITARU_SANDBOX`, or "
            "`kitaru sandbox set ...`."
        )
    manager = _SandboxRuntimeManager(config, _get_current_execution_id())
    _set_current_sandbox_manager(manager)
    return manager


def _require_runtime_manager() -> _SandboxRuntimeManager:
    """Return the active runtime manager or raise a clear context error."""
    if not _is_inside_flow():
        raise KitaruContextError("sandbox() can only run inside a @flow.")
    return _ensure_runtime_manager()


def _normalize_session_name(name: str | None) -> str:
    """Normalize user-provided session names into stable identifiers."""
    candidate = "sandbox" if name is None else name.strip()
    if not candidate:
        raise KitaruSandboxSessionError("Sandbox session name cannot be empty.")
    normalized = _SESSION_NAME_PATTERN.sub("_", candidate).strip("_")
    if not normalized:
        raise KitaruSandboxSessionError("Sandbox session name cannot be empty.")
    return normalized


def _normalize_call_name(name: str | None) -> str | None:
    """Normalize an optional sandbox call name."""
    if name is None:
        return None
    candidate = name.strip()
    if not candidate:
        raise KitaruSandboxSessionError("Sandbox call name cannot be empty.")
    normalized = _SESSION_NAME_PATTERN.sub("_", candidate).strip("_")
    if not normalized:
        raise KitaruSandboxSessionError("Sandbox call name cannot be empty.")
    return normalized


def _save_with_fallback(name: str, value: Any, *, artifact_type: str) -> str:
    """Persist an artifact, falling back to repr when serialization fails."""
    try:
        save(name, value, type=artifact_type)
        return name
    except Exception:
        fallback_name = f"{name}_repr"
        save(fallback_name, repr(value), type="blob")
        return fallback_name


def _monty_error_display(error: Exception) -> str | None:
    """Render a Monty exception into human-readable text when possible."""
    display = getattr(error, "display", None)
    if not callable(display):
        return None
    for args in [(), ("traceback",), ("full",), ("msg",)]:
        try:
            rendered = display(*args)
        except Exception:
            continue
        if isinstance(rendered, str) and rendered.strip():
            return rendered
    return None


def _encode_snapshot(snapshot: bytes | None) -> str:
    """Compress and encode a snapshot for metadata storage."""
    if snapshot is None:
        return ""
    compressed = zlib.compress(snapshot)
    return base64.b64encode(compressed).decode("ascii")


def _decode_snapshot(snapshot: Any) -> bytes | None:
    """Decode a snapshot from metadata storage."""
    if not isinstance(snapshot, str) or not snapshot:
        return None
    try:
        compressed = base64.b64decode(snapshot.encode("ascii"))
        return zlib.decompress(compressed)
    except Exception as exc:
        raise KitaruSandboxSessionError(
            "Stored sandbox snapshot could not be restored."
        ) from exc


def _load_execution_state(execution_id: str | None) -> dict[str, Any]:
    """Load persisted execution-scoped sandbox state from run metadata."""
    if execution_id is None:
        return {"version": 1, "sessions": {}}
    try:
        run = Client().get_pipeline_run(
            name_id_or_prefix=execution_id,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception:
        return {"version": 1, "sessions": {}}
    metadata = getattr(run, "run_metadata", {}) or {}
    state = metadata.get(_SANDBOX_STATE_METADATA_KEY)
    if isinstance(state, dict):
        return {
            "version": state.get("version", 1),
            "sessions": dict(state.get("sessions", {})),
        }
    return {"version": 1, "sessions": {}}


def _persist_execution_state(execution_id: str | None, state: dict[str, Any]) -> None:
    """Persist execution-scoped sandbox state to pipeline-run metadata."""
    if execution_id is None:
        return
    try:
        run_uuid = UUID(str(execution_id))
    except ValueError:
        return
    Client().create_run_metadata(
        metadata={_SANDBOX_STATE_METADATA_KEY: state},
        resources=[
            RunMetadataResource(
                id=run_uuid,
                type=MetadataResourceTypes.PIPELINE_RUN,
            )
        ],
    )


def sandbox(*, name: str | None = None) -> SandboxSession:
    """Return a stateful sandbox session for the current flow/checkpoint."""
    if not _is_inside_flow():
        raise KitaruContextError("sandbox() can only run inside a @flow.")
    manager = _ensure_runtime_manager()
    normalized_name = _normalize_session_name(name)
    if _is_inside_checkpoint():
        checkpoint_scope = _get_current_checkpoint()
        scope = "checkpoint"
        scope_id = (
            checkpoint_scope.checkpoint_id
            if (
                checkpoint_scope is not None
                and checkpoint_scope.checkpoint_id is not None
            )
            else _LOCAL_CHECKPOINT_SCOPE_ID
        )
    else:
        flow_scope = _get_current_flow()
        scope = "execution"
        scope_id = (
            flow_scope.execution_id
            if flow_scope is not None and flow_scope.execution_id is not None
            else _LOCAL_EXECUTION_SCOPE_ID
        )
    return manager.open_session_handle(
        name=normalized_name,
        scope=scope,
        scope_id=scope_id,
    )


def run_sandbox_smoke_test(config: ResolvedSandboxConfig) -> None:
    """Run a minimal provider smoke test outside flow runtime context."""
    provider = _provider_for_config(config)
    backend = provider.open_session(
        session_name="sandbox_test",
        scope="execution",
        config=config,
    )
    try:
        backend.run_code("counter = 40")
        snapshot = backend.dump()
        backend.close()
        backend = provider.open_session(
            session_name="sandbox_test",
            scope="execution",
            config=config,
            restored_snapshot=snapshot,
        )
        result = backend.run_code("counter + 2")
        if result.value != 42:
            raise KitaruSandboxProviderError(
                f"Sandbox smoke test expected `42` after resume, got {result.value!r}."
            )
    finally:
        backend.close()


__all__ = [
    "SandboxCapabilities",
    "SandboxExecutionResult",
    "SandboxSession",
    "sandbox",
]
