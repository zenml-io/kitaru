"""Small bridge around Claude Agent SDK query calls."""

import inspect
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any, cast

from kitaru._llm_usage import coerce_cost_usd
from kitaru.errors import KitaruUsageError

from ._serialization import to_json_safe
from ._transcripts import load_transcript_payload, resolve_claude_transcript_path
from ._types import ClaudeRunRequest
from ._usage import normalize_usage
from ._utils import elapsed_ms

try:
    from claude_agent_sdk import AssistantMessage as _SDK_ASSISTANT_MESSAGE_TYPE
except (ImportError, AttributeError):
    _SDK_ASSISTANT_MESSAGE_TYPE: type[Any] | None = None

try:
    from claude_agent_sdk import ResultMessage as _SDK_RESULT_MESSAGE_TYPE
except (ImportError, AttributeError):
    _SDK_RESULT_MESSAGE_TYPE: type[Any] | None = None

try:
    from claude_agent_sdk import StreamEvent as _SDK_STREAM_EVENT_TYPE
except (ImportError, AttributeError):
    _SDK_STREAM_EVENT_TYPE: type[Any] | None = None


_ALLOWED_ASSISTANT_ERROR_CATEGORIES = frozenset(
    {
        "authentication_failed",
        "billing_error",
        "rate_limit",
        "invalid_request",
        "server_error",
        "unknown",
    }
)


@dataclass(frozen=True)
class ClaudeResultDiagnostic:
    """Allowlisted diagnostics for a failed Claude SDK result."""

    assistant_error: str | None
    api_error_status: int | None
    subtype: str | None

    @classmethod
    def from_result_message(
        cls,
        final_message: Any,
        *,
        assistant_error: Any = None,
    ) -> "ClaudeResultDiagnostic":
        """Extract only safe diagnostic fields from an SDK failure."""
        return cls(
            assistant_error=_assistant_error_or_none(assistant_error),
            api_error_status=_api_error_status_or_none(
                getattr(final_message, "api_error_status", None)
            ),
            subtype=_diagnostic_subtype_or_none(
                getattr(final_message, "subtype", None)
            ),
        )

    def as_metadata(self) -> dict[str, str | int | None]:
        """Return the stable diagnostic metadata representation."""
        return {
            "assistant_error": self.assistant_error,
            "api_error_status": self.api_error_status,
            "subtype": self.subtype,
        }


@dataclass(frozen=True)
class ClaudeInvocationPayload:
    """Internal, pre-result payload extracted from one SDK invocation."""

    session_id: str | None
    final_text: str | None
    messages: list[Any]
    transcript_path: str | None
    transcript_payload: dict[str, Any] | None
    usage: dict[str, Any] | None
    cost_usd: float | None
    model_usage: dict[str, Any] | None
    stop_reason: str | None
    subtype: str | None
    num_turns: int | None
    duration_ms: float
    duration_api_ms: float | None = None
    warnings: list[str] = field(default_factory=list)


def claude_agent_sdk_version() -> str:
    """Return the installed Claude Agent SDK version when discoverable."""
    try:
        return metadata.version("claude-agent-sdk")
    except metadata.PackageNotFoundError:
        return "unknown"


async def run_claude_invocation(
    *,
    request: ClaudeRunRequest,
    options: Any | None,
    save_transcript_file: bool,
) -> ClaudeInvocationPayload:
    """Execute ``claude_agent_sdk.query(...)`` and extract boundary fields."""
    return await _run_claude_invocation(
        request=request,
        options=options,
        save_transcript_file=save_transcript_file,
        on_event=None,
        store_stream_events=True,
    )


async def run_claude_invocation_streamed(
    *,
    request: ClaudeRunRequest,
    options: Any | None,
    save_transcript_file: bool,
    on_event: Callable[[Any], Any] | None = None,
) -> ClaudeInvocationPayload:
    """Execute ``query(...)`` while forwarding yielded SDK messages live."""
    return await _run_claude_invocation(
        request=request,
        options=options,
        save_transcript_file=save_transcript_file,
        on_event=on_event,
        store_stream_events=False,
    )


async def _run_claude_invocation(
    *,
    request: ClaudeRunRequest,
    options: Any | None,
    save_transcript_file: bool,
    on_event: Callable[[Any], Any] | None,
    store_stream_events: bool,
) -> ClaudeInvocationPayload:
    import time

    from claude_agent_sdk import query

    started_at = time.perf_counter()
    messages: list[Any] = []
    final_message: Any | None = None
    latest_assistant_error: str | None = None

    query_result = query(prompt=request.prompt, options=options)
    if inspect.isawaitable(query_result):
        query_result = await query_result
    messages_iterable = cast(AsyncIterable[Any], query_result)

    async for message in messages_iterable:
        if on_event is not None:
            await _notify_stream_callback(on_event, message)
        if store_stream_events or not is_stream_event(message):
            messages.append(to_json_safe(message))
        if is_assistant_message(message):
            assistant_error = _assistant_error_or_none(getattr(message, "error", None))
            if assistant_error is not None:
                latest_assistant_error = assistant_error
        if is_result_message(message):
            final_message = message

    if final_message is None:
        raise RuntimeError("Claude Agent SDK did not return a final ResultMessage.")
    if bool(getattr(final_message, "is_error", False)):
        raise ClaudeResultMessageError(
            final_message,
            assistant_error=latest_assistant_error,
        )

    session_id = _string_or_none(getattr(final_message, "session_id", None))
    transcript_path: str | None = None
    transcript_payload: dict[str, Any] | None = None
    warnings: list[str] = []
    if save_transcript_file:
        transcript_path, transcript_payload, transcript_warnings = _load_transcript(
            session_id=session_id,
            request=request,
            options=options,
        )
        warnings.extend(transcript_warnings)

    sdk_duration_ms = _float_or_none(getattr(final_message, "duration_ms", None))
    return ClaudeInvocationPayload(
        session_id=session_id,
        final_text=_string_or_none(getattr(final_message, "result", None)),
        messages=messages,
        transcript_path=transcript_path,
        transcript_payload=transcript_payload,
        usage=normalize_usage(getattr(final_message, "usage", None)),
        cost_usd=_cost_usd_or_none(
            getattr(final_message, "total_cost_usd", None), warnings
        ),
        model_usage=normalize_usage(getattr(final_message, "model_usage", None)),
        stop_reason=_string_or_none(getattr(final_message, "stop_reason", None)),
        subtype=_string_or_none(getattr(final_message, "subtype", None)),
        num_turns=_int_or_none(getattr(final_message, "num_turns", None)),
        duration_ms=sdk_duration_ms
        if sdk_duration_ms is not None
        else elapsed_ms(started_at),
        duration_api_ms=_float_or_none(getattr(final_message, "duration_api_ms", None)),
        warnings=warnings,
    )


async def _notify_stream_callback(on_event: Callable[[Any], Any], message: Any) -> None:
    try:
        callback_result = on_event(message)
        if inspect.isawaitable(callback_result):
            await callback_result
    except Exception:
        return


def is_assistant_message(message: Any) -> bool:
    """Return whether a message looks like Claude's AssistantMessage."""
    if _SDK_ASSISTANT_MESSAGE_TYPE is not None and isinstance(
        message, _SDK_ASSISTANT_MESSAGE_TYPE
    ):
        return True
    return type(message).__name__ == "AssistantMessage"


def is_result_message(message: Any) -> bool:
    """Return whether ``message`` looks like Claude's final ``ResultMessage``."""
    if _SDK_RESULT_MESSAGE_TYPE is not None and isinstance(
        message, _SDK_RESULT_MESSAGE_TYPE
    ):
        return True
    return type(message).__name__ == "ResultMessage"


def is_stream_event(message: Any) -> bool:
    """Return whether ``message`` looks like Claude's raw partial stream event."""
    if _SDK_STREAM_EVENT_TYPE is not None and isinstance(
        message, _SDK_STREAM_EVENT_TYPE
    ):
        return True
    return type(message).__name__ == "StreamEvent"


class ClaudeResultMessageError(RuntimeError):
    """Claude SDK final error containing only allowlisted diagnostics."""

    def __init__(self, final_message: Any, *, assistant_error: Any = None) -> None:
        self.diagnostic = ClaudeResultDiagnostic.from_result_message(
            final_message,
            assistant_error=assistant_error,
        )
        message = format_result_error(self.diagnostic)
        self.safe_live_message = message
        super().__init__(message)


def format_result_error(diagnostic: ClaudeResultDiagnostic) -> str:
    """Format a failed Claude result without content-bearing SDK fields."""
    parts = ["Claude Agent SDK returned an error ResultMessage"]
    if diagnostic.assistant_error is not None:
        parts.append(f"assistant_error={diagnostic.assistant_error!r}")
    if diagnostic.api_error_status is not None:
        parts.append(f"api_error_status={diagnostic.api_error_status}")
    if diagnostic.subtype is not None:
        parts.append(f"subtype={diagnostic.subtype!r}")
    return "; ".join(parts)


def _assistant_error_or_none(value: Any) -> str | None:
    if type(value) is str and value in _ALLOWED_ASSISTANT_ERROR_CATEGORIES:
        return value
    return None


def _api_error_status_or_none(value: Any) -> int | None:
    if type(value) is int:
        return value
    return None


def _diagnostic_subtype_or_none(value: Any) -> str | None:
    if type(value) is str:
        return value
    return None


def _load_transcript(
    *,
    session_id: str | None,
    request: ClaudeRunRequest,
    options: Any | None,
) -> tuple[str | None, dict[str, Any] | None, list[str]]:
    if not session_id:
        return (
            None,
            None,
            [
                "Claude transcript capture was requested, but the final result "
                "had no session_id."
            ],
        )
    cwd = _resolve_effective_cwd(request=request, options=options)
    try:
        transcript_path = resolve_claude_transcript_path(session_id, cwd=cwd)
    except ValueError as exc:
        return None, None, [str(exc)]
    payload, warnings = load_transcript_payload(transcript_path)
    return transcript_path, payload, warnings


def _resolve_effective_cwd(*, request: ClaudeRunRequest, options: Any | None) -> str:
    options_cwd = _extract_cwd_from_options(options)
    if options_cwd:
        return options_cwd

    if request.cwd:
        return request.cwd

    return str(Path.cwd())


def _extract_cwd_from_options(options: Any | None) -> str | None:
    if options is None:
        return None

    if isinstance(options, dict):
        value = options.get("cwd")
    else:
        value = getattr(options, "cwd", None)

    if value is None:
        return None

    return str(value)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _cost_usd_or_none(value: Any, warnings: list[str]) -> float | None:
    if value is None:
        return None
    cost = coerce_cost_usd(value)
    if cost is None:
        warnings.append(
            "Claude Agent SDK returned an invalid total_cost_usd; falling back "
            "to the configured cost calculator or genai-prices."
        )
    return cost


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise KitaruUsageError(
            f"Could not coerce Claude numeric field {value!r}."
        ) from exc


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise KitaruUsageError(
            f"Could not coerce Claude integer field {value!r}."
        ) from exc
