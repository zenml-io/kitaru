"""Execution live-event watching helpers."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from zenml.analytics import source_context
from zenml.constants import API, VERSION_1

from kitaru._client._coercion import optional_string
from kitaru._client._models import ExecutionEvent
from kitaru._source_aliases import normalize_checkpoint_name
from kitaru.errors import KitaruBackendError, KitaruFeatureNotAvailableError

logger = logging.getLogger(__name__)

_DEFAULT_RECONNECT_BACKOFF_SECONDS = (0.1, 0.5, 1.0)
_MAX_STREAM_ERROR_BODY_BYTES = 4096
_MAX_STREAM_ERROR_DETAIL_CHARS = 240

QueryParams = list[tuple[str, str]]
OpenStream = Callable[[str | None], "_StreamingResponse"]


@runtime_checkable
class _StreamingResponse(Protocol):
    """Small response protocol needed by the SSE watcher."""

    status_code: int
    text: str

    def iter_content(
        self,
        chunk_size: int = 1,
        decode_unicode: bool = False,
    ) -> Iterable[bytes | str]:
        """Yield response body chunks."""

    def iter_lines(self, *, decode_unicode: bool = False) -> Iterable[str | bytes]:
        """Yield response lines."""

    def close(self) -> None:
        """Close the response body."""


@runtime_checkable
class StreamingSession(Protocol):
    """Small session protocol used to open a streaming request."""

    def get(
        self,
        url: str,
        *,
        params: QueryParams,
        headers: dict[str, str],
        stream: bool,
        timeout: Any,
        verify: Any,
    ) -> _StreamingResponse:
        """Open a GET request."""


@runtime_checkable
class StreamingStore(Protocol):
    """Small RestZenStore protocol used by the watcher transport."""

    url: str
    session: StreamingSession
    config: Any

    def authenticate(self, force: bool = False) -> None:
        """Authenticate the store session."""


class _NonReconnectableStreamError(Exception):
    """Raised when reconnecting would hide a stream protocol/data problem."""


@dataclass
class SSEFrame:
    """One parsed SSE frame."""

    event: str | None = None
    data_lines: list[str] = field(default_factory=list)
    cursor: str | None = None

    @property
    def data(self) -> str:
        """Return the frame data with SSE multiline semantics."""
        return "\n".join(self.data_lines)

    @property
    def is_empty(self) -> bool:
        """Whether the frame only contained comments/blank lines."""
        return self.event is None and not self.data_lines and self.cursor is None


def parse_sse_lines(lines: Iterable[str | bytes]) -> Iterator[SSEFrame]:
    """Parse raw SSE lines into frames.

    Heartbeat/comment-only frames are ignored. Multiline ``data:`` fields are
    joined with newline characters, matching the SSE specification.
    """
    frame = SSEFrame()

    for raw_line in lines:
        line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line

        line = line.rstrip("\r\n")
        if line == "":
            if not frame.is_empty:
                yield frame
            frame = SSEFrame()
            continue

        if line.startswith(":"):
            continue

        field, separator, value = line.partition(":")
        value = (value[1:] if value.startswith(" ") else value) if separator else ""

        if field == "event":
            frame.event = value
        elif field == "data":
            frame.data_lines.append(value)
        elif field == "id":
            frame.cursor = value or None
        else:
            # Ignore retry and forward-compatible fields.
            continue

    if not frame.is_empty:
        yield frame


def open_rest_sse_stream(
    store: StreamingStore,
    *,
    path: str,
    params: QueryParams,
    last_event_id: str | None,
) -> _StreamingResponse:
    """Open an authenticated REST-backed SSE stream."""
    response = _open_rest_sse_stream_once(
        store,
        path=path,
        params=params,
        last_event_id=last_event_id,
        force_auth=False,
    )
    if response.status_code != 401:
        return response

    response.close()
    return _open_rest_sse_stream_once(
        store,
        path=path,
        params=params,
        last_event_id=last_event_id,
        force_auth=True,
    )


def _open_rest_sse_stream_once(
    store: StreamingStore,
    *,
    path: str,
    params: QueryParams,
    last_event_id: str | None,
    force_auth: bool,
) -> _StreamingResponse:
    store.authenticate(force=force_auth)
    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        source_context.name: source_context.get().value,
    }
    if last_event_id is not None:
        headers["Last-Event-ID"] = last_event_id

    config = store.config
    # WHY: RestZenStore.get() is JSON-oriented and cannot expose a streaming
    # body, so we reuse ZenML's REST URL constants/session/auth state but issue
    # this raw text/event-stream request ourselves.
    url = f"{store.url.rstrip('/')}{API}{VERSION_1}{path}"
    return store.session.get(
        url,
        params=params,
        headers=headers,
        stream=True,
        timeout=getattr(config, "http_timeout", None),
        verify=getattr(config, "verify_ssl", True),
    )


def _read_stream_error_body_prefix(response: _StreamingResponse) -> str:
    """Read a small response-body prefix for error detail extraction."""
    chunks: list[bytes] = []
    remaining = _MAX_STREAM_ERROR_BODY_BYTES
    for raw_chunk in response.iter_content(chunk_size=min(1024, remaining)):
        if not raw_chunk:
            continue
        chunk = (
            raw_chunk.encode("utf-8", errors="replace")
            if isinstance(raw_chunk, str)
            else bytes(raw_chunk)
        )
        chunks.append(chunk[:remaining])
        remaining -= min(len(chunk), remaining)
        if remaining <= 0:
            break
    return b"".join(chunks).decode("utf-8", errors="replace").strip()


def _safe_stream_response_suffix(response: _StreamingResponse) -> str:
    """Return a sanitized, bounded error suffix for stream HTTP failures."""
    body_prefix = _read_stream_error_body_prefix(response)
    if not body_prefix:
        return ""

    try:
        payload = json.loads(body_prefix)
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, dict):
        return ""

    detail = payload.get("detail")
    if not isinstance(detail, str):
        return ""

    sanitized = " ".join(detail.split()).strip()
    if not sanitized:
        return ""
    if len(sanitized) > _MAX_STREAM_ERROR_DETAIL_CHARS:
        sanitized = sanitized[: _MAX_STREAM_ERROR_DETAIL_CHARS - 3].rstrip() + "..."
    return f" Server response detail: {sanitized}"


def ensure_stream_response_supported(response: _StreamingResponse) -> None:
    """Map streaming HTTP status codes into public Kitaru errors."""
    status_code = response.status_code
    if 200 <= status_code < 300:
        return

    suffix = _safe_stream_response_suffix(response)
    if status_code == 501:
        raise KitaruFeatureNotAvailableError(
            "Execution event watching requires server streaming support, but "
            "streaming is disabled on the active backend." + suffix
        )
    if status_code in {404, 405}:
        raise KitaruFeatureNotAvailableError(
            "The active backend does not expose the execution event streaming "
            "endpoint. Upgrade the server runtime or use a backend with "
            "streaming support." + suffix
        )
    if status_code == 401:
        raise KitaruBackendError(
            "Execution event streaming is not authorized. Log in again or "
            "refresh your Kitaru credentials, then retry." + suffix
        )
    if status_code == 403:
        raise KitaruBackendError(
            "You do not have permission to watch live events for this "
            "execution." + suffix
        )
    if status_code == 503:
        raise KitaruBackendError(
            "Execution event streaming is temporarily unavailable because the "
            "server or stream broker is overloaded or unavailable." + suffix
        )

    raise KitaruBackendError(
        f"Execution event streaming request failed with HTTP {status_code}." + suffix
    )


def map_stream_event(frame: SSEFrame, *, fallback_exec_id: str) -> ExecutionEvent:
    """Map one ZenML StreamEvent SSE frame into Kitaru's public DTO."""
    try:
        raw_payload = json.loads(frame.data or "{}")
    except json.JSONDecodeError as exc:
        raise KitaruBackendError(
            "Malformed execution event stream payload: expected JSON data."
        ) from exc

    if not isinstance(raw_payload, dict):
        raise KitaruBackendError(
            "Malformed execution event stream payload: expected a JSON object."
        )

    payload = raw_payload.get("payload", {})
    if not isinstance(payload, dict):
        raise KitaruBackendError(
            "Malformed execution event payload: expected `payload` to be an object."
        )

    kind = optional_string(raw_payload.get("kind")) or frame.event
    if kind is None:
        raise KitaruBackendError("Malformed execution event payload: missing `kind`.")

    correlation_id = optional_string(raw_payload.get("correlation_id"))
    index = _optional_index(raw_payload.get("index"))
    step_name = optional_string(raw_payload.get("step_name"))
    step_run_id = optional_string(raw_payload.get("step_run_id"))
    kitaru_payload = payload.get("kitaru")
    if not isinstance(kitaru_payload, dict):
        kitaru_payload = {}

    checkpoint_id = optional_string(kitaru_payload.get("checkpoint_id")) or step_run_id
    checkpoint_name = optional_string(kitaru_payload.get("checkpoint_name"))
    if checkpoint_name is None and step_name is not None:
        checkpoint_name = normalize_checkpoint_name(step_name)

    return ExecutionEvent(
        exec_id=optional_string(raw_payload.get("pipeline_run_id")) or fallback_exec_id,
        kind=kind,
        payload=payload,
        correlation_id=correlation_id,
        index=index,
        cursor=frame.cursor,
        checkpoint_id=checkpoint_id,
        checkpoint_name=checkpoint_name,
        step_name=step_name,
    )


def watch_execution_events(
    *,
    open_stream: OpenStream,
    fallback_exec_id: str,
    checkpoint: str | None = None,
    reconnect: bool,
    backoff_seconds: Sequence[float] = _DEFAULT_RECONNECT_BACKOFF_SECONDS,
) -> Iterator[ExecutionEvent]:
    """Yield execution events from an SSE stream with bounded reconnects."""
    cursor: str | None = None
    reconnect_attempts = 0

    while True:
        response: _StreamingResponse | None = None
        try:
            response = open_stream(cursor)
            try:
                ensure_stream_response_supported(response)
            except KitaruBackendError as exc:
                if response.status_code == 503:
                    raise
                raise _NonReconnectableStreamError(str(exc)) from exc

            for frame in parse_sse_lines(response.iter_lines(decode_unicode=True)):
                frame_event = frame.event
                if frame.cursor is not None:
                    cursor = frame.cursor

                if frame_event == "end":
                    return
                if frame_event == "cursor":
                    continue
                if frame_event == "gap":
                    reason = _control_reason(frame)
                    raise _NonReconnectableStreamError(
                        "Execution event stream reported a delivery gap"
                        f"{f' ({reason})' if reason else ''}. Missing live "
                        "events cannot be reconstructed."
                    )
                if frame_event == "error":
                    reason = _control_reason(frame)
                    raise _NonReconnectableStreamError(
                        "Execution event stream failed"
                        f"{f' ({reason})' if reason else ''}."
                    )
                if frame_event == "system":
                    continue
                if frame_event is None and not frame.data:
                    continue

                try:
                    event = map_stream_event(frame, fallback_exec_id=fallback_exec_id)
                except KitaruBackendError as exc:
                    raise _NonReconnectableStreamError(str(exc)) from exc

                reconnect_attempts = 0
                if checkpoint is not None and event.checkpoint_name != checkpoint:
                    continue

                yield event

            raise KitaruBackendError(
                "Execution event stream ended before the server sent an end marker."
            )
        except KitaruFeatureNotAvailableError:
            raise
        except _NonReconnectableStreamError as exc:
            raise KitaruBackendError(str(exc)) from exc
        except Exception as exc:
            reconnect_attempts = _sleep_before_reconnect_or_raise(
                exc=exc,
                reconnect=reconnect,
                reconnect_attempts=reconnect_attempts,
                backoff_seconds=backoff_seconds,
                cursor=cursor,
            )
        finally:
            if response is not None:
                response.close()


def _sleep_before_reconnect_or_raise(
    *,
    exc: Exception,
    reconnect: bool,
    reconnect_attempts: int,
    backoff_seconds: Sequence[float],
    cursor: str | None,
) -> int:
    """Sleep before the next reconnect attempt, or raise a public error."""
    if not reconnect:
        if isinstance(exc, KitaruBackendError):
            raise exc
        raise KitaruBackendError(f"Execution event stream disconnected: {exc}") from exc

    if reconnect_attempts >= len(backoff_seconds):
        retry_error = KitaruBackendError(
            "Execution event stream disconnected repeatedly and Kitaru gave up "
            "reconnecting. Retry watching the execution later."
        )
        if isinstance(exc, KitaruBackendError):
            raise retry_error from None
        raise retry_error from exc

    delay = backoff_seconds[reconnect_attempts]
    logger.debug(
        "Execution event stream disconnected; reconnecting with cursor %r",
        cursor,
        exc_info=not isinstance(exc, KitaruBackendError),
    )
    if delay > 0:
        time.sleep(delay)
    return reconnect_attempts + 1


def _optional_index(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise KitaruBackendError(
            "Malformed execution event payload: `index` must be an integer."
        )
    return value


def _control_reason(frame: SSEFrame) -> str | None:
    try:
        payload = json.loads(frame.data or "{}")
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    reason = payload.get("reason")
    if reason is None:
        reason = payload.get("unknown_type")
    return optional_string(reason)


__all__ = [
    "OpenStream",
    "QueryParams",
    "SSEFrame",
    "StreamingStore",
    "ensure_stream_response_supported",
    "map_stream_event",
    "open_rest_sse_stream",
    "parse_sse_lines",
    "watch_execution_events",
]
