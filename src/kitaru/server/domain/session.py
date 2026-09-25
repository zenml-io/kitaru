#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Session entity, rollups, and errors."""

import base64
import binascii
import hashlib
import re
import uuid
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.payload import Payload


class SessionNotFound(NotFoundError):
    """Raised when a session lookup does not resolve."""

    def __init__(self, session_id: uuid.UUID | None = None) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the missing session, None when unidentified.
        """
        if session_id is None:
            super().__init__("A referenced session was not found")
        else:
            super().__init__(f"Session {session_id} was not found")


class SessionAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} is not accessible to this caller")


class SessionBaselineNotFound(NotFoundError):
    """Raised when a session was not produced by a replay."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} was not produced by a replay")


class DuplicateSessionExternalId(ConflictError):
    """Raised when an imported_from and external id pair is already registered."""

    def __init__(self, imported_from: str | None, external_id: str | None) -> None:
        """Initialize the error.

        Args:
            imported_from: Source system the session was imported from.
            external_id: Id from the source system.
        """
        super().__init__(
            f"Session with imported_from '{imported_from}' and external_id "
            f"'{external_id}' is already registered"
        )


class SessionInUse(ConflictError):
    """Raised when a cohort version, investigation, or replay references a session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session that cannot be deleted.
        """
        super().__init__(
            f"Session {session_id} is referenced by a cohort version, "
            "investigation, or replay and cannot be deleted"
        )


class SessionAgentVersionMismatch(ValidationError):
    """Raised when a session names a different agent version than its task runs."""

    def __init__(
        self,
        task_id: uuid.UUID,
        agent_version_id: uuid.UUID | None,
        task_agent_version_id: uuid.UUID | None,
    ) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the producing task.
            agent_version_id: Agent version the session names.
            task_agent_version_id: Agent version the task runs.
        """
        super().__init__(
            f"Session names agent version {agent_version_id}, task {task_id} "
            f"runs agent version {task_agent_version_id}"
        )


class SessionAgentMismatch(ValidationError):
    """Raised when a session names a different agent than its task creates under."""

    def __init__(
        self, task_id: uuid.UUID, agent_id: uuid.UUID | None, task_agent_id: uuid.UUID
    ) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the producing task.
            agent_id: Agent the session names.
            task_agent_id: Agent the task creates sessions under.
        """
        super().__init__(
            f"Session names agent {agent_id}, task {task_id} creates sessions "
            f"under agent {task_agent_id}"
        )


class SessionAgentRequired(ValidationError):
    """Raised when a session create carries no agent and none can be inferred."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Session names no agent and no task to infer one from")


class SessionStatusCannotBeCleared(ValidationError):
    """Raised when a session update tries to clear the status."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} status cannot be cleared")


class SessionNotEvaluatable(ConflictError):
    """Raised when a session does not currently accept evaluations."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} does not accept evaluations")


class SessionNotIngestable(ConflictError):
    """Raised when a session does not currently accept node ingestion."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} does not accept node ingestion")


class SessionNotUpdatable(ConflictError):
    """Raised when a session does not currently accept updates."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} does not accept updates")


class SessionReplayFinalizationInvalid(ValidationError):
    """Raised when a Mastra replay input transition is incomplete or unauthorized."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} has invalid Mastra replay finalization")


class SessionReplayNotReady(ConflictError):
    """Raised when a Mastra baseline cannot be replayed yet."""

    def __init__(self, session_id: uuid.UUID, reason: str) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the baseline session.
            reason: Stable replay eligibility reason code.
        """
        super().__init__(f"Session {session_id}: {reason}")
        self.session_id = session_id
        self.reason = reason


def mastra_replay_uses_observational_memory(envelope: dict[str, Any]) -> bool:
    """Return whether a recorded Mastra replay input enables observational memory."""
    config = envelope.get("configuration")
    memory = config.get("memoryConfig") if isinstance(config, dict) else None
    om = memory.get("observationalMemory") if isinstance(memory, dict) else None
    return om is True or (isinstance(om, dict) and om.get("enabled") is not False)


def mastra_replay_v3_current(envelope: dict[str, Any]) -> bool:
    """Check that a version-3 input carries its recorded key order and turn start.

    Early version-3 inputs lack both, and the adapter can no longer decode them.
    """
    key_order = envelope.get("keyOrder")
    started = envelope.get("turnStartedAt")
    if not (
        isinstance(key_order, dict)
        and isinstance(key_order.get("permutations"), str)
        and isinstance(key_order.get("sha256"), str)
        and re.fullmatch(r"[a-f0-9]{64}", key_order["sha256"]) is not None
        and isinstance(started, str)
    ):
        return False
    try:
        datetime.fromisoformat(started)
    except ValueError:
        return False
    return True


def mastra_replay_v3_complete(envelope: dict[str, Any]) -> bool:
    """Check the required shape of a finalized Mastra replay input."""
    snapshot = envelope.get("initialSnapshot")
    config = envelope.get("configuration")
    files = envelope.get("files")
    return (
        envelope.get("version") == 3
        and envelope.get("complete") is True
        and envelope.get("reasons") == []
        and isinstance(envelope.get("invocationId"), str)
        and bool(envelope["invocationId"])
        and "rawInput" in envelope
        and isinstance(snapshot, dict)
        and isinstance(snapshot.get("threadId"), str)
        and isinstance(snapshot.get("resourceId"), str)
        and isinstance(snapshot.get("messages"), list)
        and isinstance(snapshot.get("records"), list)
        and isinstance(config, dict)
        and isinstance(config.get("memoryConfig"), dict)
        and isinstance(envelope.get("requestContext"), dict)
        and isinstance(files, list)
        and _mastra_replay_files_complete(files)
        and isinstance(envelope.get("omTape"), list)
        and mastra_replay_v3_current(envelope)
    )


_MASTRA_FILE_REFERENCE = re.compile(r"kitaru-file://sha256/[a-f0-9]{64}")
_SHA256 = re.compile(r"[a-f0-9]{64}")
_MASTRA_MAX_FILE_BYTES = 16 * 1_048_576
_MASTRA_MAX_INLINE_FILE_BYTES = 8 * 1_048_576


class MastraStoredFile(FrozenModel):
    """A recorded Mastra file whose content is stored as a blob."""

    blob_id: uuid.UUID
    sha256: str
    length: int

    def is_held_by(self, blob: Blob | None) -> bool:
        """Return whether the blob exists and holds this file's content.

        Args:
            blob: The stored blob this file names, or None when it is missing.

        Returns:
            Whether the blob's hash and size match the recorded file.
        """
        return (
            blob is not None and blob.sha256 == self.sha256 and blob.size == self.length
        )


def _read_mastra_stored_file(file: dict[str, Any]) -> MastraStoredFile | None:
    """Read a file entry that names the blob holding its content.

    Args:
        file: Recorded file entry.

    Returns:
        The blob reference, or None when the entry is malformed.
    """
    blob_id = file.get("blobId")
    if not isinstance(blob_id, str):
        return None
    try:
        parsed = uuid.UUID(blob_id)
    except ValueError:
        return None
    if str(parsed) != blob_id:
        return None
    return MastraStoredFile(
        blob_id=parsed, sha256=file["sha256"], length=file["length"]
    )


def _mastra_inline_file_complete(file: dict[str, Any], url: str) -> bool:
    """Check a file entry that holds its content inline as base64.

    Args:
        file: Recorded file entry.
        url: The entry's content reference.

    Returns:
        Whether the content matches its length, hash, and reference.
    """
    encoded = file["base64"]
    if not isinstance(encoded, str) or file["length"] > _MASTRA_MAX_INLINE_FILE_BYTES:
        return False
    try:
        content = base64.b64decode(encoded, validate=True)
        reference = (
            "kitaru-file://sha256/"
            + hashlib.sha256(file["mediaType"].encode() + b"\0" + content).hexdigest()
        )
    except (binascii.Error, ValueError, UnicodeEncodeError):
        return False
    return (
        base64.b64encode(content).decode("ascii") == encoded
        and len(content) == file["length"]
        and hashlib.sha256(content).hexdigest() == file["sha256"]
        and url == reference
    )


def _mastra_replay_files_complete(files: list[Any]) -> bool:
    """Validate bounded file references before publishing replay eligibility.

    A file names the blob that stores its content, or holds the content
    inline as base64. Blob entries are checked against the stored blobs
    separately, because that needs the blob registry.
    """
    if len(files) > 64:
        return False
    seen: set[str] = set()
    total_bytes = 0
    for file in files:
        if not isinstance(file, dict):
            return False
        url = file.get("url")
        media_type = file.get("mediaType")
        length = file.get("length")
        digest = file.get("sha256")
        if (
            not isinstance(url, str)
            or url in seen
            or _MASTRA_FILE_REFERENCE.fullmatch(url) is None
            or not isinstance(media_type, str)
            or not media_type
            or not isinstance(length, int)
            or isinstance(length, bool)
            or length < 0
            or length > _MASTRA_MAX_FILE_BYTES
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
        ):
            return False
        if "base64" in file:
            if "blobId" in file or not _mastra_inline_file_complete(file, url):
                return False
        elif _read_mastra_stored_file(file) is None:
            return False
        total_bytes += length
        if total_bytes > _MASTRA_MAX_FILE_BYTES:
            return False
        seen.add(url)
    return True


def mastra_replay_stored_files(inputs: Any) -> list[MastraStoredFile]:
    """Return the blob-stored files a Mastra replay input records.

    Entries that are malformed or hold their content inline are skipped.

    Args:
        inputs: Session or task inputs, of any shape.

    Returns:
        The recorded files stored as blobs.
    """
    envelope = inputs.get("mastra_memory_replay") if isinstance(inputs, dict) else None
    files = envelope.get("files") if isinstance(envelope, dict) else None
    if not isinstance(files, list):
        return []
    stored: list[MastraStoredFile] = []
    for file in files:
        if (
            not isinstance(file, dict)
            or "base64" in file
            or not isinstance(file.get("sha256"), str)
            or not isinstance(file.get("length"), int)
            or isinstance(file.get("length"), bool)
        ):
            continue
        entry = _read_mastra_stored_file(file)
        if entry is not None:
            stored.append(entry)
    return stored


class SessionRollups(FrozenModel):
    """Session rollup deltas."""

    cost: Decimal = Decimal(0)
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0
    llm_call_count: int = 0
    tool_call_count: int = 0


def combine_rollups(deltas: Iterable[SessionRollups]) -> SessionRollups:
    """Sum a sequence of rollup deltas into one total.

    Returns:
        Combined rollup delta.
    """
    cost = Decimal(0)
    input_tokens = 0
    output_tokens = 0
    cached_input_tokens = 0
    reasoning_tokens = 0
    llm_call_count = 0
    tool_call_count = 0
    for delta in deltas:
        cost += delta.cost
        input_tokens += delta.input_tokens
        output_tokens += delta.output_tokens
        cached_input_tokens += delta.cached_input_tokens
        reasoning_tokens += delta.reasoning_tokens
        llm_call_count += delta.llm_call_count
        tool_call_count += delta.tool_call_count
    return SessionRollups(
        cost=cost,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        reasoning_tokens=reasoning_tokens,
        llm_call_count=llm_call_count,
        tool_call_count=tool_call_count,
    )


def rollup_delta(old: SessionRollups, new: SessionRollups) -> SessionRollups:
    """Compute the field-wise difference from one rollup bundle to another.

    Returns:
        Rollup delta from ``old`` to ``new``.
    """
    return SessionRollups(
        cost=new.cost - old.cost,
        input_tokens=new.input_tokens - old.input_tokens,
        output_tokens=new.output_tokens - old.output_tokens,
        cached_input_tokens=new.cached_input_tokens - old.cached_input_tokens,
        reasoning_tokens=new.reasoning_tokens - old.reasoning_tokens,
        llm_call_count=new.llm_call_count - old.llm_call_count,
        tool_call_count=new.tool_call_count - old.tool_call_count,
    )


class Session(DomainModel):
    """Session."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    number: int
    agent_version_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    import_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus = SessionStatus.IN_PROGRESS
    name: str | None = None
    input_text_selector: str | None = None
    output_text_selector: str | None = None
    inputs: Payload | None = None
    outputs: Payload | None = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    imported_from: str | None = None
    framework: str | None = None
    adapter_version: str | None = None
    cost: Decimal | None = None
    tokens: TokenUsage | None = None
    llm_call_count: int = 0
    tool_call_count: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str | None) -> None:
        """Set a new session name.

        Args:
            name: New name.
        """
        self.name = name

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata

    def link_task(self, task_id: uuid.UUID) -> None:
        """Set the task this session was produced by.

        Args:
            task_id: Id of the producing task.
        """
        self.task_id = task_id

    def unlink_task(self) -> None:
        """Clear the task this session was produced by."""
        self.task_id = None

    def check_update(self) -> None:
        """Require the session to currently accept updates.

        Raises:
            SessionNotUpdatable: The session is not in progress.
        """
        if self.status != SessionStatus.IN_PROGRESS:
            raise SessionNotUpdatable(self.id)

    def resolve_replay_metadata(
        self,
        status: SessionStatus,
        metadata: dict[str, Any],
        inputs: Any,
        replacing_inputs: bool,
    ) -> dict[str, Any]:
        """Validate a Mastra replay input transition and return the metadata to store.

        A pending Mastra recording that ends without a replay decision is
        stored as ineligible: ``abandoned`` when it failed, ``unfinalized``
        when it completed.

        Args:
            status: Session status after the update.
            metadata: Session metadata after the update.
            inputs: Replacement input value, when supplied.
            replacing_inputs: Whether the request explicitly supplied inputs.

        Raises:
            SessionReplayFinalizationInvalid: The update replaces inputs outside
                a pending Mastra finalization, or publishes an invalid replay
                state or input.

        Returns:
            Metadata to store with the update.
        """
        is_mastra_recording = (
            self.framework == "mastra" and self.origin == SessionOrigin.RECORDED
        )
        if not is_mastra_recording:
            if replacing_inputs:
                raise SessionReplayFinalizationInvalid(self.id)
            return metadata
        prior = self.metadata.get("mastra_replay_state")
        next_state = metadata.get("mastra_replay_state")
        if prior != "pending":
            if replacing_inputs:
                raise SessionReplayFinalizationInvalid(self.id)
            return metadata
        if status == SessionStatus.IN_PROGRESS:
            if replacing_inputs or next_state != "pending":
                raise SessionReplayFinalizationInvalid(self.id)
            return metadata
        if next_state in {None, "pending"} and not replacing_inputs:
            # Cleanup of a recorder that died mid-turn, and clients that do not
            # send a replay decision, must still be able to close the session.
            return {
                **metadata,
                "mastra_replay_state": "ineligible",
                "mastra_replay_reason": "abandoned"
                if status == SessionStatus.FAILED
                else "unfinalized",
            }
        if next_state not in {"eligible", "ineligible"}:
            raise SessionReplayFinalizationInvalid(self.id)
        if replacing_inputs and (
            not isinstance(inputs, dict)
            or not isinstance(inputs.get("mastra_memory_replay"), dict)
        ):
            raise SessionReplayFinalizationInvalid(self.id)
        if next_state == "eligible":
            envelope = (
                inputs.get("mastra_memory_replay")
                if replacing_inputs and isinstance(inputs, dict)
                else None
            )
            if (
                status != SessionStatus.COMPLETED
                or not isinstance(envelope, dict)
                or not mastra_replay_v3_complete(envelope)
            ):
                raise SessionReplayFinalizationInvalid(self.id)
            if mastra_replay_uses_observational_memory(envelope) and not isinstance(
                envelope.get("omTape"), list
            ):
                raise SessionReplayFinalizationInvalid(self.id)
        return metadata

    def check_evaluate(self) -> None:
        """Require the session to currently accept evaluations.

        Raises:
            SessionNotEvaluatable: The session is in progress.
        """
        if self.status == SessionStatus.IN_PROGRESS:
            raise SessionNotEvaluatable(self.id)

    def check_node_ingest(self) -> None:
        """Require the session to currently accept node ingestion.

        Raises:
            SessionNotIngestable: The session is not in progress, its origin
                is not imported, and it names no import source.
        """
        if self.status == SessionStatus.IN_PROGRESS:
            return
        if self.origin == SessionOrigin.IMPORTED:
            return
        if self.imported_from is not None:
            return
        raise SessionNotIngestable(self.id)

    def finish(
        self,
        status: SessionStatus,
        output_text_selector: str | None,
        error: str | None,
        ended_at: datetime | None,
    ) -> None:
        """Apply a status transition with its output selector, error, and end time."""
        self.status = status
        self.output_text_selector = output_text_selector
        self.error = error
        self.ended_at = ended_at
