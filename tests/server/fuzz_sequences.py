#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Reusable isolation and failure receipts for generated API sequences."""

import re
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Iterable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from enum import StrEnum
from types import TracebackType
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from conftest import lifespan_client, local_settings
from kitaru.server.api.config import APISettings

_ACCOUNT_PASSWORD = "sequence-secret"
_SECRET_FIELDS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "key",
        "password",
        "task_token",
        "token",
    }
)
_UUID_PATTERN = re.compile(
    r"(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-f])",
    re.IGNORECASE,
)


class CredentialRole(StrEnum):
    """Symbolic credential roles understood by a generated sequence."""

    ACCOUNT = "account"
    WORKER = "worker"
    TASK_ATTEMPT_1 = "task_attempt_1"
    TASK_ATTEMPT_2 = "task_attempt_2"
    TASK_ATTEMPT_3 = "task_attempt_3"
    FOREIGN_TASK = "foreign_task"


class SequenceAction(BaseModel):
    """One replayable operation expressed without live ids or credentials."""

    model_config = ConfigDict(extra="forbid")

    name: str
    target: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)


class SequenceStepReceipt(BaseModel):
    """Sanitized result and checked invariants for one sequence operation."""

    action: SequenceAction
    credential_role: CredentialRole
    status: int
    response: Any = None
    invariants: list[str] = Field(default_factory=list)


class SequenceReceipt(BaseModel):
    """Sanitized evidence needed to understand and replay a sequence."""

    requested_actions: list[SequenceAction] = Field(default_factory=list)
    steps: list[SequenceStepReceipt] = Field(default_factory=list)
    successful_operations: int = 0

    def _record_requested_action(self, action: SequenceAction) -> None:
        """Store one already-sanitized caller action before execution begins."""
        self.requested_actions.append(action)

    def record_step(
        self,
        *,
        action: SequenceAction,
        credential_role: CredentialRole,
        status: int,
        response: Any = None,
        invariants: Iterable[str] = (),
    ) -> None:
        """Record one sanitized operation result.

        Args:
            action: Symbolic operation that was executed.
            credential_role: Role used for the request.
            status: Observed HTTP status.
            response: Sanitized response summary.
            invariants: Names of the invariants checked after the response.
        """
        self.steps.append(
            SequenceStepReceipt(
                action=action,
                credential_role=credential_role,
                status=status,
                response=response,
                invariants=list(invariants),
            )
        )
        if 200 <= status < 300:
            self.successful_operations += 1

    def assert_accepted(self) -> None:
        """Reject an empty sequence or one without a successful prerequisite."""
        if not self.steps:
            raise AssertionError("Sequence receipt contains no operations")
        has_prerequisite = any(
            step.action.name.startswith("create_") and 200 <= step.status < 300
            for step in self.steps
        )
        if not has_prerequisite:
            raise AssertionError("Sequence contains no successful prerequisite")

    def serialize(self) -> str:
        """Serialize the receipt as stable, readable JSON."""
        return self.model_dump_json(indent=2)


class SequenceCleanupError(RuntimeError):
    """Report cleanup separately from any failure raised by the sequence."""

    def __init__(
        self,
        *,
        sequence_error: BaseException | None,
        cleanup_error: BaseException,
    ) -> None:
        sequence_summary = (
            "none"
            if sequence_error is None
            else f"{type(sequence_error).__name__}: {sequence_error}"
        )
        super().__init__(
            "Sequence cleanup failed "
            f"after sequence error {sequence_summary}; "
            f"cleanup error {type(cleanup_error).__name__}: {cleanup_error}"
        )
        self.sequence_error = sequence_error
        self.cleanup_error = cleanup_error


class SequenceRuntime:
    """Keep live ids and credentials outside the replayable receipt."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        receipt: SequenceReceipt,
        *,
        database_name: str | None = None,
        settings: APISettings | None = None,
    ) -> None:
        self.client = client
        self.receipt = receipt
        self.database_name = database_name
        self.settings = settings
        self._ids: dict[str, str] = {}
        self._credentials: dict[CredentialRole, str] = {}

    def bind_id(self, symbol: str, value: str | uuid.UUID) -> None:
        """Bind a symbolic resource name to one id from the current database."""
        raw_value = str(value)
        if symbol in self._ids:
            raise AssertionError(f"Symbolic id is already bound: {symbol}")
        if raw_value in self._ids.values():
            raise AssertionError("Live id is already bound to another symbol")
        self._ids[symbol] = raw_value

    def resolve_id(self, symbol: str) -> str:
        """Resolve a symbolic resource name for the current sequence run."""
        try:
            return self._ids[symbol]
        except KeyError as exc:
            raise AssertionError(f"Symbolic id is not bound: {symbol}") from exc

    def set_credential(self, role: CredentialRole, value: str) -> None:
        """Bind one live credential to a symbolic role."""
        self._credentials[role] = value

    def get_headers(self, role: CredentialRole) -> dict[str, str]:
        """Build request headers for one bound credential role."""
        try:
            credential = self._credentials[role]
        except KeyError as exc:
            raise AssertionError(f"Credential role is not bound: {role}") from exc
        return {"Authorization": f"Bearer {credential}"}

    def record_requested_action(self, action: SequenceAction) -> None:
        """Record one caller action after sanitizing its replay arguments."""
        sanitized_arguments = self.sanitize(action.arguments)
        if not isinstance(sanitized_arguments, dict):
            raise AssertionError(
                "Sanitized sequence action arguments must be a mapping"
            )
        self.receipt._record_requested_action(
            SequenceAction(
                name=action.name,
                target=action.target,
                arguments=sanitized_arguments,
            )
        )

    def sanitize(self, value: Any) -> Any:
        """Replace live ids and secrets with replayable symbolic values."""
        if isinstance(value, uuid.UUID):
            value = str(value)
        if isinstance(value, str):
            for symbol, live_id in sorted(
                self._ids.items(), key=lambda item: len(item[1]), reverse=True
            ):
                if value == live_id:
                    return {"$ref": symbol}
                value = value.replace(live_id, f"<$ref:{symbol}>")
            for role, credential in sorted(
                self._credentials.items(), key=lambda item: len(item[1]), reverse=True
            ):
                if value == credential or value == f"Bearer {credential}":
                    return {"$credential": role.value}
                value = value.replace(
                    f"Bearer {credential}", f"<$credential:{role.value}>"
                )
                value = value.replace(credential, f"<$credential:{role.value}>")
            return _UUID_PATTERN.sub("<unbound-uuid>", value)
        if isinstance(value, dict):
            return {
                str(key): (
                    "<redacted>"
                    if str(key).lower() in _SECRET_FIELDS
                    else self.sanitize(item)
                )
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self.sanitize(item) for item in value]
        return value

    def record_response(
        self,
        *,
        action: SequenceAction,
        credential_role: CredentialRole,
        response: httpx.Response,
        invariants: Iterable[str] = (),
    ) -> None:
        """Record one HTTP response without retaining live secrets or ids."""
        try:
            body = response.json()
        except ValueError:
            body = response.text or None
        self.receipt.record_step(
            action=SequenceAction(
                name=action.name,
                target=action.target,
                arguments=self.sanitize(action.arguments),
            ),
            credential_role=credential_role,
            status=response.status_code,
            response=self.sanitize(body),
            invariants=invariants,
        )


@asynccontextmanager
async def report_cleanup_failures(
    manager: AbstractAsyncContextManager[httpx.AsyncClient],
) -> AsyncIterator[httpx.AsyncClient]:
    """Preserve a sequence failure when its context cleanup also fails."""
    client = await manager.__aenter__()
    sequence_error: BaseException | None = None
    sequence_traceback: TracebackType | None = None
    try:
        yield client
    except BaseException as exc:
        sequence_error = exc
        sequence_traceback = exc.__traceback__

    try:
        suppressed = await manager.__aexit__(
            type(sequence_error) if sequence_error is not None else None,
            sequence_error,
            sequence_traceback,
        )
    except BaseException as cleanup_error:
        raise SequenceCleanupError(
            sequence_error=sequence_error,
            cleanup_error=cleanup_error,
        ) from cleanup_error

    if sequence_error is not None and not suppressed:
        raise sequence_error.with_traceback(sequence_traceback)


@asynccontextmanager
async def isolate_sequence(
    receipt: SequenceReceipt,
    **settings_overrides: Any,
) -> AsyncGenerator[SequenceRuntime, None]:
    """Run one API sequence in one fresh database and authenticated lifespan.

    Args:
        receipt: Receipt populated by the isolated sequence.
        **settings_overrides: Server settings for the isolated lifespan.
    """
    settings_values: dict[str, Any] = {
        "DEFAULT_ACCOUNT_PASSWORD": _ACCOUNT_PASSWORD,
        "TASK_SWEEP_INTERVAL_SECONDS": 0,
        **settings_overrides,
    }
    settings = local_settings(use_db=True, **settings_values)
    manager = lifespan_client(settings)
    async with report_cleanup_failures(manager) as client:
        response = await client.post(
            "/api/v1/login",
            data={"username": "default", "password": _ACCOUNT_PASSWORD},
        )
        if response.status_code != 200:
            raise AssertionError(
                f"Sequence login returned HTTP {response.status_code}: {response.text}"
            )
        runtime = SequenceRuntime(
            client,
            receipt,
            database_name=settings.DB_NAME,
            settings=settings,
        )
        runtime.set_credential(
            CredentialRole.ACCOUNT,
            response.json()["access_token"],
        )
        yield runtime


@contextmanager
def annotate_sequence_failure(receipt: SequenceReceipt) -> Iterator[None]:
    """Attach a sanitized receipt to an assertion without replacing it."""
    try:
        yield
    except BaseException as exc:
        if isinstance(exc, (GeneratorExit, KeyboardInterrupt, SystemExit)):
            raise
        exc.add_note(f"Sequence receipt:\n{receipt.serialize()}")
        raise
