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
"""Fast contract tests for generated API sequence receipts and cleanup."""

from contextlib import AbstractAsyncContextManager
from types import TracebackType

import httpx
import pytest
from fuzz_sequences import (
    CredentialRole,
    SequenceAction,
    SequenceCleanupError,
    SequenceReceipt,
    SequenceRuntime,
    annotate_sequence_failure,
    report_cleanup_failures,
)


def test_receipt_acceptance_rejects_empty_and_read_only_sequences() -> None:
    """Require at least one successful prerequisite before accepting a run."""
    empty = SequenceReceipt()
    with pytest.raises(AssertionError, match="no operations"):
        empty.assert_accepted()

    read_only = SequenceReceipt()
    read_only.record_step(
        action=SequenceAction(name="list_agents"),
        credential_role=CredentialRole.ACCOUNT,
        status=200,
    )
    with pytest.raises(AssertionError, match="no successful prerequisite"):
        read_only.assert_accepted()


async def test_failure_note_redacts_nested_ids_and_credentials() -> None:
    """Attach replay evidence without leaking embedded ids or credentials."""
    raw_id = "018f7777-1234-7abc-8123-123456789abc"
    other_id = "018f8888-5678-7def-9234-abcdef123456"
    raw_token = "live-worker-token"
    receipt = SequenceReceipt()
    async with httpx.AsyncClient() as client:
        runtime = SequenceRuntime(client, receipt)
        runtime.bind_id("agent_0", raw_id)
        runtime.set_credential(CredentialRole.WORKER, raw_token)
        runtime.record_response(
            action=SequenceAction(
                name="create_agent",
                target="agent_0",
                arguments={
                    "resource": f"/agents/{raw_id}",
                    "authorization": f"prefix Bearer {raw_token} suffix",
                },
            ),
            credential_role=CredentialRole.WORKER,
            response=httpx.Response(
                404,
                json={
                    "detail": f"Agent {raw_id} was not found; related {other_id}",
                    "diagnostic": f"authentication failed for Bearer {raw_token}",
                },
            ),
            invariants=["forced_failure"],
        )

    with (
        pytest.raises(RuntimeError, match="forced") as raised,
        annotate_sequence_failure(receipt),
    ):
        raise RuntimeError("forced")

    notes = "\n".join(raised.value.__notes__)
    assert "Sequence receipt:\n{" in notes
    assert '"credential_role": "worker"' in notes
    assert "<$ref:agent_0>" in notes
    assert "<unbound-uuid>" in notes
    assert "<$credential:worker>" in notes
    assert raw_id not in notes
    assert other_id not in notes
    assert raw_token not in notes


async def test_requested_action_redacts_ids_and_credentials() -> None:
    """Sanitize replay inputs before adding them to a failure receipt."""
    raw_id = "018f7777-1234-7abc-8123-123456789abc"
    other_id = "018f8888-5678-7def-9234-abcdef123456"
    raw_token = "live-worker-token"
    receipt = SequenceReceipt()
    async with httpx.AsyncClient() as client:
        runtime = SequenceRuntime(client, receipt)
        runtime.bind_id("agent_0", raw_id)
        runtime.set_credential(CredentialRole.WORKER, raw_token)
        runtime.record_requested_action(
            SequenceAction(
                name="update_agent",
                target="agent_0",
                arguments={
                    "resource": f"/agents/{raw_id}",
                    "related": other_id,
                    "authorization": f"Bearer {raw_token}",
                    "password": "plain-secret",
                },
            )
        )

    serialized = receipt.serialize()
    assert "<$ref:agent_0>" in serialized
    assert "<unbound-uuid>" in serialized
    assert '"authorization": "<redacted>"' in serialized
    assert '"password": "<redacted>"' in serialized
    assert raw_id not in serialized
    assert other_id not in serialized
    assert raw_token not in serialized
    assert "plain-secret" not in serialized


def test_failure_note_does_not_intercept_interrupt() -> None:
    """Propagate an interruption without turning it into a receipt failure."""
    receipt = SequenceReceipt()
    interruption = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt) as raised, annotate_sequence_failure(receipt):
        raise interruption
    assert raised.value is interruption
    assert not getattr(raised.value, "__notes__", [])


class _ControlledManager(AbstractAsyncContextManager[httpx.AsyncClient]):
    """Expose entry and exit behavior for cleanup reporting tests."""

    def __init__(self, cleanup_error: BaseException | None = None) -> None:
        self.client = httpx.AsyncClient()
        self.cleanup_error = cleanup_error
        self.exited = False

    async def __aenter__(self) -> httpx.AsyncClient:
        return self.client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        _ = exc_type, exc_value, traceback
        self.exited = True
        await self.client.aclose()
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return None


async def test_assertion_and_interruption_enter_cleanup() -> None:
    """Exit the sequence context for assertion failures and interruptions."""
    for failure in (AssertionError("broken invariant"), KeyboardInterrupt()):
        manager = _ControlledManager()
        with pytest.raises(type(failure)):
            async with report_cleanup_failures(manager):
                raise failure
        assert manager.exited


async def test_cleanup_failure_preserves_the_sequence_failure() -> None:
    """Distinguish teardown failure with or without an earlier body failure."""
    for sequence_error in (None, AssertionError("broken invariant")):
        cleanup_error = RuntimeError("database drop failed")
        manager = _ControlledManager(cleanup_error)

        with pytest.raises(
            SequenceCleanupError, match="database drop failed"
        ) as raised:
            async with report_cleanup_failures(manager):
                if sequence_error is not None:
                    raise sequence_error

        assert raised.value.sequence_error is sequence_error
        assert raised.value.cleanup_error is cleanup_error
        assert manager.exited
