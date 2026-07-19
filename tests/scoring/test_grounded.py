"""Grounded scorer access tests."""

from __future__ import annotations

import time

import pytest

from kitaru.scoring import GroundedCapabilityDeclaration, GroundedPolicySnapshot
from kitaru.scoring._grounded import (
    GroundedCapability,
    GroundedCapabilityBlocked,
    GroundedWorld,
)


def test_grounded_world_is_default_deny_and_records_allowed_read() -> None:
    world = GroundedWorld(
        policy=GroundedPolicySnapshot(policy_id="policy-1"),
        capabilities={
            "lookup": GroundedCapability(
                name="lookup",
                revision="v1",
                read_only=True,
                call=lambda resource: {"resource": resource},
            )
        },
    )

    with pytest.raises(GroundedCapabilityBlocked, match="not allowed"):
        world.call("lookup", "doc:1")

    allowed = GroundedWorld(
        policy=GroundedPolicySnapshot(
            policy_id="policy-2",
            capabilities=[
                GroundedCapabilityDeclaration(
                    name="lookup",
                    revision="v1",
                    read_only=True,
                )
            ],
            allowed_resources={"lookup": ["doc:*"]},
        ),
        capabilities={
            "lookup": GroundedCapability(
                name="lookup",
                revision="v1",
                read_only=True,
                call=lambda resource: {"resource": resource},
            )
        },
    )

    assert allowed.call("lookup", "doc:1") == {"resource": "doc:1"}
    assert allowed.provenance.policy.policy_id == "policy-2"
    assert allowed.provenance.calls[0].resource_identifier == "doc:1"


def test_grounded_world_enforces_timeout_before_call_returns() -> None:
    world = GroundedWorld(
        policy=GroundedPolicySnapshot(
            policy_id="policy-timeout",
            capabilities=[
                GroundedCapabilityDeclaration(
                    name="slow",
                    revision="v1",
                    read_only=True,
                )
            ],
            allowed_resources={"slow": ["doc:1"]},
            timeout_seconds=0.01,
        ),
        capabilities={
            "slow": GroundedCapability(
                name="slow",
                revision="v1",
                read_only=True,
                call=lambda resource: (time.sleep(0.2), resource)[1],
            )
        },
    )

    started = time.perf_counter()
    with pytest.raises(TimeoutError, match="timeout"):
        world.call("slow", "doc:1")

    assert time.perf_counter() - started < 0.15
    assert len(world.provenance.calls) == 1
    assert world.provenance.calls[0].result_summary["error_type"] == "TimeoutError"


def test_grounded_world_retries_up_to_policy_limit() -> None:
    attempts = 0

    def flaky(resource: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("not yet")
        return resource

    world = GroundedWorld(
        policy=GroundedPolicySnapshot(
            policy_id="policy-retry",
            capabilities=[
                GroundedCapabilityDeclaration(
                    name="lookup",
                    revision="v1",
                    read_only=True,
                )
            ],
            allowed_resources={"lookup": ["doc:1"]},
            retry_limit=2,
        ),
        capabilities={
            "lookup": GroundedCapability(
                name="lookup",
                revision="v1",
                read_only=True,
                call=flaky,
            )
        },
    )

    assert world.call("lookup", "doc:1") == "doc:1"
    assert attempts == 3
    assert len(world.provenance.calls) == 3


def test_grounded_world_retries_do_not_exceed_policy_limit() -> None:
    attempts = 0

    def broken(resource: str) -> str:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(resource)

    world = GroundedWorld(
        policy=GroundedPolicySnapshot(
            policy_id="policy-retry-bound",
            capabilities=[
                GroundedCapabilityDeclaration(
                    name="lookup",
                    revision="v1",
                    read_only=True,
                )
            ],
            allowed_resources={"lookup": ["doc:1"]},
            retry_limit=1,
        ),
        capabilities={
            "lookup": GroundedCapability(
                name="lookup",
                revision="v1",
                read_only=True,
                call=broken,
            )
        },
    )

    with pytest.raises(RuntimeError):
        world.call("lookup", "doc:1")
    assert attempts == 2
    assert len(world.provenance.calls) == 2


def test_grounded_world_blocks_write_capable_runtime_registration() -> None:
    world = GroundedWorld(
        policy=GroundedPolicySnapshot(
            policy_id="policy-1",
            capabilities=[
                GroundedCapabilityDeclaration(
                    name="lookup",
                    revision="v1",
                    read_only=True,
                )
            ],
            allowed_resources={"lookup": ["doc:1"]},
        ),
        capabilities={
            "lookup": GroundedCapability(
                name="lookup",
                revision="v1",
                read_only=False,
                call=lambda resource: resource,
            )
        },
    )

    with pytest.raises(GroundedCapabilityBlocked, match="not read-only"):
        world.call("lookup", "doc:1")
