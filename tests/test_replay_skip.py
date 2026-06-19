"""Unit tests for the skip= selector in build_replay_plan."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kitaru.errors import KitaruUsageError
from kitaru.replay import build_replay_plan
from tests.test_replay import _run, _step


def test_skip_freezes_named_checkpoints_and_reexecutes_rest() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(name="fetch", invocation_id="fetch", started_at=t0)
    write = _step(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["fetch"],
    )
    publish = _step(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["write"],
    )

    plan = build_replay_plan(run=_run(fetch, write, publish), skip=["fetch", "write"])

    # fetch + write are frozen (cached); publish re-executes
    assert plan.steps_to_skip == {"fetch", "write"}


def test_from_and_skip_are_mutually_exclusive() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(name="fetch", invocation_id="fetch", started_at=t0)
    write = _step(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
    )
    publish = _step(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
    )

    with pytest.raises(KitaruUsageError):
        build_replay_plan(
            run=_run(fetch, write, publish),
            from_="publish",
            skip=["fetch"],
        )


def test_one_of_from_or_skip_required() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(name="fetch", invocation_id="fetch", started_at=t0)
    write = _step(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
    )
    publish = _step(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
    )

    with pytest.raises(KitaruUsageError):
        build_replay_plan(run=_run(fetch, write, publish))


def test_skip_with_overrides_raises() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(name="fetch", invocation_id="fetch", started_at=t0)
    write = _step(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["fetch"],
    )
    publish = _step(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["write"],
    )

    run = _run(fetch, write, publish)
    with pytest.raises(KitaruUsageError):
        build_replay_plan(run=run, skip=["fetch"], overrides={"checkpoint.write": "x"})
