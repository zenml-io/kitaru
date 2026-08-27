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
"""Tests for task hook execution around the task process."""

import asyncio
import os
import uuid
from pathlib import Path

import pytest

from kitaru.api_models.v1.hook import (
    CopyWorkdirHook as CopyWorkdirHookSpec,
)
from kitaru.api_models.v1.hook import (
    SetupCommandHook as SetupCommandHookSpec,
)
from kitaru.api_models.v1.hook import TaskHook
from kitaru.api_models.v1.hook import (
    TeardownCommandHook as TeardownCommandHookSpec,
)
from kitaru.worker.hooks import (
    CopyWorkdirHook,
    HookContext,
    HookError,
    SetupCommandHook,
    TeardownCommandHook,
    build_hooks,
)
from kitaru.worker.process import TaskProcess


def _make_ctx(scratch_dir: Path, working_dir: str | None = None) -> HookContext:
    """Build a hook context around a placeholder task process."""
    process = TaskProcess(
        command="true",
        working_dir=working_dir,
        env=dict(os.environ),
        timeout_seconds=30,
    )
    return HookContext(
        task_id=uuid.uuid4(),
        scratch_dir=scratch_dir,
        canceled=asyncio.Event(),
        process=process,
    )


def _make_workdir_ctx(tmp_path: Path) -> tuple[HookContext, Path]:
    """Build a hook context whose process has a working directory."""
    working_dir = tmp_path / "work"
    working_dir.mkdir()
    return _make_ctx(tmp_path, working_dir=str(working_dir)), working_dir


def test_build_hooks_maps_each_spec_type_in_order() -> None:
    """Each spec type builds its implementation in declared order."""
    specs: list[TaskHook] = [
        SetupCommandHookSpec(command="setup.sh"),
        CopyWorkdirHookSpec(),
        TeardownCommandHookSpec(command="teardown.sh"),
    ]

    hooks = build_hooks(specs)

    assert [type(hook) for hook in hooks] == [
        SetupCommandHook,
        CopyWorkdirHook,
        TeardownCommandHook,
    ]


async def test_copy_workdir_copies_the_working_dir_and_rewires_the_process(
    tmp_path: Path,
) -> None:
    """The setup copies the working directory and points the process at it."""
    original = tmp_path / "original"
    (original / "nested").mkdir(parents=True)
    (original / "nested" / "inner.txt").write_text("inner")
    (original / "link.txt").symlink_to("nested/inner.txt")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch, working_dir=str(original))

    await CopyWorkdirHook(0).setup(ctx)

    copy = scratch / "hook-0-workdir"
    assert ctx.process.working_dir == str(copy)
    assert (copy / "nested" / "inner.txt").read_text() == "inner"
    assert (copy / "link.txt").is_symlink()
    (copy / "added.txt").write_text("added")
    assert not (original / "added.txt").exists()


async def test_copy_workdir_without_a_working_dir_raises(tmp_path: Path) -> None:
    """A process without a working directory fails the copy setup."""
    ctx = _make_ctx(tmp_path)

    with pytest.raises(HookError, match="no working directory"):
        await CopyWorkdirHook(0).setup(ctx)


async def test_setup_command_runs_in_the_working_dir(tmp_path: Path) -> None:
    """A setup command runs in the task process's working directory."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = SetupCommandHookSpec(command="touch marker.txt")

    await SetupCommandHook(spec).setup(ctx)

    assert (working_dir / "marker.txt").exists()


async def test_setup_command_after_copy_workdir_runs_in_the_copy(
    tmp_path: Path,
) -> None:
    """A setup command after a copy_workdir setup runs in the copy."""
    original = tmp_path / "original"
    original.mkdir()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch, working_dir=str(original))
    hooks = build_hooks(
        [CopyWorkdirHookSpec(), SetupCommandHookSpec(command="touch marker.txt")]
    )

    for hook in hooks:
        await hook.setup(ctx)

    assert (scratch / "hook-0-workdir" / "marker.txt").exists()
    assert not (original / "marker.txt").exists()


async def test_failing_setup_command_raises_with_the_exit_code(
    tmp_path: Path,
) -> None:
    """A setup command with a nonzero exit raises with the exit code."""
    ctx = _make_ctx(tmp_path, working_dir=str(tmp_path))
    spec = SetupCommandHookSpec(command="exit 3")

    with pytest.raises(HookError, match="exited with code 3"):
        await SetupCommandHook(spec).setup(ctx)


async def test_teardown_command_does_not_run_at_setup(tmp_path: Path) -> None:
    """A teardown command hook runs nothing during setup."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = TeardownCommandHookSpec(command="touch marker.txt")

    await TeardownCommandHook(spec).setup(ctx)

    assert not (working_dir / "marker.txt").exists()


async def test_setup_command_does_not_run_at_teardown(tmp_path: Path) -> None:
    """A setup command hook runs nothing during teardown."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = SetupCommandHookSpec(command="touch marker.txt")

    await SetupCommandHook(spec).teardown(ctx, True)

    assert not (working_dir / "marker.txt").exists()


async def test_teardown_command_runs_on_success(tmp_path: Path) -> None:
    """A teardown command runs after a successful task process."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = TeardownCommandHookSpec(command="touch marker.txt")

    await TeardownCommandHook(spec).teardown(ctx, True)

    assert (working_dir / "marker.txt").exists()


async def test_teardown_command_skipped_on_failure_by_default(
    tmp_path: Path,
) -> None:
    """A failed task process runs no teardown command by default."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = TeardownCommandHookSpec(command="touch marker.txt")

    await TeardownCommandHook(spec).teardown(ctx, False)

    assert not (working_dir / "marker.txt").exists()


async def test_teardown_command_on_failure_runs_only_on_failure(
    tmp_path: Path,
) -> None:
    """An on=failure teardown command runs after a failure and not a success."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = TeardownCommandHookSpec(command="touch marker.txt", on="failure")

    await TeardownCommandHook(spec).teardown(ctx, True)
    assert not (working_dir / "marker.txt").exists()

    await TeardownCommandHook(spec).teardown(ctx, False)
    assert (working_dir / "marker.txt").exists()


async def test_teardown_command_on_always_runs_on_both_outcomes(
    tmp_path: Path,
) -> None:
    """An on=always teardown command runs after either outcome."""
    ctx, working_dir = _make_workdir_ctx(tmp_path)
    spec = TeardownCommandHookSpec(command="touch success.txt", on="always")

    await TeardownCommandHook(spec).teardown(ctx, True)
    assert (working_dir / "success.txt").exists()

    failure_spec = TeardownCommandHookSpec(command="touch failure.txt", on="always")
    await TeardownCommandHook(failure_spec).teardown(ctx, False)
    assert (working_dir / "failure.txt").exists()
