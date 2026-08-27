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
"""Task hook execution around the task process."""

import asyncio
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

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
from kitaru.worker.process import TaskProcess, run_task_process

# Bound on each command a hook runs.
HOOK_TIMEOUT_SECONDS = 600


class HookError(Exception):
    """Hook execution error."""


@dataclass
class HookContext:
    """Hook context."""

    task_id: uuid.UUID
    scratch_dir: Path
    canceled: asyncio.Event
    process: TaskProcess


class Hook:
    """Task hook."""

    async def setup(self, ctx: HookContext) -> None:
        """Run before the task process starts.

        Args:
            ctx: Hook context.
        """

    async def teardown(self, ctx: HookContext, success: bool) -> None:
        """Run after the task process exits.

        Args:
            ctx: Hook context.
            success: Whether the task process exited with code 0.
        """


class CopyWorkdirHook(Hook):
    """Copy workdir hook."""

    def __init__(self, index: int) -> None:
        """Initialize the hook.

        Args:
            index: Position of the hook in the spec's hook list.
        """
        self._index = index

    async def setup(self, ctx: HookContext) -> None:
        """Copy the working directory and point the process at the copy.

        Args:
            ctx: Hook context.

        Raises:
            HookError: The task process has no working directory.
        """
        if ctx.process.working_dir is None:
            raise HookError("The task process has no working directory to copy.")
        dest = ctx.scratch_dir / f"hook-{self._index}-workdir"
        await asyncio.to_thread(
            shutil.copytree, ctx.process.working_dir, dest, symlinks=True
        )
        ctx.process = ctx.process._replace(working_dir=str(dest))


class SetupCommandHook(Hook):
    """Setup command hook."""

    def __init__(self, spec: SetupCommandHookSpec) -> None:
        """Initialize the hook.

        Args:
            spec: Hook spec.
        """
        self._spec = spec

    async def setup(self, ctx: HookContext) -> None:
        """Run the command before the task process starts.

        Args:
            ctx: Hook context.

        Raises:
            HookError: The command did not exit successfully.
        """
        await _run_command(ctx, self._spec.command)


class TeardownCommandHook(Hook):
    """Teardown command hook."""

    def __init__(self, spec: TeardownCommandHookSpec) -> None:
        """Initialize the hook.

        Args:
            spec: Hook spec.
        """
        self._spec = spec

    async def teardown(self, ctx: HookContext, success: bool) -> None:
        """Run the command after the task process exits on a matching outcome.

        Args:
            ctx: Hook context.
            success: Whether the task process exited with code 0.

        Raises:
            HookError: The command did not exit successfully.
        """
        if success and self._spec.on == "failure":
            return
        if not success and self._spec.on == "success":
            return
        await _run_command(ctx, self._spec.command)


def build_hooks(specs: list[TaskHook]) -> list[Hook]:
    """Build the hook implementations for a task's hook specs.

    Args:
        specs: Hook specs in declared order.

    Returns:
        Hook implementations in declared order.
    """
    hooks: list[Hook] = []
    for index, spec in enumerate(specs):
        if isinstance(spec, CopyWorkdirHookSpec):
            hooks.append(CopyWorkdirHook(index))
        elif isinstance(spec, SetupCommandHookSpec):
            hooks.append(SetupCommandHook(spec))
        else:
            hooks.append(TeardownCommandHook(spec))
    return hooks


async def _run_command(ctx: HookContext, command: str) -> None:
    """Run a hook command in the task process's working directory.

    Args:
        ctx: Hook context.
        command: Shell command to run.

    Raises:
        HookError: The command was canceled, timed out, or exited with a
            nonzero code.
    """
    process = TaskProcess(
        command=command,
        working_dir=ctx.process.working_dir,
        env=ctx.process.env,
        timeout_seconds=HOOK_TIMEOUT_SECONDS,
    )
    result = await run_task_process(process, ctx.canceled)
    if result.returncode is None:
        outcome = "was canceled" if ctx.canceled.is_set() else "timed out"
        raise HookError(f"Command {outcome}.")
    if result.returncode != 0:
        message = f"Command exited with code {result.returncode}."
        if result.tail:
            message = f"{message}\n\n{result.tail}"
        raise HookError(message)
