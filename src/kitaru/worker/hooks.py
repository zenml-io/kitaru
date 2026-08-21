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
    GitCloneHook as GitCloneHookSpec,
)
from kitaru.api_models.v1.hook import (
    GitPushHook as GitPushHookSpec,
)
from kitaru.api_models.v1.hook import TaskHook
from kitaru.worker.process import TaskProcess, run_task_process

# Bound on each git command a hook runs.
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


class GitCloneHook(Hook):
    """Git clone hook."""

    def __init__(self, spec: GitCloneHookSpec, index: int) -> None:
        """Initialize the hook.

        Args:
            spec: Hook spec.
            index: Position of the hook in the spec's hook list.
        """
        self._spec = spec
        self._index = index

    async def setup(self, ctx: HookContext) -> None:
        """Clone the repository and point the process at the clone.

        Args:
            ctx: Hook context.

        Raises:
            HookError: A git command did not exit successfully.
        """
        dest = ctx.scratch_dir / f"hook-{self._index}-clone"
        await _run_git_command(
            ctx,
            ["git", "clone", self._spec.url, str(dest)],
            None,
            "clone the repository",
        )
        if self._spec.ref is not None:
            await _run_git_command(
                ctx,
                ["git", "checkout", self._spec.ref],
                str(dest),
                "check out the ref",
            )
        ctx.process = ctx.process._replace(working_dir=str(dest))


class GitPushHook(Hook):
    """Git push hook."""

    def __init__(self, spec: GitPushHookSpec) -> None:
        """Initialize the hook.

        Args:
            spec: Hook spec.
        """
        self._spec = spec

    async def teardown(self, ctx: HookContext, success: bool) -> None:
        """Commit and push the working directory changes on success.

        Args:
            ctx: Hook context.
            success: Whether the task process exited with code 0.

        Raises:
            HookError: A git command did not exit successfully.
        """
        if not success:
            return
        working_dir = ctx.process.working_dir
        await _run_git_command(
            ctx, ["git", "add", "-A"], working_dir, "stage the changes"
        )
        staged = await _run_git_command(
            ctx,
            ["git", "diff", "--cached", "--quiet"],
            working_dir,
            "check for staged changes",
            expected_returncodes=(0, 1),
        )
        if staged == 0:
            return
        branch = self._spec.branch or f"kitaru/task-{ctx.task_id}"
        await _run_git_command(
            ctx,
            ["git", "commit", "-m", f"Kitaru task {ctx.task_id}"],
            working_dir,
            "commit the changes",
        )
        # Qualify the destination because a detached HEAD, left behind by a
        # ref checkout, cannot resolve an unqualified new branch name.
        await _run_git_command(
            ctx,
            ["git", "push", "origin", f"HEAD:refs/heads/{branch}"],
            working_dir,
            "push the changes",
        )


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
        elif isinstance(spec, GitCloneHookSpec):
            hooks.append(GitCloneHook(spec, index))
        else:
            hooks.append(GitPushHook(spec))
    return hooks


async def _run_git_command(
    ctx: HookContext,
    command: list[str],
    working_dir: str | None,
    purpose: str,
    expected_returncodes: tuple[int, ...] = (0,),
) -> int:
    """Run a git command, raising on an unexpected outcome.

    Args:
        ctx: Hook context.
        command: Command to run.
        working_dir: Directory the command runs in.
        purpose: Action named in the error message.
        expected_returncodes: Exit codes that are not an error.

    Raises:
        HookError: The command was canceled, timed out, or exited with an
            unexpected code.

    Returns:
        Exit code.
    """
    process = TaskProcess(
        command=command,
        working_dir=working_dir,
        env=ctx.process.env,
        timeout_seconds=HOOK_TIMEOUT_SECONDS,
    )
    result = await run_task_process(process, ctx.canceled)
    if result.returncode is None:
        outcome = "was canceled" if ctx.canceled.is_set() else "timed out"
        raise HookError(f"Command to {purpose} {outcome}.")
    if result.returncode not in expected_returncodes:
        message = f"Command to {purpose} exited with code {result.returncode}."
        if result.tail:
            message = f"{message}\n\n{result.tail}"
        raise HookError(message)
    return result.returncode
