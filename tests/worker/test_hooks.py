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
import subprocess
import uuid
from pathlib import Path

import pytest

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
from kitaru.worker.hooks import (
    CopyWorkdirHook,
    GitCloneHook,
    GitPushHook,
    HookContext,
    HookError,
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


def _run_git(cwd: Path, *args: str) -> str:
    """Run a git command in a directory and return its stripped stdout."""
    result = subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _configure_git_identity(repo: Path) -> None:
    """Set a per-repo committer identity so commits need no global config."""
    _run_git(repo, "config", "user.email", "worker@example.com")
    _run_git(repo, "config", "user.name", "Kitaru Worker")


def _init_repo(path: Path) -> Path:
    """Initialize a git repository with a committer identity."""
    path.mkdir()
    _run_git(path, "init", "--quiet")
    _configure_git_identity(path)
    return path


def _commit_file(repo: Path, name: str, content: str) -> str:
    """Write a file, commit it, and return the commit SHA."""
    (repo / name).write_text(content)
    _run_git(repo, "add", "-A")
    _run_git(repo, "commit", "--quiet", "-m", f"Add {name}")
    return _run_git(repo, "rev-parse", "HEAD")


def _make_origin(tmp_path: Path) -> Path:
    """Build a bare repository with one commit to push to and clone from."""
    seed = _init_repo(tmp_path / "seed")
    _commit_file(seed, "README.md", "seed\n")
    bare = tmp_path / "origin.git"
    _run_git(tmp_path, "clone", "--quiet", "--bare", str(seed), str(bare))
    return bare


def _clone(origin: Path, dest: Path) -> Path:
    """Clone a repository and set a per-repo committer identity."""
    _run_git(dest.parent, "clone", "--quiet", str(origin), str(dest))
    _configure_git_identity(dest)
    return dest


def test_build_hooks_maps_each_spec_type_in_order() -> None:
    """Each spec type builds its implementation in declared order."""
    specs: list[TaskHook] = [
        GitCloneHookSpec(url="https://example.com/repo.git"),
        CopyWorkdirHookSpec(),
        GitPushHookSpec(),
    ]

    hooks = build_hooks(specs)

    assert [type(hook) for hook in hooks] == [
        GitCloneHook,
        CopyWorkdirHook,
        GitPushHook,
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


async def test_copy_workdir_after_git_clone_copies_the_clone(tmp_path: Path) -> None:
    """A copy_workdir setup after a git_clone setup copies the clone."""
    repo = _init_repo(tmp_path / "repo")
    _commit_file(repo, "app.py", "print('hi')\n")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch)
    hooks = build_hooks([GitCloneHookSpec(url=str(repo)), CopyWorkdirHookSpec()])

    for hook in hooks:
        await hook.setup(ctx)

    assert ctx.process.working_dir == str(scratch / "hook-1-workdir")
    assert (scratch / "hook-1-workdir" / "app.py").read_text() == "print('hi')\n"


async def test_git_clone_clones_and_rewires_the_working_dir(tmp_path: Path) -> None:
    """The setup clones the repository and points the process at the clone."""
    repo = _init_repo(tmp_path / "repo")
    _commit_file(repo, "app.py", "print('hi')\n")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch)

    await GitCloneHook(GitCloneHookSpec(url=str(repo)), 0).setup(ctx)

    clone = scratch / "hook-0-clone"
    assert ctx.process.working_dir == str(clone)
    assert (clone / "app.py").read_text() == "print('hi')\n"


async def test_git_clone_checks_out_the_ref(tmp_path: Path) -> None:
    """A spec ref is checked out after the clone."""
    repo = _init_repo(tmp_path / "repo")
    first = _commit_file(repo, "app.py", "one\n")
    _commit_file(repo, "app.py", "two\n")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch)

    await GitCloneHook(GitCloneHookSpec(url=str(repo), ref=first), 0).setup(ctx)

    assert (scratch / "hook-0-clone" / "app.py").read_text() == "one\n"


async def test_git_clone_of_a_missing_path_raises_with_the_exit_code(
    tmp_path: Path,
) -> None:
    """A clone of a nonexistent path raises with the git exit code."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch)
    spec = GitCloneHookSpec(url=str(tmp_path / "missing"))

    with pytest.raises(HookError, match="exited with code 128"):
        await GitCloneHook(spec, 0).setup(ctx)


async def test_git_push_teardown_does_nothing_without_success(tmp_path: Path) -> None:
    """A failed task process pushes nothing."""
    origin = _make_origin(tmp_path)
    work = _clone(origin, tmp_path / "work")
    (work / "README.md").write_text("changed\n")
    ctx = _make_ctx(tmp_path, working_dir=str(work))

    await GitPushHook(GitPushHookSpec()).teardown(ctx, False)

    assert _run_git(origin, "branch", "--list", f"kitaru/task-{ctx.task_id}") == ""


async def test_git_push_pushes_a_task_commit_to_the_task_branch(
    tmp_path: Path,
) -> None:
    """A successful task's changes are committed and pushed to the task branch."""
    origin = _make_origin(tmp_path)
    work = _clone(origin, tmp_path / "work")
    (work / "README.md").write_text("changed\n")
    ctx = _make_ctx(tmp_path, working_dir=str(work))

    await GitPushHook(GitPushHookSpec()).teardown(ctx, True)

    branch = f"kitaru/task-{ctx.task_id}"
    assert _run_git(origin, "log", "-1", "--format=%s", branch) == (
        f"Kitaru task {ctx.task_id}"
    )


async def test_git_push_pushes_to_the_explicit_branch(tmp_path: Path) -> None:
    """A spec branch overrides the default task branch."""
    origin = _make_origin(tmp_path)
    work = _clone(origin, tmp_path / "work")
    (work / "README.md").write_text("changed\n")
    ctx = _make_ctx(tmp_path, working_dir=str(work))

    await GitPushHook(GitPushHookSpec(branch="results")).teardown(ctx, True)

    assert _run_git(origin, "log", "-1", "--format=%s", "results") == (
        f"Kitaru task {ctx.task_id}"
    )


async def test_git_push_without_changes_pushes_nothing(tmp_path: Path) -> None:
    """A clean working directory pushes no branch."""
    origin = _make_origin(tmp_path)
    work = _clone(origin, tmp_path / "work")
    ctx = _make_ctx(tmp_path, working_dir=str(work))

    await GitPushHook(GitPushHookSpec()).teardown(ctx, True)

    assert _run_git(origin, "branch", "--list", f"kitaru/task-{ctx.task_id}") == ""


async def test_git_push_after_a_detached_head_checkout(tmp_path: Path) -> None:
    """A push succeeds from the detached HEAD a ref checkout leaves behind."""
    origin = _make_origin(tmp_path)
    sha = _run_git(origin, "rev-parse", "HEAD")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ctx = _make_ctx(scratch)
    await GitCloneHook(GitCloneHookSpec(url=str(origin), ref=sha), 0).setup(ctx)
    clone = scratch / "hook-0-clone"
    _configure_git_identity(clone)
    (clone / "README.md").write_text("changed\n")

    await GitPushHook(GitPushHookSpec()).teardown(ctx, True)

    branch = f"kitaru/task-{ctx.task_id}"
    assert _run_git(origin, "log", "-1", "--format=%s", branch) == (
        f"Kitaru task {ctx.task_id}"
    )
