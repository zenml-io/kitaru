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
"""`kitaru setup`: skills install and MCP registration per client."""

import io
import json
import os
import stat
import tarfile
from pathlib import Path

import pytest

from kitaru.cli import setup as setup_cli
from kitaru.cli.output import CLIError
from kitaru.cli.setup import McpLaunch, ProcessResult


def _archive(
    skills: dict[str, dict[str, str]], *, extra: dict[str, str] | None = None
) -> bytes:
    """Build a GitHub-style repository tarball with the given skills."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        entries = {
            f"kitaru-skills-main/skills/{name}/{file}": content
            for name, files in skills.items()
            for file, content in files.items()
        }
        entries.update(extra or {})
        for path, content in entries.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


_SKILLS = {
    "kitaru-investigation": {
        "SKILL.md": "---\nname: kitaru-investigation\ndescription: Investigate.\n---\n",
        "reference/steps.md": "steps",
    },
    "kitaru-replay-experiment": {
        "SKILL.md": "---\nname: kitaru-replay-experiment\ndescription: Replay.\n---\n",
    },
}


@pytest.fixture
def home(tmp_path: Path, monkeypatch) -> Path:
    """An isolated home with no clients and a user-scope launch."""
    user_home = tmp_path / "home"
    user_home.mkdir()
    monkeypatch.setenv("HOME", str(user_home))
    monkeypatch.setattr(setup_cli.shutil, "which", lambda name: None)
    monkeypatch.setattr(
        setup_cli,
        "resolve_mcp_launch",
        lambda cwd, home: McpLaunch(
            command="/opt/kitaru/bin/kitaru-mcp",
            args=(),
            scope="user",
            project_dir=None,
        ),
    )
    monkeypatch.setattr(setup_cli, "get_server_url", lambda: None)

    async def fetch() -> bytes:
        return _archive(_SKILLS)

    monkeypatch.setattr(setup_cli, "_fetch_skills_archive", fetch)
    return user_home


async def _run(home: Path, **overrides):
    """Run setup with defaults against the isolated home."""
    options = {
        "server": None,
        "mode": "standard",
        "install_skills": True,
        "register_mcp": True,
        "cwd": home / "work",
        "home": home,
    }
    options.update(overrides)
    (home / "work").mkdir(exist_ok=True)
    return await setup_cli.setup(**options)


async def test_installs_skills_into_agents_dir_and_reports_manual_snippet(home: Path):
    """With no client detected, skills land in ~/.agents and the snippet is printed."""
    result = await _run(home)

    item = result.item
    agents = home / ".agents" / "skills"
    assert (agents / "kitaru-investigation" / "SKILL.md").is_file()
    assert (
        agents / "kitaru-investigation" / "reference" / "steps.md"
    ).read_text() == "steps"
    assert (agents / "kitaru-replay-experiment" / "SKILL.md").is_file()
    assert not (home / ".claude").exists()
    assert item["skills"] == [
        "kitaru-investigation",
        "kitaru-replay-experiment",
    ]
    assert item["server_url"] == "http://localhost:8000"
    assert item["mcp_snippet"] == {
        "mcpServers": {
            "kitaru": {
                "command": "/opt/kitaru/bin/kitaru-mcp",
                "args": ["--server", "http://localhost:8000", "--mode", "standard"],
            }
        }
    }
    statuses = {
        (step["kind"], step["target"]): step["status"] for step in item["steps"]
    }
    assert statuses == {("skills", str(agents)): "done", ("mcp", "manual"): "skipped"}
    assert result.exit_code == 0


async def test_rerun_replaces_stale_skill_files(home: Path):
    """A re-run removes files the previous skill version shipped."""
    await _run(home)
    stale = home / ".agents" / "skills" / "kitaru-investigation" / "old.md"
    stale.write_text("stale", encoding="utf-8")

    await _run(home)

    assert not stale.exists()
    assert (stale.parent / "SKILL.md").is_file()


async def test_archive_entries_outside_skills_are_ignored(home: Path, monkeypatch):
    """Path traversal, top-level files, and skill dirs without SKILL.md are dropped."""

    async def fetch() -> bytes:
        return _archive(
            _SKILLS,
            extra={
                "kitaru-skills-main/README.md": "readme",
                "kitaru-skills-main/skills/../../escape.md": "bad",
                "kitaru-skills-main/skills/no-manifest/notes.md": "no manifest",
            },
        )

    monkeypatch.setattr(setup_cli, "_fetch_skills_archive", fetch)
    await _run(home)

    skills = home / ".agents" / "skills"
    assert sorted(path.name for path in skills.iterdir()) == sorted(_SKILLS)
    assert not (home / "escape.md").exists()
    assert not (home / ".agents" / "escape.md").exists()


async def test_claude_and_codex_clients_register_through_their_clis(
    home: Path, monkeypatch
):
    """Detected CLIs get the skills copy and an MCP entry via `mcp add`."""
    monkeypatch.setattr(
        setup_cli.shutil,
        "which",
        lambda name: {"claude": "/bin/claude", "codex": "/bin/codex"}.get(name),
    )
    calls: list[tuple[str, ...]] = []

    async def run(executable: str, *arguments: str) -> ProcessResult:
        calls.append((executable, *arguments))
        if arguments[:2] == ("mcp", "get"):
            return ProcessResult(returncode=0, stdout="kitaru: ...", stderr="")
        return ProcessResult(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(setup_cli, "_run_command", run)

    result = await _run(home, server="http://localhost:9000", mode="read-only")

    for directory in (".agents", ".claude", ".codex"):
        assert (
            home / directory / "skills" / "kitaru-investigation" / "SKILL.md"
        ).is_file()
    expected = (
        "/opt/kitaru/bin/kitaru-mcp",
        "--server",
        "http://localhost:9000",
        "--mode",
        "read-only",
    )
    assert calls == [
        ("/bin/claude", "mcp", "get", "kitaru"),
        ("/bin/claude", "mcp", "remove", "--scope", "user", "kitaru"),
        ("/bin/claude", "mcp", "add", "--scope", "user", "kitaru", "--", *expected),
        ("/bin/codex", "mcp", "add", "kitaru", "--", *expected),
    ]
    mcp_steps = [s for s in result.item["steps"] if s["kind"] == "mcp"]
    assert [(s["target"], s["status"]) for s in mcp_steps] == [
        ("Claude Code", "done"),
        ("Codex", "done"),
    ]
    assert result.warnings == []


async def test_failed_client_registration_is_a_warning_not_an_error(
    home: Path, monkeypatch
):
    """One failing client does not stop the others or fail the command."""
    monkeypatch.setattr(
        setup_cli.shutil,
        "which",
        lambda name: {"claude": "/bin/claude", "codex": "/bin/codex"}.get(name),
    )

    async def run(executable: str, *arguments: str) -> ProcessResult:
        if executable == "/bin/claude":
            return ProcessResult(
                returncode=1, stdout="", stderr="boom\nclaude exploded"
            )
        return ProcessResult(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(setup_cli, "_run_command", run)

    result = await _run(home, install_skills=False)

    steps = {s["target"]: s for s in result.item["steps"]}
    assert steps["Claude Code"]["status"] == "failed"
    assert steps["Claude Code"]["detail"] == "exit 1: claude exploded"
    assert steps["Codex"]["status"] == "done"
    assert result.warnings == ["Claude Code: exit 1: claude exploded"]
    assert result.exit_code == 0


async def test_json_clients_merge_into_existing_config(home: Path):
    """Cursor and Windsurf keep other servers and replace only `kitaru`."""
    cursor = home / ".cursor"
    cursor.mkdir()
    (cursor / "mcp.json").write_text(
        json.dumps(
            {"mcpServers": {"other": {"command": "x"}, "kitaru": {"command": "old"}}}
        ),
        encoding="utf-8",
    )
    (home / ".codeium" / "windsurf").mkdir(parents=True)

    result = await _run(home, install_skills=False)
    await _run(home, install_skills=False)

    cursor_config = json.loads((cursor / "mcp.json").read_text(encoding="utf-8"))
    assert cursor_config["mcpServers"]["other"] == {"command": "x"}
    assert cursor_config["mcpServers"]["kitaru"] == {
        "command": "/opt/kitaru/bin/kitaru-mcp",
        "args": ["--server", "http://localhost:8000", "--mode", "standard"],
    }
    windsurf_config = json.loads(
        (home / ".codeium" / "windsurf" / "mcp_config.json").read_text(encoding="utf-8")
    )
    assert set(windsurf_config["mcpServers"]) == {"kitaru"}
    assert [(s["target"], s["status"]) for s in result.item["steps"]] == [
        ("Cursor", "done"),
        ("Windsurf", "done"),
    ]


async def test_project_install_uses_uv_run_and_project_scope(home: Path, monkeypatch):
    """A project launch registers Claude in project scope and Cursor in the repo."""
    project = home / "repo"
    project.mkdir()
    (home / ".cursor").mkdir()
    monkeypatch.setattr(
        setup_cli,
        "resolve_mcp_launch",
        lambda cwd, home: McpLaunch(
            command="/bin/uv",
            args=("run", "--directory", str(project), "kitaru-mcp"),
            scope="project",
            project_dir=project,
        ),
    )
    monkeypatch.setattr(
        setup_cli.shutil,
        "which",
        lambda name: "/bin/claude" if name == "claude" else None,
    )
    calls: list[tuple[str, ...]] = []

    async def run(executable: str, *arguments: str) -> ProcessResult:
        calls.append((executable, *arguments))
        return ProcessResult(
            returncode=1 if arguments[:2] == ("mcp", "get") else 0, stdout="", stderr=""
        )

    monkeypatch.setattr(setup_cli, "_run_command", run)

    result = await _run(home, install_skills=False, cwd=project)

    assert calls == [
        ("/bin/claude", "mcp", "get", "kitaru"),
        (
            "/bin/claude",
            "mcp",
            "add",
            "--scope",
            "project",
            "kitaru",
            "--",
            "/bin/uv",
            "run",
            "--directory",
            str(project),
            "kitaru-mcp",
            "--server",
            "http://localhost:8000",
            "--mode",
            "standard",
        ),
    ]
    assert (project / ".cursor" / "mcp.json").is_file()
    assert result.item["install"] == "project"
    assert result.item["project_dir"] == str(project)
    assert result.next_actions == [
        "Restart your coding agent so it picks up the new MCP server."
    ]


async def test_stored_server_is_the_default_target(home: Path, monkeypatch):
    """The MCP server follows the CLI's selected server unless --server is given."""
    monkeypatch.setattr(setup_cli, "get_server_url", lambda: "https://team.example")

    result = await _run(home, install_skills=False)

    assert result.item["server_url"] == "https://team.example"


async def test_invalid_server_is_rejected(home: Path):
    """A malformed --server is an argument error."""
    with pytest.raises(CLIError) as error:
        await _run(home, server="not a url", install_skills=False)
    assert error.value.kind == "invalid_arguments"


async def test_skill_download_failure_is_reported_per_destination(
    home: Path, monkeypatch
):
    """A download failure marks every destination failed and still registers MCP."""

    async def fetch() -> bytes:
        raise CLIError("network_error", "offline", retryable=True)

    monkeypatch.setattr(setup_cli, "_fetch_skills_archive", fetch)

    result = await _run(home)

    skills_steps = [s for s in result.item["steps"] if s["kind"] == "skills"]
    assert [s["status"] for s in skills_steps] == ["failed"]
    assert result.warnings == ["Skills were not installed: offline"]
    assert result.item["steps"][-1]["target"] == "manual"


def test_resolve_mcp_launch_project_mode(tmp_path: Path, monkeypatch):
    """A .venv under a pyproject, run from inside the project, launches via uv run."""
    project = tmp_path / "repo"
    (project / ".venv" / "bin").mkdir(parents=True)
    (project / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    monkeypatch.setattr(setup_cli.sys, "prefix", str(project / ".venv"))
    monkeypatch.setattr(
        setup_cli.shutil, "which", lambda name: "/bin/uv" if name == "uv" else None
    )

    launch = setup_cli.resolve_mcp_launch(project / "src", tmp_path)

    assert launch == McpLaunch(
        command="/bin/uv",
        args=("run", "--directory", str(project), "kitaru-mcp"),
        scope="project",
        project_dir=project,
    )
    outside = setup_cli.resolve_mcp_launch
    with pytest.raises(CLIError):
        # From outside the project there is no sibling executable either.
        monkeypatch.setattr(
            setup_cli.sys, "executable", str(project / ".venv" / "bin" / "python")
        )
        outside(tmp_path, tmp_path)


def test_resolve_mcp_launch_user_mode_uses_sibling_executable(
    tmp_path: Path, monkeypatch
):
    """A tool install points at the absolute kitaru-mcp next to the interpreter."""
    bin_dir = tmp_path / "tools" / "kitaru" / "bin"
    bin_dir.mkdir(parents=True)
    mcp = bin_dir / "kitaru-mcp"
    mcp.write_text("#!/bin/sh\n", encoding="utf-8")
    mcp.chmod(mcp.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(setup_cli.sys, "prefix", str(bin_dir.parent))
    monkeypatch.setattr(setup_cli.sys, "executable", str(bin_dir / "python"))

    launch = setup_cli.resolve_mcp_launch(tmp_path, tmp_path)

    assert launch.scope == "user"
    assert launch.command == str(mcp)
    assert launch.args == ()
    assert os.access(launch.command, os.X_OK)
