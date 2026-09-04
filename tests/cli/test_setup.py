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

MCP = "/opt/kitaru/bin/kitaru-mcp"


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
    """An isolated home with no clients, no stored server, a user-scope launch."""
    user_home = tmp_path / "home"
    user_home.mkdir()
    monkeypatch.setenv("HOME", str(user_home))
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    monkeypatch.delenv("KITARU_LOCAL_URL", raising=False)
    monkeypatch.setattr(setup_cli.shutil, "which", lambda name: None)
    monkeypatch.setattr(
        setup_cli,
        "resolve_mcp_launch",
        lambda cwd, home: McpLaunch(
            command=MCP, args=(), scope="user", project_dir=None
        ),
    )
    monkeypatch.setattr("kitaru.cli.config.get_server_url", lambda: None, raising=True)

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


def _clis(monkeypatch, **paths: str) -> None:
    monkeypatch.setattr(setup_cli.shutil, "which", lambda name: paths.get(name))


def _fake_claude(calls: list[tuple[str, ...]], *, existing: bool, winner: str):
    """A claude/codex CLI stub; `winner` is what `claude mcp get` reports."""

    async def run(executable: str, *arguments: str) -> ProcessResult:
        calls.append((executable, *arguments))
        if arguments[:2] == ("mcp", "get"):
            # Before the add: only "existing" answers. After: the winner.
            adds = [c for c in calls if c[1:3] == ("mcp", "add")]
            if not adds and not existing:
                return ProcessResult(returncode=1, stdout="", stderr="not found")
            return ProcessResult(returncode=0, stdout=f"kitaru: {winner}", stderr="")
        return ProcessResult(returncode=0, stdout="", stderr="")

    return run


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
    assert not [p for p in agents.iterdir() if p.name.startswith(".")]
    assert item["skills"] == ["kitaru-investigation", "kitaru-replay-experiment"]
    assert item["server_url"] == "http://localhost:8000"
    assert item["mcp_snippet"] == {
        "mcpServers": {
            "kitaru": {
                "command": MCP,
                "args": ["--server", "http://localhost:8000", "--mode", "standard"],
            }
        }
    }
    statuses = {(s["kind"], s["target"]): s["status"] for s in item["steps"]}
    assert statuses == {("skills", str(agents)): "done", ("mcp", "manual"): "skipped"}
    assert result.exit_code == 0
    assert result.warnings == []


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
    assert sorted(p.name for p in skills.iterdir()) == sorted(_SKILLS)
    assert not (home / "escape.md").exists()
    assert not (home / ".agents" / "escape.md").exists()


async def test_oversized_member_is_skipped_and_reported(home: Path, monkeypatch):
    """A file over the size limit is left out and named in the step detail."""
    monkeypatch.setattr(setup_cli, "_MAX_SKILL_FILE_BYTES", 80)

    async def fetch() -> bytes:
        return _archive(
            _SKILLS,
            extra={"kitaru-skills-main/skills/kitaru-investigation/big.md": "x" * 100},
        )

    monkeypatch.setattr(setup_cli, "_fetch_skills_archive", fetch)
    result = await _run(home, register_mcp=False)

    step = result.item["steps"][0]
    assert step["status"] == "done"
    assert "Skipped oversized files: kitaru-investigation/big.md" in step["detail"]
    assert not (
        home / ".agents" / "skills" / "kitaru-investigation" / "big.md"
    ).exists()
    assert result.item["skills"] == ["kitaru-investigation", "kitaru-replay-experiment"]


async def test_claude_and_codex_clients_register_through_their_clis(
    home: Path, monkeypatch
):
    """Detected CLIs get the skills copy and an MCP entry via `mcp add`."""
    _clis(monkeypatch, claude="/bin/claude", codex="/bin/codex")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        setup_cli, "_run_command", _fake_claude(calls, existing=True, winner=MCP)
    )

    result = await _run(home, server="http://localhost:9000", mode="read-only")

    for directory in (".agents", ".claude", ".codex"):
        assert (
            home / directory / "skills" / "kitaru-investigation" / "SKILL.md"
        ).is_file()
    expected = (MCP, "--server", "http://localhost:9000", "--mode", "read-only")
    assert calls == [
        ("/bin/claude", "mcp", "get", "kitaru"),
        ("/bin/claude", "mcp", "remove", "--scope", "user", "kitaru"),
        ("/bin/claude", "mcp", "add", "--scope", "user", "kitaru", "--", *expected),
        ("/bin/claude", "mcp", "get", "kitaru"),
        ("/bin/codex", "mcp", "add", "kitaru", "--", *expected),
    ]
    mcp_steps = [s for s in result.item["steps"] if s["kind"] == "mcp"]
    assert [(s["target"], s["status"]) for s in mcp_steps] == [
        ("Claude Code", "done"),
        ("Codex", "done"),
    ]
    assert result.warnings == []
    assert result.exit_code == 0


async def test_claude_entry_shadowed_by_another_scope_is_reported(
    home: Path, monkeypatch
):
    """When `claude mcp get` still shows another command after the add, fail."""
    _clis(monkeypatch, claude="/bin/claude")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        setup_cli,
        "_run_command",
        _fake_claude(calls, existing=True, winner="/old/kitaru-mcp"),
    )

    result = await _run(home, install_skills=False)

    step = result.item["steps"][0]
    assert step["status"] == "failed"
    assert "another scope still wins" in step["detail"]
    assert result.exit_code == 1


async def test_one_failed_client_among_several_is_a_warning(home: Path, monkeypatch):
    """One failing client does not stop the others or fail the command."""
    _clis(monkeypatch, claude="/bin/claude", codex="/bin/codex")

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


async def test_every_detected_client_failing_exits_nonzero(home: Path):
    """Detected but unconfigurable clients are not reported as 'none detected'."""
    cursor = home / ".cursor"
    cursor.mkdir()
    (cursor / "mcp.json").write_text("not json", encoding="utf-8")

    result = await _run(home, install_skills=False)

    targets = [s["target"] for s in result.item["steps"]]
    assert targets == ["Cursor"]
    assert result.item["steps"][0]["status"] == "failed"
    assert any(
        "could not be registered with any detected" in w for w in result.warnings
    )
    assert result.exit_code == 1


async def test_json_clients_merge_into_existing_config_and_keep_mode(home: Path):
    """Cursor and Windsurf keep other servers, replace only `kitaru`, keep 0600."""
    cursor = home / ".cursor"
    cursor.mkdir()
    config = cursor / "mcp.json"
    config.write_text(
        json.dumps(
            {"mcpServers": {"other": {"command": "x"}, "kitaru": {"command": "old"}}}
        ),
        encoding="utf-8",
    )
    config.chmod(0o600)
    (home / ".codeium" / "windsurf").mkdir(parents=True)

    result = await _run(home, install_skills=False)
    await _run(home, install_skills=False)

    cursor_config = json.loads(config.read_text(encoding="utf-8"))
    assert cursor_config["mcpServers"]["other"] == {"command": "x"}
    assert cursor_config["mcpServers"]["kitaru"] == {
        "command": MCP,
        "args": ["--server", "http://localhost:8000", "--mode", "standard"],
    }
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    assert [p.name for p in cursor.iterdir()] == ["mcp.json"]
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
    _clis(monkeypatch, claude="/bin/claude")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        setup_cli, "_run_command", _fake_claude(calls, existing=False, winner="/bin/uv")
    )

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
        ("/bin/claude", "mcp", "get", "kitaru"),
    ]
    assert (project / ".cursor" / "mcp.json").is_file()
    assert result.item["install"] == "project"
    assert result.item["project_dir"] == str(project)
    assert result.next_actions == [
        "Restart your coding agent so it picks up the new MCP server."
    ]


async def test_server_resolution_matches_other_commands(home: Path, monkeypatch):
    """KITARU_API_URL wins over the stored server; KITARU_LOCAL_URL is the fallback."""
    monkeypatch.setenv("KITARU_LOCAL_URL", "http://localhost:9100")
    result = await _run(home, install_skills=False)
    assert result.item["server_url"] == "http://localhost:9100"

    monkeypatch.setattr(
        "kitaru.cli.config.get_server_url", lambda: "https://team.example"
    )
    result = await _run(home, install_skills=False)
    assert result.item["server_url"] == "https://team.example"

    monkeypatch.setenv("KITARU_API_URL", "https://env.example")
    result = await _run(home, install_skills=False)
    assert result.item["server_url"] == "https://env.example"


async def test_invalid_server_and_mode_are_rejected(home: Path):
    """A malformed --server or an unknown --mode is an argument error."""
    with pytest.raises(CLIError) as error:
        await _run(home, server="not a url", install_skills=False)
    assert error.value.kind == "invalid_arguments"
    with pytest.raises(CLIError) as error:
        await _run(home, mode="yolo")
    assert error.value.kind == "invalid_arguments"


async def test_no_mcp_installs_skills_without_kitaru_mcp(home: Path, monkeypatch):
    """A kitaru[cli]-only install with --no-mcp still gets its skills."""

    def missing(cwd, home):
        raise CLIError(
            "invalid_configuration", "kitaru-mcp is not installed next to this kitaru."
        )

    monkeypatch.setattr(setup_cli, "resolve_mcp_launch", missing)

    result = await _run(home, register_mcp=False)

    assert (home / ".agents" / "skills" / "kitaru-investigation" / "SKILL.md").is_file()
    assert result.item["mcp_snippet"] is None
    assert result.item["server_url"] is None
    assert result.exit_code == 0


async def test_missing_kitaru_mcp_is_a_failed_step_not_an_abort(
    home: Path, monkeypatch
):
    """Skills still install when MCP registration cannot even resolve a launch."""

    def missing(cwd, home):
        raise CLIError(
            "invalid_configuration",
            "kitaru-mcp is not installed next to this kitaru.",
            hint="Install the MCP extra.",
        )

    monkeypatch.setattr(setup_cli, "resolve_mcp_launch", missing)

    result = await _run(home)

    kinds = [(s["kind"], s["status"]) for s in result.item["steps"]]
    assert kinds == [("skills", "done"), ("mcp", "failed")]
    assert "Install the MCP extra." in result.item["steps"][1]["detail"]
    assert result.exit_code == 1


async def test_skill_download_failure_is_reported_per_destination(
    home: Path, monkeypatch
):
    """A download failure marks every destination failed, MCP still proceeds."""

    async def fetch() -> bytes:
        raise CLIError("network_error", "offline", retryable=True)

    monkeypatch.setattr(setup_cli, "_fetch_skills_archive", fetch)

    result = await _run(home)

    skills_steps = [s for s in result.item["steps"] if s["kind"] == "skills"]
    assert [s["status"] for s in skills_steps] == ["failed"]
    assert result.warnings[0].startswith("Skills, ")
    assert result.item["steps"][-1]["target"] == "manual"
    assert result.exit_code == 1


async def test_unwritable_destination_leaves_previous_skill_intact(
    home: Path, monkeypatch
):
    """A write failure in one destination neither destroys its old skill nor others."""
    _clis(monkeypatch, codex="/bin/codex")
    await _run(home, register_mcp=False)
    codex_skill = home / ".codex" / "skills" / "kitaru-investigation"
    assert codex_skill.is_dir()

    real_mkdtemp = setup_cli.tempfile.mkdtemp

    def failing_mkdtemp(*args, **kwargs):
        if str(kwargs.get("dir", "")).startswith(str(home / ".codex")):
            raise OSError("disk full")
        return real_mkdtemp(*args, **kwargs)

    monkeypatch.setattr(setup_cli.tempfile, "mkdtemp", failing_mkdtemp)

    result = await _run(home, register_mcp=False)

    statuses = {s["target"]: s["status"] for s in result.item["steps"]}
    assert statuses[str(home / ".agents" / "skills")] == "done"
    assert statuses[str(home / ".codex" / "skills")] == "failed"
    assert (codex_skill / "SKILL.md").is_file()
    assert result.exit_code == 0


async def test_nothing_to_do_is_a_warning(home: Path):
    """--no-skills --no-mcp does nothing and says so."""
    result = await _run(home, install_skills=False, register_mcp=False)
    assert result.item["steps"] == []
    assert result.warnings == [
        "Nothing to do: both --no-skills and --no-mcp were given."
    ]


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
    monkeypatch.setattr(
        setup_cli.sys, "executable", str(project / ".venv" / "bin" / "python")
    )
    with pytest.raises(CLIError):
        # From outside the project there is no sibling executable either.
        setup_cli.resolve_mcp_launch(tmp_path, tmp_path)


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
