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
"""Wire Kitaru skills and the MCP server into installed coding agents.

`kitaru setup` is the re-runnable half of the one-line installer. It installs
the agent skills from `zenml-io/kitaru-skills` into every skill location a
detected client reads, then registers `kitaru-mcp` with every client that can
be configured programmatically. Re-running it after installing a new coding
agent wires that agent up too. Every write replaces the previous Kitaru entry,
so repeated runs update rather than duplicate.
"""

import asyncio
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import httpx

from kitaru.cli.config import resolve_target
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.skill_discovery import SKILLS_URL, get_kitaru_skill_status

SKILLS_ARCHIVE_URL = f"{SKILLS_URL}/archive/refs/heads/main.tar.gz"
DEFAULT_SERVER_URL = "http://localhost:8000"
LOCAL_URL_ENV = "KITARU_LOCAL_URL"
MCP_SERVER_NAME = "kitaru"
McpMode = Literal["read-only", "standard", "destructive"]
MCP_MODES: tuple[McpMode, ...] = ("read-only", "standard", "destructive")

_MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
_MAX_SKILL_FILE_BYTES = 4 * 1024 * 1024
_COMMAND_TIMEOUT = 60.0


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Captured subprocess result."""

    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class McpLaunch:
    """How a client should start the Kitaru MCP server."""

    command: str
    args: tuple[str, ...]
    scope: Literal["project", "user"]
    project_dir: Path | None

    def as_json(self) -> dict[str, Any]:
        """Return the entry shape file-configured MCP clients expect."""
        return {"command": self.command, "args": list(self.args)}


@dataclass(frozen=True, slots=True)
class ExtractedSkills:
    """Skills read out of the repository tarball."""

    files: dict[str, dict[str, bytes]]
    skipped: list[str]


async def setup(
    *,
    server: str | None,
    mode: str,
    install_skills: bool,
    register_mcp: bool,
    cwd: Path | None = None,
    home: Path | None = None,
) -> CommandResult:
    """Install skills and register the MCP server with detected clients.

    Args:
        server: Server URL the MCP server should target. Defaults to the
            same resolution every other command uses (``KITARU_API_URL``,
            then the stored server), then ``KITARU_LOCAL_URL``, then the
            local Docker server.
        mode: MCP capability mode.
        install_skills: Whether to install the agent skills.
        register_mcp: Whether to register the MCP server.
        cwd: Working directory; defaults to the process working directory.
        home: User home; defaults to the current user's home.

    Returns:
        One step per client or location with its outcome, plus next actions.
        The exit code is 1 when a detected client could not be configured or
        no skill destination could be written.

    Raises:
        CLIError: If the mode or server URL is invalid.
    """
    if mode not in MCP_MODES:
        raise CLIError(
            "invalid_arguments",
            f"--mode must be one of {', '.join(MCP_MODES)}.",
        )
    current = (cwd or Path.cwd()).absolute()
    user_home = (home or Path.home()).absolute()
    steps: list[dict[str, Any]] = []
    warnings: list[str] = []
    exit_code = 0

    if not install_skills and not register_mcp:
        warnings.append("Nothing to do: both --no-skills and --no-mcp were given.")

    if install_skills:
        destinations = skill_destinations(user_home)
        outcomes = await _install_skills(destinations)
        written = 0
        for destination, outcome in zip(destinations, outcomes, strict=True):
            steps.append(outcome)
            if outcome["status"] == "done":
                written += 1
            else:
                warnings.append(f"Skills, {destination}: {outcome['detail']}")
        if written == 0:
            exit_code = 1

    # The launch is only needed for MCP registration. A kitaru[cli]-only
    # install with --no-mcp must still get its skills.
    launch: McpLaunch | None = None
    server_url: str | None = None
    snippet: dict[str, Any] | None = None
    if register_mcp:
        server_url = _resolve_server_url(server)
        try:
            launch = resolve_mcp_launch(current, user_home)
        except CLIError as error:
            detail = error.message + (f" {error.hint}" if error.hint else "")
            steps.append(_step("mcp", "kitaru-mcp", "failed", detail))
            warnings.append(f"MCP server not registered: {detail}")
            exit_code = 1
        else:
            mcp_args = (*launch.args, "--server", server_url, "--mode", mode)
            snippet = {
                "mcpServers": {
                    MCP_SERVER_NAME: {
                        "command": launch.command,
                        "args": list(mcp_args),
                    }
                }
            }
            clients = detect_mcp_clients(current, user_home, launch)
            registered = 0
            for client in clients:
                step = await client.register(launch.command, mcp_args)
                steps.append(step)
                if step["status"] == "done":
                    registered += 1
                else:
                    warnings.append(f"{client.name}: {step['detail']}")
            if not clients:
                steps.append(
                    _step(
                        "mcp",
                        "manual",
                        "skipped",
                        "No configurable MCP client detected. Add the snippet "
                        "below (mcp_snippet in JSON output) to your client's "
                        "MCP configuration.",
                    )
                )
            elif registered == 0:
                warnings.append(
                    "The MCP server could not be registered with any detected "
                    "client. Fix the errors above and run `kitaru setup` again."
                )
                exit_code = 1

    scope = launch.scope if launch else _scope_only(current)
    result_item: dict[str, Any] = {
        "install": scope,
        "project_dir": str(launch.project_dir)
        if launch and launch.project_dir
        else None,
        "server_url": server_url,
        "mode": mode,
        "skills": get_kitaru_skill_status(cwd=current, home=user_home)["skills"],
        "steps": steps,
        "mcp_snippet": snippet,
    }
    next_actions: list[str] = []
    if register_mcp and launch is not None:
        next_actions.append(
            "Restart your coding agent so it picks up the new MCP server."
        )
    if scope == "user":
        next_actions.append(
            "Replays need Kitaru inside the agent's own project: run "
            "`kitaru setup` again from that repository after adding it there."
        )
    return CommandResult(
        item=result_item,
        warnings=warnings,
        next_actions=next_actions,
        exit_code=exit_code,
    )


# ---------------------------------------------------------------------------
# Launch resolution
# ---------------------------------------------------------------------------


def resolve_mcp_launch(cwd: Path, home: Path) -> McpLaunch:
    """Decide how clients should launch kitaru-mcp for this installation.

    A project install (this interpreter lives in a project's virtual
    environment) launches through `uv run --directory <project>` so the
    client gets the project's environment without activating it, and Claude
    Code gets a project-scoped entry. Any other install points at the
    kitaru-mcp executable next to this interpreter, by absolute path, so a
    client that does not share the user's PATH still finds it.
    """
    project_dir = _find_project_dir(Path(sys.prefix), cwd)
    uv = shutil.which("uv")
    if project_dir is not None and uv is not None:
        return McpLaunch(
            command=uv,
            args=("run", "--directory", str(project_dir), "kitaru-mcp"),
            scope="project",
            project_dir=project_dir,
        )
    executable = _find_sibling_executable("kitaru-mcp")
    if executable is None:
        raise CLIError(
            "invalid_configuration",
            "kitaru-mcp is not installed next to this kitaru.",
            hint='Install the MCP extra: uv add "kitaru[cli,mcp,worker]" '
            'or uv tool install "kitaru[cli,mcp,worker]".',
        )
    return McpLaunch(command=str(executable), args=(), scope="user", project_dir=None)


def _scope_only(cwd: Path) -> Literal["project", "user"]:
    """Report the install scope without requiring kitaru-mcp to exist."""
    if _find_project_dir(Path(sys.prefix), cwd) is not None:
        return "project"
    return "user"


def _find_project_dir(prefix: Path, cwd: Path) -> Path | None:
    """Return the project a virtual environment belongs to, if any."""
    if prefix.name != ".venv":
        return None
    project = prefix.parent
    if not (project / "pyproject.toml").is_file():
        return None
    # The working directory must be inside the project, otherwise a setup
    # run from elsewhere would register a project-scoped entry in the wrong
    # place.
    try:
        cwd.relative_to(project)
    except ValueError:
        return None
    return project


def _find_sibling_executable(name: str) -> Path | None:
    """Find an executable in the directory of the running interpreter."""
    directory = Path(sys.executable).parent
    for candidate in (directory / name, directory / f"{name}.exe"):
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _resolve_server_url(explicit: str | None) -> str:
    """Pick the server URL the MCP server should target.

    Uses the same resolution as every other command (explicit option,
    ``KITARU_API_URL``, stored server), then ``KITARU_LOCAL_URL`` as the
    installer always has, then the default local Docker server.
    """
    try:
        return resolve_target(explicit_server=explicit).server_url
    except CLIError as error:
        if error.kind != "invalid_configuration" or explicit is not None:
            raise
        if "No Kitaru server was resolved" not in error.message:
            raise
    local = os.environ.get(LOCAL_URL_ENV)
    if local:
        return resolve_target(explicit_server=local).server_url
    return DEFAULT_SERVER_URL


# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------


def skill_destinations(home: Path) -> list[Path]:
    """Return every skill directory a detected client reads.

    `~/.agents/skills` is the cross-agent location and always included.
    Client-specific directories are added when that client's CLI is on PATH
    or its home directory already exists.
    """
    destinations = [home / ".agents" / "skills"]
    for executable, directory in (("claude", ".claude"), ("codex", ".codex")):
        if shutil.which(executable) or (home / directory).is_dir():
            destinations.append(home / directory / "skills")
    return destinations


async def _install_skills(destinations: Sequence[Path]) -> list[dict[str, Any]]:
    """Download the skills archive and install every skill into each destination.

    Returns one step per destination. A destination is written skill by
    skill through a staging directory and an atomic rename, so a failure
    or interrupt leaves each skill either at its previous version or at the
    new one, never half-copied, and the other destinations are unaffected.
    """
    try:
        archive = await _fetch_skills_archive()
        extracted = _extract_skills(archive)
    except CLIError as error:
        return [
            _step("skills", str(destination), "failed", error.message)
            for destination in destinations
        ]
    skills = extracted.files
    if not skills:
        return [
            _step(
                "skills",
                str(destination),
                "failed",
                "The skills archive contained no skills.",
            )
            for destination in destinations
        ]
    skipped_note = (
        f" Skipped oversized files: {', '.join(extracted.skipped)}."
        if extracted.skipped
        else ""
    )
    outcomes: list[dict[str, Any]] = []
    for destination in destinations:
        try:
            _write_skills(destination, skills)
        except OSError as error:
            outcomes.append(
                _step("skills", str(destination), "failed", f"{error}{skipped_note}")
            )
            continue
        outcomes.append(
            _step(
                "skills",
                str(destination),
                "done",
                f"{len(skills)} skills: {', '.join(sorted(skills))}{skipped_note}",
            )
        )
    return outcomes


def _write_skills(destination: Path, skills: dict[str, dict[str, bytes]]) -> None:
    """Replace each skill directory under ``destination`` atomically.

    Every skill is written in full to a staging directory next to its final
    location, then swapped in with a rename. The previous version is only
    removed once the new one is complete.
    """
    destination.mkdir(parents=True, exist_ok=True)
    for name, files in skills.items():
        target = destination / name
        staging = Path(
            tempfile.mkdtemp(prefix=f".{name}.", suffix=".staging", dir=destination)
        )
        try:
            for relative, content in files.items():
                path = staging / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            if not (staging / "SKILL.md").is_file():
                raise OSError(f"{name}: SKILL.md missing after extraction")
            if target.is_symlink() or target.is_file():
                target.unlink()
            elif target.is_dir():
                retired = Path(
                    tempfile.mkdtemp(prefix=f".{name}.", suffix=".old", dir=destination)
                )
                retired.rmdir()
                os.replace(target, retired)
                os.replace(staging, target)
                shutil.rmtree(retired, ignore_errors=True)
                continue
            os.replace(staging, target)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise


async def _fetch_skills_archive() -> bytes:
    """Download the skills repository tarball, refusing oversized bodies."""
    try:
        async with (
            httpx.AsyncClient(follow_redirects=True, timeout=60) as client,
            client.stream("GET", SKILLS_ARCHIVE_URL) as response,
        ):
            response.raise_for_status()
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > _MAX_ARCHIVE_BYTES:
                    raise CLIError(
                        "internal_error",
                        "The skills archive is unexpectedly large.",
                    )
                chunks.append(chunk)
    except httpx.HTTPError as error:
        raise CLIError(
            "network_error",
            f"Could not download the skills from {SKILLS_ARCHIVE_URL}: {error}",
            retryable=True,
        ) from error
    return b"".join(chunks)


def _extract_skills(archive: bytes) -> ExtractedSkills:
    """Read `skills/<name>/...` regular files out of the repository tarball.

    Members are read explicitly instead of extracted, so no archive path can
    escape the destination and no symlink or device entry is ever created.
    Oversized members are skipped and reported by name.
    """
    skills: dict[str, dict[str, bytes]] = {}
    skipped: list[str] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                parts = PurePosixPath(member.name).parts
                # <repo>-<ref>/skills/<name>/<file...>
                if len(parts) < 4 or parts[1] != "skills":
                    continue
                if any(part in {"", ".", ".."} for part in parts):
                    continue
                relative = str(Path(*parts[3:]))
                if member.size > _MAX_SKILL_FILE_BYTES:
                    skipped.append(f"{parts[2]}/{relative}")
                    continue
                name = parts[2]
                extracted = tar.extractfile(member)
                if extracted is None:
                    continue
                skills.setdefault(name, {})[relative] = extracted.read()
    except (tarfile.TarError, EOFError, OSError) as error:
        raise CLIError(
            "internal_error", f"The skills archive could not be read: {error}"
        ) from error
    return ExtractedSkills(
        files={name: files for name, files in skills.items() if "SKILL.md" in files},
        skipped=skipped,
    )


# ---------------------------------------------------------------------------
# MCP clients
# ---------------------------------------------------------------------------


class McpClient:
    """One coding agent that can be pointed at the Kitaru MCP server."""

    name: str

    async def register(self, command: str, args: tuple[str, ...]) -> dict[str, Any]:
        """Register the server and report the outcome as one step."""
        raise NotImplementedError


class ClaudeCodeClient(McpClient):
    """Claude Code, configured through its own `claude mcp` commands."""

    name = "Claude Code"

    def __init__(self, executable: str, scope: Literal["project", "user"]) -> None:
        """Remember the CLI path and the scope to register under."""
        self.executable = executable
        self.scope = scope

    async def register(self, command: str, args: tuple[str, ...]) -> dict[str, Any]:
        """Replace any existing `kitaru` entry and verify it is the one in use.

        `claude mcp get` is scope-agnostic and reports whichever entry wins.
        After adding ours, read it back: if the winning entry does not carry
        our command, an entry in another scope shadows it and the user has to
        remove that one.
        """
        existing = await _run_command(self.executable, "mcp", "get", MCP_SERVER_NAME)
        if existing.returncode == 0:
            await _run_command(
                self.executable, "mcp", "remove", "--scope", self.scope, MCP_SERVER_NAME
            )
        added = await _run_command(
            self.executable,
            "mcp",
            "add",
            "--scope",
            self.scope,
            MCP_SERVER_NAME,
            "--",
            command,
            *args,
        )
        if added.returncode != 0:
            return _step("mcp", self.name, "failed", _failure_detail(added))
        current = await _run_command(self.executable, "mcp", "get", MCP_SERVER_NAME)
        if current.returncode == 0 and command not in current.stdout:
            return _step(
                "mcp",
                self.name,
                "failed",
                f"registered in {self.scope} scope, but an entry named "
                f"'{MCP_SERVER_NAME}' in another scope still wins. Remove it "
                f"with `claude mcp remove {MCP_SERVER_NAME}` in that scope and "
                "run `kitaru setup` again.",
            )
        return _step(
            "mcp", self.name, "done", f"server '{MCP_SERVER_NAME}', {self.scope} scope"
        )


class CodexClient(McpClient):
    """Codex CLI, configured through `codex mcp add` (which overwrites)."""

    name = "Codex"

    def __init__(self, executable: str) -> None:
        """Remember the CLI path."""
        self.executable = executable

    async def register(self, command: str, args: tuple[str, ...]) -> dict[str, Any]:
        """Add or overwrite the `kitaru` entry."""
        added = await _run_command(
            self.executable, "mcp", "add", MCP_SERVER_NAME, "--", command, *args
        )
        if added.returncode != 0:
            return _step("mcp", self.name, "failed", _failure_detail(added))
        return _step("mcp", self.name, "done", f"server '{MCP_SERVER_NAME}'")


class JsonFileClient(McpClient):
    """A client configured by an `mcpServers` object in a JSON file."""

    def __init__(self, name: str, path: Path) -> None:
        """Remember the display name and the configuration file."""
        self.name = name
        self.path = path

    async def register(self, command: str, args: tuple[str, ...]) -> dict[str, Any]:
        """Merge the `kitaru` entry into the file, keeping other servers.

        The file is rewritten through a uniquely named temporary file in the
        same directory and swapped in with a rename, preserving the original
        file mode (these files can hold other servers' secrets).
        """
        try:
            document = _read_json_object(self.path)
            servers = document.get("mcpServers")
            if not isinstance(servers, dict):
                servers = {}
            servers[MCP_SERVER_NAME] = {"command": command, "args": list(args)}
            document["mcpServers"] = servers
            self.path.parent.mkdir(parents=True, exist_ok=True)
            mode = self.path.stat().st_mode if self.path.exists() else None
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    handle.write(json.dumps(document, indent=2) + "\n")
                if mode is not None:
                    os.chmod(temporary, mode)
                os.replace(temporary, self.path)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        except (OSError, ValueError) as error:
            return _step("mcp", self.name, "failed", f"{self.path}: {error}")
        return _step("mcp", self.name, "done", str(self.path))


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read a JSON object from a file, treating a missing file as empty."""
    if not path.exists():
        return {}
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("expected a JSON object at the top level")
    return document


def detect_mcp_clients(cwd: Path, home: Path, launch: McpLaunch) -> list[McpClient]:
    """Return every configurable MCP client found on this machine.

    Claude Code and Codex are detected by their CLIs. Cursor and Windsurf are
    detected by their home directories and configured through their JSON
    files; Cursor gets a project-level file for a project install.
    """
    clients: list[McpClient] = []
    claude = shutil.which("claude")
    if claude:
        clients.append(ClaudeCodeClient(claude, launch.scope))
    codex = shutil.which("codex")
    if codex:
        clients.append(CodexClient(codex))
    if (home / ".cursor").is_dir():
        root = launch.project_dir if launch.project_dir is not None else home
        clients.append(JsonFileClient("Cursor", root / ".cursor" / "mcp.json"))
    windsurf = home / ".codeium" / "windsurf"
    if windsurf.is_dir():
        clients.append(JsonFileClient("Windsurf", windsurf / "mcp_config.json"))
    return clients


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _run_command(executable: str, *arguments: str) -> ProcessResult:
    """Run one bounded external command with stdin closed."""
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            *arguments,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as error:
        return ProcessResult(returncode=127, stdout="", stderr=str(error))
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), _COMMAND_TIMEOUT)
    except TimeoutError:
        process.kill()
        await process.communicate()
        return ProcessResult(
            returncode=124, stdout="", stderr=f"{executable} did not finish in time."
        )
    return ProcessResult(
        returncode=process.returncode or 0,
        stdout=stdout.decode("utf-8", errors="replace"),
        stderr=stderr.decode("utf-8", errors="replace"),
    )


def _failure_detail(result: ProcessResult) -> str:
    """Summarize a failed command from its last output line."""
    output = (result.stderr or result.stdout).strip().splitlines()
    tail = output[-1] if output else "no output"
    return f"exit {result.returncode}: {tail}"


def _step(kind: str, target: str, status: str, detail: str) -> dict[str, Any]:
    """Build one fixed-shape setup step."""
    return {"kind": kind, "target": target, "status": status, "detail": detail}
