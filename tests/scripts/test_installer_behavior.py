"""Run installer control-flow regressions without touching user configuration."""

import os
import subprocess
from pathlib import Path

import pytest

INSTALLER = Path(__file__).resolve().parents[2] / "install.sh"


@pytest.mark.parametrize("flag", ["--version", "--with", "--server", "--python"])
@pytest.mark.parametrize("suffix", [[], ["--global"], [""]])
def test_missing_option_value_is_reported(flag, suffix):
    result = subprocess.run(
        ["bash", str(INSTALLER), flag, *suffix], capture_output=True, text=True
    )
    assert result.returncode != 0
    assert f"{flag} needs a value" in result.stderr


@pytest.fixture
def installer(tmp_path):
    # Substitute the source's HOME references, leaving the host environment alone.
    script = tmp_path / "install.sh"
    script.write_text(INSTALLER.read_text().replace("$HOME", "$TEST_HOME"))
    stub_dir = tmp_path / "stubs"
    stub_dir.mkdir()
    tool_dir = tmp_path / "tools"
    (tool_dir / "kitaru").mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    binary = bin_dir / "kitaru"
    binary.write_text(
        '#!/bin/bash\nprintf "%s\\n" "$*" >> "$BINARY_CALLS"\n'
        '[ "${BROKEN_CLI:-0}" = 0 ] || { echo "fixture startup failed" >&2; exit 7; }\n'
        'if [ "$*" = --help ] && [ "${HAS_OUTPUT:-0}" = 1 ]; then echo --output; fi\n'
        "echo fixture\n"
    )
    binary.chmod(0o755)
    uv = stub_dir / "uv"
    uv.write_text(
        '#!/bin/bash\nprintf "%s\\n" "$*" >> "$CALLS"\n'
        'if [ -n "${UV_FAIL_COMMAND:-}" ] && [[ "$*" = "$UV_FAIL_COMMAND"* ]]; '
        "then exit 8; fi\n"
        'case "$*" in\n'
        '  --version) echo "uv 0.13.4" ;;\n'
        '  "tool dir --bin") echo "$BIN_DIR" ;;\n'
        '  "tool dir") echo "$TOOL_DIR" ;;\n'
        '  run*) echo "${BIN_DIR%/bin}" ;;\n'
        "  *) exit 0 ;;\nesac\n"
    )
    uv.chmod(0o755)
    env = dict(os.environ)
    for key in ("KITARU_PYTHON", "KITARU_VERSION", "KITARU_PRE", "KITARU_SCOPE"):
        env.pop(key, None)
    env.update(
        PATH=f"{stub_dir}:/usr/bin:/bin",
        TEST_HOME=str(tmp_path / "home"),
        UV_INSTALL_DIR=str(tmp_path / "uv-bin"),
        XDG_BIN_HOME=str(tmp_path / "xdg-bin"),
        XDG_DATA_HOME=str(tmp_path / "xdg-data"),
        CALLS=str(tmp_path / "calls"),
        BINARY_CALLS=str(tmp_path / "binary-calls"),
        BIN_DIR=str(bin_dir),
        TOOL_DIR=str(tool_dir),
    )
    return script, env


def run_installer(installer, tmp_path, *args):
    script, env = installer
    return subprocess.run(
        ["bash", str(script), "--no-skills", "--no-mcp", "--no-modify-path", *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("scope", ["--global", "--project"])
def test_broken_cli_cannot_report_success(installer, tmp_path, scope):
    installer[1]["BROKEN_CLI"] = "1"
    (tmp_path / "pyproject.toml").write_text('[project]\nname="fixture"\n')
    result = run_installer(installer, tmp_path, scope)
    assert result.returncode != 0
    assert "fixture startup failed" in result.stderr
    assert "could not start" in result.stderr
    assert "Kitaru is installed" not in result.stdout


@pytest.mark.parametrize(
    "metadata",
    ["[tool.poetry]\n", "[tool.pdm]\n", " [ tool.poetry ]\n", "[tool.ruff]\n", ""],
)
def test_ambiguous_project_is_not_modified(installer, tmp_path, metadata):
    if metadata:
        (tmp_path / "pyproject.toml").write_text(metadata)
    else:
        (tmp_path / "uv.lock").touch()
    result = run_installer(installer, tmp_path)
    assert result.returncode != 0
    assert "--global" in result.stderr
    assert "add " not in Path(installer[1]["CALLS"]).read_text()


@pytest.mark.parametrize("lock", ["poetry.lock", "pdm.lock", "Pipfile", "Pipfile.lock"])
def test_foreign_lock_prevents_project_mutation(installer, tmp_path, lock):
    (tmp_path / "pyproject.toml").write_text('[project]\nname="fixture"\n')
    (tmp_path / lock).touch()
    result = run_installer(installer, tmp_path)
    assert result.returncode != 0
    assert "--global" in result.stderr


def test_ordinary_global_rerun_uses_upgrade(installer, tmp_path):
    receipt = Path(installer[1]["TOOL_DIR"]) / "kitaru" / "uv-receipt.toml"
    receipt.touch()
    result = run_installer(installer, tmp_path, "--global")
    assert result.returncode == 0, result.stderr
    calls = Path(installer[1]["CALLS"]).read_text()
    assert "tool upgrade --quiet kitaru" in calls
    assert "tool install" not in calls


@pytest.mark.parametrize(
    "override", [["--version=1.2.3"], ["--pre"], ["--with=addon"], ["--python=3.11"]]
)
def test_explicit_global_configuration_uses_install(installer, tmp_path, override):
    (Path(installer[1]["TOOL_DIR"]) / "kitaru" / "uv-receipt.toml").touch()
    result = run_installer(installer, tmp_path, "--global", *override)
    assert result.returncode == 0, result.stderr
    assert "tool install" in Path(installer[1]["CALLS"]).read_text()


@pytest.mark.parametrize(
    "variable,value",
    [("KITARU_VERSION", "1.2.3"), ("KITARU_PRE", "1"), ("KITARU_PYTHON", "3.11")],
)
def test_environment_configuration_is_explicit(installer, tmp_path, variable, value):
    (Path(installer[1]["TOOL_DIR"]) / "kitaru" / "uv-receipt.toml").touch()
    installer[1][variable] = value
    result = run_installer(installer, tmp_path, "--global")
    assert result.returncode == 0, result.stderr
    assert "tool install" in Path(installer[1]["CALLS"]).read_text()


@pytest.mark.parametrize(
    "variable,cargo_layout",
    [
        ("UV_INSTALL_DIR", False),
        ("XDG_BIN_HOME", False),
        ("XDG_DATA_HOME", False),
        ("CARGO_DIST_FORCE_INSTALL_DIR", False),
        ("UV_UNMANAGED_INSTALL", False),
        ("UV_INSTALL_DIR", True),
    ],
)
def test_broken_uv_bootstrap_respects_destination_and_original_path(
    installer, tmp_path, variable, cargo_layout
):
    _script, env = installer
    stub = Path(env["PATH"].split(":")[0]) / "uv"
    working = tmp_path / "working-uv"
    working.write_bytes(stub.read_bytes())
    working.chmod(0o755)
    stub.write_text("#!/bin/bash\nexit 9\n")
    curl = stub.with_name("curl")
    curl.write_text('#!/bin/bash\ncat "$PAYLOAD"\n')
    curl.chmod(0o755)
    payload = tmp_path / "payload"
    payload.write_text(
        'printf "%s\\n%s\\n" "$PATH" "${UV_NO_MODIFY_PATH:-unset}" > "$OBSERVED"\n'
        'install_dir="$UV_INSTALL_DIR"\n'
        'if [ "$install_dir" = "${CARGO_HOME:-$HOME/.cargo}" ]; then\n'
        '  install_dir="$install_dir/bin"\nfi\n'
        'mkdir -p "$install_dir"\ncp "$WORKING_UV" "$install_dir/uv"\n'
    )
    for key in (
        "UV_INSTALL_DIR",
        "XDG_BIN_HOME",
        "XDG_DATA_HOME",
        "CARGO_DIST_FORCE_INSTALL_DIR",
        "UV_UNMANAGED_INSTALL",
        "CARGO_HOME",
    ):
        env.pop(key, None)
    destination = tmp_path / "custom"
    destination.mkdir()
    if cargo_layout:
        env["CARGO_HOME"] = str(destination)
    env.update(
        {
            variable: str(destination),
            "PAYLOAD": str(payload),
            "WORKING_UV": str(working),
            "OBSERVED": str(tmp_path / "observed"),
        }
    )
    result = run_installer(installer, tmp_path, "--global")
    assert result.returncode == 0, result.stderr
    expected = (
        destination if variable != "XDG_DATA_HOME" else destination.parent / "bin"
    )
    if cargo_layout:
        expected = destination / "bin"
    assert (expected / "uv").is_file()
    assert Path(env["OBSERVED"]).read_text().splitlines() == [env["PATH"], "1"]


def extract_function(name):
    return (
        name
        + "() {"
        + INSTALLER.read_text().split(name + "() {", 1)[1].split("\n}", 1)[0]
        + "\n}\n"
    )


def test_profile_failure_is_best_effort(tmp_path):
    (tmp_path / ".profile").mkdir()
    function = extract_function("persist_path").replace("$HOME", "$TEST_HOME")
    result = subprocess.run(
        [
            "bash",
            "-eu",
            "-c",
            function
            + '\nwarn() { echo "$*" >&2; }; persist_path /fixture/bin; echo continued',
        ],
        env={**os.environ, "TEST_HOME": str(tmp_path), "SHELL": "/bin/bash"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "continued" in result.stdout
    assert 'export PATH="/fixture/bin:$PATH"' in result.stderr


@pytest.mark.parametrize("phase", ["copy", "publish"])
@pytest.mark.parametrize("signal", ["INT", "TERM"])
def test_interrupted_legacy_skill_replacement_restores_and_cleans(
    tmp_path, phase, signal
):
    import tarfile

    skill = tmp_path / "repo" / "skills" / "kitaru-fixture"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("new")
    archive = tmp_path / "skills.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(skill.parents[1], arcname="repo")
    dest = tmp_path / "dest"
    old = dest / "kitaru-fixture"
    old.mkdir(parents=True)
    (old / "SKILL.md").write_text("old")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    command = "cp" if phase == "copy" else "mv"
    wrapper = stubs / command
    wrapper.write_text(
        "#!/bin/bash\n"
        + (
            '/bin/cp "$@"\nkill -"$INTERRUPT_SIGNAL" "$PPID"\nexit 1\n'
            if phase == "copy"
            else 'case "$1" in */.kitaru-skills.*/kitaru-fixture) '
            'kill -"$INTERRUPT_SIGNAL" "$PPID"; exit 1 ;; esac\n/bin/mv "$@"\n'
        )
    )
    wrapper.chmod(0o755)
    script = (
        extract_function("install_skills") + '\nfetch_to_stdout() { cat "$ARCHIVE"; }; '
        'skill_destinations() { echo "$DEST"; }; '
        'warn() { echo "$*" >&2; }; install_skills'
    )
    result = subprocess.run(
        ["bash", "-eu", "-c", script],
        env={
            **os.environ,
            "PATH": f"{stubs}:/usr/bin:/bin",
            "TMPDIR": str(scratch),
            "ARCHIVE": str(archive),
            "DEST": str(dest),
            "KITARU_SKILLS_REPO": "fixture",
            "INTERRUPT_SIGNAL": signal,
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert (old / "SKILL.md").read_text() == "old"
    assert sorted(p.name for p in dest.iterdir()) == ["kitaru-fixture"], result.stderr
    assert list(scratch.iterdir()) == []


@pytest.mark.parametrize("command", ["tool install", "tool upgrade"])
def test_failed_global_install_or_upgrade_cannot_report_success(
    installer, tmp_path, command
):
    installer[1]["UV_FAIL_COMMAND"] = command
    if command == "tool upgrade":
        (Path(installer[1]["TOOL_DIR"]) / "kitaru" / "uv-receipt.toml").touch()
    result = run_installer(installer, tmp_path, "--global")
    assert result.returncode != 0
    assert "failed" in result.stderr
    assert "Kitaru is installed" not in result.stdout


@pytest.mark.parametrize("supports_output", ["0", "1"])
def test_setup_output_option_is_compatible_with_older_releases(
    installer, tmp_path, supports_output
):
    script, env = installer
    env["HAS_OUTPUT"] = supports_output
    result = subprocess.run(
        ["bash", str(script), "--global", "--no-mcp", "--no-modify-path"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    calls = Path(env["BINARY_CALLS"]).read_text().splitlines()
    assert any("setup --mode standard --no-mcp" in call for call in calls)
    assert any("--output text setup" in call for call in calls) == (
        supports_output == "1"
    )


def test_legacy_skill_destinations_avoid_codex_copy(tmp_path):
    (tmp_path / ".codex").mkdir()
    function = extract_function("skill_destinations").replace("$HOME", "$TEST_HOME")
    result = subprocess.run(
        [
            "bash",
            "-eu",
            "-c",
            function + '\nhave() { [ "$1" = codex ]; }; skill_destinations',
        ],
        env={**os.environ, "TEST_HOME": str(tmp_path)},
        capture_output=True,
        text=True,
    )
    assert result.stdout.splitlines() == [str(tmp_path / ".agents" / "skills")]
    assert not (tmp_path / ".codex" / "skills").exists()
