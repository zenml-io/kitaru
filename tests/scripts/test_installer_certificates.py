"""Exercise the macOS installer workflow's inline trust repair and probes."""

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/installer.yml"
REPAIR = "Repair macOS certificate bundles"
PREFLIGHT = "Verify macOS curl and Git certificate rejection"
UV_PROBE = "Verify macOS uv certificate rejection"

# AAA Certificate Services, from the incident root inventory:
# https://emudhra.we-have-a-problem.com/roots.json
BLOCKED_ROOT = """-----BEGIN CERTIFICATE-----
MIIEMjCCAxqgAwIBAgIBATANBgkqhkiG9w0BAQUFADB7MQswCQYDVQQGEwJHQjEb
MBkGA1UECAwSR3JlYXRlciBNYW5jaGVzdGVyMRAwDgYDVQQHDAdTYWxmb3JkMRow
GAYDVQQKDBFDb21vZG8gQ0EgTGltaXRlZDEhMB8GA1UEAwwYQUFBIENlcnRpZmlj
YXRlIFNlcnZpY2VzMB4XDTA0MDEwMTAwMDAwMFoXDTI4MTIzMTIzNTk1OVowezEL
MAkGA1UEBhMCR0IxGzAZBgNVBAgMEkdyZWF0ZXIgTWFuY2hlc3RlcjEQMA4GA1UE
BwwHU2FsZm9yZDEaMBgGA1UECgwRQ29tb2RvIENBIExpbWl0ZWQxITAfBgNVBAMM
GEFBQSBDZXJ0aWZpY2F0ZSBTZXJ2aWNlczCCASIwDQYJKoZIhvcNAQEBBQADggEP
ADCCAQoCggEBAL5AnfRu4ep2hxxNRUSOvkbIgwadwSr+GB+O5AL686tdUIoWMQua
BtDFcCLNSS1UY8y2bmhGC1Pqy0wkwLxyTurxFa70VJoSCsN6sjNg4tqJVfMiWPPe
3M/vg4aijJRPn2jymJBGhCfHdr/jzDUsi14HZGWCwEiwqJH5YZ92IFCokcdmtet4
YgNW8IoaE+oxox6gmf049vYnMlhvB/VruPsUK6+3qszWY19zjNoFmag4qMsXeDZR
rOme9Hg6jc8P2ULimAyrL58OAd7vn5lJ8S3frHRNG5i1R8XlKdH5kBjHYpy+g8cm
ez6KJcfA3Z3mNWgQIJ2P2N7Sw4ScDV7oL8kCAwEAAaOBwDCBvTAdBgNVHQ4EFgQU
oBEKIz6W8Qfs4q8p74Klf9AwpLQwDgYDVR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQF
MAMBAf8wewYDVR0fBHQwcjA4oDagNIYyaHR0cDovL2NybC5jb21vZG9jYS5jb20v
QUFBQ2VydGlmaWNhdGVTZXJ2aWNlcy5jcmwwNqA0oDKGMGh0dHA6Ly9jcmwuY29t
b2RvLm5ldC9BQUFDZXJ0aWZpY2F0ZVNlcnZpY2VzLmNybDANBgkqhkiG9w0BAQUF
AAOCAQEACFb8AvCb6P+k+tZ7xkSAzk/ExfYAWMymtrwUSWgEdujm7l3sAg9g1o1Q
GE8mTgHj5rCl7r+8dFRBv/38ErjHT1r0iWAFf2C3BUrz9vHCv8S5dIa2LX1rzNLz
Rt0vxuBqw8M0Ayx9lt1awg6nCpnBBYurDC/zXDrPbDdVCYfeU0BsWO/8tqtlbgT2
G9w84FoVxp7Z8VlIMCFlA2zs6SFz7JsDoeA3raAVGI/6ugLOpyypEBMs1OUIJqsi
l2D4kF501KKaU73yqWjgom7C12yxow+ev+to51byrvLjKzg6CYG1a4XXvi3tPxq3
smPi9WIsgtRqAEFQ8TmDn5XpNpaYbg==
-----END CERTIFICATE-----
"""

# Self-signed unrelated EC root. No private key or runtime network is needed.
UNRELATED_ROOT = """-----BEGIN CERTIFICATE-----
MIIBcDCCARagAwIBAgIUOGgclGq7NlpDRPjywokv9VLNJmQwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTVW5yZWxhdGVkIHRlc3Qgcm9vdDAeFw0yNjExMDQwODU0MzJa
Fw0zNjExMDEwODU0MzJaMB4xHDAaBgNVBAMME1VucmVsYXRlZCB0ZXN0IHJvb3Qw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARGvbpTghQQ+uo+YovcDLnIrzBt8sKn
VLrr96YRrQsHxWaNuPGXoTXejh+dpz4LCB0oHZPKzC8YL534On/rj8bWozIwMDAd
BgNVHQ4EFgQUt+dlSw46fzPR6/X0eDalRMRKZsMwDwYDVR0TAQH/BAUwAwEB/zAK
BggqhkjOPQQDAgNIADBFAiEA2n1hJvMXjHQ1F/xRsCeumFopsfpvQxoeieh72PFx
9ugCIAqefknJhJ1qdhGCvJgTYbOwGGogKgPsuRd/Irb3aynW
-----END CERTIFICATE-----
"""


def _get_steps() -> list[dict[str, Any]]:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["install"]["steps"]


def _get_step(name: str) -> dict[str, Any]:
    return next(step for step in _get_steps() if step.get("name") == name)


def _get_python(name: str) -> str:
    match = re.search(r"<<'PY'\n(.*?)\nPY(?:\n|$)", _get_step(name)["run"], re.DOTALL)
    assert match is not None, f"No Python heredoc in {name}"
    return match[1]


def _execute_python(name: str) -> None:
    exec(compile(_get_python(name), str(WORKFLOW), "exec"), {"__name__": "__main__"})


def test_mac_certificates_are_repaired_and_verified_before_checkout() -> None:
    steps = _get_steps()
    names = [step.get("name") for step in steps]
    checkout = next(
        i
        for i, step in enumerate(steps)
        if step.get("uses", "").startswith("actions/checkout@")
    )
    assert names.index(REPAIR) < names.index(PREFLIGHT) < checkout
    for name in (REPAIR, PREFLIGHT, UV_PROBE):
        assert _get_step(name)["if"] == "runner.os == 'macOS'"
        assert _get_step(name)["shell"] == "bash"


def test_filter_removes_real_blocked_root_preserving_symlink_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "bundle.pem"
    target.write_text(BLOCKED_ROOT + UNRELATED_ROOT)
    link = tmp_path / "system.pem"
    link.symlink_to(target)
    absent = tmp_path / "missing-homebrew.pem"
    monkeypatch.setattr(sys, "argv", ["-", str(link), str(absent)])
    real_run = subprocess.run
    installs = []

    def run(command: list[str], **kwargs):
        if command[0] == "sudo":
            installs.append(command)
            shutil.copyfile(command[-2], command[-1])
            return subprocess.CompletedProcess(command, 0)
        return real_run(command, **kwargs)

    with patch("subprocess.run", side_effect=run):
        _execute_python(REPAIR)
        _execute_python(REPAIR)
    assert target.read_text() == UNRELATED_ROOT
    assert link.is_symlink()
    assert link.resolve() == target
    assert not absent.exists()
    assert len(installs) == 1
    assert installs[0][-1] == str(target)


@pytest.mark.parametrize(
    "bundle",
    [
        "",
        BLOCKED_ROOT,
        "not a certificate",
        "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n",
        UNRELATED_ROOT
        + "-----BEGIN CERTIFICATE-----\n%%%%\n-----END CERTIFICATE-----\n",
    ],
    ids=["empty", "only-blocked", "no-pem", "invalid-der", "malformed-base64"],
)
def test_bad_bundle_fails_before_exporting_trust_environment(
    tmp_path: Path, bundle: str
) -> None:
    _run_repair_shell(tmp_path, bundle)


def test_failed_privileged_install_fails_before_exporting_trust_environment(
    tmp_path: Path,
) -> None:
    _run_repair_shell(tmp_path, BLOCKED_ROOT + UNRELATED_ROOT)


def _run_repair_shell(tmp_path: Path, bundle: str) -> None:
    source = tmp_path / "bundle.pem"
    source.write_text(bundle)
    env_file = tmp_path / "github-env"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    sudo = bin_dir / "sudo"
    sudo.write_text("#!/bin/sh\nexit 1\n")
    sudo.chmod(0o755)
    script = re.sub(
        r"/usr/bin/python3.*?<<'PY'",
        f"{shlex.quote(sys.executable)} - {shlex.quote(str(source))} <<'PY'",
        _get_step(REPAIR)["run"],
        count=1,
        flags=re.DOTALL,
    )
    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
        env={
            **os.environ,
            "GITHUB_ENV": str(env_file),
            "RUNNER_TEMP": str(tmp_path),
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
        },
        check=False,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert not env_file.exists() or env_file.read_text() == ""
    assert source.read_text() == bundle


def _run_probe(
    name: str,
    *,
    failure_tool: str | None = None,
    returncode: int = 0,
    stderr: str = "",
    healthy_failure: bool = False,
    timeout: bool = False,
) -> list[list[str]]:
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs):
        calls.append(command)
        tool = Path(command[0]).name
        attack = "we-have-a-problem.com" in command[-1]
        if tool == failure_tool and (not attack if healthy_failure else attack):
            if timeout:
                raise subprocess.TimeoutExpired(command, 45)
            result = subprocess.CompletedProcess(command, returncode, "", stderr)
        elif attack:
            errors = {
                "curl": (60, "SSL certificate problem"),
                "git": (
                    128,
                    "SSL certificate problem: unable to get local issuer certificate",
                ),
                "uv": (2, "invalid peer certificate: UnknownIssuer"),
            }
            code, message = errors[tool]
            result = subprocess.CompletedProcess(command, code, "", message)
        else:
            result = subprocess.CompletedProcess(command, 0, "", "")
        if kwargs.get("check"):
            result.check_returncode()
        return result

    with (
        patch("subprocess.run", side_effect=run),
        patch("subprocess.check_output", return_value="uv 0.13.5\n"),
        patch("shutil.which", return_value="/opt/homebrew/bin/curl"),
    ):
        _execute_python(name)
    return calls


def test_preflight_checks_system_and_path_curl_and_git() -> None:
    calls = _run_probe(PREFLIGHT)
    for executable in ("/usr/bin/curl", "/opt/homebrew/bin/curl"):
        tool_calls = [call for call in calls if call[0] == executable]
        assert (
            len([call for call in tool_calls if "we-have-a-problem.com" in call[-1]])
            == 4
        )
        for host in ("github.com", "kitaru.ai", "astral.sh", "pypi.org"):
            assert any(host in call[-1] for call in tool_calls)
    git_calls = [call for call in calls if call[0] == "git"]
    assert any("github.com" in " ".join(call) for call in git_calls)
    assert any(
        "git-emudhra.we-have-a-problem.com" in " ".join(call) for call in git_calls
    )


@pytest.mark.parametrize("tool", ["curl", "git", "uv"])
@pytest.mark.parametrize(
    ("returncode", "stderr"),
    [
        (0, ""),
        (6, "Could not resolve host"),
        (22, "HTTP 500"),
        (28, "Connection timed out"),
    ],
    ids=["successful-attack", "dns", "http", "timeout-status"],
)
def test_probes_reject_failures_unrelated_to_certificates(
    tool: str, returncode: int, stderr: str
) -> None:
    # Git and uv report many unrelated network failures with the same status
    # as certificate failures, so their diagnostics must distinguish the cause.
    if returncode:
        returncode = {"curl": returncode, "git": 128, "uv": 2}[tool]
    with pytest.raises(SystemExit):
        _run_probe(
            UV_PROBE if tool == "uv" else PREFLIGHT,
            failure_tool=tool,
            returncode=returncode,
            stderr=stderr,
        )


@pytest.mark.parametrize("tool", ["curl", "git", "uv"])
def test_probes_fail_when_healthy_control_fails(tool: str) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        _run_probe(
            UV_PROBE if tool == "uv" else PREFLIGHT,
            failure_tool=tool,
            returncode=60,
            stderr="certificate verify failed",
            healthy_failure=True,
        )


@pytest.mark.parametrize("tool", ["curl", "git", "uv"])
def test_probes_fail_on_subprocess_timeout(tool: str) -> None:
    with pytest.raises(subprocess.TimeoutExpired):
        _run_probe(
            UV_PROBE if tool == "uv" else PREFLIGHT, failure_tool=tool, timeout=True
        )


@pytest.mark.parametrize(
    "message",
    [
        "SSL certificate problem",
        "SSL certificate OpenSSL verify result: "
        "unable to get local issuer certificate (20)",
        "certificate verify failed",
        "server certificate verification failed",
        "certificate has been revoked",
    ],
)
def test_git_accepts_certificate_specific_rejection(message: str) -> None:
    _run_probe(PREFLIGHT, failure_tool="git", returncode=128, stderr=message)


@pytest.mark.parametrize(
    "message",
    [
        "invalid peer certificate",
        "certificate verify failed",
        "certificate verification failed",
    ],
)
def test_uv_accepts_certificate_specific_rejection(message: str) -> None:
    calls = _run_probe(UV_PROBE, failure_tool="uv", returncode=2, stderr=message)
    assert any("pypi.org" in call[-1] for call in calls)
    assert len([call for call in calls if "we-have-a-problem.com" in call[-1]]) == 2
