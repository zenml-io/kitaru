from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SMOKE_SCRIPT = REPO_ROOT / "scripts" / "smoke-test.sh"


def _run_smoke_script(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    explicit_env = env or {}
    merged_env.update(explicit_env)

    if "PATH" in explicit_env:
        return subprocess.run(
            ["bash", str(SMOKE_SCRIPT), *args],
            cwd=REPO_ROOT,
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    with tempfile.TemporaryDirectory(prefix="kitaru-smoke-test-bin-") as fake_bin_raw:
        fake_bin = Path(fake_bin_raw)
        _write_fake_uv(fake_bin / "uv", Path(os.devnull))
        merged_env["PATH"] = f"{fake_bin}{os.pathsep}{merged_env['PATH']}"
        return subprocess.run(
            ["bash", str(SMOKE_SCRIPT), *args],
            cwd=REPO_ROOT,
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=30,
        )


def test_gemini_model_smoke_allows_for_variable_thinking_latency() -> None:
    script = SMOKE_SCRIPT.read_text(encoding="utf-8")
    match = re.search(
        r'gemini_interactions_adapter\.py --mode model" \\\n\s+timed (\d+)',
        script,
    )

    assert match is not None
    assert int(match.group(1)) == 180


def test_smoke_covers_langfuse_uri_help_without_network_calls() -> None:
    script = SMOKE_SCRIPT.read_text(encoding="utf-8")

    assert '"Langfuse import help exposes trace URIs"' in script
    assert (
        "kitaru import langfuse --help | grep -q 'langfuse://trace/TRACE_ID'" in script
    )
    assert script.count("kitaru import langfuse --help") == 1
    assert '"Replay-fork import help exposes trace URIs"' in script
    assert "demo.py import-traces --help | grep -q 'langfuse://trace/<id>'" in script


def test_smoke_script_has_valid_bash_syntax() -> None:
    completed = subprocess.run(
        ["bash", "-n", str(SMOKE_SCRIPT)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


def test_fresh_local_agent_smoke_registers_before_current_success() -> None:
    script = SMOKE_SCRIPT.read_text(encoding="utf-8")

    uninitialized = script.index(
        'run_expected_failure "fresh local Agent is uninitialized"'
    )
    registration = script.index('run_test "Register local PydanticAI Agent"')
    current_success = script.index('run_test "kitaru agents current"')

    assert uninitialized < registration < current_success
    assert 'Agent(TestModel(), name="local-smoke-agent"' in script
    assert 'agent.register(entrypoint="local_smoke_agent:agent")' in script
    assert 'mktemp -d "$REPO_ROOT/.kitaru-local-agent-smoke.XXXXXX"' in script
    assert "${TMPDIR:-/tmp}/kitaru-local-agent-smoke" not in script
    assert 'rm -rf "$LOCAL_AGENT_SMOKE_ENV"' in script


def test_help_documents_remote_stack_smoke_contract() -> None:
    completed = _run_smoke_script("--help")

    assert completed.returncode == 0
    help_text = completed.stdout
    assert "--remote-stack-smoke" in help_text
    assert "--remote-server-url URL" in help_text
    assert "--remote-kubernetes-stack STACK" in help_text
    assert "--remote-local-remote-artifact-stack STACK" in help_text
    assert "--remote-flow-image IMAGE" in help_text
    assert "KITARU_REMOTE_SMOKE_SERVER_URL" in help_text
    assert "KITARU_REMOTE_SMOKE_KUBERNETES_STACK" in help_text
    assert "KITARU_REMOTE_SMOKE_LOCAL_REMOTE_ARTIFACT_STACK" in help_text
    assert "KITARU_REMOTE_SMOKE_FLOW_IMAGE" in help_text
    assert "just dev-image REPO=<operator-image-repo>" in help_text
    assert "--pro-project-smoke" in help_text
    assert "--pro-project-server-url URL" in help_text
    assert "--pro-project-login-timeout SECONDS" in help_text
    assert "--pro-project-run-prefix PREFIX" in help_text
    assert "KITARU_PRO_PROJECT_SMOKE" in help_text
    assert "KITARU_PRO_PROJECT_SMOKE_SERVER_URL" in help_text
    assert "KITARU_PRO_PROJECT_SMOKE_LOGIN_TIMEOUT" in help_text
    assert "KITARU_PRO_PROJECT_SMOKE_RUN_PREFIX" in help_text


def test_default_remote_smoke_is_opt_in_and_records_no_remote_checks(
    tmp_path: Path,
) -> None:
    json_out = tmp_path / "smoke-results.json"
    completed = _run_smoke_script(
        "--json-out",
        str(json_out),
        env={"KITARU_SMOKE_TEST_RUN_REMOTE_SECTION_ONLY": "1", "NO_COLOR": "1"},
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["counts"] == {
        "failed": 0,
        "passed": 0,
        "release_relevant_skipped": 0,
        "skipped": 1,
    }
    assert payload["checks"] == [
        {
            "duration_seconds": 0,
            "evidence": {},
            "label": "remote stack smoke",
            "provider_area": "none",
            "reason": (
                "not opted in; set KITARU_REMOTE_SMOKE=1 or pass "
                "--remote-stack-smoke for remote stack release evidence"
            ),
            "release_relevant": False,
            "required_env": [],
            "section": "Remote stack smoke",
            "status": "skipped",
        }
    ]


def _write_fake_uv(path: Path, command_log: Path) -> None:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
if [[ "${{1:-}}" == "--version" ]]; then
    printf 'uv 0.0.0-fake\\n'
    exit 0
fi
if [[ "${{1:-}}" != "run" ]]; then
    printf 'unexpected fake uv command: %s\\n' "$*" >&2
    exit 1
fi
shift
if [[ "${{1:-}}" == "--python" ]]; then
    shift 2
fi
if [[ "${{1:-}}" == "python" && "${{2:-}}" == "--version" ]]; then
    printf 'Python 3.12.0\\n'
    exit 0
fi
if [[ "${{1:-}}" == "kitaru" && "${{2:-}}" == "login" ]]; then
    printf 'remote login ok\\n'
    exit 0
fi
if [[ "${{1:-}}" == "kitaru" && "${{2:-}}" == "stack" && "${{3:-}}" == "show" ]]; then
    stack_name="${{4:-}}"
    printf 'stack show diagnostic on stderr\\n' >&2
    if [[ "$stack_name" == "k8s-stack" ]]; then
        cat <<'JSON'
{{"command":"stack.show","item":{{"stack_type":"kubernetes","components":[{{"role":"runner","backend":"kubernetes"}},{{"role":"storage","backend":"s3"}}]}}}}
JSON
    else
        cat <<'JSON'
{{"command":"stack.show","item":{{"stack_type":"local","components":[{{"role":"runner","backend":"local"}},{{"role":"storage","backend":"s3"}}]}}}}
JSON
    fi
    exit 0
fi
if [[ "${{1:-}}" == "python" && "${{2:-}}" == "scripts/remote_stack_smoke.py" ]]; then
    shift 2
    if [[ "${{1:-}}" == "validate-stack" ]]; then
        payload=$(cat)
        python - "$payload" "${{3:-unknown}}" <<'PY'
import json
import sys

json.loads(sys.argv[1])
print(json.dumps({{"evidence": {{"category": sys.argv[2]}}, "valid": True}}))
PY
        exit $?
    fi
    if [[ "${{1:-}}" == "run-flow" ]]; then
        printf '%s\\n' "$*" >> {command_log}
        printf 'run-flow diagnostic on stderr\\n' >&2
        printf '{{"evidence":{{"category":"remote-test","status":"completed"}}}}\\n'
        exit 0
    fi
fi
printf 'unexpected fake uv run command: %s\\n' "$*" >&2
exit 1
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def test_opted_in_remote_smoke_fails_clearly_when_required_config_missing(
    tmp_path: Path,
) -> None:
    json_out = tmp_path / "smoke-results.json"
    completed = _run_smoke_script(
        "--remote-stack-smoke",
        "--json-out",
        str(json_out),
        env={"KITARU_SMOKE_TEST_RUN_REMOTE_SECTION_ONLY": "1", "NO_COLOR": "1"},
    )

    assert completed.returncode == 1
    assert "remote smoke configuration" in completed.stdout
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["counts"]["failed"] == 1
    assert payload["checks"] == [
        {
            "duration_seconds": 0,
            "evidence": {},
            "label": "remote smoke configuration",
            "provider_area": "none",
            "reason": (
                "missing required remote smoke config: "
                "KITARU_REMOTE_SMOKE_SERVER_URL "
                "KITARU_REMOTE_SMOKE_KUBERNETES_STACK "
                "KITARU_REMOTE_SMOKE_LOCAL_REMOTE_ARTIFACT_STACK"
            ),
            "release_relevant": False,
            "required_env": [
                "KITARU_REMOTE_SMOKE_SERVER_URL",
                "KITARU_REMOTE_SMOKE_KUBERNETES_STACK",
                "KITARU_REMOTE_SMOKE_LOCAL_REMOTE_ARTIFACT_STACK",
            ],
            "section": "Remote stack smoke",
            "status": "failed",
        }
    ]


def test_remote_smoke_rejects_non_positive_timeout_before_login(
    tmp_path: Path,
) -> None:
    json_out = tmp_path / "smoke-results.json"
    completed = _run_smoke_script(
        "--remote-stack-smoke",
        "--json-out",
        str(json_out),
        env={
            "KITARU_REMOTE_SMOKE_SERVER_URL": "https://private.example.invalid",
            "KITARU_REMOTE_SMOKE_KUBERNETES_STACK": "k8s-stack",
            "KITARU_REMOTE_SMOKE_LOCAL_REMOTE_ARTIFACT_STACK": "local-remote-stack",
            "KITARU_REMOTE_SMOKE_LOGIN_TIMEOUT": "abc",
            "KITARU_REMOTE_SMOKE_EXECUTION_TIMEOUT": "0",
            "KITARU_SMOKE_TEST_RUN_REMOTE_SECTION_ONLY": "1",
            "NO_COLOR": "1",
        },
    )

    assert completed.returncode == 1
    assert "remote smoke timeout configuration" in completed.stdout
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["counts"]["failed"] == 1
    check = payload["checks"][0]
    assert check["label"] == "remote smoke timeout configuration"
    assert check["reason"] == (
        "remote smoke timeouts must be positive integers: "
        "KITARU_REMOTE_SMOKE_LOGIN_TIMEOUT "
        "KITARU_REMOTE_SMOKE_EXECUTION_TIMEOUT"
    )
    assert check["required_env"] == [
        "KITARU_REMOTE_SMOKE_LOGIN_TIMEOUT",
        "KITARU_REMOTE_SMOKE_EXECUTION_TIMEOUT",
        "KITARU_REMOTE_SMOKE_LOG_TIMEOUT",
    ]


def test_remote_flow_image_is_only_passed_to_kubernetes_lane(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    command_log = tmp_path / "remote-flow-commands.txt"
    _write_fake_uv(fake_bin / "uv", command_log)
    json_out = tmp_path / "smoke-results.json"

    completed = _run_smoke_script(
        "--remote-stack-smoke",
        "--json-out",
        str(json_out),
        env={
            "KITARU_REMOTE_SMOKE_SERVER_URL": "https://private.example.invalid",
            "KITARU_REMOTE_SMOKE_KUBERNETES_STACK": "k8s-stack",
            "KITARU_REMOTE_SMOKE_LOCAL_REMOTE_ARTIFACT_STACK": "local-remote-stack",
            "KITARU_REMOTE_SMOKE_FLOW_IMAGE": (
                "private-registry.example/team/image:latest"
            ),
            "KITARU_SMOKE_TEST_RUN_REMOTE_SECTION_ONLY": "1",
            "NO_COLOR": "1",
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    commands = command_log.read_text(encoding="utf-8").splitlines()
    assert len(commands) == 2
    kubernetes_command, local_remote_artifact_command = commands
    assert "--category kubernetes" in kubernetes_command
    assert "--image private-registry.example/team/image:latest" in kubernetes_command
    assert "--category local-remote-artifact" in local_remote_artifact_command
    assert "--image" not in local_remote_artifact_command

    payload = json.loads(json_out.read_text(encoding="utf-8"))
    flow_checks = [
        check
        for check in payload["checks"]
        if "flow execution/readback" in check["label"]
    ]
    assert len(flow_checks) == 2
    assert all(
        check["evidence"] == {"category": "remote-test", "status": "completed"}
        for check in flow_checks
    )
