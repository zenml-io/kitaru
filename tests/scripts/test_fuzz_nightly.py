"""Exercise nightly fuzz selection and failure reporting without GitHub writes."""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).parents[2]


def _get_workflow_step(job: str, name: str) -> dict[str, Any]:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/fuzz-nightly.yml").read_text()
    )
    return next(
        step for step in workflow["jobs"][job]["steps"] if step.get("name") == name
    )


@pytest.mark.parametrize(
    ("behavior", "expected_status", "expected_outcome"),
    [
        ("pass", 0, "passed"),
        ("fail", 7, "failed"),
        ("timeout", 124, "timed_out"),
        ("kill", 137, "timed_out"),
        ("capturefail", 23, "failed"),
        ("both", 7, "failed"),
    ],
)
def test_fuzz_process_reports_outcome_and_preserves_logs(
    tmp_path: Path, behavior: str, expected_status: int, expected_outcome: str
) -> None:
    timeout = shutil.which("timeout") or shutil.which("gtimeout")
    if timeout is None:
        pytest.skip("GNU timeout is required to exercise the Ubuntu runner command")
    step = _get_workflow_step("fuzz", "Run fuzz recipe")
    limit = "1s" if behavior in {"timeout", "kill"} else "5s"
    script, replacements = re.subn(
        r"--kill-after=\S+ \S+ uvx", f"--kill-after=0.2s {limit} uvx", step["run"]
    )
    assert replacements == 1
    binaries = tmp_path / "bin"
    binaries.mkdir()
    (binaries / "timeout").symlink_to(timeout)
    fake_uvx = binaries / "uvx"
    fake_uvx.write_text(
        "#!/bin/bash\n"
        "printf 'fuzz output\\n'\n"
        'case "$FUZZ_BEHAVIOR" in\n'
        "  pass) exit 0 ;;\n"
        "  fail|both) exit 7 ;;\n"
        "  timeout) exec sleep 5 ;;\n"
        "  kill) trap '' TERM; sleep 5 & wait ;;\n"
        "esac\n"
    )
    fake_uvx.chmod(0o755)
    if behavior in {"capturefail", "both"}:
        fake_tee = binaries / "tee"
        fake_tee.write_text('#!/bin/bash\ncat > "$1"\nexit 23\n')
        fake_tee.chmod(0o755)
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{binaries}{os.pathsep}{os.environ['PATH']}",
            "FUZZ_BEHAVIOR": behavior,
            "RECIPE": "fuzz-mcp-boundary",
            "GITHUB_OUTPUT": str(tmp_path / "output"),
            "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == expected_status, result.stdout + result.stderr
    assert (tmp_path / "output").read_text().strip() == f"outcome={expected_outcome}"
    assert "fuzz output" in (tmp_path / "fuzz.log").read_text()
    assert expected_outcome in (tmp_path / "summary").read_text()


@pytest.mark.parametrize("existing_issue", [False, True])
def test_reporter_opens_issues_for_failures_timeouts_and_cancellations(
    tmp_path: Path, existing_issue: bool
) -> None:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    fake_gh = binaries / "gh"
    fake_gh.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "with Path('commands.jsonl').open('a') as output:\n"
        "    output.write(json.dumps(args) + '\\n')\n"
        "if args[0] == 'api':\n"
        "    print('fuzz (mcp-boundary)\\tfailure')\n"
        "    print('fuzz (mcp-redaction)\\ttimed_out')\n"
        "    print('fuzz (importers)\\tcancelled')\n"
        "elif args[:2] == ['issue', 'list']:\n"
        "    print('Nightly fuzz run failed (mcp-boundary-other)')\n"
        "    if os.environ['EXISTING_ISSUE'] == '1':\n"
        "        print('Nightly fuzz run failed (mcp-boundary)')\n"
        "elif args[:2] == ['issue', 'create']:\n"
        "    title = args[args.index('--title') + 1]\n"
        "    body = Path(args[args.index('--body-file') + 1]).read_text()\n"
        "    with Path('created.jsonl').open('a') as output:\n"
        "        output.write(json.dumps({'title': title, 'body': body}) + '\\n')\n"
        "else:\n"
        "    raise SystemExit(f'Unexpected gh command: {args}')\n"
    )
    fake_gh.chmod(0o755)
    step = _get_workflow_step("report", "Open issues for failed or incomplete jobs")
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{binaries}{os.pathsep}{os.environ['PATH']}",
            "EXISTING_ISSUE": "1" if existing_issue else "0",
            "GH_REPO": "zenml-io/kitaru",
            "REPO_URL": "https://github.com/zenml-io/kitaru",
            "RUN_ID": "123",
            "RUN_ATTEMPT": "1",
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    issues = [
        json.loads(line)
        for line in (tmp_path / "created.jsonl").read_text().splitlines()
    ]
    assert {issue["title"] for issue in issues} == {
        "Nightly fuzz run failed (mcp-redaction)",
        "Nightly fuzz run failed (importers)",
    } | (set() if existing_issue else {"Nightly fuzz run failed (mcp-boundary)"})
    bodies = {issue["title"]: issue["body"] for issue in issues}
    assert "timed out" in bodies["Nightly fuzz run failed (mcp-redaction)"]
    assert "cancelled/incomplete" in bodies["Nightly fuzz run failed (importers)"]
    assert all(
        "https://github.com/zenml-io/kitaru/actions/runs/123" in body
        for body in bodies.values()
    )
    commands = [
        json.loads(line)
        for line in (tmp_path / "commands.jsonl").read_text().splitlines()
    ]
    assert commands[0][1] == "repos/zenml-io/kitaru/actions/runs/123/attempts/1/jobs"
    assert "--paginate" in commands[0]
    for command in commands[1:]:
        assert command[command.index("--repo") + 1] == "zenml-io/kitaru"


def test_reporter_selects_failed_timed_out_and_cancelled_matrix_jobs() -> None:
    jq = shutil.which("jq")
    if jq is None:
        pytest.skip("jq is required to exercise the GitHub CLI job selection")
    step = _get_workflow_step("report", "Open issues for failed or incomplete jobs")
    query = re.search(r"--jq '([^']+)'", step["run"])
    assert query is not None
    jobs = [
        {"name": f"fuzz ({conclusion})", "conclusion": conclusion}
        for conclusion in [
            "failure",
            "timed_out",
            "cancelled",
            "success",
            "skipped",
            None,
        ]
    ]
    jobs.append({"name": "Report scheduled fuzz failures", "conclusion": "failure"})
    result = subprocess.run(
        [jq, "-r", query.group(1)],
        input=json.dumps({"jobs": jobs}),
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "fuzz (failure)\tfailure",
        "fuzz (timed_out)\ttimed_out",
        "fuzz (cancelled)\tcancelled",
    ]


def test_timeout_leaves_time_for_diagnostics_and_cancelled_job_reporting() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/fuzz-nightly.yml").read_text()
    )
    fuzz = workflow["jobs"]["fuzz"]
    run = _get_workflow_step("fuzz", "Run fuzz recipe")
    assert run["timeout-minutes"] < fuzz["timeout-minutes"]
    report = workflow["jobs"]["report"]
    assert report["needs"] == "fuzz"
    assert "always()" in report["if"]
    assert "needs.fuzz.result == 'cancelled'" in report["if"]
    upload = _get_workflow_step("fuzz", "Upload fuzz diagnostics")
    assert "cancelled()" in upload["if"]
    assert "fuzz.log" in upload["with"]["path"]
