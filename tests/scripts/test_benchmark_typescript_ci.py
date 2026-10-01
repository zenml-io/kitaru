"""Verify the hosted TypeScript trial retains suites, coverage, and failures."""

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts/benchmark-typescript-ci.mjs"
PUBLISHED = ["kitaru", "kitaru-mastra", "kitaru-vercel-ai"]
EXAMPLES = [
    "kitaru-example-mastra-support-triage",
    "kitaru-example-mastra-adaptive-conversation",
]


@pytest.fixture
def fake_pnpm(tmp_path: Path) -> dict[str, str]:
    binary = tmp_path / "bin" / "pnpm"
    binary.parent.mkdir()
    binary.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "with Path(os.environ['PNPM_CALLS']).open('a') as log:\n"
        "    log.write(json.dumps({'args': args, 'postgres': "
        "os.environ.get('KITARU_TEST_MASTRA_POSTGRES_URL')}) + '\\n')\n"
        "if args[1] == os.environ.get('FAIL_PACKAGE'):\n"
        "    sys.exit(7)\n"
        "output = next(arg.split('=', 1)[1] for arg in args "
        "if arg.startswith('--outputFile='))\n"
        "Path(output).write_text(json.dumps({'success': True}))\n"
        "for arg in args:\n"
        "    if arg.startswith('--coverage.reportsDirectory='):\n"
        "        directory = Path(arg.split('=', 1)[1])\n"
        "        directory.mkdir(parents=True)\n"
        "        (directory / 'coverage-final.json').write_text('{}')\n"
        "        for metric in ['Statements', 'Branches', 'Functions', 'Lines']:\n"
        "            if metric != os.environ.get('OMIT_COVERAGE_METRIC'):\n"
        "                print(metric + ' : 100% ( 1/1 )')\n"
    )
    binary.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{binary.parent}{os.pathsep}{os.environ['PATH']}",
        "PNPM_CALLS": str(tmp_path / "calls.jsonl"),
        "KITARU_TEST_MASTRA_POSTGRES_URL": "postgres://localhost/test-only",
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary.md"),
        "GITHUB_SHA": "benchmark-test-sha",
    }


def _run_trial(
    tmp_path: Path, environment: dict[str, str], mode: str
) -> subprocess.CompletedProcess[str]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is unavailable")
    return subprocess.run(
        [node, str(SCRIPT), mode, str(tmp_path / "evidence")],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )


@pytest.mark.parametrize("mode", ["repeated", "coverage-once"])
def test_trial_retains_canonical_suites_and_postgres_coverage_environment(
    tmp_path: Path, fake_pnpm: dict[str, str], mode: str
) -> None:
    result = _run_trial(tmp_path, fake_pnpm, mode)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [
        json.loads(line)
        for line in Path(fake_pnpm["PNPM_CALLS"]).read_text().splitlines()
    ]
    expected_packages = PUBLISHED + EXAMPLES
    if mode == "repeated":
        expected_packages += PUBLISHED
    assert [call["args"][1] for call in calls] == [
        f"@zenml-io/{package}" for package in expected_packages
    ]
    assert all(
        call["postgres"] == fake_pnpm["KITARU_TEST_MASTRA_POSTGRES_URL"]
        for call in calls[:5]
    )
    for index, call in enumerate(calls):
        covered = (mode == "coverage-once" and index < 3) or index >= 5
        assert ("--coverage.enabled" in call["args"]) == covered
        assert call["args"][2:5] == ["exec", "vitest", "run"]
        if covered:
            assert "--coverage.provider=v8" in call["args"]
            assert "--coverage.include=src/**" in call["args"]
            assert "--coverage.exclude=src/generated/**" in call["args"]
            assert "--coverage.reporter=text-summary" in call["args"]
            assert "--coverage.reporter=json" in call["args"]
        if index in (3, 4):
            assert "test" in call["args"]
        if index >= 5:
            assert call["postgres"] is None

    evidence = tmp_path / "evidence"
    assert sorted(
        path.stem for path in (evidence / "outcomes").glob("*.json")
    ) == sorted(PUBLISHED + EXAMPLES)
    assert sorted(
        path.parent.name
        for path in (evidence / "coverage").glob("*/coverage-final.json")
    ) == sorted(PUBLISHED)
    metadata = json.loads((evidence / "runs.json").read_text())
    assert metadata["mode"] == mode
    assert metadata["sha"] == "benchmark-test-sha"
    assert len(metadata["runs"]) == len(calls)
    assert all(run["exitCode"] == 0 for run in metadata["runs"])
    assert all(run["elapsedMs"] >= 0 for run in metadata["runs"])
    repo_root = SCRIPT.parents[1]
    script = json.loads((repo_root / "package.json").read_text())["scripts"][
        "test:built"
    ]
    tokens = shlex.split(script)
    selected_packages = [
        tokens[index + 1] for index, token in enumerate(tokens) if token == "--filter"
    ]
    assert [
        call["args"][1]
        for call, run in zip(calls, metadata["runs"], strict=True)
        if run["canonical"]
    ] == selected_packages
    manifests = {
        manifest["name"]: (path, manifest)
        for path in (
            *repo_root.glob("packages/*/package.json"),
            *repo_root.glob("examples/typescript/*/package.json"),
        )
        for manifest in [json.loads(path.read_text())]
    }
    for package in selected_packages:
        path, manifest = manifests[package]
        expected_script = (
            "vitest run"
            if path.relative_to(repo_root).parts[0] == "packages"
            else "vitest run test"
        )
        assert manifest["scripts"]["test"] == expected_script
    assert Path(fake_pnpm["GITHUB_STEP_SUMMARY"]).read_text().count("Lines : 100%") == 3


def test_trial_propagates_test_failure_without_running_remaining_suites(
    tmp_path: Path, fake_pnpm: dict[str, str]
) -> None:
    fake_pnpm["FAIL_PACKAGE"] = "@zenml-io/kitaru"
    result = _run_trial(tmp_path, fake_pnpm, "coverage-once")

    assert result.returncode == 7
    calls = Path(fake_pnpm["PNPM_CALLS"]).read_text().splitlines()
    assert len(calls) == 1
    metadata = json.loads((tmp_path / "evidence/runs.json").read_text())
    assert metadata["runs"][0]["exitCode"] == 7


def test_trial_rejects_unsupported_mode_before_running_pnpm(
    tmp_path: Path, fake_pnpm: dict[str, str]
) -> None:
    result = _run_trial(tmp_path, fake_pnpm, "unsupported")

    assert result.returncode == 1
    assert "Usage:" in result.stderr
    assert not Path(fake_pnpm["PNPM_CALLS"]).exists()


def test_trial_fails_when_coverage_summary_omits_a_metric(
    tmp_path: Path, fake_pnpm: dict[str, str]
) -> None:
    fake_pnpm["OMIT_COVERAGE_METRIC"] = "Branches"
    fake_pnpm.pop("GITHUB_STEP_SUMMARY")
    result = _run_trial(tmp_path, fake_pnpm, "coverage-once")

    assert result.returncode == 1
    assert "omitted an expected coverage summary metric" in result.stderr
    assert len(Path(fake_pnpm["PNPM_CALLS"]).read_text().splitlines()) == 1
