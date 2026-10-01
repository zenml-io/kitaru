"""Verify parallel pytest execution preserves CI coverage and failures."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def parallel_test_project(tmp_path: Path) -> Path:
    (tmp_path / "subject.py").write_text(
        "def choose(child: bool) -> str:\n"
        "    if child:\n"
        '        return "child"\n'
        '    return "parent"\n'
    )
    (tmp_path / "test_parent.py").write_text(
        "from subject import choose\n\n"
        "def test_parent() -> None:\n"
        '    assert choose(False) == "parent"\n'
    )
    (tmp_path / "test_child.py").write_text(
        "import subprocess\n"
        "import sys\n\n"
        "def test_child() -> None:\n"
        "    subprocess.run(\n"
        '        [sys.executable, "-c",\n'
        "         \"from subject import choose; assert choose(True) == 'child'\"],\n"
        "        check=True, timeout=15,\n"
        "    )\n"
    )
    (tmp_path / "pytest.ini").write_text("[pytest]\naddopts =\n")
    (tmp_path / ".coveragerc").write_text(
        "[run]\n"
        "source = subject\n"
        "branch = true\n"
        "relative_files = true\n"
        "patch = subprocess\n"
    )
    return tmp_path


def _run_python(
    project: Path,
    data_file: Path,
    *arguments: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("COVERAGE_")
        and key
        not in {"PYTEST_ADDOPTS", "KITARU_TEST_SHARD_INDEX", "KITARU_TEST_SHARD_COUNT"}
    }
    env["COVERAGE_FILE"] = str(data_file)
    env.update(env_overrides or {})
    return subprocess.run(
        [sys.executable, "-m", *arguments],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_parallel_execution_preserves_subprocess_lines_and_branches(
    parallel_test_project: Path,
) -> None:
    project = parallel_test_project
    reports = []
    for mode, pytest_arguments in (
        ("serial", ()),
        ("parallel", ("-n", "2", "--dist", "loadfile")),
    ):
        data_file = project / f".coverage-{mode}"
        result = _run_python(
            project,
            data_file,
            "coverage",
            "run",
            "--rcfile=.coveragerc",
            "-m",
            "pytest",
            "-c",
            "pytest.ini",
            "--noconftest",
            "-q",
            *pytest_arguments,
            "test_parent.py",
            "test_child.py",
        )
        assert result.returncode == 0, result.stdout + result.stderr
        combined = _run_python(project, data_file, "coverage", "combine")
        assert combined.returncode == 0, combined.stdout + combined.stderr
        report_path = project / f"{mode}.json"
        report = _run_python(
            project, data_file, "coverage", "json", "-o", str(report_path)
        )
        assert report.returncode == 0, report.stdout + report.stderr
        reports.append(json.loads(report_path.read_text())["files"]["subject.py"])

    for report in reports:
        assert set(report["executed_lines"]) == {1, 2, 3, 4}
        assert [2, 3] in report["executed_branches"]
        assert [2, 4] in report["executed_branches"]
        assert report["missing_lines"] == []
        assert report["missing_branches"] == []
    assert reports[0]["executed_lines"] == reports[1]["executed_lines"]
    assert reports[0]["executed_branches"] == reports[1]["executed_branches"]


def test_parallel_execution_returns_failure_for_a_failed_test(
    parallel_test_project: Path,
) -> None:
    project = parallel_test_project
    (project / "test_failure.py").write_text(
        "def test_injected_failure() -> None:\n"
        '    assert False, "injected test failure"\n'
    )
    result = _run_python(
        project,
        project / ".coverage-failure",
        "pytest",
        "-c",
        "pytest.ini",
        "--noconftest",
        "-q",
        "-n",
        "2",
        "--dist",
        "loadfile",
        "test_parent.py",
        "test_failure.py",
    )

    assert result.returncode == 1
    assert "FAILED test_failure.py::test_injected_failure" in result.stdout
    assert "injected test failure" in result.stdout


@pytest.mark.parametrize(
    "module",
    ["tests/server/test_insights_api.py", "tests/server/test_task_domain.py"],
)
def test_ci_collection_is_stable_across_processes_and_hash_seeds(
    tmp_path: Path, module: str
) -> None:
    repo_root = Path(__file__).parents[2]
    collections = []
    for seed in ("1", "2"):
        result = _run_python(
            repo_root,
            tmp_path / f".coverage-{seed}",
            "pytest",
            "--collect-only",
            "-q",
            "-o",
            "addopts=",
            module,
            env_overrides={"PYTHONHASHSEED": seed},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        node_ids = [
            line
            for line in result.stdout.splitlines()
            if line.startswith(f"{module}::")
        ]
        assert node_ids, result.stdout + result.stderr
        collections.append(node_ids)

    assert collections[0] == collections[1]
