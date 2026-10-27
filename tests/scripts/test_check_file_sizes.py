"""Exercise the code file size ratchet against the limit and its baseline."""

import json
import subprocess
from pathlib import Path

import pytest
from scripts.check_file_sizes import (
    BASELINE_PATH,
    find_violations,
    is_checked,
    main,
    tighten_baseline,
)


@pytest.mark.parametrize(
    ("path", "checked"),
    [
        ("src/kitaru/cli/app.py", True),
        ("packages/mastra/src/memory-binding.ts", True),
        ("docs/components/diagrams.tsx", True),
        ("packages/core/src/generated/openapi.ts", False),
        ("src/kitaru/server/database/migrations/versions/001_initial.py", False),
        ("openapi/openapi.json", False),
        ("uv.lock", False),
    ],
)
def test_only_hand_written_code_is_checked(path: str, checked: bool) -> None:
    """Skip generated code, frozen migrations, data files, and lock files."""
    assert is_checked(path) is checked


def test_limit_applies_to_unlisted_files() -> None:
    """Allow files at the limit and reject a new file one line over it."""
    findings = find_violations({"ok.py": 10, "big.py": 11}, {}, limit=10)
    assert [finding.path for finding in findings] == ["big.py"]
    assert "exceeds the 10-line limit" in findings[0].message


def test_listed_file_may_not_grow() -> None:
    """Hold a listed file to its recorded count instead of the limit."""
    assert find_violations({"old.py": 50}, {"old.py": 50}, limit=10) == []
    findings = find_violations({"old.py": 51}, {"old.py": 50}, limit=10)
    assert [finding.path for finding in findings] == ["old.py"]
    assert "grew from 50 to 51" in findings[0].message


@pytest.mark.parametrize(
    ("counts", "expected"),
    [({"old.py": 40}, "shrank to 40 lines"), ({}, "is gone")],
)
def test_stale_baseline_entries_fail(counts: dict[str, int], expected: str) -> None:
    """Report entries that would leave slack for the file to grow back."""
    findings = find_violations(counts, {"old.py": 50}, limit=10)
    assert [finding.path for finding in findings] == ["old.py"]
    assert expected in findings[0].message
    assert "--update" in findings[0].message


def test_tighten_baseline_only_lowers_entries() -> None:
    """Lower shrunk entries, drop finished or deleted ones, and never raise any."""
    baseline = {"shrank.py": 50, "grew.py": 50, "fixed.py": 50, "gone.py": 50}
    counts = {"shrank.py": 40, "grew.py": 60, "fixed.py": 10, "new.py": 99}
    assert tighten_baseline(counts, baseline, limit=10) == {
        "grew.py": 50,
        "shrank.py": 40,
    }


def _make_repo(root: Path, files: dict[str, int], baseline: dict[str, int]) -> None:
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    for name, lines in files.items():
        (root / name).write_text("x = 1\n" * lines, encoding="utf-8")
    (root / BASELINE_PATH).parent.mkdir(parents=True)
    (root / BASELINE_PATH).write_text(json.dumps(baseline), encoding="utf-8")


def test_cli_reports_violations_and_update_tightens(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Find untracked files, fail on growth, and let --update lower the record."""
    _make_repo(tmp_path, {"new.py": 1001, "old.py": 1200}, {"old.py": 1100})
    assert main(["--root", str(tmp_path)]) == 1
    lines = capsys.readouterr().out.splitlines()
    assert [line.split(": ")[0] for line in lines] == ["new.py", "old.py"]

    (tmp_path / "new.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "old.py").write_text("x = 1\n" * 1050, encoding="utf-8")
    assert main(["--root", str(tmp_path)]) == 1
    assert "shrank to 1050" in capsys.readouterr().out
    assert main(["--root", str(tmp_path), "--update"]) == 0
    baseline = json.loads((tmp_path / BASELINE_PATH).read_text(encoding="utf-8"))
    assert baseline == {"old.py": 1050}


def test_cli_rejects_missing_baseline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A wrong root must not look like a passing check."""
    assert main(["--root", str(tmp_path)]) == 2
    assert "checker failed" in capsys.readouterr().err
