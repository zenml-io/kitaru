"""Tests for the exact-target exporter smoke driver."""

import subprocess
from pathlib import Path

from scripts import smoke_export_artifacts


def test_copy_template_source_accepts_relative_path(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "kitaru-template"
    source.mkdir()
    (source / "README.md").write_text("template\n")
    subprocess.run(["git", "init", "-q"], cwd=source, check=True)
    subprocess.run(["git", "add", "README.md"], cwd=source, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Kitaru Tests",
            "-c",
            "user.email=tests@kitaru.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=source,
        check=True,
    )
    monkeypatch.setattr(smoke_export_artifacts, "KITARU_TEMPLATE_COMMIT", "HEAD")
    monkeypatch.chdir(tmp_path)

    destination = tmp_path / "copied"
    smoke_export_artifacts._copy_template_source(Path("kitaru-template"), destination)

    assert (destination / "README.md").read_text() == "template\n"
