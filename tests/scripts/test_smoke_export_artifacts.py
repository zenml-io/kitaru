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


def test_generator_uses_installed_exporter_discovery() -> None:
    source = Path(smoke_export_artifacts.__file__).read_text()
    generate_start = source.index("def _generate_artifacts(")
    generate_end = source.index("\ndef _verify_harbor(", generate_start)
    generator = source[generate_start:generate_end]

    assert 'resolve_exporter("harbor")' in generator
    assert 'resolve_exporter("verifiers-v1")' in generator
    assert "kitaru.exports.formats" not in generator


def test_exact_target_smoke_builds_both_exporter_wheels() -> None:
    source = Path(smoke_export_artifacts.__file__).read_text()

    assert 'repository / "plugins/packages/harbor-exporter"' in source
    assert 'repository / "plugins/packages/verifiers-exporter"' in source
    assert 'root / "core-only"' in source
    assert 'root / f"one-{format_name}"' in source
