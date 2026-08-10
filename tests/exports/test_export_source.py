"""Tests for safe agent source inventory."""

import os
import stat
from pathlib import Path

import pytest

from kitaru.exports.models import ExportError
from kitaru.exports.source import copy_source, inventory_source


def test_inventory_source_is_stable_and_excludes_sensitive_files(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    (tmp_path / "notes.txt").write_text("notes\n")
    (tmp_path / ".env.local").write_text("TOKEN=secret\n")
    (tmp_path / "credentials.json").write_text("{}\n")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("private\n")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "dependency.js").write_text("large\n")

    first = inventory_source(tmp_path)
    second = inventory_source(tmp_path)

    assert [file.path for file in first.files] == ["agent.py", "notes.txt"]
    assert first.digest == second.digest
    assert ".env.local" in first.excluded
    assert "credentials.json" in first.excluded
    assert ".git" in first.excluded
    assert "node_modules" in first.excluded


def test_inventory_source_rejects_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside.txt"
    outside.write_text("secret")
    os.symlink(outside, tmp_path / "escape")

    with pytest.raises(ExportError, match="source_symlink_escape"):
        inventory_source(tmp_path)


def test_inventory_source_excludes_nested_destination(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    destination = tmp_path / "generated"
    destination.mkdir()
    (destination / "old.txt").write_text("old")

    inventory = inventory_source(tmp_path, destination=destination)

    assert [file.path for file in inventory.files] == ["agent.py"]
    assert "generated" in inventory.excluded


def test_inventory_source_enforces_total_size(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_bytes(b"12345")

    with pytest.raises(ExportError, match="source_too_large"):
        inventory_source(tmp_path, max_total_bytes=4)


def test_copy_source_uses_inventory_and_detects_changes(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source_file = root / "agent.py"
    source_file.write_text("print('ok')\n")
    source_file.chmod(0o755)
    inventory = inventory_source(root)

    copy_source(inventory, tmp_path / "copy")
    assert (tmp_path / "copy" / "agent.py").read_text() == "print('ok')\n"
    assert stat.S_IMODE((tmp_path / "copy" / "agent.py").stat().st_mode) == 0o755

    source_file.write_text("print('changed')\n")
    with pytest.raises(ExportError, match="source_changed"):
        copy_source(inventory, tmp_path / "changed-copy")
