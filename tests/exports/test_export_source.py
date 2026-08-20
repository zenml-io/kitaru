"""Tests for safe agent source inventory."""

import os
import stat
from pathlib import Path
from typing import BinaryIO

import pytest

from kitaru.exports._dependencies import classify_dependencies
from kitaru.exports._runtime import reject_protected_source
from kitaru.exports._sanitize import EphemeralSanitizer
from kitaru.exports.models import ExportError, SourcePolicy
from kitaru.exports.source import copy_source, inventory_source


def test_inventory_source_is_stable_and_excludes_sensitive_files(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    (tmp_path / "notes.txt").write_text("notes\n")
    (tmp_path / ".env.local").write_text("TOKEN=secret\n")
    (tmp_path / ".envrc").write_text("export TOKEN=secret\n")
    (tmp_path / "credentials.json").write_text("{}\n")
    (tmp_path / ".kitaru").mkdir()
    (tmp_path / ".kitaru" / "state.json").write_text("{}\n")
    (tmp_path / ".zen").mkdir()
    (tmp_path / ".state").mkdir()
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("private\n")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "dependency.js").write_text("large\n")

    first = inventory_source(tmp_path)
    second = inventory_source(tmp_path)

    assert [file.path for file in first.files] == ["agent.py", "notes.txt"]
    assert first.digest == second.digest
    assert ".env.local" in first.excluded
    assert ".envrc" in first.excluded
    assert "credentials.json" in first.excluded
    assert ".kitaru" in first.excluded
    assert ".zen" in first.excluded
    assert ".state" in first.excluded
    assert ".git" in first.excluded
    assert "node_modules" in first.excluded


def test_inventory_source_rejects_every_symlink(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside.txt"
    outside.write_text("secret")
    os.symlink(outside, tmp_path / "escape")

    with pytest.raises(ExportError, match="unsupported_source_symlink"):
        inventory_source(tmp_path)


def test_inventory_source_rejects_directory_symlink_even_when_generated(
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    os.symlink(outside, tmp_path / "node_modules")

    with pytest.raises(ExportError, match="unsupported_source_symlink"):
        inventory_source(tmp_path)


def test_inventory_source_rejects_symlink_root(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir()
    linked_root = tmp_path / "linked"
    os.symlink(real_root, linked_root)

    with pytest.raises(ExportError, match="unsupported_source_symlink"):
        inventory_source(linked_root)


def test_inventory_source_allows_explicit_generated_file_include(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "main.js").write_text("main();\n")
    (tmp_path / "dist" / "map.js").write_text("map();\n")

    default = inventory_source(tmp_path)
    included = inventory_source(
        tmp_path,
        source_policy=SourcePolicy(include=("dist/main.js",)),
    )

    assert [item.path for item in default.files] == ["agent.py"]
    assert [item.path for item in included.files] == ["agent.py", "dist/main.js"]
    assert "dist/map.js" in included.excluded


@pytest.mark.parametrize(
    "path",
    [".env", ".envrc", "id_rsa", "private.pem", ".kitaru/state.json"],
)
def test_inventory_source_rejects_protected_explicit_include(
    tmp_path: Path, path: str
) -> None:
    with pytest.raises(ExportError, match="protected_source_path"):
        inventory_source(tmp_path, source_policy=SourcePolicy(include=(path,)))


def test_inventory_source_rejects_missing_policy_path(tmp_path: Path) -> None:
    with pytest.raises(ExportError, match="source_policy_path_missing"):
        inventory_source(
            tmp_path,
            source_policy=SourcePolicy(include=("dist/main.js",)),
        )


def test_inventory_source_excludes_nested_destination(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    destination = tmp_path / "generated"
    destination.mkdir()
    (destination / "old.txt").write_text("old")

    inventory = inventory_source(tmp_path, destination=destination)

    assert [file.path for file in inventory.files] == ["agent.py"]
    assert "generated" in inventory.excluded


def test_inventory_source_excludes_archive_and_stale_staging_siblings(
    tmp_path: Path,
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    (tmp_path / "bundle.zip").write_text("old archive")
    (tmp_path / ".bundle.kitaru-stale.tmp").mkdir()
    (tmp_path / ".bundle.kitaru-stale.tmp" / "partial").write_text("partial")

    inventory = inventory_source(
        tmp_path,
        destination=tmp_path / "bundle",
        archive_path=tmp_path / "bundle.zip",
    )

    assert [file.path for file in inventory.files] == ["agent.py"]
    assert ".bundle.kitaru-stale.tmp" in inventory.excluded
    assert "bundle.zip" in inventory.excluded


def test_inventory_source_enforces_total_size(tmp_path: Path) -> None:
    (tmp_path / "agent.py").write_bytes(b"12345")

    with pytest.raises(ExportError, match="source_too_large"):
        inventory_source(tmp_path, max_total_bytes=4)


def test_inventory_source_enforces_path_count_and_utf8_length(tmp_path: Path) -> None:
    (tmp_path / "one.py").write_text("1")
    inventory_source(tmp_path, max_files=1)
    (tmp_path / "two.py").write_text("2")
    with pytest.raises(ExportError, match="too_many_source_files"):
        inventory_source(tmp_path, max_files=1)

    (tmp_path / "two.py").unlink()
    with pytest.raises(ExportError, match="source_path_too_long"):
        inventory_source(tmp_path, max_path_bytes=5)


def test_inventory_source_rejects_hard_links(tmp_path: Path) -> None:
    source = tmp_path / "agent.py"
    source.write_text("print('ok')\n")
    try:
        os.link(source, tmp_path / "alias.py")
    except OSError as error:
        pytest.skip(f"hard links are unavailable: {error}")

    with pytest.raises(ExportError, match="unsupported_source_hardlink"):
        inventory_source(tmp_path)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFOs are unavailable")
def test_inventory_source_rejects_nonregular_files(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "pipe")

    with pytest.raises(ExportError, match="unsupported_source_file"):
        inventory_source(tmp_path)


def test_copy_source_uses_the_bytes_opened_during_inventory(tmp_path: Path) -> None:
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
    copy_source(inventory, tmp_path / "second-copy")
    assert (tmp_path / "second-copy" / "agent.py").read_text() == "print('ok')\n"


def test_inventory_source_opens_each_file_once_and_copy_does_not_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "agent.py").write_text("print('ok')\n")
    opened: list[Path] = []
    from kitaru.exports import source as source_module

    original = source_module._open_source_file

    def recording_open(path: Path) -> BinaryIO:
        opened.append(path)
        return original(path)

    monkeypatch.setattr(source_module, "_open_source_file", recording_open)
    inventory = inventory_source(tmp_path)
    copy_source(inventory, tmp_path / "copy")

    assert opened == [tmp_path / "agent.py"]


def test_inventory_source_rejects_mutation_during_open_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_file = tmp_path / "agent.py"
    source_file.write_text("before\n")
    from kitaru.exports import source as source_module

    original = source_module._read_opened_file

    def mutating_read(source: BinaryIO, *, max_bytes: int) -> bytes:
        content = original(source, max_bytes=max_bytes)
        source_file.write_text("after!\n")
        return content

    monkeypatch.setattr(source_module, "_read_opened_file", mutating_read)

    with pytest.raises(ExportError, match="source_changed"):
        inventory_source(tmp_path)


def test_inventory_source_rejects_identity_change_during_open_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_file = tmp_path / "agent.py"
    source_file.write_text("before\n")
    replacement = tmp_path / "replacement"
    from kitaru.exports import source as source_module

    original = source_module._read_opened_file

    def replacing_read(source: BinaryIO, *, max_bytes: int) -> bytes:
        content = original(source, max_bytes=max_bytes)
        replacement.write_text("before\n")
        replacement.replace(source_file)
        return content

    monkeypatch.setattr(source_module, "_read_opened_file", replacing_read)

    with pytest.raises(ExportError, match="source_changed"):
        inventory_source(tmp_path)


def test_dependency_and_protected_source_checks_use_retained_snapshot_bytes(
    tmp_path: Path,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "agent"\nversion = "1"\ndependencies = []\n'
    )
    agent = tmp_path / "agent.py"
    agent.write_text('TOKEN = "secret-value"\n')
    inventory = inventory_source(tmp_path)

    pyproject.write_text("not valid TOML")
    agent.write_text("TOKEN = None\n")

    assert classify_dependencies(inventory).status == "declared"
    with pytest.raises(ExportError, match="protected_value_in_source"):
        reject_protected_source(
            inventory,
            sanitizer=EphemeralSanitizer(["secret-value"]),
        )
