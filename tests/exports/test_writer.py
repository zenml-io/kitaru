"""Tests for deterministic and conflict-safe bundle writing."""

import zipfile
from pathlib import Path

import pytest

from kitaru.exports.models import ExportError
from kitaru.exports.writer import (
    directory_digest,
    publish_bundle,
    write_canonical_json,
)


def _render(root: Path) -> None:
    write_canonical_json(root / "kitaru-export.json", {"z": 1, "a": [2, 3]})
    (root / "nested").mkdir()
    (root / "nested" / "agent.py").write_text("print('ok')\n")


def test_publish_bundle_writes_canonical_json_and_deterministic_zip(
    tmp_path: Path,
) -> None:
    first = publish_bundle(tmp_path / "first", _render, archive=True)
    second = publish_bundle(tmp_path / "second", _render, archive=True)

    assert (
        first.destination / "kitaru-export.json"
    ).read_bytes() == b'{"a":[2,3],"z":1}\n'
    assert first.digest == second.digest == directory_digest(first.destination)
    assert first.archive_path is not None
    assert second.archive_path is not None
    assert first.archive_path.read_bytes() == second.archive_path.read_bytes()
    with zipfile.ZipFile(first.archive_path) as archive:
        assert archive.namelist() == ["kitaru-export.json", "nested/agent.py"]
        assert all(
            info.date_time == (1980, 1, 1, 0, 0, 0) for info in archive.infolist()
        )


def test_publish_bundle_refuses_existing_destination(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    destination.mkdir()

    with pytest.raises(ExportError, match="destination_conflict"):
        publish_bundle(destination, _render)

    assert list(tmp_path.iterdir()) == [destination]


def test_publish_bundle_refuses_existing_archive(tmp_path: Path) -> None:
    archive = tmp_path / "bundle.zip"
    archive.write_bytes(b"existing")

    with pytest.raises(ExportError, match="archive_conflict"):
        publish_bundle(tmp_path / "bundle", _render, archive=True)

    assert archive.read_bytes() == b"existing"
    assert not (tmp_path / "bundle").exists()


def test_publish_bundle_cleans_only_its_staging_directory_on_failure(
    tmp_path: Path,
) -> None:
    unrelated = tmp_path / ".bundle.kitaru-unrelated.tmp"
    unrelated.mkdir()

    def fail(root: Path) -> None:
        (root / "partial.txt").write_text("partial")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        publish_bundle(tmp_path / "bundle", fail)

    assert unrelated.exists()
    assert not (tmp_path / "bundle").exists()
    assert list(tmp_path.iterdir()) == [unrelated]
