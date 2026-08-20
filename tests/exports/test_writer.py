"""Tests for deterministic and conflict-safe bundle writing."""

import os
import stat
import threading
import zipfile
from pathlib import Path

import pytest

from kitaru.exports.models import ExportError
from kitaru.exports.operation import ExportOperationRevoked
from kitaru.exports.writer import (
    commit_staged_bundle,
    directory_digest,
    publish_bundle,
    stage_bundle,
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


def test_staging_and_commit_are_separate(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"

    staged = stage_bundle(destination, _render, archive=True)

    assert not destination.exists()
    assert not destination.with_suffix(".zip").exists()
    published = commit_staged_bundle(staged)
    assert published.destination == destination
    assert published.archive_path == destination.with_suffix(".zip")


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


def test_publish_bundle_reports_crash_style_incomplete_reservation(
    tmp_path: Path,
) -> None:
    reservation = tmp_path / ".bundle.kitaru-reservation"
    reservation.write_bytes(b"previous-export")

    with pytest.raises(ExportError, match="incomplete export reservation"):
        publish_bundle(tmp_path / "bundle", _render)

    assert reservation.read_bytes() == b"previous-export"
    assert not (tmp_path / "bundle").exists()


def test_publish_bundle_preserves_destination_created_after_staging(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "bundle"

    def race(root: Path) -> None:
        _render(root)
        destination.mkdir()
        (destination / "existing.txt").write_bytes(b"existing")

    with pytest.raises(ExportError, match="destination_conflict"):
        publish_bundle(destination, race, archive=True)

    assert (destination / "existing.txt").read_bytes() == b"existing"
    assert not destination.with_suffix(".zip").exists()


def test_publish_bundle_preserves_archive_created_after_staging(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    archive = destination.with_suffix(".zip")

    def race(root: Path) -> None:
        _render(root)
        archive.write_bytes(b"existing")

    with pytest.raises(ExportError, match="archive_conflict"):
        publish_bundle(destination, race, archive=True)

    assert archive.read_bytes() == b"existing"
    assert not destination.exists()


def test_directory_conflict_rolls_back_an_already_published_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "bundle"

    def conflict(_source: Path, target: Path) -> None:
        target.mkdir()
        (target / "existing.txt").write_bytes(b"existing")
        raise FileExistsError(target)

    monkeypatch.setattr("kitaru.exports.writer._publish_directory_noreplace", conflict)

    with pytest.raises(ExportError, match="destination_conflict"):
        publish_bundle(destination, _render, archive=True)

    assert (destination / "existing.txt").read_bytes() == b"existing"
    assert not destination.with_suffix(".zip").exists()


def test_rollback_refuses_to_delete_a_swapped_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "bundle"
    archive = destination.with_suffix(".zip")
    owned_archive = tmp_path / "owned-archive-residue.zip"

    def swap_then_conflict(_source: Path, target: Path) -> None:
        archive.rename(owned_archive)
        archive.write_bytes(b"preexisting")
        target.mkdir()
        raise FileExistsError(target)

    monkeypatch.setattr(
        "kitaru.exports.writer._publish_directory_noreplace", swap_then_conflict
    )

    with pytest.raises(ExportError, match="cleanup_ownership_lost"):
        publish_bundle(destination, _render, archive=True)

    assert archive.read_bytes() == b"preexisting"
    assert owned_archive.exists()


def test_concurrent_exporters_do_not_overwrite(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    rendering = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    def blocked_render(root: Path) -> None:
        rendering.set()
        assert release.wait(timeout=5)
        _render(root)

    def first_export() -> None:
        try:
            publish_bundle(destination, blocked_render)
        except BaseException as error:
            failures.append(error)

    worker = threading.Thread(target=first_export)
    worker.start()
    assert rendering.wait(timeout=5)
    try:
        with pytest.raises(ExportError, match="destination_conflict"):
            publish_bundle(destination, _render)
    finally:
        release.set()
        worker.join(timeout=5)

    assert not worker.is_alive()
    assert failures == []
    assert (destination / "kitaru-export.json").exists()


def test_rendering_observes_cancellation_checkpoint(tmp_path: Path) -> None:
    revoked = False

    def checkpoint() -> None:
        if revoked:
            raise ExportOperationRevoked

    def render(root: Path) -> None:
        nonlocal revoked
        revoked = True
        write_canonical_json(root / "must-not-complete.json", {"value": 1})

    with pytest.raises(ExportOperationRevoked):
        publish_bundle(
            tmp_path / "bundle",
            render,
            cancellation_checkpoint=checkpoint,
        )

    assert list(tmp_path.iterdir()) == []


def test_archive_creation_observes_cancellation_checkpoint(tmp_path: Path) -> None:
    def checkpoint() -> None:
        if tuple(tmp_path.glob(".bundle.kitaru-*.tmp.zip")):
            raise ExportOperationRevoked

    with pytest.raises(ExportOperationRevoked):
        publish_bundle(
            tmp_path / "bundle",
            _render,
            archive=True,
            cancellation_checkpoint=checkpoint,
        )

    assert list(tmp_path.iterdir()) == []


def test_publication_is_private_under_a_permissive_umask(tmp_path: Path) -> None:
    previous_umask = os.umask(0)
    try:
        published = publish_bundle(tmp_path / "bundle", _render, archive=True)
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE(published.destination.stat().st_mode) == 0o700
    assert published.archive_path is not None
    assert stat.S_IMODE(published.archive_path.stat().st_mode) == 0o600


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


def test_cleanup_refuses_a_swapped_staging_path(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    moved_staging = tmp_path / "owned-staging"
    replacement: Path | None = None

    def swap(root: Path) -> None:
        nonlocal replacement
        root.rename(moved_staging)
        root.mkdir()
        replacement = root
        (root / "preexisting.txt").write_bytes(b"preserve")
        raise RuntimeError("boom")

    with pytest.raises(ExportError, match="cleanup_ownership_lost"):
        publish_bundle(destination, swap)

    assert replacement is not None
    assert (replacement / "preexisting.txt").read_bytes() == b"preserve"
    assert moved_staging.exists()
    assert not destination.exists()


def test_cleanup_refuses_a_swapped_destination_parent(tmp_path: Path) -> None:
    parent = tmp_path / "shared"
    parent.mkdir()
    moved_parent = tmp_path / "moved-shared"
    destination = parent / "bundle"

    def swap(root: Path) -> None:
        _render(root)
        parent.rename(moved_parent)
        parent.mkdir()
        (parent / "preexisting.txt").write_bytes(b"preserve")
        raise RuntimeError("boom")

    with pytest.raises(ExportError, match="destination_parent_changed"):
        publish_bundle(destination, swap)

    assert (parent / "preexisting.txt").read_bytes() == b"preserve"
    assert not destination.exists()
    assert moved_parent.exists()
