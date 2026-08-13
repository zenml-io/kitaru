import json
from pathlib import Path

import pytest
from scripts.release_ui import UIReleaseError, load_ui_release


def _write_declaration(root: Path, version: str, **changes: object) -> None:
    values: dict[str, object] = {
        "schema-version": 1,
        "kitaru-version": version,
        "ui-repository": "zenml-io/zenml-frontend-monorepo",
        "ui-tag": "kitaru-ui-v0.3.0-rc.1",
        "ui-archive": "kitaru-ui.tar.gz",
        "ui-sha256": "a" * 64,
        "allow-prerelease": True,
    }
    values.update(changes)
    path = root / "releases" / "python" / "kitaru" / f"{version}.toml"
    path.parent.mkdir(parents=True)
    lines = []
    for key, value in values.items():
        if isinstance(value, bool):
            rendered = str(value).lower()
        elif isinstance(value, int):
            rendered = str(value)
        else:
            rendered = json.dumps(value)
        lines.append(f"{key} = {rendered}")
    path.write_text("\n".join(lines) + "\n")


def test_load_ui_release_accepts_an_exact_rc_artifact(tmp_path: Path) -> None:
    _write_declaration(tmp_path, "0.22.0rc1")

    release = load_ui_release("0.22.0rc1", tmp_path)

    assert release.tag == "kitaru-ui-v0.3.0-rc.1"
    assert release.sha256 == "a" * 64
    assert json.loads(release.to_json())["allow_prerelease"] is True


def test_load_ui_release_requires_the_requested_version(tmp_path: Path) -> None:
    _write_declaration(
        tmp_path,
        "0.22.0rc1",
        **{"kitaru-version": "0.22.0rc2"},
    )

    with pytest.raises(UIReleaseError, match="does not match"):
        load_ui_release("0.22.0rc1", tmp_path)


def test_load_ui_release_rejects_an_unapproved_prerelease(tmp_path: Path) -> None:
    _write_declaration(
        tmp_path,
        "0.22.0rc1",
        **{"allow-prerelease": False},
    )

    with pytest.raises(UIReleaseError, match="requires allow-prerelease"):
        load_ui_release("0.22.0rc1", tmp_path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("ui-repository", "other/repository", "ui-repository"),
        ("ui-archive", "ui.zip", "ui-archive"),
        ("ui-sha256", "not-a-checksum", "ui-sha256"),
    ],
)
def test_load_ui_release_rejects_untrusted_identity(
    tmp_path: Path, field: str, value: str, message: str
) -> None:
    _write_declaration(tmp_path, "0.22.0rc1", **{field: value})

    with pytest.raises(UIReleaseError, match=message):
        load_ui_release("0.22.0rc1", tmp_path)
