"""Focused contract tests for scripts/download-ui.sh release selection."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOWNLOAD_SCRIPT = _REPO_ROOT / "scripts" / "download-ui.sh"
_JUSTFILE = _REPO_ROOT / "Justfile"


def _make_ui_archive(tmp_path: Path) -> tuple[Path, Path, str]:
    """Create a tiny valid Kitaru UI archive and checksum file."""
    source_dir = tmp_path / "ui-source"
    source_dir.mkdir()
    (source_dir / "index.html").write_text("<html>Kitaru UI</html>\n")
    assets_dir = source_dir / "assets"
    assets_dir.mkdir()
    (assets_dir / "app.js").write_text("console.log('kitaru');\n")

    archive = tmp_path / "kitaru-ui.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for path in sorted(source_dir.rglob("*")):
            tar.add(path, arcname=path.relative_to(source_dir))

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checksum = tmp_path / "kitaru-ui.tar.gz.sha256"
    checksum.write_text(f"{digest}  kitaru-ui.tar.gz\n")
    return archive, checksum, digest


def _write_fake_curl(tmp_path: Path) -> Path:
    """Write a fake curl that serves release JSON and local asset files."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl = bin_dir / "curl"
    curl.write_text(
        r"""#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import sys
from urllib.parse import unquote

args = sys.argv[1:]
log_path = os.environ.get("FAKE_CURL_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(json.dumps(args) + "\n")

dest = None
url = None
skip_next = False
for index, arg in enumerate(args):
    if skip_next:
        skip_next = False
        continue
    if arg in {"-o", "--output"}:
        dest = args[index + 1]
        skip_next = True
        continue
    if arg in {"-H", "--header", "-w"}:
        skip_next = True
        continue
    if arg.startswith("-"):
        continue
    url = arg

if not url:
    print("fake curl: no URL", file=sys.stderr)
    raise SystemExit(2)

if url.endswith("/releases?per_page=100"):
    sys.stdout.write(os.environ["FAKE_RELEASES_JSON"])
    raise SystemExit(0)

if "/releases/tags/" in url:
    tag = unquote(url.rsplit("/", 1)[-1])
    prerelease_tags = set(
        filter(None, os.environ.get("FAKE_PRERELEASE_TAGS", "").split(","))
    )
    release = {
        "tag_name": tag,
        "draft": False,
        "prerelease": tag in prerelease_tags,
        "assets": [
            {
                "name": "kitaru-ui.tar.gz",
                "url": f"https://api.github.test/assets/archive-{tag}",
                "browser_download_url": f"https://github.test/download/{tag}/kitaru-ui.tar.gz",
            },
            {
                "name": "kitaru-ui.tar.gz.sha256",
                "url": f"https://api.github.test/assets/checksum-{tag}",
                "browser_download_url": f"https://github.test/download/{tag}/kitaru-ui.tar.gz.sha256",
            },
        ],
    }
    sys.stdout.write(json.dumps(release))
    raise SystemExit(0)

if dest and "/assets/archive-" in url:
    shutil.copyfile(os.environ["FAKE_ARCHIVE"], dest)
    raise SystemExit(0)

if dest and "/assets/checksum-" in url:
    shutil.copyfile(os.environ["FAKE_CHECKSUM"], dest)
    raise SystemExit(0)

print(f"fake curl: unexpected URL {url}", file=sys.stderr)
raise SystemExit(3)
""",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    return bin_dir


def _run_download_script(
    tmp_path: Path,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run download-ui.sh in an isolated fake repository."""
    archive, checksum, _digest = _make_ui_archive(tmp_path)
    fake_bin = _write_fake_curl(tmp_path)
    workdir = tmp_path / "repo"
    (workdir / "src" / "kitaru" / "_ui").mkdir(parents=True)
    log_path = tmp_path / "curl.log"

    releases = [
        {"tag_name": "kitaru-ui-v0.9.0", "draft": False, "prerelease": False},
        {"tag_name": "kitaru-ui-v0.10.0", "draft": False, "prerelease": False},
        {"tag_name": "kitaru-ui-v0.11.0", "draft": False, "prerelease": True},
        {"tag_name": "other-product-v9.0.0", "draft": False, "prerelease": False},
    ]

    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "PYTHON_BIN": sys.executable,
        "FAKE_ARCHIVE": str(archive),
        "FAKE_CHECKSUM": str(checksum),
        "FAKE_CURL_LOG": str(log_path),
        "FAKE_RELEASES_JSON": json.dumps(releases),
    }
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        ["bash", str(_DOWNLOAD_SCRIPT)],
        cwd=workdir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_justfile_exposes_local_ui_bundle_helpers() -> None:
    """Local helper recipes should use the prepared bundle via KITARU_UI_DIST_PATH."""
    justfile = _JUSTFILE.read_text()

    for recipe_name in ("ui-bundle", "ui-bundle-prerelease", "ui-login", "ui-smoke"):
        assert f"\n{recipe_name}:" in justfile
    assert "KITARU_UI_INSTALL_DIR" in justfile
    assert "KITARU_UI_ALLOW_PRERELEASE=true" in justfile
    assert "KITARU_UI_DIST_PATH" in justfile
    assert "./scripts/smoke-test.sh --keep-server" in justfile


def test_download_ui_defaults_to_highest_stable_kitaru_ui_release(
    tmp_path: Path,
) -> None:
    """Default selection ignores prereleases and non-Kitaru releases."""
    result = _run_download_script(
        tmp_path,
        extra_env={"KITARU_UI_RELEASE_TOKEN": "secret-token"},
    )

    assert result.returncode == 0, result.stderr
    workdir = tmp_path / "repo"
    manifest = json.loads(
        (workdir / "src" / "kitaru" / "_ui" / "bundle_manifest.json").read_text()
    )
    assert manifest["repo"] == "zenml-io/zenml-frontend-monorepo"
    assert manifest["tag"] == "kitaru-ui-v0.10.0"
    assert manifest["ui_version"] == "kitaru-ui-v0.10.0"
    assert manifest["asset_source_url"].endswith("archive-kitaru-ui-v0.10.0")
    assert (workdir / "src" / "kitaru" / "_ui" / "dist" / "index.html").is_file()

    curl_log = (tmp_path / "curl.log").read_text()
    assert "Authorization: Bearer secret-token" in curl_log


def test_download_ui_rejects_bare_v_tag_before_downloading(tmp_path: Path) -> None:
    """Old bare v* tags should fail clearly under the monorepo policy."""
    result = _run_download_script(tmp_path, extra_env={"TAG": "v0.10.0"})

    assert result.returncode != 0
    assert "kitaru-ui-v<semver>" in result.stderr
    assert not (tmp_path / "repo" / "src" / "kitaru" / "_ui" / "dist").exists()


def test_download_ui_rejects_prerelease_without_opt_in(tmp_path: Path) -> None:
    """Explicit prerelease tags require KITARU_UI_ALLOW_PRERELEASE=true."""
    result = _run_download_script(
        tmp_path,
        extra_env={
            "TAG": "kitaru-ui-v0.11.0-rc.1",
            "FAKE_PRERELEASE_TAGS": "kitaru-ui-v0.11.0-rc.1",
        },
    )

    assert result.returncode != 0
    assert "KITARU_UI_ALLOW_PRERELEASE=true" in result.stderr
    assert not (tmp_path / "repo" / "src" / "kitaru" / "_ui" / "dist").exists()


def test_download_ui_allows_prerelease_with_explicit_opt_in(tmp_path: Path) -> None:
    """Local/smoke lanes can opt into a prerelease tag explicitly."""
    result = _run_download_script(
        tmp_path,
        extra_env={
            "TAG": "kitaru-ui-v0.11.0-rc.1",
            "KITARU_UI_ALLOW_PRERELEASE": "true",
            "FAKE_PRERELEASE_TAGS": "kitaru-ui-v0.11.0-rc.1",
        },
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads(
        (
            tmp_path / "repo" / "src" / "kitaru" / "_ui" / "bundle_manifest.json"
        ).read_text()
    )
    assert manifest["tag"] == "kitaru-ui-v0.11.0-rc.1"
    assert manifest["checksum"] == manifest["bundle_sha256"]
