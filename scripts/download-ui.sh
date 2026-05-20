#!/usr/bin/env bash
# Download and bundle Kitaru UI assets for wheel inclusion or local testing.
#
# Fetches kitaru-ui.tar.gz from zenml-io/zenml-frontend-monorepo GitHub
# releases, verifies its SHA256 checksum, extracts it into a Kitaru UI package
# directory, and writes a bundle_manifest.json for runtime identity checks.
#
# Environment variables:
#   TAG                         — explicit release tag (kitaru-ui-v<semver>)
#   KITARU_UI_TAG               — alias for TAG (used by release workflows)
#   KITARU_UI_ALLOW_PRERELEASE  — set to "true" to allow prerelease tags
#   KITARU_UI_RELEASE_TOKEN     — GitHub token with Contents: read access
#   KITARU_UI_INSTALL_DIR       — destination parent dir (default: src/kitaru/_ui)
#   VERIFY_CHECKSUM             — set to "false" to skip checksum verification
#   PYTHON_BIN                  — Python executable for JSON/semver parsing
#
# Usage:
#   bash scripts/download-ui.sh
#   TAG=kitaru-ui-v0.2.0 bash scripts/download-ui.sh
#   KITARU_UI_ALLOW_PRERELEASE=true TAG=kitaru-ui-v0.3.0-rc.1 bash scripts/download-ui.sh

set -euo pipefail

TMP_DIR=""
cleanup() { [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"; }
trap cleanup EXIT

UI_RELEASE_REPO="${KITARU_UI_RELEASE_REPO:-zenml-io/zenml-frontend-monorepo}"
GITHUB_API_URL="${GITHUB_API_URL:-https://api.github.com}"
ARCHIVE_NAME="kitaru-ui.tar.gz"
INSTALL_DIR="${KITARU_UI_INSTALL_DIR:-./src/kitaru/_ui}"
DIST_DIR="$INSTALL_DIR/dist"
MANIFEST_FILE="$INSTALL_DIR/bundle_manifest.json"
VERIFY_CHECKSUM="${VERIFY_CHECKSUM:-true}"
ALLOW_PRERELEASE="${KITARU_UI_ALLOW_PRERELEASE:-false}"
TAG_PATTERN='^kitaru-ui-v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?(\+[0-9A-Za-z][0-9A-Za-z.-]*)?$'

find_python() {
    if [ -n "${PYTHON_BIN:-}" ]; then
        echo "$PYTHON_BIN"
        return
    fi
    if command -v python3 &>/dev/null; then
        echo "python3"
        return
    fi
    if command -v python &>/dev/null; then
        echo "python"
        return
    fi
    echo "Error: python3 or python required" >&2
    exit 1
}

PYTHON_BIN="$(find_python)"

# --- GitHub helpers ---

github_headers() {
    printf '%s\0%s\0' \
        -H 'Accept: application/vnd.github+json' \
        -H 'X-GitHub-Api-Version: 2022-11-28'
    if [ -n "${KITARU_UI_RELEASE_TOKEN:-}" ]; then
        printf '%s\0%s\0' -H "Authorization: Bearer ${KITARU_UI_RELEASE_TOKEN}"
    fi
}

github_api() {
    local url="$1"
    local -a headers=()
    while IFS= read -r -d '' item; do
        headers+=("$item")
    done < <(github_headers)
    curl -fSsL "${headers[@]}" "$url"
}

download_github_asset() {
    local url="$1" dest="$2"
    local -a headers=(
        -H 'Accept: application/octet-stream'
        -H 'X-GitHub-Api-Version: 2022-11-28'
    )
    if [ -n "${KITARU_UI_RELEASE_TOKEN:-}" ]; then
        headers+=(-H "Authorization: Bearer ${KITARU_UI_RELEASE_TOKEN}")
    fi
    echo "Downloading $url"
    curl -fSsL "${headers[@]}" "$url" -o "$dest"
}

# --- Tag and release resolution ---

validate_tag_shape() {
    local tag="$1"
    if [[ ! "$tag" =~ $TAG_PATTERN ]]; then
        echo "Error: Kitaru UI release tags must use kitaru-ui-v<semver> shape." >&2
        echo "Received: $tag" >&2
        echo "Example: kitaru-ui-v0.2.0" >&2
        exit 1
    fi
}

select_latest_stable_tag() {
    local releases_file="$1"
    "$PYTHON_BIN" - "$releases_file" <<'PY'
import json
import re
import sys

releases = json.loads(open(sys.argv[1], encoding="utf-8").read())
pattern = re.compile(r"^kitaru-ui-v(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z][0-9A-Za-z.-]*)?$")
valid: list[tuple[tuple[int, int, int], str]] = []
for release in releases:
    tag = str(release.get("tag_name", ""))
    match = pattern.match(tag)
    if not match:
        continue
    if release.get("draft") or release.get("prerelease"):
        continue
    valid.append(((int(match.group(1)), int(match.group(2)), int(match.group(3))), tag))
if not valid:
    print(
        "Error: no stable kitaru-ui-v<semver> releases found in zenml-frontend-monorepo.",
        file=sys.stderr,
    )
    raise SystemExit(1)
print(max(valid)[1])
PY
}

resolve_tag() {
    # Prefer explicit TAG, then KITARU_UI_TAG, then resolve highest stable release.
    if [ -n "${TAG:-}" ]; then
        validate_tag_shape "$TAG"
        return
    fi
    if [ -n "${KITARU_UI_TAG:-}" ]; then
        TAG="$KITARU_UI_TAG"
        validate_tag_shape "$TAG"
        return
    fi

    echo "No TAG or KITARU_UI_TAG set; resolving highest stable Kitaru UI release..."
    local releases_file="$TMP_DIR/releases.json"
    github_api "$GITHUB_API_URL/repos/$UI_RELEASE_REPO/releases?per_page=100" > "$releases_file"
    TAG=$(select_latest_stable_tag "$releases_file")
    echo "Resolved stable tag: $TAG"
}

load_release_metadata() {
    local release_file="$1" metadata_file="$2"
    "$PYTHON_BIN" - "$release_file" "$TAG" "$ALLOW_PRERELEASE" "$ARCHIVE_NAME" <<'PY' > "$metadata_file"
import json
import shlex
import sys

release_file, tag, allow_prerelease, archive_name = sys.argv[1:]
release = json.loads(open(release_file, encoding="utf-8").read())

if release.get("draft"):
    print(f"Error: release {tag} is a draft and cannot be bundled.", file=sys.stderr)
    raise SystemExit(1)

is_prerelease = bool(release.get("prerelease"))
if is_prerelease and allow_prerelease != "true":
    print(
        f"Error: release {tag} is a prerelease. Set "
        "KITARU_UI_ALLOW_PRERELEASE=true for local/smoke testing.",
        file=sys.stderr,
    )
    raise SystemExit(1)

assets = {asset.get("name"): asset for asset in release.get("assets", [])}
archive = assets.get(archive_name)
checksum = assets.get(f"{archive_name}.sha256")
missing = [name for name, asset in ((archive_name, archive), (f"{archive_name}.sha256", checksum)) if not asset]
if missing:
    print(f"Error: release {tag} is missing required assets: {', '.join(missing)}", file=sys.stderr)
    raise SystemExit(1)

archive_url = archive.get("url") or archive.get("browser_download_url")
checksum_url = checksum.get("url") or checksum.get("browser_download_url")
if not archive_url or not checksum_url:
    print(f"Error: release {tag} assets do not expose download URLs.", file=sys.stderr)
    raise SystemExit(1)

print(f"ASSET_SOURCE_URL={shlex.quote(str(archive_url))}")
print(f"CHECKSUM_SOURCE_URL={shlex.quote(str(checksum_url))}")
print(f"RELEASE_PRERELEASE={shlex.quote(str(is_prerelease).lower())}")
PY
}

# --- Checksum verification ---

compute_sha256() {
    local file="$1"
    if command -v sha256sum &>/dev/null; then
        sha256sum "$file" | cut -d' ' -f1
    elif command -v shasum &>/dev/null; then
        shasum -a 256 "$file" | cut -d' ' -f1
    else
        echo "Error: sha256sum or shasum required" >&2
        exit 1
    fi
}

verify_checksum() {
    local archive="$1" checksum_file="$2"
    if [ "$VERIFY_CHECKSUM" = "false" ]; then
        echo "Skipping checksum verification" >&2
        return
    fi
    echo "Verifying checksum..." >&2
    local actual expected
    actual=$(compute_sha256 "$archive")
    expected=$(cut -d' ' -f1 < "$checksum_file")
    if [ "$actual" != "$expected" ]; then
        echo "Error: SHA256 mismatch" >&2
        echo "  expected: $expected" >&2
        echo "  actual:   $actual" >&2
        exit 1
    fi
    echo "Checksum OK: $actual" >&2
}

# --- Manifest generation ---

write_manifest() {
    local sha256="$1" tag="$2" asset_source_url="$3" checksum_source_url="$4" dest="$5"
    mkdir -p "$(dirname "$dest")"
    "$PYTHON_BIN" - "$UI_RELEASE_REPO" "$tag" "$asset_source_url" "$checksum_source_url" "$sha256" "$dest" <<'PY'
import json
import sys
from pathlib import Path

repo, tag, asset_source_url, checksum_source_url, sha256, dest = sys.argv[1:]
manifest = {
    "schema_version": 1,
    "ui_version": tag,
    "repo": repo,
    "tag": tag,
    "bundle_sha256": sha256,
    "checksum": sha256,
    "source": asset_source_url,
    "asset_source_url": asset_source_url,
    "checksum_source_url": checksum_source_url,
}
Path(dest).write_text(json.dumps(manifest, indent=4) + "\n", encoding="utf-8")
PY
    echo "Wrote manifest: $dest"
}

# --- Main ---

main() {
    TMP_DIR=$(mktemp -d -t kitaru-ui-XXXXXX)
    resolve_tag

    local release_file="$TMP_DIR/release.json"
    local metadata_file="$TMP_DIR/release.env"
    github_api "$GITHUB_API_URL/repos/$UI_RELEASE_REPO/releases/tags/$TAG" > "$release_file"
    load_release_metadata "$release_file" "$metadata_file"
    # shellcheck source=/dev/null
    . "$metadata_file"

    local archive="$TMP_DIR/$ARCHIVE_NAME"
    local checksum_file="$TMP_DIR/$ARCHIVE_NAME.sha256"

    download_github_asset "$ASSET_SOURCE_URL" "$archive"
    download_github_asset "$CHECKSUM_SOURCE_URL" "$checksum_file"
    verify_checksum "$archive" "$checksum_file"

    local bundle_sha256
    bundle_sha256=$(compute_sha256 "$archive")

    # Clean and extract.
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR"
    tar xzf "$archive" -C "$DIST_DIR"

    # Verify sentinel.
    if [ ! -f "$DIST_DIR/index.html" ]; then
        echo "Error: index.html not found in extracted archive" >&2
        echo "Archive contents:" >&2
        ls -la "$DIST_DIR" >&2
        exit 1
    fi

    # Write bundle manifest.
    write_manifest "$bundle_sha256" "$TAG" "$ASSET_SOURCE_URL" "$CHECKSUM_SOURCE_URL" "$MANIFEST_FILE"

    echo ""
    echo "Kitaru UI $TAG bundled successfully into $DIST_DIR"
    echo "Files: $(find "$DIST_DIR" -type f | wc -l | tr -d ' ')"
}

main "$@"
