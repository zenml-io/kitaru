#!/usr/bin/env bash
# Download and bundle Kitaru UI assets for wheel inclusion.
#
# Fetches kitaru-ui.tar.gz from the kitaru-ui GitHub releases,
# verifies its SHA256 checksum, extracts it into src/kitaru/_ui/dist/,
# and writes a bundle_manifest.json for runtime idempotence checks.
#
# Environment variables:
#   TAG                 — explicit release tag (e.g. v0.2.0); if unset, uses latest
#   KITARU_UI_TAG       — alias for TAG (used by release.yml)
#   VERIFY_CHECKSUM     — set to "false" to skip checksum verification (default: true)
#
# Usage:
#   bash scripts/download-ui.sh          # download latest release
#   TAG=v0.2.0 bash scripts/download-ui.sh  # download specific tag

set -euo pipefail

TMP_DIR=""
cleanup() { [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"; }
trap cleanup EXIT

REPO_URL="https://github.com/zenml-io/kitaru-ui"
ARCHIVE_NAME="kitaru-ui.tar.gz"
INSTALL_DIR="./src/kitaru/_ui"
DIST_DIR="$INSTALL_DIR/dist"
MANIFEST_FILE="$INSTALL_DIR/bundle_manifest.json"
VERIFY_CHECKSUM="${VERIFY_CHECKSUM:-true}"

# --- Tag resolution ---

resolve_tag() {
    # Prefer explicit TAG, then KITARU_UI_TAG, then resolve latest from GitHub.
    if [ -n "${TAG:-}" ]; then
        return
    fi
    if [ -n "${KITARU_UI_TAG:-}" ]; then
        TAG="$KITARU_UI_TAG"
        return
    fi
    echo "No TAG or KITARU_UI_TAG set; resolving latest release..."
    TAG=$(curl -Ls -o /dev/null -w '%{url_effective}' "$REPO_URL/releases/latest" \
        | grep -oE "[^/]+$")
    if [ -z "$TAG" ]; then
        echo "Error: could not resolve latest release tag" >&2
        exit 1
    fi
    echo "Resolved latest tag: $TAG"
}

# --- Download ---

download_file() {
    local url="$1" dest="$2"
    echo "Downloading $url"
    curl -fSsL "$url" -o "$dest"
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
    local sha256="$1" tag="$2" source_url="$3" dest="$4"
    cat > "$dest" <<EOF
{
    "schema_version": 1,
    "ui_version": "$tag",
    "bundle_sha256": "$sha256",
    "source": "$source_url"
}
EOF
    echo "Wrote manifest: $dest"
}

# --- Main ---

main() {
    resolve_tag

    local base_url="$REPO_URL/releases/download/$TAG"
    TMP_DIR=$(mktemp -d -t kitaru-ui-XXXXXX)

    local archive="$TMP_DIR/$ARCHIVE_NAME"
    local checksum_file="$TMP_DIR/$ARCHIVE_NAME.sha256"

    download_file "$base_url/$ARCHIVE_NAME" "$archive"
    download_file "$base_url/$ARCHIVE_NAME.sha256" "$checksum_file"
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
    write_manifest "$bundle_sha256" "$TAG" "$base_url/$ARCHIVE_NAME" "$MANIFEST_FILE"

    echo ""
    echo "Kitaru UI $TAG bundled successfully into $DIST_DIR"
    echo "Files: $(find "$DIST_DIR" -type f | wc -l | tr -d ' ')"
}

main "$@"
