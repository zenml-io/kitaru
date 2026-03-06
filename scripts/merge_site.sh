#!/usr/bin/env bash
# Merge the docs static export into the Astro site build.
#
# After running:
#   1. docs build  →  docs/out/
#   2. site build  →  site/dist/
#
# This script copies docs/out/* into site/dist/docs/ so the unified
# Worker deployment serves docs at /docs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS_OUT="$REPO_ROOT/docs/out"
SITE_DIST="$REPO_ROOT/site/dist"
MERGE_TARGET="$SITE_DIST/docs"

if [ ! -d "$DOCS_OUT" ]; then
  echo "ERROR: docs/out/ does not exist. Run 'just docs-build' first." >&2
  exit 1
fi

if [ ! -d "$SITE_DIST" ]; then
  echo "ERROR: site/dist/ does not exist. Run 'just site-build-only' first." >&2
  exit 1
fi

# Clear any stale docs from a previous merge
rm -rf "$MERGE_TARGET"
mkdir -p "$MERGE_TARGET"

# Copy docs export contents (not the directory itself) into site/dist/docs/
cp -r "$DOCS_OUT"/* "$MERGE_TARGET"/

echo "Merged docs/out/ into site/dist/docs/"
echo "  $(find "$MERGE_TARGET" -type f | wc -l | tr -d ' ') files"
