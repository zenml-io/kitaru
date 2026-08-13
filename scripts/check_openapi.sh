#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE="${ROOT}/openapi/openapi.json"
GENERATED="${ROOT}/openapi/.build/openapi.generated.json"

mkdir -p "${ROOT}/openapi/.build"
python "${ROOT}/scripts/generate_openapi.py" "${GENERATED}"

if ! diff -u "${BASELINE}" "${GENERATED}"; then
  echo "OpenAPI specification is out of date. Run: uv run python scripts/generate_openapi.py"
  exit 1
fi
echo "OpenAPI specification is up to date."
