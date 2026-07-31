#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <container>" >&2
    exit 2
fi

container="$1"

docker exec "$container" python -c '
from pathlib import Path
import kitaru

path = Path(kitaru.__file__).parent / "_ui" / "dist" / "index.html"
assert path.is_file(), f"missing: {path}"
print("OK package UI:", path)
'
