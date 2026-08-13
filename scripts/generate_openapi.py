#!/usr/bin/env python3
"""Generate OpenAPI specification from the FastAPI app."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from kitaru.server.api.app import create_app  # noqa: E402
from kitaru.server.api.config import APISettings  # noqa: E402

DEFAULT_OUTPUT = ROOT / "openapi" / "openapi.json"


def main() -> None:
    """Write OpenAPI JSON to openapi/openapi.json (or argv path)."""
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT
    settings = APISettings(
        DB_HOST="localhost", SECRET_ENCRYPTION_KEY="unused", JWT_SIGNING_KEY="unused"
    )
    app = create_app(settings)
    schema = app.openapi()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(schema, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
