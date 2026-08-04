"""Launch the CLI-first document processing example."""

import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

EXAMPLE_DIR = Path(__file__).parent
PREPARED_TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"
RUNNER_PATH = EXAMPLE_DIR / "run.sh"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the example's CLI command workflow."""
    args = list(sys.argv[1:] if argv is None else argv)
    return subprocess.call(["bash", str(RUNNER_PATH), *args])


if __name__ == "__main__":
    raise SystemExit(main())
