"""Render the requirements-triage Replay Lab JSON report as static HTML."""

from __future__ import annotations

import argparse
from pathlib import Path

try:  # Package import path used by tests and repo-root execution.
    from ..verdict_renderer import render_html_report
except ImportError:  # Direct script path used by example commands.
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from verdict_renderer import render_html_report  # type: ignore[no-redef]

DEFAULT_REPORT_JSON = (
    Path(__file__).parent / "reports" / "requirements-triage-sample.json"
)
DEFAULT_HTML = Path(__file__).parent / "reports" / "requirements-triage-sample.html"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json-path",
        type=Path,
        default=DEFAULT_REPORT_JSON,
        help="Replay Lab JSON report path.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_HTML,
        help="Static HTML output path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Render the report and print the generated path."""
    args = parse_args(argv)
    output_path = render_html_report(args.json_path, args.output_path)
    print(f"Wrote HTML report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
