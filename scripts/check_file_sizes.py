"""Keep hand-written code files under a line limit, with a shrinking baseline.

Files that were already over the limit when the check was introduced are listed
in the baseline with their line counts. A listed file may shrink but never grow,
and every other file must stay at or under the limit. Run with ``--update`` after
shrinking a listed file to lower its recorded count; the update never raises one.
"""

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

LIMIT = 1000
BASELINE_PATH = Path("scripts/file_size_baseline.json")
CODE_SUFFIXES = frozenset(
    {".py", ".ts", ".tsx", ".mts", ".cts", ".js", ".mjs", ".cjs", ".sh"}
)
# Generated files are rewritten by tools, and applied migrations are frozen
# history, so splitting either would only fight the process that owns them.
EXCLUDED_PREFIXES = (
    "packages/core/src/generated/",
    "src/kitaru/server/database/migrations/versions/",
)


@dataclass(frozen=True, order=True)
class Violation:
    """A file whose line count breaks the limit or its baseline entry."""

    path: str
    message: str


def is_checked(path: str) -> bool:
    """Return whether a repository-relative path is hand-written code."""
    return Path(path).suffix in CODE_SUFFIXES and not path.startswith(EXCLUDED_PREFIXES)


def find_violations(
    counts: dict[str, int], baseline: dict[str, int], limit: int = LIMIT
) -> list[Violation]:
    """Compare current line counts against the limit and the baseline."""
    findings: list[Violation] = []
    for path, lines in counts.items():
        recorded = baseline.get(path)
        if recorded is None and lines > limit:
            findings.append(
                Violation(
                    path,
                    f"{lines} lines exceeds the {limit}-line limit; "
                    "split it into smaller modules.",
                )
            )
        elif recorded is not None and lines > recorded:
            findings.append(
                Violation(
                    path,
                    f"grew from {recorded} to {lines} lines; this file is over "
                    f"the {limit}-line limit, so move new code to another module.",
                )
            )
    for path, recorded in baseline.items():
        lines = counts.get(path)
        if lines is None or lines < recorded:
            current = "is gone" if lines is None else f"shrank to {lines} lines"
            findings.append(
                Violation(
                    path,
                    f"{current} but the baseline records {recorded}; run "
                    "`uv run python scripts/check_file_sizes.py --update`.",
                )
            )
    return sorted(findings)


def tighten_baseline(
    counts: dict[str, int], baseline: dict[str, int], limit: int = LIMIT
) -> dict[str, int]:
    """Lower shrunk entries and drop ones that are gone or back under the limit."""
    return {
        path: min(recorded, counts[path])
        for path, recorded in sorted(baseline.items())
        if path in counts and counts[path] > limit
    }


def _list_files(root: Path) -> list[str]:
    # Include untracked files so a new oversized file fails before it is staged.
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return [path for path in result.stdout.splitlines() if is_checked(path)]


def _count_lines(root: Path, paths: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in paths:
        file = root / path
        # git still lists a deleted file until the deletion is staged.
        if file.is_file():
            counts[path] = len(file.read_text(encoding="utf-8").splitlines())
    return counts


def main(argv: list[str] | None = None) -> int:
    """Check code file sizes under the selected root, or tighten the baseline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Lower baseline entries for files that shrank; never raise them.",
    )
    args = parser.parse_args(argv)
    baseline_file = args.root / BASELINE_PATH
    try:
        baseline: dict[str, int] = json.loads(baseline_file.read_text("utf-8"))
        counts = _count_lines(args.root, _list_files(args.root))
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"{BASELINE_PATH}: checker failed: {error}", file=sys.stderr)
        return 2
    if args.update:
        baseline = tighten_baseline(counts, baseline)
        baseline_file.write_text(json.dumps(baseline, indent=2) + "\n", "utf-8")
    findings = find_violations(counts, baseline)
    for finding in findings:
        print(f"{finding.path}: {finding.message}")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
