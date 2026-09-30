"""Check deterministic DB syntax rules; transaction and schema semantics need review.

Repositories leave request commits to the REST boundary. One-row queries expose
unexpected multiplicity. ORM constraints use names shared with error translation
and migrations; annotations determine nullability.

ORM checks recognize direct SQLAlchemy imports and import aliases. Assignment
aliases and shadowed imports need review; this is a syntax check, not type inference.
"""

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path

DB_PATH = Path("src/kitaru/server/adapters/db")


@dataclass(frozen=True, order=True)
class Violation:
    """A repository-relative diagnostic in deterministic display order."""

    path: str
    line: int
    code: str
    message: str


def _get_name(node: ast.expr, aliases: dict[str, str]) -> str:
    if isinstance(node, ast.Name):
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        return f"{_get_name(node.value, aliases)}.{node.attr}"
    return ""


def check_source(source: str, path: Path) -> list[Violation]:
    """Find DB violations in Python source at a repository-relative path."""
    repository = path.is_relative_to(DB_PATH / "repositories")
    orm = path.is_relative_to(DB_PATH / "orm")
    if not repository and not orm:
        return []
    tree = ast.parse(source, filename=path.as_posix())
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = (
                    alias.name if alias.asname else alias.name.split(".")[0]
                )
    findings: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        rules: list[tuple[str, str]] = []
        if repository and isinstance(node.func, ast.Attribute):
            if node.func.attr == "commit":
                rules.append(
                    ("KIT001", "Use flush(); commit at the transaction boundary.")
                )
            elif node.func.attr == "first":
                rules.append(
                    (
                        "KIT002",
                        "Use one_or_none() or scalar_one_or_none(); "
                        "intentional top-one queries require LIMIT 1.",
                    )
                )
        if orm and _get_name(node.func, aliases) in {
            "sqlalchemy.orm.mapped_column",
            "sqlalchemy.orm._orm_constructors.mapped_column",
        }:
            if any(
                keyword.arg in {"nullable", "unique", "index"}
                for keyword in node.keywords
            ):
                rules.append(
                    (
                        "KIT003",
                        "Infer nullability from Mapped; "
                        "use named UniqueConstraint or Index declarations.",
                    )
                )
            if any(
                isinstance(child, ast.Call)
                and _get_name(child.func, aliases)
                in {"sqlalchemy.ForeignKey", "sqlalchemy.schema.ForeignKey"}
                for child in ast.walk(node)
            ):
                rules.append(
                    ("KIT004", "Use a named ForeignKeyConstraint in __table_args__.")
                )
        for code, message in rules:
            findings.append(Violation(path.as_posix(), node.lineno, code, message))
    return sorted(findings)


def main(argv: list[str] | None = None) -> int:
    """Check DB Python files under the selected root and print all diagnostics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args(argv)
    findings: list[Violation] = []
    checked_files = 0
    if not (args.root / DB_PATH).is_dir():
        print(f"{DB_PATH}: checker failed: DB directory is missing", file=sys.stderr)
        return 2
    for directory in ("repositories", "orm"):
        if not (args.root / DB_PATH / directory).is_dir():
            print(
                f"{DB_PATH / directory}: checker failed: directory is missing",
                file=sys.stderr,
            )
            return 2
        for path in sorted((args.root / DB_PATH / directory).rglob("*.py")):
            checked_files += 1
            relative = path.relative_to(args.root)
            try:
                findings.extend(
                    check_source(path.read_text(encoding="utf-8"), relative)
                )
            except (SyntaxError, UnicodeError, OSError) as error:
                print(f"{relative}: checker failed: {error}", file=sys.stderr)
                return 2
    if not checked_files:
        print(f"{DB_PATH}: checker failed: no Python files found", file=sys.stderr)
        return 2
    for finding in sorted(findings):
        print(f"{finding.path}:{finding.line}: {finding.code} {finding.message}")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
