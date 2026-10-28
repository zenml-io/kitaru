"""Check deterministic syntax rules (codes KIT001 and up) in the Kitaru package.

Transaction and schema semantics need review; this is a syntax check, not type
inference. ORM checks recognize direct SQLAlchemy imports and import aliases, but
assignment aliases and shadowed imports need review.
"""

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path

PACKAGE_PATH = Path("src/kitaru")
DB_PATH = PACKAGE_PATH / "server/adapters/db"
REST_PATH = PACKAGE_PATH / "server/adapters/rest"
INNER_LAYER_PATHS = (
    PACKAGE_PATH / "server/domain",
    PACKAGE_PATH / "server/application",
)
DTO_SUFFIXES = ("Request", "Response", "ListParams")
MAPPED_COLUMN_NAMES = {
    "sqlalchemy.orm.mapped_column",
    "sqlalchemy.orm._orm_constructors.mapped_column",
}
FOREIGN_KEY_NAMES = {"sqlalchemy.ForeignKey", "sqlalchemy.schema.ForeignKey"}
MESSAGES = {
    "KIT001": "Use flush(); commit at the transaction boundary.",
    "KIT002": "Use one_or_none() or scalar_one_or_none(); "
    "intentional top-one queries require LIMIT 1.",
    "KIT003": "Infer nullability from Mapped; "
    "use named UniqueConstraint or Index declarations.",
    "KIT004": "Use a named ForeignKeyConstraint in __table_args__.",
    "KIT005": "Return to_domain(); flush already sets client-side ids and timestamps.",
    "KIT006": "TimestampMixin renews updated on every UPDATE.",
    "KIT007": "Construct response DTOs field by field in mapping/.",
    "KIT008": "Take an application command or filter model; "
    "API request, response, and list params DTOs stay in REST.",
    "KIT009": "Check capabilities with a Protocol, ABC, or isinstance.",
}


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


def _get_import_aliases(tree: ast.Module) -> dict[str, str]:
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
    return aliases


def _is_api_models_module(module: str | None) -> bool:
    return module == "kitaru.api_models" or (module or "").startswith(
        "kitaru.api_models."
    )


def _get_assignment_targets(node: ast.AST) -> list[ast.expr]:
    if isinstance(node, ast.Assign):
        return node.targets
    if isinstance(node, ast.AugAssign | ast.AnnAssign):
        return [node.target]
    return []


def check_source(source: str, path: Path) -> list[Violation]:
    """Find violations in Python source at a repository-relative path."""
    if not path.is_relative_to(PACKAGE_PATH):
        return []
    repository = path.is_relative_to(DB_PATH / "repositories")
    orm = path.is_relative_to(DB_PATH / "orm")
    rest = path.is_relative_to(REST_PATH)
    inner_layer = any(path.is_relative_to(layer) for layer in INNER_LAYER_PATHS)
    tree = ast.parse(source, filename=path.as_posix())
    aliases = _get_import_aliases(tree) if orm else {}
    findings: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.stmt | ast.expr):
            continue
        codes: list[str] = []
        if (
            inner_layer
            and isinstance(node, ast.ImportFrom)
            and _is_api_models_module(node.module)
            and any(alias.name.endswith(DTO_SUFFIXES) for alias in node.names)
        ):
            codes.append("KIT008")
        if repository and any(
            isinstance(target, ast.Attribute) and target.attr == "updated"
            for target in _get_assignment_targets(node)
        ):
            codes.append("KIT006")
        if isinstance(node, ast.Call):
            if repository and isinstance(node.func, ast.Attribute):
                codes.extend(
                    code
                    for attr, code in (
                        ("commit", "KIT001"),
                        ("first", "KIT002"),
                        ("refresh", "KIT005"),
                    )
                    if node.func.attr == attr
                )
            if orm and _get_name(node.func, aliases) in MAPPED_COLUMN_NAMES:
                if any(
                    keyword.arg in {"nullable", "unique", "index"}
                    for keyword in node.keywords
                ):
                    codes.append("KIT003")
                if any(
                    isinstance(child, ast.Call)
                    and _get_name(child.func, aliases) in FOREIGN_KEY_NAMES
                    for child in ast.walk(node)
                ):
                    codes.append("KIT004")
            if rest and any(
                keyword.arg == "from_attributes" for keyword in node.keywords
            ):
                codes.append("KIT007")
            if isinstance(node.func, ast.Name) and node.func.id == "hasattr":
                codes.append("KIT009")
        findings.extend(
            Violation(path.as_posix(), node.lineno, code, MESSAGES[code])
            for code in codes
        )
    return sorted(findings)


def main(argv: list[str] | None = None) -> int:
    """Check package Python files under the selected root and print all diagnostics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args(argv)
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
    findings: list[Violation] = []
    paths = sorted((args.root / PACKAGE_PATH).rglob("*.py"))
    if not paths:
        print(f"{PACKAGE_PATH}: checker failed: no Python files found", file=sys.stderr)
        return 2
    for path in paths:
        relative = path.relative_to(args.root)
        try:
            findings.extend(check_source(path.read_text(encoding="utf-8"), relative))
        except (SyntaxError, UnicodeError, OSError) as error:
            print(f"{relative}: checker failed: {error}", file=sys.stderr)
            return 2
    for finding in sorted(findings):
        print(f"{finding.path}:{finding.line}: {finding.code} {finding.message}")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
