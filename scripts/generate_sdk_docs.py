"""Extract the Kitaru Python SDK public API to JSON for docs generation.

Uses griffe (via fumapy) to introspect the kitaru package and produces a
filtered JSON file containing only the public API surface. The JSON is
consumed by the Node-side conversion script (docs/scripts/convert-sdk-docs.mjs)
to produce MDX pages for FumaDocs.

The two-script split exists because griffe is Python-only while the MDX
conversion uses fumadocs-python's Node API. In CI, this script runs after
pnpm install (so fumapy can be pip-installed from the npm package).

Requires: fumapy (pip install ./docs/node_modules/fumadocs-python)

Output: docs/.generated/sdk-api.json (gitignored intermediate artifact)

Usage:
    uv run python scripts/generate_sdk_docs.py
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

try:
    import griffe
    from fumapy.mksource import CustomEncoder, parse_module
    from griffe_typingdoc import TypingDocExtension

    _HAS_GRIFFE = True
except ImportError:
    _HAS_GRIFFE = False

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / ".generated"
OUTPUT_FILE = OUTPUT_DIR / "sdk-api.json"

# Modules to include in the SDK reference. Only explicitly listed modules
# become published reference pages. This prevents CLI internals, empty
# scaffolding, and private modules from leaking into the docs.
PUBLIC_MODULES = ["kitaru"]

# Submodules to exclude from the reference even if they appear in a
# public module's tree. These have their own docs or are not public.
EXCLUDED_SUBMODULES = {"cli", "adapters", "runtime"}


def _is_private(name: str) -> bool:
    """Check if a module/symbol name is private by convention."""
    return name.rsplit(".", 1)[-1].startswith("_")


def _is_documented_member(name: str) -> bool:
    """Return whether a member should appear in generated docs."""
    return name == "__init__" or not _is_private(name)


def _exported_names(data: dict) -> set[str] | None:
    """Return ``__all__`` names when the module defines them."""
    for attr in data.get("attributes", []):
        if attr.get("name") != "__all__":
            continue
        raw_value = attr.get("value")
        if not isinstance(raw_value, str):
            return None
        try:
            parsed = ast.literal_eval(raw_value)
        except (SyntaxError, ValueError):
            return None
        if isinstance(parsed, list) and all(isinstance(name, str) for name in parsed):
            return set(parsed)
        return None
    return None


def _filter_attributes(
    attributes: list[dict],
    *,
    exported_names: set[str] | None = None,
) -> list[dict]:
    """Filter a list of serialized attributes to public ones only."""
    filtered: list[dict] = []
    for attr in attributes:
        name = attr.get("name", "")
        if name == "__all__":
            continue
        if exported_names is not None:
            if name not in exported_names:
                continue
        elif _is_private(name):
            continue
        filtered.append(attr)
    return filtered


def _filter_class(data: dict) -> dict:
    """Filter private methods and attributes from one serialized class."""
    filtered = dict(data)
    filtered["functions"] = {
        name: func
        for name, func in data.get("functions", {}).items()
        if _is_documented_member(name)
    }
    filtered["classes"] = {
        name: _filter_class(cls)
        for name, cls in data.get("classes", {}).items()
        if _is_documented_member(name)
    }
    filtered["attributes"] = _filter_attributes(data.get("attributes", []))
    filtered["inherited_members"] = {
        name: member
        for name, member in data.get("inherited_members", {}).items()
        if _is_documented_member(name)
    }
    return filtered


def _filter_module(data: dict, *, is_root: bool = False) -> dict:
    """Filter a module dict to remove excluded and private submodules.

    For the root module, also promotes re-exported symbols from private
    submodules to the top level (matching __all__ behavior).
    """
    filtered = dict(data)
    exported_names = _exported_names(data) if is_root else None

    # Collect symbols from private submodules before removing them
    promoted_classes: dict = {}
    promoted_functions: dict = {}

    for name, submod in list(data.get("modules", {}).items()):
        if name in EXCLUDED_SUBMODULES:
            continue
        if _is_private(name) and is_root:
            # Promote public symbols from private modules to root
            promoted_classes.update(
                {
                    symbol_name: _filter_class(symbol)
                    for symbol_name, symbol in submod.get("classes", {}).items()
                    if exported_names is not None
                    and symbol_name in exported_names
                    and _is_documented_member(symbol_name)
                }
            )
            promoted_functions.update(
                {
                    symbol_name: symbol
                    for symbol_name, symbol in submod.get("functions", {}).items()
                    if exported_names is not None
                    and symbol_name in exported_names
                    and _is_documented_member(symbol_name)
                }
            )

    # Remove excluded and private submodules
    filtered["modules"] = {
        name: _filter_module(submod)
        for name, submod in data.get("modules", {}).items()
        if name not in EXCLUDED_SUBMODULES and not _is_private(name)
    }

    filtered["classes"] = {
        name: _filter_class(cls)
        for name, cls in data.get("classes", {}).items()
        if _is_documented_member(name)
        and (exported_names is None or name in exported_names)
    }
    filtered["functions"] = {
        name: func
        for name, func in data.get("functions", {}).items()
        if _is_documented_member(name)
        and (exported_names is None or name in exported_names)
    }

    # Merge promoted symbols into root
    if is_root:
        existing_classes = dict(filtered["classes"])
        existing_classes.update(promoted_classes)
        filtered["classes"] = existing_classes

        existing_functions = dict(filtered["functions"])
        existing_functions.update(promoted_functions)
        filtered["functions"] = existing_functions

    filtered["attributes"] = _filter_attributes(
        data.get("attributes", []),
        exported_names=exported_names,
    )

    return filtered


def extract_api(module_name: str) -> dict:
    """Extract the public API of a Python module using griffe."""
    if not _HAS_GRIFFE:
        msg = (
            "Missing dependency: fumapy (and griffe).\n"
            "  Install: uv pip install ./docs/node_modules/fumadocs-python\n"
            "  (requires 'pnpm install' in docs/ first)"
        )
        raise ImportError(msg)
    extensions = griffe.load_extensions(TypingDocExtension)
    loaded = griffe.load(
        module_name,
        docstring_parser="google",
        store_source=True,
        extensions=extensions,
    )
    if not isinstance(loaded, griffe.Object):
        msg = f"Expected griffe.Object, got {type(loaded).__name__}"
        raise TypeError(msg)

    parsed = parse_module(loaded)

    # Round-trip through JSON to get plain dicts (CustomEncoder handles
    # griffe Expr objects, Path objects, etc.)
    raw = json.loads(json.dumps(parsed, cls=CustomEncoder))
    return raw


def main() -> int:
    """Extract and filter the Kitaru SDK API to JSON."""
    if not _HAS_GRIFFE:
        print("ERROR: Missing dependency: fumapy (and griffe).")
        print("  Install: uv pip install ./docs/node_modules/fumadocs-python")
        print("  (requires 'pnpm install' in docs/ first)")
        return 1

    for module_name in PUBLIC_MODULES:
        try:
            __import__(module_name)
        except ImportError:
            print(f"ERROR: Cannot import '{module_name}'. Is it installed?")
            return 1

    print("Extracting SDK API...")
    raw = extract_api("kitaru")
    filtered = _filter_module(raw, is_root=True)

    # Validate: if we ended up with nothing meaningful, fail loudly
    has_content = (
        filtered.get("classes") or filtered.get("functions") or filtered.get("modules")
    )
    if not has_content:
        print("WARNING: No public API surface found. Skipping SDK docs generation.")
        print("  This is expected if the SDK has no public symbols yet.")
        return 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(filtered, indent=2) + "\n")

    rel = OUTPUT_FILE.relative_to(REPO_ROOT)
    n_classes = len(filtered.get("classes", {}))
    n_functions = len(filtered.get("functions", {}))
    n_modules = len(filtered.get("modules", {}))
    print(
        f"Extracted {n_classes} classes, {n_functions} functions, {n_modules} modules"
    )
    print(f"Wrote {rel}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
