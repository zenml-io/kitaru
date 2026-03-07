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

import json
import sys
from pathlib import Path

try:
    import griffe
    from fumapy.mksource import CustomEncoder, parse_module
    from griffe_typingdoc import TypingDocExtension
except ImportError as e:
    print(f"ERROR: Missing dependency: {e}")
    print("  Install fumapy: uv pip install ./docs/node_modules/fumadocs-python")
    print("  (requires 'pnpm install' in docs/ first)")
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / ".generated"
OUTPUT_FILE = OUTPUT_DIR / "sdk-api.json"

# Modules to include in the SDK reference. Only explicitly listed modules
# become published reference pages. This prevents CLI internals, empty
# scaffolding, and private modules from leaking into the docs.
PUBLIC_MODULES = ["kitaru"]

# Submodules to exclude from the reference even if they appear in a
# public module's tree. These have their own docs or are not public.
EXCLUDED_SUBMODULES = {"cli", "adapters"}


def _is_private(name: str) -> bool:
    """Check if a module/symbol name is private by convention."""
    return name.startswith("_")


def _filter_module(data: dict, *, is_root: bool = False) -> dict:
    """Filter a module dict to remove excluded and private submodules.

    For the root module, also promotes re-exported symbols from private
    submodules to the top level (matching __all__ behavior).
    """
    filtered = dict(data)

    # Collect symbols from private submodules before removing them
    promoted_classes: dict = {}
    promoted_functions: dict = {}

    for name, submod in list(data.get("modules", {}).items()):
        if name in EXCLUDED_SUBMODULES:
            continue
        if _is_private(name) and is_root:
            # Promote public symbols from private modules to root
            promoted_classes.update(submod.get("classes", {}))
            promoted_functions.update(submod.get("functions", {}))

    # Remove excluded and private submodules
    filtered["modules"] = {
        name: _filter_module(submod)
        for name, submod in data.get("modules", {}).items()
        if name not in EXCLUDED_SUBMODULES and not _is_private(name)
    }

    # Merge promoted symbols into root
    if is_root:
        existing_classes = dict(filtered.get("classes", {}))
        existing_classes.update(promoted_classes)
        filtered["classes"] = existing_classes

        existing_functions = dict(filtered.get("functions", {}))
        existing_functions.update(promoted_functions)
        filtered["functions"] = existing_functions

    # Filter __all__ from attributes (implementation detail, not useful in docs)
    filtered["attributes"] = [
        attr for attr in data.get("attributes", []) if attr.get("name") != "__all__"
    ]

    return filtered


def extract_api(module_name: str) -> dict:
    """Extract the public API of a Python module using griffe."""
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
