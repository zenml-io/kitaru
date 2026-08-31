"""Extract the Kitaru Python SDK public API to JSON for docs generation.

Uses griffe (via fumapy) to introspect the kitaru package and produces a
filtered JSON file containing only the allowlisted public API surface. The
JSON is consumed by the Node-side conversion script
(docs/scripts/convert-sdk-docs.mjs) to produce MDX pages for FumaDocs.

The public surface is defined by the ``PUBLIC_API`` allowlist below rather
than by ``__all__`` in the package: the v2 root ``kitaru/__init__.py`` is
intentionally empty, and griffe's serialized output drops import aliases, so
re-exported names must be pulled from the module that defines them.

The two-script split exists because griffe is Python-only while the MDX
conversion uses fumadocs-python's Node API. In CI, this script runs after
pnpm install (so fumapy can be pip-installed from the npm package).

Requires: fumapy (uv pip install ./docs/node_modules/fumadocs-python)

Output: docs/.generated/sdk-api.json (gitignored intermediate artifact)

Usage:
    uv run python scripts/generate_sdk_docs.py
"""

import builtins
import json
import re
from dataclasses import dataclass, field
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

ROOT_MODULE = "kitaru"


@dataclass(frozen=True)
class ModuleSpec:
    """Allowlist entry for one published reference module."""

    # Names defined somewhere inside this module's own subtree. Griffe drops
    # import aliases, so a name listed here is looked up in the module itself
    # and then in its submodules (e.g. KitaruAPIClient lives in
    # kitaru.client.api_client but is published under kitaru.client).
    symbols: frozenset[str] = field(default_factory=frozenset)
    # Names re-exported from elsewhere in the kitaru tree, mapped to the
    # module path that actually defines them.
    reexports: dict[str, str] = field(default_factory=dict)


# Only modules listed here become published reference pages, and only the
# listed symbols appear on them. Everything else in the package (server, cli,
# mcp, worker, adapters, analytics, api_models internals, ...) stays out.
PUBLIC_API: dict[str, ModuleSpec] = {
    "kitaru.client": ModuleSpec(
        symbols=frozenset(
            {
                "KitaruAPIClient",
                "KitaruClient",
                "KitaruSyncClient",
                "ClientConfig",
                "load_config",
                "save_config",
                "get_server_url",
                "set_server_url",
                "TokenAuth",
                "StaticTokenAuth",
                "RenewingTokenAuth",
                "TokenSource",
                "CredentialStoreTokenSource",
                "CredentialStore",
                "ApiToken",
                "ApiType",
                "ServerCredentials",
                "ControlPlaneSession",
                "ControlPlaneLoginError",
                "control_plane_login",
                "device_login",
                "DeviceLoginError",
                "KitaruClientError",
                "APIError",
                "AuthenticationError",
                "AuthorizationError",
                "NotFoundError",
                "ValidationError",
                "ServerError",
                "TokenGrantError",
            }
        ),
    ),
    # kitaru/client/resources/__init__.py re-exports nothing, so every class
    # below is found in its defining submodule (sessions.py, replays.py, ...)
    # and published under the package path users import from.
    "kitaru.client.resources": ModuleSpec(
        symbols=frozenset(
            {
                "SessionsResource",
                "ReplaysResource",
                "AgentsResource",
                "AgentVersionsResource",
                "JobsResource",
                "EvaluatorsResource",
                "EvaluationsResource",
                "CohortsResource",
                "CohortVersionsResource",
                "ExperimentsResource",
                "ExperimentRunsResource",
                "InvestigationsResource",
                "iterate_pages",
            }
        ),
    ),
    "kitaru.task": ModuleSpec(
        symbols=frozenset({"get_task_id", "get_task_inputs"}),
    ),
    # Container entries: kitaru.api_models and kitaru.api_models.v1 define no
    # developer-facing symbols themselves, but the attach step in
    # build_public_api requires every published module's parent to be
    # published too. An empty ModuleSpec flows through the normal pipeline and
    # yields a module with no classes/functions, which the converter renders
    # as an index page linking the children — no special-casing needed.
    "kitaru.api_models": ModuleSpec(),
    "kitaru.api_models.v1": ModuleSpec(),
    "kitaru.api_models.v1.base": ModuleSpec(
        symbols=frozenset({"Page", "ListParams", "CursorParams"}),
    ),
    "kitaru.api_models.v1.session": ModuleSpec(
        symbols=frozenset(
            {
                "SessionCreateRequest",
                "SessionUpdateRequest",
                "SessionEvaluationsRequest",
                "SessionListParams",
                "SessionResponse",
                "SessionDetailResponse",
                "SessionOrigin",
                "SessionStatus",
                "TokenUsage",
            }
        ),
    ),
    "kitaru.api_models.v1.session_node": ModuleSpec(
        symbols=frozenset(
            {
                "SessionNodeCreateRequest",
                "SessionNodeBatchRequest",
                "SessionNodeListParams",
                "SessionNodeResponse",
                "SessionWithNodesResponse",
                "NodeType",
                "NodeStatus",
            }
        ),
    ),
    # EvaluationResult stays published under kitaru.task.evaluator (see its
    # reexports entry); publishing it here too would create two pages for one
    # symbol.
    "kitaru.api_models.v1.evaluation": ModuleSpec(
        symbols=frozenset(
            {
                "EvaluationBatchCreateRequest",
                "EvaluationListParams",
                "EvaluationResponse",
                "EvaluationDataType",
            }
        ),
    ),
    "kitaru.api_models.v1.replay": ModuleSpec(
        symbols=frozenset(
            {
                "BaselineEvaluationMode",
                "ReplayCreateRequest",
                "ReplayListParams",
                "ReplayResponse",
                "ReplayStatus",
                "ToolLookupMatch",
                "ToolLookupRequest",
                "ToolLookupResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.replay_config": ModuleSpec(
        symbols=frozenset(
            {
                "EvaluatorConfig",
                "HistoryConfig",
                "PassthroughConfig",
                "StaticConfig",
                "StaticCase",
                "LLMConfig",
                "ToolPolicy",
                "ReplayOverride",
                "HistoryScope",
                "ToolPolicyOnMiss",
                "StaticMatchMode",
            }
        ),
    ),
    "kitaru.api_models.v1.job": ModuleSpec(
        symbols=frozenset(
            {
                "JobResponse",
                "JobListParams",
                "JobTasksListParams",
                "JobStatus",
                "JobKind",
            }
        ),
    ),
    # ImportFailure / ImportStats / MAX_IMPORT_FAILURES stay published under
    # kitaru.task.importer (see its reexports entry).
    "kitaru.api_models.v1.imports": ModuleSpec(
        symbols=frozenset({"ImportCreateRequest"}),
    ),
    "kitaru.api_models.v1.agent": ModuleSpec(
        symbols=frozenset(
            {
                "AgentCreateRequest",
                "AgentUpdateRequest",
                "AgentListParams",
                "AgentResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.agent_version": ModuleSpec(
        symbols=frozenset(
            {
                "AgentVersionCreateRequest",
                "AgentVersionUpdateRequest",
                "AgentVersionListParams",
                "AgentVersionResponse",
                "RunSpec",
                "AgentCapabilities",
                "ReplayCapabilities",
            }
        ),
    ),
    "kitaru.api_models.v1.cohort": ModuleSpec(
        symbols=frozenset(
            {
                "CohortCreateRequest",
                "CohortUpdateRequest",
                "CohortListParams",
                "CohortResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.cohort_version": ModuleSpec(
        symbols=frozenset(
            {
                "CohortVersionCreateRequest",
                "CohortVersionUpdateRequest",
                "CohortVersionListParams",
                "CohortVersionResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.evaluator": ModuleSpec(
        symbols=frozenset(
            {
                "EvaluatorCreateRequest",
                "EvaluatorUpdateRequest",
                "EvaluatorListParams",
                "EvaluatorResponse",
                "EvaluatorVersionCreateRequest",
                "EvaluatorVersionUpdateRequest",
                "EvaluatorVersionResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.experiment": ModuleSpec(
        symbols=frozenset(
            {
                "ExperimentCreateRequest",
                "ExperimentUpdateRequest",
                "ExperimentListParams",
                "ExperimentResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.experiment_run": ModuleSpec(
        symbols=frozenset(
            {
                "ExperimentRunCreateRequest",
                "ExperimentRunListParams",
                "ExperimentRunJobsListParams",
                "ExperimentRunResponse",
                "ExperimentRunProgress",
                "ExperimentRunStatus",
            }
        ),
    ),
    "kitaru.api_models.v1.investigation": ModuleSpec(
        symbols=frozenset(
            {
                "InvestigationCreateRequest",
                "InvestigationUpdateRequest",
                "InvestigationListParams",
                "InvestigationResponse",
                "InvestigationStatus",
                "InvestigationSessionVerdict",
                "InvestigationSessionInput",
                "InvestigationSessionHighlight",
                "InvestigationSessionQuestion",
                "InvestigationSessionsListParams",
                "InvestigationSessionUpdateRequest",
                "InvestigationSessionResponse",
            }
        ),
    ),
    "kitaru.api_models.v1.annotation": ModuleSpec(
        symbols=frozenset(
            {
                "AnnotationCreateRequest",
                "ManualAnnotationCreateRequest",
                "InvestigationAnswerCreateRequest",
                "AnnotationUpdateRequest",
                "AnnotationListParams",
                "AnnotationResponse",
                "AnnotationSpan",
                "AnnotationSelector",
            }
        ),
    ),
    "kitaru.api_models.v1.filter": ModuleSpec(
        symbols=frozenset(
            {
                "FilterOp",
                "FilterCondition",
                "AndFilter",
                "OrFilter",
                "NotFilter",
                "FilterableListParams",
            }
        ),
    ),
    "kitaru.task.evaluator": ModuleSpec(
        symbols=frozenset(
            {
                "EvaluationError",
                "EvaluatorReturn",
                "SessionView",
                "call_evaluator",
            }
        ),
        reexports={"EvaluationResult": "kitaru.api_models.v1.evaluation"},
    ),
    "kitaru.task.importer": ModuleSpec(
        symbols=frozenset(
            {
                "ImportedSession",
                "ImportedNode",
                "ImportedItem",
                "SessionImportError",
                "Parser",
                "call_parser",
                "flatten_nodes",
                "ingest_session",
                "session_request",
                "NODE_BATCH_SIZE",
            }
        ),
        reexports={
            "ImportFailure": "kitaru.api_models.v1.imports",
            "ImportStats": "kitaru.api_models.v1.imports",
            "MAX_IMPORT_FAILURES": "kitaru.api_models.v1.imports",
        },
    ),
}


# Pydantic machinery that appears as class members but is not part of the
# documented API surface. Members inherited from pydantic.BaseModel itself
# (model_dump, model_validate, ...) never reach the serialized output because
# griffe only records inherited members from bases inside the loaded package.
# This is a name list, not a general rule: it names the hooks kitaru models
# currently override, and grows when a newly overridden hook leaks into the
# docs. A blanket model_* filter would be wrong — model_params/model_provider
# are genuine Kitaru API fields.
EXCLUDED_CLASS_MEMBERS = {
    "model_config",
    "model_fields",
    "model_computed_fields",
    "model_post_init",
}


class PublicApiError(Exception):
    """Raised when the extracted API does not match the allowlist."""


def _is_private(name: str) -> bool:
    """Check if a module/symbol name is private by convention."""
    return name.rsplit(".", 1)[-1].startswith("_")


def _is_documented_member(name: str) -> bool:
    """Return whether a member should appear in generated docs."""
    if name in EXCLUDED_CLASS_MEMBERS:
        return False
    return name == "__init__" or not _is_private(name)


def _filter_attributes(attributes: list[dict]) -> list[dict]:
    """Filter a list of serialized attributes to public ones only."""
    return [
        attr
        for attr in attributes
        if not _is_private(attr.get("name", ""))
        and attr.get("name") not in EXCLUDED_CLASS_MEMBERS
    ]


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
    # inherited_members maps a base-class path to the list of members
    # inherited from it; filter each list and drop bases with nothing left.
    inherited: dict[str, list[dict]] = {}
    for base_path, members in data.get("inherited_members", {}).items():
        # Inherited entries are references carrying only "kind" and "path",
        # so the member name is the last path segment.
        kept = [
            member
            for member in members
            if _is_documented_member(
                str(member.get("name") or member.get("path", "")).rsplit(".", 1)[-1]
            )
        ]
        if kept:
            inherited[base_path] = kept
    filtered["inherited_members"] = inherited
    return filtered


def _index_modules(data: dict) -> dict[str, dict]:
    """Map every dotted module path in the serialized tree to its dict."""
    index: dict[str, dict] = {}

    def walk(module: dict) -> None:
        path = str(module.get("path") or "")
        if path:
            index[path] = module
        for submodule in module.get("modules", {}).values():
            walk(submodule)

    walk(data)
    return index


_DOTTED_KITARU_NAME = re.compile(r"\bkitaru(?:\.[A-Za-z_][A-Za-z0-9_]*)+\b")


def _rewrite_references(
    obj: object,
    symbol_map: dict[str, str],
    published_classes: dict[str, dict],
) -> object:
    """Rewrite dotted kitaru names in ``path`` and ``annotation`` strings.

    Published symbols keep pointing at their defining module otherwise, which
    would produce links to unpublished pages. Names that stay fully qualified
    are real import locations, just undocumented ones. This runs as one
    whole-tree pass after assembly because annotations may reference symbols
    published from a different module than the one being built.
    """
    # Longest key first, so a member path like <class>.<method> matches its
    # class before any shorter module-level entry could.
    prefixes = sorted(symbol_map, key=len, reverse=True)

    def publish_path(value: str) -> str:
        for prefix in prefixes:
            if value == prefix or value.startswith(prefix + "."):
                return symbol_map[prefix] + value[len(prefix) :]
        return value

    def resolve_annotation_name(token: str) -> str:
        if token in symbol_map:
            return symbol_map[token]
        # A method that shadows a builtin (e.g. CredentialStore.list) makes
        # griffe resolve the builtin in its own annotation to the method
        # itself; collapse the reference back to the builtin name.
        prefix, _, member = token.rpartition(".")
        cls = published_classes.get(symbol_map.get(prefix, ""))
        if (
            cls is not None
            and member in cls.get("functions", {})
            and hasattr(builtins, member)
        ):
            return member
        return token

    def walk(node: object) -> object:
        if isinstance(node, dict):
            rewritten: dict = {}
            for key, value in node.items():
                if key == "path" and isinstance(value, str):
                    rewritten[key] = publish_path(value)
                elif key == "annotation" and isinstance(value, str):
                    rewritten[key] = _DOTTED_KITARU_NAME.sub(
                        lambda match: resolve_annotation_name(match.group(0)), value
                    )
                else:
                    rewritten[key] = walk(value)
            return rewritten
        if isinstance(node, list):
            return [walk(item) for item in node]
        return node

    return walk(obj)


def _find_symbol(module: dict, name: str) -> tuple[str, dict, str] | None:
    """Locate a symbol in a module's subtree.

    Returns (kind, payload, defining module path) where kind is one of
    "class", "function", or "attribute", or None when the name is absent.
    """
    path = str(module.get("path") or "")
    if name in module.get("classes", {}):
        return ("class", module["classes"][name], path)
    if name in module.get("functions", {}):
        return ("function", module["functions"][name], path)
    for attr in module.get("attributes", []):
        if attr.get("name") == name:
            return ("attribute", attr, path)
    for submodule in module.get("modules", {}).values():
        found = _find_symbol(submodule, name)
        if found is not None:
            return found
    return None


def _build_public_module(
    module_path: str,
    spec: ModuleSpec,
    index: dict[str, dict],
) -> tuple[dict, dict[str, str]]:
    """Assemble one published module dict from the allowlist and the raw tree.

    Also returns the module's symbol map: each symbol's defining dotted path
    mapped to its published one, for the reference rewrite across modules.
    Symbol payloads keep their defining paths here; ``_rewrite_references``
    rewrites the assembled tree once the full symbol map is known.
    """
    raw_module = index.get(module_path)
    if raw_module is None:
        raise PublicApiError(f"Module '{module_path}' not found in extracted API")

    filtered = dict(raw_module)
    classes: dict[str, dict] = {}
    functions: dict[str, dict] = {}
    attributes: list[dict] = []

    lookups: list[tuple[str, dict]] = [
        (name, raw_module) for name in sorted(spec.symbols)
    ]
    for name, source_path in sorted(spec.reexports.items()):
        source_module = index.get(source_path)
        if source_module is None:
            raise PublicApiError(
                f"Re-export source '{source_path}' for '{module_path}.{name}' "
                "not found in extracted API"
            )
        lookups.append((name, source_module))

    symbol_map: dict[str, str] = {}
    for name, search_root in lookups:
        found = _find_symbol(search_root, name)
        if found is None:
            raise PublicApiError(
                f"Public symbol '{module_path}.{name}' not found in extracted API "
                "(was it renamed or removed?)"
            )
        kind, payload, defining_path = found
        symbol_map[f"{defining_path}.{name}"] = f"{module_path}.{name}"
        if kind == "class":
            classes[name] = _filter_class(payload)
        elif kind == "function":
            functions[name] = payload
        else:
            attributes.append(payload)

    filtered["classes"] = classes
    filtered["functions"] = functions
    filtered["attributes"] = _filter_attributes(attributes)
    filtered["modules"] = {}
    return filtered, symbol_map


def build_public_api(raw: dict) -> dict:
    """Filter the raw extracted tree down to the PUBLIC_API allowlist."""
    index = _index_modules(raw)

    built: dict[str, dict] = {}
    symbol_map: dict[str, str] = {}
    for path, spec in PUBLIC_API.items():
        built[path], module_symbol_map = _build_public_module(path, spec, index)
        symbol_map.update(module_symbol_map)

    # Reattach each built module under its parent so the output is a single
    # tree rooted at 'kitaru' (convert-sdk-docs.mjs reads the root name).
    root = dict(raw)
    root["classes"] = {}
    root["functions"] = {}
    root["attributes"] = []
    root["modules"] = {}
    tree_nodes: dict[str, dict] = {ROOT_MODULE: root}
    for path in sorted(built):
        parent_path, _, name = path.rpartition(".")
        parent = tree_nodes.get(parent_path)
        if parent is None:
            raise PublicApiError(
                f"Public module '{path}' has no published parent '{parent_path}'"
            )
        parent["modules"][name] = built[path]
        tree_nodes[path] = built[path]

    published_classes = {
        f"{module_path}.{name}": cls
        for module_path, module in built.items()
        for name, cls in module["classes"].items()
    }
    rewritten = _rewrite_references(root, symbol_map, published_classes)
    assert isinstance(rewritten, dict)
    return rewritten


_MISSING_FUMAPY_MSG = (
    "Missing dependency: fumapy (and griffe).\n"
    "  Install: uv pip install ./docs/node_modules/fumadocs-python\n"
    "  (requires 'pnpm install' in docs/ first)"
)


def extract_api(module_name: str) -> dict:
    """Extract the full API of a Python module using griffe."""
    if not _HAS_GRIFFE:
        raise ImportError(_MISSING_FUMAPY_MSG)
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


def count_symbols(module: dict) -> tuple[int, int, int]:
    """Count classes, functions, and modules across a serialized module tree."""
    n_classes = len(module.get("classes", {}))
    n_functions = len(module.get("functions", {}))
    n_modules = len(module.get("modules", {}))
    for submodule in module.get("modules", {}).values():
        sub_classes, sub_functions, sub_modules = count_symbols(submodule)
        n_classes += sub_classes
        n_functions += sub_functions
        n_modules += sub_modules
    return n_classes, n_functions, n_modules


def main() -> int:
    """Extract and filter the Kitaru SDK API to JSON."""
    print("Extracting SDK API...")
    try:
        raw = extract_api(ROOT_MODULE)
        filtered = build_public_api(raw)
    except (ImportError, PublicApiError) as error:
        print(f"ERROR: {error}")
        return 1

    n_classes, n_functions, n_modules = count_symbols(filtered)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(filtered, indent=2) + "\n")

    rel = OUTPUT_FILE.relative_to(REPO_ROOT)
    print(
        f"Extracted {n_classes} classes, {n_functions} functions, {n_modules} modules"
    )
    print(f"Wrote {rel}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
