"""Tests for the SDK documentation generator."""

import ast
import importlib
import inspect
import json
from pathlib import Path
from typing import ClassVar

import pytest
from scripts.generate_sdk_docs import (
    PUBLIC_API,
    ModuleSpec,
    PublicApiError,
    _build_public_module,
    _filter_class,
    _is_documented_member,
    _is_private,
    _rewrite_references,
    build_public_api,
    count_symbols,
    extract_api,
)

EXCLUDED_MODULE_SEGMENTS = ("server", "cli", "mcp", "worker", "adapters")


def iter_string_values(obj: object, key: str):
    """Yield every string stored under ``key`` anywhere in a dict/list tree."""
    if isinstance(obj, dict):
        value = obj.get(key)
        if isinstance(value, str):
            yield value
        for child in obj.values():
            yield from iter_string_values(child, key)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_string_values(item, key)


class TestPrivateDetection:
    def test_private_underscore(self) -> None:
        assert _is_private("_internal") is True

    def test_dunder_is_private(self) -> None:
        assert _is_private("__init__") is True

    def test_public_name(self) -> None:
        assert _is_private("call_evaluator") is False

    def test_dotted_private_module_is_private(self) -> None:
        assert _is_private("kitaru._experiments") is True


class TestDocumentedMemberDetection:
    def test_init_is_kept(self) -> None:
        assert _is_documented_member("__init__") is True

    def test_private_helper_is_excluded(self) -> None:
        assert _is_documented_member("_helper") is False

    def test_pydantic_machinery_is_excluded(self) -> None:
        assert _is_documented_member("model_config") is False


class TestRewriteReferences:
    SYMBOL_MAP: ClassVar[dict[str, str]] = {
        "kitaru.client.api_client.KitaruAPIClient": "kitaru.client.KitaruAPIClient",
        "kitaru.client.credential_store.CredentialStore": (
            "kitaru.client.CredentialStore"
        ),
    }
    PUBLISHED_CLASSES: ClassVar[dict[str, dict]] = {
        "kitaru.client.CredentialStore": {"functions": {"list": {"name": "list"}}}
    }

    def rewrite(self, data: dict) -> object:
        return _rewrite_references(data, self.SYMBOL_MAP, self.PUBLISHED_CLASSES)

    def test_published_path_gets_published_prefix(self) -> None:
        data = {"path": "kitaru.client.api_client.KitaruAPIClient", "name": "x"}
        assert self.rewrite(data) == {
            "path": "kitaru.client.KitaruAPIClient",
            "name": "x",
        }

    def test_member_path_matches_its_class_prefix(self) -> None:
        data = {"path": "kitaru.client.api_client.KitaruAPIClient.with_token"}
        assert self.rewrite(data) == {
            "path": "kitaru.client.KitaruAPIClient.with_token"
        }

    def test_unpublished_sibling_path_is_kept(self) -> None:
        # A neighbor in the same defining module that is not itself published
        # must keep its real path rather than gaining a nonexistent page.
        data = {"path": "kitaru.client.api_client.SomeUnpublishedBase"}
        assert self.rewrite(data) == data

    def test_rewrites_nested_structures(self) -> None:
        data = {
            "functions": {"f": {"path": "kitaru.client.api_client.KitaruAPIClient.f"}},
            "attributes": [{"path": "kitaru.client.credential_store.CredentialStore"}],
        }
        assert self.rewrite(data) == {
            "functions": {"f": {"path": "kitaru.client.KitaruAPIClient.f"}},
            "attributes": [{"path": "kitaru.client.CredentialStore"}],
        }

    def test_published_annotation_gets_published_path(self) -> None:
        data = {"annotation": "kitaru.client.api_client.KitaruAPIClient"}
        assert self.rewrite(data) == {"annotation": "kitaru.client.KitaruAPIClient"}

    def test_rewrites_annotations_embedded_in_generics(self) -> None:
        data = {"annotation": "list[kitaru.client.api_client.KitaruAPIClient] | None"}
        assert self.rewrite(data) == {
            "annotation": "list[kitaru.client.KitaruAPIClient] | None"
        }

    def test_builtin_shadowing_method_collapses_to_builtin(self) -> None:
        data = {"annotation": "kitaru.client.credential_store.CredentialStore.list"}
        assert self.rewrite(data) == {"annotation": "list"}

    def test_unpublished_importable_annotation_is_kept(self) -> None:
        data = {"annotation": "kitaru.api_models.v1.session.SessionCreateRequest"}
        assert self.rewrite(data) == data

    def test_other_keys_are_untouched(self) -> None:
        data = {"value": "kitaru.client.api_client.KitaruAPIClient"}
        assert self.rewrite(data) == data


class TestFilterClass:
    def test_filters_private_members(self) -> None:
        data = {
            "name": "Widget",
            "functions": {
                "__init__": {"name": "__init__"},
                "_helper": {"name": "_helper"},
                "render": {"name": "render"},
            },
            "classes": {},
            "attributes": [{"name": "_secret"}, {"name": "label"}],
            "inherited_members": {
                "pkg.Base": [
                    {"kind": "attribute", "path": "pkg.Base.model_config"},
                    {"kind": "function", "path": "pkg.Base.display"},
                ]
            },
        }
        filtered = _filter_class(data)
        assert set(filtered["functions"]) == {"__init__", "render"}
        assert [attr["name"] for attr in filtered["attributes"]] == ["label"]
        inherited = filtered["inherited_members"]["pkg.Base"]
        assert [member["path"] for member in inherited] == ["pkg.Base.display"]

    def test_drops_bases_with_no_documented_members(self) -> None:
        data = {
            "name": "Widget",
            "functions": {},
            "classes": {},
            "attributes": [],
            "inherited_members": {
                "pkg.Base": [{"kind": "attribute", "path": "pkg.Base.model_config"}]
            },
        }
        assert _filter_class(data)["inherited_members"] == {}


class TestBuildPublicModule:
    def test_missing_symbol_fails_loudly(self) -> None:
        index = {
            "kitaru.demo": {
                "name": "demo",
                "path": "kitaru.demo",
                "classes": {},
                "functions": {},
                "attributes": [],
                "modules": {},
            }
        }
        spec = ModuleSpec(symbols=frozenset({"Missing"}))
        with pytest.raises(PublicApiError, match="Missing"):
            _build_public_module("kitaru.demo", spec, index)

    def test_missing_module_fails_loudly(self) -> None:
        with pytest.raises(PublicApiError, match="not found"):
            _build_public_module("kitaru.nope", ModuleSpec(), {})


# Public names in published api_models modules that are deliberately not in
# the allowlist. The structural drift test fails when a module gains a public
# name that is neither allowlisted nor listed here, so new symbols surface
# loudly instead of silently staying undocumented.
API_MODELS_DOC_EXCLUSIONS: dict[str, frozenset[str]] = {
    # Shared base machinery and validation aliases; developers receive these
    # via the concrete request/response models, never construct them directly.
    "kitaru.api_models.v1.base": frozenset(
        {
            "RequestModel",
            "ResponseModel",
            "DiscriminatedRequestModel",
            "TimestampedResponseModel",
            "OwnedResponseModel",
            "ErrorBody",
            "PlainSerializedSecretStr",
            "FiniteFloat",
            "JsonValue",
            "ItemT",
        }
    ),
    # EvaluationResult is published under kitaru.task.evaluator (reexport);
    # the rest are validation internals.
    "kitaru.api_models.v1.evaluation": frozenset(
        {"EvaluationResult", "EvaluationName", "MAX_NAME_LENGTH"}
    ),
    # Published under kitaru.task.importer (reexports).
    "kitaru.api_models.v1.imports": frozenset(
        {"ImportFailure", "ImportStats", "MAX_IMPORT_FAILURES"}
    ),
    # Annotated discriminated-union alias; the four concrete configs are the
    # developer-facing entry points.
    "kitaru.api_models.v1.replay_config": frozenset({"ToolConfig"}),
    # Annotated union aliases; the concrete filter models are published.
    "kitaru.api_models.v1.filter": frozenset({"Filter", "FilterParam"}),
    "kitaru.api_models.v1.replay": frozenset(),
    "kitaru.api_models.v1.session": frozenset(),
    "kitaru.api_models.v1.session_node": frozenset(),
    "kitaru.api_models.v1.job": frozenset(),
    "kitaru.api_models.v1.agent": frozenset(),
    "kitaru.api_models.v1.agent_version": frozenset(),
    "kitaru.api_models.v1.cohort": frozenset(),
    "kitaru.api_models.v1.cohort_version": frozenset(),
    "kitaru.api_models.v1.evaluator": frozenset(),
    "kitaru.api_models.v1.experiment": frozenset(),
    "kitaru.api_models.v1.experiment_run": frozenset(),
    "kitaru.api_models.v1.investigation": frozenset(),
    "kitaru.api_models.v1.annotation": frozenset(),
}

# The published resource classes mapped to the submodule that defines each.
PUBLISHED_RESOURCE_CLASSES: dict[str, str] = {
    "SessionsResource": "sessions",
    "ReplaysResource": "replays",
    "AgentsResource": "agents",
    "AgentVersionsResource": "agent_versions",
    "JobsResource": "jobs",
    "EvaluatorsResource": "evaluators",
    "EvaluationsResource": "evaluations",
    "CohortsResource": "cohorts",
    "CohortVersionsResource": "cohort_versions",
    "ExperimentsResource": "experiments",
    "ExperimentRunsResource": "experiment_runs",
    "InvestigationsResource": "investigations",
}


def get_public_source_names(module_path: str) -> set[str]:
    """Collect public top-level names defined in a module's source file.

    Uses the AST rather than runtime ``vars()`` so imported names (pydantic
    helpers, sibling models) do not count as definitions, mirroring what
    griffe extracts.
    """
    module = importlib.import_module(module_path)
    assert module.__file__ is not None
    tree = ast.parse(Path(module.__file__).read_text())
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(
                target.id for target in node.targets if isinstance(target, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return {name for name in names if not name.startswith("_")}


class TestAllowlistConfiguration:
    def test_published_module_set_is_exact(self) -> None:
        api_models_leaves = {
            f"kitaru.api_models.v1.{name}"
            for name in (
                "base",
                "session",
                "session_node",
                "evaluation",
                "replay",
                "replay_config",
                "job",
                "imports",
                "agent",
                "agent_version",
                "cohort",
                "cohort_version",
                "evaluator",
                "experiment",
                "experiment_run",
                "investigation",
                "annotation",
                "filter",
            )
        }
        assert (
            set(PUBLIC_API)
            == {
                "kitaru.client",
                "kitaru.client.resources",
                "kitaru.task",
                "kitaru.task.evaluator",
                "kitaru.task.importer",
                "kitaru.api_models",
                "kitaru.api_models.v1",
            }
            | api_models_leaves
        )

    def test_api_models_allowlists_track_module_sources(self) -> None:
        # Structural drift check for modules without __all__: every
        # allowlisted symbol must exist in the module source, and every public
        # name in the source must be either allowlisted or in the documented
        # exclusions above.
        assert set(API_MODELS_DOC_EXCLUSIONS) == {
            path for path in PUBLIC_API if path.startswith("kitaru.api_models.v1.")
        }
        for module_path, excluded in API_MODELS_DOC_EXCLUSIONS.items():
            spec = PUBLIC_API[module_path]
            documented = set(spec.symbols) | set(spec.reexports)
            public_names = get_public_source_names(module_path)
            missing = documented - public_names
            assert not missing, f"{module_path}: allowlisted but not defined: {missing}"
            unaccounted = public_names - documented - excluded
            assert not unaccounted, (
                f"{module_path}: public names neither allowlisted nor in "
                f"API_MODELS_DOC_EXCLUSIONS: {sorted(unaccounted)}"
            )
            stale = excluded - public_names
            assert not stale, f"{module_path}: stale exclusions: {sorted(stale)}"

    def test_resource_allowlist_matches_defined_classes(self) -> None:
        spec = PUBLIC_API["kitaru.client.resources"]
        assert set(PUBLISHED_RESOURCE_CLASSES) | {"iterate_pages"} == set(spec.symbols)
        for class_name, submodule in PUBLISHED_RESOURCE_CLASSES.items():
            module = importlib.import_module(f"kitaru.client.resources.{submodule}")
            assert isinstance(getattr(module, class_name), type), class_name

    def test_no_excluded_segment_in_allowlist(self) -> None:
        for path in PUBLIC_API:
            assert not any(
                segment in path.split(".") for segment in EXCLUDED_MODULE_SEGMENTS
            )

    def test_allowlist_tracks_module_all_exports(self) -> None:
        # A symbol newly added to a published module's __all__ must show up
        # here too, or it silently never reaches the reference site.
        import kitaru.client
        import kitaru.task.evaluator
        import kitaru.task.importer

        # Worker-side task entrypoints are exported for task/__main__.py but
        # are not user API.
        worker_entrypoints = {"run"}
        for module_path, module in [
            ("kitaru.client", kitaru.client),
            ("kitaru.task.evaluator", kitaru.task.evaluator),
            ("kitaru.task.importer", kitaru.task.importer),
        ]:
            spec = PUBLIC_API[module_path]
            documented = set(spec.symbols) | set(spec.reexports)
            assert documented == set(module.__all__) - worker_entrypoints, module_path


@pytest.fixture(scope="module")
def public_api() -> dict:
    """Extract and filter the real kitaru package once for all extraction tests."""
    pytest.importorskip("fumapy")
    return build_public_api(extract_api("kitaru"))


class TestExtractedPublicApi:
    """End-to-end tests over the real extracted package (requires fumapy)."""

    def test_root_module_is_kitaru(self, public_api: dict) -> None:
        assert public_api["name"] == "kitaru"
        assert set(public_api["modules"]) == {"api_models", "client", "task"}

    def test_api_models_containers_publish_no_symbols(self, public_api: dict) -> None:
        api_models = public_api["modules"]["api_models"]
        v1 = api_models["modules"]["v1"]
        for container in (api_models, v1):
            assert container["classes"] == {}
            assert container["functions"] == {}
        assert set(api_models["modules"]) == {"v1"}
        assert len(v1["modules"]) == 18

    def test_page_is_published_under_api_models_base(self, public_api: dict) -> None:
        base = public_api["modules"]["api_models"]["modules"]["v1"]["modules"]["base"]
        assert set(base["classes"]) == {"Page", "ListParams", "CursorParams"}

    def test_resource_classes_expose_all_public_methods(self, public_api: dict) -> None:
        # Structural drift check: a public method added to a published
        # resource class must show up on its generated page.
        resources = public_api["modules"]["client"]["modules"]["resources"]
        assert "iterate_pages" in resources["functions"]
        for class_name, submodule in PUBLISHED_RESOURCE_CLASSES.items():
            module = importlib.import_module(f"kitaru.client.resources.{submodule}")
            cls = getattr(module, class_name)
            real_methods = {
                name
                for name, member in vars(cls).items()
                if inspect.isfunction(member) and not name.startswith("_")
            }
            documented = set(resources["classes"][class_name]["functions"])
            missing = real_methods - documented
            assert not missing, f"{class_name}: undocumented methods {sorted(missing)}"

    def test_sessions_resource_page_has_expected_methods(
        self, public_api: dict
    ) -> None:
        resources = public_api["modules"]["client"]["modules"]["resources"]
        sessions = resources["classes"]["SessionsResource"]
        for method in ("create", "get", "get_with_nodes", "list", "iter", "delete"):
            assert method in sessions["functions"], method

    def test_output_is_json_serializable(self, public_api: dict) -> None:
        assert json.loads(json.dumps(public_api)) == public_api

    def test_confirmed_public_symbols_present(self, public_api: dict) -> None:
        client = public_api["modules"]["client"]
        assert "KitaruAPIClient" in client["classes"]

        task = public_api["modules"]["task"]
        evaluator = task["modules"]["evaluator"]
        assert "EvaluationResult" in evaluator["classes"]
        assert "SessionView" in evaluator["classes"]

        importer = task["modules"]["importer"]
        assert "ImportedSession" in importer["classes"]
        assert "ImportedNode" in importer["classes"]
        assert "ImportFailure" in importer["classes"]

    def test_output_is_non_empty(self, public_api: dict) -> None:
        n_classes, n_functions, _ = count_symbols(public_api)
        assert n_classes + n_functions > 20

    def test_no_excluded_module_paths_leak(self, public_api: dict) -> None:
        leaked = [
            path
            for path in iter_string_values(public_api, "path")
            if any(seg in path.split(".") for seg in EXCLUDED_MODULE_SEGMENTS)
        ]
        assert leaked == []

    def test_no_private_top_level_symbols_leak(self, public_api: dict) -> None:
        def check_module(module: dict) -> None:
            for name in list(module["classes"]) + list(module["functions"]):
                assert not name.startswith("_"), name
            for attr in module["attributes"]:
                assert not attr["name"].startswith("_"), attr["name"]
            for name, submodule in module["modules"].items():
                assert not name.startswith("_"), name
                check_module(submodule)

        check_module(public_api)

    def test_worker_entrypoints_are_not_published(self, public_api: dict) -> None:
        task = public_api["modules"]["task"]
        assert "run" not in task["modules"]["evaluator"]["functions"]
        assert "run" not in task["modules"]["importer"]["functions"]

    def test_annotations_use_published_paths(self, public_api: dict) -> None:
        client = public_api["modules"]["client"]
        with_token = client["classes"]["KitaruAPIClient"]["functions"]["with_token"]
        assert with_token["returns"]["annotation"] == "kitaru.client.KitaruAPIClient"

    def test_builtin_shadowing_annotation_is_collapsed(self, public_api: dict) -> None:
        credential_store = public_api["modules"]["client"]["classes"]["CredentialStore"]
        returns = credential_store["functions"]["list"]["returns"]
        assert returns["annotation"] == "list"

    def test_no_defining_module_paths_leak_in_annotations(
        self, public_api: dict
    ) -> None:
        # Published client symbols live in internal submodules (api_client,
        # credentials, ...); annotations must reference the published paths.
        internal_prefixes = tuple(
            f"kitaru.client.{submodule}."
            for submodule in ("api_client", "client", "sync_client", "credentials")
        )
        leaked = [
            annotation
            for annotation in iter_string_values(public_api, "annotation")
            if any(prefix in annotation for prefix in internal_prefixes)
        ]
        assert leaked == []

    def test_api_models_reexports_carry_published_paths(self, public_api: dict) -> None:
        importer = public_api["modules"]["task"]["modules"]["importer"]
        assert (
            importer["classes"]["ImportFailure"]["path"]
            == "kitaru.task.importer.ImportFailure"
        )
