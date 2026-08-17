"""Tests for the SDK documentation generator."""

import json
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


class TestAllowlistConfiguration:
    def test_only_client_and_task_modules_are_published(self) -> None:
        assert set(PUBLIC_API) == {
            "kitaru.client",
            "kitaru.task",
            "kitaru.task.evaluator",
            "kitaru.task.importer",
        }

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
        assert set(public_api["modules"]) == {"client", "task"}

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
