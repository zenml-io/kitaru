"""Tests for the SDK documentation generator."""

from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from generate_sdk_docs import (
    EXCLUDED_SUBMODULES,
    _filter_module,
    _is_private,
    extract_api,
)


@pytest.fixture
def output_dir() -> Generator[Path]:
    """Temporary directory for generated docs."""
    d = Path(tempfile.mkdtemp(prefix="test-sdk-docs-"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


class TestPrivateDetection:
    """Tests for _is_private helper."""

    def test_private_underscore(self) -> None:
        assert _is_private("_internal") is True

    def test_dunder_is_private(self) -> None:
        assert _is_private("__init__") is True

    def test_public_name(self) -> None:
        assert _is_private("flow") is False


class TestFilterModule:
    """Tests for module filtering logic."""

    def test_excludes_cli_module(self) -> None:
        data = {
            "modules": {
                "cli": {"name": "cli"},
                "core": {"name": "core", "modules": {}},
            },
            "classes": {},
            "functions": {},
            "attributes": [],
        }
        filtered = _filter_module(data)
        assert "cli" not in filtered["modules"]
        assert "core" in filtered["modules"]

    def test_excludes_adapters_module(self) -> None:
        data = {
            "modules": {"adapters": {"name": "adapters"}},
            "classes": {},
            "functions": {},
            "attributes": [],
        }
        filtered = _filter_module(data)
        assert "adapters" not in filtered["modules"]

    def test_excludes_private_modules_at_root(self) -> None:
        data = {
            "modules": {
                "_internal": {
                    "name": "_internal",
                    "modules": {},
                    "classes": {"Foo": {"name": "Foo"}},
                    "functions": {"bar": {"name": "bar"}},
                },
            },
            "classes": {},
            "functions": {},
            "attributes": [],
        }
        filtered = _filter_module(data, is_root=True)
        assert "_internal" not in filtered["modules"]

    def test_promotes_symbols_from_private_modules(self) -> None:
        data = {
            "modules": {
                "_internal": {
                    "name": "_internal",
                    "modules": {},
                    "classes": {"Foo": {"name": "Foo"}},
                    "functions": {"bar": {"name": "bar"}},
                },
            },
            "classes": {},
            "functions": {},
            "attributes": [],
        }
        filtered = _filter_module(data, is_root=True)
        assert "Foo" in filtered["classes"]
        assert "bar" in filtered["functions"]

    def test_filters_dunder_all_attribute(self) -> None:
        data = {
            "modules": {},
            "classes": {},
            "functions": {},
            "attributes": [
                {"name": "__all__", "value": "['flow']"},
                {"name": "VERSION", "value": "'1.0'"},
            ],
        }
        filtered = _filter_module(data)
        names = [a["name"] for a in filtered["attributes"]]
        assert "__all__" not in names
        assert "VERSION" in names

    def test_does_not_promote_at_non_root(self) -> None:
        data = {
            "modules": {
                "_private": {
                    "name": "_private",
                    "modules": {},
                    "classes": {"Secret": {"name": "Secret"}},
                    "functions": {},
                },
            },
            "classes": {},
            "functions": {},
            "attributes": [],
        }
        filtered = _filter_module(data, is_root=False)
        # Private modules are excluded even at non-root
        assert "_private" not in filtered["modules"]
        # But symbols are NOT promoted when not root
        assert "Secret" not in filtered["classes"]


class TestExcludedModules:
    """Tests for the exclusion list configuration."""

    def test_cli_in_exclusions(self) -> None:
        assert "cli" in EXCLUDED_SUBMODULES

    def test_adapters_in_exclusions(self) -> None:
        assert "adapters" in EXCLUDED_SUBMODULES

    def test_runtime_in_exclusions(self) -> None:
        assert "runtime" in EXCLUDED_SUBMODULES


class TestExtractApi:
    """Tests for griffe-based API extraction."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_fumapy(self) -> None:
        """Skip tests if fumapy is not installed."""
        pytest.importorskip("fumapy")

    def test_extracts_kitaru_package(self) -> None:
        raw = extract_api("kitaru")
        assert raw["name"] == "kitaru"
        assert raw["path"] == "kitaru"

    def test_extraction_includes_modules(self) -> None:
        raw = extract_api("kitaru")
        assert "modules" in raw
        assert isinstance(raw["modules"], dict)

    def test_extraction_produces_valid_json(self) -> None:
        raw = extract_api("kitaru")
        # Should be JSON-serializable without custom encoder
        json_str = json.dumps(raw)
        assert json.loads(json_str) == raw

    def test_filtered_extraction_excludes_cli_and_adapters(self) -> None:
        raw = extract_api("kitaru")
        filtered = _filter_module(raw, is_root=True)
        # CLI and adapters should never appear in filtered output
        assert "cli" not in filtered.get("modules", {})
        assert "adapters" not in filtered.get("modules", {})

    def test_filtered_extraction_includes_stack_helpers(self) -> None:
        raw = extract_api("kitaru")
        filtered = _filter_module(raw, is_root=True)

        # Stack helpers live in kitaru.config (a public submodule), so they
        # appear under the config module rather than being promoted to root.
        config_funcs = set(
            filtered.get("modules", {}).get("config", {}).get("functions", {}).keys()
        )
        assert {"list_runners", "current_runner", "use_runner"}.issubset(config_funcs)
