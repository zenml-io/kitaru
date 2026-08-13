import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.release_units import (
    ReleaseInventoryError,
    build_plugin_matrix,
    format_inventory,
    load_inventory,
    parse_package_tag,
    validate_version,
)

REPO_ROOT = Path(__file__).parents[2]
INVENTORY_PATH = REPO_ROOT / "release" / "release-units.toml"

EXPECTED_UNITS = {
    "kitaru": "kitaru",
    "braintrust-importer": "kitaru-braintrust-importer",
    "evaluator": "kitaru-evaluator",
    "jsonl-importer": "kitaru-jsonl-importer",
    "langfuse-importer": "kitaru-langfuse-importer",
    "langgraph": "kitaru-langgraph",
    "langsmith-importer": "kitaru-langsmith-importer",
    "openai-agents": "kitaru-openai-agents",
    "opentelemetry-importer": "kitaru-opentelemetry-importer",
    "pydantic-ai": "kitaru-pydantic-ai",
}

EXPECTED_DEFAULT_DISTRIBUTIONS = {
    "kitaru-braintrust-importer",
    "kitaru-evaluator",
    "kitaru-jsonl-importer",
    "kitaru-langfuse-importer",
    "kitaru-langsmith-importer",
    "kitaru-opentelemetry-importer",
}


@pytest.fixture
def release_repo(tmp_path: Path) -> Path:
    for relative_path in (
        "pyproject.toml",
        "release/release-units.toml",
        "plugins/default-requirements.txt",
        "src/kitaru/server/api/bootstrap.py",
    ):
        source = REPO_ROOT / relative_path
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)

    for manifest in (REPO_ROOT / "plugins" / "packages").glob("*/pyproject.toml"):
        relative_path = manifest.relative_to(REPO_ROOT)
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(manifest, destination)

    return tmp_path


def test_inventory_describes_exactly_the_ten_python_distributions() -> None:
    inventory = load_inventory()

    assert {unit.slug: unit.distribution for unit in inventory.units} == EXPECTED_UNITS
    assert {
        unit.distribution for unit in inventory.units if unit.default_catalog
    } == EXPECTED_DEFAULT_DISTRIBUTIONS
    assert all(unit.registry == "pypi" for unit in inventory.units)
    assert all(unit.path != "docs" for unit in inventory.units)
    assert all(not unit.path.startswith("packages/") for unit in inventory.units)


def test_inventory_versions_and_tags_match_project_manifests() -> None:
    inventory = load_inventory()

    for unit in inventory.units:
        resolved = parse_package_tag(unit.tag, inventory)
        assert resolved == unit
        assert unit.tag == f"python/{unit.distribution}/v{unit.version}"


@pytest.mark.parametrize("version", ["0.22.0rc1", "0.22.0", "1.0.dev1"])
def test_canonical_python_versions_are_accepted(version: str) -> None:
    assert validate_version(version) == version


@pytest.mark.parametrize(
    "version",
    ["0.22.0-rc.1", "v0.22.0", "01.2.3", "not-a-version"],
)
def test_noncanonical_python_versions_are_rejected(version: str) -> None:
    with pytest.raises(ReleaseInventoryError, match="canonical PEP 440"):
        validate_version(version)


def test_local_python_versions_are_rejected_for_pypi() -> None:
    with pytest.raises(ReleaseInventoryError, match="local segment"):
        validate_version("1.0+build.1")


def test_legacy_release_workflow_accepts_canonical_python_rc_versions() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert "1.2.3rc1" in workflow
    assert "HELM_VERSION" in workflow


@pytest.mark.parametrize(
    ("tag", "message"),
    [
        ("kitaru-v0.21.0", "package tag"),
        ("python/unknown/v0.1.0", "unknown distribution"),
        ("python/kitaru/v0.22.0-rc.1", "canonical PEP 440"),
        ("python/kitaru/v0.22.0", "does not match manifest version"),
    ],
)
def test_invalid_or_mismatched_package_tags_are_rejected(
    tag: str, message: str
) -> None:
    with pytest.raises(ReleaseInventoryError, match=message):
        parse_package_tag(tag, load_inventory())


def test_inventory_rejects_duplicate_unit_identity(release_repo: Path) -> None:
    inventory_path = release_repo / "release" / "release-units.toml"
    document = inventory_path.read_text()
    inventory_path.write_text(
        document.replace('slug = "evaluator"', 'slug = "braintrust-importer"', 1)
    )

    with pytest.raises(
        ReleaseInventoryError, match="duplicate slug: braintrust-importer"
    ):
        load_inventory(release_repo)


def test_inventory_rejects_an_unlisted_plugin_project(release_repo: Path) -> None:
    manifest = release_repo / "plugins" / "packages" / "unlisted" / "pyproject.toml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('[project]\nname = "kitaru-unlisted"\nversion = "0.1.0"\n')

    with pytest.raises(
        ReleaseInventoryError, match="unlisted plugin project: unlisted"
    ):
        load_inventory(release_repo)


def test_inventory_rejects_a_missing_plugin_project(release_repo: Path) -> None:
    shutil.rmtree(release_repo / "plugins" / "packages" / "langgraph")

    with pytest.raises(
        ReleaseInventoryError, match="langgraph: project path does not exist"
    ):
        load_inventory(release_repo)


def test_inventory_rejects_an_adapter_in_the_default_catalog(
    release_repo: Path,
) -> None:
    inventory_path = release_repo / "release" / "release-units.toml"
    document = inventory_path.read_text()
    langgraph = document.index('slug = "langgraph"')
    next_unit = document.index("[[units]]", langgraph)
    langgraph_block = document[langgraph:next_unit].replace(
        "default-catalog = false", "default-catalog = true"
    )
    inventory_path.write_text(
        f"{document[:langgraph]}{langgraph_block}{document[next_unit:]}"
    )

    with pytest.raises(
        ReleaseInventoryError, match="default catalog does not match inventory"
    ):
        load_inventory(release_repo)


def test_text_and_json_outputs_contain_the_same_unit_identities() -> None:
    inventory = load_inventory()
    text_output = format_inventory(inventory)
    json_output = json.loads(inventory.to_json())

    assert json_output["schema_version"] == 1
    assert {unit["slug"] for unit in json_output["units"]} == {
        unit.slug for unit in inventory.units
    }
    assert all(unit.distribution in text_output for unit in inventory.units)


def test_plugin_matrix_is_generated_from_the_nine_plugin_units() -> None:
    matrix = build_plugin_matrix(load_inventory())

    assert matrix == {
        "include": [
            {
                "package": slug,
                "path": f"plugins/packages/{slug}",
            }
            for slug in EXPECTED_UNITS
            if slug != "kitaru"
        ]
    }


def test_legacy_plugin_workflow_choices_match_the_inventory() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release-plugins.yml").read_text()
    match = re.search(
        r"(?ms)^        options:\n(?P<options>(?:^          - [a-z0-9-]+\n)+)",
        workflow,
    )
    assert match is not None
    choices = {
        line.removeprefix("          - ")
        for line in match.group("options").splitlines()
    }

    assert choices == {unit.slug for unit in load_inventory().plugin_units}


def test_ci_plugin_matrix_is_loaded_from_the_release_inventory() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()

    assert (
        "plugin_matrix=$(uv run --no-project --with packaging==26.2 python "
        "scripts/release_units.py matrix)" in workflow
    )
    assert (
        "matrix: ${{ fromJSON(needs.release-units.outputs.plugin-matrix).matrix }}"
        in workflow
    )
    assert "PACKAGE_PATH: ${{ matrix.path }}" in workflow
    assert "--package" in workflow
    assert '"$PACKAGE_PATH"' in workflow
    assert "          - braintrust-importer\n" not in workflow


def test_each_unit_exposes_its_exact_release_critical_checks() -> None:
    inventory = load_inventory()

    for unit in inventory.units:
        assert inventory.common_checks <= unit.required_checks
        if unit.slug == "kitaru":
            assert "cli-artifact-contract" in unit.required_checks
            assert "mcp-wheel-contract" in unit.required_checks
        else:
            assert f"plugin package ({unit.slug})" in unit.required_checks


def _run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "release_units.py"), *arguments],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("arguments", "expected_fragment"),
    [
        (["list"], "SLUG\tDISTRIBUTION\tVERSION\tDEFAULT\tTAG"),
        (["resolve", "--unit", "kitaru"], "python/kitaru/v"),
        (["validate"], "Validated 10 release units."),
    ],
)
def test_cli_text_commands_succeed(
    arguments: list[str], expected_fragment: str
) -> None:
    result = _run_cli(*arguments)

    assert result.returncode == 0
    assert expected_fragment in result.stdout
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("arguments", "expected_key"),
    [
        (["list", "--format", "json"], "units"),
        (["matrix"], "matrix"),
        (["resolve", "--unit", "kitaru", "--format", "json"], "unit"),
        (["validate", "--format", "json"], "status"),
    ],
)
def test_cli_json_commands_succeed(arguments: list[str], expected_key: str) -> None:
    result = _run_cli(*arguments)

    assert result.returncode == 0
    assert json.loads(result.stdout)[expected_key]
    assert result.stderr == ""


def test_cli_resolves_the_current_package_tag() -> None:
    tag = load_inventory().units[0].tag
    result = _run_cli("resolve", "--tag", tag, "--format", "json")

    assert result.returncode == 0
    assert json.loads(result.stdout)["unit"]["tag"] == tag
    assert result.stderr == ""


def test_cli_json_errors_are_structured() -> None:
    result = _run_cli("resolve", "--unit", "unknown", "--format", "json")

    assert result.returncode == 2
    assert result.stdout == ""
    assert json.loads(result.stderr) == {
        "schema_version": 1,
        "error": {
            "kind": "release_inventory_error",
            "message": "unknown release unit: unknown",
        },
    }
