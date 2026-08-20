"""Tests for export dependency classification."""

from pathlib import Path

import pytest

from kitaru.exports._dependencies import classify_dependencies
from kitaru.exports.models import ExportError
from kitaru.exports.source import inventory_source


def _write_pyproject(root: Path, dependencies: list[str], **project: str) -> None:
    requires_python = project.get("requires_python", ">=3.11")
    dependency_rows = ",\n".join(f'    "{value}"' for value in dependencies)
    (root / "pyproject.toml").write_text(
        "[project]\n"
        'name = "fixture-agent"\n'
        'version = "1.0.0"\n'
        f'requires-python = "{requires_python}"\n'
        "dependencies = [\n"
        f"{dependency_rows}\n"
        "]\n"
    )


def test_pyproject_with_uv_lock_is_locked_and_deterministic(tmp_path: Path) -> None:
    _write_pyproject(tmp_path, ["Example_B==1.0", "example-a>=2"])
    (tmp_path / "uv.lock").write_text("version = 1\nrevision = 3\n")

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.status == "locked"
    assert plan.manifests == ("pyproject.toml", "uv.lock")
    assert [item.project for item in plan.requirements] == ["example-a", "example-b"]
    assert all(len(item.requirement_digest) == 64 for item in plan.requirements)


def test_pyproject_without_lock_is_honestly_declared(tmp_path: Path) -> None:
    _write_pyproject(tmp_path, ["example>=1"])

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.status == "declared"
    assert plan.manifests == ("pyproject.toml",)


@pytest.mark.parametrize(("hashed", "status"), [(True, "locked"), (False, "declared")])
def test_requirements_status_depends_on_complete_hashes(
    tmp_path: Path, hashed: bool, status: str
) -> None:
    suffix = " --hash=sha256:" + "a" * 64 if hashed else ""
    (tmp_path / "requirements.txt").write_text(f"example==1.0{suffix}\n")

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.status == status
    assert plan.manifests == ("requirements.txt",)


@pytest.mark.parametrize(
    ("files", "code"),
    [
        ({"agent.py": "pass\n"}, "missing_dependency_metadata"),
        (
            {
                "pyproject.toml": (
                    '[project]\nname="agent"\nversion="1"\ndependencies=[]\n'
                ),
                "requirements.txt": "example==1\n",
            },
            "ambiguous_dependency_metadata",
        ),
    ],
)
def test_dependency_metadata_missing_or_ambiguous(
    tmp_path: Path, files: dict[str, str], code: str
) -> None:
    for name, content in files.items():
        (tmp_path / name).write_text(content)

    with pytest.raises(ExportError) as raised:
        classify_dependencies(inventory_source(tmp_path))

    assert raised.value.code == code


def test_dependency_conflicts_use_normalized_project_names(tmp_path: Path) -> None:
    _write_pyproject(tmp_path, ["Example_Pkg==1", "example-pkg==2"])

    with pytest.raises(ExportError) as raised:
        classify_dependencies(inventory_source(tmp_path))

    assert raised.value.code == "dependency_conflict"


def test_in_root_relative_dependency_is_preserved(tmp_path: Path) -> None:
    package = tmp_path / "packages" / "helper"
    package.mkdir(parents=True)
    (package / "pyproject.toml").write_text('[project]\nname="helper"\nversion="1"\n')
    _write_pyproject(tmp_path, ["helper"])
    with (tmp_path / "pyproject.toml").open("a") as pyproject:
        pyproject.write('\n[tool.uv.sources]\nhelper = { path = "packages/helper" }\n')

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.requirements[0].source_path == "packages/helper"


def test_relative_dependency_must_be_present_in_snapshot(tmp_path: Path) -> None:
    package = tmp_path / "dist" / "helper"
    package.mkdir(parents=True)
    (package / "pyproject.toml").write_text('[project]\nname="helper"\nversion="1"\n')
    _write_pyproject(tmp_path, ["helper"])
    with (tmp_path / "pyproject.toml").open("a") as pyproject:
        pyproject.write('\n[tool.uv.sources]\nhelper = { path = "dist/helper" }\n')

    with pytest.raises(ExportError) as raised:
        classify_dependencies(inventory_source(tmp_path))

    assert raised.value.code == "unsafe_dependency"


def test_uv_workspace_dependency_resolves_to_an_in_root_member(tmp_path: Path) -> None:
    package = tmp_path / "packages" / "helper"
    package.mkdir(parents=True)
    (package / "pyproject.toml").write_text('[project]\nname="helper"\nversion="1"\n')
    _write_pyproject(tmp_path, ["helper"])
    with (tmp_path / "pyproject.toml").open("a") as pyproject:
        pyproject.write(
            "\n[tool.uv.sources]\nhelper = { workspace = true }\n"
            '\n[tool.uv.workspace]\nmembers = ["packages/*"]\n'
        )

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.requirements[0].source_path == "packages/helper"


@pytest.mark.parametrize(
    "requirement",
    [
        "project @ git+https://example.com/project.git@main",
        "project @ https://user:token@example.com/project.whl#sha256=" + "a" * 64,
        "project @ https://example.com/project.whl",
    ],
)
def test_mutable_or_credential_bearing_direct_dependencies_are_rejected(
    tmp_path: Path, requirement: str
) -> None:
    _write_pyproject(tmp_path, [requirement])

    with pytest.raises(ExportError) as raised:
        classify_dependencies(inventory_source(tmp_path))

    assert raised.value.code == "unsafe_dependency"
    assert requirement not in str(raised.value)


@pytest.mark.parametrize(
    "requirement",
    [
        "project @ git+https://example.com/project.git@" + "a" * 40,
        "project @ https://example.com/project.whl#sha256=" + "a" * 64,
    ],
)
def test_immutable_direct_dependencies_are_preserved(
    tmp_path: Path, requirement: str
) -> None:
    _write_pyproject(tmp_path, [requirement])

    plan = classify_dependencies(inventory_source(tmp_path))

    assert plan.requirements[0].requirement == requirement


def test_incompatible_python_bounds_are_rejected(tmp_path: Path) -> None:
    _write_pyproject(tmp_path, [], requires_python=">=3.13")

    with pytest.raises(ExportError) as raised:
        classify_dependencies(inventory_source(tmp_path))

    assert raised.value.code == "incompatible_python"
