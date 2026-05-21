from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import Any, cast

from zenml.utils import yaml_utils

_VERSION_PATTERN = r"[0-9]+(?:\.[0-9]+)+"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@cache
def _read_file(relative_path: str) -> str:
    return (_repo_root() / relative_path).read_text()


def _read_dockerfile() -> str:
    return _read_file("docker/Dockerfile")


def _read_server_dev_dockerfile() -> str:
    return _read_file("docker/Dockerfile.server-dev")


def _read_pyproject() -> str:
    return _read_file("pyproject.toml")


@cache
def _read_toml(relative_path: str) -> dict[str, Any]:
    return tomllib.loads(_read_file(relative_path))


@cache
def _read_yaml(relative_path: str) -> dict[str, Any]:
    data = yaml_utils.read_yaml(str(_repo_root() / relative_path))
    assert isinstance(data, dict), f"{relative_path} should contain a YAML mapping."
    return data


def _extract_zenml_minimum(requirement: str) -> str:
    match = re.search(rf"\bzenml(?:\[[^\]]+\])?>=({_VERSION_PATTERN})", requirement)
    assert match, f"Could not extract ZenML minimum from {requirement!r}."
    return match.group(1)


def _expected_zenml_version() -> str:
    pyproject = _read_toml("pyproject.toml")
    dependencies = pyproject["project"]["dependencies"]  # type: ignore[index]
    zenml_requirements = [
        requirement
        for requirement in dependencies
        if isinstance(requirement, str) and requirement.startswith("zenml[")
    ]
    assert len(zenml_requirements) == 1
    return _extract_zenml_minimum(zenml_requirements[0])


# ---------------------------------------------------------------------------
# Packaging contract: no git direct refs remain
# ---------------------------------------------------------------------------


def test_pyproject_has_no_zenml_git_refs() -> None:
    """All ZenML dependencies should come from PyPI, not git refs."""
    pyproject = _read_pyproject()
    assert "git+https://github.com/zenml-io/zenml.git" not in pyproject, (
        "pyproject.toml still contains a ZenML git direct reference. "
        "Use a PyPI version spec (e.g., zenml>=0.94.1) instead."
    )


def test_pyproject_has_no_direct_reference_allowance() -> None:
    """The Hatch direct-reference escape hatch should be removed."""
    toml = tomllib.loads(_read_pyproject())
    hatch_meta = toml.get("tool", {}).get("hatch", {}).get("metadata", {})
    assert not hatch_meta.get("allow-direct-references", False), (
        "tool.hatch.metadata.allow-direct-references should be removed "
        "now that all dependencies come from PyPI."
    )


# ---------------------------------------------------------------------------
# Production Dockerfile contract
# ---------------------------------------------------------------------------


def test_dockerfile_uses_zenml_server_base_image() -> None:
    """The production image should be based on the official ZenML server image."""
    dockerfile = _read_dockerfile()
    assert re.search(
        r"FROM\s+zenmldocker/zenml-server:\$\{ZENML_SERVER_TAG\}\s+AS\s+server",
        dockerfile,
    ), (
        "Dockerfile should use FROM zenmldocker/zenml-server:"
        "${ZENML_SERVER_TAG} AS server"
    )


def _extract_zenml_server_tag(dockerfile: str) -> str:
    """Extract the ZENML_SERVER_TAG default from a Dockerfile."""
    match = re.search(r"^ARG ZENML_SERVER_TAG=(.+)$", dockerfile, re.MULTILINE)
    assert match, "Dockerfile should declare ARG ZENML_SERVER_TAG with a default."
    return match.group(1)


def test_dockerfile_pins_zenml_server_tag() -> None:
    """The ZenML server image tag should be explicitly pinned."""
    tag = _extract_zenml_server_tag(_read_dockerfile())
    assert tag != "latest", (
        "ZENML_SERVER_TAG should be pinned to a specific version, not 'latest'."
    )


def test_dockerfile_installs_kitaru() -> None:
    """The image should support both PyPI and local-source Kitaru installs."""
    dockerfile = _read_dockerfile()
    assert "COPY . /tmp/kitaru" in dockerfile
    assert "pip install" in dockerfile
    assert "KITARU_VERSION" in dockerfile


def test_dockerfile_copies_packaged_kitaru_ui() -> None:
    """The image should copy UI files from the installed Kitaru package."""
    dockerfile = _read_dockerfile()
    assert "KITARU_UI_DIST" in dockerfile
    assert "Path(kitaru.__file__).parent" in dockerfile
    assert '"_ui" / "dist"' in dockerfile
    assert "Kitaru package UI assets missing" in dockerfile
    assert 'cp -a "$KITARU_UI_DIST/." "$DASHBOARD_DIR/"' in dockerfile


def test_dockerfile_does_not_download_kitaru_ui_release_assets() -> None:
    """Docker must not have a second hidden UI release download path."""
    dockerfile = _read_dockerfile()
    forbidden_markers = [
        "ARG KITARU_UI_TAG",
        "KITARU_UI_REPO_URL",
        "zenml-io/kitaru-ui",
        "kitaru-ui.tar.gz",
        "releases/latest/download",
        "releases/download",
        "curl ",
        "sha256sum",
    ]
    for marker in forbidden_markers:
        assert marker not in dockerfile, (
            f"Dockerfile still contains UI download marker {marker!r}. "
            "Bundle UI into the Kitaru package before Docker builds instead."
        )


def test_dockerfile_configures_workload_manager_for_deployments() -> None:
    """Official server image must keep workload-manager support enabled."""
    dockerfile = _read_dockerfile()
    assert (
        "ZENML_SERVER_WORKLOAD_MANAGER_IMPLEMENTATION_SOURCE="
        "zenml.zen_server.pipeline_execution.in_memory_workload_manager.InMemoryWorkloadManager"
    ) in dockerfile


def test_dockerfile_verifies_dashboard_sentinel() -> None:
    """The image build should fail if index.html is missing."""
    dockerfile = _read_dockerfile()
    assert "zen_server/dashboard" in dockerfile
    assert "index.html" in dockerfile


def test_dockerfile_has_no_legacy_git_bundling() -> None:
    """The old git-clone + install-dashboard.sh flow should be gone."""
    dockerfile = _read_dockerfile()
    for legacy_marker in [
        "ZENML_GIT_REF",
        "git clone",
        "install-dashboard.sh",
        "ZENML_SERVER_EXTRAS",
        "ZENML_DASHBOARD_TAG",
    ]:
        assert legacy_marker not in dockerfile, (
            f"Dockerfile still contains legacy marker '{legacy_marker}'. "
            "The production image should use the ZenML server base image."
        )


# ---------------------------------------------------------------------------
# Server-dev Dockerfile contract
# ---------------------------------------------------------------------------


def test_server_dev_dockerfile_exists() -> None:
    """A separate server-dev Dockerfile should exist for local UI testing."""
    assert (_repo_root() / "docker" / "Dockerfile.server-dev").is_file()


def test_server_dev_dockerfile_uses_same_base() -> None:
    """The server-dev image should use the same ZenML server base."""
    dockerfile = _read_server_dev_dockerfile()
    assert "zenmldocker/zenml-server" in dockerfile


def test_dockerfiles_use_same_zenml_server_tag() -> None:
    """Both server Dockerfiles should pin the same ZenML server version."""
    prod_tag = _extract_zenml_server_tag(_read_dockerfile())
    dev_tag = _extract_zenml_server_tag(_read_server_dev_dockerfile())
    assert prod_tag == dev_tag, (
        f"Dockerfile ({prod_tag}) and Dockerfile.server-dev ({dev_tag}) "
        "have different ZENML_SERVER_TAG defaults — they must stay aligned."
    )


def _extract_just_assignment(name: str) -> str:
    match = re.search(
        rf"^{re.escape(name)}\s*:=\s*\"([^\"]+)\"$",
        _read_file("Justfile"),
        re.MULTILINE,
    )
    assert match, f"Justfile should assign {name}."
    return match.group(1)


def _find_yaml_values(data: object, name: str) -> list[str]:
    if isinstance(data, Mapping):
        mapping = cast(Mapping[str, object], data)
        values: list[str] = []
        value = mapping.get(name)
        if isinstance(value, str):
            values.append(value)
        for nested_value in mapping.values():
            values.extend(_find_yaml_values(nested_value, name))
        return values
    if isinstance(data, list):
        values = []
        for item in data:
            values.extend(_find_yaml_values(item, name))
        return values
    return []


def _extract_workflow_env_value(relative_path: str, name: str) -> str:
    values = _find_yaml_values(_read_yaml(relative_path), name)
    assert len(values) == 1, f"{relative_path} should set env {name} once."
    return values[0]


def _extract_release_build_arg(name: str) -> str:
    match = re.search(
        rf"^\s+{re.escape(name)}=({_VERSION_PATTERN})\s*$",
        _read_file(".github/workflows/release.yml"),
        re.MULTILINE,
    )
    assert match, f"release.yml should pass build arg {name}."
    return match.group(1)


def _extract_helm_dependency_version(name: str) -> str:
    chart = _read_yaml("helm/Chart.yaml")
    dependencies = chart.get("dependencies")
    assert isinstance(dependencies, list), (
        "helm/Chart.yaml should declare dependencies."
    )
    matches = [
        dependency
        for dependency in dependencies
        if isinstance(dependency, dict) and dependency.get("name") == name
    ]
    assert len(matches) == 1, f"helm/Chart.yaml should declare dependency {name}."
    version = matches[0].get("version")
    assert isinstance(version, str), f"Helm dependency {name} should have a version."
    return version.strip('"')


def _lock_package(name: str) -> dict[str, Any]:
    lock = _read_toml("uv.lock")
    packages = lock["package"]  # type: ignore[index]
    matches = [
        package
        for package in packages
        if isinstance(package, dict) and package.get("name") == name
    ]
    assert len(matches) == 1, f"uv.lock should contain exactly one {name} package."
    return matches[0]


def _zenml_lock_specifiers(entries: object) -> list[str]:
    assert isinstance(entries, list)
    specifiers: list[str] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        mapping = cast(Mapping[str, object], entry)
        specifier = mapping.get("specifier")
        if mapping.get("name") == "zenml" and isinstance(specifier, str):
            specifiers.append(specifier)
    return specifiers


def test_zenml_python_dependency_surfaces_are_aligned() -> None:
    """Python dependency specs and the lockfile should move together."""
    expected = _expected_zenml_version()
    pyproject = _read_toml("pyproject.toml")

    project_dependencies = pyproject["project"]["dependencies"]  # type: ignore[index]
    local_extra = pyproject["project"]["optional-dependencies"]["local"]  # type: ignore[index]
    dev_dependencies = pyproject["dependency-groups"]["dev"]  # type: ignore[index]

    requirements = [
        next(req for req in project_dependencies if req.startswith("zenml[local]")),
        next(req for req in local_extra if req.startswith("zenml[server]")),
        next(req for req in dev_dependencies if req.startswith("zenml[server]")),
    ]
    assert [_extract_zenml_minimum(req) for req in requirements] == [
        expected,
        expected,
        expected,
    ]

    kitaru_package = _lock_package("kitaru")
    metadata = kitaru_package["metadata"]  # type: ignore[index]
    lock_specs = [
        *_zenml_lock_specifiers(metadata["requires-dist"]),  # type: ignore[index]
        *_zenml_lock_specifiers(
            metadata["requires-dev"]["dev"]  # type: ignore[index]
        ),
    ]
    assert lock_specs == [f">={expected}", f">={expected}", f">={expected}"]
    assert _lock_package("zenml")["version"] == expected


def test_zenml_server_version_surfaces_are_aligned() -> None:
    """Server image, workflow, Justfile, and Helm pins should match Python."""
    expected = _expected_zenml_version()
    surfaces = {
        "docker/Dockerfile": _extract_zenml_server_tag(_read_dockerfile()),
        "docker/Dockerfile.server-dev": _extract_zenml_server_tag(
            _read_server_dev_dockerfile()
        ),
        "ci.yml docker-smoke env": _extract_workflow_env_value(
            ".github/workflows/ci.yml",
            "ZENML_SERVER_TAG",
        ),
        "release.yml Docker build arg": _extract_release_build_arg("ZENML_SERVER_TAG"),
        "Justfile": _extract_just_assignment("ZENML_SERVER_TAG"),
        "helm/Chart.yaml": _extract_helm_dependency_version("zenml"),
    }
    assert surfaces == {name: expected for name in surfaces}


def test_server_dev_dockerfile_copies_local_ui_dist() -> None:
    """The server-dev image should copy local UI dist, not download from GitHub."""
    dockerfile = _read_server_dev_dockerfile()
    assert "docker/kitaru-ui-dist/" in dockerfile
    assert "kitaru-ui.tar.gz" not in dockerfile


# ---------------------------------------------------------------------------
# Flow-execution Dockerfile contract
# ---------------------------------------------------------------------------


def _read_dev_dockerfile() -> str:
    """Read the flow-execution image Dockerfile (not the dev *server*)."""
    return _read_file("docker/Dockerfile.dev")


def _extract_dockerfile_dev_zenml_minimums() -> list[str]:
    """Extract executable ZenML lower bounds from the flow-execution image."""
    executable_lines = "\n".join(
        line
        for line in _read_dev_dockerfile().splitlines()
        if not line.lstrip().startswith("#")
    )
    return re.findall(
        rf"\bzenml(?:\[[^\]]+\])?>=({_VERSION_PATTERN})",
        executable_lines,
    )


def test_dockerfile_dev_has_no_git_refs() -> None:
    """Dockerfile.dev should install ZenML from PyPI, not git refs."""
    dockerfile = _read_dev_dockerfile()
    for marker in ["git+https://", "git clone", "@develop", "@main"]:
        assert marker not in dockerfile, (
            f"Dockerfile.dev contains git ref marker '{marker}'. "
            "Use PyPI version specs instead."
        )


def test_dockerfile_dev_zenml_minimum_matches_package_contract() -> None:
    """Dockerfile.dev may use >=, but its floor must match pyproject."""
    expected = _expected_zenml_version()
    minimums = _extract_dockerfile_dev_zenml_minimums()
    assert minimums, "Dockerfile.dev should contain at least one ZenML minimum."
    assert minimums == [expected for _ in minimums]


# ---------------------------------------------------------------------------
# Cross-file consistency
# ---------------------------------------------------------------------------


def test_dockerignore_allows_generated_package_ui() -> None:
    """Source Docker builds must include generated package UI but not local caches."""
    dockerignore = _read_file(".dockerignore")
    assert "dist/" in dockerignore
    assert "!src/kitaru/_ui/dist/" in dockerignore
    assert "!src/kitaru/_ui/dist/**" in dockerignore
    assert "!src/kitaru/_ui/bundle_manifest.json" in dockerignore
    assert ".kitaru-ui-bundles/" in dockerignore


def test_just_server_image_bundles_ui_before_docker_build() -> None:
    """Local source server builds should prepare package UI before Docker runs."""
    justfile = _read_file("Justfile")
    server_image_recipe = justfile.split("\nserver-image:", maxsplit=1)[1].split(
        "\n# Build and push production server image",
        maxsplit=1,
    )[0]
    assert "bash scripts/download-ui.sh" in server_image_recipe
    assert "--build-arg KITARU_UI_TAG" not in server_image_recipe


def test_server_dockerfiles_switch_to_root_for_build() -> None:
    """Server Dockerfiles must switch to root before COPY/RUN build steps.

    The base image runs as non-root user "zenml". Without USER root,
    COPY creates root-owned files that subsequent RUN commands (as zenml)
    cannot clean up.
    """
    for name, content in [
        ("Dockerfile", _read_dockerfile()),
        ("Dockerfile.server-dev", _read_server_dev_dockerfile()),
    ]:
        assert "USER root" in content, (
            f"{name} must contain 'USER root' to switch to root "
            "before package installation and file operations."
        )
        assert "USER zenml" in content, (
            f"{name} must contain 'USER zenml' to switch back to "
            "the non-root runtime user after build steps."
        )
