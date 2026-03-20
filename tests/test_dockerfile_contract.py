from __future__ import annotations

import re
import tomllib
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_dockerfile() -> str:
    return (_repo_root() / "docker" / "Dockerfile").read_text()


def _read_server_dev_dockerfile() -> str:
    return (_repo_root() / "docker" / "Dockerfile.server-dev").read_text()


def _read_pyproject() -> str:
    return (_repo_root() / "pyproject.toml").read_text()


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


def test_dockerfile_installs_kitaru_from_local_source() -> None:
    """The image should install Kitaru from the repo source."""
    dockerfile = _read_dockerfile()
    assert "COPY . /tmp/kitaru" in dockerfile
    assert "pip install" in dockerfile


def test_dockerfile_downloads_kitaru_ui() -> None:
    """The image should download the Kitaru UI release archive."""
    dockerfile = _read_dockerfile()
    assert "kitaru-ui.tar.gz" in dockerfile
    assert "sha256sum" in dockerfile or "sha256" in dockerfile


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
    return (_repo_root() / "docker" / "Dockerfile.dev").read_text()


def test_dockerfile_dev_has_no_git_refs() -> None:
    """Dockerfile.dev should install ZenML from PyPI, not git refs."""
    dockerfile = _read_dev_dockerfile()
    for marker in ["git+https://", "git clone", "@develop", "@main"]:
        assert marker not in dockerfile, (
            f"Dockerfile.dev contains git ref marker '{marker}'. "
            "Use PyPI version specs instead."
        )


# ---------------------------------------------------------------------------
# Cross-file consistency
# ---------------------------------------------------------------------------


def test_dockerfile_uses_curl_fail_flag() -> None:
    """curl must use --fail (-f) so HTTP errors are not silently ignored."""
    dockerfile = _read_dockerfile()
    for line in dockerfile.splitlines():
        if "curl " in line and "-o " in line:
            assert re.search(r"-[a-zA-Z]*f", line) or "--fail" in line, (
                f"curl download missing --fail flag: {line.strip()}"
            )


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
