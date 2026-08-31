import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.version import Version
from scripts.release_units import (
    ReleaseInventoryError,
    build_plugin_matrix,
    default_requirements,
    format_inventory,
    load_inventory,
    parse_package_tag,
    prepare_core_development_reset,
    propose_core_version,
    validate_canonical_version,
    validate_version,
)

REPO_ROOT = Path(__file__).parents[2]
INVENTORY_PATH = REPO_ROOT / "release" / "release-units.toml"

EXPECTED_UNITS = {
    "kitaru": "kitaru",
    "braintrust": "kitaru-braintrust",
    "braintrust-importer": "kitaru-braintrust-importer",
    "evaluator": "kitaru-evaluator",
    "jsonl-importer": "kitaru-jsonl-importer",
    "langfuse": "kitaru-langfuse",
    "langfuse-importer": "kitaru-langfuse-importer",
    "langgraph": "kitaru-langgraph",
    "logfire-importer": "kitaru-logfire-importer",
    "langsmith-importer": "kitaru-langsmith-importer",
    "openai-agents": "kitaru-openai-agents",
    "phoenix-importer": "kitaru-phoenix-importer",
    "pydantic-ai": "kitaru-pydantic-ai",
}

EXPECTED_DEFAULT_DISTRIBUTIONS = {
    "kitaru-braintrust-importer",
    "kitaru-evaluator",
    "kitaru-jsonl-importer",
    "kitaru-langfuse-importer",
    "kitaru-logfire-importer",
    "kitaru-langsmith-importer",
    "kitaru-phoenix-importer",
}


@pytest.fixture
def release_repo(tmp_path: Path) -> Path:
    for relative_path in (
        "pyproject.toml",
        "CHANGELOG.md",
        "openapi/openapi.json",
        "uv.lock",
        "release/release-units.toml",
        "plugins/uv.lock",
        "src/kitaru/server/api/bootstrap.py",
    ):
        source = REPO_ROOT / relative_path
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)

    for project in (REPO_ROOT / "plugins" / "packages").iterdir():
        for filename in ("pyproject.toml", "README.md", "CHANGELOG.md"):
            source = project / filename
            relative_path = source.relative_to(REPO_ROOT)
            destination = tmp_path / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)

    return tmp_path


@pytest.fixture(params=["0.22.3", "1.4.2"])
def core_release_repo(
    tmp_path: Path, request: pytest.FixtureRequest
) -> tuple[Path, str]:
    """Create stable release files independently of the checkout's release state."""
    version = str(request.param)
    documents = {
        "pyproject.toml": (
            f'[project]\nname = "kitaru"\nversion = "{version}"\n'
            f'description = "Release {version}"\n'
        ),
        "CHANGELOG.md": (
            f"# Changelog\n\n## [{version}]\n\n- Current release.\n\n"
            "## [0.1.0]\n\n- Previous release.\n"
        ),
        "openapi/openapi.json": json.dumps(
            {
                "openapi": "3.1.0",
                "info": {"title": "Kitaru", "version": version},
                "paths": {},
                "components": {"schemas": {"Example": {"example": version}}},
            },
            indent=2,
        )
        + "\n",
    }
    for lock_path, source in (("uv.lock", "."), ("plugins/uv.lock", "../")):
        documents[lock_path] = (
            'version = 1\n\n[[package]]\nname = "kitaru"\n'
            f'version = "{version}"\nsource = {{ editable = "{source}" }}\n'
            '\n[[package]]\nname = "example-plugin"\n'
            f'version = "{version}"\n'
            'source = { registry = "https://pypi.org/simple" }\n'
        )
    for relative_path, content in documents.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return tmp_path, version


def test_inventory_describes_core_and_twelve_plugin_distributions() -> None:
    inventory = load_inventory()

    assert {unit.slug: unit.distribution for unit in inventory.units} == EXPECTED_UNITS
    assert {
        unit.distribution for unit in inventory.units if unit.default_catalog
    } == EXPECTED_DEFAULT_DISTRIBUTIONS
    assert all(unit.registry == "pypi" for unit in inventory.units)
    assert all(unit.path != "docs" for unit in inventory.units)
    assert all(not unit.path.startswith("packages/") for unit in inventory.units)
    assert all((REPO_ROOT / unit.changelog).is_file() for unit in inventory.units)
    assert all((REPO_ROOT / unit.lock_source).is_file() for unit in inventory.units)
    assert len({unit.release_label for unit in inventory.units}) == len(inventory.units)
    assert len({unit.maintenance_branch for unit in inventory.units}) == len(
        inventory.units
    )
    assert all(unit.impact_paths for unit in inventory.units)


def test_default_requirements_are_derived_from_release_units() -> None:
    assert set(default_requirements(load_inventory()).values()) == {
        "kitaru-braintrust-importer==0.1.0",
        "kitaru-evaluator==0.1.2",
        "kitaru-jsonl-importer==0.1.0",
        "kitaru-langfuse-importer==0.1.1",
        "kitaru-langsmith-importer==0.1.0",
        "kitaru-logfire-importer==0.1.1",
        "kitaru-phoenix-importer==0.1.0",
    }


def test_inventory_versions_and_tags_match_project_manifests() -> None:
    inventory = load_inventory()

    for unit in inventory.units:
        assert unit.tag == f"python/{unit.distribution}/v{unit.version}"
        version = Version(unit.version)
        assert unit.maintenance_branch.endswith(f"/{version.major}.{version.minor}")
        if Version(unit.version).local is not None:
            with pytest.raises(ReleaseInventoryError, match="local segment"):
                parse_package_tag(unit.tag, inventory)
            continue
        assert parse_package_tag(unit.tag, inventory) == unit


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


@pytest.mark.parametrize("version", ["0.22.2+dev", "0.22.0rc1", "1.0.dev1"])
def test_manifest_versions_keep_their_local_segment(version: str) -> None:
    assert validate_canonical_version(version) == version


def test_noncanonical_manifest_versions_are_rejected() -> None:
    with pytest.raises(ReleaseInventoryError, match="canonical PEP 440"):
        validate_canonical_version("v0.22.0")


def test_local_package_tags_are_rejected_for_pypi() -> None:
    with pytest.raises(ReleaseInventoryError, match="local segment"):
        parse_package_tag("python/kitaru/v0.22.2+dev", load_inventory())


@pytest.mark.parametrize(
    ("latest_version", "labels", "expected"),
    [
        ("0.22.2", [], "0.22.3"),
        ("0.22.2", ["Breaking Change"], "0.23.0"),
        ("1.4.2", ["enhancement"], "1.4.3"),
        ("1.4.2", ["Breaking Change"], "2.0.0"),
    ],
)
def test_core_version_proposal_follows_release_labels(
    latest_version: str, labels: list[str], expected: str
) -> None:
    assert propose_core_version(latest_version, labels) == expected


@pytest.mark.parametrize("version", ["0.22", "0.23.0rc1", "0.23.0.dev1", "0.22.3+dev"])
def test_core_version_proposal_requires_a_stable_semantic_version(
    version: str,
) -> None:
    with pytest.raises(ReleaseInventoryError, match="latest stable core version"):
        propose_core_version(version, [])


def test_core_development_reset_updates_only_release_state(
    core_release_repo: tuple[Path, str],
) -> None:
    release_repo, release_version = core_release_repo
    development_version = f"{release_version}+dev"
    project = release_repo / "pyproject.toml"
    changelog = release_repo / "CHANGELOG.md"
    openapi = release_repo / "openapi" / "openapi.json"
    root_lock = release_repo / "uv.lock"
    plugin_lock = release_repo / "plugins" / "uv.lock"

    originals = {
        path: path.read_text()
        for path in (project, openapi, root_lock, plugin_lock, changelog)
    }

    assert (
        prepare_core_development_reset(release_version, release_repo)
        == development_version
    )
    assert project.read_text() == originals[project].replace(
        f'version = "{release_version}"', f'version = "{development_version}"', 1
    )
    assert openapi.read_text() == originals[openapi].replace(
        f'"version": "{release_version}"', f'"version": "{development_version}"', 1
    )
    for lock in (root_lock, plugin_lock):
        assert lock.read_text() == originals[lock].replace(
            f'name = "kitaru"\nversion = "{release_version}"',
            f'name = "kitaru"\nversion = "{development_version}"',
            1,
        )
    release_heading = f"## [{release_version}]"
    assert changelog.read_text() == originals[changelog].replace(
        release_heading, f"## [Unreleased]\n\n{release_heading}", 1
    )


@pytest.mark.parametrize("version", ["0.23.0rc1", "0.23.0+dev", "1.0.post1"])
def test_core_development_reset_requires_a_stable_release(
    tmp_path: Path, version: str
) -> None:
    with pytest.raises(ReleaseInventoryError, match=r"stable X\.Y\.Z"):
        prepare_core_development_reset(version, tmp_path)


def test_core_development_reset_fails_before_partial_writes(
    core_release_repo: tuple[Path, str],
) -> None:
    release_repo, release_version = core_release_repo
    changelog = release_repo / "CHANGELOG.md"
    release_heading = f"## [{release_version}]"
    changelog.write_text(
        changelog.read_text().replace(
            release_heading, f"## [Unreleased]\n\n{release_heading}", 1
        )
    )
    originals = {
        path: path.read_bytes() for path in release_repo.rglob("*") if path.is_file()
    }

    with pytest.raises(ReleaseInventoryError, match="already contains"):
        prepare_core_development_reset(release_version, release_repo)

    assert {path: path.read_bytes() for path in originals} == originals


@pytest.mark.parametrize(
    ("relative_path", "error"),
    [
        ("pyproject.toml", "project"),
        ("openapi/openapi.json", "OpenAPI"),
        ("uv.lock", "root lock"),
        ("plugins/uv.lock", "plugin lock"),
    ],
)
def test_core_development_reset_rejects_mismatched_versions_without_writes(
    core_release_repo: tuple[Path, str], relative_path: str, error: str
) -> None:
    release_repo, release_version = core_release_repo
    path = release_repo / relative_path
    path.write_text(path.read_text().replace(release_version, f"{release_version}+dev"))
    originals = {
        path: path.read_bytes() for path in release_repo.rglob("*") if path.is_file()
    }

    with pytest.raises(
        ReleaseInventoryError, match=f"{error} must contain exactly one"
    ):
        prepare_core_development_reset(release_version, release_repo)

    assert {path: path.read_bytes() for path in originals} == originals


def test_core_release_publishes_deployables_without_waiting_for_plugins() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    plugin_workflow = (
        REPO_ROOT / ".github" / "workflows" / "release-plugins.yml"
    ).read_text()

    assert "workflow_run:" not in workflow
    assert "python/kitaru/v*" in workflow
    assert "bundle/kitaru/v*" not in workflow
    assert "publish-deployables:" in workflow
    assert 'bundle_version="${BASH_REMATCH[1]}-rc.${BASH_REMATCH[2]}"' in workflow
    assert "gh release upload" not in workflow
    assert "create-release:" in workflow
    assert (
        "needs: [build, publish-python, publish-deployables, promote-latest]"
        in workflow
    )
    assert "promote-latest:" in workflow
    assert "publish-deployables:" not in plugin_workflow
    assert "!python/kitaru/**" in plugin_workflow


def test_stable_core_release_creates_a_draft_development_reset_pr() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    reset_job = workflow.split("\n  create-development-reset-pr:\n", maxsplit=1)[1]

    assert "github.event_name == 'push'" in reset_job
    assert "needs.build.outputs.is-prerelease == 'false'" in reset_job
    assert "needs: [build, create-release, advance-maintenance-branch]" in reset_job
    assert "secrets.RELEASE_GIT_TOKEN" in reset_job
    assert "prepare-core-development-reset" in reset_job
    assert "uv lock --check" in reset_job
    assert "uv lock --project plugins --check" in reset_job
    assert "ref: ${{ github.sha }}" in reset_job
    assert "ref: develop" not in reset_job
    assert "--base develop" in reset_job
    assert "--draft" in reset_job
    assert "main` contains release commit" in reset_job
    assert (
        "git add pyproject.toml uv.lock plugins/uv.lock openapi/openapi.json "
        "CHANGELOG.md" in reset_job
    )


def test_managed_image_failure_does_not_block_the_release() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    managed_start = workflow.index("      - name: Publish managed image\n")
    report_start = workflow.index("      - name: Report managed image failure\n")
    helm_start = workflow.index("      - name: Configure AWS credentials for Helm\n")
    managed_step = workflow[managed_start:report_start]
    report_step = workflow[report_start:helm_start]

    assert "        id: publish-managed-image\n" in managed_step
    assert "        continue-on-error: true\n" in managed_step
    assert workflow.count("continue-on-error: true") == 1
    assert "if: steps.publish-managed-image.outcome == 'failure'" in report_step
    assert "::warning::Managed image publication failed" in report_step
    assert "GITHUB_STEP_SUMMARY" in report_step


def test_release_images_use_the_pypi_propagation_retry() -> None:
    for dockerfile in (
        "docker/release-client.Dockerfile",
        "docker/release-server.Dockerfile",
        "docker/release-worker.Dockerfile",
    ):
        content = (REPO_ROOT / dockerfile).read_text()
        assert "COPY --chown=$USERNAME:$USER_GID " in content
        assert "docker/install-release-wheel.sh ./" in content
        assert "sh ./install-release-wheel.sh" in content


def test_release_wheel_install_retries_until_the_package_is_available(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    attempts = tmp_path / "attempts"
    (fake_bin / "uv").write_text(
        "#!/bin/sh\n"
        'attempt=$(($(cat "$ATTEMPTS" 2>/dev/null || echo 0) + 1))\n'
        'printf "%s\\n" "$attempt" > "$ATTEMPTS"\n'
        'printf "%s\\n" "$*" >> "$UV_ARGS"\n'
        '[ "$attempt" -ge 3 ]\n'
    )
    (fake_bin / "sleep").write_text('#!/bin/sh\nprintf "%s\\n" "$1" >> "$SLEEPS"\n')
    (fake_bin / "uv").chmod(0o755)
    (fake_bin / "sleep").chmod(0o755)
    env = {
        **os.environ,
        "ATTEMPTS": str(attempts),
        "KITARU_INSTALL_RETRY_DELAY": "0",
        "KITARU_VERSION": "0.22.0rc2",
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "SLEEPS": str(tmp_path / "sleeps"),
        "UV_ARGS": str(tmp_path / "uv-args"),
    }

    result = subprocess.run(
        ["sh", str(REPO_ROOT / "docker" / "install-release-wheel.sh")],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert attempts.read_text() == "3\n"
    assert (tmp_path / "sleeps").read_text() == "0\n0\n"
    uv_args = (tmp_path / "uv-args").read_text().splitlines()
    assert len(uv_args) == 3
    assert all("--refresh-package kitaru" in args for args in uv_args)
    assert all("kitaru==0.22.0rc2" in args for args in uv_args)


def test_release_wheel_install_fails_after_the_attempt_limit(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    attempts = tmp_path / "attempts"
    (fake_bin / "uv").write_text(
        "#!/bin/sh\n"
        'attempt=$(($(cat "$ATTEMPTS" 2>/dev/null || echo 0) + 1))\n'
        'printf "%s\\n" "$attempt" > "$ATTEMPTS"\n'
        "exit 1\n"
    )
    (fake_bin / "sleep").write_text("#!/bin/sh\nexit 0\n")
    (fake_bin / "uv").chmod(0o755)
    (fake_bin / "sleep").chmod(0o755)
    env = {
        **os.environ,
        "ATTEMPTS": str(attempts),
        "KITARU_INSTALL_ATTEMPTS": "3",
        "KITARU_INSTALL_RETRY_DELAY": "0",
        "KITARU_VERSION": "0.22.0rc2",
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }

    result = subprocess.run(
        ["sh", str(REPO_ROOT / "docker" / "install-release-wheel.sh")],
        env=env,
        check=False,
    )

    assert result.returncode == 1
    assert attempts.read_text() == "3\n"


@pytest.mark.parametrize(
    ("tag", "message"),
    [
        ("kitaru-v0.21.0", "package tag"),
        ("python/unknown/v0.1.0", "unknown distribution"),
        ("python/kitaru/v0.22.0-rc.1", "canonical PEP 440"),
        ("python/kitaru/v0.22.3", "does not match manifest version"),
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


def test_inventory_rejects_an_invalid_maintenance_branch_prefix(
    release_repo: Path,
) -> None:
    inventory_path = release_repo / "release" / "release-units.toml"
    inventory_path.write_text(
        inventory_path.read_text().replace(
            'maintenance-branch-prefix = "release/langfuse/"',
            'maintenance-branch-prefix = "../langfuse/"',
        )
    )

    with pytest.raises(
        ReleaseInventoryError, match="invalid maintenance branch prefix"
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


def test_inventory_rejects_plugin_metadata_without_a_readme(
    release_repo: Path,
) -> None:
    manifest = (
        release_repo / "plugins" / "packages" / "langfuse-importer" / "pyproject.toml"
    )
    manifest.write_text(manifest.read_text().replace('readme = "README.md"\n', ""))

    with pytest.raises(ReleaseInventoryError, match="readme must be"):
        load_inventory(release_repo)


def test_inventory_rejects_plugin_metadata_with_a_missing_readme_file(
    release_repo: Path,
) -> None:
    readme = release_repo / "plugins" / "packages" / "langfuse-importer" / "README.md"
    readme.unlink()

    with pytest.raises(ReleaseInventoryError, match="non-empty file"):
        load_inventory(release_repo)


def test_inventory_rejects_plugin_metadata_without_project_urls(
    release_repo: Path,
) -> None:
    manifest = (
        release_repo / "plugins" / "packages" / "langfuse-importer" / "pyproject.toml"
    )
    manifest.write_text(
        manifest.read_text().replace(
            'Documentation = "https://docs.zenml.io/kitaru/guides/import-langfuse-traces"\n',
            "",
        )
    )

    with pytest.raises(
        ReleaseInventoryError, match="missing project URL: Documentation"
    ):
        load_inventory(release_repo)


def test_inventory_rejects_plugin_metadata_without_keywords(
    release_repo: Path,
) -> None:
    manifest = (
        release_repo / "plugins" / "packages" / "langfuse-importer" / "pyproject.toml"
    )
    manifest.write_text(
        manifest.read_text().replace(
            'keywords = ["ai-agents", "kitaru", "langfuse", "observability", "traces"]',
            "keywords = []",
        )
    )

    with pytest.raises(ReleaseInventoryError, match="keywords must not be empty"):
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


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            'requirement="kitaru-langfuse-importer==0.1.1"',
            'requirement="kitaru-langfuse-importer==0.1.0"',
        ),
        ('display_version="0.1.1"', 'display_version="0.1.0"'),
    ],
)
def test_inventory_rejects_stale_server_default_versions(
    release_repo: Path, old: str, new: str
) -> None:
    bootstrap = release_repo / "src" / "kitaru" / "server" / "api" / "bootstrap.py"
    bootstrap.write_text(bootstrap.read_text().replace(old, new, 1))

    with pytest.raises(
        ReleaseInventoryError,
        match=r"server default requirement and display version must match 0\.1\.1",
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


def test_plugin_matrix_is_generated_from_the_twelve_plugin_units() -> None:
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


def test_plugin_release_workflow_resolves_plugin_tags_from_the_inventory() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release-plugins.yml").read_text()

    assert "python/**" in workflow
    assert "!python/kitaru/**" in workflow
    assert "scripts/release_units.py resolve --tag" in workflow
    assert "scripts/release_ui.py --version" not in workflow
    assert "uv version" not in workflow
    assert "name: pypi-${{ needs.build.outputs.distribution }}" in workflow


def test_python_release_workflows_accept_develop_and_maintenance_sources() -> None:
    workflows = [
        (REPO_ROOT / ".github" / "workflows" / name).read_text()
        for name in ("release.yml", "release-plugins.yml")
    ]

    for workflow in workflows:
        build_job = workflow.split("\n  publish-python:\n", maxsplit=1)[0]
        assert "git fetch origin develop" in build_job
        assert 'git merge-base --is-ancestor "$release_sha" origin/develop' in build_job
        assert "origin/$MAINTENANCE_BRANCH" in build_job
        assert "git fetch origin main" not in build_job


def test_stable_python_releases_advance_maintenance_branches() -> None:
    core = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    plugins = (REPO_ROOT / ".github" / "workflows" / "release-plugins.yml").read_text()

    assert "  advance-maintenance-branch:\n" in core
    assert "  advance-maintenance-branch:\n" in plugins
    for workflow in (core, plugins):
        assert "needs.build.outputs.is-prerelease == 'false'" in workflow
        assert 'ref="refs/heads/$MAINTENANCE_BRANCH"' in workflow
        assert "-F force=false" in workflow
        assert "needs: [build, create-release]" in workflow


def test_core_release_workflow_leaves_main_promotion_to_the_release_owner() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
    branch_job = workflow.split("\n  advance-maintenance-branch:\n", maxsplit=1)[1]

    assert "git fetch origin main" not in branch_job
    assert "git/refs/heads/main" not in branch_job


def test_plugin_release_workflow_validates_wheel_metadata_before_publish() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release-plugins.yml").read_text()
    build_job = workflow.split("\n  publish-python:\n", maxsplit=1)[0]

    build_step = build_job.index("      - name: Build plugin\n")
    metadata_step = build_job.index("      - name: Validate distribution metadata\n")
    checksum_step = build_job.index("      - name: Record artifact checksums\n")

    assert build_step < metadata_step < checksum_step
    assert "scripts/smoke_plugin_artifacts.py" in build_job
    assert '--validate-wheel "$wheel"' in build_job
    assert '--distribution "$DISTRIBUTION"' in build_job
    assert '--version "$VERSION"' in build_job


def test_python_release_workflow_can_resume_after_partial_publication() -> None:
    workflows = [
        (REPO_ROOT / ".github" / "workflows" / name).read_text()
        for name in ("release.yml", "release-plugins.yml")
    ]

    for workflow in workflows:
        assert "skip-existing: true" in workflow
        assert "GH_REPO: ${{ github.repository }}" in workflow
        assert 'gh release view "$PACKAGE_TAG"' in workflow
        assert "already exists; leaving it unchanged" in workflow
        assert "gh release upload" not in workflow


def test_stable_github_releases_are_marked_latest() -> None:
    workflows = [
        (REPO_ROOT / ".github" / "workflows" / name).read_text()
        for name in ("release.yml", "release-plugins.yml", "release-typescript.yml")
    ]

    for workflow in workflows:
        assert "latest=(--latest=false)" in workflow
        assert "latest=(--latest)" in workflow
        assert '"${latest[@]}"' in workflow


def test_core_github_release_is_created_after_registry_publication() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert workflow.index("  create-release:\n") > workflow.index(
        "  publish-deployables:\n"
    )
    assert workflow.index("  create-release:\n") > workflow.index("  promote-latest:\n")
    assert "needs.publish-deployables.result == 'success'" in workflow
    assert "needs.promote-latest.result == 'success'" in workflow


def test_python_release_workflow_can_install_a_new_core_release() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert '--exclude-newer-package "kitaru=0 days"' in workflow


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


def test_ci_quickstart_example_job_enforces_the_walkthrough() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "\n  quickstart-example:\n" in workflow
    lint_job = workflow.split("\n  lint:\n", maxsplit=1)[1].split(
        "\n  typescript:\n", maxsplit=1
    )[0]
    example_job = workflow.split("\n  quickstart-example:\n", maxsplit=1)[1].split(
        "\n  links:\n", maxsplit=1
    )[0]

    assert "scripts/audit-example-coverage.py" in lint_job
    assert "name: Quickstart example end to end" in example_job
    assert "repository: zenml-io/kitaru-template" not in workflow
    assert (
        "working-directory: examples/python/pydantic_ai_ticket_resolver" in example_job
    )
    assert "Install current Kitaru artifacts into quickstart example" in example_job
    assert "plugins/candidate-wheels/kitaru-*.whl" in example_job
    assert "plugins/candidate-wheels/kitaru_pydantic_ai-*.whl" in example_job
    assert "plugins/candidate-wheels/kitaru_langfuse_importer-*.whl" in example_job
    assert "tests/test_contract.py" in example_job
    assert "tests/test_repository_contract.py" in example_job
    assert "uv run --no-sync python scripts/run_ci_e2e.py" in example_job


def test_each_unit_exposes_its_exact_release_critical_checks() -> None:
    inventory = load_inventory()

    assert "Quickstart example end to end" in inventory.common_checks
    assert "Public template against current Kitaru" not in inventory.common_checks
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
        (["validate"], "Validated 13 release units."),
        (
            [
                "propose-core-version",
                "--latest-version",
                "0.22.2",
                "--label",
                "Breaking Change",
            ],
            "0.23.0",
        ),
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
        (
            [
                "propose-core-version",
                "--latest-version",
                "0.22.2",
                "--label",
                "Breaking Change",
                "--format",
                "json",
            ],
            "proposed_version",
        ),
    ],
)
def test_cli_json_commands_succeed(arguments: list[str], expected_key: str) -> None:
    result = _run_cli(*arguments)

    assert result.returncode == 0
    assert json.loads(result.stdout)[expected_key]
    assert result.stderr == ""


def test_cli_resolves_the_current_package_tag() -> None:
    tag = next(
        unit.tag
        for unit in load_inventory().units
        if Version(unit.version).local is None
    )
    result = _run_cli("resolve", "--tag", tag, "--format", "json")

    assert result.returncode == 0
    assert json.loads(result.stdout)["unit"]["tag"] == tag
    assert result.stderr == ""


def test_cli_refuses_to_release_a_local_version_tag() -> None:
    result = _run_cli("resolve", "--tag", "python/kitaru/v0.22.2+dev")

    assert result.returncode == 2
    assert "local segment" in result.stderr


def test_cli_rejects_a_candidate_that_ignores_breaking_change_labels() -> None:
    result = _run_cli(
        "propose-core-version",
        "--latest-version",
        "0.22.2",
        "--label",
        "Breaking Change",
        "--candidate",
        "0.22.3",
    )

    assert result.returncode == 2
    assert "does not match required version 0.23.0" in result.stderr


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
