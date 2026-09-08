"""Validate and query Kitaru's Python release-unit inventory."""

import argparse
import ast
import json
import re
import sys
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_TAG_PATTERN = re.compile(
    r"python/(?P<distribution>[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?)/v(?P<version>[^/]+)"
)
SLUG_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")
MAINTENANCE_BRANCH_PREFIX_PATTERN = re.compile(
    r"release/[a-z0-9](?:[a-z0-9-]*[a-z0-9])?/"
)
REQUIRED_PLUGIN_PROJECT_URLS = frozenset(
    {"Homepage", "Documentation", "Repository", "Issues", "Changelog"}
)
BREAKING_CHANGE_LABEL = "Breaking Change"
PLUGIN_CI_SHARD_COUNT = 3


class ReleaseInventoryError(ValueError):
    """Raised when release-unit metadata is invalid or inconsistent."""


@dataclass(frozen=True, slots=True)
class ReleaseUnit:
    """One independently versioned Python distribution."""

    slug: str
    path: str
    distribution: str
    registry: str
    version_source: str
    changelog: str
    lock_source: str
    version: str
    default_catalog: bool
    release_label: str
    impact_paths: tuple[str, ...]
    tag_prefix: str
    maintenance_branch_prefix: str
    required_checks: frozenset[str]

    @property
    def tag(self) -> str:
        """Build the immutable package tag for the current manifest version."""
        return f"{self.tag_prefix}{self.version}"

    @property
    def maintenance_branch(self) -> str:
        """Build the stable major-minor maintenance branch for this version."""
        version = Version(self.version)
        return f"{self.maintenance_branch_prefix}{version.major}.{version.minor}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the unit using the stable public field names."""
        return {
            "slug": self.slug,
            "path": self.path,
            "distribution": self.distribution,
            "registry": self.registry,
            "version_source": self.version_source,
            "changelog": self.changelog,
            "lock_source": self.lock_source,
            "version": self.version,
            "default_catalog": self.default_catalog,
            "release_label": self.release_label,
            "impact_paths": list(self.impact_paths),
            "tag_prefix": self.tag_prefix,
            "tag": self.tag,
            "maintenance_branch_prefix": self.maintenance_branch_prefix,
            "maintenance_branch": self.maintenance_branch,
            "required_checks": sorted(self.required_checks),
        }


@dataclass(frozen=True, slots=True)
class ReleaseInventory:
    """Validated release-unit metadata for the repository."""

    schema_version: int
    common_checks: frozenset[str]
    units: tuple[ReleaseUnit, ...]

    @property
    def plugin_units(self) -> tuple[ReleaseUnit, ...]:
        """Return the independently packaged plugin units."""
        return tuple(unit for unit in self.units if unit.slug != "kitaru")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the inventory as a versioned automation contract."""
        return {
            "schema_version": self.schema_version,
            "common_checks": sorted(self.common_checks),
            "units": [unit.to_dict() for unit in self.units],
        }

    def to_json(self) -> str:
        """Serialize the inventory as deterministic compact JSON."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def validate_canonical_version(value: str) -> str:
    """Validate and return a canonical PEP 440 version."""
    try:
        version = Version(value)
    except InvalidVersion as error:
        raise ReleaseInventoryError(
            f"version must be canonical PEP 440: {value}"
        ) from error
    canonical = str(version)
    if canonical != value:
        raise ReleaseInventoryError(
            f"version must be canonical PEP 440: {value} normalizes to {canonical}"
        )
    return canonical


def validate_version(value: str) -> str:
    """Validate and return a canonical PEP 440 public version."""
    try:
        version = Version(value)
    except InvalidVersion as error:
        raise ReleaseInventoryError(
            f"version must be canonical PEP 440: {value}"
        ) from error
    if version.local is not None:
        raise ReleaseInventoryError(
            f"PyPI release versions must not contain a local segment: {value}"
        )
    canonical = str(version)
    if canonical != value:
        raise ReleaseInventoryError(
            f"version must be canonical PEP 440: {value} normalizes to {canonical}"
        )
    return canonical


def propose_core_version(latest_version: str, labels: Iterable[str]) -> str:
    """Propose the next stable core version from merged change labels."""
    latest = Version(validate_canonical_version(latest_version))
    label_set = set(labels)
    if (
        len(latest.release) != 3
        or latest.epoch != 0
        or latest.is_prerelease
        or latest.is_devrelease
        or latest.post is not None
        or latest.local is not None
    ):
        raise ReleaseInventoryError(
            "latest stable core version must use X.Y.Z without pre, dev, post, "
            f"epoch, or local segments: {latest_version}"
        )

    major, minor, patch = latest.release
    if BREAKING_CHANGE_LABEL in label_set:
        if major == 0:
            return f"0.{minor + 1}.0"
        return f"{major + 1}.0.0"
    return f"{major}.{minor}.{patch + 1}"


def prepare_core_development_reset(
    release_version: str, repo_root: Path = REPO_ROOT
) -> str:
    """Prepare the deterministic post-release core development reset."""
    version = Version(validate_canonical_version(release_version))
    if (
        len(version.release) != 3
        or version.epoch != 0
        or version.is_prerelease
        or version.is_devrelease
        or version.post is not None
        or version.local is not None
    ):
        raise ReleaseInventoryError(
            f"development reset requires a stable X.Y.Z core version: {release_version}"
        )

    development_version = f"{release_version}+dev"
    paths = {
        "project": repo_root / "pyproject.toml",
        "changelog": repo_root / "CHANGELOG.md",
        "OpenAPI": repo_root / "openapi" / "openapi.json",
        "root lock": repo_root / "uv.lock",
        "plugin lock": repo_root / "plugins" / "uv.lock",
    }
    try:
        documents = {name: path.read_text() for name, path in paths.items()}
    except FileNotFoundError as error:
        raise ReleaseInventoryError(
            f"missing release file: {error.filename}"
        ) from error

    replacements = {
        "project": (
            f'version = "{release_version}"',
            f'version = "{development_version}"',
        ),
        "OpenAPI": (
            '"info": {\n    "title": "Kitaru",\n'
            f'    "version": "{release_version}"\n  }}',
            '"info": {\n    "title": "Kitaru",\n'
            f'    "version": "{development_version}"\n  }}',
        ),
        "root lock": (
            f'name = "kitaru"\nversion = "{release_version}"\n'
            'source = { editable = "." }',
            f'name = "kitaru"\nversion = "{development_version}"\n'
            'source = { editable = "." }',
        ),
        "plugin lock": (
            f'name = "kitaru"\nversion = "{release_version}"\n'
            'source = { editable = "../" }',
            f'name = "kitaru"\nversion = "{development_version}"\n'
            'source = { editable = "../" }',
        ),
    }
    updated = dict(documents)
    for name, (current, replacement) in replacements.items():
        if documents[name].count(current) != 1:
            raise ReleaseInventoryError(
                f"{name} must contain exactly one core {release_version} entry"
            )
        updated[name] = documents[name].replace(current, replacement, 1)

    unreleased_heading = "## [Unreleased]"
    release_heading = f"## [{release_version}]"
    if documents["changelog"].count(release_heading) != 1:
        raise ReleaseInventoryError(
            f"changelog must contain exactly one {release_heading} heading"
        )
    current_release_offset = documents["changelog"].index(release_heading)
    if unreleased_heading in documents["changelog"][:current_release_offset]:
        raise ReleaseInventoryError(
            "changelog already contains an Unreleased section above the release"
        )
    updated["changelog"] = documents["changelog"].replace(
        release_heading, f"{unreleased_heading}\n\n{release_heading}", 1
    )

    for name, path in paths.items():
        path.write_text(updated[name])
    return development_version


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text())
    except FileNotFoundError as error:
        raise ReleaseInventoryError(f"missing TOML file: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ReleaseInventoryError(f"invalid TOML file {path}: {error}") from error


def _get_string(document: dict[str, Any], key: str, context: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ReleaseInventoryError(f"{context}: {key} must be a non-empty string")
    return value


def _get_string_list(document: dict[str, Any], key: str, context: str) -> list[str]:
    value = document.get(key)
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ReleaseInventoryError(f"{context}: {key} must be a list of strings")
    if len(value) != len(set(value)):
        raise ReleaseInventoryError(f"{context}: {key} contains duplicates")
    return value


def _resolve_repo_path(repo_root: Path, value: str, context: str) -> Path:
    relative_path = Path(value)
    if relative_path.is_absolute():
        raise ReleaseInventoryError(f"{context}: path must be repository-relative")
    root = repo_root.resolve()
    resolved = (root / relative_path).resolve()
    if not resolved.is_relative_to(root):
        raise ReleaseInventoryError(f"{context}: path escapes the repository")
    return resolved


def _parse_manifest(
    repo_root: Path, project_path: Path, version_source: str, context: str
) -> tuple[str, str, dict[str, Any]]:
    manifest_path = _resolve_repo_path(repo_root, version_source, context)
    if not manifest_path.is_relative_to(project_path):
        raise ReleaseInventoryError(f"{context}: version source is outside the project")
    manifest = _read_toml(manifest_path)
    project = manifest.get("project")
    if not isinstance(project, dict):
        raise ReleaseInventoryError(f"{context}: version source has no [project] table")
    name = _get_string(project, "name", context)
    version = validate_canonical_version(_get_string(project, "version", context))
    return name, version, project


def _validate_plugin_project_metadata(
    project_path: Path, project: dict[str, Any], context: str
) -> None:
    """Require the metadata rendered on each plugin's PyPI page."""
    _get_string(project, "description", context)
    if _get_string(project, "license", context) != "Apache-2.0":
        raise ReleaseInventoryError(f"{context}: license must be Apache-2.0")

    readme_value = _get_string(project, "readme", context)
    readme_path = Path(readme_value)
    if readme_path.is_absolute():
        raise ReleaseInventoryError(f"{context}: readme must be project-relative")
    resolved_readme = (project_path / readme_path).resolve()
    if not resolved_readme.is_relative_to(project_path.resolve()):
        raise ReleaseInventoryError(f"{context}: readme escapes the project")
    if not resolved_readme.is_file() or not resolved_readme.read_text().strip():
        raise ReleaseInventoryError(
            f"{context}: readme must reference a non-empty file"
        )

    authors = project.get("authors")
    if not isinstance(authors, list) or not authors:
        raise ReleaseInventoryError(f"{context}: authors must be a non-empty list")
    for author in authors:
        if not isinstance(author, dict):
            raise ReleaseInventoryError(f"{context}: each author must be a table")
        _get_string(author, "name", f"{context} author")
        _get_string(author, "email", f"{context} author")

    for key in ("classifiers", "keywords"):
        values = _get_string_list(project, key, context)
        if not values:
            raise ReleaseInventoryError(f"{context}: {key} must not be empty")
        if any(value != value.strip() for value in values):
            raise ReleaseInventoryError(
                f"{context}: {key} values must not contain outer whitespace"
            )

    urls = project.get("urls")
    if not isinstance(urls, dict):
        raise ReleaseInventoryError(f"{context}: [project.urls] must be a table")
    missing_urls = sorted(REQUIRED_PLUGIN_PROJECT_URLS - urls.keys())
    if missing_urls:
        raise ReleaseInventoryError(
            f"{context}: missing project URL: {missing_urls[0]}"
        )
    for label, value in urls.items():
        if not isinstance(label, str) or not label:
            raise ReleaseInventoryError(
                f"{context}: project URL labels must be non-empty strings"
            )
        if not isinstance(value, str) or not value.startswith("https://"):
            raise ReleaseInventoryError(
                f"{context}: project URL {label} must be an HTTPS URL"
            )


def _parse_requirement(value: str, context: str) -> Requirement:
    try:
        return Requirement(value)
    except InvalidRequirement as error:
        raise ReleaseInventoryError(
            f"{context}: invalid requirement {value}"
        ) from error


def _load_bootstrap_requirements(
    repo_root: Path,
) -> dict[str, set[tuple[str, str]]]:
    bootstrap_path = repo_root / "src" / "kitaru" / "server" / "api" / "bootstrap.py"
    try:
        module = ast.parse(bootstrap_path.read_text(), filename=str(bootstrap_path))
    except FileNotFoundError as error:
        raise ReleaseInventoryError("missing server plugin catalog") from error
    except SyntaxError as error:
        raise ReleaseInventoryError(
            f"invalid server plugin catalog: {error}"
        ) from error

    requirements: dict[str, set[tuple[str, str]]] = {}
    for node in ast.walk(module):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "DefaultPluginDefinition"
        ):
            continue
        requirement_keywords = [
            keyword for keyword in node.keywords if keyword.arg == "requirement"
        ]
        if len(requirement_keywords) != 1:
            raise ReleaseInventoryError(
                "server plugin catalog entries must declare one requirement"
            )
        requirement_value = requirement_keywords[0].value
        if not isinstance(requirement_value, ast.Constant) or not isinstance(
            requirement_value.value, str
        ):
            raise ReleaseInventoryError(
                "server plugin catalog requirements must be string literals"
            )
        requirement = _parse_requirement(
            requirement_value.value, "server plugin catalog"
        )
        display_version_keywords = [
            keyword for keyword in node.keywords if keyword.arg == "display_version"
        ]
        if len(display_version_keywords) != 1:
            raise ReleaseInventoryError(
                "server plugin catalog entries must declare one display version"
            )
        display_version_value = display_version_keywords[0].value
        if not isinstance(display_version_value, ast.Constant) or not isinstance(
            display_version_value.value, str
        ):
            raise ReleaseInventoryError(
                "server plugin catalog display versions must be string literals"
            )
        name = str(canonicalize_name(requirement.name))
        requirements.setdefault(name, set()).add(
            (str(requirement.specifier), display_version_value.value)
        )
    return requirements


def _validate_plugin_coverage(repo_root: Path, units: tuple[ReleaseUnit, ...]) -> None:
    packages_root = repo_root / "plugins" / "packages"
    actual = {
        manifest.parent.name for manifest in packages_root.glob("*/pyproject.toml")
    }
    expected = {unit.slug for unit in units if unit.slug != "kitaru"}
    unlisted = sorted(actual - expected)
    missing = sorted(expected - actual)
    if unlisted:
        raise ReleaseInventoryError(f"unlisted plugin project: {unlisted[0]}")
    if missing:
        raise ReleaseInventoryError(
            f"inventory plugin project is missing: {missing[0]}"
        )


def _validate_default_catalog(repo_root: Path, units: tuple[ReleaseUnit, ...]) -> None:
    inventory_defaults = {
        str(canonicalize_name(unit.distribution)): unit
        for unit in units
        if unit.default_catalog
    }
    bootstrap_defaults = _load_bootstrap_requirements(repo_root)
    if set(inventory_defaults) != set(bootstrap_defaults):
        raise ReleaseInventoryError("default catalog does not match inventory")

    for name, unit in inventory_defaults.items():
        expected = {(f"=={unit.version}", unit.version)}
        if bootstrap_defaults[name] != expected:
            raise ReleaseInventoryError(
                f"{name}: server default requirement and display version "
                f"must match {unit.version}"
            )


def default_requirements(inventory: ReleaseInventory) -> dict[str, str]:
    """Return exact requirements for packages in the server default catalog."""
    return {
        str(canonicalize_name(unit.distribution)): (
            f"{unit.distribution}=={unit.version}"
        )
        for unit in inventory.plugin_units
        if unit.default_catalog
    }


def load_inventory(
    repo_root: Path = REPO_ROOT, inventory_path: Path | None = None
) -> ReleaseInventory:
    """Load and validate the repository's release-unit inventory."""
    root = repo_root.resolve()
    path = inventory_path or root / "release" / "release-units.toml"
    document = _read_toml(path)
    schema_version = document.get("schema-version")
    if schema_version != 1:
        raise ReleaseInventoryError(
            f"unsupported release inventory schema version: {schema_version}"
        )
    checks_document = document.get("release-checks")
    if not isinstance(checks_document, dict):
        raise ReleaseInventoryError("release-checks must be a table")
    common_checks = frozenset(
        _get_string_list(checks_document, "common", "release-checks")
    )

    raw_units = document.get("units")
    if not isinstance(raw_units, list) or not raw_units:
        raise ReleaseInventoryError("units must be a non-empty array of tables")

    units: list[ReleaseUnit] = []
    seen_slugs: set[str] = set()
    seen_distributions: set[str] = set()
    seen_release_labels: set[str] = set()
    seen_tag_prefixes: set[str] = set()
    seen_maintenance_branch_prefixes: set[str] = set()
    for raw_unit in raw_units:
        if not isinstance(raw_unit, dict):
            raise ReleaseInventoryError("each release unit must be a table")
        slug = _get_string(raw_unit, "slug", "release unit")
        if slug in seen_slugs:
            raise ReleaseInventoryError(f"duplicate slug: {slug}")
        seen_slugs.add(slug)
        context = slug
        if SLUG_PATTERN.fullmatch(slug) is None:
            raise ReleaseInventoryError(f"{context}: invalid slug")

        project_path = _get_string(raw_unit, "path", context)
        expected_project_path = "." if slug == "kitaru" else f"plugins/packages/{slug}"
        if project_path != expected_project_path:
            raise ReleaseInventoryError(
                f"{context}: project path must be {expected_project_path}"
            )
        resolved_project = _resolve_repo_path(root, project_path, context)
        if not resolved_project.is_dir():
            raise ReleaseInventoryError(f"{context}: project path does not exist")

        distribution = _get_string(raw_unit, "distribution", context)
        normalized_distribution = canonicalize_name(distribution)
        if normalized_distribution in seen_distributions:
            raise ReleaseInventoryError(f"duplicate distribution: {distribution}")
        seen_distributions.add(normalized_distribution)

        registry = _get_string(raw_unit, "registry", context)
        if registry != "pypi":
            raise ReleaseInventoryError(f"{context}: unsupported registry {registry}")
        version_source = _get_string(raw_unit, "version-source", context)
        manifest_name, version, project_metadata = _parse_manifest(
            root, resolved_project, version_source, context
        )
        if manifest_name != distribution:
            raise ReleaseInventoryError(
                f"{context}: manifest name {manifest_name} does not match "
                f"{distribution}"
            )
        if slug != "kitaru":
            _validate_plugin_project_metadata(
                resolved_project, project_metadata, context
            )

        changelog = _get_string(raw_unit, "changelog", context)
        changelog_path = _resolve_repo_path(root, changelog, context)
        if not changelog_path.is_file():
            raise ReleaseInventoryError(f"{context}: changelog does not exist")

        lock_source = _get_string(raw_unit, "lock-source", context)
        lock_path = _resolve_repo_path(root, lock_source, context)
        if not lock_path.is_file():
            raise ReleaseInventoryError(f"{context}: lock source does not exist")

        release_label = _get_string(raw_unit, "release-label", context)
        valid_release_label = (
            release_label == "requires:core"
            if slug == "kitaru"
            else release_label.startswith("requires:plugin:")
        )
        if not valid_release_label:
            raise ReleaseInventoryError(
                f"{context}: invalid release label {release_label}"
            )
        if release_label in seen_release_labels:
            raise ReleaseInventoryError(f"duplicate release label: {release_label}")
        seen_release_labels.add(release_label)
        impact_paths = tuple(_get_string_list(raw_unit, "impact-paths", context))
        if not impact_paths:
            raise ReleaseInventoryError(f"{context}: impact-paths must not be empty")
        for impact_path in impact_paths:
            if impact_path.startswith("/") or ".." in Path(impact_path).parts:
                raise ReleaseInventoryError(
                    f"{context}: impact path must be repository-relative"
                )

        default_catalog = raw_unit.get("default-catalog")
        if not isinstance(default_catalog, bool):
            raise ReleaseInventoryError(
                f"{context}: default-catalog must be true or false"
            )
        tag_prefix = _get_string(raw_unit, "tag-prefix", context)
        expected_tag_prefix = f"python/{distribution}/v"
        if tag_prefix != expected_tag_prefix:
            raise ReleaseInventoryError(
                f"{context}: tag prefix must be {expected_tag_prefix}"
            )
        if tag_prefix in seen_tag_prefixes:
            raise ReleaseInventoryError(f"duplicate tag prefix: {tag_prefix}")
        seen_tag_prefixes.add(tag_prefix)

        maintenance_branch_prefix = _get_string(
            raw_unit, "maintenance-branch-prefix", context
        )
        if (
            MAINTENANCE_BRANCH_PREFIX_PATTERN.fullmatch(maintenance_branch_prefix)
            is None
        ):
            raise ReleaseInventoryError(
                f"{context}: invalid maintenance branch prefix "
                f"{maintenance_branch_prefix}"
            )
        if maintenance_branch_prefix in seen_maintenance_branch_prefixes:
            raise ReleaseInventoryError(
                f"duplicate maintenance branch prefix: {maintenance_branch_prefix}"
            )
        seen_maintenance_branch_prefixes.add(maintenance_branch_prefix)

        unit_checks = frozenset(_get_string_list(raw_unit, "checks", context))
        units.append(
            ReleaseUnit(
                slug=slug,
                path=project_path,
                distribution=distribution,
                registry=registry,
                version_source=version_source,
                changelog=changelog,
                lock_source=lock_source,
                version=version,
                default_catalog=default_catalog,
                release_label=release_label,
                impact_paths=impact_paths,
                tag_prefix=tag_prefix,
                maintenance_branch_prefix=maintenance_branch_prefix,
                required_checks=common_checks | unit_checks,
            )
        )

    resolved_units = tuple(units)
    root_units = [unit for unit in resolved_units if unit.path == "."]
    if len(root_units) != 1 or root_units[0].slug != "kitaru":
        raise ReleaseInventoryError("inventory must contain one root kitaru unit")
    _validate_plugin_coverage(root, resolved_units)
    _validate_default_catalog(root, resolved_units)
    inventory = ReleaseInventory(
        schema_version=schema_version,
        common_checks=common_checks,
        units=resolved_units,
    )
    plugin_checks = build_plugin_checks(inventory)
    return replace(
        inventory,
        units=tuple(
            replace(
                unit,
                required_checks=unit.required_checks
                | plugin_checks.get(unit.slug, frozenset()),
            )
            for unit in inventory.units
        ),
    )


def _resolve_unit(selector: str, inventory: ReleaseInventory) -> ReleaseUnit:
    for unit in inventory.units:
        if unit.slug == selector:
            return unit
    raise ReleaseInventoryError(f"unknown release unit: {selector}")


def parse_package_tag(tag: str, inventory: ReleaseInventory) -> ReleaseUnit:
    """Resolve a namespaced package tag and verify its manifest version."""
    match = PACKAGE_TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ReleaseInventoryError(f"invalid package tag: {tag}")
    distribution = match.group("distribution")
    version = validate_version(match.group("version"))
    unit = next(
        (unit for unit in inventory.units if unit.distribution == distribution), None
    )
    if unit is None:
        raise ReleaseInventoryError(
            f"unknown distribution in package tag: {distribution}"
        )
    if version != unit.version:
        raise ReleaseInventoryError(
            f"{tag}: version does not match manifest version {unit.version}"
        )
    return unit


def _build_plugin_shards(
    plugin_units: tuple[ReleaseUnit, ...],
) -> tuple[tuple[ReleaseUnit, ...], ...]:
    """Split plugin units into nonempty balanced CI shards."""
    if not plugin_units:
        return ()

    shard_count = min(PLUGIN_CI_SHARD_COUNT, len(plugin_units))
    base_size, extra_units = divmod(len(plugin_units), shard_count)
    start = 0
    shards: list[tuple[ReleaseUnit, ...]] = []
    for index in range(shard_count):
        size = base_size + (index < extra_units)
        shard = plugin_units[start : start + size]
        start += size
        shards.append(shard)
    return tuple(shards)


def build_plugin_checks(inventory: ReleaseInventory) -> dict[str, frozenset[str]]:
    """Map every plugin release unit to its generated CI artifact check."""
    shards = _build_plugin_shards(inventory.plugin_units)
    shard_count = len(shards)
    checks: dict[str, frozenset[str]] = {}
    for index, shard in enumerate(shards):
        check = f"plugin packages ({index + 1}/{shard_count})"
        for unit in shard:
            checks[unit.slug] = frozenset({check})
    return checks


def build_plugin_matrix(
    inventory: ReleaseInventory,
) -> dict[str, list[dict[str, str]]]:
    """Build balanced GitHub Actions shards for plugin artifact checks."""
    shards = _build_plugin_shards(inventory.plugin_units)
    shard_count = len(shards)
    return {
        "include": [
            {
                "shard": f"{index + 1}/{shard_count}",
                "package_paths": "\n".join(unit.path for unit in shard),
            }
            for index, shard in enumerate(shards)
        ],
    }


def format_units(units: tuple[ReleaseUnit, ...]) -> str:
    """Render concise deterministic release-unit rows for human operators."""
    lines = ["SLUG\tDISTRIBUTION\tVERSION\tDEFAULT\tTAG"]
    lines.extend(
        "\t".join(
            (
                unit.slug,
                unit.distribution,
                unit.version,
                "yes" if unit.default_catalog else "no",
                unit.tag,
            )
        )
        for unit in units
    )
    return "\n".join(lines)


def format_inventory(inventory: ReleaseInventory) -> str:
    """Render a concise deterministic inventory for human operators."""
    return format_units(inventory.units)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and query Kitaru Python release units."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List all release units.")
    list_parser.add_argument("--format", choices=("text", "json"), default="text")

    subparsers.add_parser("matrix", help="Print the plugin CI matrix as JSON.")

    resolve_parser = subparsers.add_parser(
        "resolve", help="Resolve one unit or package tag."
    )
    selector = resolve_parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--unit")
    selector.add_argument("--tag")
    resolve_parser.add_argument("--format", choices=("text", "json"), default="text")

    validate_parser = subparsers.add_parser(
        "validate", help="Validate the inventory and repository manifests."
    )
    validate_parser.add_argument("--format", choices=("text", "json"), default="text")

    propose_parser = subparsers.add_parser(
        "propose-core-version",
        help="Propose or validate the next stable core version.",
    )
    propose_parser.add_argument("--latest-version", required=True)
    propose_parser.add_argument("--label", action="append", default=[])
    propose_parser.add_argument("--candidate")
    propose_parser.add_argument("--format", choices=("text", "json"), default="text")

    reset_parser = subparsers.add_parser(
        "prepare-core-development-reset",
        help="Prepare the post-release core development reset.",
    )
    reset_parser.add_argument("--release-version", required=True)
    return parser.parse_args()


def main() -> int:
    """Run the release-unit query CLI."""
    args = _parse_args()
    try:
        inventory = load_inventory()
        if args.command == "list":
            output = (
                inventory.to_json()
                if args.format == "json"
                else format_inventory(inventory)
            )
        elif args.command == "matrix":
            output = json.dumps(
                {
                    "schema_version": inventory.schema_version,
                    "matrix": build_plugin_matrix(inventory),
                },
                separators=(",", ":"),
            )
        elif args.command == "resolve":
            unit = (
                parse_package_tag(args.tag, inventory)
                if args.tag
                else _resolve_unit(args.unit, inventory)
            )
            output = (
                json.dumps(
                    {
                        "schema_version": inventory.schema_version,
                        "unit": unit.to_dict(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if args.format == "json"
                else format_units((unit,))
            )
        elif args.command == "propose-core-version":
            proposed_version = propose_core_version(args.latest_version, args.label)
            if args.candidate is not None:
                candidate = validate_version(args.candidate)
                if candidate != proposed_version:
                    raise ReleaseInventoryError(
                        f"candidate core version {candidate} does not match required "
                        f"version {proposed_version}"
                    )
            output = (
                json.dumps(
                    {
                        "schema_version": inventory.schema_version,
                        "latest_version": args.latest_version,
                        "proposed_version": proposed_version,
                        "breaking_change": BREAKING_CHANGE_LABEL in args.label,
                    },
                    separators=(",", ":"),
                )
                if args.format == "json"
                else proposed_version
            )
        elif args.command == "prepare-core-development-reset":
            output = prepare_core_development_reset(args.release_version)
        else:
            output = (
                json.dumps(
                    {
                        "schema_version": inventory.schema_version,
                        "status": "valid",
                        "unit_count": len(inventory.units),
                    },
                    separators=(",", ":"),
                )
                if args.format == "json"
                else f"Validated {len(inventory.units)} release units."
            )
    except ReleaseInventoryError as error:
        if getattr(args, "format", "text") == "json":
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "error": {
                            "kind": "release_inventory_error",
                            "message": str(error),
                        },
                    },
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
        else:
            print(f"error: {error}", file=sys.stderr)
        return 2
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
