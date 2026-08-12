"""Validate package-scoped release labels and calculate proposed versions."""

import argparse
import json
import re
import subprocess
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = REPOSITORY_ROOT / "plugins" / "packages"
BUMPS = ("major", "minor", "patch", "stable", "none")
CHANNELS = ("stable", "rc")
DEFAULT_PLUGINS = {
    "braintrust-importer",
    "evaluator",
    "jsonl-importer",
    "langfuse-importer",
    "langsmith-importer",
    "opentelemetry-importer",
}
CORE_PATHS = (
    "docker/release-",
    "helm/",
    "pyproject.toml",
    "src/kitaru/",
    "uv.lock",
)
VERSION_PATTERN = re.compile(
    r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?:rc(?P<rc>\d+))?$"
)
LABEL_PATTERN = re.compile(
    r"^release:(?P<unit>[a-z0-9-]+):"
    r"(?P<bump>major|minor|patch|stable|none)$"
)


class ReleasePlanError(Exception):
    """Report an invalid or incomplete release plan."""


@dataclass(frozen=True)
class Version:
    """Represent a supported stable or release-candidate Python version."""

    major: int
    minor: int
    patch: int
    rc: int | None = None

    @classmethod
    def parse(cls, value: str) -> "Version":
        """Parse a canonical PEP 440 version used by Kitaru packages."""
        match = VERSION_PATTERN.fullmatch(value)
        if match is None:
            raise ReleasePlanError(f"Unsupported package version: {value}")
        return cls(
            major=int(match.group("major")),
            minor=int(match.group("minor")),
            patch=int(match.group("patch")),
            rc=int(match.group("rc")) if match.group("rc") else None,
        )

    def bump(self, bump: str, channel: str) -> "Version":
        """Apply one release bump and optional RC channel."""
        if bump == "stable":
            if self.rc is None:
                raise ReleasePlanError("A stable bump requires a current RC version")
            return Version(self.major, self.minor, self.patch)
        if channel == "rc" and self.rc is not None:
            return Version(self.major, self.minor, self.patch, rc=self.rc + 1)
        if bump == "major":
            version = Version(self.major + 1, 0, 0)
        elif bump == "minor":
            version = Version(self.major, self.minor + 1, 0)
        elif bump == "patch":
            version = Version(self.major, self.minor, self.patch + 1)
        else:
            raise ReleasePlanError(f"Cannot calculate a version for bump: {bump}")
        if channel == "rc":
            return Version(version.major, version.minor, version.patch, rc=1)
        return version

    def __str__(self) -> str:
        """Return the canonical PEP 440 representation."""
        suffix = f"rc{self.rc}" if self.rc is not None else ""
        return f"{self.major}.{self.minor}.{self.patch}{suffix}"


@dataclass(frozen=True)
class PackagePlan:
    """Describe one affected release unit."""

    unit: str
    distribution: str
    current_version: str
    bump: str
    next_version: str | None
    default_plugin: bool


@dataclass(frozen=True)
class ReleasePlan:
    """Describe all release intent declared by one pull request."""

    channel: str
    packages: tuple[PackagePlan, ...]


def _get_plugin_units() -> tuple[str, ...]:
    """Get independently versioned plugin directory names."""
    return tuple(
        sorted(
            path.name
            for path in PLUGIN_ROOT.iterdir()
            if path.is_dir() and (path / "pyproject.toml").is_file()
        )
    )


def _get_changed_paths(base: str, head: str) -> tuple[str, ...]:
    """Get paths changed between two Git refs."""
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...{head}"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return tuple(path for path in result.stdout.splitlines() if path)


def get_affected_units(paths: tuple[str, ...]) -> tuple[str, ...]:
    """Map changed repository paths to independently releasable units."""
    units: set[str] = set()
    plugin_units = set(_get_plugin_units())
    for path in paths:
        parts = Path(path).parts
        if len(parts) >= 3 and parts[:2] == ("plugins", "packages"):
            if parts[2] in plugin_units:
                units.add(parts[2])
            continue
        if any(path == prefix or path.startswith(prefix) for prefix in CORE_PATHS):
            units.add("core")
    return tuple(sorted(units))


def _get_project_file(unit: str) -> Path:
    """Get the project metadata path for one release unit."""
    return (
        REPOSITORY_ROOT / "pyproject.toml"
        if unit == "core"
        else PLUGIN_ROOT / unit / "pyproject.toml"
    )


def _get_package_metadata(unit: str, ref: str | None = None) -> tuple[str, Version]:
    """Get distribution name and current version for one release unit."""
    project_file = _get_project_file(unit)
    if ref is None:
        with project_file.open("rb") as file:
            project = tomllib.load(file)["project"]
    else:
        repository_path = project_file.relative_to(REPOSITORY_ROOT)
        result = subprocess.run(
            ["git", "show", f"{ref}:{repository_path}"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
        )
        project = tomllib.loads(result.stdout.decode())["project"]
    return str(project["name"]), Version.parse(str(project["version"]))


def _get_channel(labels: tuple[str, ...]) -> str:
    """Get the requested release channel from PR labels."""
    channels = {
        label.removeprefix("release:channel:")
        for label in labels
        if label.startswith("release:channel:")
    }
    unknown = channels.difference(CHANNELS)
    if unknown:
        raise ReleasePlanError(
            f"Unsupported release channel labels: {', '.join(sorted(unknown))}"
        )
    if len(channels) > 1:
        raise ReleasePlanError("Select only one release channel label")
    return next(iter(channels), "stable")


def create_plan(
    paths: tuple[str, ...],
    labels: tuple[str, ...],
    base_ref: str | None = None,
    require_version_bumps: bool = False,
) -> ReleasePlan:
    """Validate labels and create a release plan for changed paths."""
    affected_units = set(get_affected_units(paths))
    declared: dict[str, list[str]] = {}
    for label in labels:
        match = LABEL_PATTERN.fullmatch(label)
        if match is None:
            continue
        declared.setdefault(match.group("unit"), []).append(match.group("bump"))

    missing = affected_units.difference(declared)
    extra = set(declared).difference(affected_units)
    duplicate = {unit for unit, bumps in declared.items() if len(bumps) != 1}
    errors: list[str] = []
    if missing:
        errors.append(
            "Missing release label for: "
            + ", ".join(sorted(missing))
            + ". Add release:<unit>:major|minor|patch|stable|none."
        )
    if extra:
        errors.append(
            "Release labels name unaffected units: " + ", ".join(sorted(extra))
        )
    if duplicate:
        errors.append("Select one release label for: " + ", ".join(sorted(duplicate)))
    if errors:
        raise ReleasePlanError("\n".join(errors))

    channel = _get_channel(labels)
    packages: list[PackagePlan] = []
    for unit in sorted(affected_units):
        distribution, current = _get_package_metadata(unit, ref=base_ref)
        bump = declared[unit][0]
        next_version = None if bump == "none" else str(current.bump(bump, channel))
        if require_version_bumps:
            _, head_version = _get_package_metadata(unit)
            expected = str(current) if next_version is None else next_version
            if str(head_version) != expected:
                raise ReleasePlanError(
                    f"{unit} has version {head_version}, expected {expected} for "
                    f"release:{unit}:{bump}"
                )
        packages.append(
            PackagePlan(
                unit=unit,
                distribution=distribution,
                current_version=str(current),
                bump=bump,
                next_version=next_version,
                default_plugin=unit in DEFAULT_PLUGINS,
            )
        )
    return ReleasePlan(channel=channel, packages=tuple(packages))


def format_markdown(plan: ReleasePlan) -> str:
    """Format a release plan for a GitHub job summary."""
    lines = ["## Release plan", "", f"Channel: `{plan.channel}`", ""]
    if not plan.packages:
        lines.append("No independently releasable package changed.")
        return "\n".join(lines)
    lines.extend(
        [
            "| Unit | Distribution | Current | Bump | Proposed |",
            "|---|---|---:|---|---:|",
        ]
    )
    for package in plan.packages:
        proposed = package.next_version or "none"
        lines.append(
            f"| `{package.unit}` | `{package.distribution}` | "
            f"`{package.current_version}` | `{package.bump}` | `{proposed}` |"
        )
    default_releases = [
        package.unit
        for package in plan.packages
        if package.default_plugin and package.next_version is not None
    ]
    if default_releases:
        lines.extend(
            [
                "",
                "A later core release must adopt the new exact default-plugin "
                "pins for: "
                + ", ".join(f"`{unit}`" for unit in default_releases)
                + ".",
            ]
        )
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="origin/develop")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--labels", default="")
    parser.add_argument("--paths-json")
    parser.add_argument("--require-version-bumps", action="store_true")
    parser.add_argument("--output", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    """Validate PR release intent and print the calculated plan."""
    args = _parse_args()
    labels = tuple(label.strip() for label in args.labels.split(",") if label.strip())
    paths = (
        tuple(json.loads(args.paths_json))
        if args.paths_json is not None
        else _get_changed_paths(args.base, args.head)
    )
    try:
        plan = create_plan(
            paths,
            labels,
            base_ref=args.base,
            require_version_bumps=args.require_version_bumps,
        )
    except ReleasePlanError as exc:
        print(f"Release plan error:\n{exc}")
        return 1
    if args.output == "json":
        print(json.dumps(asdict(plan), indent=2))
    else:
        print(format_markdown(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
