"""Deterministic Agent registration identity and Pipeline binding helpers."""

from __future__ import annotations

import hashlib
import importlib
import json
import subprocess
import sys
import threading
import types
from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from kitaru._config._agents import _AgentVersionManifest
from kitaru._run_identity import (
    extract_resource_pipeline_id,
    extract_run_project_identity,
)
from kitaru._source_aliases import build_pipeline_registration_name
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError

_REGISTRATION_IDENTITY_SCHEMA_VERSION = 1
_UNTRACKED_DIGEST_CACHE_MAX_SIZE = 512
_UNTRACKED_DIGEST_CACHE: OrderedDict[
    Path, tuple[tuple[int, int, int, int, int], bytes]
] = OrderedDict()
_UNTRACKED_DIGEST_CACHE_LOCK = threading.Lock()


@dataclass(frozen=True)
class RegistrationIdentity:
    """Resolved, canonical version-defining identity."""

    entrypoint: str
    git_sha: str
    git_dirty: bool
    working_tree_hash: str | None
    configuration_hash: str
    worldview_hash: str
    fingerprint: str
    canonical_json: str


@dataclass(frozen=True)
class RegisteredAgentVersionBinding:
    """Immutable Project/Pipeline binding carried by a registered flow."""

    project_id: str
    manifest: _AgentVersionManifest

    @property
    def pipeline_id(self) -> str:
        return self.manifest.pipeline_id

    @property
    def pipeline_name(self) -> str:
        return self.manifest.pipeline_name

    @property
    def fingerprint(self) -> str:
        return self.manifest.fingerprint


def qualified_declared_path(value: Any) -> str:
    """Return the stable declared path for a class or callable."""
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if not isinstance(module, str) or not module:
        raise KitaruUsageError(
            "Registration identity contains a value without a stable import path."
        )
    if not isinstance(qualname, str) or not qualname:
        raise KitaruUsageError(
            "Registration identity contains a callable without a stable declared path."
        )
    return f"{module}:{qualname}"


def qualified_import_path(value: Any) -> str:
    """Return the stable import path for a class or callable."""
    path = qualified_declared_path(value)
    if "<locals>" in path:
        raise KitaruUsageError(
            "Registration identity contains a callable without a stable import path."
        )
    return path


def type_import_path(value: Any) -> str:
    """Return the stable import path for a value's runtime type."""
    return qualified_import_path(value if isinstance(value, type) else type(value))


def canonicalize_registration_value(value: Any) -> Any:
    """Convert a version-defining value into deterministic JSON data."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise KitaruUsageError(
                "Registration identity does not support non-finite numbers."
            )
        return value
    if isinstance(value, Enum):
        return canonicalize_registration_value(value.value)
    if isinstance(value, Path):
        return value.as_posix()
    origin = get_origin(value)
    if origin is not None:
        if origin in (Union, types.UnionType):
            origin_identity = "union"
        elif origin is Annotated:
            origin_identity = "annotated"
        elif origin is Literal:
            origin_identity = "literal"
        else:
            origin_identity = qualified_import_path(origin)
        return {
            "type_annotation": {
                "origin": origin_identity,
                "arguments": [
                    canonicalize_registration_value(argument)
                    for argument in get_args(value)
                ],
            }
        }
    if isinstance(value, BaseModel):
        return canonicalize_registration_value(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "dataclass": qualified_import_path(type(value)),
            "fields": {
                field.name: canonicalize_registration_value(getattr(value, field.name))
                for field in fields(value)
            },
        }
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, (str, Enum, Path)):
                raise KitaruUsageError(
                    "Registration identity mapping keys must be strings."
                )
            normalized[str(key)] = canonicalize_registration_value(item)
        return dict(sorted(normalized.items()))
    if isinstance(value, (set, frozenset)):
        normalized_items = [canonicalize_registration_value(item) for item in value]
        return sorted(normalized_items, key=_canonical_json)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [canonicalize_registration_value(item) for item in value]
    if isinstance(value, type) or callable(value):
        return {"import_path": qualified_import_path(value)}
    raise KitaruUsageError(
        "Registration identity contains an unsupported non-serializable value "
        f"of type {type(value).__module__}.{type(value).__qualname__}."
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def hash_registration_value(value: Any) -> str:
    """Hash a canonical value without retaining its potentially sensitive content."""
    return _sha256_text(_canonical_json(canonicalize_registration_value(value)))


def _run_git(repo_root: Path, *args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise KitaruStateError(
            "Unable to resolve the Git identity required for Agent registration."
        ) from exc


def _add_git_output_to_digest(
    digest: hashlib._Hash, repo_root: Path, *args: str
) -> int:
    """Stream Git output into a digest and return the emitted byte count."""
    try:
        process = subprocess.Popen(
            ["git", "-C", str(repo_root), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise KitaruStateError(
            "Unable to resolve the Git identity required for Agent registration."
        ) from exc
    stdout = process.stdout
    if stdout is None:  # pragma: no cover - guaranteed by stdout=PIPE
        process.kill()
        raise KitaruStateError(
            "Unable to resolve the Git identity required for Agent registration."
        )
    byte_count = 0
    for chunk in iter(lambda: stdout.read(1024 * 1024), b""):
        byte_count += len(chunk)
        digest.update(chunk)
    if process.wait() != 0:
        raise KitaruStateError(
            "Unable to resolve the Git identity required for Agent registration."
        )
    return byte_count


def _file_signature(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _untracked_file_digest(path: Path) -> bytes:
    """Hash an untracked file, reusing its digest while its stat identity is stable."""
    try:
        before = _file_signature(path)
    except OSError as exc:
        raise KitaruStateError(
            "Unable to hash an untracked file for Agent registration."
        ) from exc
    with _UNTRACKED_DIGEST_CACHE_LOCK:
        cached = _UNTRACKED_DIGEST_CACHE.get(path)
        if cached is not None and cached[0] == before:
            _UNTRACKED_DIGEST_CACHE.move_to_end(path)
            return cached[1]

    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        after = _file_signature(path)
    except OSError as exc:
        raise KitaruStateError(
            "Unable to hash an untracked file for Agent registration."
        ) from exc
    if before != after:
        raise KitaruStateError(
            "An untracked file changed while resolving Agent registration identity."
        )
    value = digest.digest()
    with _UNTRACKED_DIGEST_CACHE_LOCK:
        _UNTRACKED_DIGEST_CACHE[path] = (after, value)
        _UNTRACKED_DIGEST_CACHE.move_to_end(path)
        while len(_UNTRACKED_DIGEST_CACHE) > _UNTRACKED_DIGEST_CACHE_MAX_SIZE:
            _UNTRACKED_DIGEST_CACHE.popitem(last=False)
    return value


def _working_tree_identity(repo_root: Path) -> tuple[bool, str | None]:
    digest = hashlib.sha256()

    def add_section(name: str, payload: bytes) -> None:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)

    digest.update(b"tracked\0")
    tracked_bytes = _add_git_output_to_digest(
        digest, repo_root, "diff", "HEAD", "--binary", "--no-ext-diff"
    )

    untracked_raw = _run_git(
        repo_root, "ls-files", "--others", "--exclude-standard", "-z"
    )
    untracked_paths = sorted(
        path
        for path in untracked_raw.decode("utf-8", errors="surrogateescape").split("\0")
        if path
    )
    for relative_path in untracked_paths:
        path = (repo_root / relative_path).resolve()
        try:
            path.relative_to(repo_root)
            payload = _untracked_file_digest(path)
        except (OSError, ValueError) as exc:
            raise KitaruStateError(
                "Unable to hash an untracked file for Agent registration."
            ) from exc
        add_section(f"untracked:{relative_path}", payload)
    dirty = tracked_bytes > 0 or bool(untracked_paths)
    return dirty, f"sha256:{digest.hexdigest()}" if dirty else None


def resolve_registration_identity(
    *,
    repo_root: Path,
    entrypoint: str,
    configuration: Mapping[str, Any],
    worldview: Mapping[str, Any],
) -> RegistrationIdentity:
    """Resolve the canonical identity shared by registration and preflight."""
    normalized_root = repo_root.resolve()
    revision_lines = (
        _run_git(normalized_root, "rev-parse", "--show-toplevel", "HEAD")
        .decode("utf-8")
        .splitlines()
    )
    if len(revision_lines) != 2:
        raise KitaruStateError(
            "Unable to resolve the Git identity required for Agent registration."
        )
    git_top_level = Path(revision_lines[0]).resolve()
    try:
        normalized_root.relative_to(git_top_level)
    except ValueError as exc:
        raise KitaruStateError(
            "The Kitaru repository is outside the active Git working tree."
        ) from exc

    git_sha = revision_lines[1].strip()
    dirty, working_tree_hash = _working_tree_identity(normalized_root)

    canonical_configuration = canonicalize_registration_value(configuration)
    canonical_worldview = canonicalize_registration_value(worldview)
    configuration_json = _canonical_json(canonical_configuration)
    worldview_json = _canonical_json(canonical_worldview)
    configuration_hash = _sha256_text(configuration_json)
    worldview_hash = _sha256_text(worldview_json)

    document = {
        "schema_version": _REGISTRATION_IDENTITY_SCHEMA_VERSION,
        "entrypoint": entrypoint,
        "git": {
            "sha": git_sha,
            "dirty": dirty,
            "working_tree_hash": working_tree_hash,
        },
        "configuration": canonical_configuration,
        "worldview": canonical_worldview,
    }
    canonical_json = _canonical_json(document)
    return RegistrationIdentity(
        entrypoint=entrypoint,
        git_sha=git_sha,
        git_dirty=dirty,
        working_tree_hash=working_tree_hash,
        configuration_hash=configuration_hash,
        worldview_hash=worldview_hash,
        fingerprint=_sha256_text(canonical_json),
        canonical_json=canonical_json,
    )


def _module_path_within_repository(module: Any, repo_root: Path) -> bool:
    module_file = getattr(module, "__file__", None)
    if not isinstance(module_file, str):
        return False
    try:
        Path(module_file).resolve().relative_to(repo_root)
    except ValueError:
        return False
    return True


def _resolve_attribute(module: Any, attribute_path: str) -> Any:
    value = module
    for part in attribute_path.split("."):
        if not part or not part.isidentifier():
            raise KitaruUsageError(
                "Agent entrypoints must use an importable module:attribute path."
            )
        try:
            value = getattr(value, part)
        except AttributeError as exc:
            raise KitaruUsageError(
                "The Agent entrypoint does not resolve to an object."
            ) from exc
    return value


def validate_agent_entrypoint(
    entrypoint: str,
    *,
    target: Any,
    repo_root: Path,
) -> str:
    """Require an importable repo-local entrypoint resolving to the same wrapper."""
    if ":" not in entrypoint:
        raise KitaruUsageError(
            "Agent entrypoints must use the form 'module:attribute'."
        )
    module_name, attribute_path = entrypoint.split(":", 1)
    if not module_name or not attribute_path:
        raise KitaruUsageError(
            "Agent entrypoints must use the form 'module:attribute'."
        )
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise KitaruUsageError("Unable to import the Agent entrypoint module.") from exc
    if not _module_path_within_repository(module, repo_root.resolve()):
        raise KitaruUsageError(
            "The Agent entrypoint must be defined inside the Kitaru repository."
        )
    if _resolve_attribute(module, attribute_path) is not target:
        raise KitaruUsageError(
            "The Agent entrypoint must resolve to this KitaruAgent instance."
        )
    return f"{module_name}:{attribute_path}"


def resolve_agent_entrypoint(
    *,
    target: Any,
    repo_root: Path,
    entrypoint: str | None = None,
) -> str:
    """Validate an explicit entrypoint or infer one from loaded repo modules."""
    if entrypoint is not None:
        return validate_agent_entrypoint(
            entrypoint.strip(), target=target, repo_root=repo_root
        )

    candidates: list[str] = []
    for module_name, module in tuple(sys.modules.items()):
        if module is None or not _module_path_within_repository(
            module, repo_root.resolve()
        ):
            continue
        try:
            module_values = vars(module)
        except TypeError:
            continue
        for attribute_name, value in module_values.items():
            if (
                value is target
                and attribute_name.isidentifier()
                and not attribute_name.startswith("_")
            ):
                candidates.append(f"{module_name}:{attribute_name}")
    for candidate in sorted(set(candidates)):
        try:
            return validate_agent_entrypoint(
                candidate, target=target, repo_root=repo_root
            )
        except KitaruUsageError:
            continue
    raise KitaruUsageError(
        "Unable to infer an importable Agent entrypoint. Assign the KitaruAgent "
        "to a module-level name or pass entrypoint='module:attribute'."
    )


def build_agent_version_pipeline_name(
    *,
    agent_name: str,
    identity: RegistrationIdentity,
) -> str:
    """Build the deterministic ZenML Pipeline name for one AgentVersion."""
    git_fragment = identity.git_sha[:8]
    fingerprint_fragment = identity.fingerprint.removeprefix("sha256:")[:12]
    return build_pipeline_registration_name(
        f"{agent_name}__av_{git_fragment}_{fingerprint_fragment}"
    )


def _page_items(page: Any) -> list[Any]:
    items = getattr(page, "items", None)
    if items is not None and not callable(items):
        return list(items)
    if isinstance(page, Iterable) and not isinstance(page, (str, bytes)):
        return list(page)
    raise KitaruStateError(
        "Unexpected Pipeline lookup response from the configured runtime."
    )


def _resource_project_id(resource: Any) -> str | None:
    return extract_run_project_identity(resource).project_id


def find_exact_project_pipeline(
    client: Any,
    *,
    project_id: str,
    pipeline_name: str,
) -> Any | None:
    """Resolve exactly one Pipeline by project and exact name."""
    try:
        page = client.list_pipelines(
            name=f"equals:{pipeline_name}",
            project=project_id,
            hydrate=True,
            size=2,
        )
    except Exception as exc:
        raise KitaruBackendError(
            "Unable to resolve the registered AgentVersion Pipeline."
        ) from exc
    pipelines = _page_items(page)
    if len(pipelines) > 1:
        raise KitaruStateError(
            "Multiple Pipelines match the registered AgentVersion name."
        )
    if not pipelines:
        return None
    pipeline = pipelines[0]
    if str(getattr(pipeline, "name", "")).strip() != pipeline_name:
        raise KitaruStateError("The exact Pipeline lookup returned a different name.")
    if _resource_project_id(pipeline) != project_id:
        raise KitaruStateError(
            "The registered AgentVersion Pipeline belongs to a different Project."
        )
    return pipeline


def verify_registered_pipeline(
    client: Any,
    binding: RegisteredAgentVersionBinding,
) -> Any:
    """Require the manifest Pipeline UUID before a registered write."""
    pipeline = find_exact_project_pipeline(
        client,
        project_id=binding.project_id,
        pipeline_name=binding.pipeline_name,
    )
    if pipeline is None:
        raise KitaruStateError(
            "The registered AgentVersion Pipeline no longer exists. "
            "Registration is required."
        )
    if str(getattr(pipeline, "id", "")).strip() != binding.pipeline_id:
        raise KitaruStateError(
            "The registered Pipeline name now resolves to a different UUID. "
            "Registration is required."
        )
    return pipeline


def _run_snapshot(run: Any) -> Any | None:
    snapshot = getattr(run, "snapshot", None)
    if snapshot is not None:
        return snapshot
    resources = getattr(run, "resources", None)
    return getattr(resources, "snapshot", None)


def _snapshot_pipeline_id(snapshot: Any) -> str | None:
    return extract_resource_pipeline_id(snapshot)


def verify_submitted_run_binding(
    client: Any,
    *,
    run: Any,
    binding: RegisteredAgentVersionBinding,
) -> Any:
    """Verify the persisted run snapshot still references the manifest UUID."""
    run_id = str(getattr(run, "id", "")).strip()
    if not run_id:
        raise KitaruStateError(
            "The registered submission returned a run without an ID."
        )
    try:
        hydrated_run = client.get_pipeline_run(
            name_id_or_prefix=run_id,
            allow_name_prefix_match=False,
            hydrate=True,
            project=binding.project_id,
        )
    except Exception as exc:
        raise KitaruBackendError(
            "Unable to verify the registered execution attribution."
        ) from exc

    return verify_hydrated_submitted_run_binding(
        hydrated_run,
        binding=binding,
    )


def verify_hydrated_submitted_run_binding(
    run: Any,
    *,
    binding: RegisteredAgentVersionBinding,
) -> Any:
    """Validate one already-hydrated run against its registered binding."""
    if _resource_project_id(run) != binding.project_id:
        raise KitaruStateError(
            "The registered execution belongs to a different Agent Project."
        )
    snapshot = _run_snapshot(run)
    if snapshot is None:
        raise KitaruStateError(
            "The registered execution has no hydrated Pipeline snapshot."
        )
    if _resource_project_id(snapshot) != binding.project_id:
        raise KitaruStateError(
            "The registered execution snapshot belongs to a different Agent Project."
        )
    if _snapshot_pipeline_id(snapshot) != binding.pipeline_id:
        raise KitaruStateError(
            "The execution snapshot references a different Pipeline UUID. "
            "The registered Pipeline may have been deleted and recreated."
        )
    return run


def identity_drift_categories(
    expected: RegistrationIdentity,
    actual: RegistrationIdentity,
) -> list[str]:
    """Return safe category names for changed version-defining identity."""
    changed: list[str] = []
    if expected.entrypoint != actual.entrypoint:
        changed.append("entrypoint")
    if (
        expected.git_sha != actual.git_sha
        or expected.git_dirty != actual.git_dirty
        or expected.working_tree_hash != actual.working_tree_hash
    ):
        changed.append("source/Git")
    if expected.configuration_hash != actual.configuration_hash:
        changed.append("configuration")
    if expected.worldview_hash != actual.worldview_hash:
        changed.append("worldview")
    if expected.canonical_json != actual.canonical_json and not changed:
        changed.append("identity")
    if expected.fingerprint != actual.fingerprint and not changed:
        changed.append("fingerprint")
    return changed
