#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Shared asset registration, upload, and exact-resolution helpers."""

import ast
import json
import re
import shlex
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeVar

import yaml
from packaging.requirements import InvalidRequirement, Requirement
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from kitaru.api_models.v1.agent import AgentCreateRequest, AgentListParams
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    RunSpec,
)
from kitaru.api_models.v1.annotation import AnnotationListParams
from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.experiment_run import ExperimentRunListParams
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterVersionCreateRequest,
)
from kitaru.api_models.v1.investigation import InvestigationListParams
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.api_models.v1.replay import ReplayListParams
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.config import build_api_client, resolve_credential
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.references import (
    ParentKind,
    ReferenceResolutionError,
    resolve_parent_resource,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore

_MAX_REQUIREMENT_LENGTH = 255
_MODULE_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_ATTRIBUTE_RE = re.compile(r"^[A-Za-z_]\w*$")
_ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ListParamsT = TypeVar("ListParamsT", bound=ListParams)


class _SpecModel(BaseModel):
    """Strict base for CLI-only spec documents."""

    model_config = ConfigDict(extra="forbid")


class AgentParentSpec(_SpecModel):
    """Parent fields accepted in an agent registration spec."""

    description: str | None = None


class AgentRegistrationSpec(_SpecModel):
    """Agent parent and initial-version spec document."""

    parent: AgentParentSpec = Field(default_factory=AgentParentSpec)
    version: AgentVersionCreateRequest


@dataclass(frozen=True, slots=True)
class ScriptSource:
    """Validated script bytes read exactly once."""

    path: Path
    content: bytes
    entrypoint: str


@dataclass(frozen=True, slots=True)
class PackageSource:
    """Validated pinned package source."""

    requirement: str
    entrypoint: str


PluginSourceInput = ScriptSource | PackageSource


@asynccontextmanager
async def open_client(
    server_url: str,
    credential_store: CredentialStore,
    timeout: float,
) -> AsyncIterator[KitaruAPIClient]:
    """Open one SDK client using the established credential precedence."""
    credential = resolve_credential(server_url, credential_store)
    client = build_api_client(server_url, credential, credential_store, timeout)
    try:
        yield client
    finally:
        await client.close()


def page_result(page: Page[Any], *, size: int) -> CommandResult:
    """Project an SDK page into the shared CLI list envelope."""
    return CommandResult(
        items=[item.model_dump(mode="json") for item in page.items],
        page={
            "limit": size,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor is not None,
        },
    )


async def resolve_asset(resource: Any, reference: str, label: str) -> Any:
    """Resolve one parent through the shared bounded reference helper."""
    try:
        kind = ParentKind(label.lower())
    except ValueError as error:
        raise CLIError(
            "internal_error", f"Unsupported asset kind {label!r}."
        ) from error
    try:
        return await resolve_parent_resource(resource, kind, reference)
    except ReferenceResolutionError as error:
        raise CLIError(
            error.code,
            error.message,
            details=error.details,
        ) from error


def parse_version_reference(reference: str, label: str) -> tuple[str, int | str]:
    """Parse ``PARENT@VERSION`` with optional ``latest`` reads."""
    parent, separator, version = reference.rpartition("@")
    if not separator or not parent or not version:
        raise CLIError(
            "invalid_arguments",
            f"{label} version reference must be PARENT@VERSION.",
        )
    if version == "latest":
        return parent, version
    try:
        number = int(version)
    except ValueError as error:
        raise CLIError(
            "invalid_arguments",
            f"{label} version must be a positive integer or 'latest'.",
        ) from error
    if number < 1 or str(number) != version:
        raise CLIError(
            "invalid_arguments",
            f"{label} version must be a canonical positive integer or 'latest'.",
        )
    return parent, number


async def get_agent_version(client: Any, reference: str) -> tuple[Any, Any]:
    """Resolve an agent and one exact server-assigned version."""
    parent_reference, requested = parse_version_reference(reference, "Agent")
    agent = await resolve_asset(client.agents, parent_reference, "Agent")
    version = agent.latest_version if requested == "latest" else requested
    matches = [
        item
        async for item in client.agents.iter_versions(agent.id)
        if item.version == version
    ]
    if not matches:
        raise CLIError("not_found", f"Agent {agent.name!r} has no version {version}.")
    if len(matches) > 1:
        raise CLIError(
            "conflict",
            f"Agent {agent.name!r} has multiple records for version {version}.",
            details={"ids": [str(item.id) for item in matches]},
        )
    return agent, matches[0]


async def get_plugin_version(
    resource: Any, reference: str, label: str
) -> tuple[Any, Any]:
    """Resolve an importer/evaluator and one exact server-assigned version."""
    parent_reference, requested = parse_version_reference(reference, label)
    parent = await resolve_asset(resource, parent_reference, label)
    version = parent.latest_version if requested == "latest" else requested
    item = await resource.get_version(parent.id, version)
    return parent, item


def load_agent_register_spec(
    name: str, path: Path
) -> tuple[AgentCreateRequest, AgentVersionCreateRequest]:
    """Load a strict parent-plus-version YAML or JSON document."""
    data = _load_document(path)
    spec = AgentRegistrationSpec.model_validate(data)
    _validate_agent_version(spec.version)
    return (
        AgentCreateRequest(name=name, description=spec.parent.description),
        spec.version,
    )


def load_agent_version_spec(path: Path) -> AgentVersionCreateRequest:
    """Load a strict agent version YAML or JSON document."""
    request = AgentVersionCreateRequest.model_validate(_load_document(path))
    _validate_agent_version(request)
    return request


def build_agent_requests(
    name: str,
    *,
    command: str | None,
    entrypoint: str | None,
    description: str | None,
    version_description: str | None,
    display_version: str | None,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
    tools: list[str] | None,
    mcp_servers: list[str] | None,
    skills: list[str] | None,
) -> tuple[AgentCreateRequest, AgentVersionCreateRequest]:
    """Build direct agent parent and version requests."""
    normalized_command = normalize_agent_source(command=command, entrypoint=entrypoint)
    run_options: dict[str, Any] = {
        "command": normalized_command,
        "working_dir": working_dir,
        "env": parse_env(env or []),
        "secret_ids": secret_ids or [],
    }
    if timeout_seconds is not None:
        run_options["timeout_seconds"] = timeout_seconds
    capabilities = AgentCapabilities(
        tools=tools or [], mcp_servers=mcp_servers or [], skills=skills or []
    )
    return (
        AgentCreateRequest(name=name, description=description),
        AgentVersionCreateRequest(
            display_version=display_version,
            description=version_description,
            run_spec=RunSpec(**run_options),
            capabilities=capabilities,
        ),
    )


def build_agent_version_request(
    *,
    command: str | None,
    entrypoint: str | None,
    description: str | None,
    display_version: str | None,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
    tools: list[str] | None,
    mcp_servers: list[str] | None,
    skills: list[str] | None,
) -> AgentVersionCreateRequest:
    """Build one direct agent version request."""
    _, request = build_agent_requests(
        "unused",
        command=command,
        entrypoint=entrypoint,
        description=None,
        version_description=description,
        display_version=display_version,
        working_dir=working_dir,
        env=env,
        secret_ids=secret_ids,
        timeout_seconds=timeout_seconds,
        tools=tools,
        mcp_servers=mcp_servers,
        skills=skills,
    )
    return request


def normalize_agent_source(*, command: str | None, entrypoint: str | None) -> str:
    """Validate one agent source and return the stored shell command."""
    if entrypoint is not None:
        raise CLIError(
            "invalid_arguments",
            "Agent --entrypoint is not supported; use --command.",
        )
    if command is None:
        raise CLIError("invalid_arguments", "Agent registration requires --command.")
    if not command.strip():
        raise CLIError("invalid_arguments", "--command cannot be blank.")
    return command


def parse_env(values: list[str]) -> dict[str, str]:
    """Parse repeatable ``KEY=VALUE`` environment options."""
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not _ENV_RE.fullmatch(key):
            raise CLIError(
                "invalid_arguments",
                f"Invalid environment value {value!r}; use KEY=VALUE.",
            )
        if key in result:
            raise CLIError(
                "invalid_arguments", f"Environment key {key!r} was repeated."
            )
        result[key] = item
    return result


def prepare_plugin_source(
    *, script: Path | None, package: str | None, entrypoint: str | None
) -> PluginSourceInput:
    """Validate exactly one script or package plugin source locally."""
    if (script is None) == (package is None):
        raise CLIError(
            "invalid_arguments", "Exactly one of --script or --package is required."
        )
    if entrypoint is None or not entrypoint.strip():
        raise CLIError("invalid_arguments", "--entrypoint is required.")
    if script is not None:
        return validate_script_source(script, entrypoint)
    assert package is not None
    return validate_package_source(package, entrypoint)


def validate_script_source(path: Path, entrypoint: str) -> ScriptSource:
    """Read and validate a Python script without executing it."""
    if not path.exists() or not path.is_file():
        raise CLIError(
            "invalid_arguments", f"Script {str(path)!r} is not a regular file."
        )
    if not _ATTRIBUTE_RE.fullmatch(entrypoint):
        raise CLIError(
            "invalid_arguments",
            "Script entrypoint must be one top-level attribute name.",
        )
    try:
        content = path.read_bytes()
        text = content.decode("utf-8")
        tree = ast.parse(text, filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as error:
        raise CLIError(
            "invalid_arguments", f"Invalid script {str(path)!r}: {error}"
        ) from error
    names = _top_level_names(tree)
    if entrypoint not in names:
        raise CLIError(
            "invalid_arguments",
            f"Script {str(path)!r} has no top-level attribute {entrypoint!r}.",
        )
    return ScriptSource(path=path, content=content, entrypoint=entrypoint)


def validate_package_source(requirement: str, entrypoint: str) -> PackageSource:
    """Validate one pinned PEP 508 requirement without importing it."""
    validate_module_entrypoint(entrypoint)
    if len(requirement) > _MAX_REQUIREMENT_LENGTH:
        raise CLIError(
            "invalid_arguments",
            f"Package requirement exceeds {_MAX_REQUIREMENT_LENGTH} characters.",
        )
    try:
        parsed = Requirement(requirement)
    except InvalidRequirement as error:
        raise CLIError(
            "invalid_arguments", f"Invalid package requirement: {error}"
        ) from error
    specifiers = list(parsed.specifier)
    pinned = (
        parsed.url is None
        and parsed.marker is None
        and len(specifiers) == 1
        and specifiers[0].operator == "=="
        and "*" not in specifiers[0].version
    )
    if not pinned:
        raise CLIError(
            "invalid_arguments",
            "--package must have one exact == version without a marker or URL.",
        )
    return PackageSource(requirement=str(parsed), entrypoint=entrypoint)


def validate_module_entrypoint(reference: str) -> tuple[str, str]:
    """Validate a top-level ``MODULE:ATTRIBUTE`` reference."""
    module, separator, attribute = reference.partition(":")
    if (
        not separator
        or ":" in attribute
        or not _MODULE_RE.fullmatch(module)
        or not _ATTRIBUTE_RE.fullmatch(attribute)
    ):
        raise CLIError(
            "invalid_arguments",
            f"Invalid entrypoint {reference!r}; expected MODULE:ATTRIBUTE.",
        )
    return module, attribute


def parse_json_object(value: str | None, *, option: str) -> dict[str, Any]:
    """Parse a JSON object option."""
    if value is None:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise CLIError(
            "invalid_arguments", f"{option} is not valid JSON: {error}"
        ) from error
    if not isinstance(parsed, dict):
        raise CLIError("invalid_arguments", f"{option} must contain a JSON object.")
    return parsed


def parse_replay_override(value: str, *, option: str) -> ReplayOverride:
    """Parse an inline replay override using the existing API model."""
    return ReplayOverride.model_validate(parse_json_object(value, option=option))


def parse_tool_policy(value: str, *, option: str) -> ToolPolicy:
    """Parse an inline tool policy using the existing API model."""
    return ToolPolicy.model_validate(parse_json_object(value, option=option))


async def resolve_evaluator_configs(
    client: Any,
    evaluator_tokens: Sequence[str],
    parameter_entries: Sequence[str],
) -> tuple[list[EvaluatorConfig], list[dict[str, Any]], list[uuid.UUID]]:
    """Resolve exact evaluator configurations and bounded identities."""
    if not evaluator_tokens:
        raise CLIError("invalid_arguments", "Provide at least one --evaluator.")
    if len(set(evaluator_tokens)) != len(evaluator_tokens):
        raise CLIError("invalid_arguments", "Each --evaluator token must be unique.")

    selected = set(evaluator_tokens)
    params_by_token: dict[str, dict[str, Any]] = {}
    for entry in parameter_entries:
        token, separator, value = entry.partition("=")
        if not separator or not token:
            raise CLIError(
                "invalid_arguments",
                "--evaluator-params must be EVALUATOR@VERSION=JSON_OBJECT.",
            )
        if token not in selected:
            raise CLIError(
                "invalid_arguments",
                f"--evaluator-params token {token!r} is not a selected evaluator.",
            )
        if token in params_by_token:
            raise CLIError(
                "invalid_arguments",
                f"Parameters for evaluator token {token!r} were provided "
                "more than once.",
            )
        params_by_token[token] = parse_json_object(value, option="--evaluator-params")

    configs: list[EvaluatorConfig] = []
    identities: list[dict[str, Any]] = []
    version_ids: list[uuid.UUID] = []
    seen_versions: set[uuid.UUID] = set()
    for token in evaluator_tokens:
        parent, version = await get_plugin_version(
            client.evaluators, token, "Evaluator"
        )
        if version.id in seen_versions:
            raise CLIError(
                "invalid_arguments",
                "Different evaluator tokens resolved to the same evaluator version.",
            )
        seen_versions.add(version.id)
        version_ids.append(version.id)
        configs.append(
            EvaluatorConfig(
                evaluator=parent.name,
                version=version.version,
                params=params_by_token.get(token, {}),
            )
        )
        identities.append(
            {
                "id": str(parent.id),
                "name": parent.name,
                "version_id": str(version.id),
                "version": version.version,
            }
        )
    return configs, identities, version_ids


async def upload_plugin_source(
    client: Any, source: PluginSourceInput
) -> tuple[ScriptPluginSource | PackagePluginSource, Any | None]:
    """Upload script bytes when needed and build an API plugin source."""
    if isinstance(source, PackageSource):
        return (
            PackagePluginSource(
                requirement=source.requirement, entrypoint=source.entrypoint
            ),
            None,
        )
    blob = await client.blobs.upload(
        source.content,
        media_type="text/x-python",
        filename=source.path.name,
    )
    return ScriptPluginSource(blob_id=blob.id, entrypoint=source.entrypoint), blob


async def register_agent(
    client: Any,
    parent_request: AgentCreateRequest,
    version_request: AgentVersionCreateRequest,
) -> CommandResult:
    """Create an agent parent followed by its initial version."""
    parent = await client.agents.create(parent_request)
    try:
        version = await client.agents.create_version(parent.id, version_request)
    except Exception as error:
        raise _partial_failure(
            "agent",
            parent,
            error,
            hint=(
                f"The agent exists. Retry with `kitaru agent version register "
                f"{shlex.quote(str(parent.id))} ...`."
            ),
        ) from error
    return _registration_result("agent", parent, version)


async def register_agent_version(
    client: Any, reference: str, request: AgentVersionCreateRequest
) -> CommandResult:
    """Resolve an agent and create its next server-assigned version."""
    parent = await resolve_asset(client.agents, reference, "Agent")
    version = await client.agents.create_version(parent.id, request)
    return _registration_result("agent", parent, version)


async def register_plugin(
    client: Any,
    *,
    kind: str,
    parent_request: ImporterCreateRequest | EvaluatorCreateRequest,
    source: PluginSourceInput,
    display_version: str | None,
) -> CommandResult:
    """Create an importer/evaluator parent, upload if needed, then create a version."""
    resource = _plugin_resource(client, kind)
    parent = await resource.create(parent_request)
    blob = None
    try:
        plugin_source, blob = await upload_plugin_source(client, source)
        request_type = (
            ImporterVersionCreateRequest
            if kind == "importer"
            else EvaluatorVersionCreateRequest
        )
        version = await resource.create_version(
            parent.id,
            request_type(source=plugin_source, display_version=display_version),
        )
    except Exception as error:
        raise _partial_failure(
            kind,
            parent,
            error,
            blob=blob,
            hint=(
                f"The {kind} exists. Retry with `kitaru {kind} version register "
                f"{shlex.quote(str(parent.id))} ...`."
            ),
        ) from error
    return _registration_result(kind, parent, version, blob=blob)


async def register_plugin_version(
    client: Any,
    *,
    kind: str,
    reference: str,
    source: PluginSourceInput,
    display_version: str | None,
) -> CommandResult:
    """Resolve a plugin parent, upload if needed, and create a version."""
    resource = _plugin_resource(client, kind)
    parent = await resolve_asset(resource, reference, kind.title())
    plugin_source, blob = await upload_plugin_source(client, source)
    request_type = (
        ImporterVersionCreateRequest
        if kind == "importer"
        else EvaluatorVersionCreateRequest
    )
    try:
        version = await resource.create_version(
            parent.id,
            request_type(source=plugin_source, display_version=display_version),
        )
    except Exception as error:
        if blob is None:
            raise
        raise _partial_failure(
            kind,
            parent,
            error,
            blob=blob,
            hint="The blob is stored and may be reused; no version was created.",
        ) from error
    return _registration_result(kind, parent, version, blob=blob)


def plugin_parent_request(
    kind: str,
    name: str,
    *,
    description: str | None,
    provider: str | None,
    metadata: str | None,
) -> ImporterCreateRequest | EvaluatorCreateRequest:
    """Build one kind-specific plugin parent request."""
    parsed_metadata = parse_json_object(metadata, option="--metadata")
    if kind == "importer":
        return ImporterCreateRequest(
            name=name,
            description=description,
            provider=provider,
            metadata=parsed_metadata,
        )
    if provider is not None:
        raise CLIError("invalid_arguments", "--provider is only valid for importers.")
    return EvaluatorCreateRequest(
        name=name, description=description, metadata=parsed_metadata
    )


def list_params(
    kind: Literal[
        "agent",
        "annotation",
        "cohort",
        "evaluation",
        "evaluator",
        "experiment",
        "experiment_run",
        "importer",
        "investigation",
        "replay",
        "session",
    ],
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> ListParams:
    """Build a kind-specific list request."""
    request_type = {
        "agent": AgentListParams,
        "annotation": AnnotationListParams,
        "cohort": CohortListParams,
        "evaluation": EvaluationListParams,
        "evaluator": EvaluatorListParams,
        "experiment": ExperimentListParams,
        "experiment_run": ExperimentRunListParams,
        "importer": ImporterListParams,
        "investigation": InvestigationListParams,
        "replay": ReplayListParams,
        "session": SessionListParams,
    }[kind]
    return build_list_params(
        request_type,
        size=size,
        cursor=cursor,
        sort=sort,
        filter=filter,
    )


def version_list_params(*, size: int, cursor: str | None, sort: str) -> ListParams:
    """Build shared version-list parameters."""
    _validate_created_sort(sort)
    try:
        return ListParams(size=size, cursor=cursor, sort=sort)
    except ValidationError as error:
        raise _list_validation_error(error) from None


def build_list_params(
    request_type: type[ListParamsT],
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> ListParamsT:
    """Build filterable list params against the shared server sort contract."""
    _validate_created_sort(sort)
    try:
        return request_type.model_validate(
            {
                "size": size,
                "cursor": cursor,
                "sort": sort,
                "filter": filter,
            }
        )
    except ValidationError as error:
        raise _list_validation_error(error) from None


def _validate_created_sort(sort: str) -> None:
    """Validate the shared server sort allowlist used by collection reads."""
    if sort not in {"created:asc", "created:desc"}:
        raise CLIError(
            "invalid_arguments",
            "--sort must be created:asc or created:desc.",
            hint="Use --filter to narrow results without changing their order.",
        )


def _list_validation_error(error: ValidationError) -> CLIError:
    """Convert list-model validation into a concise option-named error."""
    issue = error.errors(include_url=False)[0]
    field = str(issue["loc"][0]) if issue["loc"] else "parameters"
    if field == "filter":
        message = "--filter must be a valid JSON filter expression."
    elif field == "size":
        message = "--size must be between 1 and 1000."
    elif field == "sort":
        message = "--sort must be created:asc or created:desc."
    else:
        message = f"Invalid --{field.replace('_', '-')}: {issue['msg']}."
    return CLIError("invalid_arguments", message)


def _load_document(path: Path) -> dict[str, Any]:
    """Read one YAML or JSON mapping."""
    if not path.exists() or not path.is_file():
        raise CLIError(
            "invalid_arguments", f"Spec {str(path)!r} is not a regular file."
        )
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise CLIError(
            "invalid_arguments", f"Could not read spec {str(path)!r}: {error}"
        ) from error
    if not isinstance(data, dict):
        raise CLIError("invalid_arguments", "Spec must contain one mapping document.")
    return data


def _validate_agent_version(request: AgentVersionCreateRequest) -> None:
    """Require a nonblank run command for CLI-registered versions."""
    if request.run_spec is None or not request.run_spec.command.strip():
        raise CLIError(
            "invalid_arguments",
            "Agent specs must include a nonblank version.run_spec.command.",
        )


def _top_level_names(tree: ast.Module) -> set[str]:
    """Return names bound by top-level statements without executing them."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def _plugin_resource(client: Any, kind: str) -> Any:
    """Return the SDK resource for one plugin kind."""
    if kind == "importer":
        return client.importers
    if kind == "evaluator":
        return client.evaluators
    raise ValueError(f"Unsupported plugin kind {kind!r}")


def _registration_result(
    kind: str, parent: Any, version: Any, *, blob: Any | None = None
) -> CommandResult:
    """Build a structured multi-phase registration receipt."""
    phases: dict[str, Any] = {
        "parent": {"completed": True, "id": str(parent.id)},
        "version": {
            "completed": True,
            "id": str(version.id),
            "version": version.version,
        },
    }
    if blob is not None:
        phases["blob"] = {"completed": True, "id": str(blob.id)}
    return CommandResult(
        item={
            kind: parent.model_dump(mode="json"),
            "version": version.model_dump(mode="json"),
            "phases": phases,
        }
    )


def _partial_failure(
    kind: str,
    parent: Any,
    error: Exception,
    *,
    blob: Any | None = None,
    hint: str,
) -> CLIError:
    """Describe completed registration phases without deleting remote state."""
    details: dict[str, Any] = {
        "asset_type": kind,
        "parent": {"completed": True, "id": str(parent.id)},
        "version": {"completed": False},
        "cause": str(error),
    }
    if blob is not None:
        details["blob"] = {"completed": True, "id": str(blob.id)}
    return CLIError(
        "partial_failure",
        f"Created the {kind} parent but did not complete registration.",
        details=details,
        hint=hint,
    )
