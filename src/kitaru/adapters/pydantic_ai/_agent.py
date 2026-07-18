import asyncio
import inspect
import os
import sys
import tempfile
import threading
import time
import uuid
import warnings
import weakref
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import (
    asynccontextmanager,
    contextmanager,
)
from contextvars import ContextVar
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import kitaru
from zenml.client import Client

from kitaru._agent_registration import (
    RegistrationIdentity,
    RegisteredAgentVersionBinding,
    build_agent_version_pipeline_name,
    canonicalize_registration_value,
    find_exact_project_pipeline,
    hash_registration_value,
    identity_drift_categories,
    qualified_declared_path,
    qualified_import_path,
    resolve_agent_entrypoint,
    resolve_registration_identity,
    type_import_path,
)
from kitaru._config._agents import (
    AgentRegistrationResult,
    _AgentVersionManifest,
    _agent_info_from_project_model,
    _complete_project_metadata,
    _manifest_for_fingerprint,
    _parse_agent_metadata,
    _reconcile_agent_version_registration,
)
from kitaru.cohort import CohortResult
from kitaru.config import ImageInput
from kitaru._experiments import (
    ExperimentReplayResult,
    ReplayTrialPlan,
    execute_replay_attempt,
    freeze_replay_attempt,
    preplan_replay_attempt,
)
from kitaru._config._projects import (
    _active_project_id,
    _active_project_model,
    _connected_store_url_is_known_pro_cloud,
    _get_project_by_exact_selector,
)
from kitaru._repository import find_repository_root
from kitaru.replay import ExperimentReplayContext
from kitaru.scoring import scorer_snapshot
from kitaru.scoring._evaluation import ScoreEvaluationService
from kitaru._source_aliases import build_pipeline_registration_name
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import (
    KitaruMetadataConflictError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.flow import (
    _is_multiple_terminal_steps_output_error,
    _temporary_active_project,
)

from pydantic_ai import _utils, messages as _messages, models, usage as _usage
from pydantic_ai.agent import AbstractAgent, AgentRun, WrapperAgent
from pydantic_ai.agent.abstract import (
    AgentInstructions,
    AgentMetadata,
    AgentModelSettings,
    EventStreamHandler,
)
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.capabilities import CombinedCapability
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT, OutputSpec
from pydantic_ai.tools import AgentDepsT, AgentNativeTool, DeferredToolResults
from pydantic_ai.toolsets import AbstractToolset
from pydantic_ai.mcp import MCPServer, MCPToolset
from pydantic_ai.toolsets import FunctionToolset

from ._constants import (
    ADAPTER_CHECKPOINT_KIND_TURN,
    ARTIFACT_SLOT_OUTPUT,
)
from ._events import PydanticAIUsageSummary
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._logging import logger
from ._mcp_server import has_running_mcp_toolset
from ._model import KitaruModel, model_cache_run_context
from ._policy import CapturePolicy
from ._streaming import (
    PydanticAIStreamPublisher,
    current_stream_surface,
    stream_surface,
)
from ._toolset import KitaruToolset, kitaruify_toolset
from ._threading_compat import inline_sync_tool_execution as _inline_sync_tool_execution
from ._tracking import get_current_tracker, tracker_scope
from ._utils import (
    CheckpointConfig,
    CheckpointStrategy,
    ToolCheckpointOverrides,
    adapter_streaming_fallback_checkpoint,
    checkpoint_input_value,
    has_any_explicit_tool_checkpoint_opt_out,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    turn_cache_key,
    validate_checkpoint_config,
    with_adapter_checkpoint_metadata,
    validate_checkpoint_strategy,
    validate_tool_checkpoint_overrides,
)

# Pydantic AI renamed "built-in tools" to "native tools" in v1.95 while
# keeping the old ``builtin_tools=`` runtime keyword as a deprecated alias.
# Kitaru still exposes the old keyword for compatibility, so keep the local
# type name stable while importing the new upstream type.
AgentBuiltinTool = AgentNativeTool
_UPSTREAM_RUN_RETRIES_PARAM = (
    "retries"
    if "retries" in inspect.signature(AbstractAgent.run).parameters
    else "output_retries"
)

_TRACKING_ACTIVE: ContextVar[bool] = ContextVar("kitaru_tracking_active", default=False)
_INTERNAL_ITER_ALLOWED: ContextVar[bool] = ContextVar(
    "kitaru_internal_iter_allowed", default=False
)
_INTERNAL_RUN_SYNC_DELEGATION: ContextVar[bool] = ContextVar(
    "kitaru_internal_run_sync_delegation", default=False
)


@dataclass(frozen=True)
class _TurnCheckpointCallConfig:
    cache_key: str
    checkpoint_inputs: dict[str, Any]
    checkpoint_config: CheckpointConfig
    force_turn_checkpoint: bool
    mark_streaming_fallback_checkpoint: bool


@dataclass(frozen=True)
class _RegisteredAgentState:
    repo_root: Path
    identity: RegistrationIdentity
    binding: RegisteredAgentVersionBinding


_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "access_key",
        "access_key_id",
        "access_token",
        "api_key",
        "apikey",
        "api_token",
        "auth_token",
        "authorization",
        "bearer_token",
        "certificate",
        "cert",
        "client_secret",
        "credential",
        "credentials",
        "oauth_client_secret",
        "password",
        "private_key",
        "proxy_authorization",
        "secret",
        "secret_access_key",
        "token",
        "x_api_key",
    }
)
_SENSITIVE_HEADER_NAMES = _SENSITIVE_FIELD_NAMES | {
    "cookie",
    "proxy_authenticate",
    "set_cookie",
}
_SENSITIVE_QUERY_PARAMETER_NAMES = _SENSITIVE_FIELD_NAMES | {
    "auth",
    "key",
    "sig",
    "signature",
    "x_amz_credential",
    "x_amz_security_token",
    "x_amz_signature",
}
_HEADER_FLAGS = frozenset({"--header", "-H"})
_HEADER_MAPPING_NAMES = frozenset({"headers", "http_headers"})
_PROVIDER_URI_FIELD_NAMES = frozenset(
    {
        "api_base",
        "app_url",
        "azure_endpoint",
        "base_url",
        "endpoint_url",
    }
)
_BASE_URL_PROVIDER_PROJECTION = {
    "base_url": ("base_url", "client.base_url"),
}
_PROVIDER_BEHAVIOR_PROJECTIONS: dict[str, dict[str, tuple[str, ...]]] = {
    provider_type: _BASE_URL_PROVIDER_PROJECTION
    for provider_type in {
        "pydantic_ai.providers.alibaba:AlibabaProvider",
        "pydantic_ai.providers.anthropic:AnthropicProvider",
        "pydantic_ai.providers.cerebras:CerebrasProvider",
        "pydantic_ai.providers.cohere:CohereProvider",
        "pydantic_ai.providers.deepseek:DeepSeekProvider",
        "pydantic_ai.providers.fireworks:FireworksProvider",
        "pydantic_ai.providers.github:GitHubProvider",
        "pydantic_ai.providers.google_gla:GoogleGLAProvider",
        "pydantic_ai.providers.grok:GrokProvider",
        "pydantic_ai.providers.groq:GroqProvider",
        "pydantic_ai.providers.heroku:HerokuProvider",
        "pydantic_ai.providers.huggingface:HuggingFaceProvider",
        "pydantic_ai.providers.litellm:LiteLLMProvider",
        "pydantic_ai.providers.mistral:MistralProvider",
        "pydantic_ai.providers.moonshotai:MoonshotAIProvider",
        "pydantic_ai.providers.nebius:NebiusProvider",
        "pydantic_ai.providers.ollama:OllamaProvider",
        "pydantic_ai.providers.openai:OpenAIProvider",
        "pydantic_ai.providers.openrouter:OpenRouterProvider",
        "pydantic_ai.providers.ovhcloud:OVHcloudProvider",
        "pydantic_ai.providers.sambanova:SambaNovaProvider",
        "pydantic_ai.providers.together:TogetherProvider",
        "pydantic_ai.providers.vercel:VercelProvider",
        "pydantic_ai.providers.voyageai:VoyageAIProvider",
        "pydantic_ai.providers.xai:XaiProvider",
    }
}
_PROVIDER_BEHAVIOR_PROJECTIONS.update(
    {
        "pydantic_ai.providers.azure:AzureProvider": {
            **_BASE_URL_PROVIDER_PROJECTION,
            "api_version": ("client._api_version",),
            "azure_deployment": ("client._azure_deployment",),
            "azure_endpoint": ("client._azure_endpoint",),
        },
        "pydantic_ai.providers.bedrock:BedrockProvider": {
            "base_url": ("base_url",),
            "endpoint_url": ("client.meta.endpoint_url",),
            "region_name": ("client.meta.region_name",),
        },
        "pydantic_ai.providers.google:GoogleProvider": {
            **_BASE_URL_PROVIDER_PROJECTION,
            "location": ("client._api_client.location",),
            "project": ("client._api_client.project",),
            "vertexai": ("client.vertexai", "client._api_client.vertexai"),
        },
        "pydantic_ai.providers.google_cloud:GoogleCloudProvider": {
            **_BASE_URL_PROVIDER_PROJECTION,
            "location": ("client._api_client.location",),
            "project": ("client._api_client.project",),
            "vertexai": ("client.vertexai", "client._api_client.vertexai"),
        },
        "pydantic_ai.providers.google_vertex:GoogleVertexProvider": {
            **_BASE_URL_PROVIDER_PROJECTION,
            "model_publisher": ("model_publisher",),
            "project_id": ("project_id",),
            "region": ("region",),
        },
        "pydantic_ai.providers.outlines:OutlinesProvider": {},
        "pydantic_ai.providers.sentence_transformers:SentenceTransformersProvider": {},
    }
)
_PROVIDER_VALUE_MISSING = object()
_SENSITIVE_FIELD_SUFFIXES = tuple(
    tuple(name.split("_")) for name in _SENSITIVE_FIELD_NAMES
)
_SENSITIVE_HEADER_SUFFIXES = tuple(
    tuple(name.split("_")) for name in _SENSITIVE_HEADER_NAMES
)
_SENSITIVE_QUERY_PARAMETER_SUFFIXES = tuple(
    tuple(name.split("_")) for name in _SENSITIVE_QUERY_PARAMETER_NAMES
)


def _normalized_field_name(value: Any) -> str:
    return str(value).strip().lstrip("-").split("=", 1)[0].lower().replace("-", "_")


def _matches_sensitive_suffix(
    value: Any,
    suffixes: Sequence[tuple[str, ...]],
) -> bool:
    segments = tuple(
        segment for segment in _normalized_field_name(value).split("_") if segment
    )
    while segments[-1:] in {("file",), ("path",)}:
        segments = segments[:-1]
    return any(
        len(segments) >= len(suffix) and segments[-len(suffix) :] == suffix
        for suffix in suffixes
    )


def _is_sensitive_field(value: Any) -> bool:
    return _matches_sensitive_suffix(value, _SENSITIVE_FIELD_SUFFIXES)


def _is_sensitive_header(value: Any) -> bool:
    return _matches_sensitive_suffix(value, _SENSITIVE_HEADER_SUFFIXES)


def _is_sensitive_query_parameter(value: Any) -> bool:
    return _matches_sensitive_suffix(value, _SENSITIVE_QUERY_PARAMETER_SUFFIXES)


def _safe_uri_fragment(value: str) -> str:
    if "=" not in value:
        return value
    fragment_items = parse_qsl(value, keep_blank_values=True)
    if not any(_is_sensitive_query_parameter(key) for key, _item in fragment_items):
        return value
    return urlencode(
        [
            (key, item)
            for key, item in fragment_items
            if not _is_sensitive_query_parameter(key)
        ],
        doseq=True,
    )


def _safe_uri(value: str) -> str:
    """Remove user-info and credential query values from an absolute URI."""
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise KitaruUsageError(
            "Registration found a malformed URI in version-defining settings."
        ) from exc
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    has_sensitive_query = any(
        _is_sensitive_query_parameter(key) for key, _item in query_items
    )
    fragment = _safe_uri_fragment(parsed.fragment)
    has_sensitive_fragment = fragment != parsed.fragment
    if not parsed.netloc:
        if has_sensitive_query or has_sensitive_fragment:
            raise KitaruUsageError(
                "Registration cannot safely project credential query or fragment "
                "values from an ambiguous URI."
            )
        return value

    netloc = parsed.netloc.rsplit("@", 1)[-1]
    query = urlencode(
        [
            (key, item)
            for key, item in query_items
            if not _is_sensitive_query_parameter(key)
        ],
        doseq=True,
    )
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, fragment))


def _safe_headers(value: Any) -> Any:
    """Project structured headers while retaining only non-credential values."""
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key.strip():
                raise KitaruUsageError(
                    "Registration found malformed structured HTTP headers."
                )
            projected[key] = (
                None if _is_sensitive_header(key) else _safe_identity_value(item)
            )
        return projected
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        projected_items: list[list[Any]] = []
        for item in value:
            if (
                not isinstance(item, Sequence)
                or isinstance(item, (str, bytes))
                or len(item) != 2
                or not isinstance(item[0], str)
                or not item[0].strip()
            ):
                raise KitaruUsageError(
                    "Registration found malformed structured HTTP headers."
                )
            projected_items.append(
                [
                    item[0],
                    None
                    if _is_sensitive_header(item[0])
                    else _safe_identity_value(item[1]),
                ]
            )
        return projected_items
    raise KitaruUsageError("Registration found malformed structured HTTP headers.")


def _safe_identity_value(value: Any) -> Any:
    """Recursively remove credentials from version-defining values."""
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for key, item in value.items():
            if _is_sensitive_field(key):
                continue
            normalized_key = _normalized_field_name(key)
            projected[str(key)] = (
                _safe_headers(item)
                if normalized_key in _HEADER_MAPPING_NAMES
                else _safe_identity_value(item)
            )
        return projected
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_safe_identity_value(item) for item in value]
    if isinstance(value, str):
        return _safe_uri(value)
    return value


def _safe_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Exclude credential-like fields from version-defining projections."""
    if value is None:
        return {}
    return dict(_safe_identity_value(value))


def _provider_path_value(provider: Any, path: str, field_name: str) -> Any:
    value = provider
    for segment in path.split("."):
        try:
            value = getattr(value, segment)
        except AttributeError:
            return _PROVIDER_VALUE_MISSING
        except Exception as exc:
            raise KitaruUsageError(
                f"Registration cannot read provider field '{field_name}'."
            ) from exc
    return value


def _normalized_provider_identity_value(field_name: str, value: Any) -> Any:
    if field_name in _PROVIDER_URI_FIELD_NAMES:
        try:
            rendered = str(value)
        except Exception as exc:
            raise KitaruUsageError(
                f"Registration cannot normalize provider field '{field_name}'."
            ) from exc
        return _safe_uri(rendered)
    return _safe_identity_value(value)


def _provider_behavior_identity(provider: Any) -> dict[str, Any]:
    """Project explicit routing fields for a supported provider implementation."""
    provider_type = type_import_path(provider)
    field_projections = _PROVIDER_BEHAVIOR_PROJECTIONS.get(provider_type)
    if field_projections is None:
        raise KitaruUsageError(
            "Registration does not support provider implementation "
            f"{provider_type!r}; its routing configuration cannot be projected safely."
        )

    projected: dict[str, Any] = {}
    for field_name, paths in field_projections.items():
        candidates: list[Any] = []
        for path in paths:
            value = _provider_path_value(provider, path, field_name)
            if value is not _PROVIDER_VALUE_MISSING and value is not None:
                candidates.append(
                    _normalized_provider_identity_value(field_name, value)
                )
        if not candidates:
            continue
        first = candidates[0]
        if any(candidate != first for candidate in candidates[1:]):
            raise KitaruUsageError(
                f"Registration found ambiguous provider field '{field_name}'."
            )
        projected[field_name] = first

    if (
        provider_type == "pydantic_ai.providers.google:GoogleProvider"
        and projected.get("vertexai") is not True
    ):
        projected.pop("location", None)
        projected.pop("project", None)
    return projected


def _safe_environment(value: Any) -> dict[str, str | None]:
    """Retain non-secret MCP environment identity without credential values."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise KitaruUsageError(
            "Registration found malformed MCP stdio environment settings."
        )
    projected: dict[str, str | None] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(item, str):
            raise KitaruUsageError(
                "Registration found malformed MCP stdio environment settings."
            )
        projected[key] = None if _is_sensitive_field(key) else _safe_uri(item)
    return projected


def _function_tool_implementation(value: Any) -> str | dict[str, Any]:
    """Identify an importable tool or a safely canonicalized local closure."""
    try:
        return qualified_import_path(value)
    except KitaruUsageError:
        declared_path = qualified_declared_path(value)

    closure = getattr(value, "__closure__", None)
    code = getattr(value, "__code__", None)
    freevars = getattr(code, "co_freevars", ())
    if not isinstance(freevars, tuple) or (
        closure is not None and len(freevars) != len(closure)
    ):
        raise KitaruUsageError(
            "Registration found a local tool without stable closure identity."
        )

    closure_values: dict[str, Any] = {}
    for name, cell in zip(freevars, closure or (), strict=True):
        try:
            closure_values[name] = cell.cell_contents
        except ValueError as exc:
            raise KitaruUsageError(
                "Registration found a local tool with an empty closure cell."
            ) from exc

    safe_values = _safe_identity_value(closure_values)
    canonical_values = canonicalize_registration_value(safe_values)
    canonical_safe_values = canonicalize_registration_value(
        _safe_identity_value(canonical_values)
    )
    return {
        "declared_path": declared_path,
        "closure": canonical_safe_values,
    }


def _safe_command_header(value: Any) -> str:
    if not isinstance(value, str):
        raise KitaruUsageError(
            "Registration found a non-string HTTP header command argument."
        )
    name, separator, header_value = value.partition(":")
    if not separator or not name.strip():
        raise KitaruUsageError(
            "Registration cannot safely project a malformed HTTP header argument."
        )
    if _is_sensitive_header(name):
        return f"{name.strip()}:"
    return f"{name}:{_safe_uri(header_value)}"


def _safe_command_args(args: Sequence[Any]) -> list[Any]:
    """Exclude credentials from structured command arguments."""
    sanitized: list[Any] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if isinstance(arg, str) and arg in _HEADER_FLAGS:
            if index + 1 >= len(args):
                raise KitaruUsageError(
                    "Registration cannot safely project a header flag without a value."
                )
            sanitized.extend((arg, _safe_command_header(args[index + 1])))
            index += 2
            continue
        if isinstance(arg, str) and arg.startswith("--header="):
            sanitized.append(f"--header={_safe_command_header(arg.split('=', 1)[1])}")
            index += 1
            continue
        if isinstance(arg, str) and arg.startswith("-H") and arg != "-H":
            sanitized.append(f"-H{_safe_command_header(arg[2:])}")
            index += 1
            continue
        if isinstance(arg, str) and arg.startswith("-") and _is_sensitive_field(arg):
            flag, separator, _value = arg.partition("=")
            sanitized.append(flag)
            if separator:
                index += 1
                continue
            if index + 1 >= len(args) or (
                isinstance(args[index + 1], str) and args[index + 1].startswith("-")
            ):
                raise KitaruUsageError(
                    f"Registration cannot safely project credential flag {flag!r} "
                    "without an unambiguous value."
                )
            index += 2
            continue
        sanitized.append(_safe_identity_value(arg))
        index += 1
    return sanitized


def _toolset_worldview(toolset: Any) -> dict[str, Any]:
    """Project one toolset without prompts, credentials, or object reprs."""
    component = toolset.wrapped if isinstance(toolset, KitaruToolset) else toolset
    projection: dict[str, Any] = {"kind": type_import_path(component)}

    if not isinstance(component, AbstractToolset):
        raise KitaruUsageError(
            "Registration requires Pydantic AI toolsets with stable public identity."
        )
    if component.id:
        projection["id"] = component.id

    if isinstance(component, FunctionToolset):
        projected_tools: list[dict[str, Any]] = []
        for declared_name, tool in sorted(
            component.tools.items(), key=lambda item: str(item[0])
        ):
            try:
                implementation = _function_tool_implementation(tool.function)
                schema = tool.function_schema.json_schema
            except AttributeError as exc:
                raise KitaruUsageError(
                    f"Tool {declared_name!r} has no stable registration identity."
                ) from exc
            projected_tools.append(
                {
                    "name": str(declared_name),
                    "implementation": implementation,
                    "schema": schema if isinstance(schema, Mapping) else None,
                    "max_retries": tool.max_retries,
                    "strict": tool.strict,
                    "sequential": tool.sequential,
                    "requires_approval": tool.requires_approval,
                }
            )
        return {
            **projection,
            "tools": projected_tools,
            "max_retries": component.max_retries,
            "timeout": component.timeout,
            "strict": component.strict,
            "sequential": component.sequential,
            "requires_approval": component.requires_approval,
            "include_return_schema": component.include_return_schema,
        }

    if isinstance(component, MCPServer):
        component_values = vars(component)
        projection.update(
            {
                "max_retries": component.max_retries,
                "timeout": component.timeout,
                "cache_prompts": component.cache_prompts,
                "cache_tools": component.cache_tools,
                "cache_resources": component.cache_resources,
                "include_instructions": component.include_instructions,
                "include_return_schema": component.include_return_schema,
                "allow_sampling": component.allow_sampling,
            }
        )
        if "command" in component_values and "args" in component_values:
            command = component_values["command"]
            args = component_values["args"]
            if (
                not isinstance(command, str)
                or not isinstance(args, Sequence)
                or isinstance(args, (str, bytes))
            ):
                raise KitaruUsageError(
                    "Registration found malformed MCP stdio transport settings."
                )
            cwd = component_values.get("cwd")
            env = component_values.get("env")
            safe_env = _safe_environment(env)
            projection.update(
                {
                    "command": command,
                    "args_hash": hash_registration_value(_safe_command_args(args)),
                    "cwd": str(cwd) if cwd is not None else None,
                    "env_keys": sorted(safe_env),
                    "env_hash": hash_registration_value(safe_env),
                }
            )
        elif "url" in component_values:
            projection.update(
                {
                    "url": _safe_uri(str(component_values["url"])),
                    "headers_hash": hash_registration_value(
                        _safe_headers(component_values.get("headers", {}))
                    ),
                }
            )
        else:
            raise KitaruUsageError(
                "Registration does not support this MCP server transport."
            )
        return projection

    if isinstance(component, MCPToolset):
        if not isinstance(component.client, (str, Path)):
            raise KitaruUsageError(
                "Registration requires MCPToolset clients with a stable path or URL."
            )
        return {
            **projection,
            "client": (
                _safe_uri(component.client)
                if isinstance(component.client, str)
                else str(component.client)
            ),
            "max_retries": component.max_retries,
            "cache_prompts": component.cache_prompts,
            "cache_tools": component.cache_tools,
            "cache_resources": component.cache_resources,
            "include_instructions": component.include_instructions,
            "include_return_schema": component.include_return_schema,
        }

    raise KitaruUsageError(
        f"Registration does not support toolset type {type_import_path(component)}."
    )


# Auto-flow bodies keyed by uuid. The @kitaru.flow entrypoint must be module-
# level for ZenML dynamic-pipeline source resolution, so it can't close over
# its body — the registry bridges the gap. In-process only; remote stacks
# require an explicit @kitaru.flow.
_AUTO_FLOW_BODIES: dict[str, "_AutoFlowSlot"] = {}
# Generated module entrypoints remain installed for ZenML source resolution,
# while flow definitions are weakly retained and recreated on demand.
_AUTO_FLOW_DEFINITIONS: weakref.WeakValueDictionary[str, Any] = (
    weakref.WeakValueDictionary()
)
_AUTO_FLOW_LOCK = threading.Lock()

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def _strategy_from_granular_checkpoints(
    granular_checkpoints: bool,
) -> CheckpointStrategy:
    return "calls" if granular_checkpoints else "turn"


def _resolve_checkpoint_strategy(
    *,
    checkpoint_strategy: CheckpointStrategy | None,
    granular_checkpoints: bool | None,
) -> CheckpointStrategy:
    if checkpoint_strategy is None:
        if granular_checkpoints is None:
            return "calls"
        return _strategy_from_granular_checkpoints(granular_checkpoints)

    validated_strategy = validate_checkpoint_strategy(checkpoint_strategy)
    if granular_checkpoints is None:
        return validated_strategy

    mapped_from_bool = _strategy_from_granular_checkpoints(granular_checkpoints)
    if mapped_from_bool != validated_strategy:
        raise KitaruUsageError(
            "`checkpoint_strategy` and `granular_checkpoints` conflict. "
            'Use `checkpoint_strategy="calls"` with `granular_checkpoints=True`, '
            'or `checkpoint_strategy="turn"` with `granular_checkpoints=False`.'
        )
    return validated_strategy


def _builtin_tools_kwargs(
    builtin_tools: Sequence[AgentBuiltinTool[Any]] | None,
) -> dict[str, Sequence[AgentBuiltinTool[Any]]]:
    """Return deprecated upstream kwargs only when the caller supplied tools."""
    if builtin_tools is None:
        return {}
    return {"builtin_tools": builtin_tools}


@contextmanager
def _maybe_suppress_model_stream_live_events(
    model: KitaruModel,
    event_stream_handler: EventStreamHandler[Any] | None,
) -> Iterator[None]:
    if event_stream_handler is None:
        yield
        return
    with model.suppress_live_stream_events(claim_first_stream_task=True):
        yield


class _AutoFlowSlot:
    __slots__ = ("body", "error", "has_result", "result")

    def __init__(self, body: Callable[[], Any]) -> None:
        self.body = body
        self.result: Any = None
        self.error: BaseException | None = None
        self.has_result = False


def _load_auto_flow_body(serialized_body_path: str) -> Callable[[], Any]:
    try:
        import cloudpickle
    except ImportError as error:  # pragma: no cover - depends on env packaging
        raise KitaruUsageError(
            "Auto-flow requires `cloudpickle` in the local runtime environment."
        ) from error
    with open(serialized_body_path, "rb") as stream:
        return cloudpickle.load(stream)


def _try_serialize_auto_flow_body(body: Callable[[], Any]) -> str | None:
    """Best-effort cloudpickle of ``body`` for remote-stack workers; returns ``None`` on failure."""
    try:
        import cloudpickle
    except ImportError:
        return None
    path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".kitaru-autoflow"
        ) as stream:
            path = stream.name
            cloudpickle.dump(body, stream)
        return path
    except Exception:
        logger.debug(
            "Auto-flow body could not be cloudpickled; remote stacks will need "
            "an explicit `@kitaru.flow` wrapper.",
            exc_info=True,
        )
        if path is not None:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        return None


def _run_auto_flow_body(run_id: str, serialized_body_path: str | None = None) -> Any:
    with _AUTO_FLOW_LOCK:
        slot = _AUTO_FLOW_BODIES.get(run_id)
    if slot is None and serialized_body_path is not None:
        slot = _AutoFlowSlot(_load_auto_flow_body(serialized_body_path))
    if slot is None:
        raise KitaruUsageError(
            f"Kitaru auto-flow body {run_id!r} not found in registry. Auto-flow "
            "is local-only; wrap your agent call in an explicit `@kitaru.flow` "
            "for remote stacks."
        )
    try:
        slot.result = slot.body()
        slot.has_result = True
        return slot.result
    except Exception as exc:
        slot.error = exc
        raise


# Keep the original importable auto-flow for runs recorded before per-agent
# auto-flow names existed.
@kitaru.flow
def _kitaru_pydantic_ai_auto_flow(
    run_id: str, serialized_body_path: str | None = None
) -> Any:
    return _run_auto_flow_body(run_id, serialized_body_path)


def _auto_flow_name_for_agent(agent_name: str) -> str:
    return build_pipeline_registration_name(f"{agent_name}_flow")


def _install_auto_flow_source_entrypoint(
    flow_name: str,
    entrypoint: Callable[[str, str | None], Any],
) -> None:
    # ZenML reloads dynamic pipelines by importing module attributes; install
    # generated entrypoints before decoration so Kitaru can source-alias them.
    setattr(sys.modules[__name__], flow_name, entrypoint)


def _make_auto_flow_entrypoint(flow_name: str) -> Callable[[str, str | None], Any]:
    def _auto_flow_entrypoint(
        run_id: str, serialized_body_path: str | None = None
    ) -> Any:
        return _run_auto_flow_body(run_id, serialized_body_path)

    _auto_flow_entrypoint.__name__ = flow_name
    _auto_flow_entrypoint.__qualname__ = flow_name
    _auto_flow_entrypoint.__module__ = __name__
    return _auto_flow_entrypoint


def _auto_flow_for_agent(
    agent_name: str,
    *,
    pipeline_name: str | None = None,
) -> Any:
    flow_name = pipeline_name or _auto_flow_name_for_agent(agent_name)
    with _AUTO_FLOW_LOCK:
        flow_definition = _AUTO_FLOW_DEFINITIONS.get(flow_name)
        if flow_definition is not None:
            return flow_definition

        entrypoint = _make_auto_flow_entrypoint(flow_name)
        _install_auto_flow_source_entrypoint(flow_name, entrypoint)
        flow_definition = kitaru.flow(entrypoint)
        _AUTO_FLOW_DEFINITIONS[flow_name] = flow_definition
        return flow_definition


def _is_wrapped_handler(handler: Any) -> bool:
    if getattr(handler, "_kitaru_wrapped", False):
        return True
    inner = getattr(handler, "func", None) or getattr(handler, "__func__", None)
    return bool(inner is not None and getattr(inner, "_kitaru_wrapped", False))


_STREAMING_HOOK_ATTRS = (
    "on_event",
    "on_run_event_stream",
    "event",
    "run_event_stream",
)
_STREAMING_HOOK_REGISTRY_KEYS = ("_on_event", "wrap_run_event_stream")


def _capabilities_imply_streaming_hooks(
    capabilities: Sequence[AbstractCapability[Any]] | None,
) -> bool:
    if capabilities is None:
        return False

    for capability in capabilities:
        if getattr(capability, "has_wrap_run_event_stream", False):
            return True

        registry = getattr(capability, "_registry", None)
        if isinstance(registry, Mapping) and any(
            registry.get(key) for key in _STREAMING_HOOK_REGISTRY_KEYS
        ):
            return True

        for attr in _STREAMING_HOOK_ATTRS:
            value = getattr(capability, attr, None)
            if value is not None and value is not _utils.UNSET:
                return True

    return False


def _resolve_run_retries(
    *,
    output_retries: int | None,
    retries: Any,
) -> Any:
    """Return the effective PydanticAI retry override for a run call."""
    if output_retries is not None and retries is not None:
        raise KitaruUsageError(
            "Pass only one of `output_retries` or `retries` to a PydanticAI "
            "agent run. `output_retries` is the legacy name; `retries` is the "
            "current PydanticAI name."
        )
    if retries is not None:
        return retries
    return output_retries


def _upstream_run_kwargs(
    *,
    conversation_id: str | None,
    retries: Any,
) -> dict[str, Any]:
    """Return PydanticAI run kwargs only when callers explicitly set them."""
    kwargs: dict[str, Any] = {}
    if conversation_id is not None:
        kwargs["conversation_id"] = conversation_id
    if retries is not None:
        kwargs[_UPSTREAM_RUN_RETRIES_PARAM] = retries
    return kwargs


def _track_run_completed(method: str, error: BaseException | None) -> None:
    if error is None:
        status = "completed"
    elif isinstance(error, asyncio.CancelledError):
        status = "cancelled"
    else:
        status = "failed"
    payload: dict[str, Any] = {"method": method, "status": status}
    if error is not None:
        payload["error_type"] = type(error).__name__
    track(AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED, payload)


def _verified_replay_execution_ids(result: ExperimentReplayResult) -> list[str]:
    """Return the exact verified child set, recovering it for terminal retries."""
    submitted_ids = [
        row.replay_exec_id
        for row in result.submission.results
        if row.membership_verified is True
    ]
    expected_count = result.record.counts.verified
    if submitted_ids:
        if len(submitted_ids) != expected_count:
            raise KitaruStateError(
                "Replay scoring received an incomplete verified child projection."
            )
        return submitted_ids

    unverified_ids = {
        issue.child_execution_id
        for issue in result.record.unverified_children
        if issue.child_execution_id is not None
    }
    recovered_ids: list[str] = []
    seen_ids: set[str] = set()
    page_number = 1
    page_size = 50
    while True:
        page = result.runs.list(page=page_number, size=page_size)
        items = getattr(page, "items", None)
        if items is None or callable(items):
            raise KitaruStateError(
                "Replay scoring received an unexpected member-run response."
            )
        page_items = list(items)
        for run in page_items:
            run_id = str(getattr(run, "id", "")).strip()
            if not run_id:
                raise KitaruStateError(
                    "Replay scoring found a member without an execution ID."
                )
            if run_id in seen_ids:
                raise KitaruStateError(
                    "Replay scoring found duplicate experiment members."
                )
            seen_ids.add(run_id)
            if run_id not in unverified_ids:
                recovered_ids.append(run_id)
        if len(page_items) < page_size:
            break
        page_number += 1

    if len(recovered_ids) != expected_count:
        raise KitaruStateError(
            "Replay scoring could not recover the complete verified child set."
        )
    if not recovered_ids:
        raise KitaruStateError(
            "Replay scoring requires at least one verified child execution."
        )
    return recovered_ids


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
        capture: CapturePolicy | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        turn_checkpoint_config: CheckpointConfig | None = None,
        checkpoint_strategy: CheckpointStrategy | None = None,
        granular_checkpoints: bool | None = None,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
        mcp_checkpoint_config: CheckpointConfig | None = None,
        persist_message_history: bool = False,
        allow_sync_tool_body_waits: bool = False,
        cost_calculator: Callable[[PydanticAIUsageSummary], float | None] | None = None,
    ) -> None:
        """Wrap an agent so its runs become durable under Kitaru.

        Outside a flow, ``run()`` / ``run_sync()`` auto-open a ``@kitaru.flow``.

        **Calls strategy (default):** no turn checkpoint; each top-level
        model/tool/MCP call per turn opens its own checkpoint, giving per-call
        replay/retry boundaries and a less crowded artifact view. Sub-calls
        nested inside an already-open granular checkpoint (for example tool
        calls inside the turn's model request) fall back to inline tracking —
        they do *not* open a second checkpoint. The ``model_/tool_/mcp_checkpoint_config``
        kwargs (and the per-tool ``tool_checkpoint_config_by_name`` map, where
        ``False`` opts a tool out entirely) are honored in this strategy.
        ``granular_checkpoints=True`` is kept as a legacy-compatible alias.
        Cross-run cache behavior for adapter-created granular checkpoints is
        still being tightened.

        **Turn strategy** (``checkpoint_strategy="turn"`` or legacy
        ``granular_checkpoints=False``): each run opens one
        ``@kitaru.checkpoint`` named after the agent; model/tool/MCP calls are
        recorded as child events under that checkpoint.

        If an ordinary synchronous Pydantic AI tool body calls
        ``kp.wait_for_input(...)`` directly, pass both
        ``tool_checkpoint_config_by_name={"tool_name": False}`` and
        ``allow_sync_tool_body_waits=True``. The ``False`` override only skips
        the adapter-created tool checkpoint. The explicit
        ``allow_sync_tool_body_waits`` flag asks Pydantic AI to keep supported
        sync tool bodies on the workflow thread for the whole run, and Kitaru
        fails before tool execution if that private compatibility hook is not
        available.

        When ``persist_message_history=True``, the adapter remembers the final
        ``result.all_messages()`` of each run on the instance and auto-injects
        it as ``message_history`` on the next call if the caller doesn't supply
        one — one instance then represents one conversation. Pass an explicit
        ``message_history=`` to override for a single call.

        Limits of ``persist_message_history``:

        - **In-memory only**: history lives on the Python instance. Adapter-owned
          cached run results can refresh it, but restarts, new processes, and
          replay paths that skip this adapter call start with no instance history.
        - **Serial use**: concurrent ``run`` / ``run_sync`` calls on the same
          instance race on the stored history. Gate concurrency externally or
          use one instance per concurrent conversation.
        - **Unbounded**: the list grows with each successful run; apply your
          own truncation or summarization for long-lived conversations.
        - **Success-only**: history is only updated after a successful run,
          so a partial failure leaves the last-successful history in place.
        """
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise UserError(
                "KitaruAgent requires the wrapped agent to define a concrete model at construction time; "
                "pass `model=` to the Agent constructor."
            )

        self._name = name or wrapped.name
        if self._name is None:
            raise UserError(
                "KitaruAgent requires a stable `name`; pass `name=` to KitaruAgent or set the wrapped agent name."
            )
        self._capture = capture or CapturePolicy()
        self._event_stream_handler = event_stream_handler
        self._turn_checkpoint_config: CheckpointConfig = (
            validate_checkpoint_config(
                turn_checkpoint_config, context="turn_checkpoint_config"
            )
            or {}
        )
        self._checkpoint_strategy = _resolve_checkpoint_strategy(
            checkpoint_strategy=checkpoint_strategy,
            granular_checkpoints=granular_checkpoints,
        )
        self._warned_streaming_fallback = False
        self._warned_checkpoint_history_limit = False
        has_granular_configs = any(
            value is not None
            for value in (
                model_checkpoint_config,
                tool_checkpoint_config,
                tool_checkpoint_config_by_name,
                mcp_checkpoint_config,
            )
        )
        if has_granular_configs and not self._uses_calls_strategy:
            raise KitaruUsageError(
                'Per-call checkpoint configs require `checkpoint_strategy="calls"` '
                "or legacy `granular_checkpoints=True`."
            )
        if allow_sync_tool_body_waits and not self._uses_calls_strategy:
            raise KitaruUsageError(
                "`allow_sync_tool_body_waits=True` requires "
                '`checkpoint_strategy="calls"` or legacy '
                "`granular_checkpoints=True`, and a matching checkpoint opt-out "
                'such as `tool_checkpoint_config_by_name={"tool_name": False}`.'
            )
        if allow_sync_tool_body_waits and not has_any_explicit_tool_checkpoint_opt_out(
            tool_checkpoint_config_by_name
        ):
            raise KitaruUsageError(
                "`allow_sync_tool_body_waits=True` requires at least one per-tool "
                "checkpoint opt-out such as "
                '`tool_checkpoint_config_by_name={"tool_name": False}`. '
                "The opt-out keeps `kp.wait_for_input(...)` out of a synthetic "
                "tool checkpoint; the flag only controls Pydantic AI sync-tool "
                "threading."
            )
        self._allow_sync_tool_body_waits = allow_sync_tool_body_waits
        self._cost_calculator = cost_calculator
        if self._uses_calls_strategy:
            self._model_checkpoint_config = (
                validate_checkpoint_config(
                    model_checkpoint_config or {},
                    context="model_checkpoint_config",
                )
                or {}
            )
            self._tool_checkpoint_config = (
                validate_checkpoint_config(
                    tool_checkpoint_config or {},
                    context="tool_checkpoint_config",
                )
                or {}
            )
            self._tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = (
                validate_tool_checkpoint_overrides(
                    tool_checkpoint_config_by_name,
                    context="tool_checkpoint_config_by_name",
                )
            )
            self._mcp_checkpoint_config = (
                validate_checkpoint_config(
                    mcp_checkpoint_config or {},
                    context="mcp_checkpoint_config",
                )
                or {}
            )
        else:
            self._model_checkpoint_config = None
            self._tool_checkpoint_config = None
            self._tool_checkpoint_config_by_name = None
            self._mcp_checkpoint_config = None
        self._model = KitaruModel(
            wrapped.model,
            capture=self._capture,
            agent_name=self._name,
            checkpoint_config=self._model_checkpoint_config,
        )
        self._toolsets = self._prepare_toolsets(list(wrapped.toolsets))
        self._persist_message_history = persist_message_history
        self._last_messages: list[_messages.ModelMessage] | None = None
        self._message_history_lock = threading.Lock()
        self._registration_lock = threading.RLock()
        self._registered_state: _RegisteredAgentState | None = None
        track(
            AnalyticsEvent.PYDANTIC_AI_WRAPPED,
            {
                "toolset_count": len(self._toolsets),
                "checkpoint_strategy": self._checkpoint_strategy,
                "granular_checkpoints": self._uses_calls_strategy,
                "persist_message_history": persist_message_history,
                "allow_sync_tool_body_waits": allow_sync_tool_body_waits,
                "has_cost_calculator": cost_calculator is not None,
            },
        )

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        raise UserError(
            "The agent name cannot be changed after creation. Create a new KitaruAgent instead."
        )

    @property
    def model(self) -> Model:
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        return self._toolsets

    @property
    def event_stream_handler(self) -> EventStreamHandler[AgentDepsT] | None:
        return self._event_stream_handler or super().event_stream_handler

    @property
    def capture(self) -> CapturePolicy:
        return self._capture

    @property
    def checkpoint_strategy(self) -> CheckpointStrategy:
        return self._checkpoint_strategy

    @property
    def _uses_calls_strategy(self) -> bool:
        return self._checkpoint_strategy == "calls"

    def _registration_configuration(self) -> dict[str, Any]:
        cost_calculator = self._cost_calculator
        cost_calculator_name: str | None = None
        if cost_calculator is not None:
            cost_calculator_name = qualified_import_path(cost_calculator)

        return {
            "adapter": "pydantic_ai",
            "name": self._name,
            "capture": _safe_mapping(vars(self._capture)),
            "checkpoint_strategy": self._checkpoint_strategy,
            "turn_checkpoint_config": _safe_mapping(self._turn_checkpoint_config),
            "model_checkpoint_config": _safe_mapping(self._model_checkpoint_config),
            "tool_checkpoint_config": _safe_mapping(self._tool_checkpoint_config),
            "tool_checkpoint_config_by_name": _safe_mapping(
                self._tool_checkpoint_config_by_name
            ),
            "mcp_checkpoint_config": _safe_mapping(self._mcp_checkpoint_config),
            "persist_message_history": self._persist_message_history,
            "allow_sync_tool_body_waits": self._allow_sync_tool_body_waits,
            "cost_calculator": cost_calculator_name,
        }

    def _registration_worldview(self) -> dict[str, Any]:
        wrapped_model = self._model.wrapped
        model_name = wrapped_model.model_name
        if not isinstance(model_name, str) or not model_name:
            raise KitaruUsageError(
                "The Pydantic AI model has no stable name for registration."
            )
        provider = wrapped_model.provider
        provider_name = provider.name if provider is not None else None
        if provider_name is not None and (
            not isinstance(provider_name, str) or not provider_name
        ):
            raise KitaruUsageError(
                "The Pydantic AI provider has no stable name for registration."
            )

        root_capability = self.wrapped.root_capability
        capabilities = (
            root_capability.capabilities
            if isinstance(root_capability, CombinedCapability)
            else [root_capability]
        )
        capability_names = sorted(
            type_import_path(capability) for capability in capabilities
        )

        output_type = self.wrapped.output_type
        if isinstance(output_type, type):
            output_identity: Any = qualified_import_path(output_type)
        elif output_type is None:
            output_identity = None
        elif isinstance(output_type, Sequence) and not isinstance(
            output_type, (str, bytes)
        ):
            output_identity = [
                qualified_import_path(item) if isinstance(item, type) else item
                for item in output_type
            ]
        else:
            output_identity = output_type

        try:
            model_settings = vars(self.wrapped)["model_settings"]
        except (KeyError, TypeError) as exc:
            raise KitaruUsageError(
                "Registration requires an Agent with explicit model settings."
            ) from exc
        if model_settings is not None and not isinstance(model_settings, Mapping):
            raise KitaruUsageError(
                "The Pydantic AI model settings are not a stable mapping."
            )

        return {
            "framework": "pydantic_ai",
            "model": {
                "kind": type_import_path(wrapped_model),
                "name": model_name,
                "provider": {
                    "kind": type_import_path(provider)
                    if provider is not None
                    else None,
                    "name": provider_name,
                    "behavior": (
                        _provider_behavior_identity(provider)
                        if provider is not None
                        else {}
                    ),
                },
                "settings": _safe_mapping(model_settings),
            },
            "tools_and_mcp": [
                _toolset_worldview(toolset) for toolset in self._toolsets
            ],
            "capabilities": capability_names,
            "output_type": output_identity,
            "replay": True,
            "checkpoint_strategy": self._checkpoint_strategy,
        }

    def _resolve_registration_identity(
        self,
        *,
        repo_root: Path,
        entrypoint: str,
    ) -> RegistrationIdentity:
        return resolve_registration_identity(
            repo_root=repo_root,
            entrypoint=entrypoint,
            configuration=self._registration_configuration(),
            worldview=self._registration_worldview(),
        )

    def _resolve_registration_project(self, client: Any) -> Any:
        """Use a named Project on Pro/Cloud and the active default Project locally."""
        is_pro = False
        try:
            detector = client.zen_store.get_store_info().is_pro_server
            is_pro = callable(detector) and detector() is True
        except Exception:
            is_pro = _connected_store_url_is_known_pro_cloud(client)

        if not is_pro:
            return _active_project_model(client)

        try:
            return _get_project_by_exact_selector(client, self._name)
        except KeyError:
            pass

        from kitaru._config import _projects as project_ops

        try:
            project_ops.create_project(
                self._name,
                description=f"Kitaru Agent {self._name}",
                activate=False,
                client_factory=lambda: client,
            )
        except Exception as create_error:
            try:
                # A concurrent creator may have won the name race.
                return _get_project_by_exact_selector(client, self._name)
            except KeyError as recovery_error:
                raise create_error from recovery_error

        return _get_project_by_exact_selector(client, self._name)

    def _resolve_bound_registration_project(
        self,
        client: Any,
        state: _RegisteredAgentState,
    ) -> Any:
        """Verify an existing immutable Project and Pipeline binding."""
        expected_project_id = state.binding.project_id
        try:
            project = _get_project_by_exact_selector(client, expected_project_id)
        except Exception as exc:
            raise KitaruStateError(
                "The registered Project is unavailable on the current connection."
            ) from exc

        project_id = str(getattr(project, "id", "")).strip()
        if project_id != expected_project_id:
            raise KitaruStateError(
                "The registered Project ID does not match the current connection."
            )

        metadata = _complete_project_metadata(project)
        envelope = _parse_agent_metadata(project_id, metadata)
        if envelope is None:
            raise KitaruStateError(
                "The registered Project is not initialized as the bound Agent."
            )
        if envelope.agent.name != self._name:
            raise KitaruMetadataConflictError(
                "The registered Project is bound to a different logical Agent name."
            )
        stored_manifest = envelope.agent_versions.get(state.binding.pipeline_id)
        if stored_manifest != state.binding.manifest:
            raise KitaruMetadataConflictError(
                "The registered Project metadata no longer matches the bound "
                "AgentVersion."
            )

        bound_pipeline = find_exact_project_pipeline(
            client,
            project_id=project_id,
            pipeline_name=state.binding.pipeline_name,
        )
        if bound_pipeline is None:
            raise KitaruStateError(
                "The registered AgentVersion Pipeline no longer exists."
            )
        if str(getattr(bound_pipeline, "id", "")) != state.binding.pipeline_id:
            raise KitaruMetadataConflictError(
                "The registered Pipeline name no longer resolves to its bound UUID."
            )
        return project

    def _registered_flow(self) -> Any:
        with self._registration_lock:
            state = self._registered_state
            if state is None:
                raise KitaruStateError(
                    "This KitaruAgent is not registered. Call agent.register() first."
                )
            flow_definition = _auto_flow_for_agent(
                self._name,
                pipeline_name=state.binding.pipeline_name,
            )
            flow_definition._bind_registered_version(state.binding)
            return flow_definition

    def _preflight_registered_identity(self) -> None:
        with self._registration_lock:
            state = self._registered_state
            if state is None:
                raise KitaruStateError(
                    "This KitaruAgent is not registered. Call agent.register() first."
                )
            resolve_agent_entrypoint(
                target=self,
                repo_root=state.repo_root,
                entrypoint=state.identity.entrypoint,
            )
            actual = self._resolve_registration_identity(
                repo_root=state.repo_root,
                entrypoint=state.identity.entrypoint,
            )
            changed = identity_drift_categories(state.identity, actual)
            if changed or actual.fingerprint != state.binding.fingerprint:
                categories = ", ".join(changed or ["fingerprint"])
                raise KitaruStateError(
                    "Agent registration is stale because these static identity "
                    f"categories changed: {categories}. Call agent.register() on "
                    "a new KitaruAgent instance before execution."
                )

    def register(
        self,
        *,
        label: str | None = None,
        entrypoint: str | None = None,
    ) -> AgentRegistrationResult:
        """Register or reuse this AgentVersion without executing the Agent."""
        normalized_label = label.strip() if label is not None else None
        if normalized_label == "":
            raise KitaruUsageError("AgentVersion label cannot be empty.")
        repo_root = find_repository_root()
        if repo_root is None:
            raise KitaruStateError(
                "Agent registration requires a Kitaru repository. Run `kitaru init`."
            )
        resolved_entrypoint = resolve_agent_entrypoint(
            target=self,
            repo_root=repo_root,
            entrypoint=entrypoint,
        )
        identity = self._resolve_registration_identity(
            repo_root=repo_root,
            entrypoint=resolved_entrypoint,
        )

        with self._registration_lock:
            current_state = self._registered_state
            if (
                current_state is not None
                and current_state.identity.fingerprint != identity.fingerprint
            ):
                raise KitaruStateError(
                    "A KitaruAgent instance cannot be rebound to a different "
                    "AgentVersion. Create a new wrapper and register it."
                )

            client = Client()
            project = (
                self._resolve_bound_registration_project(client, current_state)
                if current_state is not None
                else self._resolve_registration_project(client)
            )
            project_id = str(getattr(project, "id", "")).strip()
            project_name = str(getattr(project, "name", "")).strip()
            if not project_id or not project_name:
                raise KitaruStateError(
                    "Unable to resolve the Project identity for Agent registration."
                )
            if (
                current_state is not None
                and current_state.binding.project_id != project_id
            ):
                raise KitaruStateError(
                    "A KitaruAgent instance cannot replace its registered Project."
                )
            metadata = _complete_project_metadata(project)
            envelope = _parse_agent_metadata(project_id, metadata)
            if envelope is not None and envelope.agent.name != self._name:
                raise KitaruMetadataConflictError(
                    "The backing Project is already registered to Agent "
                    f"{envelope.agent.name!r}, not {self._name!r}."
                )
            stored_manifest = _manifest_for_fingerprint(envelope, identity.fingerprint)
            if stored_manifest is None and current_state is not None:
                stored_manifest = current_state.binding.manifest
            if normalized_label is not None and envelope is not None:
                existing_target = envelope.agent_version_aliases.get(normalized_label)
                fingerprint_target = (
                    stored_manifest.pipeline_id if stored_manifest is not None else None
                )
                if (
                    existing_target is not None
                    and existing_target != fingerprint_target
                ):
                    raise KitaruMetadataConflictError(
                        "The AgentVersion alias already points to a different version."
                    )

            if stored_manifest is not None and (
                stored_manifest.git_sha != identity.git_sha
                or stored_manifest.git_dirty != identity.git_dirty
                or stored_manifest.working_tree_hash != identity.working_tree_hash
                or stored_manifest.configuration_hash != identity.configuration_hash
                or stored_manifest.worldview_hash != identity.worldview_hash
                or stored_manifest.entrypoint != identity.entrypoint
            ):
                raise KitaruMetadataConflictError(
                    "The stored AgentVersion manifest contradicts its fingerprint."
                )
            deterministic_name = build_agent_version_pipeline_name(
                agent_name=self._name,
                identity=identity,
            )
            if (
                stored_manifest is not None
                and stored_manifest.pipeline_name != deterministic_name
            ):
                raise KitaruMetadataConflictError(
                    "The stored AgentVersion name contradicts deterministic identity."
                )
            pipeline_name = (
                stored_manifest.pipeline_name
                if stored_manifest is not None
                else deterministic_name
            )
            flow_definition = _auto_flow_for_agent(
                self._name,
                pipeline_name=pipeline_name,
            )

            pipeline_model = find_exact_project_pipeline(
                client,
                project_id=project_id,
                pipeline_name=pipeline_name,
            )
            created = stored_manifest is None
            if stored_manifest is not None:
                if pipeline_model is None:
                    raise KitaruStateError(
                        "The registered AgentVersion Pipeline no longer exists."
                    )
                if (
                    str(getattr(pipeline_model, "id", ""))
                    != stored_manifest.pipeline_id
                ):
                    raise KitaruMetadataConflictError(
                        "The Pipeline name no longer resolves to the manifest UUID."
                    )
                manifest = stored_manifest
            else:
                if pipeline_model is None:
                    with _temporary_active_project(project_id):
                        returned_pipeline = flow_definition._pipeline.register()
                    pipeline_model = find_exact_project_pipeline(
                        client,
                        project_id=project_id,
                        pipeline_name=pipeline_name,
                    )
                    if pipeline_model is None:
                        raise KitaruStateError(
                            "Pipeline registration did not create a resolvable Pipeline."
                        )
                    if str(getattr(returned_pipeline, "id", "")) != str(
                        getattr(pipeline_model, "id", "")
                    ):
                        raise KitaruMetadataConflictError(
                            "Pipeline registration returned a different UUID than "
                            "the exact project-scoped lookup."
                        )
                pipeline_id = str(getattr(pipeline_model, "id", "")).strip()
                if not pipeline_id:
                    raise KitaruStateError(
                        "Pipeline registration returned no durable UUID."
                    )
                manifest = _AgentVersionManifest(
                    schema_version=1,
                    agent_version_id=pipeline_id,
                    pipeline_id=pipeline_id,
                    pipeline_name=pipeline_name,
                    fingerprint=identity.fingerprint,
                    git_sha=identity.git_sha,
                    git_dirty=identity.git_dirty,
                    working_tree_hash=identity.working_tree_hash,
                    configuration_hash=identity.configuration_hash,
                    worldview_hash=identity.worldview_hash,
                    entrypoint=identity.entrypoint,
                    registered_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    source="registration",
                )

            _reconcile_agent_version_registration(
                project_id=project_id,
                agent_name=self._name,
                manifest=manifest,
                label=normalized_label,
                client_factory=lambda: client,
            )
            reread_project = _get_project_by_exact_selector(client, project_id)
            active_project_id = _active_project_id(client)
            agent_info = _agent_info_from_project_model(
                reread_project,
                active_project_id=active_project_id,
            )
            if agent_info is None:
                raise KitaruStateError(
                    "Agent metadata verification returned an uninitialized Agent."
                )
            version_info = next(
                (
                    version
                    for version in agent_info.agent_versions
                    if version.pipeline_id == manifest.pipeline_id
                ),
                None,
            )
            if version_info is None:
                raise KitaruStateError(
                    "Agent metadata verification did not return the registered version."
                )

            binding = RegisteredAgentVersionBinding(
                project_id=project_id,
                manifest=manifest,
            )
            flow_definition._bind_registered_version(binding)
            new_state = _RegisteredAgentState(
                repo_root=repo_root,
                identity=identity,
                binding=binding,
            )
            if current_state is not None and current_state.binding != binding:
                raise KitaruStateError(
                    "A KitaruAgent instance cannot replace its registered state."
                )
            self._registered_state = new_state
            return AgentRegistrationResult(
                agent=agent_info,
                agent_version=version_info,
                label=normalized_label,
                created=created,
            )

    def replay(
        self,
        execution: str | CohortResult | Sequence[str],
        *,
        at: str,
        on_error: Literal["collect", "fail"],
        uncovered_policy: Literal["fail", "skip", "top"],
        idempotency_key: str,
        name: str | None = None,
        suite_key: str | None = None,
        repeats: int = 1,
        acknowledge_partial_cohort: bool = False,
        flow_overrides: Mapping[str, Any] | None = None,
        checkpoint_overrides: Mapping[str, Any] | None = None,
        invocation_overrides: Mapping[str, Any] | None = None,
        skip: Sequence[str] | None = None,
        tag: str | None = None,
        wait: bool | None = None,
        stack: str | None = None,
        image: ImageInput | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        scorers: Sequence[Any] = (),
    ) -> ExperimentReplayResult:
        """Create one durable replay attempt through the registered AgentVersion."""
        flow_definition = self._registered_flow()
        with self._registration_lock:
            state = self._registered_state
            if state is None:
                raise KitaruStateError(
                    "This KitaruAgent is not registered. Call agent.register() first."
                )
            binding = state.binding

        scorer_items = list(scorers)
        if isinstance(execution, CohortResult):
            target_count = len(execution.exec_ids)
        elif isinstance(execution, str):
            target_count = 1
        else:
            target_count = len(execution)
        if scorer_items and wait is False:
            raise KitaruUsageError(
                "Replay scoring requires terminal child evidence. Pass wait=True "
                "or omit wait so scoring can wait for replay children."
            )
        resolved_wait = (
            True
            if scorer_items and wait is None
            else target_count * repeats == 1
            if wait is None
            else wait
        )

        client = Client()
        self._preflight_registered_identity()
        scorer_snapshots = [scorer_snapshot(item) for item in scorer_items]
        draft = preplan_replay_attempt(
            execution,
            binding=binding,
            at=at,
            on_error=on_error,
            uncovered_policy=uncovered_policy,
            idempotency_key=idempotency_key,
            repeats=repeats,
            wait=resolved_wait,
            name=name,
            suite_key=suite_key,
            acknowledge_partial_cohort=acknowledge_partial_cohort,
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
            client=client,
            scorers=scorer_snapshots,
        )
        with _temporary_active_project(binding.project_id):
            plan = freeze_replay_attempt(draft, client=client)

        def submit_trial(
            *,
            trial: ReplayTrialPlan,
            replay_plan: Any,
            submission_id: str,
        ) -> Any:
            with flow_definition._registered_preflight_scope(
                self._preflight_registered_identity
            ):
                return flow_definition.replay(
                    trial.target_execution_id,
                    at=at,
                    flow_overrides=flow_overrides,
                    checkpoint_overrides=checkpoint_overrides,
                    invocation_overrides=invocation_overrides,
                    skip=skip,
                    tag=tag,
                    wait=resolved_wait,
                    on_error="collect",
                    stack=stack,
                    image=image,
                    cache=cache,
                    retries=retries,
                    replay_submission_id=submission_id,
                    preplanned_replay_plan=replay_plan,
                    experiment_context=ExperimentReplayContext(
                        experiment_id=plan.spec.experiment_id,
                        target_execution_id=trial.target_execution_id,
                        repeat_index=trial.repeat_index,
                        parent_execution_id=trial.parent_execution_id,
                        root_execution_id=trial.root_execution_id,
                    ),
                )

        result = execute_replay_attempt(
            plan,
            submit_trial=submit_trial,
            tag=tag,
            client_factory=lambda: client,
        )
        if scorer_items and result.record.score_aggregate is None:
            verified_child_ids = _verified_replay_execution_ids(result)
            score_result = ScoreEvaluationService(
                project_id=binding.project_id,
                client=client,
            ).evaluate_existing_attempt(
                experiment_id=plan.spec.experiment_id,
                executions=verified_child_ids,
                scorers=scorer_items,
            )
            result = replace(result, record=score_result.record)
        return result

    @contextmanager
    def _kitaru_overrides(self) -> Iterator[None]:
        with super().override(model=self._model, toolsets=self._toolsets, tools=[]):
            yield

    def _prepare_toolsets(
        self, toolsets: Sequence[AbstractToolset[AgentDepsT]]
    ) -> list[AbstractToolset[AgentDepsT]]:
        def _visit(value: AbstractToolset[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
            return kitaruify_toolset(
                value,
                capture=self._capture,
                tool_checkpoint_config=self._tool_checkpoint_config,
                tool_checkpoint_config_by_name=self._tool_checkpoint_config_by_name,
                mcp_checkpoint_config=self._mcp_checkpoint_config,
            )

        return [toolset.visit_and_replace(_visit) for toolset in toolsets]

    def _prepare_event_stream_handler(
        self,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
    ) -> EventStreamHandler[AgentDepsT] | None:
        effective_handler = event_stream_handler or self.event_stream_handler
        if effective_handler is None:
            return None
        if _is_wrapped_handler(effective_handler):
            return effective_handler

        async def _tracked_handler(ctx: Any, stream: AsyncIterable[Any]) -> None:
            started_at = time.perf_counter()
            error: BaseException | None = None
            event_count = 0
            publisher = PydanticAIStreamPublisher(
                agent_name=self._name,
                surface=current_stream_surface(default="event_stream_handler"),
                source="event_stream_handler",
                include_content=self._capture.save_stream_transcripts,
                enabled=self._capture.emit_child_events,
            )

            async def _live_stream() -> AsyncIterator[Any]:
                nonlocal event_count
                async for event in stream:
                    event_count += 1
                    publisher.event(event)
                    yield event

            publisher.started()
            try:
                with self._model.suppress_live_stream_events():
                    await effective_handler(ctx, _live_stream())
                publisher.completed(event_count=event_count)
            except BaseException as exc:
                error = exc
                publisher.failed(exc)
                raise
            finally:
                tracker = get_current_tracker()
                if tracker is not None:
                    tracker.record_stream_event(
                        duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                        error=error,
                    )

        _tracked_handler._kitaru_wrapped = True  # ty: ignore[unresolved-attribute]
        return _tracked_handler

    def _validate_model_override(
        self, model: models.Model | models.KnownModelName | str | None
    ) -> None:
        if model is None:
            return
        raise UserError(
            "KitaruAgent does not support per-run `model=` overrides; create a new KitaruAgent "
            "wrapping a different agent instead."
        )

    @contextmanager
    def override(
        self,
        *,
        name: str | _utils.Unset = _utils.UNSET,
        deps: AgentDepsT | _utils.Unset = _utils.UNSET,
        model: models.Model | models.KnownModelName | str | _utils.Unset = _utils.UNSET,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | _utils.Unset = _utils.UNSET,
        tools: Sequence[Any] | _utils.Unset = _utils.UNSET,
        instructions: AgentInstructions[AgentDepsT] | _utils.Unset = _utils.UNSET,
        model_settings: AgentModelSettings[AgentDepsT] | _utils.Unset = _utils.UNSET,
        spec: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        unsupported: list[str] = []
        if _utils.is_set(name):
            unsupported.append("`name=`")
        if _utils.is_set(model):
            unsupported.append("`model=`")
        if _utils.is_set(toolsets):
            unsupported.append("`toolsets=`")
        if _utils.is_set(tools):
            unsupported.append("`tools=`")
        if unsupported:
            overrides = ", ".join(unsupported)
            raise UserError(
                f"KitaruAgent does not support contextual {overrides} overrides; create a new KitaruAgent instead."
            )

        with super().override(
            deps=deps,
            instructions=instructions,
            model_settings=model_settings,
            spec=spec,
        ):
            yield

    def _should_track(self) -> bool:
        if _TRACKING_ACTIVE.get():
            return False
        if is_inside_checkpoint():
            return True
        return self._uses_calls_strategy and is_inside_flow()

    @contextmanager
    def _tracking_scope(self) -> Iterator[None]:
        if not self._should_track():
            yield
            return

        token = _TRACKING_ACTIVE.set(True)
        try:
            with tracker_scope(self._name, cost_calculator=self._cost_calculator):
                yield
        finally:
            _TRACKING_ACTIVE.reset(token)

    @contextmanager
    def _allow_internal_iter(self) -> Iterator[None]:
        token = _INTERNAL_ITER_ALLOWED.set(True)
        try:
            yield
        finally:
            _INTERNAL_ITER_ALLOWED.reset(token)

    @staticmethod
    def _turn_checkpoint_inputs(
        *,
        user_prompt: str | Sequence[_messages.UserContent] | None,
        message_history: Sequence[_messages.ModelMessage] | None,
    ) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        if user_prompt is not None:
            inputs["user_prompt"] = checkpoint_input_value(user_prompt)
        if message_history is not None:
            inputs["message_history"] = checkpoint_input_value(list(message_history))
        return inputs

    def _turn_checkpoint_config_for_call(
        self,
        *,
        disable_cache: bool,
    ) -> CheckpointConfig:
        if not disable_cache:
            return self._turn_checkpoint_config
        return {**self._turn_checkpoint_config, "cache": False}

    def _prepare_turn_checkpoint_call_config(
        self,
        *,
        user_prompt: str | Sequence[_messages.UserContent] | None,
        message_history: Sequence[_messages.ModelMessage] | None,
        deferred_tool_results: DeferredToolResults | None,
        output_type: OutputSpec[Any] | None,
        conversation_id: str | None,
        output_retries: int | None,
        instructions: AgentInstructions[AgentDepsT],
        deps: AgentDepsT | None,
        model_settings: AgentModelSettings[AgentDepsT] | None,
        usage_limits: _usage.UsageLimits | None,
        usage: _usage.RunUsage | None,
        metadata: AgentMetadata[AgentDepsT] | None,
        infer_name: bool,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None,
        spec: dict[str, Any] | None,
    ) -> _TurnCheckpointCallConfig:
        force_turn_checkpoint = (
            event_stream_handler is not None
            or _capabilities_imply_streaming_hooks(capabilities)
        )
        return _TurnCheckpointCallConfig(
            cache_key=turn_cache_key(
                agent_name=self._name,
                user_prompt=user_prompt,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                output_type=output_type,
                conversation_id=conversation_id,
                output_retries=output_retries,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=toolsets,
                builtin_tools=builtin_tools,
                event_stream_handler=event_stream_handler,
                capabilities=capabilities,
                spec=spec,
            ),
            checkpoint_inputs=self._turn_checkpoint_inputs(
                user_prompt=user_prompt,
                message_history=message_history,
            ),
            checkpoint_config=self._turn_checkpoint_config_for_call(
                disable_cache=force_turn_checkpoint,
            ),
            force_turn_checkpoint=force_turn_checkpoint,
            mark_streaming_fallback_checkpoint=(
                force_turn_checkpoint and self._uses_calls_strategy
            ),
        )

    async def _auto_checkpoint_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        mark_streaming_fallback_checkpoint: bool = False,
    ) -> Any:
        checkpoint_body = body
        if mark_streaming_fallback_checkpoint:

            async def _marked_body() -> Any:
                with adapter_streaming_fallback_checkpoint(
                    allow_sync_tool_body_waits=self._allow_sync_tool_body_waits
                ):
                    return await body()

            checkpoint_body = _marked_body

        config = with_adapter_checkpoint_metadata(
            checkpoint_config or self._turn_checkpoint_config,
            kind=ADAPTER_CHECKPOINT_KIND_TURN,
            input_slots=[],
            output_slots=[ARTIFACT_SLOT_OUTPUT],
        )
        return await run_async_in_checkpoint(
            config=config,
            step_name=self._name or "agent",
            body=checkpoint_body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _auto_checkpoint_sync(
        self,
        body: Callable[[], Any],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        mark_streaming_fallback_checkpoint: bool = False,
    ) -> Any:
        checkpoint_body = body
        if mark_streaming_fallback_checkpoint:

            def _marked_body() -> Any:
                with adapter_streaming_fallback_checkpoint(
                    allow_sync_tool_body_waits=self._allow_sync_tool_body_waits
                ):
                    return body()

            checkpoint_body = _marked_body

        config = with_adapter_checkpoint_metadata(
            checkpoint_config or self._turn_checkpoint_config,
            kind=ADAPTER_CHECKPOINT_KIND_TURN,
            input_slots=[],
            output_slots=[ARTIFACT_SLOT_OUTPUT],
        )
        return run_sync_in_checkpoint(
            config=config,
            step_name=self._name or "agent",
            body=checkpoint_body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _invoke_in_auto_flow(self, body: Callable[[], Any]) -> Any:
        flow_definition = self._registered_flow()
        run_id = uuid.uuid4().hex
        slot = _AutoFlowSlot(body)
        serialized_body_path: str | None = None
        flow_result: Any = None
        with _AUTO_FLOW_LOCK:
            _AUTO_FLOW_BODIES[run_id] = slot
        try:
            serialized_body_path = _try_serialize_auto_flow_body(body)
            with flow_definition._registered_preflight_scope(
                self._preflight_registered_identity
            ):
                handle = flow_definition.run(run_id, serialized_body_path)
            try:
                flow_result = handle.wait()
            except KitaruRuntimeError as error:
                # Granular auto-flows can finish with multiple terminal adapter
                # checkpoints. The auto-flow body ran in this process, so prefer
                # the in-memory result that the module-level flow stored for us.
                if slot.has_result and _is_multiple_terminal_steps_output_error(error):
                    flow_result = slot.result
                else:
                    raise
        finally:
            with _AUTO_FLOW_LOCK:
                _AUTO_FLOW_BODIES.pop(run_id, None)
            if serialized_body_path is not None:
                try:
                    os.remove(serialized_body_path)
                except FileNotFoundError:
                    pass

        if slot.error is not None:
            raise slot.error
        if slot.has_result:
            return slot.result
        return flow_result

    def _effective_message_history(
        self,
        explicit: Sequence[_messages.ModelMessage] | None,
    ) -> Sequence[_messages.ModelMessage] | None:
        if explicit is not None or not self._persist_message_history:
            return explicit
        with self._message_history_lock:
            return list(self._last_messages) if self._last_messages else None

    def _remember_messages(self, result: Any) -> None:
        if not self._persist_message_history:
            return
        all_messages = getattr(result, "all_messages", None)
        if not callable(all_messages):
            raise KitaruRuntimeError(
                "KitaruAgent could not refresh persisted message history because "
                "the run result does not expose all_messages()."
            )
        with self._message_history_lock:
            self._last_messages = list(all_messages())

    def _warn_if_persist_history_inside_checkpoint(self) -> None:
        if (
            not self._persist_message_history
            or self._warned_checkpoint_history_limit
            or _INTERNAL_RUN_SYNC_DELEGATION.get()
            or not is_inside_checkpoint()
        ):
            return
        self._warned_checkpoint_history_limit = True
        message = (
            "`persist_message_history=True` is only in-memory. This agent call "
            "is running inside an existing `@kitaru.checkpoint`; if that "
            "checkpoint is served from cache during replay/resume, the adapter "
            "will not execute and cannot restore `_last_messages`. For "
            "resume-safe conversations, call the agent at flow scope in "
            "granular mode, or pass `message_history=` explicitly from your "
            "own durable storage."
        )
        warnings.warn(message, UserWarning, stacklevel=3)

    def _use_granular(self, force_turn_checkpoint: bool) -> bool:
        # The calls strategy cannot apply to streaming turns: per-call checkpointing
        # a streamed ``request_stream`` would require draining and replaying
        # the stream inside a sync ZenML step. Fall back to the turn checkpoint
        # so model/tool events still land under a tracked boundary.
        return self._uses_calls_strategy and not force_turn_checkpoint

    def _log_streaming_fallback(self) -> None:
        if self._warned_streaming_fallback:
            return
        dropped_configs = [
            name
            for name, config in (
                ("model_checkpoint_config", self._model_checkpoint_config),
                ("tool_checkpoint_config", self._tool_checkpoint_config),
                (
                    "tool_checkpoint_config_by_name",
                    self._tool_checkpoint_config_by_name,
                ),
                ("mcp_checkpoint_config", self._mcp_checkpoint_config),
            )
            if config
        ]
        if not dropped_configs:
            return
        self._warned_streaming_fallback = True
        logger.warning(
            "Falling back to turn checkpointing for a streamed PydanticAI run; "
            "granular checkpoint configs are ignored for this call.",
            extra={"dropped_configs": dropped_configs, "agent_name": self._name},
        )
        if is_inside_flow() and not is_inside_checkpoint():
            kitaru.log(
                adapter="pydantic_ai",
                streaming_fallback=True,
                dropped_checkpoint_configs=dropped_configs,
            )

    @staticmethod
    def _ensure_run_sync_safe() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            "`KitaruAgent.run_sync()` cannot be called from a running event loop. "
            "Use `await agent.run(...)` instead."
        )

    def _should_inline_sync_tools(self) -> bool:
        """Return whether the user explicitly requested inline sync tools.

        This is deliberately run-wide rather than per-tool. Pydantic AI decides
        whether to offload sync tools before Kitaru reaches an individual tool
        body, so Kitaru enables or disables the private threading hook around
        the whole agent run. The checkpoint opt-out remains checkpoint-only;
        this separate flag controls the private Pydantic AI threading
        compatibility path.
        """
        return self._allow_sync_tool_body_waits

    def _ensure_auto_flow_mcp_lifecycle_safe(
        self,
        *,
        call_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None,
    ) -> None:
        if is_inside_flow():
            return

        if not call_toolsets:
            effective_toolsets = self._toolsets
        else:
            effective_toolsets = (*self._toolsets, *call_toolsets)
        if not has_running_mcp_toolset(effective_toolsets):
            return

        raise KitaruUsageError(
            "KitaruAgent cannot auto-open a flow around an already-running "
            "PydanticAI MCP server. Auto-flow runs async agent bodies in a "
            "worker thread/event loop so it can wait for the flow without "
            "blocking your caller loop. Your MCP server is already open on the "
            "caller loop, so moving the agent run can hang after a successful "
            "MCP request. Wrap this call in an explicit `@kitaru.flow` while "
            "the MCP server lifecycle is open, or do not pre-open the MCP server "
            "and let PydanticAI/Kitaru auto-connect it."
        )

    async def _run_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        force_turn_checkpoint: bool = False,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        auto_flow_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        mark_streaming_fallback_checkpoint: bool = False,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return await body()
            return await self._auto_checkpoint_async(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                mark_streaming_fallback_checkpoint=mark_streaming_fallback_checkpoint,
            )

        self._ensure_auto_flow_mcp_lifecycle_safe(call_toolsets=auto_flow_toolsets)

        # Outside any flow: auto-open one. FlowHandle.wait() is sync-blocking,
        # so we dispatch the flow to a worker thread (no running loop there,
        # so asyncio.run is safe for the agent coroutine).
        async def _await_body() -> Any:
            return await self._run_async(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                auto_flow_toolsets=auto_flow_toolsets,
                mark_streaming_fallback_checkpoint=mark_streaming_fallback_checkpoint,
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._invoke_in_auto_flow(lambda: asyncio.run(_await_body())),
        )

    def _run_sync(
        self,
        body: Callable[[], Any],
        *,
        force_turn_checkpoint: bool = False,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        auto_flow_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        mark_streaming_fallback_checkpoint: bool = False,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return body()
            return self._auto_checkpoint_sync(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                mark_streaming_fallback_checkpoint=mark_streaming_fallback_checkpoint,
            )
        self._ensure_auto_flow_mcp_lifecycle_safe(call_toolsets=auto_flow_toolsets)
        return self._invoke_in_auto_flow(
            lambda: self._run_sync(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                auto_flow_toolsets=auto_flow_toolsets,
                mark_streaming_fallback_checkpoint=mark_streaming_fallback_checkpoint,
            )
        )

    def _require_explicit_checkpoint(self, method_name: str) -> None:
        if _INTERNAL_ITER_ALLOWED.get():
            return
        if is_inside_checkpoint():
            return
        raise UserError(
            f"`agent.{method_name}()` requires an explicit `@kitaru.checkpoint`. "
            "Kitaru cannot auto-open one around a streaming context manager; "
            "wrap the surrounding block in `@kitaru.flow` + `@kitaru.checkpoint`, "
            "or use `agent.run()` with an `event_stream_handler` instead."
        )

    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        retries: Any = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._validate_model_override(model)
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)
        effective_retries = _resolve_run_retries(
            output_retries=output_retries,
            retries=retries,
        )
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            retries=effective_retries,
        )

        async def _body() -> Any:
            with (
                self._kitaru_overrides(),
                self._tracking_scope(),
                self._allow_internal_iter(),
                stream_surface("run"),
                _maybe_suppress_model_stream_live_events(self._model, wrapped_handler),
                _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
                model_cache_run_context(
                    conversation_id=conversation_id, message_history=effective_history
                ),
            ):
                result = await super(KitaruAgent, self).run(
                    user_prompt,
                    output_type=output_type,
                    message_history=effective_history,
                    deferred_tool_results=deferred_tool_results,
                    **upstream_run_kwargs,
                    model=None,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=prepared_toolsets,
                    **_builtin_tools_kwargs(builtin_tools),
                    event_stream_handler=wrapped_handler,
                    capabilities=capabilities,
                    spec=spec,
                )
            return result

        turn_call_config = self._prepare_turn_checkpoint_call_config(
            user_prompt=user_prompt,
            message_history=effective_history,
            deferred_tool_results=deferred_tool_results,
            output_type=output_type,
            conversation_id=conversation_id,
            output_retries=effective_retries,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=prepared_toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=wrapped_handler,
            capabilities=capabilities,
            spec=spec,
        )
        if turn_call_config.force_turn_checkpoint and self._uses_calls_strategy:
            self._log_streaming_fallback()

        error: BaseException | None = None
        try:
            result = await self._run_async(
                _body,
                force_turn_checkpoint=turn_call_config.force_turn_checkpoint,
                cache_key=turn_call_config.cache_key,
                checkpoint_inputs=turn_call_config.checkpoint_inputs,
                checkpoint_config=turn_call_config.checkpoint_config,
                auto_flow_toolsets=prepared_toolsets,
                mark_streaming_fallback_checkpoint=(
                    turn_call_config.mark_streaming_fallback_checkpoint
                ),
            )
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run", error)

    def run_sync(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        retries: Any = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._ensure_run_sync_safe()
        self._validate_model_override(model)
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)
        effective_retries = _resolve_run_retries(
            output_retries=output_retries,
            retries=retries,
        )
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            retries=effective_retries,
        )

        def _body() -> Any:
            with (
                self._kitaru_overrides(),
                self._tracking_scope(),
                self._allow_internal_iter(),
                stream_surface("run_sync"),
                _maybe_suppress_model_stream_live_events(self._model, wrapped_handler),
                _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
                model_cache_run_context(
                    conversation_id=conversation_id, message_history=effective_history
                ),
            ):
                delegation_token = _INTERNAL_RUN_SYNC_DELEGATION.set(True)
                try:
                    result = super(KitaruAgent, self).run_sync(
                        user_prompt,
                        output_type=output_type,
                        message_history=effective_history,
                        deferred_tool_results=deferred_tool_results,
                        **upstream_run_kwargs,
                        model=None,
                        instructions=instructions,
                        deps=deps,
                        model_settings=model_settings,
                        usage_limits=usage_limits,
                        usage=usage,
                        metadata=metadata,
                        infer_name=infer_name,
                        toolsets=prepared_toolsets,
                        **_builtin_tools_kwargs(builtin_tools),
                        event_stream_handler=wrapped_handler,
                        capabilities=capabilities,
                        spec=spec,
                    )
                finally:
                    _INTERNAL_RUN_SYNC_DELEGATION.reset(delegation_token)
            return result

        turn_call_config = self._prepare_turn_checkpoint_call_config(
            user_prompt=user_prompt,
            message_history=effective_history,
            deferred_tool_results=deferred_tool_results,
            output_type=output_type,
            conversation_id=conversation_id,
            output_retries=effective_retries,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=prepared_toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=wrapped_handler,
            capabilities=capabilities,
            spec=spec,
        )
        if turn_call_config.force_turn_checkpoint and self._uses_calls_strategy:
            self._log_streaming_fallback()

        error: BaseException | None = None
        try:
            result = self._run_sync(
                _body,
                force_turn_checkpoint=turn_call_config.force_turn_checkpoint,
                cache_key=turn_call_config.cache_key,
                checkpoint_inputs=turn_call_config.checkpoint_inputs,
                checkpoint_config=turn_call_config.checkpoint_config,
                auto_flow_toolsets=prepared_toolsets,
                mark_streaming_fallback_checkpoint=(
                    turn_call_config.mark_streaming_fallback_checkpoint
                ),
            )
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run_sync", error)

    @asynccontextmanager
    async def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        retries: Any = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        self._validate_model_override(model)
        self._require_explicit_checkpoint("run_stream")
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_retries = _resolve_run_retries(
            output_retries=output_retries,
            retries=retries,
        )
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            retries=effective_retries,
        )

        with (
            self._kitaru_overrides(),
            self._tracking_scope(),
            stream_surface("run_stream"),
            _maybe_suppress_model_stream_live_events(self._model, wrapped_handler),
            _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
            model_cache_run_context(
                conversation_id=conversation_id, message_history=message_history
            ),
        ):
            async with super(KitaruAgent, self).run_stream(
                user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                **upstream_run_kwargs,
                model=None,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=prepared_toolsets,
                **_builtin_tools_kwargs(builtin_tools),
                event_stream_handler=wrapped_handler,
                capabilities=capabilities,
                spec=spec,
            ) as streamed_result:
                yield streamed_result

    @asynccontextmanager
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        retries: Any = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[AgentRun[AgentDepsT, Any]]:
        # iter() yields a run handle inside an `async with` body; auto-checkpointing it
        # would require a checkpoint primitive that itself is a context manager, which
        # kitaru.checkpoint isn't. Wrap iter() in an explicit @kitaru.checkpoint instead.
        self._validate_model_override(model)
        self._require_explicit_checkpoint("iter")
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        effective_retries = _resolve_run_retries(
            output_retries=output_retries,
            retries=retries,
        )
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            retries=effective_retries,
        )
        publisher = PydanticAIStreamPublisher(
            agent_name=self._name,
            surface="iter",
            source="iter_lifecycle",
            include_content=self._capture.save_stream_transcripts,
            enabled=self._capture.emit_child_events,
        )
        with (
            self._kitaru_overrides(),
            self._tracking_scope(),
            stream_surface("iter"),
            _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
            model_cache_run_context(
                conversation_id=conversation_id, message_history=message_history
            ),
        ):
            publisher.started()
            try:
                async with self.wrapped.iter(
                    user_prompt=user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    **upstream_run_kwargs,
                    model=None,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=prepared_toolsets,
                    **_builtin_tools_kwargs(builtin_tools),
                    capabilities=capabilities,
                    spec=spec,
                ) as run:
                    yield run
            except BaseException as exc:
                publisher.failed(exc)
                raise
            else:
                publisher.completed()
