#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Default account and default plugin bootstrap at server startup."""

import hashlib
import logging
from importlib.metadata import version
from importlib.resources import files
from typing import Any

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.application.interfaces.account_repository import AccountRepository
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginKind,
    PluginNotFound,
    ScriptPluginSource,
)

_SOURCE_MEDIA_TYPE = "text/x-python"

logger = logging.getLogger(__name__)


async def ensure_default_account(
    repository: AccountRepository, name: str, password: str | None
) -> None:
    """Create the default account when it does not exist.

    Args:
        repository: Account repository.
        name: Account name.
        password: Login password, hashed before storage.
    """
    account_service = AccountService(
        repository=repository,
        password_hasher=BcryptPasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    await account_service.ensure_account(name, password)


class DefaultPluginDefinition(FrozenModel):
    """Default plugin definition."""

    kind: PluginKind
    name: str
    description: str
    provider: str | None
    entrypoint: str
    content: bytes
    version: int
    metadata: dict[str, Any] = Field(default_factory=dict)


_EVALUATOR_SOURCE = (
    files("kitaru._default_plugins").joinpath("evaluators.py").read_bytes()
)
_EVALUATOR_VERSION = 1
_EVALUATOR_SOURCE_SHA256_BY_VERSION = {
    1: "01ae8e99ad669ea0712b79d034d4198668e7715c10d5284c7f1b768cf0b7ca34"
}
_EVALUATOR_SOURCE_SHA256 = hashlib.sha256(_EVALUATOR_SOURCE).hexdigest()
if _EVALUATOR_SOURCE_SHA256_BY_VERSION[_EVALUATOR_VERSION] != _EVALUATOR_SOURCE_SHA256:
    raise RuntimeError(
        "Built-in evaluator source changed without a declared catalog revision"
    )


def _create_evaluator_definition(
    name: str,
    description: str,
    entrypoint: str,
    category: str,
    parameters: dict[str, Any],
    result_families: list[str],
) -> DefaultPluginDefinition:
    """Create one released evaluator catalog definition."""
    return DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}{name}",
        description=description,
        provider=None,
        entrypoint=entrypoint,
        content=_EVALUATOR_SOURCE,
        version=_EVALUATOR_VERSION,
        metadata={
            "built_in": True,
            "category": category,
            "contract_version": _EVALUATOR_VERSION,
            "deterministic": True,
            "execution": "manual",
            "offline": True,
            "parameters": parameters,
            "result_families": result_families,
            "source_sha256": _EVALUATOR_SOURCE_SHA256,
        },
    )


DEFAULT_PLUGIN_DEFINITIONS: tuple[DefaultPluginDefinition, ...] = (
    _create_evaluator_definition(
        "session-diagnostics",
        "Checks the completeness and internal consistency of a recorded session.",
        "session_diagnostics",
        "integrity",
        {},
        [
            "terminality",
            "node_order",
            "parent_linkage",
            "chronology_findings",
            "payload_coverage",
            "recorded_counts",
            "duration_seconds",
            "cost_coverage",
            "token_coverage",
            "resource_integrity",
        ],
    ),
    _create_evaluator_definition(
        "output-contract",
        "Checks recorded output against caller-supplied exact and structural rules.",
        "output_contract",
        "contract",
        {
            "expected": {"required": False, "type": "json"},
            "required_paths": {"items": "string", "required": False, "type": "array"},
            "type_requirements": {
                "required": False,
                "type": "object[string,json-type]",
                "values": [
                    "null",
                    "boolean",
                    "number",
                    "integer",
                    "string",
                    "array",
                    "object",
                ],
            },
        },
        ["output_availability", "exact_output", "required_paths", "type_requirements"],
    ),
    _create_evaluator_definition(
        "trajectory-signals",
        "Reports exact repetition, failed retries, and bounded short tool-use cycles.",
        "trajectory_signals",
        "descriptive_signal",
        {},
        [
            "tool_identity_coverage",
            "adjacent_identical_calls",
            "failed_identical_retries",
            "short_cycles",
            "cycle_detector_bounds",
        ],
    ),
    _create_evaluator_definition(
        "tool-health",
        "Reports recorded tool failures and result-payload anomalies.",
        "tool_health",
        "operational_diagnostic",
        {},
        [
            "failed_calls",
            "null_results",
            "empty_results",
            "error_status_inconsistencies",
            "adjacent_repeated_failures",
        ],
    ),
    _create_evaluator_definition(
        "timing-profile",
        "Reports recorded wall-clock and node timing without outlier judgments.",
        "timing_profile",
        "descriptive_resource",
        {
            "evidence_limit": {
                "default": 20,
                "maximum": 100,
                "minimum": 1,
                "required": False,
                "type": "integer",
            }
        },
        [
            "wall_clock_duration_seconds",
            "node_duration_coverage",
            "slowest_nodes",
            "overlapping_intervals",
            "invalid_intervals",
        ],
    ),
    _create_evaluator_definition(
        "resource-budget",
        "Applies caller-supplied inclusive ceilings to recorded resource use.",
        "resource_budget",
        "policy",
        {
            "max_duration_seconds": {"minimum": 0, "required": False, "type": "number"},
            "max_cost": {"minimum": 0, "required": False, "type": "number"},
            "max_total_tokens": {"minimum": 0, "required": False, "type": "number"},
            "max_nodes": {"minimum": 0, "required": False, "type": "integer"},
            "max_llm_calls": {"minimum": 0, "required": False, "type": "integer"},
            "max_tool_calls": {"minimum": 0, "required": False, "type": "integer"},
        },
        [
            "duration_budget",
            "cost_budget",
            "total_tokens_budget",
            "node_count_budget",
            "llm_call_count_budget",
            "tool_call_count_budget",
        ],
    ),
    _create_evaluator_definition(
        "tool-policy",
        "Applies exact tool requirements, prohibitions, and caller-supplied limits.",
        "tool_policy",
        "policy",
        {
            "required_tools": {"items": "string", "required": False, "type": "array"},
            "forbidden_tools": {"items": "string", "required": False, "type": "array"},
            "max_calls_per_tool": {
                "required": False,
                "type": "object[string,integer]",
            },
        },
        [
            "tool_name_coverage",
            "required_tools",
            "forbidden_tools",
            "per_tool_maximums",
        ],
    ),
    _create_evaluator_definition(
        "llm-call-signals",
        "Reports LLM failures, empty results, repetition, and metadata coverage.",
        "llm_call_signals",
        "operational_diagnostic",
        {},
        [
            "failed_calls",
            "empty_results",
            "adjacent_identical_inputs",
            "requested_model_mismatches",
            "metadata_coverage",
        ],
    ),
    _create_evaluator_definition(
        "model-policy",
        "Applies caller-supplied exact model and provider rules to recorded LLM calls.",
        "model_policy",
        "policy",
        {
            "allowed_models": {"items": "string", "required": False, "type": "array"},
            "allowed_providers": {
                "items": "string",
                "required": False,
                "type": "array",
            },
            "require_requested_model_match": {
                "default": False,
                "required": False,
                "type": "boolean",
            },
        },
        ["allowed_models", "allowed_providers", "requested_model_match"],
    ),
    _create_evaluator_definition(
        "workflow-conformance",
        "Compares recorded tool order with a caller-supplied workflow sequence.",
        "workflow_conformance",
        "policy",
        {
            "expected_tools": {"items": "string", "required": True, "type": "array"},
            "mode": {
                "default": "exact_order",
                "required": False,
                "type": "string",
                "values": ["exact_order", "in_order", "contains_all", "exact_set"],
            },
        },
        ["tool_name_coverage", "workflow_match"],
    ),
)


async def _get_or_create_plugin(
    repository: PluginRepository, definition: DefaultPluginDefinition
) -> Plugin:
    """Load a default plugin, creating it ownerless on first startup.

    Args:
        repository: Plugin repository.
        definition: Default plugin definition.

    Returns:
        Stored plugin.
    """
    try:
        return await repository.get_by_name(definition.kind, definition.name)
    except PluginNotFound:
        pass
    try:
        plugin = await repository.create(
            Plugin(
                owner_id=None,
                kind=definition.kind,
                name=definition.name,
                description=definition.description,
                provider=definition.provider,
                metadata=definition.metadata,
            )
        )
        logger.info("Created default plugin %s.", definition.name)
        return plugin
    except DuplicatePluginName:
        return await repository.get_by_name(definition.kind, definition.name)


async def register_default_plugins(
    repository: PluginRepository, blob_repository: BlobRepository
) -> None:
    """Create the built-in default plugins and their declared versions.

    Args:
        repository: Plugin repository.
        blob_repository: Blob repository.
    """
    installed_version = version("kitaru")
    blobs_by_sha256: dict[str, Blob] = {}
    plugins = [
        await _get_or_create_plugin(repository, definition)
        for definition in DEFAULT_PLUGIN_DEFINITIONS
    ]
    locked_plugins = await repository.get_many_locked([plugin.id for plugin in plugins])
    for definition, discovered_plugin in zip(
        DEFAULT_PLUGIN_DEFINITIONS, plugins, strict=True
    ):
        plugin = locked_plugins[discovered_plugin.id]
        if plugin.latest_version >= definition.version:
            logger.debug("Default plugin %s is already current.", definition.name)
            continue
        if (
            plugin.description != definition.description
            or plugin.metadata != definition.metadata
        ):
            plugin.update_description(definition.description)
            plugin.update_metadata(definition.metadata)
            plugin = await repository.update(plugin)
        sha256 = hashlib.sha256(definition.content).hexdigest()
        blob = blobs_by_sha256.get(sha256)
        if blob is None:
            blob, _ = await blob_repository.create(
                Blob(
                    owner_id=None,
                    sha256=sha256,
                    size=len(definition.content),
                    media_type=_SOURCE_MEDIA_TYPE,
                    data=definition.content,
                )
            )
            blobs_by_sha256[sha256] = blob
        try:
            await repository.create_version(
                plugin.id,
                ScriptPluginSource(blob_id=blob.id, entrypoint=definition.entrypoint),
                installed_version,
            )
            logger.info("Created a new version for default plugin %s.", definition.name)
        except DuplicatePluginVersion:
            continue
