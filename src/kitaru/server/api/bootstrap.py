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
"""Default account, default plugin, and server id bootstrap at server startup."""

import logging
import uuid

from kitaru.base import FrozenModel
from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.application.interfaces.account_repository import AccountRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.server_settings_repository import (
    ServerSettingsRepository,
)
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.names import RESERVED_NAMESPACE
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginNotFound,
)

logger = logging.getLogger(__name__)


async def ensure_server_id(
    repository: ServerSettingsRepository, configured_id: uuid.UUID | None
) -> uuid.UUID:
    """Persist the server id on first startup and load it after.

    Args:
        repository: Server settings repository.
        configured_id: Configured server id, None generates one.

    Returns:
        Stored server id.
    """
    return await repository.ensure_server_id(configured_id or uuid.uuid4())


async def ensure_default_account(
    repository: AccountRepository,
    name: str,
    password: str | None,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Create the default account when it does not exist.

    Args:
        repository: Account repository.
        name: Account name.
        password: Login password, hashed before storage.
        analytics: Analytics tracker, None skips tracking.
    """
    account_service = AccountService(
        repository=repository,
        password_hasher=BcryptPasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    await account_service.ensure_account(name, password)


class DefaultPluginDefinition(FrozenModel):
    """Default plugin definition."""

    kind: PluginKind
    name: str
    description: str
    provider: str | None
    logo_url: str | None = None
    entrypoint: str
    requirement: str
    display_version: str


DEFAULT_PLUGIN_DEFINITIONS: tuple[DefaultPluginDefinition, ...] = (
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_NAMESPACE}/braintrust",
        description="Import Braintrust project-log and UI exports.",
        provider="braintrust",
        entrypoint="kitaru_braintrust_importer.importer:parse",
        requirement="kitaru-braintrust-importer==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_NAMESPACE}/kitaru-jsonl",
        description="Import sessions matching the Kitaru JSONL contract.",
        provider="kitaru-jsonl",
        entrypoint="kitaru_jsonl_importer.importer:parse",
        requirement="kitaru-jsonl-importer==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_NAMESPACE}/langfuse",
        description="Import Langfuse JSON and JSONL trace exports.",
        provider="langfuse",
        entrypoint="kitaru_langfuse_importer.importer:parse",
        requirement="kitaru-langfuse-importer==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_NAMESPACE}/langsmith",
        description="Import LangSmith run-query and bulk-export records.",
        provider="langsmith",
        entrypoint="kitaru_langsmith_importer.importer:parse",
        requirement="kitaru-langsmith-importer==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/cost",
        description="Report the total recorded session cost.",
        provider=None,
        entrypoint="kitaru_evaluator.basic:cost",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/latency",
        description="Measure session wall-clock duration.",
        provider=None,
        entrypoint="kitaru_evaluator.basic:latency",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/tool-call-patterns",
        description="Count repeated calls to the same tool.",
        provider=None,
        entrypoint="kitaru_evaluator.basic:tool_call_patterns",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/session-diagnostics",
        description="Check session completeness and internal consistency.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:session_diagnostics",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/output-contract",
        description="Check output against exact and structural rules.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:output_contract",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/trajectory-signals",
        description="Report repetition, failed retries, and short tool cycles.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:trajectory_signals",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/tool-health",
        description="Report recorded tool failures and result anomalies.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:tool_health",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/timing-profile",
        description="Report recorded wall-clock and node timing.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:timing_profile",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/resource-budget",
        description="Apply configured ceilings to recorded resource use.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:resource_budget",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/tool-policy",
        description="Apply exact tool requirements, prohibitions, and limits.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:tool_policy",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/llm-call-signals",
        description="Report LLM failures, repetition, and metadata coverage.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:llm_call_signals",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/model-policy",
        description="Apply exact model and provider rules.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:model_policy",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/workflow-conformance",
        description="Compare recorded tool order with a configured workflow.",
        provider=None,
        entrypoint="kitaru_evaluator.deterministic:workflow_conformance",
        requirement="kitaru-evaluator==0.1.0",
        display_version="0.1.0",
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
                logo_url=definition.logo_url,
                metadata={},
            )
        )
        logger.info("Created default plugin %s.", definition.name)
        return plugin
    except DuplicatePluginName:
        return await repository.get_by_name(definition.kind, definition.name)


async def register_default_plugins(repository: PluginRepository) -> None:
    """Create the configured default plugins.

    Args:
        repository: Plugin repository.
    """
    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        plugin = await _get_or_create_plugin(repository, definition)
        source = PackagePluginSource(
            requirement=definition.requirement,
            entrypoint=definition.entrypoint,
        )
        if plugin.latest_version:
            latest = await repository.get_version(plugin.id, plugin.latest_version)
            if latest.source == source:
                logger.debug("Default plugin %s is already current.", definition.name)
                continue
        try:
            await repository.create_version(
                plugin.id,
                source,
                definition.display_version,
            )
            logger.info("Created a new version for default plugin %s.", definition.name)
        except DuplicatePluginVersion:
            continue
