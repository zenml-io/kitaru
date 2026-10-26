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
"""FastAPI dependency providers."""

from kitaru.server.adapters.rest.dependencies.auth import (
    CSRF_HEADER,
    RequestCredential,
    authorize,
    authorize_task_only,
    authorize_with_task,
    authorize_with_worker,
    authorize_with_worker_or_task,
    authorize_worker_only,
    get_auth_service,
    get_bearer_credential,
    get_device_service,
    get_optional_bearer_credential,
    require_local_account_management,
)

# Tests override this dependency directly, so keep it importable from the package.
from kitaru.server.adapters.rest.dependencies.auth import (
    _resolve_auth_context as _resolve_auth_context,
)
from kitaru.server.adapters.rest.dependencies.execution import (
    get_ephemeral_worker_starter,
    get_experiment_run_service,
    get_experiment_service,
    get_import_service,
    get_job_service,
    get_replay_service,
    get_sample_data_seeder,
    get_task_service,
)
from kitaru.server.adapters.rest.dependencies.request import (
    get_analytics_client,
    get_app_settings,
    get_auth_session,
    get_engine,
    get_ephemeral_workers,
    get_server_analytics,
    get_server_id_state,
    get_session,
    get_ui_version_state,
)
from kitaru.server.adapters.rest.dependencies.services import (
    get_account_service,
    get_agent_service,
    get_agent_version_service,
    get_analyzer_service,
    get_annotation_service,
    get_api_key_service,
    get_blob_data_stores,
    get_blob_service,
    get_cohort_service,
    get_cohort_version_service,
    get_connection_service,
    get_evaluation_service,
    get_evaluator_service,
    get_idempotency_key_repository,
    get_importer_service,
    get_insight_service,
    get_investigation_service,
    get_payload_store,
    get_permission_service,
    get_secret_service,
    get_session_node_service,
    get_session_service,
    get_tag_service,
    get_task_policy,
    get_worker_service,
)

__all__ = [
    "CSRF_HEADER",
    "RequestCredential",
    "authorize",
    "authorize_task_only",
    "authorize_with_task",
    "authorize_with_worker",
    "authorize_with_worker_or_task",
    "authorize_worker_only",
    "get_account_service",
    "get_agent_service",
    "get_agent_version_service",
    "get_analytics_client",
    "get_analyzer_service",
    "get_annotation_service",
    "get_api_key_service",
    "get_app_settings",
    "get_auth_service",
    "get_auth_session",
    "get_bearer_credential",
    "get_blob_data_stores",
    "get_blob_service",
    "get_cohort_service",
    "get_cohort_version_service",
    "get_connection_service",
    "get_device_service",
    "get_engine",
    "get_ephemeral_worker_starter",
    "get_ephemeral_workers",
    "get_evaluation_service",
    "get_evaluator_service",
    "get_experiment_run_service",
    "get_experiment_service",
    "get_idempotency_key_repository",
    "get_import_service",
    "get_importer_service",
    "get_insight_service",
    "get_investigation_service",
    "get_job_service",
    "get_optional_bearer_credential",
    "get_payload_store",
    "get_permission_service",
    "get_replay_service",
    "get_sample_data_seeder",
    "get_secret_service",
    "get_server_analytics",
    "get_server_id_state",
    "get_session",
    "get_session_node_service",
    "get_session_service",
    "get_tag_service",
    "get_task_policy",
    "get_task_service",
    "get_ui_version_state",
    "get_worker_service",
    "require_local_account_management",
]
