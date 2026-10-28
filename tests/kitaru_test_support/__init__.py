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
"""Shared in-memory fakes and service builders for the test suite."""

from kitaru_test_support.accounts import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakeDeviceRepository,
    FakeIdempotencyKeyRepository,
    create_api_key,
    create_device,
    marked_idempotent_routes,
    mint_worker_token,
    override_idempotency,
)
from kitaru_test_support.blobs_plugins import (
    DEFAULT_MAX_BLOB_SIZE_BYTES,
    DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
    BlobServiceFakes,
    FakeBlobDataStore,
    FakeBlobRepository,
    FakeEphemeralWorkers,
    FakePluginRepository,
    PayloadStoreFakes,
    build_blob_service,
    build_payload_store,
    create_blob,
    create_plugin,
)
from kitaru_test_support.cohorts_workers import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeWorkerRepository,
    create_cohort,
    create_cohort_version,
    create_worker,
)
from kitaru_test_support.common import (
    UNSCOPED_WORKER_SCOPE,
    FakeControlPlaneClient,
    FakePasswordHasher,
    ListItemT,
)
from kitaru_test_support.evaluations import (
    FakeEvaluationRepository,
    FakeExperimentRunRepository,
    create_evaluation,
    create_experiment_run,
)
from kitaru_test_support.experiments_replays import (
    FakeExperimentRepository,
    FakeImportRepository,
    FakeReplayRepository,
    create_experiment,
    create_import,
    create_replay,
    get_replay_job_id,
)
from kitaru_test_support.investigations import (
    FakeAnnotationRepository,
    FakeInvestigationRepository,
)
from kitaru_test_support.jobs_tasks import (
    FakeJobRepository,
    FakeTaskRepository,
    create_agent_task,
    create_analysis_task,
    create_evaluation_task,
    create_import_task,
    create_job,
)
from kitaru_test_support.secrets_agents import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeConnectionRepository,
    FakeSecretRepository,
    create_agent,
    create_agent_version,
    create_connection,
    create_secret,
)
from kitaru_test_support.services import (
    FakeInsightRepository,
    JobAndTaskServices,
    ReplayServices,
    TaskSubstrate,
    build_job_and_task_services,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
)
from kitaru_test_support.session_nodes import (
    FakeSessionNodeRepository,
    build_session_node,
)
from kitaru_test_support.sessions import (
    FakeSessionRepository,
    FakeTagRepository,
    create_session,
    create_tag,
)

__all__ = [
    "DEFAULT_MAX_BLOB_SIZE_BYTES",
    "DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES",
    "UNSCOPED_WORKER_SCOPE",
    "BlobServiceFakes",
    "FakeAccountRepository",
    "FakeAgentRepository",
    "FakeAgentVersionRepository",
    "FakeAnnotationRepository",
    "FakeApiKeyRepository",
    "FakeBlobDataStore",
    "FakeBlobRepository",
    "FakeCohortRepository",
    "FakeCohortVersionRepository",
    "FakeConnectionRepository",
    "FakeControlPlaneClient",
    "FakeDeviceRepository",
    "FakeEphemeralWorkers",
    "FakeEvaluationRepository",
    "FakeExperimentRepository",
    "FakeExperimentRunRepository",
    "FakeIdempotencyKeyRepository",
    "FakeImportRepository",
    "FakeInsightRepository",
    "FakeInvestigationRepository",
    "FakeJobRepository",
    "FakePasswordHasher",
    "FakePluginRepository",
    "FakeReplayRepository",
    "FakeSecretRepository",
    "FakeSessionNodeRepository",
    "FakeSessionRepository",
    "FakeTagRepository",
    "FakeTaskRepository",
    "FakeWorkerRepository",
    "JobAndTaskServices",
    "ListItemT",
    "PayloadStoreFakes",
    "ReplayServices",
    "TaskSubstrate",
    "build_blob_service",
    "build_job_and_task_services",
    "build_payload_store",
    "build_replay_services",
    "build_session_node",
    "build_task_actor",
    "build_worker_actor",
    "create_agent",
    "create_agent_task",
    "create_agent_version",
    "create_analysis_task",
    "create_api_key",
    "create_blob",
    "create_cohort",
    "create_cohort_version",
    "create_connection",
    "create_device",
    "create_evaluation",
    "create_evaluation_task",
    "create_experiment",
    "create_experiment_run",
    "create_import",
    "create_import_task",
    "create_job",
    "create_plugin",
    "create_replay",
    "create_secret",
    "create_session",
    "create_tag",
    "create_worker",
    "get_replay_job_id",
    "marked_idempotent_routes",
    "mint_worker_token",
    "override_idempotency",
]
