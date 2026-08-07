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
"""Smoke test for the application's registered route manifest."""

from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings


def test_route_manifest_is_registered() -> None:
    """Expose every router's paths through the application."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    paths = set(app.openapi()["paths"])
    expected = {
        "/health",
        "/health/live",
        "/v1/accounts",
        "/v1/accounts/{account_id}",
        "/v1/accounts/{account_id}/activate",
        "/v1/accounts/{account_id}/deactivate",
        "/v1/agent-versions/{agent_version_id}",
        "/v1/agents",
        "/v1/agents/{agent_id}",
        "/v1/agents/{agent_id}/versions",
        "/v1/annotations",
        "/v1/annotations/{annotation_id}",
        "/v1/api-keys",
        "/v1/api-keys/{api_key_id}",
        "/v1/api-keys/{api_key_id}/rotate",
        "/v1/blobs",
        "/v1/blobs/{blob_id}",
        "/v1/blobs/{blob_id}/content",
        "/v1/cohort-versions/{cohort_version_id}",
        "/v1/cohorts",
        "/v1/cohorts/{cohort_id}",
        "/v1/cohorts/{cohort_id}/versions",
        "/v1/device_authorization",
        "/v1/devices",
        "/v1/devices/{device_id}",
        "/v1/devices/{device_id}/verify",
        "/v1/evaluations",
        "/v1/evaluations/{evaluation_id}",
        "/v1/evaluators",
        "/v1/evaluators/{evaluator_id}",
        "/v1/evaluators/{evaluator_id}/versions",
        "/v1/evaluators/{evaluator_id}/versions/{version}",
        "/v1/experiment-runs",
        "/v1/experiment-runs/{experiment_run_id}",
        "/v1/experiment-runs/{experiment_run_id}/cancel",
        "/v1/experiment-runs/{experiment_run_id}/jobs",
        "/v1/experiments",
        "/v1/experiments/{experiment_id}",
        "/v1/experiments/{experiment_id}/runs",
        "/v1/importers",
        "/v1/importers/{importer_id}",
        "/v1/importers/{importer_id}/versions",
        "/v1/importers/{importer_id}/versions/{version}",
        "/v1/imports",
        "/v1/info",
        "/v1/investigations",
        "/v1/investigations/{investigation_id}",
        "/v1/investigations/{investigation_id}/sessions",
        "/v1/investigations/{investigation_id}/sessions/{session_id}",
        "/v1/jobs",
        "/v1/jobs/{job_id}",
        "/v1/jobs/{job_id}/cancel",
        "/v1/jobs/{job_id}/tasks",
        "/v1/login",
        "/v1/logout",
        "/v1/replays",
        "/v1/replays/{replay_id}",
        "/v1/replays/{replay_id}/tool-lookup",
        "/v1/secrets",
        "/v1/secrets/{secret_id}",
        "/v1/session-runs",
        "/v1/sessions",
        "/v1/sessions/{session_id}",
        "/v1/sessions/{session_id}/evaluations",
        "/v1/sessions/{session_id}/full",
        "/v1/sessions/{session_id}/nodes",
        "/v1/tags",
        "/v1/tags/{tag_id}",
        "/v1/tags/{tag_id}/links",
        "/v1/tags/{tag_id}/links/{resource_type}/{resource_id}",
        "/v1/tasks",
        "/v1/tasks/claim",
        "/v1/tasks/{task_id}",
        "/v1/tasks/{task_id}/spec",
        "/v1/worker-pools",
        "/v1/worker-pools/{pool_id}",
        "/v1/workers",
        "/v1/workers/{worker_id}",
        "/v1/workers/{worker_id}/heartbeat",
    }
    assert paths == expected
