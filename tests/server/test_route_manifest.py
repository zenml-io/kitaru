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
        "/api/v1/accounts",
        "/api/v1/accounts/me",
        "/api/v1/accounts/{account_id}",
        "/api/v1/agent-versions/{agent_version_id}",
        "/api/v1/agents",
        "/api/v1/agents/{agent_id}",
        "/api/v1/agents/{agent_id}/versions",
        "/api/v1/annotations",
        "/api/v1/annotations/{annotation_id}",
        "/api/v1/api-keys",
        "/api/v1/api-keys/{api_key_id}",
        "/api/v1/api-keys/{api_key_id}/rotate",
        "/api/v1/blobs",
        "/api/v1/blobs/{blob_id}",
        "/api/v1/blobs/{blob_id}/content",
        "/api/v1/cohort-versions/{cohort_version_id}",
        "/api/v1/cohorts",
        "/api/v1/cohorts/{cohort_id}",
        "/api/v1/cohorts/{cohort_id}/versions",
        "/api/v1/device_authorization",
        "/api/v1/devices",
        "/api/v1/devices/{device_id}",
        "/api/v1/devices/{device_id}/verify",
        "/api/v1/evaluations",
        "/api/v1/evaluations/{evaluation_id}",
        "/api/v1/evaluators",
        "/api/v1/evaluators/{evaluator_id}",
        "/api/v1/evaluators/{evaluator_id}/versions",
        "/api/v1/evaluators/{evaluator_id}/versions/{version}",
        "/api/v1/experiment-runs",
        "/api/v1/experiment-runs/{experiment_run_id}",
        "/api/v1/experiment-runs/{experiment_run_id}/cancel",
        "/api/v1/experiment-runs/{experiment_run_id}/jobs",
        "/api/v1/experiments",
        "/api/v1/experiments/{experiment_id}",
        "/api/v1/experiments/{experiment_id}/runs",
        "/api/v1/importers",
        "/api/v1/importers/{importer_id}",
        "/api/v1/importers/{importer_id}/versions",
        "/api/v1/importers/{importer_id}/versions/{version}",
        "/api/v1/imports",
        "/api/v1/info",
        "/api/v1/investigations",
        "/api/v1/investigations/{investigation_id}",
        "/api/v1/investigations/{investigation_id}/sessions",
        "/api/v1/investigations/{investigation_id}/sessions/{session_id}",
        "/api/v1/jobs",
        "/api/v1/jobs/{job_id}",
        "/api/v1/jobs/{job_id}/cancel",
        "/api/v1/jobs/{job_id}/tasks",
        "/api/v1/login",
        "/api/v1/logout",
        "/api/v1/replays",
        "/api/v1/replays/{replay_id}",
        "/api/v1/replays/{replay_id}/tool-lookup",
        "/api/v1/secrets",
        "/api/v1/secrets/{secret_id}",
        "/api/v1/service-accounts",
        "/api/v1/service-accounts/{account_id}",
        "/api/v1/session-runs",
        "/api/v1/sessions",
        "/api/v1/sessions/{session_id}",
        "/api/v1/sessions/{session_id}/evaluations",
        "/api/v1/sessions/{session_id}/full",
        "/api/v1/sessions/{session_id}/nodes",
        "/api/v1/tags",
        "/api/v1/tags/{tag_id}",
        "/api/v1/tags/{tag_id}/links",
        "/api/v1/tags/{tag_id}/links/{resource_type}/{resource_id}",
        "/api/v1/tasks",
        "/api/v1/tasks/claim",
        "/api/v1/tasks/{task_id}",
        "/api/v1/tasks/{task_id}/spec",
        "/api/v1/ui/experiment-runs/{experiment_run_id}/evaluation-aggregates",
        "/api/v1/ui/sessions",
        "/api/v1/ui/sessions/{session_id}",
        "/api/v1/users",
        "/api/v1/users/{account_id}",
        "/api/v1/users/{account_id}/activate",
        "/api/v1/users/{account_id}/deactivate",
        "/api/v1/workers",
        "/api/v1/workers/{worker_id}",
        "/api/v1/workers/{worker_id}/heartbeat",
        "/api/v1/workers/{worker_id}/token",
    }
    assert paths == expected
