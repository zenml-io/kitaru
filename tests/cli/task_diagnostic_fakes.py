#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Task diagnostics resource fake for unrelated CLI flows."""

import uuid

from kitaru.api_models.v1.task import TaskDiagnosticsResponse


class EmptyTaskDiagnostics:
    """Return a queue with no pending tasks."""

    async def diagnostics(
        self,
        *,
        job_id: uuid.UUID | None = None,
        experiment_run_id: uuid.UUID | None = None,
    ) -> TaskDiagnosticsResponse:
        return TaskDiagnosticsResponse(
            live_worker_count=1,
            pending_task_count=0,
            groups=[],
            truncated=False,
        )
