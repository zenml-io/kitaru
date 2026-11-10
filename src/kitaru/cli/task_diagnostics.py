#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Best-effort pending-work diagnostics for CLI receipts and polling."""

import asyncio
import uuid
from json import JSONDecodeError
from typing import Any

from httpx import TransportError
from pydantic import ValidationError

from kitaru.api_models.v1.task import TaskDiagnosticsResponse
from kitaru.client.exceptions import APIError, InvalidServerResponseError
from kitaru.task_diagnostics import get_task_diagnostic_warnings


async def get_task_diagnostics(
    client: Any,
    *,
    job_id: uuid.UUID | None = None,
    experiment_run_id: uuid.UUID | None = None,
    timeout: float = 5.0,
) -> tuple[TaskDiagnosticsResponse | None, list[str]]:
    """Read advisory coverage without failing the primary operation."""
    try:
        async with asyncio.timeout(max(0.0, min(timeout, 5.0))):
            diagnostics = await client.tasks.diagnostics(
                job_id=job_id, experiment_run_id=experiment_run_id
            )
    except (
        APIError,
        JSONDecodeError,
        InvalidServerResponseError,
        ValidationError,
        TransportError,
        TimeoutError,
    ):
        return None, [
            "Pending-work diagnostics are unavailable; the operation continues."
        ]
    return diagnostics, get_task_diagnostic_warnings(diagnostics)
