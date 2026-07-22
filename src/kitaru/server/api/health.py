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
"""Health and liveness endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.rest.dependencies import get_session

router = APIRouter()


@router.get("")
async def health(
    db: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    """Report service and database readiness.

    Args:
        db: Database session for the readiness probe.

    Raises:
        HTTPException: HTTP 503 when the database probe fails.

    Returns:
        JSON object with ``status`` and ``database`` fields.
    """
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "Healthy", "database": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {exc}") from exc


@router.get("/live")
async def liveness() -> dict[str, str]:
    """Confirm the API process is running.

    Returns:
        Minimal liveness JSON body.
    """
    return {"status": "ok"}
