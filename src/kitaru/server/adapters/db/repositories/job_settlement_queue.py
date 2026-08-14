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
"""SQL job settlement queue."""

import uuid

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.orm.job import JobSettlementCheckORM


class SQLJobSettlementQueue:
    """SQL implementation of the job settlement queue."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the queue.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def enqueue(self, job_id: uuid.UUID) -> None:
        """Queue a settlement check for a job.

        Args:
            job_id: Id of the job.
        """
        # The row is only read by other transactions after commit, so the
        # insert rides the ambient transaction without an immediate flush.
        self._session.add(JobSettlementCheckORM(job_id=job_id))

    async def claim(self, limit: int) -> list[uuid.UUID]:
        """Claim queued settlement checks and drop them from the queue.

        A check another transaction holds is skipped and stays queued for it.

        Args:
            limit: Maximum number of queued checks to claim.

        Returns:
            Distinct job ids of the claimed checks, oldest first.
        """
        claimable = (
            select(JobSettlementCheckORM.id)
            .order_by(JobSettlementCheckORM.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        statement = (
            delete(JobSettlementCheckORM)
            .where(JobSettlementCheckORM.id.in_(claimable))
            .returning(JobSettlementCheckORM.id, JobSettlementCheckORM.job_id)
        )
        rows = (
            await self._session.execute(
                statement, execution_options={"synchronize_session": False}
            )
        ).all()
        rows = sorted(rows, key=lambda row: row[0])
        return list(dict.fromkeys(job_id for _, job_id in rows))
