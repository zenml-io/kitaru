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
"""Replay result session id Alembic revision.

Revision ID: 004_replay_result_session_id
Revises: 003_worker_liveness_index
Create Date: 2026-08-21

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "004_replay_result_session_id"
down_revision = "003_worker_liveness_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.add_column(sa.Column("result_session_id", sa.Uuid(), nullable=True))
    # Backfill from the agent task sharing the replay's job, before the index
    # and foreign key exist, so the update skips per-row index and FK upkeep.
    op.execute(
        "UPDATE replay SET result_session_id = task.result_session_id "
        "FROM task WHERE task.job_id = replay.job_id AND task.kind = 'agent'"
    )
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.create_index(
            "ix_replay_result_session_id", ["result_session_id"], unique=False
        )
        batch_op.create_foreign_key(
            "fk_replay_result_session_id", "session", ["result_session_id"], ["id"]
        )
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_constraint("fk_task_result_session_id", type_="foreignkey")
        batch_op.drop_index("ix_task_result_session_id")
        batch_op.drop_column("result_session_id")


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(sa.Column("result_session_id", sa.Uuid(), nullable=True))
    # Restore the task-side link from the session's task pointer.
    op.execute(
        "UPDATE task SET result_session_id = session.id "
        "FROM session WHERE session.task_id = task.id AND task.kind = 'agent'"
    )
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_result_session_id", ["result_session_id"], unique=False
        )
        batch_op.create_foreign_key(
            "fk_task_result_session_id", "session", ["result_session_id"], ["id"]
        )
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.drop_constraint("fk_replay_result_session_id", type_="foreignkey")
        batch_op.drop_index("ix_replay_result_session_id")
        batch_op.drop_column("result_session_id")
