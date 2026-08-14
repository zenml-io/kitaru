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
"""Add the task queue key.

Revision ID: 002_task_queue_key
Revises: 001_initial
Create Date: 2026-08-17

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "002_task_queue_key"
down_revision = "001_initial"
branch_labels = None
depends_on = None

_BACKFILL_QUEUE_KEY = """
UPDATE task
SET queue_key = CASE kind
    WHEN 'agent' THEN 'agent:' || agent_version_id::text
    WHEN 'evaluator' THEN 'evaluator'
    WHEN 'importer' THEN 'importer'
END
"""


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("queue_key", sa.String(length=64, collation="C"), nullable=True)
        )

    op.execute(sa.text(_BACKFILL_QUEUE_KEY))

    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.alter_column("queue_key", nullable=False)
        batch_op.create_index(
            "ix_task_queue_key_id",
            ["queue_key", "id"],
            unique=False,
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.drop_index(
            "ix_task_labels",
            postgresql_using="gin",
            postgresql_where=sa.text("status = 'pending'"),
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_labels",
            ["labels"],
            unique=False,
            postgresql_using="gin",
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.drop_index(
            "ix_task_queue_key_id", postgresql_where=sa.text("status = 'pending'")
        )
        batch_op.drop_column("queue_key")
