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
"""Add the task claim indexes.

Revision ID: 002_task_claim_indexes
Revises: 001_initial
Create Date: 2026-08-17

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "002_task_claim_indexes"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_kind_id",
            ["kind", "id"],
            unique=False,
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.create_index(
            "ix_task_agent_version_id_id",
            ["agent_version_id", "id"],
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
            "ix_task_agent_version_id_id",
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.drop_index(
            "ix_task_kind_id", postgresql_where=sa.text("status = 'pending'")
        )
