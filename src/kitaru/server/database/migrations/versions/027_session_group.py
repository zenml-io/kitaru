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
"""Add session groups and the group an analysis task reads.

Revision ID: 027_session_group
Revises: 026_last_user_activity
Create Date: 2026-11-12

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "027_session_group"
down_revision = "026_last_user_activity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "session_group",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name="fk_session_group_job_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("session_group", schema=None) as batch_op:
        batch_op.create_index("ix_session_group_job_id", ["job_id"], unique=False)

    op.create_table(
        "session_group_session",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_group_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_group_id"],
            ["session_group.id"],
            name="fk_session_group_session_session_group_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_session_group_session_session_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("session_group_id", "session_id"),
    )
    with op.batch_alter_table("session_group_session", schema=None) as batch_op:
        batch_op.create_index(
            "ix_session_group_session_session_id", ["session_id"], unique=False
        )

    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(sa.Column("session_group_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_column("session_group_id")

    with op.batch_alter_table("session_group_session", schema=None) as batch_op:
        batch_op.drop_index("ix_session_group_session_session_id")
    op.drop_table("session_group_session")

    with op.batch_alter_table("session_group", schema=None) as batch_op:
        batch_op.drop_index("ix_session_group_job_id")
    op.drop_table("session_group")
