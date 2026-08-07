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
"""Worker pool table and worker pool membership.

Revision ID: 002_worker_pools
Revises: 001_initial
Create Date: 2026-08-08 00:29:36.586379

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "002_worker_pools"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "worker_pool",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("scope", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_worker_pool_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_worker_pool_name"),
    )
    with op.batch_alter_table("worker_pool", schema=None) as batch_op:
        batch_op.create_index("ix_worker_pool_owner_id", ["owner_id"], unique=False)

    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.add_column(sa.Column("pool_id", sa.Uuid(), nullable=True))
        # Backfill existing rows through a server default, then drop it so
        # the column matches the ORM declaration.
        batch_op.add_column(
            sa.Column(
                "concurrency", sa.Integer(), nullable=False, server_default=sa.text("1")
            )
        )
        batch_op.alter_column("concurrency", server_default=None)
        batch_op.create_index("ix_worker_pool_id", ["pool_id"], unique=False)
        batch_op.create_foreign_key(
            "fk_worker_pool_id", "worker_pool", ["pool_id"], ["id"]
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.drop_constraint("fk_worker_pool_id", type_="foreignkey")
        batch_op.drop_index("ix_worker_pool_id")
        batch_op.drop_column("concurrency")
        batch_op.drop_column("pool_id")

    with op.batch_alter_table("worker_pool", schema=None) as batch_op:
        batch_op.drop_index("ix_worker_pool_owner_id")

    op.drop_table("worker_pool")
