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
"""Connection Alembic revision.

Revision ID: 017_connection
Revises: 016_analyzer
Create Date: 2026-09-07

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "017_connection"
down_revision = "016_analyzer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "connection",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("provider", sa.String(length=255), nullable=False),
        sa.Column("env", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("secret_id", sa.Uuid(), nullable=False),
        sa.Column("default", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_connection_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["secret_id"], ["secret.id"], name="fk_connection_secret_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_connection_name"),
    )
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.create_index("ix_connection_owner_id", ["owner_id"], unique=False)
        batch_op.create_index(
            "uq_connection_provider",
            ["provider"],
            unique=True,
            postgresql_where=sa.text('"default"'),
        )

    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "connection_schema",
                postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                nullable=True,
            )
        )

    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.add_column(sa.Column("connection_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_import_connection_id",
            "connection",
            ["connection_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.drop_constraint("fk_import_connection_id", type_="foreignkey")
        batch_op.drop_column("connection_id")

    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.drop_column("connection_schema")

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.drop_index(
            "uq_connection_provider", postgresql_where=sa.text('"default"')
        )
        batch_op.drop_index("ix_connection_owner_id")
    op.drop_table("connection")
