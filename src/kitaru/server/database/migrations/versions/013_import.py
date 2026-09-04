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
"""Import Alembic revision.

Revision ID: 013_import
Revises: 012_insight
Create Date: 2026-09-04

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "013_import"
down_revision = "012_insight"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "import",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=True),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=True),
        sa.Column("importer_version_id", sa.Uuid(), nullable=False),
        sa.Column("payload_blob_id", sa.Uuid(), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "evaluators", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "stats",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name="fk_import_agent_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name="fk_import_agent_version_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["importer_version_id"],
            ["plugin_version.id"],
            name="fk_import_importer_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["job.id"], name="fk_import_job_id", ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_import_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["payload_blob_id"], ["blob.id"], name="fk_import_payload_blob_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="uq_import_job_id"),
    )
    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.create_index("ix_import_agent_id", ["agent_id"], unique=False)

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.add_column(sa.Column("import_id", sa.Uuid(), nullable=True))
        batch_op.create_index("ix_session_import_id", ["import_id"], unique=False)
        batch_op.create_foreign_key(
            "fk_session_import_id",
            "import",
            ["import_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # The import row now carries the request, so the task keeps only a
    # reference to it. Pending import tasks written before this revision
    # cannot run afterwards.
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(sa.Column("import_id", sa.Uuid(), nullable=True))
        batch_op.create_index("ix_task_import_id", ["import_id"], unique=False)
        batch_op.drop_column("payload_blob_id")
        batch_op.drop_column("agent_id")


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(sa.Column("agent_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("payload_blob_id", sa.Uuid(), nullable=True))
        batch_op.drop_index("ix_task_import_id")
        batch_op.drop_column("import_id")

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_constraint("fk_session_import_id", type_="foreignkey")
        batch_op.drop_index("ix_session_import_id")
        batch_op.drop_column("import_id")

    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.drop_index("ix_import_agent_id")
    op.drop_table("import")
