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
"""Analyzer Alembic revision.

Revision ID: 014_analyzer
Revises: 013_import
Create Date: 2026-09-04

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from kitaru.server.adapters.db.orm.insight import (
    INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
    INSIGHT_ANALYZER_VERSION_ID_INDEX,
    INSIGHT_TASK_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.task import (
    TASK_AGENT_ID_FOREIGN_KEY,
    TASK_AGENT_ID_INDEX,
)

# revision identifiers, used by Alembic.
revision = "014_analyzer"
down_revision = "013_import"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.add_column(sa.Column("agent_id", sa.Uuid(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "input_session_ids",
                postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                nullable=True,
            )
        )
        batch_op.create_index(TASK_AGENT_ID_INDEX, ["agent_id"], unique=False)
        batch_op.create_foreign_key(
            TASK_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "analyzers",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            )
        )

    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.add_column(sa.Column("analyzer_version_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("task_id", sa.Uuid(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "analyzer_params",
                postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("params_hash", sa.String(length=64), nullable=True)
        )
        batch_op.create_index(
            INSIGHT_ANALYZER_VERSION_ID_INDEX, ["analyzer_version_id"], unique=False
        )
        batch_op.create_foreign_key(
            INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
            "plugin_version",
            ["analyzer_version_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_foreign_key(
            INSIGHT_TASK_ID_FOREIGN_KEY,
            "task",
            ["task_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.drop_constraint(INSIGHT_TASK_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.drop_constraint(
            INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY, type_="foreignkey"
        )
        batch_op.drop_index(INSIGHT_ANALYZER_VERSION_ID_INDEX)
        batch_op.drop_column("params_hash")
        batch_op.drop_column("analyzer_params")
        batch_op.drop_column("task_id")
        batch_op.drop_column("analyzer_version_id")

    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.drop_column("analyzers")

    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_constraint(TASK_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.drop_index(TASK_AGENT_ID_INDEX)
        batch_op.drop_column("input_session_ids")
        batch_op.drop_column("agent_id")
