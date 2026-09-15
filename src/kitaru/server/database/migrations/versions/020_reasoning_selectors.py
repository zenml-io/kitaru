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
"""Reasoning selectors Alembic revision.

Revision ID: 020_reasoning_selectors
Revises: 019_import_max_sessions
Create Date: 2026-09-11

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "020_reasoning_selectors"
down_revision = "019_import_max_sessions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_session_node_reasoning_blob_id", type_="foreignkey"
        )
        batch_op.drop_column("reasoning_blob_id")
        batch_op.drop_column("reasoning")
        batch_op.add_column(
            sa.Column(
                "reasoning_selectors",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            )
        )
        batch_op.alter_column("reasoning_selectors", server_default=None)


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.drop_column("reasoning_selectors")
        batch_op.add_column(sa.Column("reasoning", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("reasoning_blob_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_session_node_reasoning_blob_id", "blob", ["reasoning_blob_id"], ["id"]
        )
