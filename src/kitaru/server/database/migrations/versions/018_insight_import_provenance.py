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
"""Preserve insight import provenance independently of task cleanup.

Revision ID: 018_insight_import_provenance
Revises: 017_connection
Create Date: 2026-09-08 14:47:29.882810

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "018_insight_import_provenance"
down_revision = "017_connection"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.add_column(sa.Column("import_id", sa.Uuid(), nullable=True))
        batch_op.create_index("ix_insight_import_id", ["import_id"], unique=False)
        batch_op.create_foreign_key(
            "fk_insight_import_id", "import", ["import_id"], ["id"], ondelete="SET NULL"
        )

    # Recover existing associations before operational tasks can be removed.
    op.execute(
        sa.text("""
        UPDATE insight
        SET import_id = task.import_id
        FROM task JOIN "import" ON "import".id = task.import_id
        WHERE insight.task_id = task.id
    """)
    )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.drop_constraint("fk_insight_import_id", type_="foreignkey")
        batch_op.drop_index("ix_insight_import_id")
        batch_op.drop_column("import_id")
