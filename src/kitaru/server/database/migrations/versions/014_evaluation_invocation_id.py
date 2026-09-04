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
"""Evaluation invocation id Alembic revision.

Revision ID: 014_evaluation_invocation_id
Revises: 013_import
Create Date: 2026-09-04

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "014_evaluation_invocation_id"
down_revision = "013_import"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.add_column(sa.Column("invocation_id", sa.Uuid(), nullable=True))

    # Backfill from the task id because every evaluator call before this
    # revision wrote exactly one task. Rows whose task was already pruned stay
    # null and are not adoptable.
    op.execute(
        "UPDATE evaluation SET invocation_id = task_id WHERE task_id IS NOT NULL"
    )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.drop_column("invocation_id")
