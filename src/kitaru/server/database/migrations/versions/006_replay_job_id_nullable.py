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
"""Nullable replay job pointer.

Revision ID: 006_replay_job_id_nullable
Revises: 005_deletion_rules
Create Date: 2026-08-21

"""

import sqlalchemy as sa
from alembic import op

from kitaru.server.adapters.db.orm.replay import REPLAY_JOB_ID_FOREIGN_KEY

# revision identifiers, used by Alembic.
revision = "006_replay_job_id_nullable"
down_revision = "005_deletion_rules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.alter_column("job_id", existing_type=sa.Uuid(), nullable=True)
        batch_op.drop_constraint(REPLAY_JOB_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            REPLAY_JOB_ID_FOREIGN_KEY,
            "job",
            ["job_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # A replay whose job was deleted carries a null job_id, which the restored
    # NOT NULL fails on. Such a row is resolved by hand rather than dropped
    # here, since the replay is a record the downgrade has no right to remove.
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.drop_constraint(REPLAY_JOB_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            REPLAY_JOB_ID_FOREIGN_KEY,
            "job",
            ["job_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.alter_column("job_id", existing_type=sa.Uuid(), nullable=False)
