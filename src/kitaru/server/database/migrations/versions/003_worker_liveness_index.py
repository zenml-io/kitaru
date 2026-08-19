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
"""Drop the worker name unique constraint and index the liveness cutoff.

Revision ID: 003_worker_liveness_index
Revises: 002_idempotency_key
Create Date: 2026-08-19

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "003_worker_liveness_index"
down_revision = "002_idempotency_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.drop_constraint("uq_worker_name", type_="unique")
        batch_op.create_index(
            "ix_worker_last_seen_at_id", ["last_seen_at", "id"], unique=False
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # Registrations under a shared name are what this revision allows, so the
    # rows the unique constraint would reject are deleted before it comes back.
    op.execute(
        "DELETE FROM worker WHERE id NOT IN "
        "(SELECT DISTINCT ON (name) id FROM worker ORDER BY name, last_seen_at DESC)"
    )
    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.drop_index("ix_worker_last_seen_at_id")
        batch_op.create_unique_constraint("uq_worker_name", ["name"])
