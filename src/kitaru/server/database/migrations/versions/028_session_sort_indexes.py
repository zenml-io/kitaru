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
"""Add composite sort indexes for session started_at and number.

Revision ID: 028_session_sort_indexes
Revises: 027_session_group
Create Date: 2026-11-23

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "028_session_sort_indexes"
down_revision = "027_session_group"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.create_index(
            "ix_session_started_at_id", ["started_at", "id"], unique=False
        )
        batch_op.create_index("ix_session_number_id", ["number", "id"], unique=False)


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_index("ix_session_number_id")
        batch_op.drop_index("ix_session_started_at_id")
