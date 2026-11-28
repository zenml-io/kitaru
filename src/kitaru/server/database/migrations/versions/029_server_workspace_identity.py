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
"""Server workspace identity Alembic revision.

Revision ID: 029_server_workspace_identity
Revises: 028_session_sort_indexes
Create Date: 2026-11-27

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "029_server_workspace_identity"
down_revision = "028_session_sort_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("server_settings", schema=None) as batch_op:
        batch_op.add_column(sa.Column("organization_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("organization_name", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("workspace_name", sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("server_settings", schema=None) as batch_op:
        batch_op.drop_column("workspace_name")
        batch_op.drop_column("organization_name")
        batch_op.drop_column("organization_id")
