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
"""API imports Alembic revision.

Revision ID: 014_api_imports
Revises: 013_import
Create Date: 2026-09-04

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "014_api_imports"
down_revision = "013_import"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("plugin_version", schema=None) as batch_op:
        batch_op.add_column(sa.Column("fetch_entrypoint", sa.Text(), nullable=True))
    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.alter_column("payload_blob_id", existing_type=sa.Uuid(), nullable=True)
        batch_op.add_column(
            sa.Column(
                "fetch_query",
                postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                nullable=True,
            )
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("import", schema=None) as batch_op:
        batch_op.drop_column("fetch_query")
        batch_op.alter_column(
            "payload_blob_id", existing_type=sa.Uuid(), nullable=False
        )
    with op.batch_alter_table("plugin_version", schema=None) as batch_op:
        batch_op.drop_column("fetch_entrypoint")
