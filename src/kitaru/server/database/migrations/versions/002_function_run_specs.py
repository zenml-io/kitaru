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
"""Function run specs.

Revision ID: 002_function_run_specs
Revises: 001_initial
Create Date: 2026-08-07

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "002_function_run_specs"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("agent_version", schema=None) as batch_op:
        batch_op.add_column(sa.Column("run_entrypoint", sa.Text(), nullable=True))

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.create_index(
            "uq_session_owner_id_external_id",
            ["owner_id", "external_id"],
            unique=True,
            postgresql_where=sa.text("status = 'pending_import'"),
        )

    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "provisional",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_column("provisional")

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_index(
            "uq_session_owner_id_external_id",
            postgresql_where=sa.text("status = 'pending_import'"),
        )

    with op.batch_alter_table("agent_version", schema=None) as batch_op:
        batch_op.drop_column("run_entrypoint")
