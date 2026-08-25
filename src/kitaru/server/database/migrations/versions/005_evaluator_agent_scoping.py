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
"""Optional agent scoping for evaluator plugins.

Revision ID: 005_evaluator_agent_scoping
Revises: 004_replay_result_session_id
Create Date: 2026-08-25

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "005_evaluator_agent_scoping"
down_revision = "004_replay_result_session_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.add_column(sa.Column("agent_id", sa.Uuid(), nullable=True))
        batch_op.create_index("ix_plugin_agent_id", ["agent_id"], unique=False)
        batch_op.create_foreign_key(
            "fk_plugin_agent_id", "agent", ["agent_id"], ["id"], ondelete="SET NULL"
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.drop_constraint("fk_plugin_agent_id", type_="foreignkey")
        batch_op.drop_index("ix_plugin_agent_id")
        batch_op.drop_column("agent_id")
