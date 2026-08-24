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
"""Task input references by id.

Revision ID: 007_task_input_references
Revises: 006_replay_job_id_nullable
Create Date: 2026-08-23

"""

from alembic import op

from kitaru.server.adapters.db.orm.task import (
    TASK_AGENT_ID_FOREIGN_KEY,
    TASK_AGENT_VERSION_ID_FOREIGN_KEY,
    TASK_INPUT_SESSION_ID_FOREIGN_KEY,
    TASK_PAYLOAD_BLOB_ID_FOREIGN_KEY,
    TASK_PLUGIN_VERSION_ID_FOREIGN_KEY,
)

# revision identifiers, used by Alembic.
revision = "007_task_input_references"
down_revision = "006_replay_job_id_nullable"
branch_labels = None
depends_on = None

# Child column, parent table, and parent column of every input reference the
# task table used to constrain.
INPUT_FOREIGN_KEYS = (
    (TASK_AGENT_VERSION_ID_FOREIGN_KEY, "agent_version_id", "agent_version"),
    (TASK_AGENT_ID_FOREIGN_KEY, "agent_id", "agent"),
    (TASK_PLUGIN_VERSION_ID_FOREIGN_KEY, "plugin_version_id", "plugin_version"),
    (TASK_PAYLOAD_BLOB_ID_FOREIGN_KEY, "payload_blob_id", "blob"),
    (TASK_INPUT_SESSION_ID_FOREIGN_KEY, "input_session_id", "session"),
)


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("task", schema=None) as batch_op:
        for name, _column, _parent in INPUT_FOREIGN_KEYS:
            batch_op.drop_constraint(name, type_="foreignkey")


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # A task pointing at a row that was deleted while the constraint was gone
    # fails the restored foreign key. Such a row is resolved by hand rather
    # than dropped here, since the task is a record of work the downgrade has
    # no right to remove.
    with op.batch_alter_table("task", schema=None) as batch_op:
        for name, column, parent in INPUT_FOREIGN_KEYS:
            batch_op.create_foreign_key(
                name, parent, [column], ["id"], ondelete="CASCADE"
            )
