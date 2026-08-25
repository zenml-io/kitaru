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
"""Payload blob offload Alembic revision.

Revision ID: 005_payload_blob_offload
Revises: 004_replay_result_session_id
Create Date: 2026-08-25

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "005_payload_blob_offload"
down_revision = "004_replay_result_session_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.add_column(sa.Column("stored_in", sa.String(length=16), nullable=True))
    op.execute("UPDATE blob SET stored_in = 'database'")
    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.alter_column("stored_in", nullable=False)

    op.create_table(
        "blob_content",
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.PrimaryKeyConstraint("sha256"),
    )
    op.execute("INSERT INTO blob_content (sha256, data) SELECT sha256, data FROM blob")

    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.drop_column("data")

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.add_column(sa.Column("inputs_blob_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("outputs_blob_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_session_inputs_blob_id", "blob", ["inputs_blob_id"], ["id"]
        )
        batch_op.create_foreign_key(
            "fk_session_outputs_blob_id", "blob", ["outputs_blob_id"], ["id"]
        )

    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.add_column(sa.Column("inputs_blob_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("outputs_blob_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("attributes_blob_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("reasoning_blob_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_session_node_inputs_blob_id", "blob", ["inputs_blob_id"], ["id"]
        )
        batch_op.create_foreign_key(
            "fk_session_node_outputs_blob_id", "blob", ["outputs_blob_id"], ["id"]
        )
        batch_op.create_foreign_key(
            "fk_session_node_attributes_blob_id",
            "blob",
            ["attributes_blob_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_session_node_reasoning_blob_id", "blob", ["reasoning_blob_id"], ["id"]
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_session_node_reasoning_blob_id", type_="foreignkey"
        )
        batch_op.drop_constraint(
            "fk_session_node_attributes_blob_id", type_="foreignkey"
        )
        batch_op.drop_constraint("fk_session_node_outputs_blob_id", type_="foreignkey")
        batch_op.drop_constraint("fk_session_node_inputs_blob_id", type_="foreignkey")
        batch_op.drop_column("reasoning_blob_id")
        batch_op.drop_column("attributes_blob_id")
        batch_op.drop_column("outputs_blob_id")
        batch_op.drop_column("inputs_blob_id")

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_constraint("fk_session_outputs_blob_id", type_="foreignkey")
        batch_op.drop_constraint("fk_session_inputs_blob_id", type_="foreignkey")
        batch_op.drop_column("outputs_blob_id")
        batch_op.drop_column("inputs_blob_id")

    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.add_column(sa.Column("data", sa.LargeBinary(), nullable=True))
    op.execute(
        "UPDATE blob SET data = blob_content.data "
        "FROM blob_content WHERE blob_content.sha256 = blob.sha256"
    )
    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.alter_column("data", nullable=False)
        batch_op.drop_column("stored_in")

    op.drop_table("blob_content")
