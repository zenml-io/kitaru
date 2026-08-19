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
"""Idempotency key Alembic revision.

Revision ID: 002_idempotency_key
Revises: 001_initial
Create Date: 2026-08-19

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "002_idempotency_key"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "idempotency_key",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("method", sa.String(length=16), nullable=False),
        sa.Column("path", sa.String(length=2048), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body", sa.LargeBinary(), nullable=True),
        sa.Column("response_content_type", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["account.id"],
            name="fk_idempotency_key_account_id",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "account_id", "key", name="uq_idempotency_key_account_id_key"
        ),
    )
    with op.batch_alter_table("idempotency_key", schema=None) as batch_op:
        batch_op.create_index("ix_idempotency_key_created", ["created"], unique=False)


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("idempotency_key", schema=None) as batch_op:
        batch_op.drop_index("ix_idempotency_key_created")

    op.drop_table("idempotency_key")
