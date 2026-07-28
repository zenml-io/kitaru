"""add device

Revision ID: 006_add_device
Revises: 005_name_foreign_keys
Create Date: 2026-07-28 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "006_add_device"
down_revision = "005_name_foreign_keys"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "device",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=True),
        sa.Column("user_code_hash", sa.String(length=128), nullable=False),
        sa.Column("device_code_hash", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("locked", sa.Boolean(), nullable=False),
        sa.Column("trusted", sa.Boolean(), nullable=False),
        sa.Column("failed_auth_attempts", sa.Integer(), nullable=False),
        sa.Column("expires", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
        sa.Column("hostname", sa.String(length=255), nullable=True),
        sa.Column("os", sa.String(length=255), nullable=True),
        sa.Column("ip_address", sa.String(length=255), nullable=True),
        sa.Column("python_version", sa.String(length=255), nullable=True),
        sa.Column("client_version", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["account.id"],
            name="fk_device_account_id",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("device", schema=None) as batch_op:
        batch_op.create_index("ix_device_account_id", ["account_id"], unique=False)
        batch_op.create_index("ix_device_expires", ["expires"], unique=False)


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("device", schema=None) as batch_op:
        batch_op.drop_index("ix_device_expires")
        batch_op.drop_index("ix_device_account_id")

    op.drop_table("device")
