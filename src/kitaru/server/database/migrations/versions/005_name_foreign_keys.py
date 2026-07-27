"""name foreign keys

Revision ID: 005_name_foreign_keys
Revises: 004_add_account_external_id
Create Date: 2026-07-27 16:20:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "005_name_foreign_keys"
down_revision = "004_add_account_external_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.execute(
        "ALTER TABLE api_key RENAME CONSTRAINT api_key_owner_id_fkey"
        " TO fk_api_key_owner_id"
    )
    op.execute(
        "ALTER TABLE secret RENAME CONSTRAINT secret_owner_id_fkey"
        " TO fk_secret_owner_id"
    )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    op.execute(
        "ALTER TABLE api_key RENAME CONSTRAINT fk_api_key_owner_id"
        " TO api_key_owner_id_fkey"
    )
    op.execute(
        "ALTER TABLE secret RENAME CONSTRAINT fk_secret_owner_id"
        " TO secret_owner_id_fkey"
    )
