"""add job kinds

Revision ID: 008_add_job_kinds
Revises: 007_rename_replay_to_job
Create Date: 2026-07-24 14:21:12.000000

"""

import sqlalchemy as sa
import sqlmodel.sql.sqltypes
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "008_add_job_kinds"
down_revision = "007_rename_replay_to_job"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "kind",
                sqlmodel.sql.sqltypes.AutoString(length=16),
                nullable=False,
                server_default="replay",
            )
        )
        batch_op.add_column(
            sa.Column(
                "execution_target",
                sqlmodel.sql.sqltypes.AutoString(length=16),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "executor_handle",
                sqlmodel.sql.sqltypes.AutoString(length=255),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("inputs", postgresql.JSONB(astext_type=sa.Text()), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "name", sqlmodel.sql.sqltypes.AutoString(length=255), nullable=True
            )
        )
        batch_op.alter_column("kind", server_default=None)
        batch_op.alter_column(
            "replay_config_id", existing_type=sa.Uuid(), nullable=True
        )
        batch_op.alter_column(
            "original_session_id", existing_type=sa.Uuid(), nullable=True
        )
        # Existing worker ids are names, not worker uuids, so the column is
        # recreated instead of converted.
        batch_op.drop_column("worker_id")
        batch_op.add_column(sa.Column("worker_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_job_worker_id",
            "worker",
            ["worker_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_constraint("fk_job_worker_id", type_="foreignkey")
        batch_op.drop_column("worker_id")
        batch_op.add_column(
            sa.Column(
                "worker_id", sa.VARCHAR(length=255), autoincrement=False, nullable=True
            )
        )
        batch_op.alter_column(
            "original_session_id", existing_type=sa.Uuid(), nullable=False
        )
        batch_op.alter_column(
            "replay_config_id", existing_type=sa.Uuid(), nullable=False
        )
        batch_op.drop_column("name")
        batch_op.drop_column("inputs")
        batch_op.drop_column("executor_handle")
        batch_op.drop_column("execution_target")
        batch_op.drop_column("kind")
