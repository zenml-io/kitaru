"""add trace import jobs

Revision ID: 005_add_trace_import_jobs
Revises: 004_add_record_replay
Create Date: 2026-07-24 12:00:00.000000

"""

import sqlalchemy as sa
import sqlmodel.sql.sqltypes
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "005_add_trace_import_jobs"
down_revision = "004_add_record_replay"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create import jobs and imported-session provenance."""
    op.create_table(
        "import_job",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=False),
        sa.Column(
            "importer_id",
            sqlmodel.sql.sqltypes.AutoString(length=255),
            nullable=False,
        ),
        sa.Column(
            "importer_version",
            sqlmodel.sql.sqltypes.AutoString(length=64),
            nullable=False,
        ),
        sa.Column(
            "source_instance",
            sqlmodel.sql.sqltypes.AutoString(length=255),
            nullable=True,
        ),
        sa.Column(
            "filename",
            sqlmodel.sql.sqltypes.AutoString(length=255),
            nullable=False,
        ),
        sa.Column("content", sa.LargeBinary(), nullable=True),
        sa.Column(
            "status",
            sqlmodel.sql.sqltypes.AutoString(length=32),
            nullable=False,
        ),
        sa.Column(
            "worker_id",
            sqlmodel.sql.sqltypes.AutoString(length=255),
            nullable=True,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_session_count", sa.Integer(), nullable=False),
        sa.Column("imported_count", sa.Integer(), nullable=False),
        sa.Column("deduplicated_count", sa.Integer(), nullable=False),
        sa.Column("failed_count", sa.Integer(), nullable=False),
        sa.Column(
            "session_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "errors",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name="fk_import_job_agent_version_id",
        ),
        sa.ForeignKeyConstraint(["owner_id"], ["account.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("import_job", schema=None) as batch_op:
        batch_op.create_index("ix_import_job_owner_id", ["owner_id"], unique=False)
        batch_op.create_index(
            "ix_import_job_status_created", ["status", "created"], unique=False
        )

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_constraint("uq_session_provider_external_id", type_="unique")
        batch_op.alter_column(
            "provider",
            existing_type=sqlmodel.sql.sqltypes.AutoString(length=32),
            type_=sqlmodel.sql.sqltypes.AutoString(length=255),
            existing_nullable=True,
        )
        batch_op.add_column(
            sa.Column(
                "source_instance",
                sqlmodel.sql.sqltypes.AutoString(length=255),
                nullable=True,
            )
        )
        batch_op.add_column(sa.Column("source_revision", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "source_digest",
                sqlmodel.sql.sqltypes.AutoString(length=64),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "source_metadata",
                postgresql.JSONB(astext_type=sa.Text()),
                server_default=sa.text("'{}'::jsonb"),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "replay_readiness",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "normalization_warnings",
                postgresql.JSONB(astext_type=sa.Text()),
                server_default=sa.text("'[]'::jsonb"),
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("import_job_id", sa.Uuid(), nullable=True))
        batch_op.add_column(
            sa.Column("supersedes_session_id", sa.Uuid(), nullable=True)
        )
        batch_op.create_foreign_key(
            "fk_session_import_job_id",
            "import_job",
            ["import_job_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_session_supersedes_session_id",
            "session",
            ["supersedes_session_id"],
            ["id"],
        )
        batch_op.create_unique_constraint(
            "uq_session_import_revision",
            [
                "owner_id",
                "provider",
                "source_instance",
                "external_id",
                "source_revision",
            ],
        )
        batch_op.create_unique_constraint(
            "uq_session_import_digest",
            [
                "owner_id",
                "provider",
                "source_instance",
                "external_id",
                "source_digest",
            ],
        )
        batch_op.create_index(
            "uq_session_provider_external_id",
            ["provider", "external_id"],
            unique=True,
            postgresql_where=sa.text("source_revision IS NULL"),
        )
        batch_op.create_index(
            "ix_session_provider_source_instance_external_id",
            ["provider", "source_instance", "external_id"],
            unique=False,
        )


def downgrade() -> None:
    """Remove import jobs and imported-session provenance."""
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_index("ix_session_provider_source_instance_external_id")
        batch_op.drop_index(
            "uq_session_provider_external_id",
            postgresql_where=sa.text("source_revision IS NULL"),
        )
        batch_op.drop_constraint(
            "uq_session_import_digest",
            type_="unique",
        )
        batch_op.drop_constraint(
            "uq_session_import_revision",
            type_="unique",
        )
        batch_op.drop_constraint("fk_session_supersedes_session_id", type_="foreignkey")
        batch_op.drop_constraint("fk_session_import_job_id", type_="foreignkey")
        batch_op.drop_column("supersedes_session_id")
        batch_op.drop_column("import_job_id")
        batch_op.drop_column("normalization_warnings")
        batch_op.drop_column("replay_readiness")
        batch_op.drop_column("source_metadata")
        batch_op.drop_column("source_digest")
        batch_op.drop_column("source_revision")
        batch_op.drop_column("source_instance")
        batch_op.alter_column(
            "provider",
            existing_type=sqlmodel.sql.sqltypes.AutoString(length=255),
            type_=sqlmodel.sql.sqltypes.AutoString(length=32),
            existing_nullable=True,
        )
        batch_op.create_unique_constraint(
            "uq_session_provider_external_id", ["provider", "external_id"]
        )

    with op.batch_alter_table("import_job", schema=None) as batch_op:
        batch_op.drop_index("ix_import_job_status_created")
        batch_op.drop_index("ix_import_job_owner_id")
    op.drop_table("import_job")
