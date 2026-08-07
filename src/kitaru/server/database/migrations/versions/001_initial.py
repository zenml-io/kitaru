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
"""Initial Alembic revision.

Revision ID: 001_initial
Revises:
Create Date: 2026-07-22

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    op.create_table(
        "account",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("is_service_account", sa.Boolean(), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("password_hash", sa.String(length=128), nullable=True),
        sa.Column("activation_token_hash", sa.String(length=64), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("external_id", sa.Uuid(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "name", "is_service_account", name="uq_account_name_is_service_account"
        ),
    )
    with op.batch_alter_table("account", schema=None) as batch_op:
        batch_op.create_index("ix_account_external_id", ["external_id"], unique=False)

    op.create_table(
        "api_key",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("key_hash", sa.String(length=128), nullable=False),
        sa.Column("previous_key_hash", sa.String(length=128), nullable=True),
        sa.Column("retain_period_minutes", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("last_used", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_rotated", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_api_key_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_api_key_name"),
    )
    with op.batch_alter_table("api_key", schema=None) as batch_op:
        batch_op.create_index("ix_api_key_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "secret",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("internal", sa.Boolean(), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=True),
        sa.Column("values_encrypted", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_secret_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_secret_name"),
    )
    with op.batch_alter_table("secret", schema=None) as batch_op:
        batch_op.create_index("ix_secret_owner_id", ["owner_id"], unique=False)

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

    op.create_table(
        "agent",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("latest_version", sa.Integer(), nullable=False),
        sa.Column("latest_session_number", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["owner_id"], ["account.id"], name="fk_agent_owner_id"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_agent_name"),
    )
    with op.batch_alter_table("agent", schema=None) as batch_op:
        batch_op.create_index("ix_agent_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "blob",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("media_type", sa.String(length=255), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.ForeignKeyConstraint(["owner_id"], ["account.id"], name="fk_blob_owner_id"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sha256", name="uq_blob_sha256"),
    )
    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.create_index("ix_blob_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "job",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["owner_id"], ["account.id"], name="fk_job_owner_id"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.create_index(
            "ix_job_cancel_requested_at",
            ["cancel_requested_at"],
            unique=False,
            postgresql_where=sa.text("cancel_requested_at IS NOT NULL"),
        )
        batch_op.create_index("ix_job_kind", ["kind"], unique=False)
        batch_op.create_index("ix_job_status", ["status"], unique=False)

    op.create_table(
        "plugin",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("provider", sa.String(length=255), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("latest_version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_plugin_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kind", "name", name="uq_plugin_kind_name"),
    )
    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.create_index(
            "ix_plugin_kind_provider", ["kind", "provider"], unique=False
        )
        batch_op.create_index("ix_plugin_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "replay_config",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column(
            "override",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "tool_policy", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "evaluators", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_replay_config_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tag",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["owner_id"], ["account.id"], name="fk_tag_owner_id"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_tag_name"),
    )
    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.create_index("ix_tag_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "worker",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("scope", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("runtime", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_worker_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_worker_name"),
    )
    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.create_index("ix_worker_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "agent_version",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("display_version", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("run_command", sa.Text(), nullable=True),
        sa.Column("run_working_dir", sa.Text(), nullable=True),
        sa.Column(
            "run_env",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("run_timeout_seconds", sa.Integer(), nullable=True),
        sa.Column(
            "capabilities", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name="fk_agent_version_agent_id"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_agent_version_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "agent_id", "version", name="uq_agent_version_agent_id_version"
        ),
    )
    with op.batch_alter_table("agent_version", schema=None) as batch_op:
        batch_op.create_index("ix_agent_version_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "cohort",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("latest_version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], ["agent.id"], name="fk_cohort_agent_id"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_cohort_name"),
    )
    op.create_table(
        "cohort_version",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("cohort_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("display_version", sa.String(length=255), nullable=True),
        sa.Column("session_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name="fk_cohort_version_cohort_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "cohort_id", "version", name="uq_cohort_version_cohort_id_version"
        ),
    )
    op.create_table(
        "experiment",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("replay_config_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_experiment_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name="fk_experiment_replay_config_id",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_experiment_name"),
    )
    op.create_table(
        "plugin_version",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("plugin_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("display_version", sa.String(length=255), nullable=True),
        sa.Column("type", sa.String(length=16), nullable=False),
        sa.Column("blob_id", sa.Uuid(), nullable=True),
        sa.Column("requirement", sa.String(length=255), nullable=True),
        sa.Column("entrypoint", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "(type = 'script' AND blob_id IS NOT NULL AND requirement IS NULL) "
            "OR (type = 'package' AND blob_id IS NULL AND requirement IS NOT NULL)",
            name="ck_plugin_version_type_blob_id_requirement",
        ),
        sa.ForeignKeyConstraint(
            ["blob_id"], ["blob.id"], name="fk_plugin_version_blob_id"
        ),
        sa.ForeignKeyConstraint(
            ["plugin_id"],
            ["plugin.id"],
            name="fk_plugin_version_plugin_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "plugin_id", "version", name="uq_plugin_version_plugin_id_version"
        ),
    )
    op.create_table(
        "tag_link",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tag_id", sa.Uuid(), nullable=False),
        sa.Column("resource_type", sa.String(length=32), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tag_id"], ["tag.id"], name="fk_tag_link_tag_id", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tag_id",
            "resource_type",
            "resource_id",
            name="uq_tag_link_tag_id_resource_type_resource_id",
        ),
    )
    with op.batch_alter_table("tag_link", schema=None) as batch_op:
        batch_op.create_index(
            "ix_tag_link_resource_type_resource_id",
            ["resource_type", "resource_id"],
            unique=False,
        )

    op.create_table(
        "agent_version_secret",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=False),
        sa.Column("secret_id", sa.Uuid(), nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name="fk_agent_version_secret_agent_version_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["secret_id"], ["secret.id"], name="fk_agent_version_secret_secret_id"
        ),
        sa.PrimaryKeyConstraint("agent_version_id", "secret_id"),
        sa.UniqueConstraint(
            "agent_version_id",
            "index",
            name="uq_agent_version_secret_agent_version_id_index",
        ),
    )
    op.create_table(
        "experiment_run",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("experiment_id", sa.Uuid(), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("cohort_version_id", sa.Uuid(), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=False),
        sa.Column("evaluate_baselines", sa.Boolean(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name="fk_experiment_run_agent_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["cohort_version_id"],
            ["cohort_version.id"],
            name="fk_experiment_run_cohort_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["experiment_id"], ["experiment.id"], name="fk_experiment_run_experiment_id"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_experiment_run_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "experiment_id", "number", name="uq_experiment_run_experiment_id_number"
        ),
    )
    with op.batch_alter_table("experiment_run", schema=None) as batch_op:
        batch_op.create_index(
            "ix_experiment_run_agent_version_id", ["agent_version_id"], unique=False
        )
        batch_op.create_index(
            "ix_experiment_run_cohort_version_id", ["cohort_version_id"], unique=False
        )

    op.create_table(
        "session",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=True),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column("origin", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column(
            "inputs",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "outputs",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("imported_from", sa.Text(), nullable=True),
        sa.Column("framework", sa.Text(), nullable=True),
        sa.Column("adapter_version", sa.Text(), nullable=True),
        sa.Column("cost", sa.Numeric(), nullable=True),
        sa.Column("input_tokens", sa.BigInteger(), nullable=True),
        sa.Column("output_tokens", sa.BigInteger(), nullable=True),
        sa.Column("cached_input_tokens", sa.BigInteger(), nullable=True),
        sa.Column("reasoning_tokens", sa.BigInteger(), nullable=True),
        sa.Column("llm_call_count", sa.Integer(), nullable=False),
        sa.Column("tool_call_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], ["agent.id"], name="fk_session_agent_id"),
        sa.ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name="fk_session_agent_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_session_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "imported_from",
            "external_id",
            name="uq_session_imported_from_external_id",
        ),
        sa.UniqueConstraint("agent_id", "number", name="uq_session_agent_id_number"),
    )
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.create_index(
            "ix_session_agent_id_id", ["agent_id", "id"], unique=False
        )
        batch_op.create_index(
            "ix_session_agent_version_id_id", ["agent_version_id", "id"], unique=False
        )
        batch_op.create_index("ix_session_owner_id", ["owner_id"], unique=False)
        batch_op.create_index("ix_session_status", ["status"], unique=False)
        batch_op.create_index("ix_session_task_id", ["task_id"], unique=False)

    op.create_table(
        "cohort_version_session",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cohort_version_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cohort_version_id"],
            ["cohort_version.id"],
            name="fk_cohort_version_session_cohort_version_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_cohort_version_session_session_id",
        ),
        sa.PrimaryKeyConstraint("cohort_version_id", "session_id"),
        sa.UniqueConstraint(
            "cohort_version_id",
            "index",
            name="uq_cohort_version_session_cohort_version_id_index",
        ),
    )
    with op.batch_alter_table("cohort_version_session", schema=None) as batch_op:
        batch_op.create_index(
            "ix_cohort_version_session_session_id_cohort_version_id",
            ["session_id", "cohort_version_id"],
            unique=False,
        )

    op.create_table(
        "replay",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("experiment_run_id", sa.Uuid(), nullable=True),
        sa.Column("replay_config_id", sa.Uuid(), nullable=False),
        sa.Column("baseline_session_id", sa.Uuid(), nullable=False),
        sa.Column("evaluate_baselines", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["baseline_session_id"],
            ["session.id"],
            name="fk_replay_baseline_session_id",
        ),
        sa.ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name="fk_replay_experiment_run_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["job.id"], name="fk_replay_job_id", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_replay_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name="fk_replay_replay_config_id",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "experiment_run_id",
            "baseline_session_id",
            name="uq_replay_experiment_run_id_baseline_session_id",
        ),
        sa.UniqueConstraint("job_id", name="uq_replay_job_id"),
    )
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.create_index(
            "ix_replay_baseline_session_id", ["baseline_session_id"], unique=False
        )
        batch_op.create_index(
            "ix_replay_experiment_run_id_status",
            ["experiment_run_id", "status"],
            unique=False,
        )

    op.create_table(
        "session_node",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("parent_id", sa.Uuid(), nullable=True),
        sa.Column(
            "secondary_parent_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("node_type", sa.String(length=32), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("input_text_selector", sa.Text(), nullable=True),
        sa.Column("output_text_selector", sa.Text(), nullable=True),
        sa.Column("system_prompt_selector", sa.Text(), nullable=True),
        sa.Column("reasoning", sa.Text(), nullable=True),
        sa.Column(
            "inputs",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "outputs",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("requested_model", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("input_tokens", sa.BigInteger(), nullable=True),
        sa.Column("output_tokens", sa.BigInteger(), nullable=True),
        sa.Column("cached_input_tokens", sa.BigInteger(), nullable=True),
        sa.Column("reasoning_tokens", sa.BigInteger(), nullable=True),
        sa.Column("cost", sa.Numeric(), nullable=True),
        sa.Column(
            "model_params",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("tool_name", sa.Text(), nullable=True),
        sa.Column("cache_key", sa.CHAR(length=64), nullable=True),
        sa.Column("subagent_id", sa.String(length=255), nullable=True),
        sa.Column(
            "attributes",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_session_node_session_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id", "external_id", name="uq_session_node_session_id_external_id"
        ),
        sa.UniqueConstraint(
            "session_id", "index", name="uq_session_node_session_id_index"
        ),
    )
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.create_index(
            "ix_session_node_cache_key",
            ["cache_key"],
            unique=False,
            postgresql_where=sa.text("cache_key IS NOT NULL"),
        )

    op.create_table(
        "task",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("agent_version_id", sa.Uuid(), nullable=True),
        sa.Column("agent_id", sa.Uuid(), nullable=True),
        sa.Column("plugin_version_id", sa.Uuid(), nullable=True),
        sa.Column("payload_blob_id", sa.Uuid(), nullable=True),
        sa.Column("input_session_id", sa.Uuid(), nullable=True),
        sa.Column("result_session_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("on_failure", sa.String(length=16), nullable=False),
        sa.Column("labels", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("env", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("worker_id", sa.Uuid(), nullable=True),
        sa.Column(
            "inputs",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "result",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["agent_id"], ["agent.id"], name="fk_task_agent_id"),
        sa.ForeignKeyConstraint(
            ["agent_version_id"], ["agent_version.id"], name="fk_task_agent_version_id"
        ),
        sa.ForeignKeyConstraint(
            ["input_session_id"], ["session.id"], name="fk_task_input_session_id"
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["job.id"], name="fk_task_job_id", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["payload_blob_id"], ["blob.id"], name="fk_task_payload_blob_id"
        ),
        sa.ForeignKeyConstraint(
            ["plugin_version_id"],
            ["plugin_version.id"],
            name="fk_task_plugin_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["result_session_id"], ["session.id"], name="fk_task_result_session_id"
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"], ["worker.id"], name="fk_task_worker_id", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "job_id",
            "input_session_id",
            "plugin_version_id",
            name="uq_task_job_id_input_session_id_plugin_version_id",
        ),
    )
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_heartbeat_at_claimed_at",
            [sa.literal_column("coalesce(heartbeat_at, claimed_at)")],  # ty: ignore[invalid-argument-type]
            unique=False,
            postgresql_where=sa.text("status IN ('claimed', 'running')"),
        )
        batch_op.create_index(
            "ix_task_id",
            ["id"],
            unique=False,
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.create_index(
            "ix_task_input_session_id", ["input_session_id"], unique=False
        )
        batch_op.create_index(
            "ix_task_job_id_status", ["job_id", "status"], unique=False
        )
        batch_op.create_index(
            "ix_task_labels",
            ["labels"],
            unique=False,
            postgresql_using="gin",
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.create_index(
            "ix_task_result_session_id", ["result_session_id"], unique=False
        )

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "fk_session_task_id",
            "task",
            ["task_id"],
            ["id"],
            ondelete="SET NULL",
            use_alter=True,
        )

    op.create_table(
        "evaluation",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("evaluator_version_id", sa.Uuid(), nullable=True),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("data_type", sa.String(length=16), nullable=False),
        sa.Column("numerical_value", sa.Double(), nullable=True),
        sa.Column("string_value", sa.Text(), nullable=True),
        sa.Column("explanation", sa.Text(), nullable=True),
        sa.Column("passed", sa.Boolean(), nullable=True),
        sa.CheckConstraint(
            "(data_type IN ('float', 'bool') AND numerical_value IS NOT NULL "
            "AND string_value IS NULL) "
            "OR (data_type = 'str' AND string_value IS NOT NULL "
            "AND numerical_value IS NULL) "
            "OR (data_type = 'categorical' AND numerical_value IS NOT NULL "
            "AND string_value IS NOT NULL)",
            name="ck_evaluation_data_type_numerical_value_string_value",
        ),
        sa.ForeignKeyConstraint(
            ["evaluator_version_id"],
            ["plugin_version.id"],
            name="fk_evaluation_evaluator_version_id",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_evaluation_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_evaluation_session_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"], ["task.id"], name="fk_evaluation_task_id", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "name", name="uq_evaluation_task_id_name"),
    )
    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.create_index(
            "ix_evaluation_evaluator_version_id", ["evaluator_version_id"], unique=False
        )
        batch_op.create_index("ix_evaluation_session_id", ["session_id"], unique=False)
        batch_op.create_index(
            "uq_evaluation_session_id_name",
            ["session_id", "name"],
            unique=True,
            postgresql_where=sa.text("task_id IS NULL"),
        )

    op.create_table(
        "investigation",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("questions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name="fk_investigation_agent_id"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_investigation_owner_id"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("investigation", schema=None) as batch_op:
        batch_op.create_index("ix_investigation_agent_id", ["agent_id"], unique=False)
        batch_op.create_index("ix_investigation_owner_id", ["owner_id"], unique=False)

    op.create_table(
        "investigation_session",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "view",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigation.id"],
            name="fk_investigation_session_investigation_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_investigation_session_session_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "investigation_id",
            "session_id",
            name="uq_investigation_session_investigation_id_session_id",
        ),
    )
    with op.batch_alter_table("investigation_session", schema=None) as batch_op:
        batch_op.create_index(
            "ix_investigation_session_session_id", ["session_id"], unique=False
        )

    op.create_table(
        "annotation",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("investigation_session_id", sa.Uuid(), nullable=True),
        sa.Column("question_key", sa.String(length=255), nullable=True),
        sa.Column(
            "selector",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["investigation_session_id"],
            ["investigation_session.id"],
            name="fk_annotation_investigation_session_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name="fk_annotation_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name="fk_annotation_session_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "investigation_session_id",
            "question_key",
            name="uq_annotation_investigation_session_id_question_key",
        ),
    )
    with op.batch_alter_table("annotation", schema=None) as batch_op:
        batch_op.create_index("ix_annotation_owner_id", ["owner_id"], unique=False)
        batch_op.create_index("ix_annotation_session_id", ["session_id"], unique=False)


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("annotation", schema=None) as batch_op:
        batch_op.drop_index("ix_annotation_session_id")
        batch_op.drop_index("ix_annotation_owner_id")

    op.drop_table("annotation")
    with op.batch_alter_table("investigation_session", schema=None) as batch_op:
        batch_op.drop_index("ix_investigation_session_session_id")

    op.drop_table("investigation_session")
    with op.batch_alter_table("investigation", schema=None) as batch_op:
        batch_op.drop_index("ix_investigation_owner_id")
        batch_op.drop_index("ix_investigation_agent_id")

    op.drop_table("investigation")
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_constraint("fk_session_task_id", type_="foreignkey")

    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.drop_index(
            "uq_evaluation_session_id_name", postgresql_where=sa.text("task_id IS NULL")
        )
        batch_op.drop_index("ix_evaluation_session_id")
        batch_op.drop_index("ix_evaluation_evaluator_version_id")

    op.drop_table("evaluation")
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_index("ix_task_result_session_id")
        batch_op.drop_index(
            "ix_task_labels",
            postgresql_using="gin",
            postgresql_where=sa.text("status = 'pending'"),
        )
        batch_op.drop_index("ix_task_job_id_status")
        batch_op.drop_index("ix_task_input_session_id")
        batch_op.drop_index(
            "ix_task_id", postgresql_where=sa.text("status = 'pending'")
        )
        batch_op.drop_index(
            "ix_task_heartbeat_at_claimed_at",
            postgresql_where=sa.text("status IN ('claimed', 'running')"),
        )

    op.drop_table("task")
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.drop_index(
            "ix_session_node_cache_key",
            postgresql_where=sa.text("cache_key IS NOT NULL"),
        )

    op.drop_table("session_node")
    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.drop_index("ix_replay_experiment_run_id_status")
        batch_op.drop_index("ix_replay_baseline_session_id")

    op.drop_table("replay")
    with op.batch_alter_table("cohort_version_session", schema=None) as batch_op:
        batch_op.drop_index("ix_cohort_version_session_session_id_cohort_version_id")

    op.drop_table("cohort_version_session")
    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_index("ix_session_task_id")
        batch_op.drop_index("ix_session_status")
        batch_op.drop_index("ix_session_owner_id")
        batch_op.drop_index("ix_session_agent_version_id_id")
        batch_op.drop_index("ix_session_agent_id_id")

    op.drop_table("session")
    with op.batch_alter_table("experiment_run", schema=None) as batch_op:
        batch_op.drop_index("ix_experiment_run_cohort_version_id")
        batch_op.drop_index("ix_experiment_run_agent_version_id")

    op.drop_table("experiment_run")
    op.drop_table("agent_version_secret")
    with op.batch_alter_table("tag_link", schema=None) as batch_op:
        batch_op.drop_index("ix_tag_link_resource_type_resource_id")

    op.drop_table("tag_link")
    op.drop_table("plugin_version")
    op.drop_table("experiment")
    op.drop_table("cohort_version")
    op.drop_table("cohort")
    with op.batch_alter_table("agent_version", schema=None) as batch_op:
        batch_op.drop_index("ix_agent_version_owner_id")

    op.drop_table("agent_version")
    with op.batch_alter_table("worker", schema=None) as batch_op:
        batch_op.drop_index("ix_worker_owner_id")

    op.drop_table("worker")
    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.drop_index("ix_tag_owner_id")

    op.drop_table("tag")
    op.drop_table("replay_config")
    with op.batch_alter_table("plugin", schema=None) as batch_op:
        batch_op.drop_index("ix_plugin_owner_id")
        batch_op.drop_index("ix_plugin_kind_provider")

    op.drop_table("plugin")
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_index("ix_job_cancel_requested_at")
        batch_op.drop_index("ix_job_kind")
        batch_op.drop_index("ix_job_status")

    op.drop_table("job")
    with op.batch_alter_table("blob", schema=None) as batch_op:
        batch_op.drop_index("ix_blob_owner_id")

    op.drop_table("blob")
    with op.batch_alter_table("agent", schema=None) as batch_op:
        batch_op.drop_index("ix_agent_owner_id")

    op.drop_table("agent")
    with op.batch_alter_table("device", schema=None) as batch_op:
        batch_op.drop_index("ix_device_expires")
        batch_op.drop_index("ix_device_account_id")

    op.drop_table("device")
    with op.batch_alter_table("secret", schema=None) as batch_op:
        batch_op.drop_index("ix_secret_owner_id")

    op.drop_table("secret")
    with op.batch_alter_table("api_key", schema=None) as batch_op:
        batch_op.drop_index("ix_api_key_owner_id")

    op.drop_table("api_key")
    with op.batch_alter_table("account", schema=None) as batch_op:
        batch_op.drop_index("ix_account_external_id")

    op.drop_table("account")
