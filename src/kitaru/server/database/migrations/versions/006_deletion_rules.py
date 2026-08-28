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
"""Deletion rules across the schema, from foreign keys to agent soft delete.

Revision ID: 006_deletion_rules
Revises: 005_evaluator_agent_scoping
Create Date: 2026-08-24

"""

import sqlalchemy as sa
from alembic import op

from kitaru.server.adapters.db.orm.agent import AGENT_NAME_UNIQUE_CONSTRAINT
from kitaru.server.adapters.db.orm.agent_version import (
    AGENT_VERSION_AGENT_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.cohort import (
    COHORT_AGENT_ID_FOREIGN_KEY,
    COHORT_AGENT_ID_NAME_UNIQUE_CONSTRAINT,
    COHORT_OWNER_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.cohort_version import (
    COHORT_VERSION_OWNER_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.evaluation import (
    EVALUATION_EVALUATOR_VERSION_ID_FOREIGN_KEY,
    EVALUATION_TASK_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.experiment import (
    EXPERIMENT_AGENT_ID_FOREIGN_KEY,
    EXPERIMENT_AGENT_ID_NAME_UNIQUE_CONSTRAINT,
)
from kitaru.server.adapters.db.orm.experiment_run import (
    EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.investigation import (
    INVESTIGATION_AGENT_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.investigation_session import (
    INVESTIGATION_SESSION_SESSION_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.orm_utils import index_name, unique_constraint_name
from kitaru.server.adapters.db.orm.replay import REPLAY_JOB_ID_FOREIGN_KEY
from kitaru.server.adapters.db.orm.session import (
    SESSION_AGENT_ID_FOREIGN_KEY,
    SESSION_AGENT_VERSION_ID_FOREIGN_KEY,
    SESSION_IMPORTED_FROM_EXTERNAL_ID_AGENT_ID_UNIQUE_CONSTRAINT,
)
from kitaru.server.adapters.db.orm.tag import (
    TAG_LINK_AGENT_VERSION_ID_FOREIGN_KEY,
    TAG_LINK_AGENT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_COHORT_ID_FOREIGN_KEY,
    TAG_LINK_COHORT_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_COHORT_VERSION_ID_FOREIGN_KEY,
    TAG_LINK_COHORT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_EXPERIMENT_ID_FOREIGN_KEY,
    TAG_LINK_EXPERIMENT_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_EXPERIMENT_RUN_ID_FOREIGN_KEY,
    TAG_LINK_EXPERIMENT_RUN_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_RESOURCE_CHECK_CONSTRAINT,
    TAG_LINK_SESSION_ID_FOREIGN_KEY,
    TAG_LINK_SESSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
    TAG_LINK_TAG_ID_INDEX,
)

# revision identifiers, used by Alembic.
revision = "006_deletion_rules"
down_revision = "005_evaluator_agent_scoping"
branch_labels = None
depends_on = None

# tag_link's old polymorphic columns are gone from the ORM, so their names are
# recomputed the same way the ORM constant used to build them.
OLD_TAG_LINK_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["tag_id", "resource_type", "resource_id"]
)
OLD_TAG_LINK_RESOURCE_INDEX = index_name("tag_link", ["resource_type", "resource_id"])

# The old unique constraints replaced below are gone from the ORM, so their
# names are recomputed the same way the ORM constants used to build them.
OLD_COHORT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("cohort", ["name"])
OLD_EXPERIMENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("experiment", ["name"])
OLD_SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session", ["imported_from", "external_id"]
)

TAG_LINK_RESOURCE_CHECK_SQL = (
    "num_nonnulls(session_id, cohort_id, cohort_version_id, "
    "agent_version_id, experiment_id, experiment_run_id) = 1"
)


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("agent", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True)
        )
        # The name constraint becomes a partial unique index under the same
        # name, so soft-deleted agents release their name for reuse.
        batch_op.drop_constraint(AGENT_NAME_UNIQUE_CONSTRAINT, type_="unique")
        batch_op.create_index(
            AGENT_NAME_UNIQUE_CONSTRAINT,
            ["name"],
            unique=True,
            postgresql_where=sa.text("deleted_at IS NULL"),
        )

    with op.batch_alter_table("agent_version", schema=None) as batch_op:
        batch_op.drop_constraint(AGENT_VERSION_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            AGENT_VERSION_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("cohort", schema=None) as batch_op:
        batch_op.create_foreign_key(
            COHORT_OWNER_ID_FOREIGN_KEY, "account", ["owner_id"], ["id"]
        )
        batch_op.drop_constraint(COHORT_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            COHORT_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.drop_constraint(OLD_COHORT_NAME_UNIQUE_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(
            COHORT_AGENT_ID_NAME_UNIQUE_CONSTRAINT, ["agent_id", "name"]
        )

    with op.batch_alter_table("cohort_version", schema=None) as batch_op:
        batch_op.create_foreign_key(
            COHORT_VERSION_OWNER_ID_FOREIGN_KEY, "account", ["owner_id"], ["id"]
        )

    with op.batch_alter_table("experiment", schema=None) as batch_op:
        batch_op.drop_constraint(EXPERIMENT_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            EXPERIMENT_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.drop_constraint(OLD_EXPERIMENT_NAME_UNIQUE_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(
            EXPERIMENT_AGENT_ID_NAME_UNIQUE_CONSTRAINT, ["agent_id", "name"]
        )

    with op.batch_alter_table("investigation", schema=None) as batch_op:
        batch_op.drop_constraint(INVESTIGATION_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            INVESTIGATION_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("session", schema=None) as batch_op:
        batch_op.drop_constraint(SESSION_AGENT_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            SESSION_AGENT_ID_FOREIGN_KEY,
            "agent",
            ["agent_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.drop_constraint(
            SESSION_AGENT_VERSION_ID_FOREIGN_KEY, type_="foreignkey"
        )
        batch_op.create_foreign_key(
            SESSION_AGENT_VERSION_ID_FOREIGN_KEY,
            "agent_version",
            ["agent_version_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.drop_constraint(
            OLD_SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT, type_="unique"
        )
        batch_op.create_unique_constraint(
            SESSION_IMPORTED_FROM_EXTERNAL_ID_AGENT_ID_UNIQUE_CONSTRAINT,
            ["imported_from", "external_id", "agent_id"],
        )

    with op.batch_alter_table("experiment_run", schema=None) as batch_op:
        batch_op.drop_constraint(
            EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY, type_="foreignkey"
        )
        batch_op.create_foreign_key(
            EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
            "experiment",
            ["experiment_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("investigation_session", schema=None) as batch_op:
        batch_op.drop_constraint(
            INVESTIGATION_SESSION_SESSION_ID_FOREIGN_KEY, type_="foreignkey"
        )
        batch_op.create_foreign_key(
            INVESTIGATION_SESSION_SESSION_ID_FOREIGN_KEY,
            "session",
            ["session_id"],
            ["id"],
        )

    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.drop_constraint(
            EVALUATION_EVALUATOR_VERSION_ID_FOREIGN_KEY, type_="foreignkey"
        )
        batch_op.create_foreign_key(
            EVALUATION_EVALUATOR_VERSION_ID_FOREIGN_KEY,
            "plugin_version",
            ["evaluator_version_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.drop_constraint(EVALUATION_TASK_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            EVALUATION_TASK_ID_FOREIGN_KEY,
            "task",
            ["task_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # A task names its inputs by id without constraining them, so deleting an
    # input neither deletes the task nor is blocked by it.
    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_constraint("fk_task_agent_id", type_="foreignkey")
        batch_op.drop_constraint("fk_task_agent_version_id", type_="foreignkey")
        batch_op.drop_constraint("fk_task_plugin_version_id", type_="foreignkey")
        batch_op.drop_constraint("fk_task_input_session_id", type_="foreignkey")
        batch_op.drop_constraint("fk_task_payload_blob_id", type_="foreignkey")

    with op.batch_alter_table("replay", schema=None) as batch_op:
        batch_op.alter_column("job_id", existing_type=sa.Uuid(), nullable=True)
        batch_op.drop_constraint(REPLAY_JOB_ID_FOREIGN_KEY, type_="foreignkey")
        batch_op.create_foreign_key(
            REPLAY_JOB_ID_FOREIGN_KEY,
            "job",
            ["job_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # tag_link moves from a polymorphic resource_type/resource_id pair to one
    # typed, foreign-keyed column per TagResourceType. Backfill the typed
    # columns from the released pair, then drop the rows the new foreign
    # keys cannot hold: links with an unknown resource type and links whose
    # resource is gone, since the old schema never enforced resource ids.
    with op.batch_alter_table("tag_link", schema=None) as batch_op:
        batch_op.drop_constraint(OLD_TAG_LINK_UNIQUE_CONSTRAINT, type_="unique")
        batch_op.drop_index(OLD_TAG_LINK_RESOURCE_INDEX)
        batch_op.add_column(sa.Column("session_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("cohort_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("cohort_version_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("agent_version_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("experiment_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("experiment_run_id", sa.Uuid(), nullable=True))
    for resource_type, column in (
        ("session", "session_id"),
        ("cohort", "cohort_id"),
        ("cohort_version", "cohort_version_id"),
        ("agent_version", "agent_version_id"),
        ("experiment", "experiment_id"),
        ("experiment_run", "experiment_run_id"),
    ):
        op.execute(
            f"UPDATE tag_link SET {column} = resource_id "
            f"WHERE resource_type = '{resource_type}'"
        )
        op.execute(
            f"DELETE FROM tag_link WHERE {column} IS NOT NULL AND NOT EXISTS "
            f"(SELECT 1 FROM {resource_type} "
            f"WHERE {resource_type}.id = tag_link.{column})"
        )
    op.execute(
        "DELETE FROM tag_link WHERE session_id IS NULL AND cohort_id IS NULL "
        "AND cohort_version_id IS NULL AND agent_version_id IS NULL "
        "AND experiment_id IS NULL AND experiment_run_id IS NULL"
    )
    with op.batch_alter_table("tag_link", schema=None) as batch_op:
        batch_op.drop_column("resource_type")
        batch_op.drop_column("resource_id")
        batch_op.create_foreign_key(
            TAG_LINK_SESSION_ID_FOREIGN_KEY,
            "session",
            ["session_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            TAG_LINK_COHORT_ID_FOREIGN_KEY,
            "cohort",
            ["cohort_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            TAG_LINK_COHORT_VERSION_ID_FOREIGN_KEY,
            "cohort_version",
            ["cohort_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            TAG_LINK_AGENT_VERSION_ID_FOREIGN_KEY,
            "agent_version",
            ["agent_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            TAG_LINK_EXPERIMENT_ID_FOREIGN_KEY,
            "experiment",
            ["experiment_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            TAG_LINK_EXPERIMENT_RUN_ID_FOREIGN_KEY,
            "experiment_run",
            ["experiment_run_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint(
            TAG_LINK_SESSION_ID_TAG_ID_UNIQUE_CONSTRAINT, ["session_id", "tag_id"]
        )
        batch_op.create_unique_constraint(
            TAG_LINK_COHORT_ID_TAG_ID_UNIQUE_CONSTRAINT, ["cohort_id", "tag_id"]
        )
        batch_op.create_unique_constraint(
            TAG_LINK_COHORT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
            ["cohort_version_id", "tag_id"],
        )
        batch_op.create_unique_constraint(
            TAG_LINK_AGENT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
            ["agent_version_id", "tag_id"],
        )
        batch_op.create_unique_constraint(
            TAG_LINK_EXPERIMENT_ID_TAG_ID_UNIQUE_CONSTRAINT,
            ["experiment_id", "tag_id"],
        )
        batch_op.create_unique_constraint(
            TAG_LINK_EXPERIMENT_RUN_ID_TAG_ID_UNIQUE_CONSTRAINT,
            ["experiment_run_id", "tag_id"],
        )
        batch_op.create_check_constraint(
            TAG_LINK_RESOURCE_CHECK_CONSTRAINT, TAG_LINK_RESOURCE_CHECK_SQL
        )
        batch_op.create_index(TAG_LINK_TAG_ID_INDEX, ["tag_id"])


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # An agent name reused after a soft delete exists on two rows, which the
    # restored plain unique constraint cannot represent.
    raise RuntimeError(
        "006_deletion_rules cannot be downgraded. The schema before it "
        "cannot hold an agent name reused after a soft delete."
    )
