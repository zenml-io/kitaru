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
"""Evaluation provenance Alembic revision.

Revision ID: 010_evaluation_provenance
Revises: 009_agent_version_run_hooks
Create Date: 2026-08-31

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "010_evaluation_provenance"
down_revision = "009_agent_version_run_hooks"
branch_labels = None
depends_on = None

BASELINE_EVALUATION_MODE_LENGTH = 16

_BOOL_TO_MODE_SQL = (
    "CASE WHEN baseline_evaluation_mode THEN 'if_missing' ELSE 'none' END"
)


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    for table in ("replay", "experiment_run"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.alter_column(
                "evaluate_baselines", new_column_name="baseline_evaluation_mode"
            )
            batch_op.alter_column(
                "baseline_evaluation_mode",
                existing_type=sa.Boolean(),
                type_=sa.String(BASELINE_EVALUATION_MODE_LENGTH),
                nullable=False,
                postgresql_using=_BOOL_TO_MODE_SQL,
            )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # A mode of "force" has no boolean representation, and later revisions
    # build on this column, so downgrading below it is not supported.
    raise RuntimeError(
        "010_evaluation_provenance cannot be downgraded. The baseline "
        "evaluation mode cannot be represented as a boolean once a value "
        "other than none or if_missing exists."
    )
