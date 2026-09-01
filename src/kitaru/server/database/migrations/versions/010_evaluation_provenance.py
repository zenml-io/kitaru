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

import json
import uuid
from hashlib import sha256

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from kitaru.server.adapters.db.orm.evaluation import (
    EVALUATION_SESSION_ID_EVALUATOR_VERSION_ID_PARAMS_HASH_INDEX,
    EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX,
)
from kitaru.server.adapters.db.orm.replay_evaluation import (
    REPLAY_EVALUATION_EVALUATION_ID_FOREIGN_KEY,
    REPLAY_EVALUATION_REPLAY_ID_FOREIGN_KEY,
)

# revision identifiers, used by Alembic.
revision = "010_evaluation_provenance"
down_revision = "009_agent_version_run_hooks"
branch_labels = None
depends_on = None

BASELINE_EVALUATION_MODE_LENGTH = 16
PARAMS_HASH_LENGTH = 64

_BOOL_TO_MODE_SQL = (
    "CASE WHEN baseline_evaluation_mode THEN 'if_missing' ELSE 'none' END"
)


def _hash_params(params: dict) -> str:
    """Hash a params dict into a stable hex digest over its canonical JSON.

    Mirrors ``kitaru.server.utils.hash_params``, inlined since migrations do
    not import application code.

    Args:
        params: Params to hash.

    Returns:
        Hex-encoded sha256 digest.
    """
    canonical = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode()).hexdigest()


def _backfill_evaluator_params() -> None:
    """Set evaluator_params and params_hash from each row's producing task."""
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT evaluation.id, task.inputs FROM evaluation "
            "JOIN task ON task.id = evaluation.task_id "
            "WHERE evaluation.task_id IS NOT NULL AND task.kind = 'evaluator'"
        )
    ).all()
    if not rows:
        return
    values = []
    for evaluation_id, inputs in rows:
        params = inputs if inputs is not None else {}
        values.append(
            {
                "params": json.dumps(params),
                "params_hash": _hash_params(params),
                "id": evaluation_id,
            }
        )
    connection.execute(
        sa.text(
            "UPDATE evaluation SET evaluator_params = CAST(:params AS jsonb), "
            "params_hash = :params_hash WHERE id = :id"
        ),
        values,
    )


def _backfill_produced_links() -> None:
    """Link each task-born evaluation to the replay of its producing job."""
    connection = op.get_bind()
    connection.execute(
        sa.text(
            "INSERT INTO replay_evaluation "
            "(replay_id, evaluation_id, created, updated) "
            "SELECT replay.id, evaluation.id, now(), now() FROM evaluation "
            "JOIN task ON task.id = evaluation.task_id "
            "JOIN replay ON replay.job_id = task.job_id "
            "WHERE evaluation.task_id IS NOT NULL "
            "ON CONFLICT DO NOTHING"
        )
    )


def _backfill_adoption_links() -> None:
    """Link IF_MISSING replays to the baseline rows their run would adopt."""
    connection = op.get_bind()
    replays = connection.execute(
        sa.text(
            "SELECT replay.id, replay.baseline_session_id, replay_config.evaluators "
            "FROM replay "
            "JOIN replay_config ON replay_config.id = replay.replay_config_id "
            "WHERE replay.baseline_evaluation_mode = 'if_missing'"
        )
    ).all()
    if not replays:
        return
    latest_by_identity: dict[tuple[uuid.UUID, uuid.UUID, str], uuid.UUID] = {}
    matches = connection.execute(
        sa.text(
            "SELECT DISTINCT ON (session_id, evaluator_version_id, params_hash) "
            "session_id, evaluator_version_id, params_hash, id FROM evaluation "
            "WHERE session_id IN :session_ids "
            "AND evaluator_version_id IS NOT NULL AND params_hash IS NOT NULL "
            "ORDER BY session_id, evaluator_version_id, params_hash, "
            "created DESC, id DESC"
        ).bindparams(sa.bindparam("session_ids", expanding=True)),
        {"session_ids": [row.baseline_session_id for row in replays]},
    ).all()
    for session_id, evaluator_version_id, params_hash, evaluation_id in matches:
        latest_by_identity[(session_id, evaluator_version_id, params_hash)] = (
            evaluation_id
        )
    links = []
    for replay_id, baseline_session_id, evaluators in replays:
        for evaluator in evaluators:
            identity = (
                baseline_session_id,
                uuid.UUID(evaluator["evaluator_version_id"]),
                _hash_params(evaluator["params"]),
            )
            evaluation_id = latest_by_identity.get(identity)
            if evaluation_id is not None:
                links.append({"replay_id": replay_id, "evaluation_id": evaluation_id})
    if not links:
        return
    connection.execute(
        sa.text(
            "INSERT INTO replay_evaluation "
            "(replay_id, evaluation_id, created, updated) "
            "VALUES (:replay_id, :evaluation_id, now(), now()) "
            "ON CONFLICT DO NOTHING"
        ),
        links,
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

    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "evaluator_params",
                postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "params_hash", sa.String(length=PARAMS_HASH_LENGTH), nullable=True
            )
        )
        batch_op.add_column(sa.Column("min_score", sa.Double(), nullable=True))
        batch_op.add_column(sa.Column("max_score", sa.Double(), nullable=True))
        batch_op.add_column(sa.Column("target_score", sa.Double(), nullable=True))

    op.create_table(
        "replay_evaluation",
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("replay_id", sa.Uuid(), nullable=False),
        sa.Column("evaluation_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["replay_id"],
            ["replay.id"],
            name=REPLAY_EVALUATION_REPLAY_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["evaluation_id"],
            ["evaluation.id"],
            name=REPLAY_EVALUATION_EVALUATION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("replay_id", "evaluation_id"),
    )

    # A row left over from the SET-NULL-turns-evaluator-row-manual bug this
    # revision fixes going forward. Its evaluator_version_id is already gone,
    # so it now pattern-matches the new manual discriminator and could
    # collide with a real manual name.
    op.execute(
        "DELETE FROM evaluation WHERE evaluator_version_id IS NULL "
        "AND task_id IS NOT NULL"
    )

    with op.batch_alter_table("evaluation", schema=None) as batch_op:
        batch_op.drop_index(
            EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX,
            postgresql_where=sa.text("task_id IS NULL"),
        )
        batch_op.create_index(
            EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX,
            ["session_id", "name"],
            unique=True,
            postgresql_where=sa.text("evaluator_version_id IS NULL"),
        )
        batch_op.drop_constraint(
            "fk_evaluation_evaluator_version_id", type_="foreignkey"
        )
        batch_op.create_index(
            EVALUATION_SESSION_ID_EVALUATOR_VERSION_ID_PARAMS_HASH_INDEX,
            ["session_id", "evaluator_version_id", "params_hash"],
        )

    _backfill_evaluator_params()
    _backfill_produced_links()
    _backfill_adoption_links()


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    # A mode of "force" has no boolean representation, the evaluator version
    # foreign key is gone, and later revisions build on this column, so
    # downgrading below it is not supported.
    raise RuntimeError(
        "010_evaluation_provenance cannot be downgraded. The baseline "
        "evaluation mode cannot be represented as a boolean once a value "
        "other than none or if_missing exists, and the dropped "
        "evaluator_version_id foreign key cannot be restored without data "
        "loss."
    )
