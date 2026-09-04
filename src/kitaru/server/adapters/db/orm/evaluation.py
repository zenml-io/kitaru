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
"""Evaluation ORM table."""

import uuid
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Double,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    check_constraint_name,
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.names import MAX_NAME_LENGTH

DATA_TYPE_LENGTH = 16

EVALUATION_OWNER_ID_FOREIGN_KEY = foreign_key_name("evaluation", ["owner_id"])
EVALUATION_SESSION_ID_FOREIGN_KEY = foreign_key_name("evaluation", ["session_id"])
EVALUATION_TASK_ID_FOREIGN_KEY = foreign_key_name("evaluation", ["task_id"])
EVALUATION_TASK_ID_NAME_UNIQUE_CONSTRAINT = unique_constraint_name(
    "evaluation", ["task_id", "name"]
)
# A partial unique index, not a plain unique constraint, since Postgres only
# supports a WHERE predicate on an index. It is the manual create's conflict
# target, and the discriminator for a manual row is evaluator_version_id
# being null rather than task_id, since an evaluator-born row keeps its
# evaluator_version_id even after its producing task is pruned.
EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX = unique_constraint_name(
    "evaluation", ["session_id", "name"]
)
EVALUATION_SESSION_ID_INDEX = index_name("evaluation", ["session_id"])
EVALUATION_EVALUATOR_VERSION_ID_INDEX = index_name(
    "evaluation", ["evaluator_version_id"]
)
# Backs the identity lookup that finds a pre-existing row to adopt for a
# baseline evaluation run under IF_MISSING.
EVALUATION_SESSION_ID_EVALUATOR_VERSION_ID_PARAMS_HASH_INDEX = index_name(
    "evaluation", ["session_id", "evaluator_version_id", "params_hash"]
)
EVALUATION_DATA_TYPE_CHECK_CONSTRAINT = check_constraint_name(
    "evaluation", ["data_type", "numerical_value", "string_value"]
)
_EVALUATION_DATA_TYPE_CHECK_SQL = (
    "(data_type IN ('float', 'bool') AND numerical_value IS NOT NULL "
    "AND string_value IS NULL) "
    "OR (data_type = 'str' AND string_value IS NOT NULL AND numerical_value IS NULL) "
    "OR (data_type = 'categorical' AND numerical_value IS NOT NULL "
    "AND string_value IS NOT NULL)"
)


def _split_value_columns(
    score: float | bool | None, value: str | None
) -> tuple[float | None, str | None]:
    """Split a domain score and value into their numerical and string columns.

    The domain entity already guarantees ``value`` is null unless the data
    type is categorical, so passing it straight through covers every data
    type without branching on it here.

    Args:
        score: Domain score, bool or float.
        value: Domain value.

    Returns:
        Numerical value (bool scores as 0/1) and string value columns.
    """
    if score is None:
        return None, value
    numerical_value = float(score) if not isinstance(score, bool) else float(int(score))
    return numerical_value, value


def _score_from_row(
    data_type: EvaluationDataType, numerical_value: float | None
) -> float | bool | None:
    """Build a domain score from the stored numerical value column.

    Args:
        data_type: Data type of the evaluation.
        numerical_value: Stored numerical value column.

    Returns:
        Bool score for bool rows, float otherwise, ``None`` when unset.
    """
    if numerical_value is None:
        return None
    if data_type == EvaluationDataType.BOOL:
        return numerical_value != 0
    return numerical_value


class EvaluationORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Evaluation table."""

    __tablename__ = "evaluation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=EVALUATION_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=EVALUATION_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["task_id"],
            ["task.id"],
            name=EVALUATION_TASK_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        UniqueConstraint(
            "task_id", "name", name=EVALUATION_TASK_ID_NAME_UNIQUE_CONSTRAINT
        ),
        Index(
            EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX,
            "session_id",
            "name",
            unique=True,
            postgresql_where=text("evaluator_version_id IS NULL"),
        ),
        Index(EVALUATION_SESSION_ID_INDEX, "session_id"),
        Index(EVALUATION_EVALUATOR_VERSION_ID_INDEX, "evaluator_version_id"),
        Index(
            EVALUATION_SESSION_ID_EVALUATOR_VERSION_ID_PARAMS_HASH_INDEX,
            "session_id",
            "evaluator_version_id",
            "params_hash",
        ),
        CheckConstraint(
            _EVALUATION_DATA_TYPE_CHECK_SQL, name=EVALUATION_DATA_TYPE_CHECK_CONSTRAINT
        ),
    )

    owner_id: Mapped[uuid.UUID]
    # No foreign key, an evaluator-born row keeps this id forever, even after
    # the plugin version it references is deleted.
    evaluator_version_id: Mapped[uuid.UUID | None]
    session_id: Mapped[uuid.UUID]
    task_id: Mapped[uuid.UUID | None]
    # No foreign key, identifies the evaluator call that produced this row
    # alongside its siblings, and outlives the task the same way
    # evaluator_version_id does.
    invocation_id: Mapped[uuid.UUID | None]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    data_type: Mapped[str] = mapped_column(String(DATA_TYPE_LENGTH))
    numerical_value: Mapped[float | None] = mapped_column(Double)
    string_value: Mapped[str | None] = mapped_column(Text)
    explanation: Mapped[str | None] = mapped_column(Text)
    passed: Mapped[bool | None]
    min_score: Mapped[float | None] = mapped_column(Double)
    max_score: Mapped[float | None] = mapped_column(Double)
    target_score: Mapped[float | None] = mapped_column(Double)
    evaluator_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    params_hash: Mapped[str | None] = mapped_column(String(64))

    @classmethod
    def column_values(cls, evaluation: Evaluation) -> dict[str, object]:
        """Map a domain evaluation to its column values, id included.

        Args:
            evaluation: Evaluation to store.

        Returns:
            Column values keyed by column name.
        """
        numerical_value, string_value = _split_value_columns(
            evaluation.score, evaluation.value
        )
        return {
            "id": evaluation.id,
            "owner_id": evaluation.owner_id,
            "evaluator_version_id": evaluation.evaluator_version_id,
            "session_id": evaluation.session_id,
            "task_id": evaluation.task_id,
            "invocation_id": evaluation.invocation_id,
            "name": evaluation.name,
            "data_type": evaluation.data_type.value,
            "numerical_value": numerical_value,
            "string_value": string_value,
            "explanation": evaluation.explanation,
            "passed": evaluation.passed,
            "min_score": evaluation.min_score,
            "max_score": evaluation.max_score,
            "target_score": evaluation.target_score,
            "evaluator_params": evaluation.evaluator_params,
            "params_hash": evaluation.params_hash,
        }

    def to_domain(self) -> Evaluation:
        """Build a domain evaluation from this row.

        Returns:
            Evaluation with timestamps set.
        """
        data_type = EvaluationDataType(self.data_type)
        return Evaluation(
            id=self.id,
            owner_id=self.owner_id,
            evaluator_version_id=self.evaluator_version_id,
            session_id=self.session_id,
            task_id=self.task_id,
            invocation_id=self.invocation_id,
            name=self.name,
            data_type=data_type,
            score=_score_from_row(data_type, self.numerical_value),
            value=self.string_value,
            explanation=self.explanation,
            passed=self.passed,
            min_score=self.min_score,
            max_score=self.max_score,
            target_score=self.target_score,
            evaluator_params=self.evaluator_params,
            params_hash=self.params_hash,
            created=self.created,
            updated=self.updated,
        )
