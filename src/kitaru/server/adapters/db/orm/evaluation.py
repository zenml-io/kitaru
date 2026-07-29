"""Evaluation ORM table."""

import uuid

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.evaluation import (
    Evaluation,
    EvaluationDataType,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH

EVALUATION_OWNER_FOREIGN_KEY = foreign_key_name("evaluation", ["owner_id"])
EVALUATION_VERSION_FOREIGN_KEY = foreign_key_name(
    "evaluation", ["evaluator_version_id"]
)
EVALUATION_SESSION_FOREIGN_KEY = foreign_key_name("evaluation", ["session_id"])
EVALUATION_TASK_FOREIGN_KEY = foreign_key_name("evaluation", ["task_id"])
EVALUATION_TASK_NAME_UNIQUE_CONSTRAINT = unique_constraint_name(
    "evaluation", ["task_id", "name"]
)
EVALUATION_MANUAL_UNIQUE_INDEX = index_name(
    "evaluation", ["session_id", "name", "manual"]
)
EVALUATION_SESSION_INDEX = index_name("evaluation", ["session_id"])
EVALUATION_VERSION_INDEX = index_name("evaluation", ["evaluator_version_id"])


class EvaluationORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Stored evaluation row."""

    __tablename__ = "evaluation"
    __table_args__ = (
        UniqueConstraint(
            "task_id", "name", name=EVALUATION_TASK_NAME_UNIQUE_CONSTRAINT
        ),
        Index(EVALUATION_SESSION_INDEX, "session_id"),
        Index(EVALUATION_VERSION_INDEX, "evaluator_version_id"),
        Index(
            EVALUATION_MANUAL_UNIQUE_INDEX,
            "session_id",
            "name",
            unique=True,
            postgresql_where=text("task_id IS NULL"),
        ),
        CheckConstraint(
            "(data_type IN ('float', 'bool') AND numerical_value IS NOT NULL "
            "AND string_value IS NULL) OR "
            "(data_type = 'str' AND numerical_value IS NULL "
            "AND string_value IS NOT NULL) OR "
            "(data_type = 'categorical' AND numerical_value IS NOT NULL "
            "AND string_value IS NOT NULL)",
            name="ck_evaluation_value_shape",
        ),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=EVALUATION_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["evaluator_version_id"],
            ["plugin_version.id"],
            name=EVALUATION_VERSION_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=EVALUATION_SESSION_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["task_id"],
            ["task.id"],
            name=EVALUATION_TASK_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    owner_id: Mapped[uuid.UUID]
    evaluator_version_id: Mapped[uuid.UUID | None]
    session_id: Mapped[uuid.UUID]
    task_id: Mapped[uuid.UUID | None]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    data_type: Mapped[str] = mapped_column(String(32))
    numerical_value: Mapped[float | None]
    string_value: Mapped[str | None]
    explanation: Mapped[str | None]

    @classmethod
    def from_domain(cls, evaluation: Evaluation) -> "EvaluationORM":
        """Build a row from an evaluation."""
        numerical = evaluation.score
        if isinstance(numerical, bool):
            numerical = 1.0 if numerical else 0.0
        return cls(
            id=evaluation.id,
            owner_id=evaluation.owner_id,
            evaluator_version_id=evaluation.evaluator_version_id,
            session_id=evaluation.session_id,
            task_id=evaluation.task_id,
            name=evaluation.name,
            data_type=evaluation.data_type.value,
            numerical_value=numerical,
            string_value=evaluation.value,
            explanation=evaluation.explanation,
        )

    def to_domain(self) -> Evaluation:
        """Build an evaluation from this row."""
        data_type = EvaluationDataType(self.data_type)
        score: float | bool | None = self.numerical_value
        if data_type is EvaluationDataType.BOOL and score is not None:
            score = bool(score)
        return Evaluation(
            id=self.id,
            owner_id=self.owner_id,
            evaluator_version_id=self.evaluator_version_id,
            session_id=self.session_id,
            task_id=self.task_id,
            name=self.name,
            data_type=data_type,
            score=score,
            value=self.string_value,
            explanation=self.explanation,
            created=self.created,
            updated=self.updated,
        )
