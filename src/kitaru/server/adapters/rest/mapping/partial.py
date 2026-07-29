"""Helpers preserving omitted fields in partial update commands."""

from typing import TypeVar

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


def to_partial(model_type: type[ModelT], body: BaseModel) -> ModelT:
    """Build a command with exactly the request fields supplied by the caller."""
    values = {name: getattr(body, name) for name in body.model_fields_set}
    return model_type(**values)
