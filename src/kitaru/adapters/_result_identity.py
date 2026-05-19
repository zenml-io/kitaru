"""Helpers for rebuilding adapter result objects with canonical class identity."""

from typing import Any, TypeVar

from pydantic import BaseModel

ResultModelT = TypeVar("ResultModelT", bound=BaseModel)


def canonicalize_result_model(
    value: Any,
    model_type: type[ResultModelT],
) -> ResultModelT:
    """Return ``value`` as an instance of the adapter's canonical model class.

    Synthetic checkpoint materialization can return a Pydantic model instance
    with the right fields but the wrong module/class identity, for example
    ``src.kitaru...OpenAIRunResult`` instead of
    ``kitaru...OpenAIRunResult``. Pydantic correctly refuses to validate that
    foreign model instance directly, so we first turn Pydantic-like objects into
    plain Python data and then validate that payload into the promised class.
    """
    if isinstance(value, model_type):
        return value

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_type.model_validate(model_dump(mode="python"))
    return model_type.model_validate(value)
