from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from kitaru.checkpoint import _KitaruOutputArtifact


def checkpoint_output_handle() -> _KitaruOutputArtifact:
    return _KitaruOutputArtifact.model_construct(
        id=uuid4(),
        step_name="render_prompt",
        output_name="output",
    )


def assert_checkpoint_handle_error(
    exc_info: pytest.ExceptionInfo[ValidationError],
    *,
    field_name: str,
) -> None:
    message = str(exc_info.value)
    assert field_name in message
    assert "Kitaru checkpoint output handle" in message
    assert "render_prompt.output" in message
    assert ".load()" in message
    assert "downstream `@checkpoint`" in message
    assert "ArtifactVersionResponse" not in message
    assert "ZenML" not in message
