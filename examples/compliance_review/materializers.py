"""Custom materializers for the compliance-review Claude boundary.

The Claude Agent SDK stores resumable session state as a local JSONL transcript.
This materializer keeps that file with the ZenML artifact so a later checkpoint
can load the result on a fresh machine and still resume the same Claude session.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar

from zenml.io import fileio
from zenml.materializers.materializer_registry import materializer_registry
from zenml.materializers.pydantic_materializer import PydanticMaterializer

from examples.compliance_review.claude_agent import ClaudeAgentResult

TRANSCRIPT_ARTIFACT_FILENAME = "claude_transcript.jsonl"


class ClaudeAgentResultMaterializer(PydanticMaterializer):
    """Materializer that bundles and restores Claude session transcripts."""

    ASSOCIATED_TYPES: ClassVar[tuple[type[Any], ...]] = (ClaudeAgentResult,)

    def save(self, data: Any) -> None:
        """Save the result metadata and its local Claude transcript JSONL."""
        if not isinstance(data, ClaudeAgentResult):
            raise TypeError(
                f"Expected ClaudeAgentResult, got {type(data).__name__}."
            )
        super().save(data)
        source_path = Path(data.transcript_path).expanduser()
        if not source_path.exists():
            raise FileNotFoundError(
                "Claude transcript does not exist, so the session cannot be "
                f"made durable: {source_path}"
            )

        fileio.copy(
            str(source_path),
            self._artifact_transcript_path,
            overwrite=True,
        )

    def load(self, data_type: type[Any]) -> ClaudeAgentResult:
        """Load the result and restore the transcript to Claude's local path."""
        result = super().load(data_type)
        if not isinstance(result, ClaudeAgentResult):
            raise TypeError(
                "Expected ClaudeAgentResult from PydanticMaterializer, got "
                f"{type(result).__name__}."
            )
        self._restore_transcript(result)
        return result

    @property
    def _artifact_transcript_path(self) -> str:
        """Return the transcript path inside the ZenML artifact directory."""
        return os.path.join(self.uri, TRANSCRIPT_ARTIFACT_FILENAME)

    def _restore_transcript(self, result: ClaudeAgentResult) -> None:
        """Copy the stored transcript back to the path Claude expects."""
        if not fileio.exists(self._artifact_transcript_path):
            raise FileNotFoundError(
                "Claude transcript was not found in the materialized artifact: "
                f"{self._artifact_transcript_path}"
            )

        destination_path = Path(result.transcript_path).expanduser()
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        fileio.copy(
            self._artifact_transcript_path,
            str(destination_path),
            overwrite=True,
        )


# Register globally by type before any compliance-review checkpoint executes.
materializer_registry.register_and_overwrite_type(
    ClaudeAgentResult,
    ClaudeAgentResultMaterializer,
)
