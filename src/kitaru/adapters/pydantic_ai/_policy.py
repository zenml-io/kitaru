from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

CaptureMode = Literal['full', 'metadata']


@dataclass(frozen=True)
class CapturePolicy:
    """Controls what the Kitaru adapter persists and correlates per run.

    Pass `None` for `tool_capture` or any entry in `tool_capture_overrides` to
    bypass tracking (no events, no artifacts, no wait correlation) for that scope.
    """

    emit_child_events: bool = True
    save_prompts: bool = True
    save_responses: bool = True
    save_stream_transcripts: bool = True
    tool_capture: CaptureMode | None = 'full'
    tool_capture_overrides: Mapping[str, CaptureMode | None] = field(default_factory=dict)
    correlate_otel_spans: bool = True

    def capture_mode_for_tool(self, name: str) -> CaptureMode | None:
        if not self.emit_child_events:
            return None
        return self.tool_capture_overrides.get(name, self.tool_capture)
