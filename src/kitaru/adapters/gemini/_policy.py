"""Capture policy for the Gemini Interactions adapter."""

from pydantic import BaseModel, ConfigDict


class GeminiInteractionCapturePolicy(BaseModel):
    """Controls what the Gemini adapter captures for one interaction turn."""

    model_config = ConfigDict(extra="forbid")

    emit_events: bool = True
    save_input: bool = True
    save_request_manifest: bool = True
    save_raw_interaction: bool = True
    save_steps: bool = True
    save_output: bool = True
    save_usage: bool = True
    redact_request_manifest: bool = True
    fail_on_artifact_capture_error: bool = False
    fail_on_event_persistence_error: bool = False
