"""Capture policy for the Claude Agent SDK adapter."""

from pydantic import BaseModel, ConfigDict


class ClaudeCapturePolicy(BaseModel):
    """Controls what the Claude adapter captures for one SDK invocation."""

    model_config = ConfigDict(extra="forbid")

    emit_events: bool = True
    save_prompt: bool = True
    save_messages: bool = True
    save_transcript_file: bool = True
    save_options_manifest: bool = True
    save_final_output: bool = True
    save_usage: bool = True
    redact_options_manifest: bool = True
