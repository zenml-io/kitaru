"""Profile: the central factory artifact — declares what one agent is.

Stages add fields here as new capabilities come online (sandbox proxy
rules in stage 3, service configs and skill sources in stages 4+).
Stage 1 only needs the four basic fields.
"""

from typing import Literal

from pydantic import BaseModel, Field

ToolName = Literal["exec", "skill", "exec_service", "ask_question"]


class Profile(BaseModel):
    """One agent's runtime profile."""

    name: str
    system_prompt: str
    model: str  # raw pydantic-ai provider string, e.g. "openai:gpt-4o-mini"
    allowed_tools: set[ToolName] = Field(default_factory=set)
