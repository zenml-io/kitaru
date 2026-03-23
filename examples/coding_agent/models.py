"""Pydantic models for the coding agent's checkpoint inputs and outputs."""

from typing import Any

from pydantic import BaseModel


class FollowUp(BaseModel):
    """Schema for the follow-up wait after the agent completes a task."""

    message: str = ""
    is_finished: bool = False


class ToolCallFunction(BaseModel):
    name: str
    arguments: str


class ToolCallRequest(BaseModel):
    id: str
    type: str = "function"
    function: ToolCallFunction


class LLMResponse(BaseModel):
    """Normalized LLM response that round-trips cleanly through ZenML."""

    role: str
    content: str | None = None
    tool_calls: list[ToolCallRequest] | None = None

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls)

    def to_message(self) -> dict[str, Any]:
        """Convert to a dict suitable for the LiteLLM messages list."""
        msg: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.has_tool_calls:
            msg["tool_calls"] = [tc.model_dump() for tc in self.tool_calls]  # type: ignore[union-attr]
        return msg


class ToolCallResult(BaseModel):
    """Normalized tool call result."""

    tool_name: str
    output: str
