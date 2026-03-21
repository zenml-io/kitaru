"""Custom materializers for agent checkpoint outputs.

These extend ZenML's PydanticMaterializer to generate dashboard
visualizations for LLM responses and tool call results.
"""

import html
import os
from typing import Any, ClassVar

from models import LLMResponse, ToolCallResult
from zenml.enums import VisualizationType
from zenml.io import fileio
from zenml.materializers.materializer_registry import materializer_registry
from zenml.materializers.pydantic_materializer import PydanticMaterializer


class LLMResponseMaterializer(PydanticMaterializer):
    """Materializer for LLMResponse with Markdown visualization."""

    ASSOCIATED_TYPES: ClassVar[tuple[type[Any], ...]] = (LLMResponse,)

    def save_visualizations(self, data: Any) -> dict[str, VisualizationType]:
        parts: list[str] = []

        if getattr(data, "content", None):
            parts.append(data.content)

        tool_calls = getattr(data, "tool_calls", None)
        if tool_calls:
            parts.append("\n---\n### Tool Calls\n")
            parts.append("| # | Tool | Arguments |")
            parts.append("|---|------|-----------|")
            for i, tc in enumerate(tool_calls):
                func = getattr(tc, "function", tc)
                name = getattr(func, "name", "?")
                args = getattr(func, "arguments", "")
                args_preview = args[:120] + "..." if len(args) > 120 else args
                parts.append(f"| {i + 1} | `{name}` | `{args_preview}` |")

        md_content = "\n".join(parts) if parts else "_Empty response_"
        vis_path = os.path.join(self.uri, "visualization.md")
        with fileio.open(vis_path, "w") as f:
            f.write(md_content)
        return {vis_path.replace("\\", "/"): VisualizationType.MARKDOWN}


class ToolCallResultMaterializer(PydanticMaterializer):
    """Materializer for ToolCallResult with tool-aware visualization."""

    ASSOCIATED_TYPES: ClassVar[tuple[type[Any], ...]] = (ToolCallResult,)

    def save_visualizations(self, data: Any) -> dict[str, VisualizationType]:
        tool_name: str = getattr(data, "tool_name", "")
        output: str = getattr(data, "output", "")

        if tool_name == "web_fetch":
            return self._save_html(output)
        if tool_name == "python_exec":
            return self._save_code_html(output)
        return self._save_markdown(tool_name, output)

    def _save_html(self, content: str) -> dict[str, VisualizationType]:
        vis_path = os.path.join(self.uri, "visualization.html")
        with fileio.open(vis_path, "w") as f:
            f.write(content)
        return {vis_path.replace("\\", "/"): VisualizationType.HTML}

    def _save_code_html(self, output: str) -> dict[str, VisualizationType]:
        escaped = html.escape(output)
        page = (
            "<html><body>"
            '<h3 style="font-family:sans-serif">Python Execution</h3>'
            f'<pre style="background:#1e1e1e;color:#d4d4d4;padding:16px;'
            f'border-radius:8px;overflow-x:auto;font-size:13px">'
            f"{escaped}</pre>"
            "</body></html>"
        )
        vis_path = os.path.join(self.uri, "visualization.html")
        with fileio.open(vis_path, "w") as f:
            f.write(page)
        return {vis_path.replace("\\", "/"): VisualizationType.HTML}

    def _save_markdown(
        self, tool_name: str, output: str
    ) -> dict[str, VisualizationType]:
        md = f"### `{tool_name}`\n\n```\n{output}\n```"
        vis_path = os.path.join(self.uri, "visualization.md")
        with fileio.open(vis_path, "w") as f:
            f.write(md)
        return {vis_path.replace("\\", "/"): VisualizationType.MARKDOWN}


# ---------------------------------------------------------------------------
# Register materializers so ZenML uses them instead of the default
# PydanticMaterializer. This must run before any checkpoint executes.
# ---------------------------------------------------------------------------

materializer_registry.register_and_overwrite_type(
    LLMResponse, LLMResponseMaterializer
)
materializer_registry.register_and_overwrite_type(
    ToolCallResult, ToolCallResultMaterializer
)
