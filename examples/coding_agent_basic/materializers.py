"""Custom materializers for agent checkpoint outputs.

These extend ZenML's PydanticMaterializer to dynamically generate
visualizations based on checkpoint content.

Generated files (HTML, Markdown, etc.) are saved as separate artifacts
via kitaru.save() inside the checkpoint — the materializer only handles
the visualization of the checkpoint's own return value (LLMResponse
or ToolCallResult).
"""

import html
import os
from typing import Any, ClassVar, Dict, Tuple, Type

from pydantic import BaseModel
from zenml.enums import VisualizationType
from zenml.io import fileio
from zenml.materializers.pydantic_materializer import PydanticMaterializer


class _LLMResponsePlaceholder(BaseModel):
    pass


class _ToolCallResultPlaceholder(BaseModel):
    pass


class LLMResponseMaterializer(PydanticMaterializer):
    """Materializer for LLMResponse with dynamic Markdown visualization."""

    ASSOCIATED_TYPES: ClassVar[Tuple[Type[Any], ...]] = (_LLMResponsePlaceholder,)

    def save_visualizations(self, data: Any) -> Dict[str, VisualizationType]:
        """Render LLM response as Markdown for the dashboard."""
        parts: list[str] = []

        content = getattr(data, "content", None)
        if content:
            parts.append(content)

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


_HTML_TOOLS = {"web_fetch"}
_CODE_TOOLS = {"python_exec"}


class ToolCallResultMaterializer(PydanticMaterializer):
    """Materializer for ToolCallResult with tool-aware visualization."""

    ASSOCIATED_TYPES: ClassVar[Tuple[Type[Any], ...]] = (_ToolCallResultPlaceholder,)

    def save_visualizations(self, data: Any) -> Dict[str, VisualizationType]:
        """Render tool output with the appropriate visualization type."""
        tool_name: str = getattr(data, "tool_name", "")
        output: str = getattr(data, "output", "")

        if tool_name in _HTML_TOOLS:
            return self._save_html(output)

        if tool_name in _CODE_TOOLS:
            return self._save_code_html(output)

        return self._save_markdown(tool_name, output)

    def _save_html(self, content: str) -> Dict[str, VisualizationType]:
        vis_path = os.path.join(self.uri, "visualization.html")
        with fileio.open(vis_path, "w") as f:
            f.write(content)
        return {vis_path.replace("\\", "/"): VisualizationType.HTML}

    def _save_code_html(self, output: str) -> Dict[str, VisualizationType]:
        """Render python_exec output as styled HTML."""
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
    ) -> Dict[str, VisualizationType]:
        md = f"### `{tool_name}`\n\n```\n{output}\n```"
        vis_path = os.path.join(self.uri, "visualization.md")
        with fileio.open(vis_path, "w") as f:
            f.write(md)
        return {vis_path.replace("\\", "/"): VisualizationType.MARKDOWN}
