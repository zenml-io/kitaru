from dataclasses import dataclass, field, replace

from pydantic_ai import FunctionToolset
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import UserError
from pydantic_ai.tools import AgentDepsT
from pydantic_ai.toolsets import ToolsetTool
from pydantic_ai.toolsets.function import FunctionToolsetTool

from ._events import ToolsetKind
from ._hitl import hitl_metadata_for_tool
from ._toolset import KitaruToolset


@dataclass
class KitaruFunctionToolset(KitaruToolset[AgentDepsT]):
    toolset_kind: ToolsetKind = field(default='function', init=False)

    # Signature matches `WrapperToolset.get_tools` so the LSP check passes.
    # Values are `FunctionToolsetTool` (a `ToolsetTool` subclass) at runtime
    # — concrete callers that need the subclass-specific shape should narrow
    # via `isinstance`.
    async def get_tools(
        self, ctx: RunContext[AgentDepsT]
    ) -> dict[str, ToolsetTool[AgentDepsT]]:
        assert isinstance(self.wrapped, FunctionToolset)
        tools: dict[str, ToolsetTool[AgentDepsT]] = {}
        for original_name, source_tool in self.wrapped.tools.items():
            max_retries = source_tool.max_retries if source_tool.max_retries is not None else self.wrapped.max_retries
            run_context = replace(
                ctx,
                tool_name=original_name,
                retry=ctx.retries.get(original_name, 0),
                max_retries=max_retries,
            )
            tool_def = await source_tool.prepare_tool_def(run_context)
            if not tool_def:
                continue

            hitl_metadata = hitl_metadata_for_tool(source_tool)
            if hitl_metadata is not None:
                tool_def = replace(tool_def, metadata={**(tool_def.metadata or {}), **hitl_metadata})

            new_name = tool_def.name
            if new_name in tools:
                raise UserError(
                    f'Tool name {new_name!r} collides with another tool in the same toolset; '
                    f'rename one of them (originating from {original_name!r}).'
                )

            tools[new_name] = FunctionToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=max_retries,
                args_validator=source_tool.function_schema.validator,
                args_validator_func=source_tool.args_validator,
                call_func=source_tool.function_schema.call,
                is_async=source_tool.function_schema.is_async,
                timeout=tool_def.timeout,
            )
        return tools
