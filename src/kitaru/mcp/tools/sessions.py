#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Compact session transcript handler."""

from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import ToolSuccessPayload
from kitaru.mcp.models.sessions import SessionReadData, SessionReadRequest
from kitaru.session_transcript import format_transcripts, read_transcripts


async def handle_session_read(
    state: MCPServerState, request: SessionReadRequest
) -> ToolSuccessPayload:
    """Render each requested session as a compact transcript."""
    try:
        transcripts = await read_transcripts(
            state.client.sessions,
            request.session_ids,
            max_chars=None if request.full else request.max_chars,
            include_reasoning=request.reasoning,
            node_index=request.node,
            concurrency=state.settings.pool_size,
        )
    except ValueError as error:
        raise MCPToolError("invalid_arguments", str(error)) from None
    text = format_transcripts(transcripts)
    if any(transcript.truncated for transcript in transcripts):
        text += (
            "\n\nCut fields end in [+N chars]; pass `node` and `full` to see one "
            "node whole."
        )
    return ToolSuccessPayload(data=SessionReadData(transcripts=transcripts), text=text)
