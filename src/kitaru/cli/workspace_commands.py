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
"""Workspace commands."""

from kitaru.cli import invocation_context, workspace_push
from kitaru.cli.command_registry import (
    ASSET_WRITE_ERRORS,
    build_spec,
    register_command,
    workspace_app,
)
from kitaru.cli.invocation_context import get_invocation
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec
from kitaru.cli.workspace_push_report import build_push_result, describe_not_copied


@register_command(
    workspace_app,
    build_spec(
        ("workspace", "push"),
        "Copy agents, connections, sessions, and insights from the selected "
        f"server to another server. Not copied: {describe_not_copied()}.",
        parameters=(
            ParameterSpec(
                "--to",
                "URL",
                "option",
                True,
                "Destination server URL. Uses the credential stored by "
                "`kitaru login URL`.",
            ),
            ParameterSpec(
                "--agent",
                "string[]",
                "option",
                False,
                "Agent name to copy with its sessions and insights. Repeat "
                "for more agents. Defaults to every agent.",
            ),
            ParameterSpec(
                "--dry-run",
                "boolean",
                "option",
                False,
                "Report what would be copied without writing to the destination.",
            ),
        ),
        read_only=False,
        side_effects=("uploads_data", "creates_remote_state"),
        idempotency="idempotent; records already on the destination are not "
        "copied again",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def workspace_push_command(
    *,
    to: str,
    agent: list[str] | None = None,
    dry_run: bool = False,
) -> CommandResult:
    """Copy workspace content from the selected server to another server."""
    invocation = get_invocation()
    source_url = invocation.resolve_target().server_url
    destination_url = workspace_push.validate_destination(to, source_url)
    async with (
        invocation_context.open_asset_client() as source,
        workspace_push.open_destination_client(
            destination_url, invocation.credential_store, invocation.request_timeout
        ) as destination,
    ):
        report = await workspace_push.push_workspace(
            source,
            destination,
            source_url=source_url,
            destination_url=destination_url,
            agent_names=agent,
            dry_run=dry_run,
        )
    return build_push_result(report)
