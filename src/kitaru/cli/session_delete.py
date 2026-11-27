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
"""Session deletion CLI command helpers."""

from pathlib import Path
from typing import Any

from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import list_params
from kitaru.cli.session_selection import parse_session_ids
from kitaru.client.exceptions import APIError

_MAX_DELETE_FAILURES = 50


async def delete_sessions(
    client: Any,
    values: list[str] | None,
    *,
    sessions_file: Path | None,
    filter: str | None,
    force: bool,
) -> CommandResult:
    """Delete one or more sessions selected by ID, file, or filter."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting sessions requires --force.")
    explicit = bool(values) or sessions_file is not None
    if explicit == (filter is not None):
        raise CLIError(
            "invalid_arguments",
            "Select sessions using SESSION IDs/--sessions-file or --filter.",
        )

    if explicit:
        session_ids = parse_session_ids(values or [], sessions_file)
    else:
        params = list_params(
            "session", size=1000, cursor=None, sort="created:desc", filter=filter
        )
        assert isinstance(params, SessionListParams)
        session_ids = [session.id async for session in client.sessions.iter(params)]
        if not session_ids:
            raise CLIError(
                "invalid_arguments", "No sessions matched the --filter selection."
            )

    if len(session_ids) == 1:
        await client.sessions.delete(session_ids[0])
        return CommandResult(
            item={
                "deleted_ids": [str(session_ids[0])],
                "deleted_count": 1,
            }
        )

    deleted_ids: list[str] = []
    failures: list[dict[str, Any]] = []
    total_failures = 0
    for session_id in session_ids:
        try:
            await client.sessions.delete(session_id)
        except APIError as error:
            if error.status_code in {401, 403} or error.status_code >= 500:
                if not deleted_ids and not total_failures:
                    raise
                total_failures += 1
                if len(failures) < _MAX_DELETE_FAILURES:
                    failures.append(
                        {
                            "id": str(session_id),
                            "status_code": error.status_code,
                            "detail": error.detail,
                        }
                    )
                break
            total_failures += 1
            if len(failures) < _MAX_DELETE_FAILURES:
                failures.append(
                    {
                        "id": str(session_id),
                        "status_code": error.status_code,
                        "detail": error.detail,
                    }
                )
        except Exception as error:
            if not deleted_ids and not total_failures:
                raise
            total_failures += 1
            if len(failures) < _MAX_DELETE_FAILURES:
                failures.append(
                    {
                        "id": str(session_id),
                        "error_type": type(error).__name__,
                        "detail": str(error),
                    }
                )
            break
        else:
            deleted_ids.append(str(session_id))

    if total_failures:
        details: dict[str, Any] = {
            "deleted_ids": deleted_ids,
            "deleted_count": len(deleted_ids),
            "failed_count": total_failures,
            "failures": failures,
        }
        if total_failures > len(failures):
            details["failures_truncated"] = True
        status_codes = {item.get("status_code") for item in failures}
        if not deleted_ids and status_codes == {409}:
            kind = "conflict"
            message = (
                f"Failed to delete {total_failures} session(s): {failures[0]['detail']}"
            )
        elif not deleted_ids and status_codes == {404}:
            kind = "not_found"
            message = (
                f"Failed to delete {total_failures} session(s): {failures[0]['detail']}"
            )
        else:
            kind = "partial_failure"
            message = (
                f"Deleted {len(deleted_ids)} of {len(session_ids)} session(s); "
                f"{total_failures} failed."
            )
        raise CLIError(kind, message, details=details)

    return CommandResult(
        item={
            "deleted_ids": deleted_ids,
            "deleted_count": len(deleted_ids),
        }
    )
