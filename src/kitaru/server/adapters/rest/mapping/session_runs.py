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
"""Session run DTO conversions."""

from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.server.application.models.job import SessionRunCreate


def session_run_create_to_command(body: SessionRunCreateRequest) -> SessionRunCreate:
    """Convert a session run create request to its command.

    Args:
        body: Session run create request.

    Returns:
        Session run create command.
    """
    return SessionRunCreate(
        agent_version_id=body.agent_version_id, inputs=body.inputs, name=body.name
    )
