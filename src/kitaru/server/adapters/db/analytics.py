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
"""Session listeners flushing buffered analytics on commit and rollback."""

from sqlalchemy import event
from sqlalchemy.orm import Session

from kitaru.server.application.services.server_analytics import (
    discard_analytics_buffer,
    flush_analytics_buffer,
)


def register_analytics_listeners() -> None:
    """Register the session listeners delivering buffered analytics.

    Registering more than once adds no duplicate listeners.
    """
    if not event.contains(Session, "after_commit", flush_analytics_buffer):
        event.listen(Session, "after_commit", flush_analytics_buffer)
    if not event.contains(Session, "after_rollback", discard_analytics_buffer):
        event.listen(Session, "after_rollback", discard_analytics_buffer)
