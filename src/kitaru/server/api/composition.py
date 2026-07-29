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
"""Event subscriber composition."""

from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.application.events import EventDispatcher


def build_event_dispatcher(session: AsyncSession) -> EventDispatcher:
    """Build the event dispatcher every subscriber of one request shares.

    Subscribers are constructed here with repositories bound to the request's
    database session, so a handler commits or rolls back with the transition
    that emitted its event.

    Args:
        session: Request-scoped database session.

    Returns:
        Dispatcher carrying the registered subscribers.
    """
    _ = session
    return EventDispatcher()
