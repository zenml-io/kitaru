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
"""Report an install made through the one-line installer.

The installer marks its `kitaru setup` call with ``KITARU_INSTALL_METHOD``.
One anonymous event is sent for that call, unless client analytics are
disabled. Running `kitaru setup` by hand sends nothing.
"""

import logging
import os
from importlib.metadata import version

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.events import AnalyticsEvent
from kitaru.analytics.source import (
    AnalyticsSource,
    EventContext,
    current_event_context,
)
from kitaru.client.config import get_analytics_id, get_client_id

logger = logging.getLogger(__name__)

ENV_INSTALL_METHOD = "KITARU_INSTALL_METHOD"
# The installer waits for this call, so a slow network must not hold it up.
_TIMEOUT = 3.0


async def report_install(scope: str, client: AnalyticsClient | None = None) -> None:
    """Send one anonymous install event when the installer asked for it.

    Args:
        scope: Where Kitaru was installed, ``project`` or ``user``.
        client: Analytics client to send through; defaults to a new one with
            a short timeout.
    """
    method = os.environ.get(ENV_INSTALL_METHOD)
    if not method:
        return
    try:
        analytics_id = get_analytics_id()
        if analytics_id is None:
            return
        analytics = client or AnalyticsClient(timeout=_TIMEOUT)
        token = current_event_context.set(EventContext(source=AnalyticsSource.CLI))
        try:
            analytics.track(
                analytics_id,
                AnalyticsEvent.INSTALL_SCRIPT_COMPLETED,
                {
                    # Login presents this id, so it joins an install to a
                    # later sign-in from the same machine.
                    "client_id": str(get_client_id()),
                    "install_method": method,
                    "install_scope": scope,
                    "version": version("kitaru"),
                },
            )
        finally:
            current_event_context.reset(token)
        await analytics.aclose()
    except Exception as exc:
        logger.debug("Failed to report the install: %s", exc)
