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
"""Plugin DTO conversions."""

import kitaru.api_models.v1.plugins as plugin_models
from kitaru.server.domain.plugin import PluginFormat


def format_to_domain(format: plugin_models.PluginFormat) -> PluginFormat:
    """Convert a code format DTO to its domain enum.

    Args:
        format: Code format from the API.

    Returns:
        Domain code format.
    """
    return PluginFormat(format.value)
