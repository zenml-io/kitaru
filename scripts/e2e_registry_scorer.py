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
"""Self-contained scorer registered as a plugin by the end-to-end driver."""

from typing import Any


def output_length(session: Any, max_chars: int = 200) -> float:
    """Score the final output on staying within a character budget."""
    text = str(session.session.outputs or "")
    if not text:
        return 0.0
    return round(min(1.0, max_chars / len(text)), 4)
