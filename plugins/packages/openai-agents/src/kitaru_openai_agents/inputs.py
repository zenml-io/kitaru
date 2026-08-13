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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""OpenAI input normalization shared by task and replay paths."""

import json
from typing import Any, cast

from agents import TResponseInputItem
from pydantic import TypeAdapter, ValidationError

_INPUT_ITEMS_ADAPTER = TypeAdapter(list[TResponseInputItem])


def normalize_openai_input(value: Any) -> str | list[TResponseInputItem]:
    """Keep valid OpenAI input lists and serialize other JSON deterministically."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        try:
            _INPUT_ITEMS_ADAPTER.validate_python(value)
        except ValidationError:
            pass
        else:
            return cast(list[TResponseInputItem], value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
