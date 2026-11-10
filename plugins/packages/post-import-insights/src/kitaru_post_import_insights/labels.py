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
"""Safety bounds for caller-controlled labels and JSON Pointer selectors."""

import re
from functools import lru_cache

from kitaru.redaction import redact_data

MAX_LABEL_LENGTH = 120
MAX_SELECTOR_LENGTH = 1_024
MAX_SELECTOR_SEGMENTS = 64
UNAVAILABLE_TOOL = "Unavailable tool"

_CONTROL_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
_PRIVATE_KEY_PATTERN_SUFFIX = "PRIVATE" + r" KEY-----"
_CREDENTIAL_PATTERNS = (
    re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b", re.IGNORECASE),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b", re.IGNORECASE),
    re.compile(r"\bxox[a-z]-[A-Za-z0-9-]{20,}\b", re.IGNORECASE),
    re.compile(r"\b(?:AK" r"IA|AS" r"IA)[A-Z0-9]{16}\b"),
    re.compile(r"\be" r"yJ[A-Za-z0-9_-]+\.e" r"yJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    re.compile(r"-----BEGIN [A-Z ]*" + _PRIVATE_KEY_PATTERN_SUFFIX, re.IGNORECASE),
    re.compile(r"\bauthorization\s*:\s*(?:bearer|basic)\s+\S+", re.IGNORECASE),
    re.compile(
        r"\b(?:authorization|proxy-authorization|x-api-key|api-key|token|secret)"
        r"\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:password|passwd|pwd|credentials?|client[_-]?secret|private[_-]?key)"
        r"\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    re.compile(r"https?://[^\s/@:]+:[^\s/@]+@", re.IGNORECASE),
    re.compile(
        r"https?://[^\s?#]+[?&](?:api[_-]?key|access[_-]?token|token|secret)="
        r"[^\s&#]+",
        re.IGNORECASE,
    ),
)
_INSTRUCTION_PATTERN = re.compile(
    r"\b(?:ignore|disregard|override)\s+(?:(?:all|any|every|the)\s+)?"
    r"(?:previous|prior|above|later|following)?\s*"
    r"(?:instructions?|directions?|prompts?|rules?)\b|"
    r"\breveal\s+(?:the\s+)?system\s+prompt\b|"
    r"\byou\s+are\s+now\s+(?:an?|the)\b",
    re.IGNORECASE,
)


def _check_label(candidate: str) -> str | None:
    """Return a bounded label unchanged, or None when it may contain a secret."""
    if not candidate.strip():
        return None
    try:
        candidate.encode("utf-8")
    except UnicodeEncodeError:
        return None
    redacted = redact_data(candidate)
    if not isinstance(redacted, str) or redacted != candidate or "***" in redacted:
        return None
    if _CONTROL_PATTERN.search(candidate):
        return None
    if any(pattern.search(candidate) for pattern in _CREDENTIAL_PATTERNS):
        return None
    if _INSTRUCTION_PATTERN.search(candidate):
        return None
    return candidate


# A scan checks the same tool and model names in every session, and the check
# is a pure function of the text, so a bounded cache keeps it off the hot path.
_check_cached_label = lru_cache(maxsize=4_096)(_check_label)


def sanitize_label(value: str | None) -> str | None:
    """Return a bounded exact label, or omit it when it may contain a secret."""
    if value is None or len(value) > MAX_LABEL_LENGTH:
        return None
    # Caching only exact strings keeps a subclass's own identity in the result.
    if type(value) is str:
        return _check_cached_label(value)
    return _check_label(value)
