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
"""Redact recognizable credentials from structured output and diagnostics."""

import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kitaru.api_models.v1.session import TokenUsage

# A name holds a credential when the noun it ends in names one, as in
# `OPENAI_API_KEY`, `HFToken`, or `PGPASSWORD`. The last word decides what the
# value is, so `credential_status`, `secret_keys`, and
# `AWS_WEB_IDENTITY_TOKEN_FILE` hold none. Two-word nouns are matched glued.
_CREDENTIAL_NOUNS = frozenset(
    {
        "accesskey",
        "accesskeys",
        "accesstokens",
        "apikey",
        "apikeys",
        "apitokens",
        "authkey",
        "authkeys",
        "authorization",
        "authtokens",
        "credential",
        "credentials",
        "devicecode",
        "devicecodes",
        "encryptionkey",
        "encryptionkeys",
        "privatekey",
        "privatekeys",
        "pwd",
        "refreshtokens",
        "secretenv",
        "secretkey",
        "signingkey",
        "signingkeys",
    }
)
_CREDENTIAL_ENDINGS = (
    "passphrase",
    "passphrases",
    "passwd",
    "password",
    "passwords",
    "secret",
    "secrets",
    "token",
)
# Explicit credential qualifiers before `token`/`tokens` always mask regardless
# of value type, taking precedence over numeric token-count exemptions.
_CREDENTIAL_TOKEN_WORDS = frozenset(
    {
        "access",
        "api",
        "auth",
        "authentication",
        "authorization",
        "bearer",
        "credential",
        "credentials",
        "csrf",
        "device",
        "id",
        "identity",
        "invitation",
        "invite",
        "jwt",
        "key",
        "login",
        "mfa",
        "oauth",
        "otp",
        "password",
        "passwords",
        "pat",
        "private",
        "recovery",
        "refresh",
        "reset",
        "secret",
        "secrets",
        "security",
        "session",
        "signing",
        "sso",
        "totp",
        "verification",
        "webhook",
        "xsrf",
    }
)
# A name ending in `tokens` counts tokens when any earlier word is one of
# these, as in `MAX_TOKENS`, `max_new_tokens`, or `prompt_tokens`; otherwise it
# holds them, as in `GITHUB_TOKENS` or `new_tokens`.
_TOKEN_COUNT_WORDS = frozenset(
    {
        "budget",
        "cache",
        "cached",
        "completion",
        "context",
        "input",
        "max",
        "min",
        "num",
        "output",
        "prompt",
        "reasoning",
        "remaining",
        "total",
        "used",
    }
)
_PLURAL_TOKEN_KEY = re.compile(r"[a-z0-9][_-]tokens$", re.IGNORECASE)
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+|KITKEY_|ZENPROKEY_)[^\s,;\]\}\"']+")

MASKED = "***"
"""The text that stands in for a masked value."""

_TOKEN_USAGE_KEYS = frozenset(TokenUsage.model_fields)
_DIGITS = re.compile(r"[0-9]+")
_MAX_DEPTH = 64


def _split_name_words(name: str) -> list[str]:
    """Split a setting or field name into lowercase alphabetic words."""
    # One capital before the word, not `[A-Z]+`: the same split, without
    # backtracking that grows with the square of a long all-caps name.
    spaced = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", name)
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", spaced)
    # Digits drop out, so `API_KEY1` and `TOKEN2` end in their nouns.
    return re.findall(r"[a-z]+", spaced.lower())


def _is_explicit_credential_token_name(words: list[str]) -> bool:
    """Return whether parsed name words name an explicit credential token."""
    return (
        len(words) >= 2
        and words[-1] in {"token", "tokens"}
        and words[-2] in _CREDENTIAL_TOKEN_WORDS
    )


def _is_token_count_name(words: list[str]) -> bool:
    """Return whether parsed name words describe a plural token-count field."""
    return (
        len(words) >= 2
        and words[-1] == "tokens"
        and not _is_explicit_credential_token_name(words)
        and not _TOKEN_COUNT_WORDS.isdisjoint(words[:-1])
    )


def _is_credential_words(words: list[str]) -> bool:
    """Return whether parsed name words say a value is a credential."""
    if not words:
        return False
    if _is_explicit_credential_token_name(words):
        return True
    if words[-1] == "tokens":
        return not _is_token_count_name(words)
    tails = (words[-1], "".join(words[-2:]))
    return any(
        tail in _CREDENTIAL_NOUNS or tail.endswith(_CREDENTIAL_ENDINGS)
        for tail in tails
    )


def is_credential_name(name: str) -> bool:
    """Return whether a setting or field name says its value is a credential."""
    return _is_credential_words(_split_name_words(name))


def redact(value: str) -> str:
    """Mask recognizable credentials in a string."""
    return _SECRET_VALUE.sub(lambda match: f"{match.group(1)}***", value)


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Return bounded JSON-safe output with recognizable credentials masked."""
    try:
        return _redact_data(value, key=key, depth=0, active_ids=set(), mask=True)
    except Exception:
        # Normalization errors can include credentials in their messages.
        return "[unavailable]"


def to_json_data(value: Any) -> Any:
    """Return bounded JSON-safe output, normalized as `redact_data` does, unmasked."""
    try:
        return _redact_data(value, key=None, depth=0, active_ids=set(), mask=False)
    except Exception:
        return "[unavailable]"


def _redact_data(
    value: Any, *, key: str | None, depth: int, active_ids: set[int], mask: bool
) -> Any:
    """Normalize one value while retaining key context and active ancestors."""
    if mask and key is not None and _should_mask_key(key, value):
        return MASKED
    if depth >= _MAX_DEPTH:
        return "[depth limit]"
    if isinstance(value, BaseModel):
        # JSON-mode dumping collapses mixed keys before we can disambiguate them.
        value = value.model_dump(mode="python")
    if isinstance(value, (dict, list, tuple, set)):
        identity = id(value)
        if identity in active_ids:
            return "[cycle]"
        active_ids.add(identity)
        try:
            if isinstance(value, dict):
                reserved = {item_key for item_key in value if isinstance(item_key, str)}
                result: dict[str, Any] = {}
                for item_key, item in value.items():
                    original_key = str(item_key)
                    output_key = original_key
                    if not isinstance(item_key, str):
                        suffix = 2
                        while output_key in reserved or output_key in result:
                            output_key = f"{original_key} [{suffix}]"
                            suffix += 1
                    result[output_key] = _redact_data(
                        item,
                        key=original_key,
                        depth=depth + 1,
                        active_ids=active_ids,
                        mask=mask,
                    )
                return result
            return [
                _redact_data(
                    item, key=None, depth=depth + 1, active_ids=active_ids, mask=mask
                )
                for item in value
            ]
        finally:
            active_ids.remove(identity)
    if isinstance(value, Enum):
        return _redact_data(
            value.value, key=key, depth=depth + 1, active_ids=active_ids, mask=mask
        )
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return redact(str(value)) if mask else str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return redact(value) if mask else value
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return "[unavailable]"


def _should_mask_key(key: str, value: Any) -> bool:
    """Return whether a mapping key must be masked for the given value."""
    words = _split_name_words(key)
    if _is_explicit_credential_token_name(words):
        return True
    is_token_count = _is_token_count_name(words)
    # Token usage and plural token-count fields hold counts, so non-count
    # values under them are masked like credentials.
    if not (_is_credential_words(words) or key in _TOKEN_USAGE_KEYS or is_token_count):
        return False
    return not _is_token_usage(value, key=key, is_token_count=is_token_count)


def _is_token_usage(value: Any, *, key: str, is_token_count: bool = False) -> bool:
    """Recognize only numeric token usage fields, including per-call means."""
    if key in _TOKEN_USAGE_KEYS:
        return _is_usage_count(value)
    if key == "tokens":
        if isinstance(value, dict):
            return all(
                field in _TOKEN_USAGE_KEYS and _is_usage_count(count)
                for field, count in value.items()
            )
        # A bare `tokens` total is a raw API count, so a fractional one is suspect.
        return _is_whole_token_count(value)
    if is_token_count:
        return _is_whole_usage_count(value)
    if _PLURAL_TOKEN_KEY.search(key):
        return _is_whole_token_count(value)
    return False


def _is_usage_count(value: Any) -> bool:
    """Recognize a token usage field's count, which a setting holds as digits."""
    return _is_token_count(value) or (
        isinstance(value, str) and _DIGITS.fullmatch(value) is not None
    )


def _is_whole_usage_count(value: Any) -> bool:
    """Recognize a whole-number token count, including a setting held as digits."""
    return _is_whole_token_count(value) or (
        isinstance(value, str) and _DIGITS.fullmatch(value) is not None
    )


def _is_token_count(value: Any) -> bool:
    """Recognize a missing, whole, or averaged token count."""
    return _is_whole_token_count(value) or isinstance(value, float)


def _is_whole_token_count(value: Any) -> bool:
    """Recognize a missing or whole-number token count."""
    return value is None or (isinstance(value, int) and not isinstance(value, bool))
