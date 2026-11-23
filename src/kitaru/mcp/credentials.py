#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Recognize setting names that look like credentials."""

import re
from itertools import pairwise

# Whole name segments (split on `_`, `-`, `.`, camelCase, and an acronym before
# a capitalized word, as in `AWSSecretKey`), so `API_TOKEN` and
# `AWS_ACCESS_KEY_ID` match but settings like `MAX_TOKENS` or `TOKENIZER_NAME`
# do not.
_SENSITIVE_SEGMENTS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "credential",
        "credentials",
        "apikey",
    }
)
_SENSITIVE_PAIRS = frozenset(
    {
        ("api", "key"),
        ("access", "key"),
        ("private", "key"),
        ("secret", "key"),
        ("auth", "key"),
    }
)


def is_credential_like(name: str) -> bool:
    """Return whether a setting name looks like it holds a credential."""
    # One capital before the word, not `[A-Z]+`: the same split, without
    # backtracking that grows with the square of a long all-caps name.
    spaced = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", name)
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", spaced)
    segments = [part for part in re.split(r"[^a-z0-9]+", spaced.lower()) if part]
    return any(part in _SENSITIVE_SEGMENTS for part in segments) or any(
        pair in _SENSITIVE_PAIRS for pair in pairwise(segments)
    )
