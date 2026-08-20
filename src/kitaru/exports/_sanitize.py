"""Ephemeral exact-value sanitization for experiment export resolution."""

import copy
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, NoReturn

from pydantic import BaseModel

from .models import V1_EXPORT_BUDGETS, ExportError

_REDACTED = "[REDACTED]"
_MINIMUM_SAFE_SECRET_BYTES = 8


class EphemeralSanitizer:
    """Hold protected values only for one in-memory resolution pass.

    The sanitizer deliberately cannot be pickled or represented with its values.
    Resolved models retain only redacted content and required runtime names.
    """

    __slots__ = ("_encoded_values", "_values")

    def __init__(self, values: list[str]) -> None:
        """Validate and retain exact protected values for the current operation."""
        total_bytes = 0
        unique: dict[str, None] = {}
        for value in values:
            encoded = value.encode("utf-8")
            if len(encoded) < _MINIMUM_SAFE_SECRET_BYTES:
                raise ExportError(
                    "unsafe_secret_value",
                    "An attached secret value cannot be used for safe exact-value "
                    "sanitization.",
                )
            total_bytes += len(encoded)
            if total_bytes > V1_EXPORT_BUDGETS.max_protected_value_bytes:
                raise ExportError(
                    "protected_values_too_large",
                    "Attached secret values exceed the 1 MiB export limit.",
                )
            unique[value] = None
        self._values = tuple(sorted(unique, key=lambda item: (-len(item), item)))
        self._encoded_values = tuple(value.encode("utf-8") for value in self._values)

    def __repr__(self) -> str:
        """Return a representation that cannot reveal protected material."""
        return f"<{type(self).__name__} protected_values={len(self._values)}>"

    def __reduce__(self) -> NoReturn:
        """Refuse serialization so protected values cannot enter receipts."""
        raise TypeError("EphemeralSanitizer cannot be serialized")

    def __getstate__(self) -> NoReturn:
        """Refuse state extraction so protected values remain ephemeral."""
        raise TypeError("EphemeralSanitizer cannot be serialized")

    def contains_text(self, value: str) -> bool:
        """Return whether text contains any exact protected value."""
        return any(secret in value for secret in self._values)

    def contains_bytes(self, value: bytes) -> bool:
        """Return whether bytes contain any UTF-8 encoded protected value."""
        return any(secret in value for secret in self._encoded_values)

    def reject_text(self, value: str, *, code: str, message: str) -> None:
        """Fail without echoing protected executable or path material."""
        if self.contains_text(value):
            raise ExportError(code, message)

    def reject_bytes(self, value: bytes, *, code: str, message: str) -> None:
        """Fail without echoing protected binary or source material."""
        if self.contains_bytes(value):
            raise ExportError(code, message)

    def sanitize(self, value: Any) -> Any:
        """Return a deep sanitized copy of supported structured data.

        Mapping keys are identifiers or path-like material. Rewriting them could
        change program behavior, so a protected value in a key fails closed.
        """
        if isinstance(value, str):
            result = value
            for secret in self._values:
                result = result.replace(secret, _REDACTED)
            return result
        if isinstance(value, BaseModel):
            payload = self.sanitize(value.model_dump(mode="python"))
            return type(value).model_validate(payload)
        if isinstance(value, SimpleNamespace):
            return SimpleNamespace(**self.sanitize(vars(value)))
        if isinstance(value, Mapping):
            sanitized: dict[Any, Any] = {}
            for key, item in value.items():
                if isinstance(key, str):
                    self.reject_text(
                        key,
                        code="protected_value_in_structured_key",
                        message=(
                            "Protected runtime material appears in a structured key; "
                            "export cannot rewrite identifiers safely."
                        ),
                    )
                sanitized[copy.deepcopy(key)] = self.sanitize(item)
            return sanitized
        if isinstance(value, list):
            return [self.sanitize(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self.sanitize(item) for item in value)
        if isinstance(value, set):
            return {self.sanitize(item) for item in value}
        return copy.deepcopy(value)
