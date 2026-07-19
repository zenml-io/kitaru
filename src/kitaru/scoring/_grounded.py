"""Default-deny grounded execution support for scoring."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import JsonValue

from kitaru.scoring._contracts import (
    GroundedCallEvidence,
    GroundedCapabilityDeclaration,
    GroundedPolicySnapshot,
    GroundedProvenance,
)

_SECRET_KEY_PARTS = frozenset(
    {"api_key", "apikey", "auth", "credential", "password", "secret", "token"}
)
_GROUNDED_CALL_EXECUTOR = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="kitaru-grounded"
)


class GroundedCapabilityBlocked(RuntimeError):
    """Raised when a grounded scorer requests denied or unsafe access."""


@dataclass(frozen=True)
class GroundedCapability:
    """Runtime registration for one read-only grounded capability."""

    name: str
    revision: str
    read_only: bool
    call: Callable[[str], Any]

    @property
    def declaration(self) -> GroundedCapabilityDeclaration:
        """Return the immutable declaration that can be persisted safely."""
        return GroundedCapabilityDeclaration(
            name=self.name,
            revision=self.revision,
            read_only=self.read_only,
        )


class GroundedWorld:
    """Typed default-deny handle passed to grounded scorers."""

    def __init__(
        self,
        *,
        policy: GroundedPolicySnapshot,
        capabilities: Mapping[str, GroundedCapability],
    ) -> None:
        self.policy = policy
        self._capabilities = dict(capabilities)
        self._calls: list[GroundedCallEvidence] = []

    @property
    def provenance(self) -> GroundedProvenance:
        """Return bounded call evidence accumulated so far."""
        return GroundedProvenance(policy=self.policy, calls=list(self._calls))

    def call(
        self,
        capability_name: str,
        resource_identifier: str,
        *,
        request_summary: Mapping[str, JsonValue] | None = None,
    ) -> Any:
        """Call an explicitly allowed read-only capability/resource."""
        capability = self._capabilities.get(capability_name)
        if capability is None:
            raise GroundedCapabilityBlocked(
                f"Grounded capability '{capability_name}' is unavailable."
            )
        declared = next(
            (
                item
                for item in self.policy.capabilities
                if item.name == capability.name and item.revision == capability.revision
            ),
            None,
        )
        if declared is None:
            raise GroundedCapabilityBlocked(
                f"Grounded capability '{capability_name}' is not allowed by policy."
            )
        if not capability.read_only or not declared.read_only:
            raise GroundedCapabilityBlocked(
                f"Grounded capability '{capability_name}' is not read-only."
            )
        if not _resource_allowed(
            resource_identifier,
            self.policy.allowed_resources.get(capability_name, []),
        ):
            raise GroundedCapabilityBlocked(
                f"Resource '{resource_identifier}' is not allowed for "
                f"'{capability_name}'."
            )

        attempts = self.policy.retry_limit + 1
        last_error: BaseException | None = None
        for _attempt in range(attempts):
            started = datetime.now(UTC).isoformat()
            start = time.perf_counter()
            future = _GROUNDED_CALL_EXECUTOR.submit(
                capability.call, resource_identifier
            )
            try:
                result = future.result(timeout=self.policy.timeout_seconds)
            except FutureTimeoutError:
                future.cancel()
                finished = datetime.now(UTC).isoformat()
                self._record_call(
                    capability_name=capability_name,
                    resource_identifier=resource_identifier,
                    started_at=started,
                    finished_at=finished,
                    request_summary=request_summary,
                    result_summary={"error_type": "TimeoutError"},
                )
                last_error = TimeoutError(
                    f"Grounded capability '{capability_name}' exceeded its timeout."
                )
                continue
            except Exception as exc:
                finished = datetime.now(UTC).isoformat()
                self._record_call(
                    capability_name=capability_name,
                    resource_identifier=resource_identifier,
                    started_at=started,
                    finished_at=finished,
                    request_summary=request_summary,
                    result_summary={"error_type": type(exc).__name__},
                )
                last_error = exc
                continue
            finished = datetime.now(UTC).isoformat()
            elapsed = time.perf_counter() - start
            self._record_call(
                capability_name=capability_name,
                resource_identifier=resource_identifier,
                started_at=started,
                finished_at=finished,
                request_summary=request_summary,
                result_summary={"elapsed_seconds": elapsed}
                if self.policy.evidence_retention == "none"
                else _summary(result),
            )
            return result
        assert last_error is not None
        raise last_error

    def _record_call(
        self,
        *,
        capability_name: str,
        resource_identifier: str,
        started_at: str,
        finished_at: str,
        request_summary: Mapping[str, JsonValue] | None,
        result_summary: Mapping[str, JsonValue],
    ) -> None:
        self._calls.append(
            GroundedCallEvidence(
                capability_name=capability_name,
                resource_identifier=resource_identifier,
                started_at=started_at,
                finished_at=finished_at,
                request_summary=_summary(dict(request_summary or {})),
                result_summary=dict(result_summary)
                if self.policy.evidence_retention == "none"
                else _summary(result_summary),
            )
        )


def _resource_allowed(resource_identifier: str, rules: list[str]) -> bool:
    for rule in rules:
        if rule.endswith("*") and resource_identifier.startswith(rule[:-1]):
            return True
        if resource_identifier == rule:
            return True
    return False


def _summary(value: Any) -> dict[str, JsonValue]:
    if isinstance(value, str | int | float | bool) or value is None:
        return {"value": value}
    if isinstance(value, Mapping):
        summary: dict[str, JsonValue] = {}
        for key, item in list(value.items())[:20]:
            normalized_key = str(key)
            secret_key = normalized_key.lower().replace("-", "_")
            if any(part in secret_key for part in _SECRET_KEY_PARTS):
                continue
            if isinstance(item, str | int | float | bool) or item is None:
                summary[normalized_key] = item
            else:
                summary[normalized_key] = type(item).__name__
        return summary
    return {"type": type(value).__name__}
