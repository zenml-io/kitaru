#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Best-effort metadata-only observation for insight generation."""

import asyncio
import importlib
import os
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.base import JsonValue


class GenerationEvent(BaseModel):
    """One content-free lifecycle event emitted by the generator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Literal["profiling", "analyst", "editor", "validation"]
    run_id: str = Field(min_length=1, max_length=255)
    stage: Literal["analyst", "editor"] | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class GenerationObserver(Protocol):
    """Receive metadata-only insight generation events."""

    async def record(self, event: GenerationEvent) -> None:
        """Record one event without receiving source or model content."""


async def observe_safely(
    observer: GenerationObserver | None,
    event: GenerationEvent,
    *,
    timeout_seconds: float = 0.1,
) -> None:
    """Record an event without allowing telemetry to affect generation."""
    if observer is None:
        return
    try:
        async with asyncio.timeout(timeout_seconds):
            await observer.record(event)
    except Exception:
        # Telemetry must not alter a generation result.
        return


class LangfuseGenerationObserver:
    """Lazy Langfuse adapter configured for a dedicated project.

    The adapter deliberately accepts only `GenerationEvent`, which makes source
    payloads, prompts, and model responses impossible to pass through this API.
    """

    def __init__(
        self,
        *,
        public_key: str | None = None,
        secret_key: str | None = None,
        host: str | None = None,
    ) -> None:
        """Construct a client using only insight-specific configuration."""
        selected_public_key = public_key or os.environ.get(
            "KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY"
        )
        selected_secret_key = secret_key or os.environ.get(
            "KITARU_INSIGHTS_LANGFUSE_SECRET_KEY"
        )
        selected_host = host or os.environ.get("KITARU_INSIGHTS_LANGFUSE_BASE_URL")
        if not selected_public_key or not selected_secret_key:
            raise ValueError(
                "insight-specific Langfuse public and secret keys are required"
            )
        module: Any = importlib.import_module("langfuse")
        client_type = module.Langfuse
        self._client: Any = client_type(
            public_key=selected_public_key,
            secret_key=selected_secret_key,
            base_url=selected_host,
        )

    def __repr__(self) -> str:
        """Return a credential-free representation."""
        return f"{type(self).__name__}()"

    async def record(self, event: GenerationEvent) -> None:
        """Record an event using the installed Langfuse client's event API."""
        payload = event.model_dump(mode="json")
        await asyncio.to_thread(self._record_sync, payload)

    def _record_sync(self, payload: dict[str, object]) -> None:
        """Bridge the synchronous optional client without blocking generation."""
        metadata = payload["metadata"]
        if not isinstance(metadata, dict):
            raise TypeError("event metadata must be a dictionary")
        observation = self._client.start_observation(
            trace_context={"trace_id": str(payload["run_id"]).replace("-", "")},
            name=f"insight-generation.{payload['name']}",
            as_type="generation" if payload["stage"] is not None else "span",
            metadata={
                "stage": payload["stage"],
                **metadata,
            },
        )
        observation.end()
