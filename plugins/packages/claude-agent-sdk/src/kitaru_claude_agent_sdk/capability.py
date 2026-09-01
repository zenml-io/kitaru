#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Public capability errors for the Claude Agent SDK adapter."""

import uuid

from claude_agent_sdk import ResultMessage


class KitaruRecordingError(RuntimeError):
    """Report a Kitaru failure after Claude execution succeeded."""

    def __init__(
        self,
        *,
        terminal_message: ResultMessage,
        session_id: uuid.UUID | None,
        phase: str,
    ) -> None:
        self.terminal_message = terminal_message
        self.session_id = session_id
        self.phase = phase
        self.retry_safe = False
        self.side_effects_possible = True
        super().__init__(
            f"Kitaru recording failed during {phase} after Claude execution; "
            "automatic retry is unsafe because model or tool side effects may "
            "already have occurred."
        )


__all__ = ["KitaruRecordingError"]
