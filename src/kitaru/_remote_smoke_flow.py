"""Private importable flow used by release remote-stack smoke tests."""

import logging
import time
import uuid

from kitaru import checkpoint, flow, save

logger = logging.getLogger(__name__)


def build_remote_smoke_marker(run_prefix: str) -> str:
    """Build a unique non-secret marker for one remote smoke execution."""
    normalized_prefix = run_prefix.strip() or "kitaru-remote-smoke"
    safe_prefix = "-".join(normalized_prefix.split())
    timestamp = int(time.time())
    short_id = uuid.uuid4().hex[:8]
    return f"{safe_prefix}-{timestamp}-{short_id}"


@checkpoint
def record_remote_smoke_marker(marker: str) -> str:
    """Persist and log the marker used to prove remote artifact readback."""
    payload = {
        "marker": marker,
        "purpose": "remote-stack-release-smoke",
    }
    logger.info("Kitaru remote stack smoke marker: %s", marker)
    print(f"Kitaru remote stack smoke marker: {marker}")
    save("remote_smoke_marker", payload, type="context")
    return marker


@flow
def remote_stack_release_smoke(marker: str) -> str:
    """Run the smallest useful workflow for remote stack release validation."""
    return record_remote_smoke_marker(marker)
