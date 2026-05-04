"""`publish_summary` — Discord-shaped webhook publish, host-side.

Resolves the `webhook-token` secret on the host and POSTs to
`http://webhook.local/webhooks/<id>` with `Authorization: Bot <token>`.
Returns the typed `{message_id, posted_at}` envelope the mock issues.
"""

import json
import urllib.request

import kitaru

from .schemas import PublishSummaryArgs, PublishSummaryResult

# Host-side `exec_service` reaches the mock via the host-bound port the
# runner publishes (Docker network aliases only resolve inside the
# `agent_factory` network). In production this would be the team's real
# webhook endpoint.
from mocks.runner import HOST_PORT

_WEBHOOK_BASE_URL = f"http://localhost:{HOST_PORT}"


def publish_summary(args: PublishSummaryArgs) -> PublishSummaryResult:
    token = kitaru.get_secret("webhook-token").values["value"]
    payload = json.dumps({"content": args.content}).encode("utf-8")
    request = urllib.request.Request(
        f"{_WEBHOOK_BASE_URL}/webhooks/{args.webhook_id}",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bot {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        body = json.loads(response.read())
    return PublishSummaryResult(
        message_id=body["message_id"],
        posted_at=body["posted_at"],
    )
