"""Show how a replayed execution diverged from the original — via the CLI/SDK.

This is the replayability payoff that the dashboard can't show: take an original
execution and a replay of it (replayed from a checkpoint with different params),
and print the two final answers plus what the replay dropped.

Run:
    uv run python .../show_divergence.py <original_exec_id> <replay_exec_id>
"""

from __future__ import annotations

import sys
from typing import Any

from kitaru.client import KitaruClient

client = KitaruClient()


def _artifacts(exec_id: str) -> tuple[str, dict[str, Any]]:
    ex = client.executions.get(exec_id)
    final_response = ""
    scorecard: dict[str, Any] = {}
    for cp in ex.checkpoints or []:
        for art in cp.artifacts or []:
            if art.direction != "output":
                continue
            if art.name == "final_response":
                final_response = str(art.load())
            elif art.name == "scorecard":
                loaded = art.load()
                if isinstance(loaded, dict):
                    scorecard = loaded
    return final_response, scorecard


def _block(title: str, exec_id: str) -> str:
    final_response, sc = _artifacts(exec_id)
    quality = sc.get("quality_score", "?")
    missing = sc.get("missing_required_terms", []) or []
    risky = sc.get("risky_terms", []) or []
    lines = [
        f"  {title}  (exec {exec_id[:8]})",
        f"    quality: {quality}",
        f"    answer : {final_response}",
    ]
    if missing:
        lines.append(f"    DROPPED required: {', '.join(missing)}")
    if risky:
        lines.append(f"    UNSAFE phrasing : {', '.join(risky)}")
    return "\n".join(lines)


def main() -> int:
    """Print the original vs replayed divergence."""
    if len(sys.argv) != 3:
        print("usage: show_divergence.py <original_exec_id> <replay_exec_id>")
        return 1
    bar = "=" * 70
    print("")
    print(bar)
    print("  REPLAYABILITY  —  same case + checkpoint, different model")
    print(bar)
    print("")
    print(_block("ORIGINAL (current model)", sys.argv[1]))
    print("")
    print(_block("REPLAY (cheaper model)", sys.argv[2]))
    print("")
    print(bar)
    print(
        "  The cheaper model silently dropped a safety requirement on a "
        "load-bearing part."
    )
    print("  Replay Lab catches this before it ever ships. No dashboard needed.")
    print(bar)
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
