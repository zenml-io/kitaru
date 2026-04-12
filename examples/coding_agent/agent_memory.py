"""Cross-session memory and conversation compaction for the coding agent.

Memory is flow-scoped (derived from the flow function name at runtime),
so entries written by one execution are visible to subsequent ones.

- **Session context** — past task summaries loaded at flow start.
- **LLM memory tools** — ``remember`` / ``recall`` / ``list_memories``
  executed in the flow body (not inside checkpoints, where memory ops
  are forbidden).
- **Conversation compaction** — when the message list grows past a
  threshold, older messages are summarized via
  ``KitaruClient.memories.compact()`` and replaced with a single summary.

Inside a ``@flow``, ``memory.list()`` / ``memory.get()`` return lazy
``OutputArtifact`` references.  To use the data you must pass them into
a ``@checkpoint`` where ZenML materializes them.
"""

import json
import os
from typing import Any

import kitaru
from kitaru import KitaruClient, checkpoint, memory

COMPACT_THRESHOLD: int = max(
    6, int(os.environ.get("CODING_AGENT_COMPACT_THRESHOLD", "20"))
)

COMPACT_KEEP_RECENT: int = 6

# Tools that run directly in the flow body (memory ops are forbidden
# inside checkpoints, and hand_back/ask_user need kitaru.wait).
FLOW_BODY_TOOLS = frozenset(
    {"hand_back", "ask_user", "remember", "recall", "list_memories"}
)


# ---------------------------------------------------------------------------
# Memory-entry helpers
# ---------------------------------------------------------------------------


def _entry_key(entry: Any) -> str:
    """Extract the key from a materialized memory entry."""
    if hasattr(entry, "key"):
        return str(entry.key)
    if isinstance(entry, dict):
        return str(entry["key"])
    if isinstance(entry, (tuple, list)) and len(entry) > 0:
        return str(entry[0])
    raise TypeError(f"Cannot extract key from {type(entry)!r}")


def _active_scope() -> str:
    """Return the scope name from the currently configured memory scope.

    Must be called after ``memory.configure()`` in the flow body.
    Uses ``KitaruClient.memories.scopes()`` is not needed — the scope
    is whatever ``memory.configure(scope_type="flow")`` resolved to,
    which is the flow function name.
    """
    # The flow function is always named "coding_agent" in this example,
    # but we read it from the internal context so it stays in sync if
    # the function is ever renamed.
    from kitaru.memory import _CURRENT_MEMORY_SCOPE

    scope = _CURRENT_MEMORY_SCOPE.get()
    if scope is not None:
        return scope.scope
    raise RuntimeError("memory.configure() must be called before _active_scope()")


# ---------------------------------------------------------------------------
# Session context (load / save)
# ---------------------------------------------------------------------------


@checkpoint
def _build_session_context(
    compacted_context: Any,
    task_values: list[Any],
) -> str:
    """Materialize memory artifacts into a session context string.

    Runs as a checkpoint so that the ``OutputArtifact`` references from
    ``memory.get()`` are resolved into real Python objects.

    Returns an empty string when there is no prior context.
    """
    parts: list[str] = []

    if compacted_context:
        parts.append(
            f"Compacted context from earlier in this session:\n{compacted_context}"
        )

    summaries = [f"- {v}" for v in task_values if v]
    if summaries:
        parts.append("Summary of recent past tasks:\n" + "\n".join(summaries))

    if not parts:
        return ""

    return (
        "Context from previous sessions (use this to stay consistent "
        "with the user's preferences and prior work):\n\n" + "\n\n".join(parts)
    )


@checkpoint
def _extract_task_keys(entries: list[Any]) -> list[str]:
    """Extract keys starting with ``tasks/`` from memory entries.

    Runs as a checkpoint so the ``OutputArtifact`` from
    ``memory.list()`` is materialized first.
    """
    task_keys = []
    for entry in entries:
        key = _entry_key(entry)
        if key.startswith("tasks/"):
            task_keys.append(key)
    return task_keys[-5:]


def load_session_context() -> str:
    """Load past session context from flow-scoped memory.

    Returns an empty string on the first execution.  Uses checkpoints
    to materialize the lazy ``OutputArtifact`` values that
    ``memory.list()`` / ``memory.get()`` return inside a flow.
    """
    entries = memory.list()
    compacted_context = memory.get("sessions/compacted_context")

    task_keys: list[str] = _extract_task_keys(entries, id="extract_task_keys").load()
    task_values = [memory.get(key) for key in task_keys]

    return _build_session_context(
        compacted_context, task_values, id="build_session_context"
    ).load()


def save_task_summary(summary: str, task_number: int) -> None:
    """Persist a task summary to flow-scoped memory."""
    memory.set(f"tasks/task_{task_number}", summary)


# ---------------------------------------------------------------------------
# Conversation compaction
# ---------------------------------------------------------------------------


def compact_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Replace older messages with an LLM-generated summary.

    Keeps the system message and the last ``COMPACT_KEEP_RECENT``
    messages.  Everything in between is summarized via
    ``KitaruClient.memories.compact()`` and injected as a single
    user message.

    Returns *messages* unchanged when below the threshold or when
    compaction fails (e.g. no model configured).
    """
    if len(messages) <= COMPACT_THRESHOLD:
        return messages

    system_msg = messages[0] if messages[0]["role"] == "system" else None
    body_start = 1 if system_msg else 0
    tail_start = max(body_start, len(messages) - COMPACT_KEEP_RECENT)

    old_messages = messages[body_start:tail_start]
    recent_messages = messages[tail_start:]

    if not old_messages:
        return messages

    scope = _active_scope()

    serialized = "\n".join(
        f"[{msg.get('role', '?')}] {(msg.get('content') or '')[:200]}"
        for msg in old_messages
    )

    try:
        client = KitaruClient()
        # Write directly via the client API (not memory.set) to avoid
        # the OutputArtifact indirection — compact() reads immediately.
        client.memories.set(
            "sessions/pending_compact",
            serialized,
            scope=scope,
            scope_type="flow",
        )
        result = client.memories.compact(
            scope=scope,
            key="sessions/pending_compact",
            target_key="sessions/compacted_context",
            source_mode="current",
            instruction=(
                "Summarize this conversation history into a concise "
                "context paragraph. Preserve key decisions, user "
                "preferences, file paths, and task progress. Omit "
                "tool call details and verbose outputs."
            ),
        )
        summary = client.artifacts.get(result.entry.artifact_id).load()
    except Exception:
        kitaru.log(compaction_status="skipped", reason="compact call failed")
        return messages

    kitaru.log(
        compaction_status="compacted",
        messages_compacted=len(old_messages),
        messages_remaining=len(recent_messages) + 2,
    )

    compacted: list[dict[str, Any]] = []
    if system_msg:
        compacted.append(system_msg)
    compacted.append(
        {
            "role": "user",
            "content": (f"[Earlier conversation compacted into summary]\n{summary}"),
        }
    )
    compacted.extend(recent_messages)
    return compacted


# ---------------------------------------------------------------------------
# Flow-body memory tool dispatch
#
# These use memory.get/set/list which return OutputArtifacts inside a
# flow.  Recall and list_memories need checkpoints to materialize the
# artifacts before stringifying them for the tool response.
# ---------------------------------------------------------------------------


@checkpoint
def _resolve_recall(value: Any, key: str) -> str:
    """Materialize a memory.get() result into a string."""
    if value is None:
        return f"No memory found for key: {key}"
    return str(value)


@checkpoint
def _resolve_list(entries: list[Any], prefix: str) -> str:
    """Materialize memory.list() entries into a key listing."""
    keys = [_entry_key(entry) for entry in entries]
    if prefix:
        keys = [key for key in keys if key.startswith(prefix)]
    return "\n".join(keys) if keys else "No memories stored."


def handle_remember(
    tool_call_id: str, args: dict[str, Any], step_id: int
) -> dict[str, Any]:
    """Execute ``remember`` in the flow body and return a tool message."""
    key = args.get("key", "")
    value = args.get("value", "")
    memory.set(key, value)
    kitaru.log(memory_set=key)
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": json.dumps({"key": key, "status": "saved"}),
    }


def handle_recall(
    tool_call_id: str, args: dict[str, Any], step_id: int
) -> dict[str, Any]:
    """Execute ``recall`` in the flow body and return a tool message."""
    key = args.get("key", "")
    raw_value = memory.get(key)
    content = _resolve_recall(raw_value, key, id=f"recall_{step_id}").load()
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": content,
    }


def handle_list_memories(
    tool_call_id: str, args: dict[str, Any], step_id: int
) -> dict[str, Any]:
    """Execute ``list_memories`` in the flow body and return a tool message."""
    prefix = args.get("prefix", "")
    entries = memory.list()
    content = _resolve_list(entries, prefix, id=f"list_mem_{step_id}").load()
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": content,
    }
