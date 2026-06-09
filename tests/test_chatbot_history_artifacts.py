from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from examples.chatbot.history_artifacts import (
    load_longest_usable_history,
    normalize_history,
)


@dataclass
class FakeArtifact:
    name: str
    value: Any = None
    metadata: dict[str, Any] | None = None
    error: Exception | None = None

    def load(self) -> Any:
        if self.error is not None:
            raise self.error
        return self.value


def test_selects_longest_loaded_history_without_length_metadata() -> None:
    short_history = [{"role": "assistant", "content": "hello"}]
    longer_history = [
        {"role": "assistant", "content": "hello"},
        {"role": "user", "content": "hi"},
    ]

    result = load_longest_usable_history(
        [
            FakeArtifact("history", short_history, metadata={"length": 100}),
            FakeArtifact("history", longer_history, metadata={}),
        ]
    )

    assert result == longer_history


def test_equal_length_history_candidates_keep_first_artifact() -> None:
    first_history = [{"role": "assistant", "content": "first"}]
    second_history = [{"role": "assistant", "content": "second"}]

    result = load_longest_usable_history(
        [
            FakeArtifact("history", first_history),
            FakeArtifact("history", second_history),
        ]
    )

    assert result == first_history


def test_skips_history_candidate_with_unsupported_message_role() -> None:
    usable_history = [{"role": "assistant", "content": "What can I help with?"}]

    result = load_longest_usable_history(
        [
            FakeArtifact(
                "history",
                [
                    {"role": "assistant", "content": "Valid start."},
                    {"role": "system", "content": "Not usable by Gradio chat."},
                ],
            ),
            FakeArtifact("history", usable_history),
        ]
    )

    assert result == usable_history


def test_skips_unloadable_and_malformed_history_candidates() -> None:
    usable_history = [
        {"role": "assistant", "content": "What can I help with?"},
        {"role": "user", "content": "A deploy question."},
    ]

    result = load_longest_usable_history(
        [
            FakeArtifact("history", error=RuntimeError("not ready yet")),
            FakeArtifact("history", [{"role": "assistant"}]),
            FakeArtifact(
                "not-history", [{"role": "assistant", "content": "ignore me"}]
            ),
            FakeArtifact("history", usable_history),
        ]
    )

    assert result == usable_history


def test_returns_empty_when_no_history_candidate_is_usable() -> None:
    result = load_longest_usable_history(
        [
            FakeArtifact("history", error=RuntimeError("boom")),
            FakeArtifact("history", [{"content": "missing role"}]),
            FakeArtifact("not-history", [{"role": "assistant", "content": "ignored"}]),
        ]
    )

    assert result == []


def test_normalize_history_accepts_dicts_and_objects_and_coerces_values() -> None:
    raw = [
        {"role": "assistant", "content": 42},
        SimpleNamespace(role="user", content="hello"),
    ]

    assert normalize_history(raw) == [
        {"role": "assistant", "content": "42"},
        {"role": "user", "content": "hello"},
    ]


def test_normalize_history_accepts_generator_input() -> None:
    raw = (
        {"role": role, "content": content}
        for role, content in [("assistant", "hello"), ("user", "hi")]
    )

    assert normalize_history(raw) == [
        {"role": "assistant", "content": "hello"},
        {"role": "user", "content": "hi"},
    ]


def test_normalize_history_rejects_message_missing_role_or_content() -> None:
    assert normalize_history([{"role": "assistant"}]) is None
    assert normalize_history([SimpleNamespace(role="user")]) is None


def test_normalize_history_rejects_unsupported_roles() -> None:
    assert (
        normalize_history([{"role": "system", "content": "hidden instruction"}]) is None
    )
    assert (
        normalize_history([SimpleNamespace(role="tool", content="raw output")]) is None
    )
