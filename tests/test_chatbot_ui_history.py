import importlib
import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import Any


@dataclass
class FakeArtifact:
    name: str
    value: list[dict[str, str]]

    def load(self) -> list[dict[str, str]]:
        return self.value


class _FakeComponent:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __enter__(self) -> "_FakeComponent":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def change(self, *args: Any, **kwargs: Any) -> None:
        pass

    def click(self, *args: Any, **kwargs: Any) -> None:
        pass

    def load(self, *args: Any, **kwargs: Any) -> None:
        pass

    def launch(self, *args: Any, **kwargs: Any) -> None:
        pass

    def submit(self, *args: Any, **kwargs: Any) -> None:
        pass


class _FakeTheme:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def set(self, *args: Any, **kwargs: Any) -> "_FakeTheme":
        return self


class _FakeStatus:
    def __init__(self, is_finished: bool = False) -> None:
        self.is_finished = is_finished


class _FakeKitaruClient:
    def __init__(self) -> None:
        self.artifacts = SimpleNamespace(list=lambda *args, **kwargs: [])
        self.executions = SimpleNamespace(
            input=lambda *args, **kwargs: None,
            list=lambda *args, **kwargs: [],
        )


def _fake_gradio_module() -> ModuleType:
    module = ModuleType("gradio")
    module_any: Any = module
    module_any.Blocks = _FakeComponent
    module_any.Button = _FakeComponent
    module_any.Chatbot = _FakeComponent
    module_any.Column = _FakeComponent
    module_any.Dropdown = _FakeComponent
    module_any.HTML = _FakeComponent
    module_any.Markdown = _FakeComponent
    module_any.Row = _FakeComponent
    module_any.State = _FakeComponent
    module_any.Textbox = _FakeComponent
    module_any.skip = lambda: None
    module_any.themes = SimpleNamespace(
        Color=_FakeTheme,
        Default=_FakeTheme,
        GoogleFont=lambda name: name,
        sizes=SimpleNamespace(radius_md="radius_md"),
    )
    return module


def _fake_kitaru_client_module() -> ModuleType:
    module = ModuleType("kitaru.client")
    module_any: Any = module
    module_any.ArtifactRef = object
    module_any.Execution = object
    module_any.ExecutionStatus = SimpleNamespace(
        COMPLETED=_FakeStatus(is_finished=True),
        RUNNING=_FakeStatus(),
        WAITING=_FakeStatus(),
    )
    module_any.KitaruClient = _FakeKitaruClient
    return module


def _import_ui_with_fakes(monkeypatch: Any) -> ModuleType:
    sys.modules.pop("examples.chatbot.ui", None)
    monkeypatch.setitem(sys.modules, "gradio", _fake_gradio_module())
    monkeypatch.setitem(sys.modules, "kitaru.client", _fake_kitaru_client_module())
    module = importlib.import_module("examples.chatbot.ui")
    sys.modules.pop("examples.chatbot.ui", None)
    return module


def test_load_history_retries_when_history_is_shorter_than_min_length(
    monkeypatch: Any,
) -> None:
    ui = _import_ui_with_fakes(monkeypatch)
    stale = [
        {"role": "assistant", "content": "Hello."},
        {"role": "user", "content": "First reply."},
    ]
    fresh = [
        *stale,
        {"role": "assistant", "content": "Second reply."},
    ]
    fetched: list[tuple[str, str]] = []

    def list_artifacts(exec_id: str, *, name: str) -> list[FakeArtifact]:
        fetched.append((exec_id, name))
        return [FakeArtifact("history", fresh)]

    ui.client.artifacts.list = list_artifacts

    result = ui._load_history(
        exec_id="exec-1",
        arts=[FakeArtifact("history", stale)],
        retries=2,
        retry_sleep=0,
        min_length=len(fresh),
    )

    assert result == fresh
    assert fetched == [("exec-1", "history")]


def test_load_history_returns_empty_instead_of_short_stale_history(
    monkeypatch: Any,
) -> None:
    ui = _import_ui_with_fakes(monkeypatch)
    stale = [{"role": "assistant", "content": "Hello."}]
    ui.client.artifacts.list = lambda *args, **kwargs: [FakeArtifact("history", stale)]

    result = ui._load_history(
        exec_id="exec-1",
        arts=[FakeArtifact("history", stale)],
        retries=2,
        retry_sleep=0,
        min_length=2,
    )

    assert result == []


def test_respond_renders_wait_question_when_next_history_is_missing(
    monkeypatch: Any,
) -> None:
    ui: Any = _import_ui_with_fakes(monkeypatch)
    seen_min_lengths: list[int] = []
    history = [{"role": "assistant", "content": "Hello."}]

    def load_history(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
        seen_min_lengths.append(kwargs["min_length"])
        return []

    ui._load_history = load_history
    ui._poll_until_ready = lambda exec_id: SimpleNamespace(
        exec_id=exec_id,
        artifacts=[],
        pending_wait=SimpleNamespace(
            wait_id="next-wait",
            question="What should I do next?",
        ),
    )

    updates = list(
        ui.respond("Can you help?", history, {"exec_id": "exec-1", "wait_id": "wait-1"})
    )

    assert seen_min_lengths == [3]
    assert updates[-1][0] == [
        *history,
        {"role": "user", "content": "Can you help?"},
        {"role": "assistant", "content": "What should I do next?"},
    ]
    assert updates[-1][2].interactive is True


def test_respond_falls_back_to_pending_when_next_wait_has_no_question(
    monkeypatch: Any,
) -> None:
    ui: Any = _import_ui_with_fakes(monkeypatch)
    history = [{"role": "assistant", "content": "Hello."}]
    ui._load_history = lambda *args, **kwargs: []
    ui._poll_until_ready = lambda exec_id: SimpleNamespace(
        exec_id=exec_id,
        artifacts=[],
        pending_wait=SimpleNamespace(wait_id="next-wait"),
    )

    updates = list(
        ui.respond("Can you help?", history, {"exec_id": "exec-1", "wait_id": "wait-1"})
    )

    assert updates[-1][0] == [
        *history,
        {"role": "user", "content": "Can you help?"},
    ]
    assert updates[-1][2].interactive is True


def test_respond_accepts_pending_history_length_when_conversation_ended(
    monkeypatch: Any,
) -> None:
    ui: Any = _import_ui_with_fakes(monkeypatch)
    seen_min_lengths: list[int] = []
    history = [{"role": "assistant", "content": "Hello."}]

    def load_history(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
        seen_min_lengths.append(kwargs["min_length"])
        return []

    ui._load_history = load_history
    ui._poll_until_ready = lambda exec_id: SimpleNamespace(
        exec_id=exec_id,
        artifacts=[],
        pending_wait=None,
    )

    updates = list(
        ui.respond("Goodbye", history, {"exec_id": "exec-1", "wait_id": "wait-1"})
    )

    assert seen_min_lengths == [2]
    assert updates[-1][0] == [*history, {"role": "user", "content": "Goodbye"}]
    assert updates[-1][3] == "Conversation ended."
