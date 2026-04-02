"""Tests for Kitaru-owned future wrappers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.futures import (
    KitaruArtifactFuture,
    KitaruMapFuture,
    KitaruStepFuture,
    unwrap_kitaru_futures,
)

# ---------------------------------------------------------------------------
# Fake native futures for testing delegation
# ---------------------------------------------------------------------------


def _fake_artifact_future(
    *,
    invocation_id: str = "step-1",
    running: bool = False,
    result: Any = "artifact-handle",
    load_value: Any = "loaded-data",
    chunk_value: Any = "chunk-0",
) -> SimpleNamespace:
    return SimpleNamespace(
        invocation_id=invocation_id,
        running=lambda: running,
        result=lambda: result,
        load=lambda disable_cache=False: (load_value, disable_cache),
        chunk=lambda index: (chunk_value, index),
    )


class _FakeNativeStepFuture:
    """Fake that supports dunder methods Python dispatches on the class."""

    def __init__(
        self,
        *,
        invocation_id: str,
        running: bool,
        result: Any,
        load_value: Any,
        artifacts_value: Any,
        artifact_futures: list[Any],
        output_keys: list[str],
        wait_callback: Any = None,
    ) -> None:
        self.invocation_id = invocation_id
        self._running = running
        self._result = result
        self._load_value = load_value
        self._artifacts_value = artifacts_value
        self._artifact_futures = artifact_futures
        self._output_keys = output_keys
        self._wait_callback = wait_callback

    def running(self) -> bool:
        return self._running

    def wait(self) -> None:
        if self._wait_callback is not None:
            self._wait_callback()

    def result(self) -> Any:
        return self._result

    def load(self, disable_cache: bool = False) -> Any:
        return (self._load_value, disable_cache)

    def artifacts(self) -> Any:
        return self._artifacts_value

    def get_artifact(self, key: str) -> Any:
        if key not in self._output_keys:
            raise KeyError(key)
        return self._artifact_futures[self._output_keys.index(key)]

    def __getitem__(self, key: int | slice) -> Any:
        if isinstance(key, int):
            return self._artifact_futures[key]
        return tuple(self._artifact_futures[key])

    def __iter__(self) -> Any:
        return iter(self._artifact_futures)

    def __len__(self) -> int:
        return len(self._artifact_futures)


def _fake_step_future(
    *,
    invocation_id: str = "step-1",
    running: bool = False,
    result: Any = "step-output",
    load_value: Any = "loaded-step",
    artifacts_value: Any = "raw-artifacts",
    output_keys: list[str] | None = None,
    wait_callback: Any = None,
) -> _FakeNativeStepFuture:
    keys = output_keys or ["output"]
    artifact_futures = [
        _fake_artifact_future(invocation_id=invocation_id) for _ in keys
    ]
    return _FakeNativeStepFuture(
        invocation_id=invocation_id,
        running=running,
        result=result,
        load_value=load_value,
        artifacts_value=artifacts_value,
        artifact_futures=artifact_futures,
        output_keys=keys,
        wait_callback=wait_callback,
    )


def _fake_map_future(
    *,
    step_futures: list[SimpleNamespace] | None = None,
    running: bool = False,
    result: Any = None,
    load_value: Any = None,
) -> SimpleNamespace:
    futures = step_futures or [
        _fake_step_future(),
        _fake_step_future(invocation_id="step-2"),
    ]
    return SimpleNamespace(
        futures=futures,
        running=lambda: running,
        result=lambda: result or ["out-1", "out-2"],
        load=lambda disable_cache=False: load_value or ["val-1", "val-2"],
        unpack=lambda: (
            [
                _fake_artifact_future(invocation_id="s1"),
                _fake_artifact_future(invocation_id="s2"),
            ],
        ),
    )


# ---------------------------------------------------------------------------
# KitaruArtifactFuture
# ---------------------------------------------------------------------------


class TestKitaruArtifactFuture:
    def test_invocation_id_delegates(self) -> None:
        native = _fake_artifact_future(invocation_id="my-step")
        wrapper = KitaruArtifactFuture(native)
        assert wrapper.invocation_id == "my-step"

    def test_running_delegates(self) -> None:
        native = _fake_artifact_future(running=True)
        assert KitaruArtifactFuture(native).running() is True

        native_done = _fake_artifact_future(running=False)
        assert KitaruArtifactFuture(native_done).running() is False

    def test_result_delegates(self) -> None:
        native = _fake_artifact_future(result="my-artifact")
        assert KitaruArtifactFuture(native).result() == "my-artifact"

    def test_load_delegates(self) -> None:
        native = _fake_artifact_future(load_value="data")
        wrapper = KitaruArtifactFuture(native)
        assert wrapper.load() == ("data", False)
        assert wrapper.load(disable_cache=True) == ("data", True)

    def test_chunk_delegates(self) -> None:
        native = _fake_artifact_future(chunk_value="c")
        assert KitaruArtifactFuture(native).chunk(3) == ("c", 3)


# ---------------------------------------------------------------------------
# KitaruStepFuture
# ---------------------------------------------------------------------------


class TestKitaruStepFuture:
    def test_invocation_id_delegates(self) -> None:
        native = _fake_step_future(invocation_id="my-step")
        assert KitaruStepFuture(native).invocation_id == "my-step"

    def test_running_delegates(self) -> None:
        assert KitaruStepFuture(_fake_step_future(running=True)).running() is True
        assert KitaruStepFuture(_fake_step_future(running=False)).running() is False

    def test_wait_delegates(self) -> None:
        called: list[bool] = []
        native = _fake_step_future(wait_callback=lambda: called.append(True))
        KitaruStepFuture(native).wait()
        assert called == [True]

    def test_result_delegates(self) -> None:
        native = _fake_step_future(result="step-result")
        assert KitaruStepFuture(native).result() == "step-result"

    def test_load_delegates(self) -> None:
        native = _fake_step_future(load_value="loaded")
        wrapper = KitaruStepFuture(native)
        assert wrapper.load() == ("loaded", False)
        assert wrapper.load(disable_cache=True) == ("loaded", True)

    def test_artifacts_delegates(self) -> None:
        native = _fake_step_future(artifacts_value="raw")
        assert KitaruStepFuture(native).artifacts() == "raw"

    def test_get_artifact_returns_wrapped_artifact_future(self) -> None:
        native = _fake_step_future(output_keys=["data", "metrics"])
        wrapper = KitaruStepFuture(native)
        artifact = wrapper.get_artifact("data")
        assert isinstance(artifact, KitaruArtifactFuture)

    def test_get_artifact_raises_on_unknown_key(self) -> None:
        native = _fake_step_future(output_keys=["data"])
        with pytest.raises(KeyError):
            KitaruStepFuture(native).get_artifact("missing")

    def test_getitem_int_returns_wrapped_artifact(self) -> None:
        native = _fake_step_future(output_keys=["a", "b"])
        wrapper = KitaruStepFuture(native)
        artifact = wrapper[0]
        assert isinstance(artifact, KitaruArtifactFuture)

    def test_getitem_slice_returns_wrapped_tuple(self) -> None:
        native = _fake_step_future(output_keys=["a", "b", "c"])
        wrapper = KitaruStepFuture(native)
        result = wrapper[0:2]
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(af, KitaruArtifactFuture) for af in result)

    def test_iter_yields_wrapped_artifacts(self) -> None:
        native = _fake_step_future(output_keys=["a", "b"])
        wrapper = KitaruStepFuture(native)
        items = list(wrapper)
        assert len(items) == 2
        assert all(isinstance(af, KitaruArtifactFuture) for af in items)

    def test_len_delegates(self) -> None:
        native = _fake_step_future(output_keys=["a", "b", "c"])
        assert len(KitaruStepFuture(native)) == 3


# ---------------------------------------------------------------------------
# KitaruMapFuture
# ---------------------------------------------------------------------------


class TestKitaruMapFuture:
    def test_futures_returns_wrapped_step_futures(self) -> None:
        native = _fake_map_future()
        wrapper = KitaruMapFuture(native)
        futures = wrapper.futures
        assert len(futures) == 2
        assert all(isinstance(f, KitaruStepFuture) for f in futures)

    def test_futures_returns_new_list_each_call(self) -> None:
        wrapper = KitaruMapFuture(_fake_map_future())
        assert wrapper.futures is not wrapper.futures

    def test_running_delegates(self) -> None:
        assert KitaruMapFuture(_fake_map_future(running=True)).running() is True
        assert KitaruMapFuture(_fake_map_future(running=False)).running() is False

    def test_result_delegates(self) -> None:
        native = _fake_map_future(result=["r1", "r2"])
        assert KitaruMapFuture(native).result() == ["r1", "r2"]

    def test_load_delegates(self) -> None:
        native = _fake_map_future(load_value=["v1", "v2"])
        assert KitaruMapFuture(native).load() == ["v1", "v2"]

    def test_artifacts_delegates(self) -> None:
        native = _fake_map_future(result=["a1", "a2"])
        assert KitaruMapFuture(native).artifacts() == ["a1", "a2"]

    def test_unpack_returns_wrapped_artifact_futures(self) -> None:
        native = _fake_map_future()
        wrapper = KitaruMapFuture(native)
        unpacked = wrapper.unpack()
        assert isinstance(unpacked, tuple)
        assert len(unpacked) == 1
        assert all(isinstance(af, KitaruArtifactFuture) for af in unpacked[0])

    def test_getitem_int_returns_wrapped_step_future(self) -> None:
        native = _fake_map_future()
        wrapper = KitaruMapFuture(native)
        assert isinstance(wrapper[0], KitaruStepFuture)

    def test_getitem_slice_returns_list(self) -> None:
        native = _fake_map_future()
        wrapper = KitaruMapFuture(native)
        result = wrapper[0:2]
        assert isinstance(result, list)
        assert all(isinstance(f, KitaruStepFuture) for f in result)

    def test_iter_yields_wrapped_step_futures(self) -> None:
        wrapper = KitaruMapFuture(_fake_map_future())
        items = list(wrapper)
        assert len(items) == 2
        assert all(isinstance(f, KitaruStepFuture) for f in items)

    def test_len_returns_count(self) -> None:
        assert len(KitaruMapFuture(_fake_map_future())) == 2


# ---------------------------------------------------------------------------
# unwrap_kitaru_futures
# ---------------------------------------------------------------------------


class TestUnwrapKitaruFutures:
    def test_unwraps_step_future(self) -> None:
        native = _fake_step_future()
        wrapper = KitaruStepFuture(native)
        assert unwrap_kitaru_futures(wrapper) is native

    def test_unwraps_artifact_future(self) -> None:
        native = _fake_artifact_future()
        wrapper = KitaruArtifactFuture(native)
        assert unwrap_kitaru_futures(wrapper) is native

    def test_unwraps_map_future(self) -> None:
        native = _fake_map_future()
        wrapper = KitaruMapFuture(native)
        assert unwrap_kitaru_futures(wrapper) is native

    def test_leaves_plain_values_unchanged(self) -> None:
        assert unwrap_kitaru_futures(42) == 42
        assert unwrap_kitaru_futures("hello") == "hello"
        assert unwrap_kitaru_futures(None) is None

    def test_recurses_into_list(self) -> None:
        native = _fake_step_future()
        wrapper = KitaruStepFuture(native)
        result = unwrap_kitaru_futures([wrapper, "plain", 1])
        assert result[0] is native
        assert result[1] == "plain"
        assert result[2] == 1

    def test_recurses_into_tuple(self) -> None:
        native = _fake_step_future()
        wrapper = KitaruStepFuture(native)
        result = unwrap_kitaru_futures((wrapper, "plain"))
        assert isinstance(result, tuple)
        assert result[0] is native
        assert result[1] == "plain"

    def test_recurses_into_dict_values(self) -> None:
        native = _fake_step_future()
        wrapper = KitaruStepFuture(native)
        result = unwrap_kitaru_futures({"future": wrapper, "data": "hello"})
        assert result["future"] is native
        assert result["data"] == "hello"

    def test_nested_structures(self) -> None:
        native = _fake_step_future()
        wrapper = KitaruStepFuture(native)
        result = unwrap_kitaru_futures({"items": [wrapper, (wrapper,)]})
        assert result["items"][0] is native
        assert result["items"][1][0] is native
