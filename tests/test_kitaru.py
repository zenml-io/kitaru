"""Tests for the Kitaru package public API surface."""

import pytest

import kitaru


def test_package_imports() -> None:
    assert kitaru.__name__ == "kitaru"


class TestPublicExports:
    """Verify all public SDK primitives are importable."""

    def test_flow_exists(self) -> None:
        assert hasattr(kitaru, "flow")

    def test_checkpoint_exists(self) -> None:
        assert hasattr(kitaru, "checkpoint")

    def test_wait_exists(self) -> None:
        assert hasattr(kitaru, "wait")

    def test_llm_exists(self) -> None:
        assert hasattr(kitaru, "llm")

    def test_save_exists(self) -> None:
        assert hasattr(kitaru, "save")

    def test_load_exists(self) -> None:
        assert hasattr(kitaru, "load")

    def test_log_exists(self) -> None:
        assert hasattr(kitaru, "log")

    def test_configure_exists(self) -> None:
        assert hasattr(kitaru, "configure")

    def test_connect_exists(self) -> None:
        assert hasattr(kitaru, "connect")

    def test_kitaru_client_exists(self) -> None:
        assert hasattr(kitaru, "KitaruClient")

    def test_all_exports_match(self) -> None:
        expected = {
            "KitaruClient",
            "checkpoint",
            "configure",
            "connect",
            "flow",
            "llm",
            "load",
            "log",
            "save",
            "wait",
        }
        assert set(kitaru.__all__) == expected


class TestPlaceholderBehavior:
    """Verify unimplemented primitives raise NotImplementedError."""

    def test_flow_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="flow"):
            kitaru.flow(lambda: None)

    def test_checkpoint_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="checkpoint"):
            kitaru.checkpoint(lambda: None)

    def test_wait_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="wait"):
            kitaru.wait()

    def test_llm_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="llm"):
            kitaru.llm("hello")

    def test_save_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="save"):
            kitaru.save("name", "value")

    def test_load_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="load"):
            kitaru.load("exec-123", "name")

    def test_log_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="log"):
            kitaru.log(cost=0.01)

    def test_configure_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="configure"):
            kitaru.configure(cache=False)

    def test_connect_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="connect"):
            kitaru.connect("https://example.com")

    def test_client_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="KitaruClient"):
            kitaru.KitaruClient()
