"""Tests for the shared source-alias module."""

from __future__ import annotations

from kitaru._source_aliases import (
    CHECKPOINT_SOURCE_ALIAS_PREFIX,
    PIPELINE_SOURCE_ALIAS_PREFIX,
    build_checkpoint_source_alias,
    build_pipeline_source_alias,
    normalize_aliases_in_text,
    normalize_checkpoint_name,
    normalize_flow_name,
)


class TestBuildPipelineSourceAlias:
    def test_normal_name(self) -> None:
        assert (
            build_pipeline_source_alias("my_flow") == "__kitaru_pipeline_source_my_flow"
        )

    def test_name_with_special_chars(self) -> None:
        result = build_pipeline_source_alias("my-flow.v2")
        assert result == "__kitaru_pipeline_source_my_flow_v2"

    def test_digit_leading_name(self) -> None:
        result = build_pipeline_source_alias("3rd_attempt")
        assert result == "__kitaru_pipeline_source_flow_3rd_attempt"

    def test_all_special_chars_become_underscores(self) -> None:
        result = build_pipeline_source_alias("---")
        assert result == "__kitaru_pipeline_source____"

    def test_truly_empty_name_uses_fallback(self) -> None:
        result = build_pipeline_source_alias("")
        assert result == "__kitaru_pipeline_source_flow"

    def test_preserves_prefix_contract(self) -> None:
        result = build_pipeline_source_alias("test")
        assert result.startswith(PIPELINE_SOURCE_ALIAS_PREFIX)


class TestBuildCheckpointSourceAlias:
    def test_normal_name(self) -> None:
        result = build_checkpoint_source_alias("fetch_data")
        assert result == "__kitaru_checkpoint_source_fetch_data"

    def test_name_with_special_chars(self) -> None:
        result = build_checkpoint_source_alias("fetch-data.v2")
        assert result == "__kitaru_checkpoint_source_fetch_data_v2"

    def test_digit_leading_name(self) -> None:
        result = build_checkpoint_source_alias("1st_step")
        assert result == "__kitaru_checkpoint_source_checkpoint_1st_step"

    def test_all_special_chars_become_underscores(self) -> None:
        result = build_checkpoint_source_alias("!!!")
        assert result == "__kitaru_checkpoint_source____"

    def test_truly_empty_name_uses_fallback(self) -> None:
        result = build_checkpoint_source_alias("")
        assert result == "__kitaru_checkpoint_source_checkpoint"

    def test_preserves_prefix_contract(self) -> None:
        result = build_checkpoint_source_alias("test")
        assert result.startswith(CHECKPOINT_SOURCE_ALIAS_PREFIX)


class TestNormalizeFlowName:
    def test_strips_pipeline_prefix(self) -> None:
        assert normalize_flow_name("__kitaru_pipeline_source_my_flow") == "my_flow"

    def test_leaves_plain_name(self) -> None:
        assert normalize_flow_name("my_flow") == "my_flow"

    def test_none_returns_none(self) -> None:
        assert normalize_flow_name(None) is None

    def test_empty_returns_none(self) -> None:
        assert normalize_flow_name("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert normalize_flow_name("   ") is None

    def test_accepts_non_string_objects(self) -> None:
        """The function accepts ``object | None`` per its signature."""
        assert normalize_flow_name(42) == "42"


class TestNormalizeCheckpointName:
    def test_strips_checkpoint_prefix(self) -> None:
        result = normalize_checkpoint_name("__kitaru_checkpoint_source_fetch_data")
        assert result == "fetch_data"

    def test_leaves_plain_name(self) -> None:
        assert normalize_checkpoint_name("fetch_data") == "fetch_data"


class TestNormalizeAliasesInText:
    def test_replaces_pipeline_alias(self) -> None:
        text = "Initiating run for `__kitaru_pipeline_source_my_flow`."
        assert normalize_aliases_in_text(text) == "Initiating run for `my_flow`."

    def test_replaces_checkpoint_alias(self) -> None:
        text = "Step `__kitaru_checkpoint_source_fetch_data` started."
        assert normalize_aliases_in_text(text) == "Step `fetch_data` started."

    def test_replaces_multiple_aliases(self) -> None:
        text = "__kitaru_pipeline_source_flow_a and __kitaru_checkpoint_source_step_b"
        assert normalize_aliases_in_text(text) == "flow_a and step_b"

    def test_leaves_unrelated_text(self) -> None:
        text = "Hello world, no aliases here."
        assert normalize_aliases_in_text(text) == text

    def test_empty_string(self) -> None:
        assert normalize_aliases_in_text("") == ""
