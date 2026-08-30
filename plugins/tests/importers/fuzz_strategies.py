#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Hypothesis strategies for importer fuzzing."""

import json
from types import ModuleType
from typing import Any

from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

import kitaru_braintrust_importer.importer as braintrust
import kitaru_jsonl_importer.importer as kitaru_jsonl
import kitaru_langfuse_importer.importer as langfuse
import kitaru_langsmith_importer.importer as langsmith
import kitaru_logfire_importer.importer as logfire
import kitaru_phoenix_importer.importer as phoenix

IMPORTERS: dict[str, ModuleType] = {
    "langfuse": langfuse,
    "braintrust": braintrust,
    "langsmith": langsmith,
    "logfire": logfire,
    "phoenix": phoenix,
    "jsonl": kitaru_jsonl,
}

# Known-bug exclusions. Widen these when the referenced issue is fixed.
MAX_PARENT_CHAIN = 200  # #905: quadratic ancestor walk + RecursionError

_ISO_TIMES = st.sampled_from(
    ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00.123456+00:00", "1970-01-01T00:00:00Z"]
)
_FINITE_DECIMAL_STRINGS = st.decimals(
    allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6, places=6
).map(str)
_WEIRD_TEXT = st.one_of(
    st.text(),
    st.text(alphabet="²³¹٣٤۵", min_size=1, max_size=4),
    st.text(min_size=1_000, max_size=5_000),
    # NEW-FINDING-1: "\ud800" (a lone UTF-16 surrogate) survives into
    # ImportedSession/ImportedNode string fields and then blows up
    # model_dump_json() with a PydanticSerializationError. See
    # design/fuzzing/new-findings.md. Excluded until an importer or the
    # ImportedSession/ImportedNode models sanitize lone surrogates.
    st.sampled_from(["", " ", "\x00", "null", "NaN", "Infinity", "-0", "1e999"]),
)


def adversarial_json_value(max_depth: int = 4) -> SearchStrategy[Any]:
    """Generate JSON-compatible values biased toward parser edge cases.

    Hypothesis bounds `st.recursive` by total leaf count rather than nesting
    depth, so `max_depth` is spent as a budget of four leaves per level.
    """
    leaves = st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**63), max_value=2**63),
        st.floats(allow_nan=False, allow_infinity=False),
        _WEIRD_TEXT,
        # #905: costs are read as strings; keep them finite until _decimal is fixed.
        _FINITE_DECIMAL_STRINGS,
    )
    return st.recursive(
        leaves,
        lambda children: st.one_of(
            st.lists(children, max_size=5),
            st.dictionaries(st.text(max_size=20), children, max_size=5),
        ),
        max_leaves=max_depth * 4,
    )


def _langfuse_records() -> SearchStrategy[list[dict[str, Any]]]:
    # Token and cost numbers reach the importer under "usageDetails" and
    # "costDetails"; it reads no "usage" or "calculatedTotalCost" key at all.
    keys = [
        "id",
        "traceId",
        "type",
        "name",
        "parentObservationId",
        "startTime",
        "endTime",
        "input",
        "output",
        "metadata",
        "model",
        "usageDetails",
        "costDetails",
        "level",
        "statusMessage",
        "sessionId",
    ]
    return _records_with_keys(keys, id_key="id", parent_key="parentObservationId")


def _braintrust_records() -> SearchStrategy[list[dict[str, Any]]]:
    # "project_id" is compared across the rows of one trace, and conflicting
    # values raise InvalidImport, so it belongs in the pool.
    #
    # NEW-FINDING-2: "span_parents" is deliberately excluded from the general
    # key pool below. importer.py does `row.get("span_parents") or []` and
    # then iterates the result directly (no type check), so any truthy
    # non-list value (bool, str, dict, number) crashes with `TypeError:
    # 'bool' object is not iterable` instead of a contained ImportFailure.
    # See design/fuzzing/new-findings.md. `parent_key="span_parents"` below
    # still exercises it, but only ever with a list or an absent key.
    keys = [
        "id",
        "span_id",
        "root_span_id",
        "input",
        "output",
        "metadata",
        "metrics",
        "span_attributes",
        "created",
        "error",
        "project_id",
    ]
    return _records_with_keys(
        keys, id_key="span_id", parent_key="span_parents", parent_is_list=True
    )


def _langsmith_records() -> SearchStrategy[list[dict[str, Any]]]:
    keys = [
        "id",
        "trace_id",
        "parent_run_id",
        "name",
        "run_type",
        "inputs",
        "outputs",
        "start_time",
        "end_time",
        "error",
        "extra",
        "total_cost",
        "prompt_tokens",
        "completion_tokens",
        "session_id",
    ]
    return _records_with_keys(keys, id_key="id", parent_key="parent_run_id")


def _logfire_records() -> SearchStrategy[list[dict[str, Any]]]:
    keys = [
        "span_id",
        "trace_id",
        "parent_span_id",
        "span_name",
        "start_timestamp",
        "end_timestamp",
        "attributes",
        "is_exception",
        "exception_message",
        "level",
        "message",
        "otel_scope_name",
        "service_name",
    ]
    return _records_with_keys(keys, id_key="span_id", parent_key="parent_span_id")


def _phoenix_records() -> SearchStrategy[list[dict[str, Any]]]:
    attr_key = st.one_of(
        st.sampled_from(
            [
                "llm.input_messages.0.message.role",
                "llm.output_messages.0.message.content",
                "llm.token_count.total",
                "llm.model_name",
                "input.value",
                "output.value",
                "tool.name",
                "openinference.span.kind",
            ]
        ),
        # #905: superscript digits crash _indexed_messages; ASCII-only until fixed.
        st.from_regex(
            r"llm\.input_messages\.[0-9]{1,2}\.message\.role", fullmatch=True
        ),
        st.text(max_size=40),
    )
    span = st.fixed_dictionaries(
        {
            "context": st.fixed_dictionaries(
                {
                    "trace_id": st.text(max_size=16),
                    "span_id": st.text(min_size=1, max_size=16),
                }
            )
        },
        optional={
            "name": _WEIRD_TEXT,
            "span_kind": st.sampled_from(["LLM", "TOOL", "CHAIN", "AGENT", "bogus"]),
            "parent_id": st.none() | st.text(max_size=16),
            "start_time": _ISO_TIMES | _WEIRD_TEXT,
            "end_time": _ISO_TIMES | _WEIRD_TEXT,
            "attributes": st.dictionaries(
                attr_key, adversarial_json_value(2), max_size=8
            ),
            "status_code": st.sampled_from(["OK", "ERROR", "UNSET", ""]),
        },
    )
    return st.lists(span, max_size=20)


def _kitaru_jsonl_records() -> SearchStrategy[list[dict[str, Any]]]:
    keys = [
        "status",
        "name",
        "inputs",
        "outputs",
        "error",
        "started_at",
        "ended_at",
        "external_id",
        "metadata",
        "framework",
        "nodes",
    ]
    return _records_with_keys(keys, id_key="external_id", parent_key=None)


def _records_with_keys(
    keys: list[str],
    *,
    id_key: str,
    parent_key: str | None,
    parent_is_list: bool = False,
) -> SearchStrategy[list[dict[str, Any]]]:
    """Build records that mostly use an importer's real keys with hostile values.

    Ids are drawn from a small pool so parent references sometimes resolve,
    sometimes dangle, and sometimes form short cycles. Chain length is bounded
    by MAX_PARENT_CHAIN (see the known-bug note above).
    """
    ids = st.sampled_from([f"id{i}" for i in range(12)])

    @st.composite
    def record(draw: st.DrawFn) -> dict[str, Any]:
        fields = draw(
            st.dictionaries(
                st.sampled_from(keys), adversarial_json_value(3), max_size=len(keys)
            )
        )
        fields[id_key] = draw(ids | _WEIRD_TEXT)
        if parent_key is not None and draw(st.booleans()):
            parent = draw(ids | st.none())
            fields[parent_key] = [parent] if parent_is_list and parent else parent
        return fields

    return st.lists(record(), max_size=min(MAX_PARENT_CHAIN, 30))


_RECORD_STRATEGIES = {
    "langfuse": _langfuse_records,
    "braintrust": _braintrust_records,
    "langsmith": _langsmith_records,
    "logfire": _logfire_records,
    "phoenix": _phoenix_records,
    "jsonl": _kitaru_jsonl_records,
}


def records_for(name: str) -> SearchStrategy[list[dict[str, Any]]]:
    """Return the record strategy for one importer."""
    return _RECORD_STRATEGIES[name]()


def encode_records(name: str, records: list[dict[str, Any]]) -> bytes:
    """Serialize records in the container shape each importer accepts."""
    if name in {"langfuse", "logfire", "jsonl"}:
        return b"\n".join(json.dumps(r).encode() for r in records)
    # braintrust, langsmith, phoenix accept a JSON array (langfuse also accepts one).
    return json.dumps(records).encode()


_PATH_SELECTORS = st.from_regex(r"(/?[a-z_]{1,8}){1,4}", fullmatch=True)


def importer_params() -> SearchStrategy[dict[str, Any]]:
    """Generate the user-controlled parameter dict."""
    return st.fixed_dictionaries(
        {},
        optional={
            "source_instance": _WEIRD_TEXT,
            "filename": _WEIRD_TEXT,
            # Every importer treats join_on as a dotted path or JSON pointer,
            # not an enum, so mix path-shaped values in with the bare names.
            "join_on": st.one_of(
                st.sampled_from(["trace", "session", "user", "metadata", "bogus", ""]),
                _PATH_SELECTORS,
                st.sampled_from(
                    ["metadata.x", "/metadata/x", "/bad~2escape", "metadata..x"]
                ),
            ),
            "join_key": _WEIRD_TEXT,
            "join_path": st.one_of(_WEIRD_TEXT, _PATH_SELECTORS),
            "infer_tool_call_links": st.booleans() | _WEIRD_TEXT,
            "framework": _WEIRD_TEXT,
            "project_id": _WEIRD_TEXT,
        },
    )


def garbage_bytes() -> SearchStrategy[bytes]:
    """Generate arbitrary bytes, including invalid UTF-8 and near-JSON."""
    return st.one_of(
        st.binary(max_size=2_000),
        st.text(max_size=2_000).map(str.encode),
        st.sampled_from(
            [
                b"",
                b"\n\n",
                b"{",
                b"[",
                b"[{}]",
                b"{}\n{",
                b"\xff\xfe",
                b"null",
                b'"str"',
            ]
        ),
    )
