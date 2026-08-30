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
from pathlib import Path
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
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType

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
    # Long strings are drawn from fixed samples rather than generated
    # character by character: a random 5_000-character draw eats so much of
    # Hypothesis's per-example entropy budget that the derandomized "ci"
    # profile fails the data_too_large health check before it finds anything.
    st.sampled_from(["x" * 1_000, "\u00e9" * 5_000, " " * 2_000]),
    # NEW-FINDING-1: "\ud800" (a lone UTF-16 surrogate) survives into
    # ImportedSession/ImportedNode string fields and then blows up
    # model_dump_json() with a PydanticSerializationError. See
    # design/fuzzing/new-findings.md. Excluded until an importer or the
    # ImportedSession/ImportedNode models sanitize lone surrogates.
    st.sampled_from(["", " ", "\x00", "null", "NaN", "Infinity", "-0", "1e999"]),
)


_IDS = st.sampled_from([f"id{i}" for i in range(12)])
_TRACE_IDS = st.sampled_from(["trace0", "trace1", "trace2"])
_PROJECT_IDS = st.sampled_from(["proj-a", "proj-b"])


def _mostly(
    main: SearchStrategy[Any], rare: SearchStrategy[Any], *, one_in: int = 8
) -> SearchStrategy[Any]:
    """Draw from `main`, falling back to `rare` about one draw in `one_in`.

    Identity fields such as trace ids gate the whole import: when they are
    drawn from a uniform mix of valid and hostile values, almost every
    generated file dies at the front door and the deeper normalizing code
    never runs. Weighting keeps hostile values in the search space while
    letting most files reach the code the property is about.
    """
    return st.sampled_from([False] * (one_in - 1) + [True]).flatmap(
        lambda use_rare: rare if use_rare else main
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
    #
    # NEW-FINDING-3: "model" is deliberately kept out of the general key pool
    # and drawn as a string or None below. The importer feeds it straight into
    # `ImportedNode.requested_model` with no type check, so a non-string value
    # such as `[]` escapes `parse()` as a pydantic `ValidationError` instead of
    # a contained `ImportFailure`. See design/fuzzing/new-findings.md.
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
        "usageDetails",
        "costDetails",
        "level",
        "statusMessage",
        "sessionId",
    ]
    return _records_with_keys(
        keys,
        id_key="id",
        parent_key="parentObservationId",
        # Shape detection reads "traceId" on the first record and every later
        # record must carry one too, so a uniformly hostile "traceId" rejects
        # the whole file before any observation is normalized.
        required={
            "traceId": _mostly(_TRACE_IDS, _WEIRD_TEXT),
            "model": st.none() | _WEIRD_TEXT,
        },
    )


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
        keys,
        id_key="span_id",
        parent_key="span_parents",
        parent_is_list=True,
        required={
            "root_span_id": _mostly(_TRACE_IDS, _WEIRD_TEXT),
            "project_id": _mostly(_PROJECT_IDS, _WEIRD_TEXT),
        },
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
        # #905: "total_cost" is left out of the pool because the adversarial
        # value strategy can hand it "NaN", which _decimal() accepts and
        # ImportedNode then rejects with a pydantic ValidationError that
        # escapes parse(). test_non_finite_cost_fails_only_its_record pins
        # that bug; leaving the key in here only makes the property flaky.
        "prompt_tokens",
        "completion_tokens",
        "session_id",
    ]
    return _records_with_keys(
        keys,
        id_key="id",
        parent_key="parent_run_id",
        # A run without a trace id or a project identity is rejected before
        # the run is turned into a node, so both need to be present usually.
        required={
            "trace_id": _mostly(_TRACE_IDS, _WEIRD_TEXT),
            "session_id": _mostly(_PROJECT_IDS, _WEIRD_TEXT),
        },
    )


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
    return _records_with_keys(
        keys,
        id_key="span_id",
        parent_key="parent_span_id",
        required={"trace_id": _mostly(_TRACE_IDS, _WEIRD_TEXT)},
    )


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
    return st.lists(span, min_size=1, max_size=20)


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
    # Every record is validated against ImportedSession, which forbids extra
    # keys and requires status/inputs/outputs/external_id/nodes, so without
    # these the whole file degrades into ImportFailure lines.
    return _records_with_keys(
        keys,
        id_key="external_id",
        parent_key=None,
        required={
            "status": _mostly(
                st.sampled_from([status.value for status in SessionStatus]),
                _WEIRD_TEXT,
            ),
            "inputs": adversarial_json_value(2),
            "outputs": adversarial_json_value(2),
            "nodes": _mostly(_kitaru_jsonl_nodes(), adversarial_json_value(2)),
        },
    )


def _kitaru_jsonl_nodes() -> SearchStrategy[list[dict[str, Any]]]:
    """Build flat indexed node lists that ImportedNode mostly accepts."""
    node = st.fixed_dictionaries(
        {
            "node_type": _mostly(
                st.sampled_from([node_type.value for node_type in NodeType]),
                _WEIRD_TEXT,
            ),
            "name": _WEIRD_TEXT,
            "status": _mostly(
                st.sampled_from([status.value for status in NodeStatus]), _WEIRD_TEXT
            ),
            "inputs": adversarial_json_value(2),
            "outputs": adversarial_json_value(2),
            "attributes": adversarial_json_value(2),
        },
        optional={
            "error": st.none() | _WEIRD_TEXT,
            "started_at": _ISO_TIMES | _WEIRD_TEXT,
            "ended_at": _ISO_TIMES | _WEIRD_TEXT,
            "external_id": _IDS,
            "model": _WEIRD_TEXT,
            "tool_name": _WEIRD_TEXT,
        },
    )
    # The importer rejects any node whose index is unset, so number them.
    return st.lists(node, max_size=4).map(
        lambda nodes: [dict(node, index=index) for index, node in enumerate(nodes)]
    )


def _records_with_keys(
    keys: list[str],
    *,
    id_key: str,
    parent_key: str | None,
    parent_is_list: bool = False,
    required: dict[str, SearchStrategy[Any]] | None = None,
) -> SearchStrategy[list[dict[str, Any]]]:
    """Build records that mostly use an importer's real keys with hostile values.

    Ids are drawn from a small pool so parent references sometimes resolve,
    sometimes dangle, and sometimes form short cycles. Chain length is bounded
    by MAX_PARENT_CHAIN (see the known-bug note above). `required` names the
    identity fields the importer needs before it will build a session; they
    are drawn last so they override anything the optional key pool produced.
    """
    required_fields = required or {}

    @st.composite
    def record(draw: st.DrawFn) -> dict[str, Any]:
        fields = draw(
            st.dictionaries(
                st.sampled_from(keys), adversarial_json_value(3), max_size=len(keys)
            )
        )
        fields[id_key] = draw(_mostly(_IDS, _WEIRD_TEXT))
        if parent_key is not None and draw(st.booleans()):
            parent = draw(_IDS | st.none())
            fields[parent_key] = [parent] if parent_is_list and parent else parent
        for key, strategy in required_fields.items():
            fields[key] = draw(strategy)
        return fields

    # An empty list is an empty file, which every importer rejects up front;
    # `garbage_bytes()` already covers that, so spend these draws on records.
    return st.lists(record(), min_size=1, max_size=min(MAX_PARENT_CHAIN, 30))


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
    """Generate the user-controlled parameter dict.

    Values are weighted toward ones the importers accept. A parameter of the
    wrong type is rejected before a single record is read, so an unweighted
    mix spends the whole budget on parameter validation instead of on the
    record-normalizing code these properties are about. `invalid_params()`
    covers the rejection paths separately.
    """
    return st.fixed_dictionaries(
        {},
        optional={
            "source_instance": _mostly(_PROJECT_IDS, _WEIRD_TEXT),
            "filename": _mostly(st.just("export.jsonl"), _WEIRD_TEXT),
            # Every importer treats join_on as a dotted path or JSON pointer,
            # not an enum, so mix path-shaped values in with the bare names.
            "join_on": _mostly(
                st.none(),
                st.one_of(
                    st.sampled_from(
                        ["trace", "session", "user", "metadata", "bogus", ""]
                    ),
                    _PATH_SELECTORS,
                    st.sampled_from(
                        ["metadata.x", "/metadata/x", "/bad~2escape", "metadata..x"]
                    ),
                ),
            ),
            "join_key": _mostly(st.none(), _WEIRD_TEXT),
            "join_path": _mostly(st.none(), st.one_of(_WEIRD_TEXT, _PATH_SELECTORS)),
            "infer_tool_call_links": st.booleans(),
            "framework": _mostly(st.none(), _WEIRD_TEXT),
            "project_id": _mostly(_PROJECT_IDS, _WEIRD_TEXT),
        },
    )


def invalid_params() -> SearchStrategy[dict[str, Any]]:
    """Generate parameter dicts whose values have the wrong type."""
    return st.fixed_dictionaries(
        {},
        optional={
            "source_instance": adversarial_json_value(2),
            "filename": adversarial_json_value(2),
            "join_on": adversarial_json_value(2),
            "join_key": adversarial_json_value(2),
            "join_path": adversarial_json_value(2),
            "infer_tool_call_links": st.one_of(
                _WEIRD_TEXT, st.integers(), st.none(), st.lists(st.booleans())
            ),
            "framework": adversarial_json_value(2),
            "project_id": adversarial_json_value(2),
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


_SEED_PATH = (
    Path(__file__).resolve().parents[3]
    / "examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl"
)


def _load_seed_lines() -> list[bytes]:
    return [line for line in _SEED_PATH.read_bytes().splitlines() if line.strip()]


_SEED_LINES = _load_seed_lines()


@st.composite
def mutated_seed_lines(draw: st.DrawFn) -> bytes:
    """Take a slice of the real Langfuse export and apply one mutation."""
    start = draw(st.integers(0, max(0, len(_SEED_LINES) - 1)))
    lines = list(_SEED_LINES[start : start + draw(st.integers(1, 40))])
    index = draw(st.integers(0, len(lines) - 1))
    mutation = draw(
        st.sampled_from(
            ["drop_key", "replace_value", "duplicate", "truncate", "shuffle"]
        )
    )
    if mutation == "truncate":
        lines[index] = lines[index][: draw(st.integers(0, len(lines[index])))]
    elif mutation == "duplicate":
        lines.insert(index, lines[index])
    elif mutation == "shuffle":
        lines = draw(st.permutations(lines))
    else:
        record = json.loads(lines[index])
        if record:
            key = draw(st.sampled_from(sorted(record)))
            if mutation == "drop_key":
                del record[key]
            else:
                record[key] = draw(adversarial_json_value(3))
        lines[index] = json.dumps(record).encode()
    return b"\n".join(lines)
