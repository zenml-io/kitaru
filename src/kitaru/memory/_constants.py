"""Constants for Kitaru memory storage and indexing."""

import builtins
import re

# The public API defines ``list()`` which shadows ``builtins.list``.
# Alias the builtin so type annotations resolve correctly under ty.
_list = builtins.list

_MEMORY_ARTIFACT_PREFIX = "kitaru_mem"
_MEMORY_TAG_MARKER = "kitaru:memory"
_MEMORY_TAG_SCOPE_PREFIX = "kitaru:memory:scope:"
_MEMORY_TAG_KEY_PREFIX = "kitaru:memory:key:"
_MEMORY_TAG_SCOPE_TYPE_PREFIX = "kitaru:memory:scope_type:"
_MEMORY_TAG_FLOW_ID_PREFIX = "kitaru:memory:flow_id:"
_MEMORY_SCOPE_TYPE_METADATA_KEY = "kitaru_memory_scope_type"
_MEMORY_DELETED_METADATA_KEY = "kitaru_memory_deleted"
_MEMORY_FLOW_ID_METADATA_KEY = "kitaru_memory_flow_id"
_MEMORY_FLOW_NAME_METADATA_KEY = "kitaru_memory_flow_name"
_MEMORY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._\-/]+$")
_MEMORY_PAGE_SIZE = 100
_MEMORY_VERSION_SORT = "desc:version_number"
_MEMORY_STEP_EXTRA_PREFIX = {"kitaru": {"boundary": "memory"}}
_MEMORY_SCOPE_TYPE_SORT_ORDER: dict[str, int] = {
    "namespace": 0,
    "flow": 1,
    "execution": 2,
}
_COMPACTION_LOG_PREFIX = "_compaction/"
_MEMORY_REINDEX_ISSUE_SAMPLE_LIMIT = 10
