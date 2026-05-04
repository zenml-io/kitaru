"""Data models for Kitaru memory."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from kitaru.memory._constants import _list

MemoryScopeType = Literal["namespace", "flow", "execution"]
_MemoryScopeType = MemoryScopeType
_MemoryCompactionSourceMode = Literal["current", "history"]


class MemoryEntry(BaseModel):
    """A single persisted memory version."""

    key: str
    value_type: str
    version: int
    scope: str
    scope_type: MemoryScopeType
    created_at: datetime
    is_deleted: bool
    artifact_id: str
    execution_id: str | None
    flow_id: str | None = None
    flow_name: str | None = None

    model_config = ConfigDict(frozen=True)


class MemoryScopeInfo(BaseModel):
    """Summary of one discovered memory scope."""

    scope: str
    scope_type: MemoryScopeType
    entry_count: int

    model_config = ConfigDict(frozen=True)


class PurgeResult(BaseModel):
    """Result of a memory purge operation."""

    versions_deleted: int
    keys_affected: int
    scope: str
    scope_type: MemoryScopeType

    model_config = ConfigDict(frozen=True)


class CompactionRecord(BaseModel):
    """Audit log entry for one compaction or purge operation."""

    operation: Literal["compact", "purge"]
    scope: str
    scope_type: MemoryScopeType
    timestamp: datetime
    source_keys: _list[str]
    source_versions: _list[int]
    target_key: str | None
    target_version: int | None
    instruction: str | None
    model: str | None
    source_mode: _MemoryCompactionSourceMode | None = None
    keys_affected: int
    versions_deleted: int
    keep: int | None

    model_config = ConfigDict(frozen=True)


class CompactResult(BaseModel):
    """Result of an LLM-powered memory compaction."""

    entry: MemoryEntry
    sources_read: int
    scope: str
    scope_type: MemoryScopeType
    compaction_record: CompactionRecord

    model_config = ConfigDict(frozen=True)


class MemoryReindexIssue(BaseModel):
    """One non-fatal issue encountered while reindexing memory versions."""

    artifact_id: str
    artifact_name: str
    scope: str | None
    key: str | None
    reason: str

    model_config = ConfigDict(frozen=True)


class MemoryReindexResult(BaseModel):
    """Summary of one memory reindex/backfill operation."""

    dry_run: bool
    versions_scanned: int
    execution_scope_versions_scanned: int
    already_indexed: int
    versions_needing_updates: int
    versions_updated: int
    scope_type_tags_identified: int
    flow_tags_identified: int
    scope_type_tags_added: int
    flow_tags_added: int
    issues_count: int
    issue_samples: _list[MemoryReindexIssue]

    model_config = ConfigDict(frozen=True)


@dataclass
class _ReindexCounters:
    """Mutable accumulator for reindex statistics."""

    versions_scanned: int = 0
    execution_scope_versions_scanned: int = 0
    already_indexed: int = 0
    versions_needing_updates: int = 0
    versions_updated: int = 0
    scope_type_tags_identified: int = 0
    flow_tags_identified: int = 0
    scope_type_tags_added: int = 0
    flow_tags_added: int = 0
    issues_count: int = 0


@dataclass(frozen=True)
class _MemoryScope:
    """Resolved or configured memory scope."""

    scope: str
    scope_type: _MemoryScopeType


@dataclass(frozen=True)
class _ExecutionFlowContext:
    """Resolved logical flow context for a memory write."""

    flow_id: str
    flow_name: str | None = None
