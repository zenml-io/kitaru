# Cohort selection (v1) — execution plan

## Usage examples

### SDK — resolve a live cohort, then replay

```python
import kitaru
from support_agent import REPLAY_POINT, support_copilot_flow

# 50 most expensive completed originals on prod deployment, with the replay anchor present
cohort = kitaru.cohort(
    flow="support_copilot_flow",
    deployment="prod",
    at=REPLAY_POINT,
    order_by="-display_cost_usd",
    limit=50,
).resolve()

print(cohort.exec_ids)       # frozen for this resolve() call
print(cohort.matched)        # 50
print(cohort.scanned)        # how many runs were inspected
print(cohort.partial)        # True if max_scan stopped early

# Same replay plan as today — cohort is just a resolved exec-id list (+ metadata)
reproduce = support_copilot_flow.replay_many(
    cohort,
    at=REPLAY_POINT,
    cache=False,
    wait=True,
    on_error="collect",
)
variant = support_copilot_flow.replay_many(
    cohort,
    at=REPLAY_POINT,
    cache=False,
    model="openai:gpt-5-nano",
    prompt_profile="trimmed_permissions",
    wait=True,
)

matrix = kitaru.diff_cohort(cohort)
for row in matrix.rows:
    for url in row.urls:
        print(row.original_exec_id, url)
```

### SDK — pin deployment version instead of tag

```python
cohort = kitaru.cohort(
    flow="support_copilot_flow",
    deployment_version=7,
    at="lookup_policy_tool",
    order_by="-total_tokens",
    limit=50,
    since="2026-06-01",
).resolve()
```

### SDK — recent originals (replaces `recent_exec_ids`)

```python
cohort = kitaru.cohort(
    flow="support_copilot_flow",
    deployment="prod",
    at=REPLAY_POINT,
    order_by="-started_at",
    limit=10,
).resolve()

exec_ids = cohort.exec_ids
```

### SDK — dry-run / inspect before replay

```python
query = kitaru.cohort(
    flow="support_copilot_flow",
    deployment="prod",
    at=REPLAY_POINT,
    order_by="-display_cost_usd",
    limit=50,
)

cohort = query.resolve(max_scan=500)
if cohort.partial:
    print(f"only found {cohort.matched} after scanning {cohort.scanned}")

for exec_id, cost in cohort.ranked:
    print(exec_id, cost)
```

### Client namespace

```python
client = kitaru.KitaruClient()

cohort = client.executions.cohort(
    flow="support_copilot_flow",
    deployment="prod",
    at="lookup_policy_tool",
    limit=50,
).resolve()

client.executions.replay_many(cohort, at="lookup_policy_tool", wait=True)
```

### CLI

```bash
# Resolve only (no replay) — JSON list of exec IDs + selection metadata
kitaru executions cohort \
  --flow support_copilot_flow \
  --deployment prod \
  --at lookup_policy_tool \
  --order-by display_cost_usd \
  --limit 50 \
  --dry-run -o json

# Resolve + replay many with the same cut
kitaru executions cohort \
  --flow support_copilot_flow \
  --deployment prod \
  --at lookup_policy_tool \
  --order-by display_cost_usd \
  --limit 50 \
  --replay \
  --cache=false \
  -o json

# Diff after replays (pass explicit IDs or a saved cohort JSON)
kitaru executions diff-cohort kr-a kr-b kr-c -o json
```

### MCP

```json
{
  "tool": "kitaru_executions_cohort",
  "arguments": {
    "flow": "support_copilot_flow",
    "deployment": "prod",
    "at": "lookup_policy_tool",
    "order_by": "-display_cost_usd",
    "limit": 50,
    "dry_run": true
  }
}
```

```json
{
  "tool": "kitaru_executions_replay_many",
  "arguments": {
    "exec_ids": ["kr-a", "kr-b"],
    "at": "lookup_policy_tool"
  }
}
```

Note: v1 MCP may return resolved exec IDs from `kitaru_executions_cohort`; replay stays on `kitaru_executions_replay_many`.

### Pydantic demo migration

```python
# Before (demo helper)
cases = recent_exec_ids(client, 3)

# After
cohort = kitaru.cohort(
    flow=FLOW_NAME,
    deployment="prod",  # or read from env / config
    at=REPLAY_POINT,
    order_by="-started_at",
    limit=3,
).resolve()
cases = cohort.exec_ids
```

---

## Goal

Add a **selection layer** for batch replay experiments: declaratively pick a set of **original** executions for one **flow** and one **deployment**, pre-filtered for a replay **`at`** checkpoint, sorted and capped — then pass the result into existing **`replay_many`** / **`diff_cohort`**.

This is **not** new replay semantics. It replaces ad-hoc helpers like `recent_exec_ids()` and manual `executions.list()` filtering.

## Non-goals (v1)

- Persisted cohort resources on the server
- Mixed deployments in one cohort
- Server-side ranking API (client-side scan + sort only)
- Per-parent replay override maps
- Live re-resolution on every `replay_many` call without an explicit `resolve()` (live **query**, snapshot **result**)
- OR/NOT query algebra

## v1 product rules

| Rule | Default / behavior |
|------|-------------------|
| Scope | `flow` required |
| Deployment | Exactly one of `deployment` (tag) or `deployment_version` required |
| Replay anchor | `at` required; used for **pre-filter** and documented as the intended replay cut |
| Originals | `originals_only=True` (exclude rows with `original_exec_id`) |
| Status | `status="completed"` default; optional `failed` inclusion |
| Sort | `order_by` required; `-field` = descending |
| Tie-break | `-started_at`, then `exec_id` |
| Time window | Optional `since` / `until` (ISO date or datetime) |
| Scan cap | `max_scan` default 500; sets `partial=True` when exhausted before `limit` |
| Empty result | Raise `KitaruUsageError` with filter counts |
| Cohort object | Accepted anywhere `Sequence[str]` exec IDs work today (`replay_many`, `diff_cohort`) |

### Supported `order_by` fields (v1)

| Field | Source |
|-------|--------|
| `started_at` | `Execution.started_at` |
| `display_cost_usd` | execution LLM usage summary / metadata |
| `total_tokens` | execution LLM usage summary |
| `duration` | `ended_at - started_at` when both present |

Default when omitted: `-started_at` (recent-first cohorts).

### Deployment matching

Match execution metadata keys already used elsewhere:

- `deployment_version`, `kitaru_deployment_version`
- nested `kitaru_deployment.version`
- deployment tag fields consistent with `deployments` CLI

Reject executions that do not match the requested deployment pin.

### Pre-filter (`at`)

Use `replay_at_status(run, at=...)` / checkpoint list on each candidate **before** ranking:

- `present` → keep
- `missing` / `ambiguous` / `no_checkpoints` → drop (count in resolve stats)

This avoids relying on `ReplayManyResult.skipped` for cohort construction.

---

## Public API sketch

### Types (`src/kitaru/cohort.py`)

```python
@dataclass(frozen=True)
class CohortQuery:
    flow: str
    at: str
    deployment: str | None = None
    deployment_version: int | None = None
    order_by: str = "-started_at"
    limit: int = 50
    originals_only: bool = True
    status: str | Sequence[str] = "completed"
    since: datetime | str | None = None
    until: datetime | str | None = None

    def resolve(self, *, max_scan: int = 500) -> CohortResult: ...

@dataclass(frozen=True)
class CohortResult:
    exec_ids: list[str]
    flow: str
    at: str
    deployment: str | None
    deployment_version: int | None
    order_by: str
    scanned: int
    matched: int
    partial: bool
    filtered: dict[str, int]  # e.g. deployment, checkpoint, status, originals
    ranked: list[tuple[str, float | None]]  # optional audit trail

def cohort(...) -> CohortQuery: ...  # factory on kitaru package
```

`CohortResult` implements `Sequence[str]` (iterates `exec_ids`) for drop-in use with `replay_many`.

### Surface area

| Surface | v1 |
|---------|-----|
| `kitaru.cohort(...)` | yes |
| `KitaruClient().executions.cohort(...)` | yes |
| `FlowHandle.replay_many(cohort, ...)` | accept `CohortResult \| Sequence[str]` |
| `kitaru.diff_cohort(cohort)` | accept `CohortResult \| Sequence[str]` |
| CLI `executions cohort` | yes (`--dry-run`, optional `--replay`) |
| MCP `kitaru_executions_cohort` | yes |
| Analytics | `COHORT_RESOLVED` with counts only (no exec IDs) |
| Smoke test | `kitaru executions cohort --help` |

---

## Implementation phases

### Phase 1 — Core resolver (SDK only)

**Files**

- `src/kitaru/cohort.py` — query, result, resolver, deployment matcher, rank helpers
- `src/kitaru/replay.py` — reuse `replay_at_status` for pre-filter (import, no behavior change)
- `src/kitaru/__init__.py` — export `cohort`, `CohortQuery`, `CohortResult`
- `src/kitaru/client.py` — `ExecutionsAPI.cohort()`
- `src/kitaru/flow.py` — type widen `replay_many(executions: CohortResult | Sequence[str], ...)`
- `src/kitaru/diff.py` — type widen `diff_cohort(exec_ids: CohortResult | Sequence[str])`

**Algorithm (`resolve`)**

1. Validate: `flow`, `at`, exactly one deployment pin, `limit >= 1`.
2. Page `client.executions.list(flow=flow, status=..., page/size)` newest-first until `scanned >= max_scan` or enough ranked candidates.
3. For each run (hydrated checkpoints only when needed for `at` filter):
   - skip if `originals_only` and `original_exec_id` set
   - skip if deployment mismatch
   - skip if `at` not `present`
   - skip if outside `since` / `until`
   - compute sort key
4. Sort with tie-break; take `limit`.
5. Return `CohortResult`; if `matched == 0`, raise `KitaruUsageError` with `filtered` breakdown.

**Tests** (`tests/test_cohort.py`)

- deployment filter accepts/rejects via metadata fixtures
- originals_only excludes replays
- pre-filter drops missing `at`
- order_by + tie-break ordering
- partial scan when `max_scan` hit
- empty cohort error message includes counts
- `CohortResult` works as `Sequence` in `replay_many` (mock)

### Phase 2 — CLI + MCP

**Files**

- `src/kitaru/_cli/_executions.py` — `cohort` command
- `src/kitaru/mcp/server.py` — `kitaru_executions_cohort`
- `scripts/smoke-test.sh` — `--help`
- `src/kitaru/analytics.py` — `COHORT_RESOLVED`

**CLI flags**

- `--flow`, `--deployment`, `--deployment-version`, `--at`
- `--order-by`, `--limit`, `--since`, `--until`
- `--max-scan`, `--include-failed` (status toggle)
- `--dry-run` (default true for cohort-only command?)
- `--replay` optional convenience path

**JSON output**

```json
{
  "command": "executions.cohort",
  "item": {
    "exec_ids": ["kr-..."],
    "scanned": 312,
    "matched": 50,
    "partial": false,
    "filtered": {"deployment": 40, "checkpoint": 12, "originals": 8}
  }
}
```

### Phase 3 — Demo + docs

**Files**

- `examples/end_to_end/pydantic_replay_fork/utils/metrics.py` — replace or wrap `recent_exec_ids` with `kitaru.cohort(...).resolve()`
- `examples/end_to_end/pydantic_replay_fork/demo.py` — optional `COHORT_DEPLOYMENT` env
- `docs/book/guides/replay-and-overrides.md` — new “Cohort selection” section
- `CHANGELOG.md` — `[Unreleased]` entry
- `examples/example-coverage.yaml` — note cohort helper if tested

Keep `recent_exec_ids()` as thin deprecated wrapper calling `cohort` if external readers exist.

### Phase 4 — Optional persistence (still client-side)

- `CohortResult.to_json()` / `CohortResult.from_json(path)` for audit artifacts
- CLI `--file cohort.json` to replay a saved cohort without re-resolve

Defer if time-constrained.

---

## Open questions (resolve before Phase 1 merge)

1. **Deployment tag vs version in demo** — hardcode `prod`, read from env (`KITARU_COHORT_DEPLOYMENT`), or infer from active deployment?
2. **Hydration cost** — pre-filter `at` on list summaries vs full `get()` per candidate; start with list + checkpoint names if available on summary, else lazy `get()` until `max_scan`.
3. **CLI default** — `executions cohort` resolve-only vs resolve+replay in one command (recommend resolve-only; `--replay` opt-in).
4. **Cost field availability** — document fallback when `display_cost_usd` missing (sort key `None` sorts last).

---

## Acceptance criteria

- [ ] `kitaru.cohort(flow, deployment=..., at=..., limit=50).resolve()` returns ≥1 exec ID or a actionable error
- [ ] All returned IDs are originals on the same deployment with `at` present
- [ ] `replay_many(cohort, at=...)` behavior unchanged vs explicit ID list
- [ ] `diff_cohort(cohort)` works on resolved IDs
- [ ] Pydantic demo cohort leg uses `kitaru.cohort` instead of `recent_exec_ids`
- [ ] Unit tests cover filter/rank/empty/partial paths without live provider
- [ ] Docs + CHANGELOG updated; smoke `--help` passes

---

## Estimated touch count

| Area | Files | Risk |
|------|-------|------|
| Core resolver | 1 new + 4 small edits | medium (scan perf) |
| CLI/MCP | 2 | low |
| Tests | 1 new | low |
| Demo/docs | 4 | low |

Total: ~1–2 focused PRs (Phase 1+tests, Phase 2–3) or single PR if preferred.
