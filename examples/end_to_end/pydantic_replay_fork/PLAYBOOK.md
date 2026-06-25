# Support copilot replay playbook

Walk through investigating one production run, trying changes with **replay** (not fork),
comparing against the **original only**, then rolling the winning change out to a **cohort**
and asking Claude Code (via Kitaru MCP) for a ship / no-ship recommendation.

**Replay anchor:** `lookup_policy_tool` (`REPLAY_POINT` in `support_agent.py`)

**Flow name:** `support_copilot_flow`

---

## Before you start

```bash
cd examples/end_to_end/pydantic_replay_fork
uv sync
uv run kitaru init
uv run kitaru login          # local server; compare URLs need a dashboard connection
uv run kitaru stack use local   # or your remote stack (see Act 5 note on --deployment)
echo "OPENAI_API_KEY=sk-..." > .env   # or export it
export KITARU_UI_URL=https://preview.demo.kitaru.zenml.io
```

### Create the “prod” run

Single run for Acts 2–4:

```bash
uv run python demo.py seed
# writes fixtures/prod_exec_id and prints kr-...
```

**Cohort prep (10 distinct originals):** run ten different support requests so
`executions replay-many --flow ... --limit 10` has enough matches locally:

```bash
uv run python demo.py seed-cohort --count 10
# writes fixtures/prod_exec_id (first run) + fixtures/cohort_exec_ids (all ten)
```

Scenarios live in `fixtures/cohort_scenarios.json` — edit or extend that file to
change the prompts/customers.

```bash
export PROD_ID="$(cat fixtures/prod_exec_id)"
# or: export PROD_ID=kr-from-dashboard
export AT=lookup_policy_tool
```

---

## Story at a glance

| Act | What you do | Primary surface |
|-----|-------------|-----------------|
| 1 | Inspect original run and Agent definition + KitaruAdapter and flow | **Kitaru UI and Python Code** |
| 2 | Replay with cheaper model → compare to original | **CLI** + UI compare link |
| 3 | Replay with tool mock → compare to original | **CLI** + UI compare link |
| 4 | Three-way compare (prod + both replays) | **CLI / UI** |
| 5 | Replay same model change on cohort (top 10 expensive) | **CLI**  |
| 6 | Export cohort JSON → Claude Code + MCP recommendation | **JSON + MCP** |

---

## Act 1 — Original prod run in the UI

1. Open the Kitaru dashboard (local server URL from `kitaru status` or your stack login).
2. Find an execution for flow **`support_copilot_flow`**.
3. Expand checkpoints — you should see at least:
   - `support_copilot_model_request`
   - `gather_context_tool`
   - **`lookup_policy_tool`** ← replay starts here
   - `support_copilot_model_request_2`
   - `publish_support_decision`
4. Open artifacts on **`publish_support_decision`** (or tool checkpoints) and note
   `risk_status`, `required_action`, and the policy tool output. This is the baseline
   you are trying to improve or keep stable.

---

## Act 2 — Replay with a cheaper model (CLI → UI diff)

Replay from the policy lookup with new flow inputs. Checkpoints **before**
`lookup_policy_tool` stay cached; policy lookup + final model decision re-run under
the new model and prompt profile.

```bash
kitaru executions replay "$PROD_ID" \
  --at "$AT" \
  --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'
```

The command prints a **Compare original vs replay** URL when you're logged in
(`kitaru login`).
Open the compare URL in the browser. Inspect checkpoint diffs — especially
`lookup_policy_tool`, `support_copilot_model_request_2`, and final decision artifacts.

---

## Act 3 — Replay with a mocked policy tool (heavier change)

Swap the `lookup_policy` implementation without editing agent code.
Run from the example directory so `mocks.lookup_policy` resolves:

```bash
cd examples/end_to_end/pydantic_replay_fork

kitaru executions replay "$PROD_ID" \
  --at "$AT" \
  --tool '{"lookup_policy": "mocks.lookup_policy"}' 

Open the compare URL in the browser (prod vs tool replay).

## Act 3.5 Decide which change is more promising

Decide which experiment you want for the cohort (this playbook assumes the
**model change** from Act 2).

---

## Act 4 — Three-way compare in the UI

After Acts 2 and 3, open prod plus every replay in one compare view. Auto-discovery
finds replays linked via `original_exec_id`:

```bash
kitaru executions diff "$PROD_ID"
```

The command prints one compare URL with the original and all discovered replays:


Open that link in the browser.
---

## Act 5 — Cohort: same model change on top 10 expensive prod runs

After `demo.py seed-cohort --count 10`, replay the variant on every matching original
in one command. `replay-many --flow` selects originals that contain the replay anchor
(`lookup_policy_tool`), filters out runs missing that checkpoint, orders by cost, and
replays up to `--limit`:

```bash
kitaru executions replay-many \
  --flow support_copilot_flow \
  --at lookup_policy_tool \
  --order-by=-display_cost_usd \
  --limit 10 \
  --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}' \
  --wait -o json
```

JSON output includes a `cohort` object with selection metadata (`matched`, `scanned`,
`filtered`) plus replay results.

Use `kitaru executions cohort` only when you want a dry-run selection without replaying.

**Demo shortcut** (resolve, replay, metrics, HTML + JSON report):

```bash
uv run python demo.py cohort --export-json reports/cohort_report.json
```

---

## Act 6 — Export cohort report → Claude Code + MCP

Export a JSON summary your agent can read (ship / no-ship prompt):

```bash
# After demo cohort export exists:
cat reports/cohort_report.json
```

Example MCP session prompt for Claude Code (with Kitaru MCP enabled):

```text
Read reports/cohort_report.json in examples/end_to_end/pydantic_replay_fork.

We changed support_copilot_flow replays at lookup_policy_tool to use
model=openai:gpt-5-nano and prompt_profile=trimmed_permissions.

Use kitaru_executions_get to spot-check any case where decision_changed is true.
Use kitaru_executions_diff with base_exec_id and variant_exec_id from each case.

Recommend ship or no-ship for production. Call out:
- decision drift rate vs original
- cost/token regressions
- any case where risk_status got worse
```

MCP tools available today:

| Tool | Use |
|------|-----|
| `kitaru_executions_get` | Inspect a single execution |
| `kitaru_executions_diff` | Original vs one replay |
| `kitaru_executions_diff_cohort` | Many originals (auto-discovers replays — prefer explicit ids from JSON) |
| `kitaru_executions_replay_many` | Batch replay with same plan |

---

## CLI JSON tips

When a replay runs inline (common on `local_remote`), ZenML pipeline logs may appear on stdout
alongside JSON output. For `-o json`, filter the JSON line:

```bash
kitaru executions replay ... -o json 2>/dev/null | grep '^{"command"'
```

For descending sort fields, use `--order-by=-display_cost_usd` (equals form) so the shell does
not treat `-display_cost_usd` as a flag.

---

## Variant constants (reference)

| Change | Mechanism | CLI |
|--------|-----------|-----|
| Cheaper model + looser prompt | flow inputs | `--args '{"model":"openai:gpt-5-nano","prompt_profile":"trimmed_permissions"}'` |
| Mock policy tool | tool swap | `--tool '{"lookup_policy":"mocks.lookup_policy"}'` |
