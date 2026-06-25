# PydanticAI replay demo — investigate prod, replay with changes, ship on a cohort

You have a support agent built with **PydanticAI** and wrapped once with the Kitaru
PydanticAI adapter. Every run is a durable execution made of model-request and
tool-call checkpoints.

This example walks an **operator story**: inspect a production run in the UI, replay
with changes from the CLI, compare diffs in the UI, roll the variant to a cohort,
and ask Claude Code (via Kitaru MCP) whether the change is safe to ship.

**Full step-by-step commands:** [`PLAYBOOK.md`](PLAYBOOK.md)

**Replay anchor:** `lookup_policy_tool` — checkpoints before it stay cached; policy
lookup and the final model decision re-run live.

```text
support_copilot_model_request → gather_context_tool → lookup_policy_tool → support_copilot_model_request_2 → publish_support_decision
                                                      ↑ replay starts here
```

We compare every replay **directly to the original prod run** — no unchanged
“reproduction” leg in the primary story.

---

## Operator story

See [PLAYBOOK.md](PLAYBOOK.md) for the full walkthrough (UI → CLI replay → cohort → MCP).

---

## Quick start

```bash
cd examples/end_to_end/pydantic_replay_fork
uv sync && uv run kitaru init
echo "OPENAI_API_KEY=sk-..." > .env

# Create the “prod” run (clone-friendly)
uv run python demo.py seed
export PROD_ID="$(cat fixtures/prod_exec_id)"

# Optional: seed 10 distinct runs for cohort Act 5
uv run python demo.py seed-cohort --count 10

# Open PROD_ID in the Kitaru UI, then replay with a model change:
kitaru executions replay "$PROD_ID" \
  --at lookup_policy_tool \
  --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'
```

See [PLAYBOOK.md](PLAYBOOK.md) for UI compare, cohort `replay-many`, and MCP steps.

---

## Demo commands

| Command | What it does |
|---------|----------------|
| `uv run python demo.py seed` | Run the agent once; write `fixtures/prod_exec_id` |
| `uv run python demo.py seed-cohort --count 10` | Ten distinct runs for local cohort demos |
| `uv run python demo.py replay <PROD-ID>` | Model + tool replay legs; print three-way compare URL |
| `uv run python demo.py cohort --export-json reports/cohort_report.json` | Batch variant replay + HTML/JSON report |

`run-all` remains for a one-shot narrated run during development; the **playbook**
is the operator-facing story.

---

## The agent

The flow builds one `pydantic_ai.Agent` named `support_copilot`, gives it two tools,
and wraps it once in `KitaruAgent(checkpoint_strategy="calls")`. Config (`model` +
`prompt_profile`) travels as flow inputs — that is why `kitaru executions replay --args`
can rebuild the agent from a fresh process.

- **`baseline`** prompt profile treats permission, SSO, and admin changes as `needs_review`.
- **`trimmed_permissions`** is looser and may answer directly when the policy tool reports a fast path.
- Output is a typed `SupportDecision`: policy label, risk status, required action, summary.
- Decision drift is judged on `risk_status` and `required_action` (not wording changes).

Tool mock for replay demos lives in `mocks.py` (`lookup_policy`).

---

## Remote stack (Kubernetes)

The flow declares image needs in `support_agent.py` and pulls `OPENAI_API_KEY` from a
Kitaru secret:

```bash
kitaru secrets set openai-creds --private --OPENAI_API_KEY=sk-...
kitaru stack use <your-k8s-stack>
uv run python demo.py seed
```

---

## Layout

```text
PLAYBOOK.md         # operator story: CLI + UI + MCP
demo.py             # seed / replay / cohort helpers
support_agent.py    # PydanticAI agent, tools, @flow, REPLAY_POINT
mocks.py            # lookup_policy mock for --tool replay demos
utils/              # metrics, cohort orchestration
reporting/          # HTML + report models
reports/            # generated output (gitignored)
fixtures/           # prod_exec_id, cohort_exec_ids (gitignored); cohort_scenarios.json
```
