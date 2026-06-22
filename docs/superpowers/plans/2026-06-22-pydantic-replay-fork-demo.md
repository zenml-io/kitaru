# PydanticAI replay & fork demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A self-contained example where a PydanticAI support-copilot runs durably through Kitaru, then is reproduced, forked (cheaper model + looser prompt), and compared — single case and across the agent's last 10 runs — for the AI Engineer Summit demo.

**Architecture:** Native-wrap: `KitaruAgent(agent, checkpoint_strategy="calls")` makes each model/tool call a Kitaru checkpoint, so a wrapped run is a native Kitaru execution. A thin `@flow` wraps the agent call so we get an `exec_id`. **Reproduce** = `flow.replay(exec_id, from_=CUT, cache=False)`. **Fork** = build a flow around a *different-model/prompt* agent and replay the baseline execution from the same `CUT` (validated by a spike; re-run-fresh fallback). Drift = the existing semantic comparator. Cohort = the agent's last 10 executions, each forked from `CUT`.

**Tech Stack:** Python 3.12, PydanticAI (`Agent`, `TestModel`), Kitaru (`KitaruAgent`, `@flow`, `client.executions.replay`), `kitaru.adapters.langgraph.replay._drift` (framework-agnostic comparator), click, pytest.

## Global Constraints

- **Native-wrap only.** No external-trace import / reconstruction (that's the other track). Reproduce uses the existing `client.executions.replay` / `flow.replay`.
- **Fork = different-agent + replay** (the spike's primary mechanism); if the spike fails, **fork = re-run the fork agent fresh on the same input**. Do NOT assume; decide from Task 1.
- **`CUT`** is a single fixed checkpoint selector — the decision model call. Discovered in Task 1; the cohort skips any execution lacking it.
- **Models:** tests use PydanticAI `TestModel` (deterministic, no key); the recorded demo uses real `gpt-5-mini` (baseline) → `gpt-5-nano` (fork), gated by `OPENAI_API_KEY`.
- **Comparison metric:** semantic decision fields only — reuse `kitaru.adapters.langgraph.replay._drift.compare_decisions` / `DriftReport`. Never byte-compare free text.
- **Flow-test convention:** any test that runs a flow requests the `primed_zenml` fixture (see `tests/conftest.py`); lightweight tests don't.
- **Self-contained example folder**; the demo file reads cleanly (logic in `utils.py`/`pipeline.py`). `.env` is gitignored.
- New example lives in `examples/end_to_end/pydantic_replay_fork/`.

---

## File Structure

- `examples/end_to_end/pydantic_replay_fork/agent.py` — the PydanticAI support-copilot + `build_agent(model, prompt_profile)` factory + `SupportDecision`/`SupportDeps` + one tool.
- `examples/end_to_end/pydantic_replay_fork/pipeline.py` — `KitaruAdapterPA` facade: the `@flow` wrapping a `KitaruAgent`, plus `run`, `reproduce`, `fork`, `diff`, `cohort`, and `CUT`.
- `examples/end_to_end/pydantic_replay_fork/demo.py` — click CLI: `run / reproduce / fork / cohort / run-all`, narrated.
- `examples/end_to_end/pydantic_replay_fork/comparison_html.py` — copy of the slim HTML renderer (self-contained).
- `tests/test_pydantic_replay_fork.py` — TestModel-driven spine + spike tests (uses `primed_zenml`).
- `docs/superpowers/notes/2026-06-22-pydantic-fork-spike.md` — Task 1 spike findings (created in Task 1).

Each task ends green + a commit. Run tests: `uv run pytest tests/test_pydantic_replay_fork.py -v`.

---

### Task 1: Spike — validate the fork mechanism and discover `CUT`

**Goal:** Prove (or disprove) that a recorded wrapped-PydanticAI execution can be replayed from a checkpoint using a *separately-constructed* fork agent, and find the decision checkpoint's selector. Decide the fork mechanism for the rest of the plan.

**Files:**
- Create (scratch, not shipped): `/tmp/pa_spike.py`
- Create: `docs/superpowers/notes/2026-06-22-pydantic-fork-spike.md`
- Test: `tests/test_pydantic_replay_fork.py` (the spike assertion)

- [ ] **Step 1: Write a spike script** that, with `TestModel`, wraps two agents (baseline → `risk_status="needs_review"`, fork → `risk_status="safe"`) and tries the fork-by-replay:

```python
# /tmp/pa_spike.py — run with a throwaway local stack:
#   ZENML_CONFIG_PATH=$(mktemp -d) uv run python /tmp/pa_spike.py
import os, tempfile
os.environ["ZENML_CONFIG_PATH"] = tempfile.mkdtemp(prefix="pa-spike-")
os.environ["ZENML_ANALYTICS_OPT_IN"] = "false"

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from kitaru import flow
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru import KitaruClient


class Decision(BaseModel):
    risk_status: str = "unknown"
    required_action: str = "unknown"


def make_flow(agent_name, risk):
    inner = Agent(TestModel(custom_output_args={"risk_status": risk, "required_action": "x"}),
                  name=agent_name, output_type=Decision)
    wrapped = KitaruAgent(inner, checkpoint_strategy="calls")

    @flow(cache=False)
    def run_agent(prompt: str) -> dict:
        return wrapped.run_sync(prompt).output.model_dump()
    return run_agent


base = make_flow("spike_agent", "needs_review")
handle = base.run("a permission request")
exec_id = handle.exec_id
print("baseline:", handle.wait())

# discover the checkpoint names
run = KitaruClient().executions.get(exec_id)
print("checkpoints:", [c.name for c in run.checkpoints])

# attempt fork-by-replay: a different flow (fork agent) replaying the baseline exec
CUT = [c.name for c in run.checkpoints][-1]   # last checkpoint = the decision model call (verify!)
fork = make_flow("spike_agent", "safe")
try:
    fh = fork.replay(exec_id, from_=CUT, cache=False)
    print("FORK-BY-REPLAY WORKED:", fh.wait())
except Exception as e:
    print("FORK-BY-REPLAY FAILED:", type(e).__name__, str(e)[:300])
```

- [ ] **Step 2: Run the spike**

Run: `ZENML_CONFIG_PATH=$(mktemp -d) uv run python /tmp/pa_spike.py 2>&1 | grep -v '^Kitaru'`
Observe: the checkpoint names (to fix `CUT`), and whether fork-by-replay produced `risk_status="safe"` (mechanism A works) or raised (use fallback B).

- [ ] **Step 3: Record findings** in `docs/superpowers/notes/2026-06-22-pydantic-fork-spike.md`: the exact decision checkpoint selector to use as `CUT`, and the verdict — **A** (fork = different-agent + replay) or **B** (fork = re-run fork agent fresh on same input). All later tasks consume this.

- [ ] **Step 4: Lock the mechanism in a test** (in `tests/test_pydantic_replay_fork.py`) that encodes the verdict so it can't silently regress:

```python
import os, tempfile
os.environ.setdefault("ZENML_CONFIG_PATH", tempfile.mkdtemp(prefix="pa-test-"))

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from kitaru import flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent


class _Decision(BaseModel):
    risk_status: str = "unknown"


def _flow(name, risk):
    inner = Agent(TestModel(custom_output_args={"risk_status": risk}), name=name, output_type=_Decision)
    wrapped = KitaruAgent(inner, checkpoint_strategy="calls")

    @flow(cache=False)
    def run_agent(prompt: str) -> dict:
        return wrapped.run_sync(prompt).output.model_dump()
    return run_agent


def test_fork_by_replay_reexecutes_tail_under_new_agent(primed_zenml):
    base = _flow("forktest_agent", "needs_review")
    exec_id = base.run("permission request").exec_id
    run = KitaruClient().executions.get(exec_id)
    cut = [c.name for c in run.checkpoints][-1]

    fork = _flow("forktest_agent", "safe")
    result = fork.replay(exec_id, from_=cut, cache=False).wait()
    assert result["risk_status"] == "safe"   # tail re-ran under the fork agent
```

- [ ] **Step 5: Run and commit**

Run: `uv run pytest tests/test_pydantic_replay_fork.py -v`
Expected: PASS if mechanism A holds. **If it FAILS**, change the test to assert the fallback (re-run fresh on the same input yields `"safe"`), record verdict **B** in the notes, and proceed with B in Tasks 4–6.

```bash
git add tests/test_pydantic_replay_fork.py docs/superpowers/notes/2026-06-22-pydantic-fork-spike.md
git commit -m "spike(pa-replay): validate fork mechanism and discover CUT selector"
```

---

### Task 2: The PydanticAI support-copilot agent

**Files:**
- Create: `examples/end_to_end/pydantic_replay_fork/agent.py`
- Test: `tests/test_pydantic_replay_fork.py` (append)

**Interfaces:**
- Produces:
  - `class SupportDecision(BaseModel): policy_label: str; risk_status: str; required_action: str; summary: str`
  - `@dataclass class SupportDeps: customer: str`
  - `build_agent(model, *, prompt_profile: str = "baseline", name: str = "support_copilot") -> Agent[SupportDeps, SupportDecision]` — `model` is a PydanticAI model (real or `TestModel`); `prompt_profile` ∈ {`baseline`, `trimmed_permissions`} selects the instructions; one `@agent.tool` `lookup_customer`.

- [ ] **Step 1: Write the failing test**

```python
def test_build_agent_runs_and_returns_decision():
    from pydantic_ai.models.test import TestModel
    import importlib, sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.agent import build_agent, SupportDeps, SupportDecision

    model = TestModel(custom_output_args={
        "policy_label": "permissions_policy", "risk_status": "needs_review",
        "required_action": "escalate_to_human", "summary": "s"})
    agent = build_agent(model, prompt_profile="baseline")
    out = agent.run_sync("Can I enable SSO?", deps=SupportDeps(customer="acme")).output
    assert isinstance(out, SupportDecision)
    assert out.risk_status == "needs_review"
```

- [ ] **Step 2: Run → FAIL** (`ModuleNotFoundError: pydantic_replay_fork`).

Run: `uv run pytest tests/test_pydantic_replay_fork.py::test_build_agent_runs_and_returns_decision -v`

- [ ] **Step 3: Implement `agent.py`**

```python
"""PydanticAI support-copilot for the replay & fork demo."""
from __future__ import annotations
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

_PROMPTS = {
    "baseline": (
        "You are a careful B2B SaaS support copilot. Decide the policy_label, "
        "risk_status, required_action, and a short summary. Permission/SSO/admin "
        "or billing-owner changes are restricted: set risk_status='needs_review' "
        "and required_action='escalate_to_human' unless the request is clearly read-only."
    ),
    "trimmed_permissions": (
        "You are a fast, helpful support copilot. Prefer answering directly. "
        "Decide policy_label, risk_status, required_action, and a short summary."
    ),
}


class SupportDecision(BaseModel):
    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


@dataclass
class SupportDeps:
    customer: str


def build_agent(model, *, prompt_profile: str = "baseline", name: str = "support_copilot"):
    agent = Agent(
        model,
        name=name,
        deps_type=SupportDeps,
        output_type=SupportDecision,
        instructions=_PROMPTS[prompt_profile],
    )

    @agent.tool
    def lookup_customer(ctx: RunContext[SupportDeps], query: str) -> dict:
        return {"customer_id": ctx.deps.customer, "plan": "Enterprise", "role": "account_owner"}

    return agent
```

- [ ] **Step 4: Run → PASS.** Run: `uv run pytest tests/test_pydantic_replay_fork.py::test_build_agent_runs_and_returns_decision -v`

- [ ] **Step 5: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/agent.py tests/test_pydantic_replay_fork.py
git commit -m "feat(pa-demo): PydanticAI support-copilot agent + build_agent factory"
```

---

### Task 3: Wrap + run as a durable Kitaru execution (`pipeline.py`)

**Files:**
- Create: `examples/end_to_end/pydantic_replay_fork/pipeline.py`
- Test: `tests/test_pydantic_replay_fork.py` (append)

**Interfaces:**
- Consumes: `agent.build_agent` (Task 2); `KitaruAgent`; `@flow`; `KitaruClient`.
- Produces:
  - `class KitaruAdapterPA:` `__init__(self, *, model, prompt_profile="baseline", name="support_copilot")` — builds `KitaruAgent(build_agent(model, prompt_profile, name), checkpoint_strategy="calls")` and a `@flow run_agent(prompt, customer)` closure.
  - `run(self, prompt, customer) -> str` (returns exec_id).
  - `decision_of(self, exec_id) -> dict` — read the produced `SupportDecision` dict from the execution's flow result (`KitaruClient().executions.get(exec_id)` / handle).
  - `CUT` — module constant; set to the spike's selector. If the spike found "last checkpoint," expose `cut_of(exec_id) -> str` that resolves it per run.

- [ ] **Step 1: Write the failing test** (`primed_zenml`, TestModel):

```python
def test_run_produces_durable_execution_with_call_checkpoints(primed_zenml):
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA
    from kitaru import KitaruClient

    model = TestModel(custom_output_args={
        "policy_label": "permissions_policy", "risk_status": "needs_review",
        "required_action": "escalate_to_human", "summary": "s"})
    adapter = KitaruAdapterPA(model=model)
    exec_id = adapter.run("Can I enable SSO?", customer="acme")
    run = KitaruClient().executions.get(exec_id)
    assert run.checkpoints                      # per-call checkpoints exist
    assert adapter.decision_of(exec_id)["risk_status"] == "needs_review"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `pipeline.py`**

```python
from __future__ import annotations
from typing import Any

from kitaru import flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent

from agent import build_agent, SupportDeps   # local package import (see demo.py path note)


class KitaruAdapterPA:
    def __init__(self, *, model, prompt_profile: str = "baseline", name: str = "support_copilot") -> None:
        self._wrapped = KitaruAgent(
            build_agent(model, prompt_profile=prompt_profile, name=name),
            checkpoint_strategy="calls",
        )
        self._client = KitaruClient()

        @flow(cache=False)
        def run_agent(prompt: str, customer: str) -> dict:
            return self._wrapped.run_sync(prompt, deps=SupportDeps(customer=customer)).output.model_dump()

        self._flow = run_agent

    def run(self, prompt: str, customer: str) -> str:
        handle = self._flow.run(prompt, customer)
        handle.wait()
        return handle.exec_id

    def cut_of(self, exec_id: str) -> str:
        # CUT = the decision (last) model-call checkpoint; per the spike.
        names = [c.name for c in self._client.executions.get(exec_id).checkpoints]
        if not names:
            raise RuntimeError(f"execution {exec_id} has no checkpoints")
        return names[-1]

    def decision_of(self, exec_id: str) -> dict:
        run = self._client.executions.get(exec_id)
        # the flow result dict is the SupportDecision; fall back to the last checkpoint artifact
        result = getattr(run, "result", None)
        if isinstance(result, dict) and "risk_status" in result:
            return result
        for cp in reversed(run.checkpoints):
            for art in cp.artifacts:
                val = art.load()
                if isinstance(val, dict) and "risk_status" in val:
                    return val
        return {}
```

Note: confirm how the flow result is read back from an `Execution` (the spike printed `handle.wait()`); if `run.result` isn't populated, use the handle's `.get()` at run time and store it, or read the terminal checkpoint artifact as shown.

- [ ] **Step 4: Run → PASS.** If `decision_of` can't find the decision, adjust to store `handle.wait()` keyed by exec_id at `run()` time. Re-run.

- [ ] **Step 5: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/pipeline.py tests/test_pydantic_replay_fork.py
git commit -m "feat(pa-demo): wrap agent as durable Kitaru execution; read decision + CUT"
```

---

### Task 4: Reproduce + diff

**Files:** Modify `pipeline.py`; Test append.

**Interfaces:**
- Consumes: `KitaruAdapterPA` (Task 3); `compare_decisions`, `DriftReport` from `kitaru.adapters.langgraph.replay._drift`.
- Produces: `reproduce(self, exec_id) -> str` (re-runs from `CUT`, no edits, returns the replay exec_id) and `diff(self, baseline_exec, other_exec) -> DriftReport` (semantic-field comparison of the two executions' decisions).

- [ ] **Step 1: Write the failing test** (`primed_zenml`, TestModel): reproduction reproduces the decision.

```python
def test_reproduce_matches_original(primed_zenml):
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    model = TestModel(custom_output_args={"policy_label": "permissions_policy",
        "risk_status": "needs_review", "required_action": "escalate_to_human", "summary": "s"})
    adapter = KitaruAdapterPA(model=model)
    base = adapter.run("Can I enable SSO?", customer="acme")
    repro = adapter.reproduce(base)
    report = adapter.diff(base, repro)
    assert report.has_fork_drift is False   # repro vs base: no change
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement** (append to `KitaruAdapterPA`):

```python
    def reproduce(self, exec_id: str) -> str:
        handle = self._flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        handle.wait()
        return handle.exec_id

    def diff(self, baseline_exec: str, other_exec: str):
        from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
        base = self.decision_of(baseline_exec)
        other = self.decision_of(other_exec)
        return DriftReport(reproduction=[], fork=compare_decisions(base, other))
```

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/pipeline.py tests/test_pydantic_replay_fork.py
git commit -m "feat(pa-demo): reproduce from CUT + semantic diff"
```

---

### Task 5: Fork (cheaper model + looser prompt)

**Files:** Modify `pipeline.py`; Test append.

**Interfaces:**
- Produces: `fork(self, exec_id, *, model, prompt_profile="trimmed_permissions") -> str` — per the **spike verdict**: (A) build a fork `KitaruAdapterPA`/flow with the new model+prompt and `replay(exec_id, from_=CUT, cache=False)`; or (B) re-run the fork agent fresh on the same input. Returns the fork exec_id.

- [ ] **Step 1: Write the failing test** — fork flips the decision (baseline TestModel → needs_review; fork TestModel → safe):

```python
def test_fork_changes_decision(primed_zenml):
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    base_model = TestModel(custom_output_args={"policy_label": "permissions_policy",
        "risk_status": "needs_review", "required_action": "escalate_to_human", "summary": "s"})
    fork_model = TestModel(custom_output_args={"policy_label": "permissions_policy",
        "risk_status": "safe", "required_action": "answer_directly", "summary": "s"})

    adapter = KitaruAdapterPA(model=base_model)
    base = adapter.run("Can I enable SSO?", customer="acme")
    fork = adapter.fork(base, model=fork_model)
    report = adapter.diff(base, fork)
    assert report.has_fork_drift is True
    changed = {c.field for c in report.fork if not c.matches}
    assert "risk_status" in changed
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `fork`** per the spike. **Mechanism A (primary):**

```python
    def fork(self, exec_id: str, *, model, prompt_profile: str = "trimmed_permissions") -> str:
        fork_adapter = KitaruAdapterPA(model=model, prompt_profile=prompt_profile, name=self._name)
        handle = fork_adapter._flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        handle.wait()
        return handle.exec_id
```
(Store `self._name` in `__init__`. The fork flow must share the agent name so the replay resolves to the same flow. **Mechanism B fallback:** `fork_adapter.run(<same prompt+customer recovered from the baseline execution inputs>)` — recover the inputs from `executions.get(exec_id)` flow inputs.)

- [ ] **Step 4: Run → PASS.** If A raises (flow-identity mismatch), switch to B and re-run.

- [ ] **Step 5: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/pipeline.py tests/test_pydantic_replay_fork.py
git commit -m "feat(pa-demo): fork (cheaper model + looser prompt) via the spike's mechanism"
```

---

### Task 6: Cohort over the agent's last 10 executions

**Files:** Modify `pipeline.py`; Test append.

**Interfaces:**
- Produces:
  - `last_executions(self, n=10) -> list[str]` — `self._client.executions.list(...)` filtered to this agent's flow, most recent `n` exec_ids.
  - `cohort(self, *, model, prompt_profile="trimmed_permissions", n=10) -> CohortReport` where `CohortReport` has `.total`, `.regressed`, `.per_case: list[(exec_id, DriftReport)]`. For each execution: resolve `CUT` (skip if absent), `reproduce` then `fork`, `diff(reproduce, fork)`; count as regressed if `has_fork_drift`.

- [ ] **Step 1: Write the failing test** — seed 3 baseline runs, cohort forks them:

```python
def test_cohort_forks_recent_executions(primed_zenml):
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    base_model = TestModel(custom_output_args={"risk_status": "needs_review", "summary": "s"})
    fork_model = TestModel(custom_output_args={"risk_status": "safe", "summary": "s"})
    adapter = KitaruAdapterPA(model=base_model)
    for i in range(3):
        adapter.run(f"permission request {i}", customer="acme")

    report = adapter.cohort(model=fork_model, n=3)
    assert report.total == 3
    assert report.regressed == 3      # every case flips needs_review -> safe
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement** `last_executions`, `cohort`, and a `CohortReport` dataclass. Use `self._client.executions.list(...)` (consult `_interface_executions.py` for the exact filter args — flow name / `flow=` kwarg) and filter to the agent's flow name; cap to `n`. Skip executions where `cut_of` raises.

- [ ] **Step 4: Run → PASS.** (If `executions.list` filtering differs, adjust the filter; verify it returns this agent's runs newest-first.)

- [ ] **Step 5: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/pipeline.py tests/test_pydantic_replay_fork.py
git commit -m "feat(pa-demo): cohort over the agent's last N executions, forked from CUT"
```

---

### Task 7: Narrated click CLI + HTML report

**Files:**
- Create: `examples/end_to_end/pydantic_replay_fork/demo.py`, `examples/end_to_end/pydantic_replay_fork/comparison_html.py` (copy from `../replay_fork_demo/comparison_html.py`).
- Test: smoke via `--help` only (the spine is covered by Tasks 3–6).

**Interfaces:**
- Consumes: `KitaruAdapterPA`; `comparison_html.write`.
- Produces: a click group `run / reproduce / fork / cohort / run-all` (narrated like the LangGraph `demo.py`), real `gpt-5-mini`→`gpt-5-nano` by default, writing `replay_vs_fork.html` on fork/run-all and printing a `K/10 regressed` cohort summary.

- [ ] **Step 1: Copy the HTML renderer** for self-containment:

```bash
cp examples/end_to_end/replay_fork_demo/comparison_html.py examples/end_to_end/pydantic_replay_fork/comparison_html.py
```

- [ ] **Step 2: Write `demo.py`** — a click group. Model factory: `openai:gpt-5-mini` for baseline, `openai:gpt-5-nano` for fork (PydanticAI accepts model strings). The script is at the example folder; it imports `agent`, `pipeline`, `comparison_html` as siblings (run from the folder: `cd examples/end_to_end/pydantic_replay_fork && uv run python demo.py run-all`). `run-all`: run a scenario → reproduce → fork → compare (+ HTML) → cohort summary, with `click.secho` narration mirroring the LangGraph demo's 1)–5) story.

```python
import click
from pipeline import KitaruAdapterPA
import comparison_html

BASE_MODEL, FORK_MODEL = "openai:gpt-5-mini", "openai:gpt-5-nano"
SCENARIO, CUSTOMER = "Please enable SSO for our Enterprise workspace.", "acme"


@click.group()
def cli() -> None:
    """PydanticAI replay & fork demo (support-copilot)."""


@cli.command("run-all")
def run_all() -> None:
    click.secho("1) Your PydanticAI agent ran in production, wrapped with Kitaru (durable).", bold=True)
    adapter = KitaruAdapterPA(model=BASE_MODEL)
    base = adapter.run(SCENARIO, customer=CUSTOMER)
    click.echo(f"   → execution {base}; decision={adapter.decision_of(base).get('risk_status')!r}")

    click.secho("2) Reproduce it from the decision step (cached head, live tail).", bold=True)
    repro = adapter.reproduce(base)
    click.echo(f"   → reproduction drift: {adapter.diff(base, repro).has_fork_drift}")

    click.secho("3) Fork it before shipping: gpt-5-nano + looser permissions.", bold=True)
    fork = adapter.fork(base, model=FORK_MODEL)
    report = adapter.diff(repro, fork)
    click.echo(f"   → {report}")

    click.secho("4) Now across your last 10 production runs.", bold=True)
    cohort = adapter.cohort(model=FORK_MODEL, n=10)
    click.echo(f"   → {cohort.regressed}/{cohort.total} regressed under the cheaper config")


if __name__ == "__main__":
    cli()
```
(Add `run`, `reproduce`, `fork`, `cohort` subcommands following the same pattern; wire `comparison_html.write` on `fork`/`run-all` exactly as the LangGraph demo does, sourcing settings_changes from `{model, prompt_profile}` and outcomes from `report.fork`.)

- [ ] **Step 3: Verify it loads**

Run: `cd examples/end_to_end/pydantic_replay_fork && uv run python demo.py --help`
Expected: the five commands listed.

- [ ] **Step 4: Commit**

```bash
git add examples/end_to_end/pydantic_replay_fork/demo.py examples/end_to_end/pydantic_replay_fork/comparison_html.py
git commit -m "feat(pa-demo): narrated click CLI + HTML comparison report"
```

---

### Task 8: Real-model dry run (manual, pre-recording)

**Goal:** Confirm the full arc on real models before the video. Not CI.

- [ ] **Step 1:** Create `examples/end_to_end/pydantic_replay_fork/.env` (gitignored) with `OPENAI_API_KEY=…`.
- [ ] **Step 2:** Seed history so the cohort has runs: `cd examples/end_to_end/pydantic_replay_fork && set -a && . ./.env && set +a && for i in 1 2 3; do uv run python demo.py run >/dev/null; done` (add a `run` subcommand that just does a baseline run).
- [ ] **Step 3:** `uv run python demo.py run-all` on your `local_remote` stack; confirm reproduction drift False, fork drift True (`needs_review → safe`), an `N/10 regressed` line, `replay_vs_fork.html` written, and executions visible in the dashboard. No commit (manual verification).

---

## Self-Review (completed by plan author)

**Spec coverage:** native-wrap (T3), reproduce (T4), fork cheaper-model+looser-prompt (T5), compare/semantic-drift (T4/T5 reuse `_drift`), cohort over last-10-from-Kitaml forked from fixed CUT skip-if-absent (T6), real-model demo + TestModel tests (T8 / T1–T6), spike-first with fallback (T1, consumed by T5/T6), narrated CLI + HTML (T7), self-contained folder (all). ✔

**Placeholder scan:** Tasks 2–7 carry real code. Task 1 is an explicit spike whose *findings* (CUT selector, A-vs-B verdict) are consumed by later tasks — that's a genuine decision gate, not a vague placeholder; the success criteria and the locking test are concrete. Two "consult/confirm" notes (reading the flow result in T3; `executions.list` filter args in T6) point at exact files to check — fill from the real API, not guesswork.

**Type consistency:** `KitaruAdapterPA` (model, prompt_profile, name) → used T3–T7. `run→exec_id`, `reproduce→exec_id`, `fork(exec_id, model, prompt_profile)→exec_id`, `diff(a,b)→DriftReport`, `cohort(model, n)→CohortReport(total, regressed, per_case)`, `cut_of(exec_id)`, `decision_of(exec_id)→dict`, `SupportDecision`/`SupportDeps`/`build_agent` — consistent across tasks. ✔

**Known risks (call out, don't placeholder):** (1) Fork-by-replay (mechanism A) is the spike's make-or-break — Task 1 gates it, Task 5 has the B fallback. (2) Reading the decision back from an `Execution` (T3) and the `executions.list` filter (T6) are the two API-shape unknowns — each names the file to verify against.

---

## REVISED TASK SEQUENCE — 2026-06-22 (replay-reframe + multi-step)

Supersedes Tasks 5–7 above; Tasks 2–4's single-step agent is reworked. Mechanism validated in `docs/superpowers/notes/2026-06-22-pydantic-multistep-spike.md` (structure **b2**: a `@flow` of explicit `@checkpoint` steps, each running a raw `pydantic_ai.Agent`; chained via artifacts → single terminal; `CUT` = the intermediate step's checkpoint; replay from CUT caches the head and re-runs the tail; `original_exec_id` proves the replay lineage).

- **R1 — Multi-step agent flow.** Rework `agent.py` + `pipeline.py`: a `@flow` `support_copilot` of `@checkpoint` steps `gather_context → decide → finalize`, each running a raw PydanticAI `Agent` built by a per-step `build_*` factory that takes `(model, prompt_profile)`. `KitaruAdapterPA(model, prompt_profile)` runs it; `run(prompt, customer)->exec_id`; `CUT="decide"`; `decision_of(exec_id)->dict` (read the `decide`/`finalize` checkpoint artifact). Test (`primed_zenml`, `TestModel`): run → execution has chained `gather_context`/`decide`/`finalize` checkpoints; decision present.
- **R2 — reproduce + diff (multi-step).** `reproduce(exec_id)` = `flow.replay(exec_id, from_=CUT, cache=False)` (head cached, decide+finalize re-run, no edits); `diff(a,b)` reuses `_drift.compare_decisions`. Test: reproduction has no drift; assert the head (`gather_context`) was served from cache (not re-run) via `original_exec_id` lineage.
- **R3 — experiment (replay + reconfigured agent, kind #2).** `experiment(exec_id, *, model=None, prompt_profile=None)` = build a reconfigured `KitaruAdapterPA` (new global model/prompt AND/OR the `decide` step reconfigured) and `flow.replay(exec_id, from_=CUT, cache=False)` so `decide` re-runs under the new config. Returns the experiment exec_id. Test: a reconfigured decide step (TestModel → "safe") flips the decision vs the baseline ("needs_review"); `diff` shows the change; lineage links to baseline.
- **R4 — cohort + improvement metrics.** `last_executions(n)` (10–100, `executions.list` filtered to this agent's flow). `cohort(*, model, prompt_profile, n)` reproduces+experiments each from CUT (skip if CUT absent) and returns a `CohortReport` with per-run + aggregate: **decision-change count**, **cost** and **latency** (from Kitaru tracked usage on the executions), and an **LLM-judge quality score** (a small judge agent scoring baseline vs experiment answer; `TestModel` in tests). "Improvement" = cheaper/faster/quality-not-worse.
- **R5 — CLI (reframed).** Document/use `kitaru executions replay --from decide` as the reproduce path. Thin example `demo.py` (click): `run`, `reproduce`, `experiment`, `cohort`, `run-all` (narrated: production run → reproduce → experiment(reconfigure decide + global) → compare → cohort metrics → "improvement?"). HTML report via the copied `comparison_html.py`. No separate "fork" verb.
- **R6 — real-model dry run** (manual, pre-recording), as Task 8 above.
