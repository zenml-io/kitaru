# Investigation: PydanticAI wait timeout should pause cleanly

## Summary
`kitaru.wait(timeout=...)` pauses cleanly when it is called directly at flow scope. The real issue is narrower: if a wait timeout escapes through a `@checkpoint` / ZenML step body, the overall run becomes paused/waiting but the step is recorded as failed, and resume can get stuck with no pending waits left to answer.

Recommended first fix: stop supporting `kitaru.wait()` inside arbitrary checkpoints, make that fail fast with a clear error, and ensure PydanticAI HITL paths that may wait run outside adapter-created tool checkpoints.

## Resolution

Implemented issue 5 by making `kitaru.wait()` reject checkpoint-contained calls before resolving ZenML wait support or creating a wait record. Flow-scope waits keep the existing timeout behavior: timeout parks the execution in a waiting state and leaves the wait answerable later. For PydanticAI, the safe HITL paths are explicit `@hitl_tool` usage or disabling granular checkpointing for native `ApprovalRequired` / `CallDeferred` tools with `tool_checkpoint_config_by_name={"tool_name": False}`.

## Symptoms
- Design intent in `src/kitaru/wait.py` says `timeout` is the maximum polling time before the runner pauses and exits, not an expiration of the wait record.
- Slack context from Michael says adapter-triggered waits may currently timeout by raising an exception that fails the flow.
- We need to verify whether this happens in core `kitaru.wait(...)`, only through the PydanticAI HITL adapter path, or not at all anymore.

## Background / Prior Research
- User confirmed issues 1-4 are already fixed and asked to investigate issue 5 specifically.
- Design note path: `/Users/strickvl/coding/zenml/repos/kitaru/design/2026-04-29-pydantic-ai-adapter-artifact-naming-and-input-tracking.md`.
- User-level guidance at `/Users/strickvl/.codex/agents.md` asks for concrete, story-style explanations and plain-language consequence chains.
- No external web research needed yet; this is a workspace behavior question requiring code inspection and a local reproduction script if possible.

## Investigator Findings
<!-- Pair investigator appends structured findings here. -->

### 2026-05-01 — Codex investigation: core pauses, checkpoint-contained waits poison resume

#### Short conclusion

`kitaru.wait(timeout=...)` **does not fail the whole execution when it is called directly at flow scope**. In the current environment, a flow-level timeout creates a pending wait record, ZenML moves the run to `paused`, and Kitaru maps that to public `ExecutionStatus.WAITING`.

The real bug is narrower and more dangerous: when `kitaru.wait(timeout=...)` runs **inside a Kitaru checkpoint / ZenML step**, the overall run still becomes `paused` / `waiting`, but the checkpoint step is recorded as `failed` with `Wait condition ... polling timed out.` In the local repro, resolving the wait and calling `resume()` did not recover the run: it stayed `running` with the checkpoint still `failed` and no pending waits for the full 20-second poll window. So the flow is not initially marked `failed`, but the pause is not clean because the checkpoint failure poisons resume.

Concrete mental model: flow-level wait is a runner-level pause; checkpoint-level wait is a runner-level pause signal escaping through a step body. ZenML catches the pause signal at the runner, but the step has already written down, “I failed.”

#### Code path findings

1. **Kitaru delegates timeout behavior to ZenML.**
   - `src/kitaru/wait.py:36-59` resolves `zenml.wait` / `zenml.execution.pipeline.dynamic.utils.wait`.
   - `src/kitaru/wait.py:78-83` documents the intended timeout contract: timeout is polling time before pause, not wait-record expiration.
   - `src/kitaru/wait.py:103-124` validates flow scope, sets default timeout, tracks analytics, and delegates to ZenML. If Kitaru thinks it is inside a checkpoint, it wraps the ZenML call in `_suspend_checkpoint_scope()`.

2. **The checkpoint workaround clears ZenML's step context, but does not move execution out of the step.**
   - `src/kitaru/runtime.py:176-198` clears Kitaru checkpoint scope and ZenML `StepContext` while preserving flow scope.
   - `src/kitaru/runtime.py:201-215` reaches into private `StepContext.__context_var__`.
   - `src/kitaru/checkpoint.py:146-174` installs Kitaru checkpoint scope around checkpoint function execution.
   - `src/kitaru/checkpoint.py:253-258` calls the underlying ZenML step. So even if `wait()` temporarily hides `StepContext`, the Python call stack is still inside the step execution.

3. **ZenML's flow-level timeout behavior is correct.** The installed ZenML version used by the repro was `0.94.3`; introspection showed the active package matched the local source mirror below:
   - local source mirror: `../zenml/src/zenml/execution/pipeline/dynamic/utils.py:109-144` delegates to `context.runner.wait(...)`.
   - local source mirror: `../zenml/src/zenml/execution/pipeline/dynamic/runner.py:1140-1146` explicitly rejects normal wait calls inside a `StepContext`.
   - local source mirror: `../zenml/src/zenml/execution/pipeline/dynamic/runner.py:1153-1167` creates the run wait condition record.
   - local source mirror: `../zenml/src/zenml/execution/pipeline/dynamic/runner.py:1268-1288` finalizes the wait-condition lease and raises `_WaitConditionPollTimeout` if the condition is still pending.
   - local source mirror: `../zenml/src/zenml/execution/pipeline/dynamic/runner.py:663-673` catches `_WaitConditionPollTimeout` around the dynamic pipeline entrypoint and returns instead of publishing a failed run.
   - local source mirror: `../zenml/src/zenml/zen_stores/sql_zen_store.py:7596-7612` sets the pipeline run to `ExecutionStatus.PAUSED` when a pending condition lease is finalized/abandoned.

4. **Kitaru's public state mapping is consistent with the ZenML paused state.**
   - `src/kitaru/_client/_models.py:17-24` exposes public statuses including `WAITING` and `FAILED`.
   - `src/kitaru/_client/_mappers.py:58-70` maps ZenML `paused` to Kitaru `ExecutionStatus.WAITING`.
   - `src/kitaru/_client/_mappers.py:463-475` also forces public status to `WAITING` if a pending wait is found on a running/paused run.
   - `src/kitaru/client.py:535-542` lists pending waits through `_list_pending_wait_conditions(...)`.
   - `src/kitaru/client.py:545-595` resolves wait input by calling ZenML `resolve_run_wait_condition(...)`.
   - `src/kitaru/client.py:630-656` only resumes when there are no pending waits and the raw ZenML run status is exactly `paused`.
   - `src/kitaru/client.py:346-374` resumes by calling the active stack orchestrator's `resume_run(snapshot=..., run=..., stack=...)`.

5. **The flow handle surface can hide this distinction.**
   - `src/kitaru/flow.py:748-750` maps raw run status through `_to_public_status(...)`.
   - `src/kitaru/flow.py:777-795` waits until ZenML reports a finished status. `paused` is not finished, so `handle.wait()` would keep polling rather than returning a “waiting” handle result. The repro avoided `handle.wait()` and inspected status directly.

#### Adapter-specific findings

1. **Explicit `@hitl_tool` now bypasses granular tool checkpoints.**
   - `src/kitaru/adapters/pydantic_ai/_toolset.py:105-108` checks `hitl_config` before resolving/opening a tool checkpoint.
   - `src/kitaru/adapters/pydantic_ai/_toolset.py:109-133` only opens `run_async_in_checkpoint(...)` for non-HITL tools.
   - `tests/test_pydantic_ai_adapter.py:413-458` pins this: explicit HITL should not open a granular tool checkpoint.
   - This means explicit `@hitl_tool` waits should behave like flow-scope waits, not like the broken checkpoint-contained repro.

2. **`wait_for_input(...)` is only a metadata wrapper around `kitaru.wait(...)`.**
   - `src/kitaru/adapters/pydantic_ai/_wait_for_input.py:13-34` forwards `timeout` and all wait arguments to `kitaru.wait(...)` with adapter metadata.
   - Therefore its timeout behavior depends entirely on where the tool body is executing. If the tool body is inside a granular tool checkpoint, it hits the checkpoint-contained wait path above.

3. **PydanticAI native deferred/approval paths can still route waits from inside a checkpoint.**
   - `src/kitaru/adapters/pydantic_ai/_toolset.py:233-258` catches `ApprovalRequired` and `CallDeferred` after calling the underlying tool.
   - `src/kitaru/adapters/pydantic_ai/_toolset.py:260-294` then calls `_handle_deferred(...)`.
   - `src/kitaru/adapters/pydantic_ai/_toolset.py:296-326` invokes `kitaru.wait(...)`.
   - `tests/test_pydantic_ai_adapter.py:553-600` shows the `ApprovalRequired` path still records `checkpoint_steps == ["publish_tool"]`, so the approval wait is reached while the adapter-created tool checkpoint is active. That is the path most likely to reproduce the poisoned checkpoint timeout behavior.

#### Reproduction scripts and commands run

All reproduction scripts were temporary and removed afterward, except normal ignored `.kitaru/config.yaml` state. No source files were edited during reproduction.

1. **Initial script outside the repo failed because ZenML could not resolve source root.**
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru /tmp/kitaru_wait_timeout_repro.py`
   - Result: `RuntimeError: Unable to resolve module <module '__main__' from '/tmp/kitaru_wait_timeout_repro.py'> ... outside the source root (/Users/strickvl/coding/zenml/repos/kitaru).`
   - Conclusion: a Kitaru/ZenML flow repro must live inside the repo source root, even if the file is temporary and ignored.

2. **Initial local environment run failed on the active stack.**
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru /tmp/kitaru_wait_timeout_repro.py`
   - Result before the source-root fix: `KitaruStackIntegrationDependencyError` because the active `local_remote` stack needed the `s3` integration / `boto3`.
   - Follow-up command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru kitaru stack list --output json`
   - Observed stacks: `default` was available and inactive; `local_remote` was active; `aws-k8s-stack` was inactive.
   - Adjustment: all successful repro runs used `fn.run(stack="default")`.

3. **Core + checkpoint timeout repro.**
   - Temporary script path: `.kitaru/tmp_wait_timeout_repro.py` (ignored), then removed.
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru .kitaru/tmp_wait_timeout_repro.py`
   - Script shape:
     - `core_wait_timeout_flow()` called `kitaru.wait(name="core_wait_timeout", question="core timeout repro", timeout=1)` directly in the flow body.
     - `checkpoint_wait_timeout_flow()` called `wait_inside_checkpoint()`, a `@checkpoint(cache=False, retries=0)` whose body called `kitaru.wait(name="checkpoint_wait_timeout", question="checkpoint timeout repro", timeout=1)`.
   - Observed core behavior:
     - `exec_id=8f293ce1-05c3-4b82-bb50-c41387c90077`
     - ZenML status: `'paused'`
     - Kitaru status: `'waiting'`
     - Failure: `None`
     - Pending wait: `core_wait_timeout`
   - Observed checkpoint behavior:
     - `exec_id=5c73cb36-3b1c-4455-b514-8bba09997925`
     - ZenML status: `'paused'`
     - Kitaru status: `'waiting'`
     - Failure: `None`
     - Pending wait: `checkpoint_wait_timeout`
     - Step status: `wait_inside_checkpoint = 'failed'`
     - Log line included: `Checkpoint 'wait_inside_checkpoint' failed: Wait condition 'checkpoint_wait_timeout' polling timed out.`

4. **Checkpoint timeout resume probe.**
   - Temporary script path: `tmp_wait_timeout_repro_root.py` at repo root so ZenML could import it during resume, then removed immediately after the run.
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru tmp_wait_timeout_repro_root.py`
   - Script shape:
     - Same checkpoint-contained wait pattern with `WAIT_NAME = "checkpoint_wait_timeout_resume_probe"` and `timeout=1`.
     - After timeout, script called `client.executions.input(run_id, wait=WAIT_NAME, value=None)` and then `client.executions.resume(run_id)`.
   - Observed after timeout:
     - `exec_id=ad0610da-157f-4046-8ac3-a2587502c380`
     - ZenML status: `paused`
     - Kitaru status: `waiting`
     - Pending wait: `checkpoint_wait_timeout_resume_probe`
     - Step status: `wait_inside_checkpoint_resume_probe = failed`
   - Observed after resolving input:
     - ZenML status: `paused`
     - Kitaru status: `waiting`
     - Pending waits: `[]`
     - Step status still `failed`
   - Observed after `resume()`:
     - `resume()` returned without raising.
     - For 20 poll iterations, status remained `running`, pending waits remained `[]`, and the step stayed `failed`.
   - Conclusion: checkpoint-contained wait timeout is not a clean pause/resume lifecycle, even though the top-level run initially says `paused` rather than `failed`.

5. **Installed ZenML introspection.**
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru /tmp/kitaru_zenml_wait_introspection.py`
   - Output:
     - `zenml_version 0.94.3`
     - `zenml_wait_file .../site-packages/zenml/execution/pipeline/dynamic/utils.py line 109`
     - `runner_file .../site-packages/zenml/execution/pipeline/dynamic/runner.py line 1099`
     - `poll_timeout_class_line 158`
   - Command: `uv run --with-editable /Users/strickvl/coding/zenml/repos/kitaru /tmp/kitaru_zenml_store_introspection.py`
   - Output:
     - `sql_store_file .../site-packages/zenml/zen_stores/sql_zen_store.py line 7560`

#### Existing test coverage gaps

1. `tests/test_kitaru.py:253-306` covers wait context guards, checkpoint-scope suspension, and forwarding the default timeout `600` to ZenML.
2. `tests/test_kitaru.py:318-338` covers the recent fix that flow-scope waits should not enter `_suspend_checkpoint_scope()`.
3. `tests/test_runtime.py:80-99` covers the scope-suspension helper around a mocked wait.
4. `tests/test_client.py:2193-2289` covers mocked resume guardrails: resume paused runs, reject pending waits, reject non-paused runs.
5. Missing: no integration test asserts that `kitaru.wait(timeout=1)` at flow scope lands in `WAITING` with an open wait.
6. Missing: no integration test asserts that `kitaru.wait(timeout=1)` inside a checkpoint either resumes cleanly or is explicitly rejected.
7. Missing: no adapter integration test for timeout behavior in `wait_for_input`, explicit `@hitl_tool`, native `ApprovalRequired`, or `CallDeferred` paths.

#### Fix recommendation

Do **not** try to fix this by returning `None` from `kitaru.wait()` on timeout. That would be unsafe: the flow would continue as if a human had approved/provided input when no human did.

The clean fix is to ensure timeout pause signals do not propagate through a ZenML step body:

1. **Short-term practical fix:** remove checkpoint wrapping from every adapter path that can trigger a wait.
   - Explicit `@hitl_tool` is already handled by `src/kitaru/adapters/pydantic_ai/_toolset.py:105-108`.
   - Extend the same idea to `wait_for_input(...)` guidance and native PydanticAI `ApprovalRequired` / `CallDeferred` paths. Where possible, detect these before `run_async_in_checkpoint(...)`; where not possible, document/require `tool_checkpoint_config_by_name={"tool_name": False}` or `@hitl_tool` for tools that may wait.

2. **Core contract decision needed:** either:
   - **Option A, recommended:** stop promising that `kitaru.wait()` is valid inside arbitrary `@checkpoint` bodies. Re-disallow or loudly warn for direct user checkpoint waits, and keep HITL correctness by making adapters bypass checkpoints before waiting.
   - **Option B, deeper ZenML work:** add/obtain a real ZenML “step suspended for wait” lifecycle so `_WaitConditionPollTimeout` inside a step does not mark the step failed. Without upstream step-level suspension semantics, Kitaru's private `StepContext` clearing hack can make the wait call reach ZenML, but it cannot make the enclosing step pause cleanly.

3. **Regression tests to add before fixing source:**
   - Flow-scope integration: local `@flow` with `kitaru.wait(timeout=1)` and no input should become public `ExecutionStatus.WAITING`, have one pending wait, and have no failure.
   - Checkpoint-scope integration: local `@flow` + `@checkpoint` wait timeout should capture today's bad behavior first: run public `WAITING`, pending wait exists, checkpoint step is `FAILED`; after input+resume it should not get stuck. This test should fail until the chosen fix lands.
   - Adapter integration: explicit `@hitl_tool` timeout should match flow-scope behavior; native `ApprovalRequired` / `CallDeferred` and `wait_for_input(...)` should either match flow-scope behavior or be explicitly rejected/configured out of checkpoints.

#### Final classification

- **Core flow-scope wait:** historical concern appears fixed / not currently reproducible. Timeout pauses cleanly.
- **Core checkpoint-contained wait:** real bug. Top-level run pauses, but the checkpoint is failed and resume is not clean.
- **PydanticAI explicit `@hitl_tool`:** likely OK after `b97f39b` because it bypasses granular tool checkpoints.
- **PydanticAI `wait_for_input`, `ApprovalRequired`, `CallDeferred`:** still risky when reached inside adapter-created tool checkpoints. These are the adapter-specific paths most likely to match Michael's remembered “timeout breaks the flow” symptom.


## Investigation Log

### Phase 1 - Initial assessment
**Hypothesis:** `kitaru.wait(timeout=...)` may raise/propagate a timeout signal as an execution failure instead of converting it into a paused execution state.
**Findings:** The design note names three possibilities: real core bug, historical bug already fixed, or adapter-specific bug caused by the HITL checkpoint suspension path.
**Evidence:** Design note issue 5, lines under "Wait timeout fails the flow instead of pausing it cleanly".
**Conclusion:** Needs code-level verification and ideally a minimal reproduction.

## Root Cause

The bug is not that wait timeout itself means failure. ZenML already has the right flow-level behavior: when no answer arrives before the polling timeout, it records the wait condition, marks the run `paused`, and Kitaru maps that to `ExecutionStatus.WAITING`.

The broken path is checkpoint-contained wait. `src/kitaru/wait.py:119-123` currently tries to make this safe by temporarily suspending checkpoint scope before delegating to ZenML wait. That hides Kitaru's checkpoint scope and ZenML's `StepContext`, but it does not physically move execution out of the ZenML step created by `src/kitaru/checkpoint.py:253-258`.

Concrete story:

1. Code is running inside a checkpoint step.
2. It calls `kitaru.wait(timeout=1)`.
3. Kitaru takes off the "I am inside a checkpoint" badge via `_suspend_checkpoint_scope()` (`src/kitaru/runtime.py:176-198`).
4. ZenML accepts the wait and creates a pending wait record.
5. Timeout happens, so ZenML raises its internal pause signal.
6. The runner catches that signal and pauses the run.
7. But the signal has traveled through the checkpoint step body, so the step writes down "failed" first.
8. Resume later sees an execution that is partly paused and partly failed. In the repro, resolving input and calling `resume()` left the run stuck `running` with no pending wait and the checkpoint still failed.

Adapter exposure:

- Explicit PydanticAI `@hitl_tool` is likely safe now because `_toolset.py:105-108` bypasses granular tool checkpoint wrapping before any wait happens.
- `wait_for_input(...)` is just a pass-through to `kitaru.wait(...)` (`_wait_for_input.py:13-34`), so it is safe or unsafe depending on whether the tool body is inside a checkpoint.
- Native PydanticAI `ApprovalRequired` / `CallDeferred` are still risky because `_toolset.py:233-326` discovers them after entering the tool body, then calls `_invoke_wait()` (`_toolset.py:335-342`) while the adapter-created checkpoint may still be active.

## Recommendations

1. **Change the core contract in `src/kitaru/wait.py`.**
   - Update the module docstring at `wait.py:1-12`; it currently says waits are valid inside `@checkpoint` bodies.
   - Add a fail-fast guard before the ZenML wait call: if `_is_inside_checkpoint()` is true, raise `KitaruContextError` with a message explaining that waits must happen at flow scope or outside adapter-created checkpoints.
   - Do not use `_suspend_checkpoint_scope()` as a way to allow general checkpoint-contained waits.

2. **Keep explicit `@hitl_tool` as the blessed PydanticAI path.**
   - `_toolset.py:105-108` already detects HITL metadata before checkpoint wrapping.
   - Preserve and strengthen tests around this behavior.

3. **Handle native `ApprovalRequired` / `CallDeferred` conservatively.**
   - First branch-scope fix: document and test that native deferred tools must opt out of tool checkpointing, e.g. `tool_checkpoint_config_by_name={"publish_tool": False}` for tools that may request approval/defer.
   - If we want better ergonomics later, add an explicit marker/decorator for tools that may wait so the adapter can bypass checkpointing before execution starts.

4. **Update `wait_for_input(...)` docs.**
   - Explain that it must not be called from inside an adapter-created tool checkpoint.
   - Point users to explicit `@hitl_tool` or per-tool checkpoint opt-out.

5. **Do not fix this by catching timeout and returning `None`.**
   - Timeout means "the runner stopped polling and parked the run".
   - It does not mean "the human answered with `None`".
   - Returning `None` would let code continue as if approval/input happened when it did not.

6. **Do not fix this by increasing the default timeout.**
   - A longer timeout only delays the poisoned-checkpoint path.
   - The real fix is to keep waits out of checkpoint steps.

## Test Plan

1. **Core flow-scope timeout integration test.**
   - Add a test with a flow that calls `kitaru.wait(name="approve_timeout", timeout=1)` directly in the flow body.
   - Do not call `handle.wait()` because `WAITING` is non-terminal.
   - Poll the execution via `KitaruClient` and assert: public status `WAITING`, raw ZenML status `paused`, pending wait exists, failure is `None`.

2. **Checkpoint-contained wait guard test.**
   - Replace tests that currently bless checkpoint-scope suspension for wait.
   - Unit-level: with `_flow_scope(...)` and `_checkpoint_scope(...)` active, `kitaru.wait(...)` raises `KitaruContextError` and does not call ZenML wait.
   - Optional integration-level: a flow whose checkpoint calls `wait()` should fail clearly with the new Kitaru error, and should not leave a pending wait behind.

3. **Adapter tests.**
   - Keep/strengthen the explicit `@hitl_tool` test showing no granular tool checkpoint is opened.
   - Add/adjust tests for `ApprovalRequired` and `CallDeferred`:
     - supported path: tool checkpoint disabled for that tool, wait is reached outside checkpoint;
     - unsafe path: checkpoint still enabled, wait raises the clear Kitaru error instead of creating a poisoned paused run.
   - Add a small `wait_for_input(...)` test that confirms `timeout` is forwarded and docs/fixtures show it needs checkpoint opt-out when used inside a tool body.

4. **Resume semantics regression.**
   - After the fix, there should be no supported path that creates the repro's contradictory state: `run=paused`, `wait=pending`, `step=failed`.
   - If such a state is found, treat it as a regression.

## Replay / Resume Semantics

Plain-language explanation to keep in docs/design notes:

`timeout` is not the human's deadline. It is the runner's patience budget.

Example:

1. The flow asks: `approved = kitaru.wait(name="approve_release", schema=bool, timeout=600)`.
2. For 10 minutes, the runner checks whether someone answered.
3. Nobody answers.
4. The runner parks the execution and exits so it does not burn compute forever.
5. The wait record stays open.
6. Two hours later, a human answers.
7. Resume starts the flow again from the saved execution snapshot.
8. Work before the wait should be replayed/cached cheaply.
9. When the flow reaches the same wait name, ZenML sees the stored answer and returns it.
10. The flow continues from there.

The invariant: timeout parks the run; it never invents an answer.

## Default Timeout Recommendation

Keep `600` seconds for now. It is a reasonable runner-polling default, and changing it does not fix issue 5.

Useful product framing:

- Shorter timeout: saves compute sooner, but users may need to resume more often.
- Longer timeout: keeps the worker alive longer, which is nicer for quick approvals but costs more.
- Neither one should change the wait record's lifetime. The wait remains answerable after the runner parks.

A future improvement could split the concepts more explicitly in docs/API naming: "polling timeout" versus "wait expiration". Today the docstring already says this, but the wording should be made more visible in HITL docs.

## Preventive Measures

- Add tests that prove waits happen either at flow scope or fail clearly before creating a wait record.
- Keep adapter HITL detection before checkpoint wrapping wherever possible.
- For any future adapter, require a design check: "Can this code path call `kitaru.wait()` while physically inside a step/checkpoint?" If yes, it must bypass the checkpoint or be rejected.
- Keep the private ZenML `StepContext` suspension helper isolated, but stop treating it as enough to make checkpoint-contained waits resume-safe.
- Document the distinction between polling timeout and human-response expiration in the public wait/HITL docs.
