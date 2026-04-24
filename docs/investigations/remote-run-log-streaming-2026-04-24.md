# Investigation: Remote `.run()` log streaming stalls after code download

## Summary
Issue #202 is a two-layer remote logging gap. First, Kitaru's SDK wait path only polls execution status and never reuses the existing `executions logs --follow` backend tailing loop. Second, for synchronous Kubernetes stacks, the observed silence can happen even earlier: `_submit()` is still blocked inside ZenML's synchronous Kubernetes monitor before Kitaru has returned a `FlowHandle`, so a fix only in `FlowHandle.wait()` would be necessary but incomplete.

## Symptoms
- Remote Kubernetes-stack `.run()` prints submitter lifecycle logs through `Code download finished`, then appears silent for the duration of the run.
- The dashboard shows the orchestrator pod and checkpoints continuing live while the submitter terminal is silent.
- Intermediate logs arrive in one flush after the run completes.
- Users may reasonably think the run has hung unless they know to run `kitaru executions logs --follow <exec_id>` separately.

## Background / Prior Research
- GitHub issue: https://github.com/zenml-io/kitaru/issues/202
- Issue opened by `strickvl` on 2026-04-21 and still open as of 2026-04-24.
- Reported environment: Kitaru `develop` around 0.5.1 plus unreleased changes, Kubernetes stack, staging.cloudinfra.zenml.io, Python 3.12.
- Initial user-suggested directions: either auto-invoke the existing backend log-follow loop from `.run()` for remote stacks, or emit progress markers while polling the orchestrator job.

## Investigator Findings
<!-- Pair investigator appends structured findings here: file:line refs, evidence, conclusions. -->

### 2026-04-24 - Submit/wait path, CLI follow path, and backend log split

#### Root-cause shape

The issue is not caused by a missing terminal rewrite rule or by the CLI follow loop being broken. The current code has two separate monitoring paths:

1. **SDK flow execution path**: `@flow.run()` submits through ZenML and returns a `FlowHandle`; `FlowHandle.wait()` only refreshes execution status and extracts the final result.
2. **CLI log-follow path**: `kitaru executions logs --follow` polls `KitaruClient().executions.logs(...)`, deduplicates entries, and separately polls execution status until a terminal state.

Those two paths are not connected today. In plain story form: `.run().wait()` is watching the **traffic light** (running/completed/failed), while `executions logs --follow` is reading the **scrolling road camera feed**. The SDK wait loop never opens the camera feed.

There is one important nuance for synchronous Kubernetes stacks: Kitaru does not construct the `FlowHandle` until after `configured_pipeline(*args, **kwargs)` returns. ZenML's Kubernetes orchestrator can block inside that call while monitoring the orchestrator job. So the reported silence after `Code download finished` can occur **before** user code reaches `FlowHandle.wait()`. Still, the same product gap remains: the SDK path relies on ZenML's submitter/orchestrator monitoring output and never reuses Kitaru's backend log-follow loop for checkpoint logs.

#### SDK submit and wait evidence

- `src/kitaru/flow.py:785-816` - `_FlowDefinition.run(...)` is a thin wrapper around `_submit(...)`.
- `src/kitaru/flow.py:1094-1138` - `_submit(...)` resolves execution config, applies pipeline options, activates the selected stack, then calls `run = configured_pipeline(*args, **kwargs)`.
- `src/kitaru/flow.py:1142-1151` - only after that call returns does Kitaru track `FLOW_SUBMITTED`, persist the frozen execution spec, and return `FlowHandle(run, ...)`.
- `src/kitaru/flow.py:671-690` - `FlowHandle.wait()` loops forever, calls `self._refresh()`, checks `run.status.is_finished`, raises/returns on terminal status, and otherwise sleeps one second. There is no call to `KitaruClient.executions.logs(...)`, `_follow_execution_logs(...)`, or any log retrieval API.
- `src/kitaru/flow.py:715-726` - `_refresh()` calls `Client().get_pipeline_run(...)` only. This confirms status polling, not log polling.

#### CLI follow evidence

- `src/kitaru/_cli/_executions.py:268-390` - `_follow_execution_logs(...)` is the existing follow loop. It repeatedly calls `client.executions.logs(...)`, deduplicates entries with `_log_entry_dedup_key(...)`, emits only new entries, then calls `client.executions.get(exec_id)` to decide whether to exit, fail, report cancellation, or print a waiting-for-input marker.
- `src/kitaru/_cli/_executions.py:590-618` - `kitaru executions logs --follow` invokes `_follow_execution_logs(...)` and exits with its return code.
- `src/kitaru/_cli/_flows.py:936-965` - deployment log follow (`kitaru flow deployments logs --follow`) reuses the same helper with a different command name.
- No source-code reference from `src/kitaru/flow.py` to `_follow_execution_logs` or `executions.logs` was found. The reusable follow behavior currently lives in the CLI layer, not in a shared SDK/runtime monitor.

#### Backend log source split evidence

- `src/kitaru/client.py:456-480` - `KitaruClient.executions.logs(...)` normalizes `source`, validates `limit` and `checkpoint`, hydrates the run, and rejects `checkpoint + source='runner'`.
- `src/kitaru/client.py:482-498` - `source='runner'` fetches `GET /runs/{run.id}/logs` and returns run-level entries without checkpoint names.
- `src/kitaru/client.py:500-533` - default/non-runner retrieval walks `run.steps`, optionally filters by checkpoint name, fetches `GET /steps/{step.id}/logs`, maps entries with checkpoint names, sorts them, and applies `limit`.
- `src/kitaru/_client/_logs.py:15-20` - source normalization is intentionally permissive: any non-empty string is allowed after stripping/lowercasing. The Kitaru CLI documents `step` and `runner`, but the client helper itself does not hard-code only those two values.
- `src/kitaru/_client/_logs.py:115-149` - raw REST entries are mapped into Kitaru `LogEntry` objects with `source` and `checkpoint_name` fields.
- `src/kitaru/_client/_models.py:79-90` - `LogEntry` carries message, level, timestamp, source, checkpoint name, module, filename, and line number.

ZenML-side references explain why this split matters:

- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/zen_server/routers/runs_endpoints.py:440-532` - `/runs/{run_id}/logs?source=...` serves run-level logs. For `source == LOGS_RUNNER_SOURCE`, it may read workload-manager logs for the run/snapshot; otherwise it looks in the run log collection.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/zen_server/routers/steps_endpoints.py:280-326` - `/steps/{step_id}/logs?source=...` serves step-level logs from a step's log collection.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/execution/pipeline/dynamic/runner.py:511-517` - dynamic pipeline orchestration wraps pipeline execution in a logging context with `source="orchestrator"`.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/orchestrators/step_runner.py:145-154` - step execution wraps checkpoint work in a logging context with `source="step"`.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/pipelines/pipeline_definition.py:1043-1050` - client-side pipeline submission uses logging context `source="client"`.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/entrypoints/base_entrypoint_configuration.py:279-293` and `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/utils/code_utils.py:286` - the `Downloading code...` / `Code download finished.` messages come from ZenML entrypoint/code-download code, not from Kitaru's CLI follow loop.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/integrations/kubernetes/orchestrators/kubernetes_orchestrator.py:833-850` - synchronous Kubernetes submission returns a `wait_for_completion` callback that logs `Waiting for orchestrator job to finish...` and calls `kube_utils.wait_for_job_to_finish(..., stream_logs=True)`.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/integrations/kubernetes/kube_utils.py:950-1008` - Kubernetes job monitoring polls pod logs and re-logs newly observed lines. This is submitter-side pod-log mirroring, separate from Kitaru's backend log retrieval.
- `/Users/strickvl/coding/zenml/repos/zenml/src/zenml/integrations/kubernetes/flavors/kubernetes_orchestrator_flavor.py:195-199` - `stream_step_logs` is marked deprecated/unused, so there is no obvious Kubernetes setting that would simply turn live step-log streaming back on in this path.

#### Tests that prove current behavior

- `tests/test_flow.py:1139-1158` - `test_flow_handle_wait_polls_until_complete` proves `FlowHandle.wait()` polls `Client.get_pipeline_run(...)`, sleeps, and returns the final output. It does not assert any log retrieval.
- `tests/test_flow.py:1690-1727`, `tests/test_flow.py:1730-1767`, and `tests/test_flow.py:1799-1820` - wait/get tests focus on terminal analytics and single terminal-event emission, not streaming output.
- `tests/test_flow.py:2029-2050` - failed `wait()` includes a retry hint, again without log-follow behavior.
- `tests/test_cli.py:2129-2178` - JSON follow mode emits JSONL `log` events followed by a `terminal` event.
- `tests/test_cli.py:2242-2288` - text follow mode streams newly appearing log entries and exits successfully on completion.
- `tests/test_cli.py:2291-2384` - follow mode handles failed executions and recovery hints.
- `tests/test_cli.py:1454-1500` - deployment log follow reuses the same JSONL follow behavior under `flow.deployments.logs`.
- `tests/test_client.py:2470-2541` - step log retrieval merges multiple checkpoint log streams by timestamp and annotates checkpoint names.
- `tests/test_client.py:2590-2620` - runner log retrieval uses `/runs/{run.id}/logs` with `source='runner'`.
- `tests/test_client.py:2623-2646` and `tests/test_cli.py:2203-2221` - `checkpoint` filtering is intentionally invalid with `source='runner'`.

Coverage gap: there is no regression test that proves SDK `.run()` or `.run().wait()` tails backend logs while a remote/server-backed execution is running. The current tests make the opposite architecture visible: SDK wait tests are status/result tests; CLI follow tests are log-stream tests.

#### Git archaeology

- `23d8590` (`Add execution runtime log retrieval`, 2026-03-09) introduced `KitaruClient.executions.logs(...)`, `kitaru executions logs`, MCP `get_execution_logs`, tests, and docs. The commit body says it implements the runtime log retrieval lane with client, CLI, and MCP surfaces.
- `8d7fa54` (`Deep structural refactor (#30)`, 2026-03-15) moved the CLI follow implementation into `src/kitaru/_cli/_executions.py` and log normalization helpers into `src/kitaru/_client/_logs.py`. Current blame for most of `_follow_execution_logs(...)` points here.
- `93bae3f` (`Show recovery hint after flow failure traceback (#120)`, 2026-04-10) added recovery-hint behavior to failed follow/wait surfaces.
- `cb8bae9` (`Implement serverless versioned deployments (SDK + CLI + MCP + docs) (#210)`, 2026-04-23) generalized `_follow_execution_logs(...)` with a `command` argument and reused it for deployment logs.

Design intent appears to have been: ship log retrieval as a separate inspection/follow surface, using ZenML REST log endpoints and a polling/dedup loop. There is no evidence that this was ever wired into SDK `.run()` or `FlowHandle.wait()`.

#### Eliminated or narrowed hypotheses

- **Not a Kitaru terminal rewrite/drop-rule issue.** `src/kitaru/_terminal_logging.py:1-10` says Kitaru rewrites terminal strings without mutating `LogRecord` objects, and keeps ZenML's storage handler untouched. The missing live checkpoint output is not explained by Kitaru dropping stored logs.
- **Not a broken CLI follow loop.** CLI follow has focused tests and concrete polling behavior. Users can run `kitaru executions logs --follow <exec_id>` because this path is independent and works by backend polling.
- **Not simply `FlowHandle.wait()` causing the whole silence window.** `FlowHandle.wait()` really is status-only, but synchronous Kubernetes `.run()` can block inside ZenML's `configured_pipeline(...)` before the handle is returned. So a fix only inside `wait()` may help async/deployment-style handles, but may not fix the observed `Code download finished` silence if the blocking call has not returned yet.

#### Recommended fix locations

1. **Extract the log-follow core out of CLI presentation code.** Move the polling/dedup/status loop from `src/kitaru/_cli/_executions.py:268-390` into a non-CLI module (for example `src/kitaru/_execution_logs.py` or `src/kitaru/_runtime_monitoring.py`). Keep CLI formatting callbacks in `_cli/_executions.py`, but make the loop reusable by SDK code.
2. **Decide where SDK follow can actually start.** If `.run()` remains synchronous for Kubernetes, `FlowHandle.wait()` alone is too late for the exact issue. The fix likely needs a pre-handle path around `src/kitaru/flow.py:1132-1138`, or a way to submit asynchronously / obtain the placeholder run id before ZenML's synchronous monitor blocks.
3. **Add an SDK-facing option or default for remote runs.** Candidate surfaces: `flow.run(..., follow_logs: bool | None = None)`, `FlowHandle.wait(follow_logs: bool = False)`, or automatic follow for inferred remote deployment types. Be careful: current `FlowHandle.wait()` has no parameters, so adding optional behavior is backward-compatible but tests/docs need updating.
4. **Prefer step logs for the missing checkpoint output, possibly plus runner/orchestrator markers.** The user-visible missing logs after code download are checkpoint/step/backend logs. Kitaru's existing default `source='step'` matches this. If run-level progress is also desired, consider a combined monitor that polls `source='runner'` or `source='orchestrator'` plus step logs, but avoid hiding the existing distinction.
5. **Regression tests to add before/with the fix:**
   - A `FlowHandle.wait(follow_logs=True)` or equivalent unit test proving it calls a shared follow/log polling helper and streams new entries before completion.
   - A `.run()`/`_submit()` test for the synchronous-remote case, proving the chosen design can start monitoring before or during `configured_pipeline(...)` if that is the intended fix.
   - CLI tests proving `kitaru executions logs --follow` still emits the same text and JSONL contracts after extracting the shared helper.
   - Client tests for any new combined source behavior if runner/orchestrator + step aggregation is introduced.


## Investigation Log

### Phase 1 - Initial issue triage
**Hypothesis:** The submitter path already learns the execution id, but remote `.run()` waits on orchestration completion without tailing the backend logs until after completion.
**Findings:** Issue #202 describes a silence window after `Code download finished` while the remote execution keeps progressing in the cluster and dashboard.
**Evidence:** GitHub issue #202 body, fetched 2026-04-24.
**Conclusion:** Needs code-path investigation around remote `.run()`, execution monitoring, and `kitaru executions logs --follow` plumbing.

### Phase 1.5 - External/git fact gathering
**Hypothesis:** Existing git history and upstream ZenML/Kubernetes behavior would clarify whether this was a new regression or a missing integration between already-existing pieces.
**Findings:** Agent-mode explore probes were attempted but failed immediately because the workspace Agent Mode API quota is exhausted until 2026-05-01. Direct fallback git archaeology found that runtime log retrieval was introduced as a separate client/CLI/MCP lane in `23d8590` (2026-03-09), structurally refactored in `8d7fa54` (2026-03-15), failure recovery hints were added in `93bae3f` (2026-04-10), and deployment logs reused the CLI follow helper in `cb8bae9` (2026-04-23). None of these commits wired the follow loop into SDK `.run()` / `FlowHandle.wait()`.
**Evidence:** `git show --no-patch 23d8590 8d7fa54 93bae3f cb8bae9`; current code references in `src/kitaru/flow.py`, `src/kitaru/_cli/_executions.py`, and `src/kitaru/client.py`.
**Conclusion:** The history supports a design-separation explanation: log following exists, but as a separate inspection surface rather than SDK run/wait behavior.

### Phase 2-4 - Context builder, pair investigator, and Oracle synthesis
**Hypothesis:** The root cause might be either missing SDK log following, backend log freshness, or ZenML Kubernetes pod-log streaming behavior.
**Findings:** `context_builder`, the pair investigator, and Oracle all converged on the same two-layer diagnosis: Kitaru's SDK path does not tail logs, and synchronous Kubernetes can block inside ZenML before the SDK has a handle. Backend freshness remains a diagnostic to verify with a live run, but it is not needed to explain why Kitaru SDK `.wait()` itself does not stream logs.
**Evidence:** `FlowHandle.wait()` and `_refresh()` in `src/kitaru/flow.py:671-726`; `_submit()` handle creation after `configured_pipeline(...)` in `src/kitaru/flow.py:1094-1151`; CLI follow loop in `src/kitaru/_cli/_executions.py:268-390`; Kubernetes synchronous monitor in ZenML `kubernetes_orchestrator.py:833-850` and `kube_utils.py:950-1008`.
**Conclusion:** Confirmed. Recommended fixes must address both post-handle log following and pre-handle synchronous remote submission behavior.

## Root Cause
Kitaru's Python SDK currently does not own live log following for remote `.run()` executions. There are two concrete layers:

1. **After a `FlowHandle` exists, `FlowHandle.wait()` is status-only.** In `src/kitaru/flow.py:671-690`, `wait()` calls `_refresh()`, checks whether the run is terminal, and sleeps. `_refresh()` in `src/kitaru/flow.py:715-726` calls `Client().get_pipeline_run(...)`; it never calls `KitaruClient.executions.logs(...)` or the CLI follow helper. So `handle.wait()` watches the run's traffic light, not the log stream.
2. **For synchronous Kubernetes stacks, the exact silence can happen before `FlowHandle.wait()` starts.** `_FlowDefinition._submit()` calls `configured_pipeline(*args, **kwargs)` in `src/kitaru/flow.py:1132-1137` and only returns `FlowHandle(...)` at `src/kitaru/flow.py:1147-1151`. ZenML's Kubernetes orchestrator can make that call block in a synchronous `wait_for_completion` callback (`zenml/.../kubernetes_orchestrator.py:833-850`) that tails Kubernetes pod logs via `wait_for_job_to_finish(..., stream_logs=True)` (`zenml/.../kube_utils.py:950-1008`). The `Code download finished.` line comes from ZenML entrypoint code (`zenml/.../base_entrypoint_configuration.py:279-293`), i.e. that bootstrap/orchestrator stream, not Kitaru's backend log-follow surface.

The practical story is: Kitaru starts the run, ZenML's synchronous Kubernetes monitor mirrors bootstrap pod logs until code download finishes, then the real checkpoint output lives in ZenML run/step log APIs. Kitaru already has a CLI loop that can poll those APIs (`src/kitaru/_cli/_executions.py:268-390`, invoked by `kitaru executions logs --follow` at `src/kitaru/_cli/_executions.py:590-618`), but the SDK `.run()` / `.wait()` path never reuses it. Therefore the terminal appears hung even though the backend execution is healthy.

Eliminated / narrowed hypotheses:

- Not a `FlowHandle.wait()` polling interval problem: it does not fetch logs at all.
- Not primarily a broken CLI follow loop: tests prove CLI follow streams incrementally when snapshots contain new entries (`tests/test_cli.py:2129-2178`, `tests/test_cli.py:2242-2288`).
- Not a Kitaru terminal rewrite issue: the terminal interceptor is separate from backend log retrieval.
- Not fully fixable by adding log-follow only to `FlowHandle.wait()`: that helps post-handle waiting, but not the synchronous Kubernetes period before the handle exists.
- Backend log freshness is still worth verifying with a live remote run. If `/steps/{step_id}/logs` only exposes entries after completion, then there is an additional ZenML/log-store freshness issue. But Kitaru's missing SDK follow path is already proven from code.

## Recommendations
1. **Extract the follow loop out of CLI code into a shared runtime/log monitor.** Move the polling, deduplication, status checking, and waiting-state handling from `src/kitaru/_cli/_executions.py:268-390` into a non-CLI module such as `src/kitaru/_client/_log_follow.py` or `src/kitaru/_runtime_monitoring.py`. Keep CLI text/JSONL rendering as callbacks/adapters.
2. **Make SDK waiting capable of following logs.** Add an explicit or auto-enabled log-follow path to `FlowHandle.wait()` in `src/kitaru/flow.py:671-690`, reusing the shared monitor rather than duplicating CLI logic. A conservative API would be `wait(logs: bool = False, ...)`; a UX-forward API would default to live logs for interactive remote runs with an opt-out.
3. **Fix the pre-handle synchronous Kubernetes window.** A `wait()`-only change is too late for the exact issue if `_submit()` is blocked inside `configured_pipeline(...)`. Investigate making remote/Kubernetes submission asynchronous enough that `_submit()` can return a `FlowHandle` soon after run creation, or wrap the `configured_pipeline(...)` call with a Kitaru-owned progress/log monitor if the run id can be obtained before terminal completion.
4. **Support the right log sources for a coherent remote stream.** Default Kitaru step logs come from `/steps/{step.id}/logs`, while `source='runner'` uses `/runs/{run.id}/logs` (`src/kitaru/client.py:456-533`). For remote `.run()` UX, consider an `auto`/`all` mode that aggregates runner/orchestrator/client run-level logs plus step logs, then sorts/deduplicates chronologically.
5. **Add regression tests before or alongside the fix.** Cover: SDK wait with log-follow enabled; SDK wait with log-follow disabled preserving current behavior; `_submit()` returning a handle promptly for the remote/synchronous case or otherwise starting monitoring before the blocking call; CLI follow behavior unchanged after extraction; and any new combined-source behavior in `tests/test_client.py`.

## Preventive Measures
- Treat live execution monitoring as a shared SDK/runtime service, not a CLI-only helper.
- Add a regression test whenever a user-facing command gains streaming/progress behavior to ensure the analogous SDK surface is either intentionally wired or intentionally documented as separate.
- Document source semantics (`step`, `runner`, and any future `auto`/`all`) so users understand where bootstrap logs versus checkpoint logs come from.
- Add a lightweight remote-stack smoke test or mocked integration test that verifies long-running remote `.run()` calls continue to emit progress before terminal completion.
