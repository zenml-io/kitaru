# Investigation: Improve UX for Missing Stack Integration Dependencies

## Summary
Kitaru issue #192 is caused by a missing translation boundary around ZenML active-stack implementation hydration during flow submission. `kitaru stack use` succeeds because it selects stack metadata, but `flow.run(...)` later causes ZenML to instantiate live stack components; when an integration dependency such as `boto3` is missing, ZenML raises an actionable `ImportError` and Kitaru currently lets the chained traceback escape.

## Symptoms
- Issue #192 reports that `uv run kitaru stack use local_remote` succeeds for an S3-backed stack, but the first flow run fails while loading/hydrating the active stack.
- The failure includes `ModuleNotFoundError: No module named 'boto3'`, followed by ZenML's actionable message explaining that the `s3` integration is missing and suggesting `zenml integration install s3` or manual package installs.
- Desired Kitaru behavior: catch this class of missing integration dependency error and show a short, actionable message by default, preserving the original traceback for debug/verbose output.

## Background / Prior Research
- GitHub issue: https://github.com/zenml-io/kitaru/issues/192
- Issue created by `strickvl` on 2026-04-17 and still open as of 2026-04-24.
- Related issue/PR named in the report: #183 / PR #189, where this surfaced during manual testing of a `local_remote` stack with an S3 artifact store.
- Delegated explore-agent fact gathering was attempted on 2026-04-24, but both sessions failed immediately because the workspace API quota is exhausted until 2026-05-01. The remaining investigation therefore uses local `gh`, git, RepoPrompt context tools, and direct evidence gathering.
- Related issue #183 (`memory.get(...)` should degrade gracefully when the entry's artifact store is unreachable) was closed by PR #189 (`Degrade memory.get gracefully when the artifact is unreachable`), merged on 2026-04-17 as merge commit `1592a38cbe23c2eb5b839488bcf499619768d0ef`. PR #189 mostly touched memory surfaces (`src/kitaru/memory.py`, `_interface_memory.py`, `_cli/_memory.py`, MCP/docs/tests), not general stack hydration.
- Upstream ZenML raises/re-raises missing integration dependency failures from `StackComponent.from_model(...)` in `zenml/src/zenml/stack/stack_component.py:420-479`. It catches an `ImportError` from the provider implementation class and raises a new `ImportError` whose message includes the integration name and install guidance.
- `Stack.from_model(...)` hydrates all stack components via `StackComponent.from_model(...)` in `zenml/src/zenml/stack/stack.py:160-201`; when an `ImportError` occurs, it appends a stack-wide `zenml stack export-requirements ...` hint and re-raises.

## Investigator Findings
<!-- Pair investigator appends structured analysis here with file:line refs, evidence, and conclusions. -->

### 2026-04-24 - Root-cause trace of issue #192

**Conclusion:** the working hypothesis is confirmed. `flow.run(...)` reaches ZenML's active-stack *implementation* hydration during pipeline submission, and Kitaru does not currently translate ZenML's missing-integration `ImportError` at that SDK boundary. `kitaru stack use` succeeds because it activates/returns stack *metadata* (`StackResponse` / Kitaru `StackInfo`) and does not instantiate the live ZenML `Stack` implementation. `kitaru stack show` fetches hydrated metadata for display, but it also does not call `Stack.from_model(...)`, so it is not the same failure point as a flow submission.

#### Flow/run path: the missing-integration error escapes during submission

- `src/kitaru/flow.py:785-815` makes `flow.run(...)` a thin wrapper around `_FlowDefinition._submit(...)`.
- `_submit(...)` resolves config and creates a ZenML pipeline configured with Kitaru options at `src/kitaru/flow.py:1111-1130`.
- The actual submission happens inside the temporary stack context at `src/kitaru/flow.py:1132-1138`:
  - enter `_temporary_active_stack(resolved_execution.stack)`;
  - collect deployment metadata;
  - call `configured_pipeline(*args, **kwargs)`.
- There is no `try/except ImportError` or broader user-error translation around that `configured_pipeline(...)` call in `src/kitaru/flow.py:1132-1138`. Any exception raised by ZenML stack hydration therefore propagates out of the SDK call path unchanged.
- `_temporary_active_stack(...)` itself only switches/restores active stack IDs with `Client().activate_stack(...)` at `src/kitaru/flow.py:82-101`; it does not hydrate the active stack implementation before yielding.

ZenML then performs the implementation hydration:

- ZenML `Pipeline.__call__(...)` prepares and runs the pipeline at `../zenml/src/zenml/pipelines/pipeline_definition.py:1585-1616`.
- ZenML `_run()` reads `stack = Client().active_stack` before snapshot creation/submission at `../zenml/src/zenml/pipelines/pipeline_definition.py:1047-1064`, then later passes that stack into `submit_pipeline(...)` at `../zenml/src/zenml/pipelines/pipeline_definition.py:1119-1121`.
- `Client.active_stack` is the implementation-hydrating property: it returns `Stack.from_model(self.active_stack_model)` at `../zenml/src/zenml/client.py:1512-1520`.
- `Stack.from_model(...)` builds a live `Stack` by listing hydrated component models and calling `StackComponent.from_model(...)` for each component at `../zenml/src/zenml/stack/stack.py:179-192`.
- When a component implementation import fails, `StackComponent.from_model(...)` catches `ImportError` and re-raises a new `ImportError` with the integration install guidance at `../zenml/src/zenml/stack/stack_component.py:447-479`.
- `Stack.from_model(...)` catches that `ImportError`, appends the stack-wide `zenml stack export-requirements ...` hint, mutates `e.args`, and re-raises at `../zenml/src/zenml/stack/stack.py:188-202`.

Plain-language chain of events: Kitaru says "run this flow"; ZenML asks "what active stack am I running on?"; ZenML turns the stack database record into live Python objects; the S3 artifact-store object imports its provider dependencies; `boto3` is missing; ZenML rewrites the error with install guidance; Kitaru does not catch/repackage it at `flow.run`, so Python shows the long traceback plus ZenML's useful message.

#### Stack command paths: metadata activation vs implementation hydration

- CLI `kitaru stack use` delegates to the facade's `set_active_stack` at `src/kitaru/_cli/_stacks.py:479-504`.
- The facade maps `set_active_stack` to `kitaru.config.use_stack` via `src/kitaru/cli.py:229-243`.
- `kitaru.config.use_stack(...)` delegates to `_config_stacks.use_stack(...)` and then tracks activation at `src/kitaru/config.py:683-706`.
- `_config_stacks.use_stack(...)` normalizes the selector, resolves it from available stack metadata, calls `client.activate_stack(resolved_stack.id)`, and returns `current_stack()` at `src/kitaru/_config/_stacks.py:2615-2634`.
- `current_stack()` reads `client_factory().active_stack_model` and converts it to Kitaru `StackInfo` at `src/kitaru/_config/_stacks.py:2551-2561`.
- ZenML `activate_stack(...)` validates via `get_stack(...)` and stores the selected stack in config at `../zenml/src/zenml/client.py:1559-1585`; this path uses `StackResponse` metadata, not `Stack.from_model(...)`.

So the user's observation is expected: `kitaru stack use local_remote` can succeed even when the stack's provider package is missing locally, because activation is "point the active-stack pointer at this record", not "instantiate every component implementation now".

`stack show` is slightly different but still not the same as a flow run:

- CLI `kitaru stack show` delegates to `_show_stack_operation(...)` at `src/kitaru/_cli/_stacks.py:450-476`.
- The facade delegates to `_config_stacks._show_stack_operation(...)` at `src/kitaru/config.py:491-496`.
- `_show_stack_operation(...)` resolves the selector, calls `client.get_stack(resolved_stack.id, hydrate=True)`, and translates returned component metadata into Kitaru display rows at `src/kitaru/_config/_stacks.py:1997-2025`.
- The component display helper explicitly translates a hydrated ZenML component model at `src/kitaru/_config/_stacks.py:1642-1648`; it reads fields like flavor/configuration/connector metadata, not implementation instances.
- ZenML `Client.get_stack(..., hydrate=True)` returns a `StackResponse` model at `../zenml/src/zenml/client.py:1249-1277`. That is response/model hydration, not live implementation hydration. The live implementation hydration point remains `Client.active_stack -> Stack.from_model(...)` at `../zenml/src/zenml/client.py:1512-1520`.

#### CLI and MCP boundaries / existing translation patterns

- Kitaru already has a shared CLI boundary: `run_with_cli_error_boundary(...)` catches configured exceptions, translates them to `InterfaceErrorDetails`, and emits text/JSON errors at `src/kitaru/_interface_errors.py:35-63`.
- The CLI emitter prints `Error: ...` for text mode or structured JSON for JSON mode, then exits with status 1 at `src/kitaru/_cli/_helpers.py:247-273`.
- Stack commands already use that boundary:
  - `stack show` wraps `_show_stack_operation(...)` at `src/kitaru/_cli/_stacks.py:459-466`;
  - `stack use` wraps `set_active_stack(...)` at `src/kitaru/_cli/_stacks.py:488-495`.
- `kitaru build` / `kitaru deploy` show the main existing pattern for adding operation-specific remediation: `_translate_build_or_deploy_error(...)` appends stack-selection guidance for `StackNotRemoteExecutable` and CLI input guidance for deployment input errors at `src/kitaru/_cli/_flows.py:83-105`, and those commands pass that translator into the shared boundary at `src/kitaru/_cli/_flows.py:445-451` and `src/kitaru/_cli/_flows.py:533-539`.
- The tests confirm this pattern: `tests/test_cli.py:720-746` asserts build/deploy errors include both `--stack <stack>` and `kitaru stack use <stack>`.

MCP is intentionally thinner:

- `run_with_mcp_error_boundary(...)` re-raises original exceptions unless a translator is explicitly provided at `src/kitaru/_interface_errors.py:65-77`.
- `tracked_mcp_tool(...)` records success/failure analytics and then re-raises exceptions at `src/kitaru/mcp/server.py:63-92`.
- `manage_stack(...)` uses `run_with_mcp_error_boundary(_manage_stack)` without a translator at `src/kitaru/mcp/server.py:720-787`.
- The MCP stack tool surface only creates/deletes stacks today (`action: Literal["create", "delete"]` at `src/kitaru/mcp/server.py:720-741`); there is no MCP `stack use` or `stack show` equivalent in this file. Stack listing exists as `kitaru_stacks_list()` at `src/kitaru/mcp/server.py:709-717` and serializes stack metadata entries.

The closest reusable precedent from #183/#189 is the memory-unreachable design:

- Core memory loading catches generic artifact load failures, warns/returns `None` in lenient mode, or raises typed `KitaruMemoryArtifactUnavailableError` in strict mode at `src/kitaru/memory.py:1351-1397` (test evidence).
- The interface layer converts an unavailable value into structured payload metadata (`value_available: False`, `value_unavailable: {...}`) at `tests/test_interface_memory.py:174-238`.
- MCP strict mode intentionally re-raises the typed unavailable error unchanged at `tests/mcp/test_server.py:1111-1113`.

#### Root cause

The root cause is not stack activation. The root cause is a missing Kitaru SDK/interface translation around ZenML active-stack implementation hydration during flow submission:

1. Stack activation stores/selects metadata and can succeed without importing provider implementation dependencies.
2. Flow submission calls ZenML's pipeline execution path.
3. ZenML's execution path asks for `Client().active_stack`, which calls `Stack.from_model(...)`.
4. `Stack.from_model(...)` instantiates stack components, triggering provider imports.
5. ZenML rewrites the missing provider dependency into an actionable `ImportError`, but Kitaru lets that `ImportError` escape from `flow.run(...)`.
6. CLI commands that load and run/deploy local flow targets would only get a short Kitaru-formatted error if the exception reaches a `run_with_cli_error_boundary(...)`; direct SDK users still see the raw traceback unless Kitaru translates at the SDK boundary. MCP mostly preserves typed/raw exceptions unless a translator is added deliberately.

#### Suggested fix direction for a later implementation

- Add a small translator for ZenML missing-integration `ImportError` messages, probably close to `kitaru.errors` or `_interface_errors`, that recognizes the stable ZenML guidance fragments (`zenml integration install`, `integration`, `export-requirements`) without depending on ZenML private exception classes.
- Use it at the SDK boundary around `configured_pipeline(*args, **kwargs)` in `src/kitaru/flow.py:1132-1138` so direct `flow.run(...)` users get a short `KitaruUsageError`/`KitaruBackendError`-style message with the original `ImportError` as `__cause__`.
- Reuse the same translator in CLI-specific boundaries if needed, but avoid losing structured JSON behavior from `run_with_cli_error_boundary(...)`.
- For MCP, decide explicitly whether to preserve the typed Kitaru exception (current MCP style) or translate to a structured payload. Current precedent favors raising a typed Kitaru exception and letting MCP clients receive that, rather than returning a "successful" error payload.

## Investigation Log

### Phase 1 - Initial Assessment
**Hypothesis:** Missing provider integration dependencies are raised during ZenML stack hydration/resolution, probably inside Kitaru runtime flow execution rather than during `kitaru stack use`.
**Findings:** Issue #192 states stack activation succeeded but the first flow run failed while loading the active stack.
**Evidence:** Issue #192 reproduction and desired behavior text.
**Conclusion:** Needs code-path tracing around stack activation, stack loading, and flow/checkpoint execution.



### Phase 1.5 - Related GitHub / Upstream Context
**Hypothesis:** The noisy traceback came from new memory-unavailable handling in #183/#189.
**Findings:** PR #189 fixed memory artifact unavailability behavior and mostly touched memory surfaces. It did not introduce a general stack hydration translation layer. Upstream ZenML already emits actionable missing-integration text in `StackComponent.from_model(...)` and appends stack-wide requirements guidance in `Stack.from_model(...)`.
**Evidence:** PR #189 merge commit `1592a38cbe23c2eb5b839488bcf499619768d0ef`; `../zenml/src/zenml/stack/stack_component.py:447-479`; `../zenml/src/zenml/stack/stack.py:188-202`.
**Conclusion:** #183/#189 is related as the manual-testing context, but the root cause for #192 is the unhandled stack-hydration `ImportError` path.

### Phase 2 - Context Builder / Oracle Initial Assessment
**Hypothesis:** The relevant Kitaru files are flow submission, stack activation/show paths, and CLI/MCP error boundaries.
**Findings:** Context Builder selected `flow.py`, `_config/_stacks.py`, CLI boundary files, MCP boundary files, Kitaru errors/tests, and upstream ZenML hydration files. Its initial Oracle answer identified `_FlowDefinition._submit(...)` as the primary escape point.
**Evidence:** `src/kitaru/flow.py:1132-1138`; `src/kitaru/_config/_stacks.py:2615-2634`; `src/kitaru/_interface_errors.py`; `../zenml/src/zenml/client.py:1512-1520`.
**Conclusion:** Confirmed the investigation should focus on SDK flow submission and active-stack implementation hydration, not stack selection alone.

### Phase 3 - Pair Investigator Trace
**Hypothesis:** `flow.run(...)` hydrates the active stack only inside ZenML pipeline execution, while stack commands operate on metadata.
**Findings:** Confirmed. `_temporary_active_stack(...)` only activates/restores stack IDs. `_submit(...)` calls `configured_pipeline(...)` without translating ZenML `ImportError`. ZenML `Pipeline._run()` reads `Client().active_stack`, which calls `Stack.from_model(...)`, which calls `StackComponent.from_model(...)` and can raise the missing-integration `ImportError`.
**Evidence:** `src/kitaru/flow.py:82-101`; `src/kitaru/flow.py:1132-1138`; `../zenml/src/zenml/pipelines/pipeline_definition.py:1055`; `../zenml/src/zenml/client.py:1512-1520`; `../zenml/src/zenml/stack/stack.py:179-202`; `../zenml/src/zenml/stack/stack_component.py:447-479`.
**Conclusion:** Confirmed root cause.

### Phase 4 - Oracle Synthesis
**Hypothesis:** The best fix is to catch this narrowly by preflight-hydrating the active stack, not by broadly wrapping all `ImportError`s from pipeline submission.
**Findings:** Oracle agreed and emphasized avoiding over-catching arbitrary user-code `ImportError`. The cleanest fix is to call `Client().active_stack` immediately after `_temporary_active_stack(...)` enters, translate only `ImportError` from that stack-hydration operation into a typed Kitaru error, then call `configured_pipeline(...)` only if preflight succeeds.
**Evidence:** `src/kitaru/flow.py:1132-1138`; `../zenml/src/zenml/client.py:1512-1520`.
**Conclusion:** Recommended fix should be scoped to the stack-hydration doorway.

## Root Cause
The root cause is a missing Kitaru SDK/interface translation around ZenML active-stack implementation hydration during flow submission.

Concrete chain:

1. `kitaru stack use local_remote` activates a stack by metadata. Kitaru resolves the stack and calls `client.activate_stack(resolved_stack.id)` in `src/kitaru/_config/_stacks.py:2615-2634`, then returns `current_stack()`, which reads `active_stack_model` metadata in `src/kitaru/_config/_stacks.py:2551-2561`.
2. `flow.run(...)` enters `_FlowDefinition._submit(...)`. Inside `src/kitaru/flow.py:1132-1138`, Kitaru enters `_temporary_active_stack(...)`, gathers metadata, and calls `configured_pipeline(*args, **kwargs)` with no Kitaru translation around ZenML missing-integration `ImportError`.
3. `_temporary_active_stack(...)` itself only reads/restores stack IDs via `Client().active_stack_model` and `Client().activate_stack(...)` in `src/kitaru/flow.py:82-101`; it does not instantiate the live stack.
4. ZenML pipeline execution then reads `Client().active_stack` in `../zenml/src/zenml/pipelines/pipeline_definition.py:1055`.
5. ZenML `Client.active_stack` calls `Stack.from_model(self.active_stack_model)` in `../zenml/src/zenml/client.py:1512-1520`.
6. ZenML `Stack.from_model(...)` instantiates each component via `StackComponent.from_model(...)` in `../zenml/src/zenml/stack/stack.py:179-192`.
7. If an integration package is missing, `StackComponent.from_model(...)` catches the provider import failure and raises an actionable `ImportError` with `zenml integration install <integration>` guidance in `../zenml/src/zenml/stack/stack_component.py:447-479`; `Stack.from_model(...)` appends stack-wide `zenml stack export-requirements ...` guidance in `../zenml/src/zenml/stack/stack.py:188-202`.
8. Kitaru does not convert that expected environment/setup failure into a concise typed Kitaru error, so direct SDK users see the full Python chained traceback around ZenML's useful guidance.

Plain-language version: `stack use` is like pointing Kitaru at a stack record in the database. The first flow run is when ZenML opens the engine bay and imports the actual provider-specific Python classes. If the S3 part needs `boto3` and `boto3` is missing, ZenML explains how to install it, but Kitaru currently does not put a clean user-facing frame around that explanation.

Eliminated hypotheses:

- **ZenML lacks actionable guidance:** rejected. ZenML already generates integration install and stack requirements guidance.
- **`kitaru stack use` should fail:** rejected. It intentionally works at metadata-selection level and should probably stay lightweight.
- **`kitaru stack show` proves local executability:** rejected/nuanced. It fetches hydrated response metadata via `client.get_stack(..., hydrate=True)`, but it does not call `Stack.from_model(...)` and therefore is not equivalent to implementation hydration.
- **Flow target loading is the culprit:** rejected. `_flow_loading` only imports/validates the user target; the failure happens later during ZenML active-stack hydration.

## Recommendations
1. **Add a typed Kitaru error** in `src/kitaru/errors.py`, e.g. `KitaruStackIntegrationDependencyError`, for “the selected/active stack cannot be hydrated because local integration dependencies are missing.” Prefer a runtime/setup classification over `KitaruBackendError`, because the server/backend may be fine; the local Python environment is incomplete.
2. **Add a narrow active-stack hydration preflight** in `src/kitaru/flow.py`. Inside `_FlowDefinition._submit(...)`, after entering `_temporary_active_stack(resolved_execution.stack)` and before calling `configured_pipeline(*args, **kwargs)`, intentionally access `Client().active_stack`. Catch `ImportError` only from that operation and translate it to the typed Kitaru error.
3. **Do not broadly catch `ImportError` around `configured_pipeline(...)`.** That would risk misclassifying arbitrary user-code import failures as stack dependency problems. The catch should live at the precise stack-hydration doorway.
4. **Raise the translated error without exception chaining** for default UX, e.g. `raise KitaruStackIntegrationDependencyError(message) from None`, while preserving/logging debug context if needed. The message should keep ZenML's actionable install guidance and add a short Kitaru frame such as: “The active stack cannot be used from this Python environment because one of its ZenML integration dependencies is missing.”
5. **Consider reusing the helper in replay/deploy paths** that run under `_temporary_active_stack(...)`, since those can also hit ZenML stack implementation hydration.
6. **Leave `kitaru stack use` lightweight by default.** Do not force implementation hydration during stack activation. If desired later, add an explicit diagnostic/preflight command such as `kitaru stack check` or `kitaru stack doctor`.
7. **CLI/MCP surfaces:** once the SDK raises a typed Kitaru error, CLI boundaries should format it normally through `run_with_cli_error_boundary(...)`; build/deploy translators can optionally append stack-selection guidance. MCP should avoid adding a translator that re-chains the original exception unless the boundary supports `from None` behavior.

Recommended tests:

- `tests/test_flow.py`: preflight `Client().active_stack` raises ZenML-style `ImportError`; assert `KitaruStackIntegrationDependencyError`, message includes `zenml integration install` and `zenml stack export-requirements`, `configured_pipeline` is not called, and the old stack is restored.
- `tests/test_flow.py`: generic `configured_pipeline(...)` failure still propagates as before.
- `tests/test_flow.py`: user-code-like `ImportError` from `configured_pipeline(...)` is **not** translated into a stack integration error.
- `tests/test_config.py`: `use_stack(...)` does not touch `client.active_stack`; it should still succeed when only `active_stack_model` metadata is available.
- `tests/test_cli.py`: build/deploy or run-facing CLI path formats the typed error without traceback in text/JSON output.
- `tests/mcp/test_server.py`: MCP execution path preserves or cleanly surfaces the typed Kitaru error without reintroducing chained traceback noise.

## Preventive Measures
- Keep a clear distinction in tests and docs between **stack metadata selection** (`active_stack_model`, `activate_stack`, `get_stack(..., hydrate=True)` response models) and **stack implementation hydration** (`Client().active_stack`, `Stack.from_model(...)`).
- Add regression tests that protect against broad `ImportError` catching, especially user-code import errors during pipeline submission.
- Prefer typed Kitaru setup/runtime errors for common environment mismatches so CLI, MCP, and SDK surfaces can share concise remediation messages.
- If stack dependency issues become common, add an explicit preflight/doctor command rather than overloading `kitaru stack use` with environment validation.
