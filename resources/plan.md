# Plan: Remove deterministic evaluator provenance hashes (issue #899)

## Root cause

Every deterministic evaluator bundle in
`plugins/packages/evaluator/src/kitaru_evaluator/deterministic.py` opens its
result list with `_base_results(view, config)`
(`deterministic.py:177-195`), which unconditionally emits two
`EvaluationResult` rows ahead of the bundle's real diagnostics:

- `input_sha256` — `_hash(_input_envelope(view))`, a SHA-256 of the
  materialized session/node fields (`_input_envelope`, `deterministic.py:127-174`).
- `config_sha256` — `_hash(normalized_config)`, a SHA-256 of the evaluator's
  normalized parameters.

These were a workaround for two gaps that no longer exist: retries could
observe a different materialized session (no immutability guarantee on
terminal sessions) and callers had no way to see what parameters an
evaluator actually ran with. Both gaps are closed now — terminal sessions are
immutable, and `EvaluationResponse.evaluator_params` already carries the
literal params the evaluator ran with
(`src/kitaru/api_models/v1/evaluation.py:188-190`, added per the
`[Unreleased]` `CHANGELOG.md` entry "Added `evaluator_params` to the
evaluation response"). So `input_sha256`/`config_sha256` are now pure
internal-provenance noise leaking into user-facing evaluation results, and
every one of the ten deterministic bundles emits them via the shared
`_base_results` helper. `expected_sha256`/`observed_sha256` inside
`output_contract`'s `exact_output` result are a different mechanism (they
bound a potentially large compared payload, computed inline via `_hash(...)`,
not through `_base_results`) and are explicitly out of scope.

Confirmed nothing outside the evaluator package depends on these two field
names: `grep -rn "input_sha256\|config_sha256"` across the repo (code, tests,
OpenAPI spec, TS types) only hits `deterministic.py`,
`plugins/tests/evaluators/test_deterministic.py`, and
`docs/book/guides/deterministic-evaluations.md`.

**Coordination note:** the unmerged remote branch
`origin/codex/issue-941-evaluator-scales` (a different, broader change —
"Emit builtin evaluator score scales") independently deletes
`_base_results`/`_input_envelope`/`_hash`-based provenance as a side effect
of converting most JSON findings to numeric scores, and *also* drops
`expected_sha256`/`observed_sha256` from `exact_output`, which conflicts with
this issue's explicit instruction to keep them. Do not use that branch as a
template beyond confirming the mechanical deletion of `_base_results`/
`_input_envelope`; if it merges before or after this change, `exact_output`
will need reconciling by hand since the two branches disagree on whether the
hashes stay.

## Code changes

### 1. `plugins/packages/evaluator/src/kitaru_evaluator/deterministic.py`

Delete the now-dead provenance scaffolding:

- Delete `_ENCODING_REVISION = 1` (line 29) — only consumers are the two
  functions below.
- Delete `_input_envelope()` (lines 127-174) — its sole caller is
  `_base_results`.
- Delete `_base_results()` (lines 177-195) — including the
  `"configuration must contain only finite JSON values"` check, which is
  unreachable in practice (every config passed to it today is already built
  from strings/ints/pre-validated JSON; nothing currently exercises the
  raise).

Do **not** touch `_hash()` (line 110-112) or `_canonical_json()`
(line 99-107) — both stay, because `output_contract`'s `exact_output` result
calls `_hash()` directly for `expected_sha256`/`observed_sha256`
(deterministic.py:688-691), and `_json_result`'s oversized-result truncation
path also hashes via `hashlib.sha256(...)` directly (deterministic.py:213).
Keep `_token_fields()` too — it's used outside `_input_envelope` in
`session_diagnostics`'s resource-integrity checks.

At each of the ten call sites, replace the `_base_results(...)` call with an
empty, explicitly typed list, and delete any `config`/dict-building code that
existed solely to feed `_base_results` (nothing else reads it):

| Function | Current | Change |
|---|---|---|
| `session_diagnostics` (~465) | `results = _base_results(session, {})` | `results: list[EvaluationResult] = []` |
| `output_contract` (~659-666) | builds `config` dict from `expected`/`paths`/`type_requirements`, then `results = _base_results(session, config)` | delete the `config` block; `results: list[EvaluationResult] = []` |
| `trajectory_signals` (~835) | `results = _base_results(session, {})` | `results: list[EvaluationResult] = []` |
| `tool_health` (~916) | `results = _base_results(session, {})` | `results: list[EvaluationResult] = []` |
| `timing_profile` (~990) | `results = _base_results(session, {"evidence_limit": limit})` | `results: list[EvaluationResult] = []` |
| `resource_budget` (~1132-1137) | builds `config` dict of formatted ceilings, then `results = _base_results(session, config)` | delete the `config` block; `results: list[EvaluationResult] = []` |
| `tool_policy` (~1284-1293) | builds `config` dict (keep the preceding `maximums = dict(sorted(...))` line — it's reused later for `per_tool_maximums`), then `results = _base_results(session, config)` | delete only the `config` block; `results: list[EvaluationResult] = []` |
| `llm_call_signals` (~1391) | `results = _base_results(session, {})` | `results: list[EvaluationResult] = []` |
| `model_policy` (~1446-1453) | builds `config` dict, then `results = _base_results(session, config)` | delete the `config` block; `results: list[EvaluationResult] = []` |
| `workflow_conformance` (~1537) | `results = _base_results(session, {"expected_tools": expected_tools, "mode": mode})` | `results: list[EvaluationResult] = []` |

`output_contract`'s `exact_output` result keeps building
`expected_sha256`/`observed_sha256` via `_hash(...)` exactly as today —
unchanged.

No import changes needed: `hashlib` stays (used by `_hash` and by the
oversized-result truncation path), `Any`/`Mapping`/etc. are all still used
elsewhere in the file.

## Tests

### `plugins/tests/evaluators/test_deterministic.py`

- `test_entrypoint_loads_through_package_plugin_contract` (~line 172): change
  `assert {result.name for result in first} >= {"input_sha256", "terminality"}`
  to assert `terminality` is present and that `input_sha256`/`config_sha256`
  are gone, e.g.:
  ```python
  assert {result.name for result in first} >= {"terminality"}
  assert {result.name for result in first}.isdisjoint(
      {"input_sha256", "config_sha256"}
  )
  ```
- `test_ordered_result_name_contracts` (~line 175): remove the leading
  `"input_sha256"`, `"config_sha256"` pair from every one of the ten lists in
  `expected` (session_diagnostics, output_contract, trajectory_signals,
  tool_health, timing_profile, resource_budget, tool_policy,
  llm_call_signals, model_policy, workflow_conformance).
- `test_shared_hashes_are_stable_and_preserve_node_array_order` (~line 311):
  delete this test entirely — it exists solely to exercise
  `input_sha256`/`config_sha256` (node-array-order preservation via
  `_input_envelope`, dict-key canonicalization via `_base_results`'s
  `config_sha256`), both of which are now dead code. Canonical-JSON
  determinism (sorted keys, stable hashing) remains covered independently by
  `test_output_contract_bounds_large_exact_result` (~line 438), which still
  exercises `_hash` through `expected_sha256`/`observed_sha256`.
- Leave everything else — including `test_output_contract_bounds_large_exact_result`,
  `test_output_contract_uses_json_encoding_equality`, and the
  `expected must contain only finite JSON values` validation path — untouched;
  none of it goes through `_base_results`.

Add a regression check that a config value cannot leak provenance: not
required as a new test, since `test_ordered_result_name_contracts` plus the
disjointness assertion above already pin the exact absence of both names
across every bundle.

## Package metadata (kitaru-evaluator release)

Per `plugins/AGENTS.md` ("Release `kitaru-evaluator` when any built-in
evaluator changes"), this is a behavior change to the package, so bump it —
confirmed by the two prior evaluator-change commits (`09bb53aa`, and the
unmerged `72cbfdee`) which both bumped package version + bootstrap.py + the
plugin's own changelog in the same commit as the evaluator source change:

1. `plugins/packages/evaluator/pyproject.toml`: bump `version = "0.1.2"` to
   `"0.1.3"` — run
   `uv version --project plugins --package kitaru-evaluator 0.1.3 --no-sync`
   (per `plugins/AGENTS.md`), which also updates `plugins/uv.lock`.
2. `plugins/packages/evaluator/CHANGELOG.md`: add
   ```markdown
   ## 0.1.3

   - Remove `input_sha256` and `config_sha256` provenance results from every deterministic evaluator bundle.
   ```
   above the existing `## 0.1.2` entry.
3. `src/kitaru/server/api/bootstrap.py`: bump every
   `requirement="kitaru-evaluator==0.1.2"` / `display_version="0.1.2"` pair
   to `0.1.3` — there are 13 pairs across all `kitaru/cost`, `kitaru/latency`,
   `kitaru/tool-call-patterns` (unchanged behavior, but they share the one
   package version) and the ten deterministic evaluators. This mirrors the
   exact pattern in commit `09bb53aa`.
4. `tests/scripts/test_release_units.py:141`: update the hardcoded
   `"kitaru-evaluator==0.1.2"` in
   `test_default_requirements_are_derived_from_release_units` to
   `"kitaru-evaluator==0.1.3"` — this reads `plugins/packages/evaluator/pyproject.toml`
   live via `release_units.load_inventory()`/`default_requirements()`, so it
   will fail once step 1 lands unless updated.

Do **not** add a root `CHANGELOG.md` entry — established practice (confirmed
by `git show` on `09bb53aa` and `92c8c13d`, both evaluator-only PRs) is that
plugin-package-scoped behavior changes are recorded only in the plugin's own
`CHANGELOG.md`, not the root one.

## Documentation

`docs/book/guides/deterministic-evaluations.md`:

- "Understand result evidence" section (line 74): remove the paragraph that
  documents `input_sha256`/`config_sha256` (`"Every bundle emits
  input_sha256 and config_sha256. ..."`). Replace it with a short note
  pointing at the real provenance source, e.g.: "Configured bundles no longer
  echo a hash of their parameters into a result. Read `evaluator_params` on
  the evaluation response to see the exact params an evaluator ran with."
  Keep the rest of the section (finding-result JSON shape, the 64,000-byte
  oversized-result truncation hash, and the exact-output
  `expected_sha256`/`observed_sha256` paragraph) unchanged — none of that is
  in scope.
- "Current evidence limits" section (line 157): the sentence "The hashes
  expose that difference, but they do not create an immutable snapshot." no
  longer applies (there is no hash). Remove just that sentence and keep the
  surrounding two sentences about session/node reads being separate fetches
  and runtime-compatibility affecting repeatability — don't replace it with
  an unverified "sessions are now immutable" claim; the docs rule in
  `docs/CLAUDE.md` ("Only document shipped features") means that stronger
  claim needs its own verification against the session-immutability work,
  which is out of scope here.

Confirmed no other doc surface references these fields (`grep -rln
"input_sha256\|config_sha256"` across `docs/`, `openapi/openapi.json`, and
any `*.ts`/`*.tsx`/`*.json` returns nothing beyond the file above).

## Verification

Plugin-scoped (per `plugins/AGENTS.md` "Required tests"):

```bash
uv run --project plugins ruff format --config plugins/pyproject.toml --check plugins
uv run --project plugins ruff check --config plugins/pyproject.toml plugins
uv run --project plugins ty check --project plugins
uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests tests/server/test_default_plugins.py
just plugin-artifact-smoke   # required: this PR changes default definitions/pins
```

Root (per root `CLAUDE.md` "run `just check` and `just test` before pushing"):

```bash
just check
just test
```

`just test` will exercise `tests/scripts/test_release_units.py`, which
catches a forgotten version-string update in step 4 above.

Before opening the PR, run `/simplify` on the diff per `CLAUDE.md`.

## Out of scope (do not touch)

- `expected_sha256`/`observed_sha256` in `output_contract`'s `exact_output`
  result, and the oversized-result-value truncation hash in `_json_result` —
  explicitly called out by the issue as staying.
- Any behavior from the unmerged `codex/issue-941-evaluator-scales` branch
  (numeric score scales replacing JSON finding payloads) — unrelated issue,
  do not merge that work in here even though it happens to touch the same
  `_base_results` scaffolding.
