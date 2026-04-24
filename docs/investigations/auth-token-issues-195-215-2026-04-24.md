# Investigation: Kitaru API/SDK auth token support for issues #195 and #215

## Summary
`kitaru auth token` already covers short-lived bearer-token generation for the active remote Kitaru server, including generated deployment curl snippets. The missing OSS-compatible surface is long-lived machine credential lifecycle management: service accounts and API keys in Kitaru's SDK and CLI; Personal Access Tokens should be treated as Pro-only and excluded from the OSS MVP.

## Symptoms
- GitHub issues #195 and #215 request some way to generate an authentication/authorization token for Kitaru use cases such as deployments.
- Current behavior of `kitaru auth token` is unclear: it may already cover part of this scenario, or it may only expose an existing token rather than creating a new persistent API token.
- Need to avoid relying on ZenML Pro-only service-account features if possible; prefer capabilities available in open-source ZenML.

## Background / Prior Research
### GitHub issues #195 and #215
- #195 (`Introduce sdk surface for service accounts/api keys/ PAT`) is open, labeled `enhancement`, has an empty body, and contains one maintainer comment asking what is needed for MVP. URL: https://github.com/zenml-io/kitaru/issues/195
- #215 (`Service Accounts + API Keys in the SDK`) is open, labeled `enhancement`, with placeholder sections only. URL: https://github.com/zenml-io/kitaru/issues/215
- Inferred acceptance criteria must therefore come from the titles plus product context: Kitaru likely needs a user-facing SDK/API/CLI surface for service accounts and API keys, and possibly a careful distinction between short-lived API tokens and long-lived deployment credentials.

### Current ZenML docs: OSS vs Pro token surfaces
- ZenML OSS exposes `GET /api/v1/api_token` to generate an API token for the current user; docs describe generic short-lived tokens and workload-scoped tokens. Source: https://docs.zenml.io/api-reference/oss-api/oss-api/api-token
- ZenML OSS docs recommend service accounts + API keys for CI/CD or long-lived automation against an OSS server. Source: https://docs.zenml.io/api-reference/oss-api/getting-started
- The OSS API docs show service-account creation at `POST /api/v1/service_accounts` and API-key creation at `POST /api/v1/service_accounts/{service_account_id}/api_keys`. Sources: https://docs.zenml.io/api-reference/oss-api/oss-api/service-accounts and https://docs.zenml.io/api-reference/oss-api/oss-api/service-accounts/api-keys
- ZenML connection docs summarize OSS automation as service account + API key, and distinguish Pro workspace / Pro management API credentials separately. Source: https://docs.zenml.io/deploying-zenml/connecting-to-zenml
- ZenML docs state Personal Access Tokens are only available in ZenML Pro; for OSS non-interactive access, use service accounts and API keys. Source: https://docs.zenml.io/deploying-zenml/connecting-to-zenml/connect-with-a-pat
- OSS service-account docs say `zenml service-account create` creates a service account and API key, and `zenml login <url> --api-key` can use that API key. They also state this OSS service-account path is for OSS servers, while ZenML Pro Workspace API access uses Pro service accounts. Source: https://docs.zenml.io/deploying-zenml/connecting-to-zenml/connect-with-a-service-account

### Early hypothesis from prior research
- `kitaru auth token` may map to ZenML's short-lived `/api/v1/api_token` flow, which is useful for temporary automation but likely insufficient as the main answer to issues #195/#215 if the desired deployment credential is durable.
- The OSS-compatible MVP should probably expose service-account and API-key management rather than PATs. PAT support should either be deferred, clearly labeled Pro-only, or not included in an OSS-first Kitaru surface.

## Investigator Findings
<!-- Pair investigator appends here. -->

### 2026-04-24 investigator pass: Kitaru auth token vs OSS service-account/API-key surface

#### Scope checked
- Re-checked GitHub issue metadata with `gh issue view`: #195 is open and titled "Introduce sdk surface for service accounts/api keys/ PAT"; #215 is open and titled "Service Accounts + API Keys in the SDK". Both issue bodies are empty/placeholder, so conclusions below infer intent from titles plus current product behavior.
- Local repos inspected: `kitaru/` and `zenml/`. This pass is source-only, except appending these findings to this report.

#### Current Kitaru behavior

| Surface | What exists today | Evidence |
|---|---|---|
| `kitaru auth token` registration | Top-level `auth` app is registered, then `_auth.py` registers `token` under it. | `kitaru/src/kitaru/_cli/__init__.py:54-80`; `kitaru/src/kitaru/_cli/_auth.py:51-69` |
| `kitaru auth token` implementation | Validates active connection, builds `Client().zen_store`, requires `RestZenStore`, and returns `store.get_or_generate_api_token()`. This is a short-lived bearer-token exchange for the active remote server, not service-account/API-key CRUD. | `kitaru/src/kitaru/_cli/_auth.py:23-48` |
| `auth token` JSON contract | JSON output is a standard single-item envelope: `{ "command": "auth.token", "item": { "token": ... } }`; text mode prints only the token. | `kitaru/src/kitaru/_cli/_auth.py:63-69`; `_emit_json_item` in `kitaru/src/kitaru/_cli/_helpers.py:485-495`; tests in `kitaru/tests/test_cli.py:475-543` |
| Remote-only behavior | `auth token` errors unless the active store is `RestZenStore`; local/non-remote stores cannot mint this token. | `kitaru/src/kitaru/_cli/_auth.py:35-40` |
| `kitaru login --api-key` CLI | `login` accepts `--api-key` for remote server authentication, rejects it for bare local login, then forwards it into `facade.login_to_server(...)`. | `kitaru/src/kitaru/_cli/_status.py:630-633`, `kitaru/src/kitaru/_cli/_status.py:654-678`, `kitaru/src/kitaru/_cli/_status.py:720-731`; forwarding test around `kitaru/tests/test_cli.py:2778-2784` |
| SDK/config consumption of API keys | `kitaru.connect(...)` and `kitaru.login_to_server(...)` accept `api_key` and pass it through to the ZenML login helpers. | `kitaru/src/kitaru/config.py:801-846`, `kitaru/src/kitaru/config.py:848-884`; passthrough implementation in `kitaru/src/kitaru/_config/_connection.py:115-190` |
| Env-based auth consumption | Public `KITARU_SERVER_URL`, `KITARU_AUTH_TOKEN`, `KITARU_PROJECT` populate connection config; direct `ZENML_STORE_URL`, `ZENML_STORE_API_KEY`, `ZENML_ACTIVE_PROJECT_ID` are a compatibility layer below them. Incomplete env config fails at first use. | `kitaru/src/kitaru/_config/_env.py:149-183`, `kitaru/src/kitaru/_config/_env.py:243-274` |
| Persisted auth consumption | Kitaru reads existing ZenML global store config and extracts token-like fields named `api_key`, `auth_token`, or `token`. | `kitaru/src/kitaru/_config/_core.py:385-401`, `kitaru/src/kitaru/_config/_core.py:689-707` |
| `KitaruClient` auth overrides | `KitaruClient(server_url=..., auth_token=..., project=...)` exists in the constructor signature/docs, but per-client overrides are not implemented and raise if supplied. | `kitaru/src/kitaru/client.py:1692-1729`; generated reference mirrors this at `kitaru/docs/content/docs/reference/python/client/KitaruClient.mdx:35-39`, `:80-83`; public export in `kitaru/src/kitaru/__init__.py:68-76`, `:114`, `:132-133` |
| Deployment curl generation | `kitaru flow deployments curl` embeds a token command, not a real token: `KITARU_SERVER_ACCESS_TOKEN="$(kitaru auth token)"`, then `Authorization: Bearer ${KITARU_SERVER_ACCESS_TOKEN}`. The request URL is `/api/v1/pipeline_snapshots/{deployment_id}/runs`. | constants/body/header in `kitaru/src/kitaru/_cli/_flows.py:78-80`, `:241-303`; payload fields in `kitaru/src/kitaru/_cli/_flows.py:321-352`; command handler in `kitaru/src/kitaru/_cli/_flows.py:789-857`; tests in `kitaru/tests/test_cli.py:985-1057` |
| Deployment docs wording | Hand-written docs already describe `kitaru auth token` as exchanging the active Kitaru connection for a short-lived server bearer token and say generated curl does not inline/store the real token. | `kitaru/docs/content/docs/guides/deployments.mdx:282-337`; `kitaru/docs/content/docs/concepts/deployments.mdx:244-252`; changelog source `kitaru/CHANGELOG.md:10-16` |

**Concrete story:** Kitaru currently has a "temporary badge printer" (`kitaru auth token`) and several ways to *use* a long-lived credential (`login --api-key`, `KITARU_AUTH_TOKEN`/`ZENML_STORE_API_KEY`). It does **not** currently have the "badge office" surface: create/list/update/rotate/delete service accounts or API keys.

#### ZenML OSS primitives available locally

| Primitive layer | What ZenML OSS exposes | Evidence / likely wrapper target |
|---|---|---|
| High-level service-account client methods | `Client.create_service_account`, `get_service_account`, `list_service_accounts`, `update_service_account`, `delete_service_account`. | `zenml/src/zenml/client.py:8352-8497` |
| High-level API-key client methods | `Client.create_api_key`, `set_api_key`, `list_api_keys`, `get_api_key`, `update_api_key`, `rotate_api_key`, `delete_api_key`. `create_api_key` and `rotate_api_key` return responses whose `key` is asserted present immediately after creation/rotation. | `zenml/src/zenml/client.py:8507-8769` |
| Service-account models | `ServiceAccountRequest`, `ServiceAccountUpdate`, `ServiceAccountResponse`, `ServiceAccountFilter`; response exposes `active`, `description`, `external_user_id`, etc. | `zenml/src/zenml/models/v2/core/service_account.py:38-297` |
| API-key models | `APIKeyRequest`, `APIKeyRotateRequest`, `APIKeyUpdate`, `APIKeyResponse`, `APIKeyFilter`; cleartext `key` is "Only set immediately after creation or rotation." | `zenml/src/zenml/models/v2/core/api_key.py:84-363`, especially `:143-154` and `:183-251` |
| REST store service-account methods | CRUD routes are already wrapped in `RestZenStore`: create/get/list/update/delete service account. | `zenml/src/zenml/zen_stores/rest_zen_store.py:2905-3004` |
| REST store API-key methods | CRUD/rotate routes are already wrapped in `RestZenStore`: create/get/list/update/rotate/delete API key. | `zenml/src/zenml/zen_stores/rest_zen_store.py:621-750` |
| REST store API-token method | `RestZenStore.get_api_token(...)` calls `/api_token` with token type, expiry, and optional workload scopes. `get_or_generate_api_token()` then authenticates through API key/password/Pro token as needed and caches a bearer token. | `zenml/src/zenml/zen_stores/rest_zen_store.py:4384-4425`, `zenml/src/zenml/zen_stores/rest_zen_store.py:4589-4728` |
| ZenML CLI service accounts | OSS CLI already has `zenml service-account create/describe/list/update/delete`. `create` creates a default API key by default (`--create-api-key` default true). | `zenml/src/zenml/cli/service_accounts.py:95-162`, `:165-285` |
| ZenML CLI API keys | Nested CLI already has `zenml service-account api-key create/describe/list/update/rotate/delete`, including `--set-key`, `--retain`, and `--output-file`. | `zenml/src/zenml/cli/service_accounts.py:288-590` |
| Server route constants | `API_KEYS = "/api_keys"`, `API_KEY_ROTATE = "/rotate"`, `API_TOKEN = "/api_token"`, `SERVICE_ACCOUNTS = "/service_accounts"`. | `zenml/src/zenml/constants.py:443-502` |
| Server endpoints: service accounts | `/api/v1/service_accounts` supports POST/GET/list/PUT/DELETE under an authorized router. | Router prefix and handlers in `zenml/src/zenml/zen_server/routers/service_accounts_endpoints.py:51-239` |
| Server endpoints: API keys | `/api/v1/service_accounts/{service_account_id}/api_keys` supports POST/GET/list/PUT/rotate/DELETE. | `zenml/src/zenml/zen_server/routers/service_accounts_endpoints.py:263-485` |
| Server endpoint: API token | `GET /api/v1/api_token` mints generic short-lived tokens or workload-scoped tokens for schedule/pipeline-run/deployment scopes. Generic expiry defaults/caps come from server config. | `zenml/src/zenml/zen_server/routers/auth_endpoints.py:471-565` |

#### Conclusions for issues #195 and #215

1. **Issue #195 is only partially covered.**
   - Covered: Kitaru can print a short-lived bearer token for the active remote server (`kitaru auth token`) and can consume existing API keys/tokens through `login --api-key` or env config.
   - Not covered: Kitaru cannot create/list/describe/update/deactivate/rotate/delete service accounts or API keys, and it does not expose a PAT management surface.
   - Therefore `kitaru auth token` is useful for deployment curl and short-lived automation, but it is not a complete answer to "sdk surface for service accounts/api keys/PAT".

2. **Issue #215 is missing in Kitaru today.**
   - ZenML OSS has the underlying methods and endpoints, but Kitaru has no SDK namespace wrapping them and no Kitaru CLI command group for service accounts/API keys.
   - This is exactly the gap implied by #215's title: "Service Accounts + API Keys in the SDK".

3. **PATs should not be part of an OSS-first MVP.**
   - Local ZenML docs contain Pro PAT docs (`zenml/docs/book/getting-started/zenml-pro/personal-access-tokens.md:1-30`, `:265-289`) and Pro API guidance (`zenml/docs/book/api-docs/pro-api/pro-api/getting-started.md:50-88`), but the local OSS source path for #215 is service accounts + API keys + short-lived API tokens.
   - Treat PATs as Pro-only/deferred, and avoid promising `kitaru pat ...` commands in OSS docs.

#### Recommended OSS-first Kitaru MVP shape

**SDK namespace**
- Add a small Kitaru-owned namespace rather than leaking ZenML names everywhere. Two reasonable shapes:
  1. `KitaruClient().auth.service_accounts.*` and `KitaruClient().auth.api_keys.*`, or
  2. `KitaruClient().service_accounts.*` with nested `.api_keys.*`.
- Prefer option 1 if future auth concepts may grow (`auth.token(...)`, maybe Pro adapters later); prefer option 2 if keeping the client flatter matters more.
- Internally wrap `zenml.client.Client` methods first, not raw REST endpoints:
  - service accounts: `Client.create/get/list/update/delete_service_account(...)` (`zenml/src/zenml/client.py:8352-8497`)
  - API keys: `Client.create/list/get/update/rotate/delete_api_key(...)` (`zenml/src/zenml/client.py:8507-8769`)
- Return Kitaru Pydantic/dataclass DTOs that intentionally expose safe fields (`id`, `name`, `description`, `active`, timestamps, service-account name/id). Include cleartext `key` only on create/rotate results and name it explicitly, e.g. `api_key_value: str | None`, mirroring ZenML's one-time display behavior (`zenml/src/zenml/models/v2/core/api_key.py:143-154`).

**SDK methods for MVP**
- `client.auth.service_accounts.create(name, description="", create_api_key=True, api_key_name="default") -> ServiceAccountCreateResult`
  - Deliberately decide whether to mirror ZenML CLI's default behavior (`create_api_key=True`) or ZenML SDK's behavior (`create_service_account` only). For Kitaru automation, mirroring CLI is probably friendlier if docs warn the key is shown once.
- `list(...)`, `get(name_or_id)`, `update(name_or_id, *, name=None, description=None, active=None)`, `delete(name_or_id)`.
- `client.auth.api_keys.create(service_account, name="default", description="")`, `list(service_account, ...)`, `get(service_account, key)`, `update(...)`, `rotate(service_account, key, retain_minutes=0)`, `delete(...)`.
- Optional but useful: `client.auth.token(expires_in: int | None = None) -> str` wrapping `RestZenStore.get_api_token(token_type=APITokenType.GENERIC, expires_in=...)`; keep `kitaru auth token` as the CLI wrapper for the common case. Validate `RestZenStore`, just like current `_active_server_access_token()` does (`kitaru/src/kitaru/_cli/_auth.py:35-40`).

**CLI commands**
- Add under existing `auth` group so auth concepts stay in one place:
  - `kitaru auth service-account create NAME [--description ...] [--no-api-key] [--api-key-name default] [-o json]`
  - `kitaru auth service-account list|get|update|delete ...`
  - `kitaru auth service-account api-key create SERVICE_ACCOUNT NAME ...`
  - `kitaru auth service-account api-key list|get|update|rotate|delete ...`
- Follow current JSON contracts:
  - single item: `{command, item}` via `_emit_json_item` (`kitaru/src/kitaru/_cli/_helpers.py:485-495`)
  - lists: `{command, items, count}` via `_emit_json_items` (`kitaru/src/kitaru/_cli/_helpers.py:498-508`)
- Never print cleartext API keys except create/rotate text output and JSON result. Add explicit warning text in non-JSON output: "Store this now; it cannot be retrieved later." This matches ZenML model/CLI behavior (`zenml/src/zenml/models/v2/core/api_key.py:143-154`; `zenml/src/zenml/cli/service_accounts.py:79-90`, `:545-558`).
- For delete/rotate, require confirmation or `--yes` like ZenML's API-key delete command (`zenml/src/zenml/cli/service_accounts.py:563-590`).

**Docs/tests/smoke tests**
- Docs:
  - Add a hand-written auth/service-accounts guide under `docs/content/docs/` explaining the two-token story: long-lived service-account API key for machines, short-lived bearer token from `kitaru auth token` for curl/API calls.
  - Update deployment docs where they currently say "auth token or API key" (`kitaru/docs/content/docs/guides/deployments.mdx:282-337`) to point users to the new Kitaru service-account/API-key commands once implemented.
  - Do not hand-edit generated CLI pages; fix command docstrings/generator inputs instead, per repo policy.
- Tests:
  - CLI unit tests in `kitaru/tests/test_cli.py` for command registration, JSON envelopes, one-time key display, no accidental key leakage on list/get, and confirmation behavior.
  - SDK tests in `kitaru/tests/test_client.py` or a new focused test file with mocked ZenML `Client` methods.
  - Config/auth tests proving generated API keys can be fed back through `kitaru.connect(..., api_key=...)` or `kitaru login --api-key`.
- Smoke test:
  - Add non-destructive `--help` entries to `scripts/smoke-test.sh`, following the existing `kitaru auth token --help` and `kitaru flow deployments curl --help` pattern (`kitaru/scripts/smoke-test.sh:251-260`).
- Analytics:
  - Add non-sensitive event names only (created/listed/rotated/deleted booleans/counts, no names, no key values), following repo analytics guidance in `AGENTS.md:115-123`.

#### Eliminated hypotheses / gotchas
- **Eliminated:** `kitaru auth token` creates a persistent API key. It does not; it delegates to `RestZenStore.get_or_generate_api_token()` for a bearer token (`kitaru/src/kitaru/_cli/_auth.py:23-48`; `zenml/src/zenml/zen_stores/rest_zen_store.py:4589-4728`).
- **Eliminated:** deployment curl output leaks the active API key/token. Tests assert the generated curl contains `KITARU_SERVER_ACCESS_TOKEN="$(kitaru auth token)"` and does not contain `KITARU_AUTH_TOKEN` or a `kat_` key (`kitaru/tests/test_cli.py:1042-1048`).
- **Eliminated:** Kitaru has an existing service-account wrapper hidden under docs/config. Searches found only cloud-provider service-account credential handling and docs references, not Kitaru-managed ZenML service accounts/API keys.
- **Gotcha:** ZenML CLI `service-account create` creates a default API key by default, but ZenML SDK `Client.create_service_account(...)` does not. A Kitaru MVP must choose this behavior deliberately (`zenml/src/zenml/cli/service_accounts.py:99-162` vs `zenml/src/zenml/client.py:8352-8377`).
- **Gotcha:** API key list/get responses should not include a recoverable cleartext key; the model only carries `key` after create/rotate (`zenml/src/zenml/models/v2/core/api_key.py:143-154`).

## Investigation Log

### Phase 1 - Initial setup
**Hypothesis:** Issues #195 and #215 are about a missing or incomplete token-generation surface for Kitaru CLI/API/SDK, likely related to ZenML auth token or service account/PAT support.
**Findings:** Created isolated worktree `/Users/strickvl/coding/zenml/worktrees/kitaru-issues-195-215-auth-tokens` on branch `investigate/issues-195-215-auth-tokens` from `origin/develop`.
**Evidence:** `git status` reports `## investigate/issues-195-215-auth-tokens...origin/develop`.
**Conclusion:** Ready for external fact gathering and workspace investigation.

### Phase 1.5 subagent verification - GitHub issues
**Hypothesis:** The GitHub issues contain minimal detail, so acceptance criteria must be inferred from metadata/title/product context.
**Findings:** Explore subagent verified #195 is open, labeled `enhancement` and Type `Feature`, attached to the Kitaru Roadmap as `In progress`; #215 is open, labeled `enhancement`, attached to the Kitaru Roadmap as `Backlog`. Both bodies are effectively empty. The subagent inferred #195 is broader (service accounts/API keys/PAT), while #215 is narrower and likely overlapping/duplicate-shaped around SDK service accounts + API keys.
**Evidence:** https://github.com/zenml-io/kitaru/issues/195 and https://github.com/zenml-io/kitaru/issues/215
**Conclusion:** Treat #195 as the umbrella issue and #215 as the OSS service-account/API-key SDK subset.

### Phase 1.5 subagent verification - ZenML docs
**Hypothesis:** OSS Kitaru should rely on ZenML OSS service accounts + API keys, not Pro-only PATs.
**Findings:** Explore subagent verified current official ZenML docs: OSS supports service accounts, service-account API keys, API-key login/token exchange, short-lived API tokens, and API-key rotation. PATs are Pro-only. OSS long-lived user-scoped PATs do not exist in the official docs; the supported OSS machine-auth story is service account + API key, optionally exchanged for a short-lived bearer token.
**Evidence:** Official docs at https://docs.zenml.io/api-reference/oss-api/getting-started, https://docs.zenml.io/api-reference/oss-api/oss-api/service-accounts, https://docs.zenml.io/api-reference/oss-api/oss-api/service-accounts/api-keys, https://docs.zenml.io/api-reference/oss-api/oss-api/api-token, https://docs.zenml.io/deploying-zenml/connecting-to-zenml/connect-with-a-service-account, and https://docs.zenml.io/deploying-zenml/connecting-to-zenml/connect-with-a-pat.
**Conclusion:** Kitaru can implement OSS machine auth without Pro by exposing service-account/API-key management; any PAT story must be explicitly Pro-only or out of MVP scope.

### Phase 2 - Context builder assessment
**Hypothesis:** Context builder should identify whether the missing feature is in Kitaru auth/token code or in a separate service-account/API-key surface.
**Findings:** Context builder selected Kitaru auth CLI/config/deployment files, KitaruClient, auth-token tests/docs, and ZenML service-account/API-key client/store/router/model files. Its initial assessment was that `kitaru auth token` is short-lived token support, while service-account/API-key lifecycle management is missing from Kitaru.
**Evidence:** Key files include `kitaru/src/kitaru/_cli/_auth.py`, `kitaru/src/kitaru/client.py`, `zenml/src/zenml/client.py`, `zenml/src/zenml/zen_stores/rest_zen_store.py`, and `zenml/src/zenml/cli/service_accounts.py`.
**Conclusion:** Confirmed by pair and oracle.

### Phase 3 - Pair investigation
**Hypothesis:** Kitaru may already cover some token needs but not the service-account/API-key SDK surface requested by #215.
**Findings:** Pair investigator appended detailed evidence under `## Investigator Findings`.
**Evidence:** See the tables above for exact file:line references.
**Conclusion:** `kitaru auth token` is useful but not sufficient for #195/#215 as written.

### Phase 4 - Oracle synthesis
**Hypothesis:** The safest implementation path is to separate long-lived API keys from short-lived bearer tokens and exclude Pro-only PATs from the OSS MVP.
**Findings:** Oracle agreed with the thesis: `kitaru auth token` should remain the short-lived bearer-token helper; the new work should expose service-account/API-key lifecycle management. It also called out UX risks around confusing `KITARU_AUTH_TOKEN` with `kitaru auth token`, one-time API-key visibility, server-level vs project-level validation, and paginated ZenML list results.
**Evidence:** Oracle synthesis over selected Kitaru and ZenML files after pair findings.
**Conclusion:** Final recommendations below use this split.

## Root Cause
The root cause is a product/API surface gap, not a missing ZenML backend primitive.

Kitaru currently has a short-lived request-token helper: `kitaru auth token` validates the active connection, requires a remote `RestZenStore`, and delegates to `RestZenStore.get_or_generate_api_token()` (`kitaru/src/kitaru/_cli/_auth.py:23-48`). Deployment curl generation correctly shells out to that helper instead of inlining secrets (`kitaru/src/kitaru/_cli/_flows.py:278-303`). Kitaru can also consume existing API keys through `kitaru login --api-key` (`kitaru/src/kitaru/_cli/_status.py:620-731`) and environment/config paths (`kitaru/src/kitaru/_config/_env.py:149-274`).

What Kitaru lacks is the higher-level "credential lifecycle" layer: no Kitaru SDK namespace or CLI command group creates, lists, updates, rotates, or deletes service accounts and service-account API keys. `KitaruClient` currently exposes executions, artifacts, memories, and deployments, and explicitly rejects per-client connection overrides (`kitaru/src/kitaru/client.py:1692-1738`). Meanwhile ZenML OSS already exposes the needed primitives through `Client.create_service_account`, `Client.create_api_key`, `Client.rotate_api_key`, etc. (`zenml/src/zenml/client.py:8352-8769`), corresponding REST store methods, and server endpoints.

The naming confusion is the sharp edge: "API key" is the long-lived machine credential; "bearer/access token" is the short-lived request credential; "PAT" is a ZenML Pro-only user credential. Kitaru currently uses `KITARU_AUTH_TOKEN` for a credential-like env var while also having a command named `kitaru auth token`, so docs and command names must be precise.

## Recommendations
1. **Keep `kitaru auth token` as the short-lived bearer-token helper.** Do not replace it with service-account/API-key creation. Optionally document more explicitly that it does not create or rotate long-lived API keys.
2. **Implement OSS-first SDK wrappers over ZenML service-account/API-key methods.** Prefer `KitaruClient().auth.service_accounts.*` and `KitaruClient().auth.api_keys.*` so auth concepts stay grouped. Delegate to ZenML `Client` methods first rather than raw REST calls.
3. **Implement matching CLI commands under the existing `auth` group.** Candidate shape: `kitaru auth service-account create/list/show/update/delete` and `kitaru auth service-account api-key create/list/show/update/rotate/delete`.
4. **Treat cleartext API keys as one-time secrets.** Only show/include the key value on create/rotate, support `--output-file`, and ensure list/show output does not leak old key values. This follows ZenML's model where the key is only set immediately after creation or rotation (`zenml/src/zenml/models/v2/core/api_key.py:143-154`).
5. **Exclude PAT management from the OSS MVP.** Document that PATs are Pro-only. For OSS automation, the official path is service account + API key, optionally exchanged for a short-lived bearer token.
6. **Avoid requiring a project for server-level auth management if feasible.** Existing Kitaru runtime work often needs server URL + credential + project, but service-account management is server-level. Consider a validation path that requires a remote server and credential but not `KITARU_PROJECT`.
7. **Add tests/docs/smoke coverage.** Add CLI JSON-envelope tests, SDK wrapper tests with mocked ZenML client methods, secret-safety tests, docs explaining the long-lived-key -> short-lived-token chain, and non-destructive `--help` smoke-test entries.

## Preventive Measures
- Maintain a docs glossary/table distinguishing API key, bearer/access token, and PAT.
- Add regression tests that `kitaru flow deployments curl` never inlines real credentials and continues to use `$(kitaru auth token)`.
- Add output tests ensuring API-key list/show commands never print a previously-created raw key.
- Keep generated CLI docs sourced from command docstrings/generation scripts; do not hand-edit generated CLI reference pages.
- Track analytics only with non-sensitive metadata: event names, counts, booleans, and command names; never key values, service-account names, or file paths.
