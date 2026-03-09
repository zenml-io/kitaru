# 22. Configuration and setup

How users connect to Kitaru, register infrastructure, and manage credentials. These items were extracted from `21-notes_for_future_work.md` because they're heavily interlinked and need dedicated planning.

**Implementation priority:** Config directory + project cleanup first (quick wins that fix the foundation), then stack creation UX, then deploy-time defaults.

---

## Config directory — COMPLETED

**Decision:** Separate directories. Kitaru's own config lives at `~/.config/kitaru/`. ZenML's config stays at its default location (`~/.config/zenml/`) but is never referenced in user-facing output, docs, or CLI messages.

```
~/.config/kitaru/
  config.yaml          # Kitaru-specific (active stack, log-store, model aliases)

~/.config/zenml/       (hidden from user, never referenced)
  global_config.yaml   # ZenML internals (auth, store URL)
  local_stores/        # ZenML local artifact stores
```

**Rationale:** Kitaru IS ZenML under the hood, so the ZenML config directory must exist regardless (it holds auth tokens, store config, etc.). But users should never see the `zenml` name. The Kitaru config directory is the only one that appears in CLI output, error messages, or docs.

**Implementation notes:**
- Move `_kitaru_global_config_path()` in `src/kitaru/config.py` to use `~/.config/kitaru/` instead of nesting inside `GlobalConfiguration().config_directory`
- Audit all CLI output (`kitaru info`, `kitaru status`, error messages) to ensure no ZenML config paths leak to the user
- The `[tool.kitaru]` section in `pyproject.toml` is already correctly named

---

## Projects — COMPLETED

**Decision:** Projects are a flat namespace. Kitaru silently uses whatever project is set as the default on the server. Users never see or interact with the project concept directly. The UI also just uses the default project.

**Internal escape hatches (for development/testing):**
- `KITARU_PROJECT` environment variable overrides the project
- `kitaru.configure(project=...)` SDK path for programmatic testing
- Both are shown in `kitaru info` output when set (so testers can verify)

**Namespace implications:** Flow names share one global namespace per server per project. Since the default project is used, two users on the same server deploying a flow called `content_pipeline` will see each other's executions. This is acceptable for Kitaru's target use case (single-team or small-team deployments).

**Spec cleanup:** Chapters 4, 14, and 19 still reference project config in code examples and CLI output. Add `[internal — not exposed to users]` annotations to project references in the spec rather than removing them entirely. This preserves useful implementation context while making the intent clear.

---

## Stack naming — COMPLETED

**Decision:** Keep "stack" as the user-facing term. Despite Hamza's suggestion of "runtime", "stack" is established in the MLOps/IaC world and avoids clashing with Python/Docker's use of "runtime". The narrowed scope (orchestrator + artifact store + container registry) is fine — users don't need a different word just because the scope is smaller than ZenML's stacks.

The sandbox provider decision (see `21-notes_for_future_work.md`) no longer blocks this naming decision.

---

## Stack creation UX — DECIDED (design details below)

### Command and verb

The command is `kitaru stack create` (not `register`). "Create" is consistent with chapter 14 and means creating a stack definition that points to existing infrastructure. It does not imply provisioning cloud resources.

### Stack naming

- Stack name is **optional**. If omitted, Kitaru auto-generates a Docker-style memorable name (e.g. `brave-falcon`, `quiet-river`).
- If provided, the name must be unique on the server.

```bash
# Explicit name
kitaru stack create my-prod --type aws --artifact-store s3://my-bucket ...

# Auto-named
kitaru stack create --type aws --artifact-store s3://my-bucket ...
# → Stack 'brave-falcon' created and activated.
```

### Auto-activation

Creating a stack automatically makes it the active stack. The CLI prints a confirmation:

```
Stack 'brave-falcon' created and activated.
```

This covers the 90% case (create then immediately use). Users who create stacks for later use can simply `kitaru stack use <other-stack>` afterward.

### Recipe types

All recipe types produce the same stack shape (runner + artifact store + container registry), but some fields are optional or auto-configured per recipe type. For example:

| Recipe type | Required | Optional / Auto-configured |
|---|---|---|
| AWS | artifact-store (S3 URI), credentials | container-registry, runner config |
| GCP | artifact-store (GCS URI), credentials | container-registry, runner config |
| Modal | credentials (API token) | artifact-store (auto-provisioned?), runner |
| Cloudflare | credentials | artifact-store, runner |

The exact flags per recipe type are not yet designed. See "Open questions" below.

### Component reuse

**Relaxed from Hamza's original constraint:** Artifact stores CAN be shared across stacks. This is pragmatic — changing a bucket or rotating credentials across 5 stacks with identical artifact store config is a real operational pain.

Other components (runner, container registry, service connectors) are still created fresh per stack.

### Service connectors

Service connectors are created behind the scenes as part of `kitaru stack create`. Users never see or manage them directly.

### Credential validation

**Validate at creation time.** When `kitaru stack create` sets up service connectors, Kitaru tests the connection immediately and fails fast with a clear error if credentials are wrong. This prevents users from discovering bad credentials 10 minutes later when they try to run a flow.

### Verbose output

`kitaru stack create --verbose` shows what Kitaru is doing under the hood (creating service connector X, artifact store component Y, etc.). Useful for debugging without requiring ZenML CLI knowledge.

### Stack inspection

`kitaru stack show <name>` reveals the stack's components in Kitaru vocabulary:

```bash
kitaru stack show brave-falcon
# → Stack: brave-falcon
#   Type: aws
#   Runner: kubernetes (namespace: ml-agents)
#   Artifact store: s3://my-kitaru-bucket
#   Container registry: 123456.dkr.ecr.us-east-1.amazonaws.com
#   Created: 2026-03-09
```

This does not expose ZenML internals (component IDs, connector types) — it shows Kitaru-level configuration only.

### Stack deletion

`kitaru stack delete <name>` removes the Kitaru stack definition only. It does NOT cascade-delete the underlying ZenML components (service connectors, artifact store components). This is the safer default — it avoids accidentally destroying shared artifact stores.

Orphaned ZenML components are invisible to the user and can be cleaned up by admins using the ZenML CLI if needed.

### AWS auth strategy

**Deferred.** AWS has multiple authentication methods (access keys, IAM roles, SSO profiles, instance profiles) and the right strategy needs its own design discussion. The stack creation spec captures the UX shape but does not commit to specific auth flags.

### Terraform support

**Post-MVP.** Terraform-based stack creation is a future goal but requires a Terraform provider or module. No design work needed now — just captured as an aspiration.

---

## Deploy-time default stack — PARTIALLY DECIDED

**Goal:** When Kitaru is deployed remotely (Helm chart), a default stack is created automatically so users don't need to register a single stack to get started.

**Decided:**
- The deploy-time stack is a **visible named stack** in `kitaru stack list` (e.g. named `default` or auto-generated). Users can see it, select it, and understand where their artifacts go.
- The Helm chart references an artifact store bucket (e.g. `s3://my-kitaru-bucket`) as a Helm value.

```yaml
# Illustrative Helm values
kitaru:
  defaultStack:
    artifactStore: s3://my-kitaru-bucket
    runner: kubernetes
    containerRegistry: ghcr.io/myorg
```

**Open question:** Who creates the bucket? Does the Helm chart just reference a pre-existing bucket (admin creates it beforehand), or does the deployment process provision it? This has lifecycle implications (cleanup on uninstall, IAM permissions). **Not yet decided** — needs more design work.

---

## Secrets — DECIDED

**Decision:** Secrets are always server-backed. There is no local-only secret store.

**Rationale:** In Kitaru's architecture, someone is always connected to a ZenML server (even the `pip install kitaru[local]` path includes a local ZenML server). The ZenML secret store is always available. Users in local-only mode who need API keys just set environment variables directly (`OPENAI_API_KEY=sk-...`). There's a clean separation: env vars for local development, `kitaru secrets` for remote/shared credential management.

The existing `kitaru secrets set/show/list/delete` implementation wrapping ZenML's centralized secret store is correct and complete for MVP.

**Remaining infra UX work:**
- End-to-end cloud stack setup experience for users who have never touched ZenML (depends on stack creation UX above)
- Service connector creation integrated into `kitaru stack create` (see above)
- Making the whole infra setup feel native to Kitaru rather than requiring ZenML knowledge

---

## Open questions

These items need further design work before they can be implemented:

1. **Per-recipe auth flags:** What specific CLI flags should each recipe type (AWS, GCP, Modal, Cloudflare) accept? This depends on what auth methods the ZenML service connectors support and which are most common for Kitaru users.

2. **Deploy-time bucket provisioning:** Should the Helm chart/deployment process create the artifact store bucket, or just reference one the admin pre-created? This has implications for lifecycle management, IAM permissions, and cleanup on uninstall.

3. **Onboarding flow:** What's the minimum viable flow for a new user going from `kitaru login` to running a flow on a cloud stack? Interactive wizard vs. single command vs. docs-driven? Needs more design work.
