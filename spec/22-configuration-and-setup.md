# Configuration and setup

How users connect to Kitaru, register infrastructure, and manage credentials. These items were extracted from `21-notes_for_future_work.md` because they're heavily interlinked and need dedicated planning.

---

## Config directory naming

The config directory is still named `zenml` in some places, and `active project` is shown but isn't a concept exposed in Kitaru.

**Action:** Use `kitaru` as the config path name. Either hide the config path or rename it. Default project should be used silently.

---

## Projects: hide from users

Projects should not be exposed to users directly. Kitaru should just use the default project.

- Hamza's rationale: "because of the UI burden" and "in OSS zenml we also don't have it"
- The Kitaru UI team also won't expose the project concept
- For internal testing (especially MVP stage), keep an env var escape hatch to override the project
- **Spec inconsistency:** chapters 4, 14, and 19 still reference project config in `pyproject.toml`, project context in `kitaru info`, and project-level config as part of the config model

**Action:** Decide whether `project` remains fully internal/defaulted. If so, clean up chapters 4/14/19 and CLI vocabulary.

---

## Stack registration recipe UX

Hamza's vision: expose stacks as first-class citizens, hide stack components and service connectors. Users pick from pre-built recipes (AWS, GCP, Cloudflare, Modal) and register with a single command:

```
kitaru stack register --type aws --aws-profile .. --aws-secret-key .. --artifact-store s3:// --container-registry something.ecr.aws
```

Key constraints:
- **No reusing of components** — each registration creates a fresh set
- Service connectors are set up behind the scenes, never exposed to users
- Eventually support this via Terraform too

Partially reflected in chapters 4/14/19 and plan phase 18, but the recipe syntax, "no component reuse" constraint, and Terraform aspiration aren't captured elsewhere.

---

## Revisit whether "stack" is the right user-facing term

Hamza's explicit scope: "a stack in kitaru simply defines the orchestrator, artifact store, and container URI (optionally). I would not include any other concept in it."

If model registration and sandbox registration become separate concepts, the current "stack" abstraction may be too broad. Hamza suggested renaming to **"runtime"**.

**Action:** Once model and sandbox registration decisions are made, revisit whether "stack" should be narrowed/renamed to "runtime".

**Dependency:** This decision is blocked on the sandbox provider decision (see `21-notes_for_future_work.md`, "Sandbox providers: register separately?"). If sandboxes live outside the stack, then "stack" really is just orchestrator + artifact store + container registry — and "runtime" becomes a more honest name.

---

## Deploy-time default stack with artifact store

Hamza proposed setting up a **default artifact store at deploy time** so users don't need to register a single stack to get started. Logs and artifacts would go to this default store automatically.

Mentioned in chapters 4/19 and the plan, but the specific mechanism (artifact store provisioned at deploy time) and the goal (zero-stack-setup experience) are worth tracking as a concrete UX requirement.

---

## Secrets and infra UX for new users — PARTIALLY RESOLVED

**Decision:** Kitaru wraps ZenML's centralized secret store with `kitaru secrets set/show/list/delete`. Secrets are private by default and use env-var-shaped keys for LiteLLM compatibility. Model aliases can reference ZenML secrets via `--secret` for remote credential resolution. See updated spec chapters 4, 8, and 14.

**Remaining work:**
- End-to-end cloud stack setup experience for users who have never touched ZenML
- Service connector creation integrated into stack creation UX
- Making the whole infra setup feel native to Kitaru rather than requiring ZenML knowledge
