# Things that need to be done once the rest is complete

## About how to run / start a workflow

The spec has 3 patterns defined currently:

```
# 1. Synchronous — blocks until complete
result = my_agent("Build a CLI tool")

# 2. Start — returns a handle for longer-running execution
handle = my_agent.start("Build a CLI tool")

# 3. Deploy — starts an execution on a named stack
handle = my_agent.deploy("Build a CLI tool", stack="aws-sandbox")
```

Hamza said the following:

result = my_agent("Build a CLI tool") -> lets get rid of this one
handle = my_agent.start("Build a CLI tool") -> this is subsumed by execute? or run. i think run is the common one that langgraph uses .. start and execute are probably not as common

## Config directory when logging in

- btw i can see that in phase 2 you have named the config directory still zenml, and we can see the active project which isnt a concept we've exposed in kitaru. 
- default project should be used and the config path either hidden or the name kitaru used

## Nice to haves

- Make the step names look a bit nicer or have some sort of metadata in the step metadata which we can extract and use on the kitaru UI?
- Swallow or have our own terminal logging for when we run a flow?

## Projects must go?

- to be discussed, but we'll need to think through projects for the `kitaru login` command. Basically it seems that we shouldn't be exposing the project concept to the user directly and we should just pick whatever project is the default project.
- note that for testing internally (esp for MVP stage) we might want to have some kind of ENV var that allows us to set the project somehow since we might need that functionality...
- (but for the kitaru UI (which is being built by a separate team), I think we won't expose the project concept to the user directly either)

## Open architecture questions (from Hamza's & Alex's feedback, March 2026)

### Resolve project handling across the spec

The "Projects must go?" section above captures the intent to hide projects from users. However, the spec is still **internally inconsistent**: chapters 4, 14, and 19 still reference project config in `pyproject.toml`, project context in `kitaru info`, and project-level config as part of the config model.

Hamza's rationale for hiding projects: "because of the UI burden" and "in OSS zenml we also don't have it." The Kitaru UI team also won't expose the project concept directly.

Future work: decide whether `project` remains a fully internal/defaulted concept, and if so, clean up chapters 4/14/19 and the CLI vocabulary accordingly. Note that for internal testing (especially MVP stage) we may still need an env var escape hatch to set the project.

### Stack registration recipe UX

Hamza's vision for stack creation: expose stacks as first-class citizens, but hide stack components and service connectors from users. Users pick from pre-built stack recipes (AWS, GCP, Cloudflare, Modal) and register with a single command:

```
kitaru stack register --type aws --aws-profile .. --aws-secret-key .. --artifact-store s3:// --container-registry something.ecr.aws
```

Key constraints from Hamza:
- **No reusing of components** — each stack registration creates its own fresh set
- Service connectors are set up behind the scenes, never exposed to the user
- Eventually users should be able to do this via Terraform too

This is partially reflected in chapters 4/14/19 and plan phase 18, but the specific recipe syntax, the "no component reuse" constraint, and the Terraform aspiration are not captured elsewhere.

### Deploy-time default stack with artifact store

Hamza proposed setting up a **default artifact store at Kitaru deploy time** so that the experience is seamless from the beginning — users wouldn't need to register even a single stack to get started. Logs and artifacts would go to this deploy-time default store automatically.

This is mentioned in chapters 4/19 and the plan, but the specific mechanism (artifact store provisioned at deploy time) and the goal (zero-stack-setup experience) are worth tracking as a concrete UX requirement.

### Secrets and infra UX for new users — PARTIALLY RESOLVED

**Decision:** Kitaru wraps ZenML's centralized secret store with `kitaru secrets set/show/list/delete`. Secrets are private by default and use env-var-shaped keys for LiteLLM compatibility. Model aliases can reference ZenML secrets via `--secret` for remote credential resolution. See updated spec chapters 4, 8, and 14.

Alex raised: "We'll need to think about secrets somehow as well. Stacks + the whole infra stuff (from service connectors to spinning up a stack etc) is sort of the big question mark still for how we can make this work well." He noted: "obv I can make it all work assuming I have a nice zenml stack setup already, but for users who don't have a zenml stack already etc... idk how this is going to work well for a cloud stack."

**Remaining future work:**
- End-to-end cloud stack setup experience for users who have never touched ZenML
- Service connector creation integrated into stack creation UX
- Making the whole infra setup feel native to Kitaru rather than requiring ZenML knowledge

### Revisit whether "stack" is the right user-facing term

Hamza's explicit stack scope definition: "For me a stack in kitaru simply defines the orchestrator, artifact store, and container URI (optionally). I would not include any other concept in it."

If model registration and sandbox registration become separate concepts (see below), then the current "stack" abstraction may be too broad. Hamza suggested renaming to **"runtime"** since it's focused on these execution primitives only.

Future work: once model and sandbox registration decisions are made, revisit whether "stack" should be narrowed/renamed to "runtime" to avoid overloading the concept. The naming decision depends on how many things end up living inside vs outside the stack.

### Model registry — RESOLVED

**Decision:** Models use a **local model registry** with LiteLLM as the backend engine. Model config is **not** stack-owned. See updated spec chapter 8.

The MVP uses `kitaru model register` to store aliases and optional credentials locally. Provider env vars (`OPENAI_API_KEY`, etc.) also work as a zero-config path since LiteLLM reads them natively. A future ZenML `llm_model` stack component may later become an additional credential-resolution backend, but it is not part of the MVP.

**Remaining future work for the model registry:**
- Richer registry UX (`kitaru model show`, `kitaru model remove`, `kitaru model test`)
- Import/export or team-sharing of alias configurations
- Optional fallback to a future ZenML `llm_model` stack component for credential resolution

**Note:** Remote credential resolution is now addressed — model aliases reference ZenML secrets via `--secret`, and `kitaru.llm()` fetches them at runtime. See updated spec chapter 8.

### Decide whether sandbox providers should be registered separately

The current spec leans toward sandbox as part of the stack/runner concept. Hamza suggested that sandbox providers **should NOT be part of the stack/runtime** ("I think it should be part of the stack / runner concept described above (too inflexible). Probably a new thing to register"):

```
kitaru sandbox-provider register --type daytona ...
```

Hamza acknowledged this is not fully thought through: "I have not thought through how this would interface with e.g. the pydantic ai integration or even simple flow/checkpoint syntax. something to think through."

Future work: decide whether sandbox is a stack component, a standalone registered concept, or something else. This also needs to consider how sandboxes interface with framework adapters (e.g. PydanticAI) and the flow/checkpoint execution model.

### Log store: OTEL integration and implementation challenges

Hamza on log storage: "by default it goes where the runner stores its artifacts and they can configure maybe an entrypoint for OTEL... this is gonna be tricky to implement outside of a stack."

The basic log-store configuration (`kitaru log-store set/show/reset`) is implemented, but the OTEL entrypoint configuration and making this work well outside of a stack context remain open challenges.

### Log formatting: Kitaru-branded terminal output

Hamza wants Kitaru's terminal output to have its own distinct look and feel, different from ZenML: "I'd also like logs of kitaru to have a certain theme and feel different from zenml. By logs I mean what gets printed out when you run a flow... 'steps' should not be shown... I imagine a really sexy and more modern checkpoint by checkpoint interface."

The existing "Nice to haves" section above mentions this briefly, but Hamza's vision is more specific: a modern, checkpoint-by-checkpoint progress display that completely hides the ZenML step abstraction underneath.

### Import style: `@flow` / `@checkpoint` instead of `@kitaru.flow` / `@kitaru.checkpoint`

Hamza's preference: `from kitaru import flow, checkpoint` then use `@flow` and `@checkpoint` directly, rather than the current `@kitaru.flow` / `@kitaru.checkpoint` style. This is a cosmetic API decision but affects every code example in the spec and docs.

Future work: decide on the canonical import style and update all examples accordingly. Both styles can coexist (the module-level `kitaru.flow` is just an attribute), but the spec and docs should be consistent about which style is recommended.

### Artifacts in Kitaru are fundamentally different from ZenML artifacts

Hamza: "The notion of artifacts in kitaru needs to be meaningfully different from artifacts in zenml. In zenml artifacts usually are pandas dataframes, models etc, in kitaru they will be dicts/json/pydantic objects. That means we can easily show them by default in the dashboard and diff them and do all sorts of things with them that we couldn't do in a general way in zenml."

This is an important product distinction. Because Kitaru artifacts are structured data (JSON/dicts/Pydantic models) rather than opaque blobs (DataFrames, ML models), the dashboard can:
- Show artifact contents inline by default
- Diff artifacts between executions or replay runs
- Enable structured search/filtering over artifact values
- Render artifacts without needing custom materializers

Future work: make this distinction explicit in the artifact system design, dashboard rendering spec, and materializer strategy. The default serialization path should optimize for JSON-friendly types rather than the general-purpose materializer zoo that ZenML needs.

### Python version support: eventually target 3.11+

Current spec targets Python 3.12+ only. Hamza pushed back: "I would like same Python support as zenml (which I believe is >=3.10). A lot of users don't have 3.12." He noted LangGraph requires 3.10+, Temporal supports 3.8+.

Alex's rationale for 3.12: it's the typing dividing line (modern `type` statement, cleaner generics syntax). Hamza accepted 3.12 for now but would have preferred 3.11.

**Consensus:** Ship with 3.12+ for MVP, but plan to add 3.11 support eventually. This will require auditing type annotations and any 3.12-specific syntax. The main cost is typing ergonomics (e.g. `type` statement, some PEP 695 features).

### ZenML branch capability status (March 2026)

The `feature/pause-pipeline-runs` branch has the following status:

- **`zenml.wait(...)`** — works, pauses in-progress runs
- **Resume (Pro/snapshot servers)** — automatic resume when wait condition is resolved
- **Resume (non-Pro/local)** — manual resume required via ZenML CLI command (exists on branch)
- **Wait resolution** — human input only (no webhook/automated triggers yet)
- **Retry failed runs** — ZenML CLI command exists but **does not work yet**

Kitaru implications:
- `kitaru.wait()` is unblocked and can wrap the ZenML primitive now
- Kitaru needs to handle the two resume paths (auto vs manual) and expose a user-friendly `kitaru executions resume` command for the manual path
- `client.executions.retry(...)` should remain stubbed until upstream retry is fixed
- Future work: automated wait resolution via webhooks/events (currently human-only)

### Docs: code snippet contrast and sidebar nesting

Two docs issues flagged:
- ~~Code snippets are hard to read in **light mode** — contrast/colors need adjustment~~ **FIXED**: Switched Shiki themes to `github-light` + `github-dark` and forced code blocks to use the dark variant via `var(--shiki-dark)`, giving high-contrast dark code blocks on the light site.
- The left sidebar has a **double nesting** issue: "Core Concepts > Core Concepts" looks weird

Remaining work: fix the sidebar nesting issue in the FumaDocs theme/config.
