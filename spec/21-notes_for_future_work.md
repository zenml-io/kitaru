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

### Secrets and infra UX for new users

Alex raised: "We'll need to think about secrets somehow as well. Stacks + the whole infra stuff (from service connectors to spinning up a stack etc) is sort of the big question mark still for how we can make this work well." He noted: "obv I can make it all work assuming I have a nice zenml stack setup already, but for users who don't have a zenml stack already etc... idk how this is going to work well for a cloud stack."

Future work: design the end-to-end cloud stack setup experience for users who have never touched ZenML. This includes credential handling, service connector creation, and making the whole thing feel native to Kitaru rather than requiring ZenML knowledge.

### Revisit whether "stack" is the right user-facing term

Hamza's explicit stack scope definition: "For me a stack in kitaru simply defines the orchestrator, artifact store, and container URI (optionally). I would not include any other concept in it."

If model registration and sandbox registration become separate concepts (see below), then the current "stack" abstraction may be too broad. Hamza suggested renaming to **"runtime"** since it's focused on these execution primitives only.

Future work: once model and sandbox registration decisions are made, revisit whether "stack" should be narrowed/renamed to "runtime" to avoid overloading the concept. The naming decision depends on how many things end up living inside vs outside the stack.

### Decide whether models need their own registry

The current spec (chapter 8) binds model config to a stack-owned `llm_model` component. Hamza proposed an alternative: a **separate model registration flow**:

```
kitaru model register --type openai --openai_key ABCDEDCEDED
```

This would act as a credential/model store independent of stacks. There might be a default type that allows generically adding credentials. In the backend, this could store either a stack component type or even just some secrets with a special prefix in ZenML, so they can always be fetched back.

Alex confirmed the intent: registered models would be "available for use in `kitaru.llm()`" — Hamza: "yeah, it's just a store of credentials."

This affects: credential ownership, alias resolution, model portability across stacks, and CLI shape. Future work: decide between stack-owned `llm_model` vs standalone model registry before implementing `kitaru.llm()`.

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
