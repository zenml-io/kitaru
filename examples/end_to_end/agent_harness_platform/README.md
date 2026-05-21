# Internal agent harness platform (runnable example)

A small, forkable platform layer that turns an agent `Profile` into a durable, sandboxed, profile-gated [PydanticAI](https://ai.pydantic.dev) agent running on [Kitaru](https://kitaru.ai/). It is aimed at platform engineers who want to give their team a fast way to spin up agents with the same safety and operations rails every time. Six stages each add one platform capability: durable execution, a command sandbox, operator-editable procedures, credential isolation, typed service calls, and human approval. The demo prompts are throwaway; the platform shape is the point.

This README is the runnable companion: setup, commands, and what you should see. The full stage-by-stage walkthrough lives in the docs tour at **[kitaru.ai/docs/agent-harness-platform](https://kitaru.ai/docs/agent-harness-platform/)**, and the [production notes](https://kitaru.ai/docs/agent-harness-platform/production-notes/) cover which pieces are teaching stand-ins and what to harden first.

> This is a runnable local reference architecture, not a turnkey enterprise platform and not a hostile-code security boundary. Read the production notes before adopting the pattern for real work.

## Quick start

Stage 1 needs only an OpenAI key, no Docker. From the repository root:

```bash
cd examples/end_to_end/agent_harness_platform
uv sync
uv run kitaru init
export OPENAI_API_KEY=sk-...
uv run python stage_1_basic_agent.py
```

The agent investigates the host (OS, kernel, current user, process count) and returns a one-paragraph summary. That is durable PydanticAI in about 30 lines.

## The six stages

Every stage calls the LLM, so keep `OPENAI_API_KEY` set throughout. Stage 1 runs on its own; stages 2-6 need Docker running and a one-time `bash setup.sh` first (see [Setup for stages 2-6](#setup-for-stages-2-6)). The usual run command is `DISABLE_CACHE=1 uv run python <stage_file>`.

| Stage | Setup | Demonstrates | What to look for |
|---|---|---|---|
| **[1. Durable agent](https://kitaru.ai/docs/agent-harness-platform/01-durable-agent/)**<br>`stage_1_basic_agent.py` | Key only | A PydanticAI agent inside a Kitaru flow; completed turns survive a crash | Re-run after a forced failure: checkpoint `default` returns `cached` (instant, $0) while `default_2` runs fresh |
| **[2. Sandbox](https://kitaru.ai/docs/agent-harness-platform/02-sandbox/)**<br>`stage_2_sandboxed_exec.py` | `bash setup.sh` | Shell commands run in a Docker container, not on your host | `[sandbox]` log lines; `cd /tmp` in turn 1 persists into turn 2; the agent reports Debian/root, not your machine |
| **[3. Skills](https://kitaru.ai/docs/agent-harness-platform/03-skills/)**<br>`stage_3_skills.py` | `bash setup.sh` | The agent reads its procedure from a `SKILL.md` file, not the system prompt | The agent calls `skill(list)` then `skill(read)` before acting; edit the markdown and re-run to change behavior with no Python edits |
| **[4. Credential proxy](https://kitaru.ai/docs/agent-harness-platform/04-credential-proxy/)**<br>`stage_4_credential_proxy.py` | `bash setup.sh` | A separate proxy holds the secret and adds the auth header; the worker never sees the token | `[proxy] injected headers for wiki.local: ['Authorization']` and a `200` from the mock; no `WIKI_TOKEN` exists inside the worker |
| **[5. Typed services](https://kitaru.ai/docs/agent-harness-platform/05-typed-services/)**<br>`stage_5_typed_services.py` | `bash setup.sh` | Structured calls go through a host-side `exec_service` dispatcher instead of shell `curl` | `lookup_wiki` and `publish_summary` produce no `[sandbox]` lines (they run host-side); the proxy stays idle for them |
| **[6. Human approval](https://kitaru.ai/docs/agent-harness-platform/06-hitl/)**<br>`stage_6_hitl.py` | `bash setup.sh` | `ask_question` pauses the flow with `kitaru.wait()` until a human answers | `Waiting on ask_question...`, then a durable pause; the flow resumes once you supply input (see [Stage 6](#stage-6-answering-the-human-pause)) |

## Setup for stages 2-6

With Docker running, once:

```bash
bash setup.sh
```

This builds the three local images (sandbox, proxy, mock services) and creates the `wiki-token` and `webhook-token` Kitaru secrets that the proxy and typed services reference. It is idempotent, so re-running it is safe (run it again if you cloned before a later stage landed).

Then run any stage:

```bash
DISABLE_CACHE=1 uv run python stage_2_sandboxed_exec.py
```

To watch the containers while a stage runs, open a second terminal. `docker ps` shows them suffixed with the execution ID (for example `agent_harness_platform_sandbox_<id>`), and `docker logs <name>` tails one.

## Env toggles

- `DISABLE_CACHE=1` forces every checkpoint to re-execute even when a cached output already exists. Use it when re-running a stage you have run before (otherwise the cached turn replays instantly and you never see the sandbox, proxy, or services actually do work), and whenever you edit a `SKILL.md`.
- `FORCE_FAILURE=1` (stages 1 and 2 only) raises between turn 1 and turn 2 to simulate a crash. This is the durability demo:

```bash
# 1. Crash after turn 1 finishes its real LLM + tool work.
FORCE_FAILURE=1 uv run python stage_1_basic_agent.py
# 2. Re-run without the flag. Turn 1 comes from cache; turn 2 runs fresh.
uv run python stage_1_basic_agent.py
```

| | Step 1 (crash) | Step 2 (re-run) |
|---|---|---|
| `default` | runs (~15s, real LLM calls) | **`cached`** (instant, $0) |
| `default_2` | never runs (the flow raised first) | runs fresh |

Without Kitaru, the crash would throw away turn 1's work and you would pay for both turns on the retry. With Kitaru, only the part that did not finish runs again.

## Stage 6: answering the human pause

Stage 6 pauses at `ask_question`. There are two ways to answer it.

**Interactive** (simplest for a first run):

```bash
DISABLE_CACHE=1 uv run python stage_6_hitl.py
```

The local runtime prompts on the same terminal. Type an answer, press enter, and the flow resumes.

**Non-interactive** (the shape a server uses):

```bash
DISABLE_CACHE=1 uv run python stage_6_hitl.py </dev/null &
# once "Waiting on ask_question..." appears:
uv run kitaru executions list
uv run kitaru executions input <execution_id> --value '"Verified by ops on call"'
```

On the local stack the flow polls for input until a 600s timeout, so you do need to answer it or it eventually times out. On a remote stack the same wait record is answered through the dashboard, CLI, or REST API.

## A note on the logs

Every stage file passes `granular_checkpoints=False` so each agent turn shows up as one readable log block (`default`, `default_2`) while you learn the primitives. A real fork drops that flag and takes the `KitaruAgent` default, where every model request and tool call gets its own checkpoint and cache key. The one exception is wait-bearing tools: keep `ask_question` at flow scope (for example `tool_checkpoint_config_by_name={"ask_question": False}`) so the pause still resolves. The [production notes](https://kitaru.ai/docs/agent-harness-platform/production-notes/) cover this in full.

Replay is a general Kitaru primitive rather than a stage of its own here. See [Replay and overrides](https://kitaru.ai/docs/guides/replay-and-overrides/) to re-run a flow from a chosen checkpoint.

## Forking this for your team

The reusable library lives in `agent_harness_platform/`; each `stage_N_*.py` is a thin entry point on top of it. To adapt it:

- swap `DEFAULT_PROFILE`'s system prompt and `allowed_tools` for what your agents need;
- point `LocalSkillSource` at your team's playbook directory;
- update the mock services to call your real wiki and webhook endpoints.

The profile hardcodes `openai:gpt-5-nano` so stage 1 has the smallest possible setup. In a real deployment, register a model alias (`kitaru model register <alias> --provider openai --model <id> --api-key ...`) and reference the alias in the profile so credentials stay centrally managed.

The [production notes](https://kitaru.ai/docs/agent-harness-platform/production-notes/) walk through each teaching stand-in (the Docker sandbox, local skill files, the self-signed proxy, the mock services) and where you would swap in something your platform team trusts.
