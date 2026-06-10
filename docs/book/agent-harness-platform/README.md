---
description: The Agent Harness Platform tour now lives in the ZenML Learn section as the Agents guide
icon: robot
---

# Agent Harness Platform

The Agent Harness Platform tour has moved. It is now the **[Agents guide](https://docs.zenml.io/user-guides/agents-guide)** in the ZenML Learn section, where it sits alongside the Starter, Production, and LLMOps guides.

The guide is the same stage-by-stage tour of a runnable internal agent platform built on Kitaru and PydanticAI:

1. [A durable agent](https://docs.zenml.io/user-guides/agents-guide/01-durable-agent) — the smallest PydanticAI agent inside a Kitaru flow
2. [Sandboxed commands](https://docs.zenml.io/user-guides/agents-guide/02-sandbox) — shell work in a Docker sandbox, not on the host
3. [Skills](https://docs.zenml.io/user-guides/agents-guide/03-skills) — operator-editable procedures in markdown files
4. [Credential proxy](https://docs.zenml.io/user-guides/agents-guide/04-credential-proxy) — secrets stay out of the worker
5. [Typed services](https://docs.zenml.io/user-guides/agents-guide/05-typed-services) — structured, profile-gated internal actions
6. [Human in the loop](https://docs.zenml.io/user-guides/agents-guide/06-hitl) — durable pauses with `kitaru.wait()`

plus [Production notes and upgrade paths](https://docs.zenml.io/user-guides/agents-guide/production-notes) for hardening guidance.

The runnable code is unchanged and stays in this repository: [`examples/end_to_end/agent_harness_platform/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/agent_harness_platform).
