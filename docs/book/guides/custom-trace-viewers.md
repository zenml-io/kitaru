---
description: Show sessions to a domain expert in a viewer drawn for their domain, inside the chat, and save their notes to Kitaru.
icon: eye
---

# Review sessions in a custom trace viewer

A trace list suits the engineer who built the agent. It rarely suits the person who knows whether the agent was right: the biologist checking a plasmid annotation, the planner deciding whether a schedule can go to the shop floor, the lawyer reading a contract summary. Shown as a trace, a plasmid is a tool call with a 2,000-character string of `ACGT`, and a production plan is a JSON array of time slots. The expert needs the sequence drawn as a map, or the plan drawn as a Gantt chart, and a way to point at the exact base or assignment that is wrong.

A custom trace viewer is one HTML file that draws a session the way that expert reads it. The Kitaru MCP server opens it inside the conversation, in chat hosts that support [MCP Apps](https://modelcontextprotocol.io/extensions/apps/overview). Every note, investigation answer, and verdict the expert saves there is an ordinary Kitaru annotation or verdict. The Kitaru UI shows it, the assistant reads it back with `kitaru_review_read`, and the [investigation](../concepts/investigations.md) carries on from it.

You do not have to write the HTML. Your coding agent builds the viewer with the `kitaru-trace-viewer` skill, from your own sessions, and the expert reviews in the chat app they already use.

<figure><img src="https://assets.kitaru.ai/docs/screenshots/trace-viewer-gene-map.png" alt="A plasmid annotation session open in a custom viewer inside the ChatGPT desktop app: a feature map over the DNA sequence, the agent's feature table, its summary with the word kanamycin underlined, two checks, and the review panel with the investigation's questions, verdict, and a saved note."><figcaption><p>A plasmid annotation session in a viewer built for a biologist. The checks flag a gene on the wrong strand and the wrong antibiotic in the agent's summary.</p></figcaption></figure>

<figure><img src="https://assets.kitaru.ai/docs/screenshots/trace-viewer-line-schedule.png" alt="A production scheduling session open in a custom viewer inside the ChatGPT desktop app: a Gantt chart of the released plan across four machines, the agent's explanation with a contradicted claim underlined, the orders table, and the review panel."><figcaption><p>A scheduling session in a viewer built for a production planner. The released plan books a rush order during machine maintenance, and the agent never checked it.</p></figcaption></figure>

## What happens when a viewer opens

1. The assistant calls `kitaru_session_view` with a session, or an investigation and a position, and the viewer's name. The result names the session and the investigation's questions, without the session's payloads.
2. The host shows Kitaru's session viewer page in a sandboxed frame.
3. The page reads the session through the app-only `kitaru_viewer_read` tool: its steps with their payloads, its saved notes, and the investigation's questions and verdicts. It reads the viewer's HTML the same way, mounts it, and hands it the session. That data goes to the page, not into the model's context.
4. When the reviewer saves a note, the page calls the app-only `kitaru_viewer_write` tool, and the MCP server writes the annotation with your Kitaru credential.

The page has no network access and never sees the credential; it reaches Kitaru only through those tools. Hosts that support MCP Apps keep `kitaru_viewer_read` and `kitaru_viewer_write` out of the assistant's tool list, so a note saved in the viewer is one a person wrote there.

## Hosts that can show a viewer

A viewer needs a host that supports MCP Apps and passes the page's tool calls on to the MCP server.

| Host | Viewers |
| --- | --- |
| ChatGPT desktop app | Work, with the MCP server started over STDIO |
| Claude Code | Do not work. The terminal shows only the tool's text summary. The desktop app shows the viewer's frame but does not pass the page's tool calls on, so the page reports that it cannot load the session. Build viewers in Claude Code, and review in a chat host. |
| Other MCP Apps hosts | Work when the host passes the page's tool calls on to the MCP server. When it does not, the page says so. |

## Set up the MCP server for viewers

Install the MCP server in a project, as in [Set up your coding agent](../agent-native/setup.md):

```bash
uv add "kitaru[mcp]"
```

Viewers live in one folder, `~/.config/kitaru/viewers` unless you name another with `--viewers-dir` or `KITARU_MCP_VIEWERS_DIR`. The built-in `default` viewer needs no file and shows any session as a list of steps. Saving notes needs `--mode standard`; in `read-only` the viewer opens for reading only.

Chat apps start the MCP server without your shell's `PATH`, so give them absolute paths. `which uv` prints the path to `uv`.

{% tabs %}
{% tab title="ChatGPT desktop app" %}
Add a custom MCP server, choose **STDIO**, and fill in:

| Field | Value |
| --- | --- |
| Command to launch | The absolute path to `uv`, for example `/Users/you/.local/bin/uv` |
| Arguments | One per row: `run`, `--directory`, `/path/to/your/project`, `kitaru-mcp`, `--server`, `http://localhost:8000`, `--mode`, `standard`, `--viewers-dir`, `/path/to/viewers` |

Leave the environment variables and working directory empty, save, and start a new chat.
{% endtab %}

{% tab title="JSON configuration" %}
Hosts configured with an `mcpServers` file take the same command:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/uv",
      "args": ["run", "--directory", "/path/to/your/project", "kitaru-mcp", "--server", "http://localhost:8000", "--mode", "standard", "--viewers-dir", "/path/to/viewers"]
    }
  }
}
```

Restart the host after changing the file.
{% endtab %}
{% endtabs %}

Use the URL of the Kitaru server you work with in place of `http://localhost:8000`. The MCP server reads each viewer file again on every open, so an edited viewer shows the next time it opens, with no restart.

## Have your agent build a viewer

Install the [Kitaru skills](../agent-native/setup.md) in your coding agent, then ask for a viewer in terms of the reviewer and the decision:

> Build a trace viewer for my plasmid annotation agent's sessions. The reviewer is a molecular biologist who decides whether an annotation can go into the registry.

The `kitaru-trace-viewer` skill then:

1. Asks who reviews the sessions, what they look at first, and what makes a session wrong in their eyes.
2. Reads two or three of your sessions, including a failed or surprising one, and notes where the reviewer's material lives in each step.
3. Describes the layout in a few sentences and waits for your agreement before writing code: the main picture, what is clickable, which text is selectable, and which checks the viewer draws, such as a gene on the wrong strand or a job booked during maintenance.
4. Writes the viewer as one HTML file in your viewers folder.
5. Runs the anchor check in the local preview, so notes land on the characters the reviewer selects, and asks you to try one note.

Building works in any coding agent, including Claude Code. The review then happens in a host from the table above. The skill ships with two finished examples, a plasmid map and a production schedule, that it reads for technique.

A viewer's checks are hints drawn on the page for the reviewer. They are never saved as annotations; only what the reviewer writes is.

## Review in a viewer

Ask the assistant to open a session or an investigation in a viewer:

> Open Kitaru investigation `<id>` in the gene-map viewer. I'll review it myself.

In the viewer, the reviewer:

- selects text to note a span of it, or clicks an item such as a feature or an assignment to note that item;
- answers the investigation's questions and sets each session's verdict: acceptable, problematic, or uncertain;
- moves to the next session of the investigation;
- sends a message to the assistant from the viewer when they want to discuss what they see.

When the reviewer is done, ask the assistant to read the notes back, for example "Read my notes on this investigation and tell me what is going wrong across these sessions". It reads them with `kitaru_review_read`. The same notes, answers, and verdicts appear in the Kitaru UI, and notes added there appear in the viewer the next time it opens.

## Preview a viewer without a chat host

`python -m kitaru.mcp.viewer_preview` starts the Kitaru MCP server and a local page that plays the host's part, with the same sandbox and content security policy:

```bash
uv run python -m kitaru.mcp.viewer_preview --server http://localhost:8000 --viewers-dir /path/to/viewers
```

Open the printed address, enter an investigation or session id and the viewer's name, and work with it as a reviewer would. The side panel shows each tool call, the text the model receives, and every message the viewer sends to the assistant. **Check anchors** reports text that would put notes on the wrong characters. `--check` runs the same check without a browser window and exits non-zero on a problem; it needs Playwright (`uv run --with playwright ...`, then `playwright install chromium`).

## Limits

- Investigations keep one answer per question. The viewer updates an existing answer rather than adding another, as the Kitaru UI does, and an answer's location does not change after it is saved.
- The viewer edits notes but does not delete them. Delete one with `kitaru_delete` in `destructive` mode or from the Kitaru UI.
- The first verdict starts a pending investigation, and setting the last missing verdict completes it, as in the Kitaru UI.
- A viewer file is at most 4 MB, and a viewer shows a session's first 2,000 steps.
- A viewer runs with your Kitaru access through the MCP tools. Open only viewers you wrote or trust.

To write or change a viewer by hand, see the [trace viewer reference](trace-viewer-reference.md).
