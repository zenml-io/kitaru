---
description: The contract between a custom trace viewer and Kitaru's session viewer page, for writing or changing a viewer by hand.
icon: code
---

# Trace viewer reference

This page is for writing or changing a viewer by hand, or for checking what an agent wrote. To set up viewers and have your agent build one, start with [Review sessions in a custom trace viewer](custom-trace-viewers.md).

## The file

A viewer is one complete HTML page with inline `<style>` blocks and inline `<script>` blocks. There is no build step.

- The page's content security policy blocks external scripts, stylesheets, fonts, images, and network calls, and the session viewer page skips `<script src>`. Inline small images as `data:` URLs and draw with SVG and CSS.
- The file name without `.html` is the viewer's name: lowercase letters, digits, `-`, and `_`, starting with a letter or digit, at most 64 characters. `default` is reserved for the built-in viewer.
- A file is at most 4 MB.

The session viewer page mounts the file's body into its own document, adds its styles, and runs each script once, in document order, keeping attributes such as `type="module"`. Data blocks such as `<script type="application/json">` stay in place and do not run. `DOMContentLoaded` has already fired by then. Render in `kitaru.onSession`, which runs after mounting and again whenever the reviewer moves to another session of an investigation:

```html
<!doctype html>
<html>
<head><style>.answer { font-size: 15px; line-height: 1.55; }</style></head>
<body>
  <h1 id="title"></h1>
  <div class="answer" id="answer"></div>
  <aside id="review"></aside>
  <script>
    kitaru.ui.reviewPanel(document.getElementById("review"));
    kitaru.ui.enableSelectionNotes();
    kitaru.onSession((view) => {
      document.getElementById("title").textContent = view.session.name;
      const last = view.nodes.findLast((node) => node.node_type === "llm_call");
      const found = last && kitaru.nodeText(last, "output");
      if (found) kitaru.renderText(document.getElementById("answer"), { text: found.text, nodeId: last.id, path: found.path });
    });
  </script>
</body>
</html>
```

The built-in `default` viewer is a complete viewer that works on any session. Copy it as a starting point: `python -c "import kitaru.mcp, pathlib; print(pathlib.Path(kitaru.mcp.__file__).parent / 'ui' / 'viewers' / 'default.html')"` prints its path.

## The view

`kitaru.onSession(view => …)` receives the open session. Render everything from `view` on every call.

| Field | Contents |
| --- | --- |
| `view.session` | The session record: `id`, `name`, `number`, `status`, `inputs`, `outputs`, `error`, `metadata`, the text selectors, counts, and cost |
| `view.nodes` | The session's steps with their payloads, in recorded order, up to 2,000; `view.nodes_truncated` is `true` when cut. Each has `id`, `external_id`, `parent_external_id`, `node_type` (`llm_call`, `tool_call`, `subagent_call`, or `span`), `name`, `status`, `error`, `inputs`, `outputs`, `reasoning`, the text selectors, `model`, `tool_name`, `tokens`, `started_at`, and `ended_at` |
| `view.annotations` | Saved notes on this session, and answers to the open investigation |
| `view.investigation` | `null`, or the investigation's `id`, `name`, and `status`, the open `position`, every session's position and verdict in `sessions`, and the open `investigation_session` with its `questions` and `verdict` |
| `view.capabilities.can_write` | `false` when the MCP server runs in `read-only` mode |
| `view.links.session` | The session in the Kitaru UI, when the server reports a dashboard address |

Payload text is data from the agent's run. Insert it only with `textContent` or `kitaru.renderText`, never as HTML: a crafted trace could otherwise run code in the page and save notes the reviewer never wrote.

## Anchors: where a note is saved

A note is saved with a selector: an optional step id (`node_id`), a JSON Pointer `path`, and an optional character `span`.

- Step paths start at the step record, with the keys `inputs`, `outputs`, `error`, `metadata`, `reasoning`, `model_params`, and `attributes`, for example `/outputs/features/2`.
- Session paths start at the session record, with the keys `inputs`, `outputs`, `error`, and `metadata`. A selector without a step id is on the session.

These are the paths the Kitaru UI writes, so notes saved in either place show in both.

Mark what the reviewer can point at:

- **Text.** `kitaru.renderText(element, { text, nodeId, path, ranges, decorate })` fills the element with the stored string exactly as stored, marks it as a text anchor, and splits it where saved span notes and your `ranges` begin and end. Saved notes get the `kv-note` class; `decorate(piece, ranges)` styles each piece, for example to underline a claim the session's own data contradicts. Call it again to repaint.
- **A field.** `kitaru.mark(element, { nodeId, path, kind: "field" })` makes any element, SVG shapes included, a note target for the value at `path`. `kitaru.ui.noteButton({ nodeId, path, kind: "field" })` returns a ready-made button that opens the note box for it and shows its note count.
- **A whole step.** `kitaru.mark(element, { nodeId, kind: "node" })`, without a path.

The marks are the Kitaru UI's attributes, `data-annot-scope`, `data-annot-kind`, and `data-annot-path`, so you can also set them directly.

Three rules keep notes on the characters the reviewer selected:

1. A text anchor's text content must be the stored string at its path, or a leading part of it. Never put line numbers, labels, rendered markdown, inserted spaces, or ellipses inside it. Put gutters and labels in sibling elements, and wrap long text with CSS: `white-space: pre-wrap`, `word-break: break-all`, a width in `ch`.
2. A text anchor's path must resolve to a string; a field anchor's path must resolve to a value.
3. A span counts UTF-16 code units from the start of the anchor's text, the unit of JavaScript string length, so an emoji counts as two. `kitaru.selection()` measures this way already.

The MCP server checks every saved span against the stored text, using the selected words the page sends with it. A span that does not match moves to where those words appear, or the note covers the whole field when they appear more than once or not at all, and the reviewer sees a warning. A path that does not resolve is refused. The preview's **Check anchors** button and `kitaru.selfCheck()` report anchors that break the rules before a reviewer meets them.

## The `kitaru` object

| Member | What it does |
| --- | --- |
| `onSession(callback)` | Calls `callback(view)` with the open session, now and after every move to another session |
| `onAnnotations(callback)` | Calls `callback(annotations)` after every saved note, answer, edit, or verdict |
| `view`, `hostContext` | The open session, and the host's theme and display mode |
| `nodeText(node, part)`, `sessionText(part)` | The prose of an `input`, `output`, `system`, `reasoning`, or `error` part as `{ text, path }`, found through the record's text selectors; `null` when absent |
| `pointer(record, path)`, `join(...keys)`, `findNode(id)` | Resolve a JSON Pointer, build one from keys, and find a step by id |
| `renderText`, `mark`, `target(element)`, `selection()` | Make anchors, and read an element's anchor or the current text selection as `{ selector, quote, element }` |
| `note({ selector, quote, text })` | Save a note on the session, a step, a field, or a span; resolves to `{ annotation, warnings }` |
| `answer(questionKey, { text })` | Save the answer to an investigation question, or update it when one exists; it is anchored where the question's first highlight points |
| `edit(annotationId, text)` | Change a note's text; its location stays as it was |
| `setVerdict(verdict)` | Set the open session's verdict: `acceptable`, `problematic`, `uncertain`, or `null` to clear it |
| `notesFor({ nodeId, path })`, `sessionNotes()`, `allNotes()`, `answerFor(key)`, `quoteOf(annotation)`, `annotationText(annotation)` | Read saved notes back to paint them |
| `refreshMarks()`, `reveal(selector)` | Repaint note marks; scroll to an anchor and flash it |
| `goTo(position)` | Open another session of the investigation |
| `tellAssistant(text)`, `shareContext(text)` | Send a message to the conversation as the reviewer; give the assistant context without a message |
| `openLink(url)`, `toggleFullscreen()`, `reportSize()` | Ask the host to open a link, switch the frame between inline and full screen, and resize the frame after a layout change |
| `selfCheck()` | Report text anchors whose rendered text differs from the stored text |

### The review kit

`kitaru.ui` holds optional controls that match the Kitaru UI's review flow. A viewer that wants its own controls can use the functions above instead.

| Member | What it does |
| --- | --- |
| `reviewPanel(element)` | Fills the element with navigation between sessions, the investigation's questions, the verdict buttons, the saved notes with inline editing, a whole-session note, and links to the assistant and the Kitaru UI |
| `enableSelectionNotes({ title })` | Offers a note button when the reviewer selects text inside a text anchor; `title(selection)` names the selection in the reviewer's terms |
| `noteButton(target, { label, title })` | A button marked as a field or step anchor that opens the note box for it |
| `compose(target, { title })`, `closeComposer()` | Open and close the note box for an anchor |
| `toast(text)` | Show a short status message |

## Styling

The page defines CSS variables that follow the host's light or dark theme: `--kv-card`, `--kv-subtle`, `--kv-ink`, `--kv-ink-2`, `--kv-ink-3`, `--kv-border`, `--kv-brand`, `--kv-brand-soft`, `--kv-good`, `--kv-bad`, `--kv-warn`, `--kv-sans`, `--kv-mono`, and `--kv-radius`. The classes `kv-btn`, `kv-primary`, `kv-link`, `kv-icon`, `kv-input`, `kv-label`, `kv-hint`, and `kv-muted` match the review kit.

A viewer sits in a narrow frame next to a conversation, so keep it clean:

- One picture carries the domain: the map, chart, or document is the largest thing on the page.
- Separate sections with space or a hairline rather than bordered cards; keep page padding around 14 to 16 px.
- One header line: the session's name, then a short muted line with the two or three facts that matter.
- Few words: small sentence-case labels, no instructions on the page, one line per check with its reason in a tooltip, and details only for what the reviewer selected.
- Label shapes only where the label fits, and name the rest on hover.
- Use color for meaning, such as a type, a status, or a broken rule, and take it from the theme variables so both themes work.
