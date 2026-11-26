// Runs the session viewer page's inlined runtime in a Node VM with a minimal
// DOM and answers its tool calls like an MCP Apps host would. It prints, for
// each scenario, what the page sent and what it shows.
// Usage: node viewer_page_driver.mjs <runtime.js>
import fs from "node:fs";
import vm from "node:vm";

const runtime = fs.readFileSync(process.argv[2], "utf8");

async function openPage(inputs) {
  const calls = [];
  const elements = [];
  let listener;
  let holdNote = false;
  let releaseNote = null;
  const receive = (data) => listener({ source: parent, data: { jsonrpc: "2.0", ...data } });
  const parent = {
    postMessage(message) {
      if (!message.id) return;
      let result = {};
      if (message.method === "tools/call") {
        const { request, ...host } = message.params.arguments;
        calls.push({ operation: request.operation, session: request.session_id ?? null, host });
        const data = {
          viewer_source: { html: "<p>viewer</p>" },
          view: {
            session: { id: request.session_id },
            nodes: [],
            nodes_cursor: request.session_id === "first" ? "more" : null,
            annotations: [],
            capabilities: { can_write: true },
          },
          nodes: { nodes: [], nodes_cursor: null },
          note: { id: `note-${calls.length}`, text: request.text },
        }[request.operation];
        result = { structuredContent: { ok: true, data } };
        if (request.operation === "note" && holdNote) {
          holdNote = false;
          releaseNote = () => receive({ id: message.id, result });
          return;
        }
      }
      queueMicrotask(() => receive({ id: message.id, result }));
    },
  };
  const node = {
    querySelectorAll: () => [], querySelector: () => null, childNodes: [],
    append() {}, replaceChildren() {}, getBoundingClientRect: () => ({ height: 10 }),
  };
  const win = {
    parent, getSelection: () => null,
    addEventListener(name, callback) { if (name === "message") listener = callback; },
  };
  const doc = {
    documentElement: node, head: node, body: node, querySelectorAll: () => [],
    getElementById: () => node, dispatchEvent() {},
    createElement(tag) {
      const element = {
        tag, style: {}, handlers: {}, removed: false, append() {}, focus() {}, setAttribute() {},
        addEventListener(name, handler) { this.handlers[name] = handler; },
        remove() { this.removed = true; },
      };
      elements.push(element);
      return element;
    },
  };
  vm.runInNewContext(runtime, {
    window: win, document: doc, console, setTimeout() {},
    crypto: { randomUUID: () => "key" },
    ResizeObserver: class { observe() {} }, CustomEvent: class {},
    DOMParser: class { parseFromString() { return doc; } },
  });
  const settle = async (done) => {
    for (let turn = 0; turn < 100 && !done(); turn++) await new Promise((r) => setImmediate(r));
  };
  const input = (session) => receive({
    method: "ui/notifications/tool-input",
    params: { arguments: { ...inputs[session], request: { session_id: session } } },
  });
  const result = async (session) => {
    receive({ method: "ui/notifications/tool-result", params: { structuredContent: { ok: true, data: { session_id: session } } } });
    await settle(() => win.kitaru?.view?.session?.id === session);
  };
  await settle(() => listener !== undefined);
  input("first");
  await result("first");
  const holdNextNote = async (save) => {
    holdNote = true;
    const saved = save();
    await settle(() => releaseNote !== null);
    return { saved, release: () => releaseNote() };
  };
  return { win, calls, elements, input, result, holdNextNote };
}

// The host sends the next tool's input before that tool's result arrives.
async function workspaces(inputs) {
  const { win, calls, input, result } = await openPage(inputs);
  input("next");
  await win.kitaru.note({ text: "on first" });
  await result("next");
  await win.kitaru.note({ text: "on next" });
  return calls;
}

// A note box and a save are both in progress on the first session when the
// next one renders.
async function sessionChangeDuringEdits() {
  const { win, calls, elements, input, result, holdNextNote } = await openPage({ first: {}, next: {} });
  win.kitaru.ui.compose({ kind: "session", selector: null, quote: null }, { title: "Note on first" });
  const box = elements.find((e) => e.tag === "textarea");
  const save = elements.find((e) => e.textContent === "Save");
  const composer = elements.find((e) => e.className === "kv-composer");
  box.value = "Draft for first";
  const pending = await holdNextNote(() => win.kitaru.note({ text: "Pending save for first" }));
  input("next");
  await result("next");
  pending.release();
  await pending.saved;
  const composerWasStillOpen = !composer.removed;
  if (composerWasStillOpen) await save.handlers.click();
  return {
    current: win.kitaru.view.session.id,
    appended: win.kitaru.allNotes().map((n) => n.text),
    composerWasStillOpen,
    noteTargets: calls.filter((c) => c.operation === "note").map((c) => c.session),
  };
}

console.log(JSON.stringify({
  hosted: await workspaces({ first: { workspace: "research" }, next: { workspace: "second" } }),
  local: await workspaces({ first: {}, next: {} }),
  sessionChange: await sessionChangeDuringEdits(),
}));
