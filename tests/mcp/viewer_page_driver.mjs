// Runs the session viewer page's inlined runtime in a Node VM with a minimal
// DOM, answers its tool calls like an MCP Apps host would, and prints every
// call's operation, session, and the host arguments sent next to `request`.
// Usage: node viewer_page_driver.mjs <runtime.js>
import fs from "node:fs";
import vm from "node:vm";

const runtime = fs.readFileSync(process.argv[2], "utf8");

async function drive(inputs) {
  const calls = [];
  let listener;
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
      }
      queueMicrotask(() => receive({ id: message.id, result }));
    },
  };
  const node = {
    querySelectorAll: () => [], querySelector: () => null, childNodes: [],
    append() {}, replaceChildren() {}, getBoundingClientRect: () => ({ height: 10 }),
  };
  const win = { parent, addEventListener(name, callback) { if (name === "message") listener = callback; } };
  const doc = {
    documentElement: node, head: node, body: node, querySelectorAll: () => [],
    getElementById: () => node, dispatchEvent() {},
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
  // The host sends the next tool's input before that tool's result arrives.
  input("next");
  await win.kitaru.note({ text: "on first" });
  await result("next");
  await win.kitaru.note({ text: "on next" });
  return calls;
}

console.log(JSON.stringify({
  hosted: await drive({ first: { workspace: "research" }, next: { workspace: "second" } }),
  local: await drive({ first: {}, next: {} }),
}));
