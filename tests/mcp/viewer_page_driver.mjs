// Runs the session viewer page's inlined runtime in a Node VM with a minimal
// DOM and answers its tool calls like an MCP Apps host would. It prints, for
// each scenario, what the page sent and what it shows.
// Usage: node viewer_page_driver.mjs <runtime.js>
import fs from "node:fs";
import vm from "node:vm";

const runtime = fs.readFileSync(process.argv[2], "utf8");

function viewData(request) {
  if (!request.investigation_id) {
    return {
      session: { id: request.session_id },
      nodes: [],
      nodes_cursor: request.session_id === "first" ? "more" : null,
      annotations: [],
      capabilities: { can_write: true },
    };
  }
  const position = request.position ?? 0;
  return {
    session: { id: `review-${position}` },
    nodes: [],
    nodes_cursor: null,
    annotations: [],
    capabilities: { can_write: true },
    investigation: {
      id: request.investigation_id, name: "Review", position, status: "open",
      sessions: [{ position: 0, verdict: null }, { position: 1, verdict: null }],
      investigation_session: {
        id: `link-${request.investigation_id}-${position}`, verdict: null,
        questions: [{ key: "cause", question: "What went wrong?" }],
      },
    },
  };
}

async function openPage(inputs, opening = { session_id: "first" }) {
  const calls = [];
  const elements = [];
  const listeners = {};
  let timers = [];
  let listener;
  let held = null;
  let release = null;
  let selected = null;
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
          view: viewData(request),
          nodes: { nodes: [], nodes_cursor: null },
          note: { id: `note-${calls.length}`, text: request.text },
          verdict: {
            investigation_session: { id: "link-saved", verdict: request.verdict, questions: [] },
            investigation_status: "in_progress",
          },
        }[request.operation];
        result = { structuredContent: { ok: true, data } };
        if (request.operation === held) {
          held = null;
          release = () => receive({ id: message.id, result });
          return;
        }
      }
      queueMicrotask(() => receive({ id: message.id, result }));
    },
  };
  const node = {
    querySelectorAll: () => [], querySelector: () => null, childNodes: [], clientWidth: 800,
    append() {}, replaceChildren() {}, getBoundingClientRect: () => ({ height: 10 }),
  };
  const win = {
    parent, scrollX: 0, scrollY: 0, scrollTo() {},
    getSelection: () => selected ?? { rangeCount: 0, isCollapsed: true, removeAllRanges() {} },
    addEventListener(name, callback) { if (name === "message") listener = callback; },
  };
  const doc = {
    documentElement: node, head: node, body: node, querySelectorAll: () => [],
    getElementById: () => node, dispatchEvent() {},
    addEventListener(name, handler) { (listeners[name] ||= []).push(handler); },
    createRange: () => ({ selectNodeContents() {}, setEnd() {}, toString: () => "" }),
    createElement(tag) {
      const element = {
        tag, style: {}, handlers: {}, attributes: {}, removed: false, append() {}, focus() {},
        setAttribute(name, value) { this.attributes[name] = value; },
        contains: () => false,
        addEventListener(name, handler) { this.handlers[name] = handler; },
        remove() { this.removed = true; },
      };
      elements.push(element);
      return element;
    },
  };
  vm.runInNewContext(runtime, {
    window: win, document: doc, console, Node: { ELEMENT_NODE: 1 },
    setTimeout(callback) { timers.push(callback); },
    crypto: { randomUUID: () => "key" },
    ResizeObserver: class { observe() {} }, CustomEvent: class {},
    DOMParser: class { parseFromString() { return doc; } },
  });
  const settle = async (done) => {
    for (let turn = 0; turn < 100 && !done(); turn++) await new Promise((r) => setImmediate(r));
  };
  // The host sends a tool's input, then its result; the page renders the
  // session the result names.
  const show = async (request, host = inputs[request.session_id] ?? {}) => {
    const before = win.kitaru?.view;
    receive({ method: "ui/notifications/tool-input", params: { arguments: { ...host, request } } });
    receive({ method: "ui/notifications/tool-result", params: { structuredContent: { ok: true, data: request } } });
    await settle(() => win.kitaru?.view && win.kitaru.view !== before);
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
  await show(opening);
  // Hold the next write of `operation` until the scenario releases it.
  const holdNext = async (operation, write) => {
    held = operation;
    release = null;
    const saved = write();
    await settle(() => release !== null);
    return { saved, release: () => release() };
  };
  const holdNextNote = (save) => holdNext("note", save);
  const compose = (text) => {
    win.kitaru.ui.compose({ kind: "session", selector: null, quote: null }, { title: text });
    const box = elements.findLast((e) => e.tag === "textarea");
    box.value = text;
    return {
      composer: elements.findLast((e) => e.className === "kv-composer"),
      save: elements.findLast((e) => e.textContent === "Save"),
    };
  };
  // Select text in a node of the session on screen, as a reviewer would.
  const select = (nodeId) => {
    const block = {
      nodeType: 1,
      closest: () => block,
      getAttribute: (name) => (name === "data-annot-scope" ? `node:${nodeId}` : "/outputs/text"),
    };
    const range = {
      startContainer: block, endContainer: block, startOffset: 0, toString: () => "quoted",
      getBoundingClientRect: () => ({ top: 50, right: 100, bottom: 70, left: 10 }),
    };
    selected = { rangeCount: 1, isCollapsed: false, getRangeAt: () => range, removeAllRanges() {} };
    for (const handler of listeners.mouseup || []) handler();
    const due = timers;
    timers = [];
    for (const callback of due) callback();
  };
  const notes = () => win.kitaru.allNotes().map((n) => n.text);
  const panel = () => {
    const container = {
      classList: { add() {} }, contains: () => false, querySelector: () => null,
      replaceChildren() {},
    };
    win.kitaru.ui.reviewPanel(container);
  };
  // The newest answer box the panel drew for `question`.
  const answerBox = (question) => elements.findLast((e) => e.tag === "textarea" && e.attributes["data-draft"]?.endsWith(`:${question}`));
  return { win, calls, elements, show, input, result, holdNext, holdNextNote, compose, select, notes, panel, answerBox };
}

const noteTargets = (calls) => calls.filter((c) => c.operation === "note").map((c) => c.session);

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
  const { win, calls, input, result, holdNextNote, compose, notes } = await openPage({ first: {}, next: {} });
  const { composer, save } = compose("Draft for first");
  const pending = await holdNextNote(() => win.kitaru.note({ text: "Pending save for first" }));
  input("next");
  await result("next");
  pending.release();
  await pending.saved;
  const composerWasStillOpen = !composer.removed;
  if (composerWasStillOpen) await save.handlers.click();
  return { current: win.kitaru.view.session.id, appended: notes(), composerWasStillOpen, noteTargets: noteTargets(calls) };
}

// The host renders the session on screen again while a box and a save on it
// are in progress.
async function sameSessionRerender() {
  const { win, calls, show, holdNextNote, compose, notes } = await openPage({ first: { workspace: "research" } });
  const { composer } = compose("Draft for first");
  const pending = await holdNextNote(() => win.kitaru.note({ text: "Pending save for first" }));
  await show({ session_id: "first" });
  pending.release();
  await pending.saved;
  return { current: win.kitaru.view.session.id, appended: notes(), composerStillOpen: !composer.removed, noteTargets: noteTargets(calls) };
}

// The reviewer opens the investigation position on screen again, then the next.
async function investigationPositions() {
  const { win, compose } = await openPage({}, { investigation_id: "review", position: 0 });
  const { composer } = compose("Draft for position 0");
  await win.kitaru.goTo(0);
  const keptOnSamePosition = !composer.removed;
  await win.kitaru.goTo(1);
  return { position: win.kitaru.view.investigation.position, keptOnSamePosition, closedOnNextPosition: composer.removed };
}

// The reviewer saves on the first session, moves to the next, and starts a
// note there before the first save finishes.
async function earlierSaveFinishesLater() {
  const { win, calls, input, result, holdNextNote, compose, notes } = await openPage({ first: {}, next: {} });
  const first = compose("Saved on first");
  const pending = await holdNextNote(() => first.save.handlers.click());
  input("next");
  await result("next");
  const next = compose("Draft for next");
  pending.release();
  await pending.saved;
  return { nextBoxStillOpen: !next.composer.removed, nextNotes: notes(), noteTargets: noteTargets(calls) };
}

// A floating "+ Note" button offered for text on the first session.
async function noteButtonAcrossSessions() {
  const { win, elements, input, result, select } = await openPage({ first: {}, next: {} });
  win.kitaru.ui.enableSelectionNotes();
  select("first-node");
  const button = elements.findLast((e) => e.textContent === "+ Note");
  const offered = button !== undefined && !button.removed;
  input("next");
  await result("next");
  return { offered, removedWithFirst: button?.removed ?? false };
}

// The host renders the investigation position on screen again while a
// verdict write is in flight.
async function verdictDuringRerender() {
  const { win, show, holdNext } = await openPage({}, { investigation_id: "review", position: 0 });
  const pending = await holdNext("verdict", () => win.kitaru.setVerdict("problematic"));
  await show({ investigation_id: "review", position: 0 });
  pending.release();
  await pending.saved;
  return { shownVerdict: win.kitaru.view.investigation.investigation_session.verdict };
}

// One session belongs to two investigations; a draft answer typed in the
// first stays there.
async function answerDraftAcrossInvestigations() {
  const { show, panel, answerBox } = await openPage({}, { investigation_id: "first-review", position: 0 });
  panel();
  const box = answerBox("cause");
  box.value = "Draft for the first review";
  box.handlers.input();
  await show({ investigation_id: "first-review", position: 0 });
  const keptOnRerender = answerBox("cause").value;
  await show({ investigation_id: "second-review", position: 0 });
  const shownInSecond = answerBox("cause").value;
  await show({ investigation_id: "first-review", position: 0 });
  return { keptOnRerender, shownInSecond, keptOnReturn: answerBox("cause").value };
}

console.log(JSON.stringify({
  hosted: await workspaces({ first: { workspace: "research" }, next: { workspace: "second" } }),
  local: await workspaces({ first: {}, next: {} }),
  sessionChange: await sessionChangeDuringEdits(),
  sameSessionRerender: await sameSessionRerender(),
  investigationPositions: await investigationPositions(),
  earlierSaveFinishesLater: await earlierSaveFinishesLater(),
  noteButtonAcrossSessions: await noteButtonAcrossSessions(),
  verdictDuringRerender: await verdictDuringRerender(),
  answerDraftAcrossInvestigations: await answerDraftAcrossInvestigations(),
}));
