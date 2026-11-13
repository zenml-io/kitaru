// Kitaru viewer runtime: the `window.kitaru` API every trace viewer page uses.
//
// The session viewer page inlines this script. The view tool's result names a
// session; this script reads that session and the requested viewer's HTML
// with tool calls, mounts the viewer into this same document, and hands it
// the session. A viewer only
// renders; this script talks to the host, and through it to the Kitaru MCP
// server, which holds the credential. The page itself has no network access.
"use strict";
(() => {
  const PROTOCOL_VERSION = "2026-01-26";
  const READ_TOOL = "kitaru_viewer_read";
  const WRITE_TOOL = "kitaru_viewer_write";
  const MAX_NODES = 2000;
  const NODE_PAGE = 500;
  const ATTR = { scope: "data-annot-scope", kind: "data-annot-kind", path: "data-annot-path", noted: "data-annot-noted", flash: "data-annot-flash" };
  const VERDICTS = ["acceptable", "problematic", "uncertain"];

  // ---------- JSON-RPC over postMessage ----------
  let nextId = 1;
  const pending = new Map();
  function request(method, params, timeoutMs = 120000) {
    const id = nextId++;
    window.parent.postMessage({ jsonrpc: "2.0", id, method, params }, "*");
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      setTimeout(() => { if (pending.delete(id)) reject(new Error(`${method} timed out`)); }, timeoutMs);
    });
  }
  const notify = (method, params) => window.parent.postMessage({ jsonrpc: "2.0", method, params }, "*");

  const state = { args: {}, view: null, openTicket: 0, mounted: false, mounting: null, hostContext: {}, sessionCallbacks: [], annotationCallbacks: [], nodeIndex: new Map() };

  window.addEventListener("message", (event) => {
    if (event.source !== window.parent) return;
    const m = event.data;
    if (!m || m.jsonrpc !== "2.0") return;
    if (m.id != null && !m.method) {
      const p = pending.get(m.id);
      if (p) { pending.delete(m.id); m.error ? p.reject(Object.assign(new Error(m.error.message || "Request failed"), { rpc: m.error })) : p.resolve(m.result); }
      return;
    }
    if (m.id != null) {
      const known = m.method === "ui/resource-teardown" || m.method === "ping";
      window.parent.postMessage(known ? { jsonrpc: "2.0", id: m.id, result: {} } : { jsonrpc: "2.0", id: m.id, error: { code: -32601, message: "Method not found" } }, "*");
      return;
    }
    const params = m.params || {};
    if (m.method === "ui/notifications/tool-input") state.args = params.arguments || {};
    else if (m.method === "ui/notifications/tool-result") onToolResult(params);
    else if (m.method === "ui/notifications/tool-cancelled") showStatus("Opening the session was cancelled.");
    else if (m.method === "ui/notifications/host-context-changed") applyHostContext(params);
  });

  let reportedHeight = 0;
  function reportSize() {
    const height = Math.ceil(document.documentElement.getBoundingClientRect().height);
    if (height !== reportedHeight) { reportedHeight = height; notify("ui/notifications/size-changed", { height }); }
  }

  function applyHostContext(ctx) {
    state.hostContext = { ...state.hostContext, ...ctx };
    const root = document.documentElement;
    if (ctx.theme) root.setAttribute("data-theme", ctx.theme);
    for (const [name, value] of Object.entries(ctx.styles?.variables || {})) if (value) root.style.setProperty(name, value);
    const fonts = ctx.styles?.css?.fonts;
    if (fonts && !document.getElementById("kv-host-fonts")) { const s = document.createElement("style"); s.id = "kv-host-fonts"; s.textContent = fonts; document.head.append(s); }
    document.dispatchEvent(new CustomEvent("kitaru:host-context", { detail: state.hostContext }));
  }

  // ---------- Tool calls ----------
  function envelopeOf(result) {
    if (result?.structuredContent) return result.structuredContent;
    try { return JSON.parse(result?.content?.[0]?.text ?? ""); } catch { return null; }
  }
  async function callTool(name, args) {
    const envelope = envelopeOf(await request("tools/call", { name, arguments: { request: args } }));
    if (!envelope) throw new Error(`${name} returned no data`);
    if (!envelope.ok) throw new Error(envelope.error?.message || `${name} failed`);
    return envelope;
  }

  async function onToolResult(result) {
    const envelope = envelopeOf(result);
    if (envelope && !envelope.ok) { showStatus(envelope.error?.message || "The session could not be opened.", true); return; }
    // Some hosts forward only the text content; the tool's input names the same session.
    const target = viewTarget(envelope?.data || state.args?.request);
    if (!target) { showStatus(result?.content?.find((c) => c.type === "text")?.text || "No session data arrived.", true); return; }
    await openView(target);
  }
  function viewTarget(source) {
    if (!source) return null;
    const viewer = source.viewer || "default";
    if (source.investigation_id) return { investigation_id: source.investigation_id, position: source.position ?? 0, viewer };
    return source.session_id ? { session_id: source.session_id, viewer } : null;
  }

  async function openView(target) {
    // A later open supersedes this one, so quick clicks between sessions never
    // leave an older session on screen.
    const ticket = ++state.openTicket;
    try {
      showStatus("Loading the session…");
      const current = () => ticket === state.openTicket;
      const reading = callTool(READ_TOOL, { operation: "view", ...target }).then(async (env) => { await loadRemainingNodes(env.data, current); return env; });
      const [envelope] = await Promise.all([reading, mountViewer(target.viewer)]);
      const data = envelope.data;
      if (!current()) return false;
      state.view = data;
      state.nodeIndex = new Map(data.nodes.map((n) => [n.id, n]));
      (envelope.warnings || []).forEach((w) => console.warn(`[kitaru] ${w}`));
      hideStatus();
      // Callbacks paint text through renderText, so only the counts remain.
      for (const cb of state.sessionCallbacks) safeCall(cb, data);
      updateNoted();
      reportSize();
      return true;
    } catch (error) {
      if (ticket === state.openTicket) showStatus(describeFailure(error), true);
      return false;
    }
  }
  // A JSON-RPC error here comes from the host, not from Kitaru: the server
  // reports its own failures inside a tool result.
  function describeFailure(error) {
    if (!error.rpc) return error.message || String(error);
    return `This host shows the viewer but did not pass its request on to the Kitaru MCP server, so the session cannot load here. Open it in another MCP Apps host, such as the ChatGPT desktop app, or preview it with python -m kitaru.mcp.viewer_preview. The host said: ${error.message}`;
  }

  async function loadRemainingNodes(data, current) {
    data.nodes_truncated = false;
    while (data.nodes_cursor && current()) {
      if (data.nodes.length >= MAX_NODES) { data.nodes_truncated = true; break; }
      const page = (await callTool(READ_TOOL, { operation: "nodes", session_id: data.session.id, cursor: data.nodes_cursor, size: NODE_PAGE })).data;
      data.nodes.push(...page.nodes);
      data.nodes_cursor = page.nodes_cursor;
    }
  }

  // ---------- Mounting the viewer page ----------
  function isJavaScript(script) {
    const type = (script.getAttribute("type") || "").trim().toLowerCase();
    return !type || type === "module" || /^(text|application)\/(x-)?(java|ecma)script$/.test(type);
  }
  async function mountViewer(name) {
    if (state.mounted) return;
    if (!state.mounting) state.mounting = (async () => {
      const { html } = (await callTool(READ_TOOL, { operation: "viewer_source", viewer: name })).data;
      const doc = new DOMParser().parseFromString(html, "text/html");
      for (const node of doc.head.querySelectorAll("style")) { const s = document.createElement("style"); s.textContent = node.textContent; s.dataset.kvViewer = name; document.head.append(s); }
      if (doc.head.querySelector('link[rel="stylesheet"]')) console.warn("[kitaru] Viewer stylesheets must be inline <style> blocks; <link> is blocked by the page's content security policy.");
      // Data blocks such as `type="application/json"` stay where they are and
      // never run; only JavaScript is re-created so that it executes.
      const scripts = [...doc.head.querySelectorAll("script"), ...doc.body.querySelectorAll("script")].filter(isJavaScript);
      scripts.forEach((s) => s.remove());
      for (const block of doc.head.querySelectorAll("script")) document.head.append(document.importNode(block, true));
      const root = document.getElementById("kv-root");
      root.replaceChildren(...[...doc.body.childNodes].map((n) => document.importNode(n, true)));
      for (const original of scripts) {
        if (original.src) { console.warn(`[kitaru] Skipped external script ${original.src}: viewers must inline their scripts.`); continue; }
        const s = document.createElement("script");
        for (const { name, value } of original.attributes) s.setAttribute(name, value);
        s.textContent = original.textContent;
        document.body.append(s); // Inline scripts run on insertion, in document order.
      }
      state.mounted = true;
    })();
    try { await state.mounting; }
    catch (error) { state.mounting = null; throw error; } // A later open retries.
  }

  // ---------- Status ----------
  function showStatus(text, isError = false) {
    const el = document.getElementById("kv-status");
    if (!el) return;
    el.hidden = false; el.textContent = text; el.className = `kv-status${isError ? " kv-error" : ""}`;
    reportSize();
  }
  function hideStatus() { const el = document.getElementById("kv-status"); if (el) el.hidden = true; }
  function safeCall(cb, ...args) { try { cb(...args); } catch (error) { console.error("[kitaru] viewer callback failed", error); showStatus(`The viewer failed to render: ${error.message}`, true); } }

  // ---------- JSON Pointer and text helpers ----------
  function pointer(doc, path) {
    if (path === "" || path == null) return doc;
    if (!path.startsWith("/")) return undefined;
    let cur = doc;
    for (const raw of path.slice(1).split("/")) {
      const token = raw.replace(/~1/g, "/").replace(/~0/g, "~");
      if (cur && typeof cur === "object" && !Array.isArray(cur) && Object.hasOwn(cur, token)) cur = cur[token];
      else if (Array.isArray(cur) && /^(0|[1-9]\d*)$/.test(token) && Number(token) < cur.length) cur = cur[Number(token)];
      else return undefined;
    }
    return cur;
  }
  const escapeToken = (t) => String(t).replace(/~/g, "~0").replace(/\//g, "~1");
  const join = (...tokens) => tokens.map((t) => `/${escapeToken(t)}`).join("");

  // Where a node's or session's prose lives, as record-rooted pointers: the
  // same paths the Kitaru UI writes for a note on that text.
  const NODE_PARTS = {
    input: (n) => n.input_text_selector != null ? `/inputs${n.input_text_selector}` : null,
    output: (n) => n.output_text_selector != null ? `/outputs${n.output_text_selector}` : null,
    system: (n) => n.system_prompt_selector != null ? `/inputs${n.system_prompt_selector}` : null,
    reasoning: (n) => typeof n.reasoning === "string" ? "/reasoning" : null,
    error: (n) => typeof n.error === "string" ? "/error" : null,
  };
  function nodeText(node, part) {
    const path = NODE_PARTS[part]?.(node);
    if (path == null) return null;
    const text = pointer(node, path);
    return typeof text === "string" ? { text, path } : null;
  }
  // A session record has the same input, output, and error selectors.
  const sessionText = (part) => (state.view ? nodeText(state.view.session, part) : null);
  const findNode = (id) => state.nodeIndex.get(id);
  function recordFor(nodeId) { return nodeId ? findNode(nodeId) : state.view?.session; }

  // ---------- Anchors (the Kitaru UI's data-annot-* contract) ----------
  function anchor({ nodeId = null, path = null, kind = "text" } = {}) {
    const attrs = { [ATTR.kind]: kind };
    if (nodeId) attrs[ATTR.scope] = `node:${nodeId}`;
    if (path != null) attrs[ATTR.path] = path;
    return attrs;
  }
  function mark(el, target) { for (const [k, v] of Object.entries(anchor(target))) el.setAttribute(k, v); return el; }
  function scopeNodeId(el) {
    const scoped = el.closest(`[${ATTR.scope}]`);
    const value = scoped?.getAttribute(ATTR.scope) || "";
    return value.startsWith("node:") ? value.slice(5) : null;
  }
  function targetOf(el) {
    const target = el?.closest?.(`[${ATTR.kind}]`);
    if (!target) return null;
    const kind = target.getAttribute(ATTR.kind);
    const nodeId = scopeNodeId(target);
    const path = target.getAttribute(ATTR.path);
    const selector = { node_id: nodeId, path: kind === "node" ? null : path, span: null };
    return { element: target, kind, selector };
  }
  function closestText(node) { const el = node?.nodeType === Node.ELEMENT_NODE ? node : node?.parentElement; return el?.closest(`[${ATTR.kind}="text"]`) || null; }
  function rangeOffsets(block, range) {
    // Offsets are half-open [start, end) into the block's text, which is why
    // annotatable text renders verbatim with `white-space: pre-wrap`.
    const before = document.createRange();
    before.selectNodeContents(block);
    before.setEnd(range.startContainer, range.startOffset);
    const start = before.toString().length;
    const end = start + range.toString().length;
    return end > start ? { start, end } : null;
  }
  function selection() {
    const sel = window.getSelection();
    if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return null;
    const range = sel.getRangeAt(0);
    const block = closestText(range.startContainer);
    if (!block || block !== closestText(range.endContainer)) return null;
    const span = rangeOffsets(block, range);
    if (!span) return null;
    return { element: block, kind: "text", quote: range.toString(), rect: range.getBoundingClientRect(), selector: { node_id: scopeNodeId(block), path: block.getAttribute(ATTR.path), span } };
  }

  // ---------- Rendering text with marks ----------
  function segments(text, ranges = []) {
    const cuts = new Set([0, text.length]);
    for (const r of ranges) { if (r.end > r.start && r.start >= 0 && r.end <= text.length) { cuts.add(r.start); cuts.add(r.end); } }
    const points = [...cuts].sort((a, b) => a - b);
    const out = [];
    for (let i = 0; i < points.length - 1; i++) {
      const start = points[i], end = points[i + 1];
      out.push({ start, end, text: text.slice(start, end), ranges: ranges.filter((r) => r.start <= start && r.end >= end) });
    }
    return out;
  }
  // Fill `el` with `text` verbatim, split into spans where `ranges` (and any
  // saved notes on this anchor) begin and end. `decorate(span, ranges)` styles
  // a piece; the text itself is never changed, so offsets stay true.
  function renderText(el, { text, nodeId = null, path, ranges = [], decorate = null }) {
    mark(el, { nodeId, path, kind: "text" });
    el.classList.add("kv-text");
    el.__kv = { text, nodeId, path, ranges, decorate };
    paintText(el);
    return el;
  }
  function paintText(el) {
    const { text, nodeId, path, ranges, decorate } = el.__kv;
    const notes = notesFor({ nodeId, path }).filter((a) => a.selector?.span).map((a) => ({ start: a.selector.span.start, end: a.selector.span.end, note: a }));
    const parts = segments(text, [...ranges, ...notes]);
    el.replaceChildren(...parts.map((p) => {
      if (!p.ranges.length) return document.createTextNode(p.text);
      const span = document.createElement("span");
      span.textContent = p.text;
      const noted = p.ranges.filter((r) => r.note);
      if (noted.length) { span.classList.add("kv-note"); span.title = noted.map((r) => annotationText(r.note)).join("\n—\n"); span.dataset.kvNote = noted.map((r) => r.note.id).join(" "); }
      if (decorate) decorate(span, p.ranges.filter((r) => !r.note));
      return span;
    }));
  }
  const anchorKey = (nodeId, path) => `${nodeId || ""}|${path ?? ""}`;
  // Repaint the text anchors a selector touches (every text anchor without
  // one), then recount the notes on field and node anchors.
  function refreshMarks(selector) {
    for (const el of document.querySelectorAll(".kv-text")) {
      if (el.__kv && (!selector || anchorKey(el.__kv.nodeId, el.__kv.path) === anchorKey(selector.node_id, selector.path))) paintText(el);
    }
    updateNoted();
  }
  function updateNoted() {
    const counts = new Map();
    for (const a of allNotes()) if (a.selector && !a.selector.span) { const k = anchorKey(a.selector.node_id, a.selector.path); counts.set(k, (counts.get(k) || 0) + 1); }
    for (const el of document.querySelectorAll(`[${ATTR.kind}]`)) {
      const path = el.getAttribute(ATTR.kind) === "node" ? null : el.getAttribute(ATTR.path);
      const count = counts.get(anchorKey(scopeNodeId(el), path));
      if (count) el.setAttribute(ATTR.noted, String(count)); else el.removeAttribute(ATTR.noted);
    }
  }
  // Scroll to and flash the element showing `selector`: its exact anchor, else
  // the anchor whose path contains it, else the node it belongs to.
  function reveal(selector) {
    if (!selector) return null;
    const nodeId = selector.node_id || null;
    const own = [...document.querySelectorAll(`[${ATTR.kind}]`)].filter((el) => scopeNodeId(el) === nodeId);
    const path = selector.path ?? null;
    const el = own.find((el) => el.getAttribute(ATTR.path) === path)
      || own.filter((el) => path != null && el.hasAttribute(ATTR.path) && path.startsWith(`${el.getAttribute(ATTR.path)}/`)).sort((a, b) => b.getAttribute(ATTR.path).length - a.getAttribute(ATTR.path).length)[0]
      || (nodeId ? document.querySelector(`[${ATTR.scope}="node:${CSS.escape(nodeId)}"]`) : null);
    if (!el) return null;
    el.scrollIntoView({ block: "center", behavior: "smooth" });
    el.setAttribute(ATTR.flash, "");
    setTimeout(() => el.removeAttribute(ATTR.flash), 1600);
    return el;
  }

  // ---------- Annotations ----------
  const annotationText = (a) => (typeof a.value === "string" ? a.value : JSON.stringify(a.value));
  const sameSelector = (a, nodeId, path) => (a.selector?.node_id || null) === (nodeId || null) && (path === undefined || (a.selector?.path ?? null) === (path ?? null));
  function notesFor({ nodeId = null, path, exact = false } = {}) {
    return (state.view?.annotations || []).filter((a) => !a.investigation_session_id && a.selector && sameSelector(a, nodeId, path) && (!exact || !a.selector.span));
  }
  function sessionNotes() { return (state.view?.annotations || []).filter((a) => !a.investigation_session_id && !a.selector); }
  function allNotes() { return (state.view?.annotations || []).filter((a) => !a.investigation_session_id); }
  function currentLink() { return state.view?.investigation?.investigation_session || null; }
  function answerFor(questionKey) {
    const link = currentLink();
    if (!link) return null;
    // One answer per question: the newest is the one the reviewer meant.
    const answers = state.view.annotations.filter((a) => a.investigation_session_id === link.id && a.question_key === questionKey);
    return answers[answers.length - 1] || null;
  }
  function quoteOf(a) {
    const s = a.selector;
    if (!s?.path) return null;
    const value = pointer(recordFor(s.node_id), s.path);
    if (typeof value !== "string") return value === undefined ? null : JSON.stringify(value).slice(0, 160);
    return s.span ? value.slice(s.span.start, s.span.end) : value.slice(0, 160);
  }
  const randomKey = () => (crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`);
  function requireWrite() { if (!state.view?.capabilities?.can_write) throw new Error("Viewing only: restart the Kitaru MCP server with --mode standard to save notes."); }
  function storeAnnotation(a) {
    const list = state.view.annotations;
    const i = list.findIndex((x) => x.id === a.id);
    if (i >= 0) list[i] = a; else list.push(a);
    refreshMarks(a.selector);
    for (const cb of state.annotationCallbacks) safeCall(cb, list);
  }
  async function writeAnnotation(args) {
    requireWrite();
    const env = await callTool(WRITE_TOOL, args);
    storeAnnotation(env.data);
    return { annotation: env.data, warnings: env.warnings || [] };
  }
  function note({ selector = null, quote = null, text }) {
    return writeAnnotation({ operation: "note", session_id: state.view.session.id, selector: cleanSelector(selector), quote, text, idempotency_key: randomKey() });
  }
  async function answer(questionKey, { text, selector, quote = null }) {
    const existing = answerFor(questionKey);
    if (existing) return edit(existing.id, text);
    const inv = state.view.investigation;
    if (!inv) throw new Error("This session is not open in an investigation.");
    const question = (inv.investigation_session.questions || []).find((q) => q.key === questionKey);
    // Like the Kitaru UI, an answer anchors where its question points.
    const anchorSelector = selector !== undefined ? selector : question?.highlights?.[0]?.selector || null;
    return writeAnnotation({ operation: "answer", investigation_id: inv.id, investigation_session_id: inv.investigation_session.id, question_key: questionKey, selector: cleanSelector(anchorSelector), quote, text, idempotency_key: randomKey() });
  }
  function edit(annotationId, text) {
    return writeAnnotation({ operation: "edit", annotation_id: annotationId, text });
  }
  async function setVerdict(verdict) {
    requireWrite();
    const inv = state.view.investigation;
    if (!inv) throw new Error("Verdicts belong to an investigation; open the session through one.");
    const env = await callTool(WRITE_TOOL, { operation: "verdict", investigation_id: inv.id, session_id: state.view.session.id, verdict: verdict || null });
    inv.investigation_session = env.data.investigation_session;
    inv.status = env.data.investigation_status;
    const entry = inv.sessions.find((s) => s.position === inv.position);
    if (entry) entry.verdict = inv.investigation_session.verdict;
    for (const cb of state.annotationCallbacks) safeCall(cb, state.view.annotations);
    return inv.investigation_session;
  }
  function cleanSelector(s) {
    if (!s) return null;
    const out = {};
    if (s.node_id) out.node_id = s.node_id;
    if (s.path != null) out.path = s.path;
    if (s.span && s.path != null) out.span = { start: s.span.start, end: s.span.end };
    return Object.keys(out).length ? out : null;
  }

  // ---------- Navigation and the conversation ----------
  async function goTo(position) {
    const inv = state.view?.investigation;
    if (!inv) throw new Error("Only an investigation has other sessions to move to.");
    if (position < 0 || position >= inv.sessions.length) return;
    if (!(await openView({ investigation_id: inv.id, position, viewer: state.view.viewer }))) return;
    window.scrollTo({ top: 0 });
    shareContext(`The reviewer moved to session ${position + 1} of ${inv.sessions.length} in investigation "${inv.name}" (session ${state.view.session.id}).`);
  }
  function tellAssistant(text) { return request("ui/message", { role: "user", content: [{ type: "text", text }] }).catch(() => {}); }
  function shareContext(text) { return request("ui/update-model-context", { content: [{ type: "text", text }] }).catch(() => {}); }
  function openLink(url) { return request("ui/open-link", { url }).catch(() => {}); }
  async function toggleFullscreen() {
    const next = state.hostContext.displayMode === "fullscreen" ? "inline" : "fullscreen";
    try { const r = await request("ui/request-display-mode", { mode: next }); state.hostContext.displayMode = r?.mode ?? next; } catch {}
    return state.hostContext.displayMode;
  }

  // ---------- Self check: does every text anchor render its stored text verbatim? ----------
  // Offsets are only true when an anchor's text is the stored string, or a
  // leading part of it for text cut short at the end. Anything a viewer adds or
  // rewrites inside a text anchor (line numbers, markdown, ellipses) shifts every
  // character after it.
  function selfCheck() {
    const problems = [];
    const hints = [];
    let checked = 0;
    for (const el of document.querySelectorAll(`[${ATTR.kind}]`)) {
      const t = targetOf(el);
      checked++;
      const where = { node_id: t.selector.node_id, path: el.getAttribute(ATTR.path), kind: t.kind };
      const record = recordFor(t.selector.node_id);
      if (!record) { problems.push({ ...where, problem: "data-annot-scope names a node that is not in this session" }); continue; }
      if (t.kind === "node") continue;
      if (where.path == null) { problems.push({ ...where, problem: "a text or field anchor needs data-annot-path" }); continue; }
      const value = pointer(record, where.path);
      if (value === undefined) { problems.push({ ...where, problem: "data-annot-path does not resolve in the record" }); continue; }
      if (t.kind !== "text") continue;
      if (typeof value !== "string") { problems.push({ ...where, problem: "a text anchor must point at a string" }); continue; }
      const shown = el.textContent;
      if (shown !== value && !(shown.length < value.length && value.startsWith(shown))) {
        let at = 0;
        while (at < shown.length && shown[at] === value[at]) at++;
        problems.push({ ...where, problem: `rendered text differs from the stored text at character ${at}`, stored: JSON.stringify(value.slice(at, at + 40)), rendered: JSON.stringify(shown.slice(at, at + 40)) });
        continue;
      }
      const cs = getComputedStyle(el);
      if (!/^(pre|preserve|break-spaces)/.test(cs.whiteSpaceCollapse || cs.whiteSpace) && /\s{2}|\n/.test(shown)) {
        hints.push({ ...where, hint: "line breaks and runs of spaces collapse on screen; use white-space: pre-wrap so the reviewer reads the text as stored" });
      }
    }
    return { ok: problems.length === 0, checked, problems, hints };
  }

  // ---------- Optional review kit: a review panel and a comment box ----------
  const h = (tag, props = {}, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(props)) {
      if (v == null || v === false) continue;
      if (k === "class") n.className = v; else if (k === "text") n.textContent = v; else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else if (k.startsWith("data-") || k.startsWith("aria-") || ["title", "type", "placeholder", "rows", "role"].includes(k)) n.setAttribute(k, v); else n[k] = v;
    }
    for (const c of kids.flat()) if (c != null && c !== false) n.append(c);
    return n;
  };
  function toast(text, isError = false) {
    const t = h("div", { class: `kv-toast${isError ? " kv-error" : ""}`, text, role: "status" });
    document.body.append(t);
    setTimeout(() => t.remove(), 4200);
  }
  async function saving(button, work) {
    button.disabled = true;
    try { const r = await work(); (r?.warnings || []).forEach((w) => toast(w)); return r; }
    catch (error) { toast(error.message, true); return null; }
    finally { button.disabled = false; }
  }

  let composer = null;
  function closeComposer() { composer?.remove(); composer = null; }
  // Open a comment box for `target` (from kitaru.selection() or kitaru.target(el)).
  function compose(target, { title } = {}) {
    closeComposer();
    if (!target) return;
    const quote = target.quote ?? quoteOf({ selector: target.selector });
    const box = h("textarea", { class: "kv-input", rows: 2, placeholder: state.view.capabilities.can_write ? "Note" : "Viewing only", disabled: !state.view.capabilities.can_write });
    const save = h("button", { class: "kv-btn kv-primary", text: "Save", disabled: !state.view.capabilities.can_write, onclick: () => saving(save, async () => {
      if (!box.value.trim()) return null;
      const r = await note({ selector: target.selector, quote: target.quote ?? null, text: box.value.trim() });
      closeComposer(); window.getSelection()?.removeAllRanges(); toast("Saved");
      return r;
    }) });
    composer = h("div", { class: "kv-composer", role: "dialog", "aria-label": "Add a note" },
      h("div", { class: "kv-composer-title", text: title || describeTarget(target) }),
      quote ? h("div", { class: "kv-note-quote", text: quote }) : null,
      box, h("div", { class: "kv-row" }, h("button", { class: "kv-btn", text: "Cancel", onclick: closeComposer }), save));
    document.body.append(composer);
    const rect = target.rect || target.element?.getBoundingClientRect();
    const top = (rect ? rect.bottom : 80) + window.scrollY + 8;
    const left = Math.max(8, Math.min((rect ? rect.left : 16) + window.scrollX, window.scrollX + document.documentElement.clientWidth - composer.offsetWidth - 8));
    composer.style.top = `${top}px`; composer.style.left = `${left}px`;
    box.focus();
    reportSize();
  }
  // A button that opens the note box for one field or node, marked as that
  // anchor so its saved-note count shows on it.
  function noteButton({ nodeId = null, path = null, kind = "field" } = {}, { label = "Note", title } = {}) {
    const btn = h("button", { class: "kv-btn", text: label, disabled: !state.view?.capabilities?.can_write });
    mark(btn, { nodeId, path, kind });
    btn.addEventListener("click", () => compose(targetOf(btn), { title }));
    return btn;
  }
  function describeTarget(t) {
    const node = t.selector.node_id ? findNode(t.selector.node_id) : null;
    const where = node ? node.name : "the session";
    return t.kind === "field" ? `Note on ${t.selector.path} in ${where}` : `Note on ${where}`;
  }
  // Offer a note button whenever the reviewer selects text inside a text
  // anchor. `title(selection)` may name the selection in the viewer's terms.
  function enableSelectionNotes({ title = null } = {}) {
    let button = null;
    const hide = () => { button?.remove(); button = null; };
    const onUp = () => setTimeout(() => {
      if (composer?.contains(document.activeElement)) return;
      const sel = selection();
      hide();
      if (!sel || !state.view) return;
      button = h("button", { class: "kv-btn kv-primary kv-float", text: "+ Note", onmousedown: (e) => e.preventDefault(), onclick: () => { hide(); compose(sel, { title: title ? title(sel) : undefined }); } });
      document.body.append(button);
      button.style.top = `${sel.rect.top + window.scrollY - button.offsetHeight - 6}px`;
      button.style.left = `${sel.rect.right + window.scrollX - button.offsetWidth}px`;
    }, 0);
    document.addEventListener("mouseup", onUp);
    document.addEventListener("keyup", (e) => { if (e.key === "Escape") { hide(); closeComposer(); } else if (e.shiftKey) onUp(); });
  }

  // The investigation's questions, the session verdict, saved notes, and
  // navigation between sessions, laid out for a narrow side column.
  function reviewPanel(container) {
    const fail = (e) => toast(e.message, true);
    // Unsaved answers and note edits outlive a re-render, which every save
    // anywhere on the page triggers.
    const drafts = new Map();
    const draftBox = (key, saved, attrs = {}) => {
      const box = h("textarea", { class: "kv-input", rows: 2, "data-draft": key, ...attrs });
      box.value = drafts.has(key) ? drafts.get(key) : saved;
      box.addEventListener("input", () => drafts.set(key, box.value));
      return box;
    };
    const render = () => {
      const v = state.view;
      if (!v) return;
      const active = document.activeElement;
      const focused = container.contains(active) && active.dataset?.draft ? { key: active.dataset.draft, start: active.selectionStart, end: active.selectionEnd } : null;
      const inv = v.investigation, canWrite = v.capabilities.can_write;
      const parts = [];
      if (!canWrite) parts.push(h("div", { class: "kv-banner", text: "Viewing only: start the Kitaru MCP server with --mode standard to save notes." }));
      if (inv) {
        const last = inv.sessions.length - 1;
        parts.push(h("div", { class: "kv-nav" },
          h("span", { class: "kv-nav-name", text: inv.name, title: inv.description || inv.name }),
          h("button", { class: "kv-icon", text: "‹", title: "Previous session", "aria-label": "Previous session", disabled: inv.position === 0, onclick: () => goTo(inv.position - 1).catch(fail) }),
          h("span", { class: "kv-dots" }, inv.sessions.map((s) => h("button", { class: `kv-dot${s.position === inv.position ? " kv-current" : ""}${s.verdict ? ` kv-${s.verdict}` : ""}`, title: `Session ${s.position + 1}${s.verdict ? `: ${s.verdict}` : ""}`, "aria-label": `Session ${s.position + 1}`, onclick: () => goTo(s.position).catch(fail) }))),
          h("button", { class: "kv-icon", text: "›", title: "Next session", "aria-label": "Next session", disabled: inv.position >= last, onclick: () => goTo(inv.position + 1).catch(fail) })));
        for (const q of inv.investigation_session.questions || []) {
          const existing = answerFor(q.key);
          const key = `answer:${v.session.id}:${q.key}`;
          const box = draftBox(key, existing ? annotationText(existing) : "", { placeholder: "Answer", disabled: !canWrite });
          const save = h("button", { class: "kv-btn kv-primary", text: existing ? "Update" : "Save", disabled: !canWrite, onclick: () => saving(save, async () => {
            if (!box.value.trim()) return null;
            const r = await answer(q.key, { text: box.value.trim() }); drafts.delete(key); toast("Saved"); return r;
          }) });
          parts.push(h("section", { class: "kv-section" },
            h("div", { class: "kv-question", text: q.question }),
            (q.highlights || []).map((hl) => h("button", { class: "kv-link", text: `↳ ${hl.description}`, onclick: () => reveal(hl.selector) || toast("Not shown in this viewer") })),
            box, h("div", { class: "kv-row kv-end" }, save)));
        }
        const verdict = inv.investigation_session.verdict;
        parts.push(h("section", { class: "kv-section" }, h("div", { class: "kv-label", text: "Verdict" }),
          h("div", { class: "kv-seg", role: "group", "aria-label": "Verdict" }, VERDICTS.map((x) => {
            const b = h("button", { class: `kv-${x}${verdict === x ? " kv-on" : ""}`, text: x, "aria-pressed": String(verdict === x), disabled: !canWrite, onclick: () => saving(b, () => setVerdict(verdict === x ? null : x)) });
            return b;
          }))));
      }
      const notes = allNotes();
      const sessionBtn = h("button", { class: "kv-link", text: "+ Session note", disabled: !canWrite, onclick: () => compose({ kind: "session", selector: { node_id: null, path: null, span: null }, element: sessionBtn }, { title: "Note on the whole session" }) });
      const list = h("section", { class: "kv-section" }, h("div", { class: "kv-label" }, h("span", { text: notes.length ? `Notes · ${notes.length}` : "Notes" }), sessionBtn));
      if (!notes.length) list.append(h("div", { class: "kv-hint", text: "Select text to add one." }));
      for (const a of notes) {
        const quote = quoteOf(a);
        const node = a.selector?.node_id ? findNode(a.selector.node_id) : null;
        const key = `edit:${a.id}`;
        const editing = drafts.has(key);
        let body = h("div", { class: "kv-note-text", text: annotationText(a) });
        if (editing) {
          const box = draftBox(key, annotationText(a), { onclick: (ev) => ev.stopPropagation() });
          const save = h("button", { class: "kv-btn kv-primary", text: "Save", onclick: (ev) => { ev.stopPropagation(); if (box.value.trim()) saving(save, async () => { const r = await edit(a.id, box.value.trim()); drafts.delete(key); render(); return r; }); } });
          body = h("div", { class: "kv-note-edit" }, box, h("div", { class: "kv-row kv-end" }, h("button", { class: "kv-btn", text: "Cancel", onclick: (ev) => { ev.stopPropagation(); drafts.delete(key); render(); } }), save));
        }
        const editBtn = h("button", { class: "kv-link kv-edit", text: "Edit", hidden: editing, disabled: !canWrite, onclick: (e) => {
          e.stopPropagation();
          drafts.set(key, annotationText(a));
          render();
          container.querySelector(`[data-draft="${key}"]`)?.focus();
        } });
        list.append(h("div", { class: "kv-note-row", onclick: () => reveal(a.selector) },
          h("div", { class: "kv-note-meta" }, h("span", { text: node ? node.name : "session" }), editBtn),
          quote ? h("div", { class: "kv-note-quote", text: quote }) : null,
          body));
      }
      parts.push(list);
      const ask = h("button", { class: "kv-link", text: "Discuss with the assistant", onclick: () => tellAssistant(`I reviewed session ${v.session.name || "#" + v.session.number} (${v.session.id}) in the ${v.viewer} viewer and saved ${notes.length} notes${inv ? ` in investigation "${inv.name}"` : ""}. Read what I saved with kitaru_review_read and tell me what stands out.`) });
      parts.push(h("div", { class: "kv-row kv-foot" }, ask, v.links?.session ? h("button", { class: "kv-link", text: "Kitaru UI ↗", onclick: () => openLink(v.links.session) }) : null));
      container.replaceChildren(...parts);
      if (focused) {
        const box = container.querySelector(`[data-draft="${focused.key}"]`);
        if (box) { box.focus(); box.setSelectionRange(focused.start, focused.end); }
      }
      reportSize();
    };
    container.classList.add("kv-panel");
    onSession(render);
    onAnnotations(render);
    return { refresh: render };
  }

  // ---------- Public API ----------
  function onSession(cb) { state.sessionCallbacks.push(cb); if (state.view && state.mounted) safeCall(cb, state.view); }
  function onAnnotations(cb) { state.annotationCallbacks.push(cb); }
  window.kitaru = Object.freeze({
    get view() { return state.view; },
    get hostContext() { return state.hostContext; },
    onSession, onAnnotations,
    pointer, join, nodeText, sessionText, findNode,
    anchor, mark, target: targetOf, selection, segments, renderText, refreshMarks, reveal,
    notesFor, sessionNotes, allNotes, answerFor, quoteOf, annotationText,
    note, answer, edit, setVerdict,
    goTo, tellAssistant, shareContext, openLink, toggleFullscreen, reportSize, selfCheck,
    ui: Object.freeze({ reviewPanel, compose, noteButton, closeComposer, enableSelectionNotes, toast }),
  });

  async function start() {
    try {
      const result = await request("ui/initialize", { protocolVersion: PROTOCOL_VERSION, appInfo: { name: "kitaru-session-viewer", version: "1.0.0" }, appCapabilities: { availableDisplayModes: ["inline", "fullscreen"] } }, 15000);
      applyHostContext(result?.hostContext || {});
    } catch {
      showStatus("This view needs a host that supports MCP Apps.", true);
    }
    notify("ui/notifications/initialized", {});
    new ResizeObserver(reportSize).observe(document.body);
    reportSize();
  }
  start();
})();
